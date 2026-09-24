"""
TOTP two-factor authentication (RFC 6238) for accounts that sign in with a password (local and LDAP; an OIDC
account's second factor belongs to its identity provider, so this module never applies to it).

- Enrolment is two steps so a typo can't lock anyone out: `begin_setup` stores a *pending* secret, and only a valid
  code from the authenticator app (`confirm_setup`) turns MFA on and returns the one-time backup codes.
- The secret is encrypted at rest (Fernet, key in `$WAREHOUSE_DIR/.metadata/mfa.key`, mode 0600), so a copy of
  `auth.db` alone (a backup, a leaked file) is not enough to mint codes. Backup codes are stored as HMACs.
- `verify_login` accepts a current code (window +-1 step) or an unused backup code. A code can be used once (a step
  at or before the last accepted one is refused: replay protection), and 5 failures lock the account's second factor
  for 5 minutes, so the 6-digit space can't be brute-forced with a stolen password.
- Login is two-phase (`create_mfa_token`): the password step returns a 5-minute token that proves only "the password
  was right"; it is not a session and works only at the MFA step. It stops working if the password changes meanwhile.
"""

import base64
import datetime
import hashlib
import hmac
import json
import os
import secrets
import struct
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import jwt
from cryptography.fernet import Fernet, InvalidToken

ISSUER_NAME = "Data Kiln Works"
STEP_SECONDS = 30
DIGITS = 6
WINDOW = 1
MAX_FAILURES = 5
LOCK_SECONDS = 300
BACKUP_CODE_COUNT = 10
MFA_TOKEN_TTL_SECONDS = 300


# ---------------------------------------------------------------- RFC 6238 / RFC 4226 primitives

def hotp(secret: bytes, counter: int, digits: int = DIGITS) -> str:
    digest = hmac.new(secret, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    value = (struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF) % (10 ** digits)
    return str(value).zfill(digits)


def totp(secret: bytes, at: Optional[float] = None, digits: int = DIGITS) -> str:
    return hotp(secret, int((time.time() if at is None else at) // STEP_SECONDS), digits)


def match_step(secret: bytes, code: str, at: Optional[float] = None) -> Optional[int]:
    """The time step `code` is valid for (within the window), or None. Compares in constant time."""
    if not (isinstance(code, str) and len(code) == DIGITS and code.isdigit()):
        return None
    now_step = int((time.time() if at is None else at) // STEP_SECONDS)
    found = None
    for step in range(now_step - WINDOW, now_step + WINDOW + 1):
        if hmac.compare_digest(hotp(secret, step), code):
            found = step
    return found


def new_secret() -> bytes:
    return secrets.token_bytes(20)


def b32(secret: bytes) -> str:
    return base64.b32encode(secret).decode().rstrip("=")


def otpauth_uri(secret: bytes, account: str) -> str:
    label = quote(f"{ISSUER_NAME}:{account}", safe="")
    return f"otpauth://totp/{label}?secret={b32(secret)}&issuer={quote(ISSUER_NAME)}&algorithm=SHA1&digits={DIGITS}&period={STEP_SECONDS}"


# ---------------------------------------------------------------- key material

def _key_path() -> str:
    return os.path.join(os.getenv("WAREHOUSE_DIR", "/workspace/warehouse"), ".metadata", "mfa.key")


def _key() -> bytes:
    path = _key_path()
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as f:
                f.write(base64.urlsafe_b64encode(secrets.token_bytes(32)))
        except FileExistsError:
            pass
    with open(path, "rb") as f:
        return f.read().strip()


def _encrypt(raw: bytes) -> str:
    return Fernet(_key()).encrypt(raw).decode()


def _decrypt(blob: str) -> bytes:
    try:
        return Fernet(_key()).decrypt(blob.encode())
    except InvalidToken as exc:
        raise ValueError("The stored second-factor secret cannot be decrypted (mfa.key changed?). An administrator must reset MFA for this account.") from exc


def _backup_hash(code: str) -> str:
    return hmac.new(_key(), code.encode(), hashlib.sha256).hexdigest()


def _normalize_backup(code: str) -> str:
    return "".join(c for c in (code or "").upper() if c.isalnum())


def _new_backup_codes() -> List[str]:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"                      # no 0/O/1/I: read aloud or copied by hand
    return ["-".join("".join(secrets.choice(alphabet) for _ in range(4)) for _ in range(3)) for _ in range(BACKUP_CODE_COUNT)]


# ---------------------------------------------------------------- storage

def _conn():
    from web.auth import get_db_connection
    return get_db_connection()


def _row(user_id: str):
    conn = _conn()
    try:
        return conn.execute("SELECT id, username, is_active, deleted_at, password_changed_at, totp_secret, totp_pending, "
                            "totp_enabled, totp_last_step, totp_backup, totp_failures, totp_locked_until FROM users WHERE id = ?",
                            (user_id,)).fetchone()
    finally:
        conn.close()


def is_enabled(user_id: str) -> bool:
    row = _row(user_id)
    return bool(row and row["totp_enabled"])


def status(user_id: str) -> Dict[str, Any]:
    row = _row(user_id)
    if not row:
        return {"enabled": False, "backup_codes_remaining": 0, "pending": False}
    return {"enabled": bool(row["totp_enabled"]), "pending": bool(row["totp_pending"]),
            "backup_codes_remaining": len(json.loads(row["totp_backup"] or "[]")) if row["totp_enabled"] else 0}


def begin_setup(user_id: str, username: str) -> Dict[str, str]:
    row = _row(user_id)
    if not row:
        raise ValueError("Account not found.")
    if row["totp_enabled"]:
        raise ValueError("Two-factor authentication is already enabled. Disable it first to enrol a new device.")
    secret = new_secret()
    conn = _conn()
    try:
        with conn:
            conn.execute("UPDATE users SET totp_pending = ? WHERE id = ?", (_encrypt(secret), user_id))
    finally:
        conn.close()
    return {"secret": b32(secret), "otpauth_uri": otpauth_uri(secret, username)}


def confirm_setup(user_id: str, code: str) -> List[str]:
    """Turns MFA on if `code` is valid for the pending secret. Returns the backup codes (shown exactly once)."""
    row = _row(user_id)
    if not row or not row["totp_pending"]:
        raise ValueError("No enrolment in progress. Start setup again.")
    secret = _decrypt(row["totp_pending"])
    step = match_step(secret, (code or "").strip())
    if step is None:
        raise ValueError("That code is not valid. Check the time on your device and try again.")
    codes = _new_backup_codes()
    conn = _conn()
    try:
        with conn:
            conn.execute("UPDATE users SET totp_secret = ?, totp_pending = NULL, totp_enabled = 1, totp_last_step = ?, totp_backup = ?, "
                         "totp_failures = 0, totp_locked_until = 0 WHERE id = ?",
                         (_encrypt(secret), step, json.dumps([_backup_hash(_normalize_backup(c)) for c in codes]), user_id))
    finally:
        conn.close()
    return codes


def verify_login(user_id: str, code: str, now: Optional[float] = None) -> Tuple[bool, str]:
    """(ok, reason). Accepts a current TOTP code or an unused backup code; enforces replay protection and lockout."""
    now = time.time() if now is None else now
    row = _row(user_id)
    if not row or not row["totp_enabled"]:
        return False, "Two-factor authentication is not enabled for this account."
    if (row["totp_locked_until"] or 0) > now:
        return False, "Too many incorrect codes. Try again in a few minutes."
    code = (code or "").strip()
    ok = False
    conn = _conn()
    try:
        secret = _decrypt(row["totp_secret"])
        step = match_step(secret, code, now)
        if step is not None and step > (row["totp_last_step"] or 0):
            ok = True
            with conn:
                conn.execute("UPDATE users SET totp_last_step = ?, totp_failures = 0 WHERE id = ?", (step, user_id))
        elif step is None and not code.isdigit():
            digest = _backup_hash(_normalize_backup(code))
            remaining = json.loads(row["totp_backup"] or "[]")
            hit = next((h for h in remaining if hmac.compare_digest(h, digest)), None)
            if hit is not None:
                ok = True
                remaining.remove(hit)
                with conn:
                    conn.execute("UPDATE users SET totp_backup = ?, totp_failures = 0 WHERE id = ?", (json.dumps(remaining), user_id))
        if not ok:
            failures = (row["totp_failures"] or 0) + 1
            with conn:
                if failures >= MAX_FAILURES:
                    conn.execute("UPDATE users SET totp_failures = 0, totp_locked_until = ? WHERE id = ?", (int(now) + LOCK_SECONDS, user_id))
                else:
                    conn.execute("UPDATE users SET totp_failures = ? WHERE id = ?", (failures, user_id))
    finally:
        conn.close()
    return (True, "") if ok else (False, "That code is not valid.")


def regenerate_backup_codes(user_id: str) -> List[str]:
    codes = _new_backup_codes()
    conn = _conn()
    try:
        with conn:
            conn.execute("UPDATE users SET totp_backup = ? WHERE id = ? AND totp_enabled = 1",
                         (json.dumps([_backup_hash(_normalize_backup(c)) for c in codes]), user_id))
    finally:
        conn.close()
    return codes


def disable(user_id: str) -> bool:
    conn = _conn()
    try:
        with conn:
            res = conn.execute("UPDATE users SET totp_secret = NULL, totp_pending = NULL, totp_enabled = 0, totp_last_step = 0, "
                               "totp_backup = NULL, totp_failures = 0, totp_locked_until = 0 WHERE id = ?", (user_id,))
            return res.rowcount > 0
    finally:
        conn.close()


# ---------------------------------------------------------------- two-phase login token

def _secret_key() -> str:
    """A key derived from (not equal to) the session-signing key, so an MFA token can never validate as a session
    even if a session check forgot to look at `purpose`."""
    from web.auth import JWT_SECRET_KEY
    return hmac.new(JWT_SECRET_KEY.encode(), b"dkw-mfa-token-v1", hashlib.sha256).hexdigest()


def create_mfa_token(user: Dict[str, Any]) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    return jwt.encode({"sub": user["id"], "purpose": "mfa", "pc": user.get("password_changed_at") or 0,
                       "iat": int(now.timestamp()), "exp": now + datetime.timedelta(seconds=MFA_TOKEN_TTL_SECONDS)},
                      _secret_key(), algorithm="HS256")


def user_from_mfa_token(token: str) -> Optional[Dict[str, Any]]:
    """The account a valid MFA token names, or None (bad/expired/other-purpose token, deleted or deactivated
    account, or a password change since the token was issued)."""
    try:
        payload = jwt.decode(token or "", _secret_key(), algorithms=["HS256"], options={"require": ["exp", "sub"]})
    except Exception:
        return None
    if payload.get("purpose") != "mfa":
        return None
    row = _row(payload["sub"])
    if not row or not row["is_active"] or row["deleted_at"] or (row["password_changed_at"] or 0) != payload.get("pc", 0):
        return None
    from web.auth import get_user_by_id
    return get_user_by_id(row["id"])
