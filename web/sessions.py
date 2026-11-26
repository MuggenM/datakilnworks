"""
Server-side sessions: every session JWT names a row in auth.db, so a session can really end.

A signed JWT on its own stays valid until it expires (24 h). Before this module the only way to cut one off was to change the
account's password (`password_changed_at`), so signing out only deleted the browser's cookie: a copy of the token taken
earlier kept working. Now `auth.create_access_token` creates a row here and puts its secret in the token's `sid` claim, and
`auth.session_user` refuses a token whose row is missing, revoked, expired or idle for too long.

Table `sessions` (auth.db):
  id            public id, shown in the session list and used by the revoke endpoints (never accepted as a credential)
  secret_hash   SHA-256 of the `sid` claim: the database alone (a backup, a leaked file) cannot be turned into a session,
                even together with the JWT signing key
  user_id, created_at, expires_at, last_seen_at, revoked_at, revoked_reason, ip, user_agent, method,
  saml_name_id, saml_session_index   so an IdP-initiated SAML Single Logout can end the matching sessions

Settings: SESSION_IDLE_MINUTES (default 0 = no idle timeout; otherwise a session unused that long is refused). last_seen_at
is written at most once a minute per session, so the idle timeout is accurate to about a minute.
"""

import hashlib
import logging
import os
import secrets
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("localspark.sessions")

TOUCH_INTERVAL_SECONDS = 60
KEEP_ENDED_SECONDS = 7 * 86400              # revoked / expired rows stay this long (the account holder can still see them), then go
MAX_USER_AGENT = 300

_last_touch: Dict[str, float] = {}           # public id -> when last_seen_at was last written (in this process)
_touch_lock = threading.Lock()
_last_prune = 0.0


def _conn() -> sqlite3.Connection:
    from web import auth
    return auth.get_db_connection()


def init_sessions_table(conn: sqlite3.Connection) -> None:
    """Called from auth.init_auth_db inside its transaction."""
    conn.execute("""CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY, secret_hash TEXT NOT NULL UNIQUE, user_id TEXT NOT NULL,
        created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, last_seen_at INTEGER NOT NULL,
        revoked_at INTEGER, revoked_reason TEXT, ip TEXT, user_agent TEXT, method TEXT,
        saml_name_id TEXT, saml_session_index TEXT)""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)")


def _hash(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def idle_seconds() -> int:
    try:
        return max(0, int(float(os.getenv("SESSION_IDLE_MINUTES", "0") or 0) * 60))
    except ValueError:
        return 0


def client_meta(request: Any) -> Dict[str, str]:
    """Address (through the trusted-proxy rules of the IP allowlist) and user agent of the request that signs in."""
    if request is None:
        return {}
    ip = ""
    try:
        from web import ip_allowlist
        ip = ip_allowlist.resolve_client(request.client.host if request.client else None, dict(request.headers)).get("text", "")
    except Exception:
        ip = request.client.host if getattr(request, "client", None) else ""
    return {"ip": (ip or "")[:64], "user_agent": (request.headers.get("user-agent") or "")[:MAX_USER_AGENT]}


def create(user_id: str, expires_at: int, method: str = "", meta: Optional[Dict[str, str]] = None,
           saml_name_id: str = "", saml_session_index: str = "") -> str:
    """Creates a session and returns its secret (goes into the JWT as `sid`; only its hash is stored)."""
    meta = meta or {}
    secret = secrets.token_urlsafe(32)
    now = int(time.time())
    conn = _conn()
    try:
        with conn:
            conn.execute("""INSERT INTO sessions (id, secret_hash, user_id, created_at, expires_at, last_seen_at, ip, user_agent, method,
                            saml_name_id, saml_session_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                         ("s_" + secrets.token_hex(12), _hash(secret), user_id, now, int(expires_at), now, meta.get("ip") or "",
                          meta.get("user_agent") or "", method or "", saml_name_id or "", saml_session_index or ""))
    finally:
        conn.close()
    _maybe_prune()
    return secret


def _row_for(secret: str) -> Optional[sqlite3.Row]:
    if not secret or not isinstance(secret, str) or len(secret) > 200:
        return None
    conn = _conn()
    try:
        return conn.execute("SELECT * FROM sessions WHERE secret_hash = ?", (_hash(secret),)).fetchone()
    finally:
        conn.close()


def validate(secret: str, user_id: str) -> Optional[str]:
    """The session's public id when the session behind `secret` is live and belongs to `user_id`, else None."""
    row = _row_for(secret)
    now = int(time.time())
    if row is None or row["user_id"] != user_id or row["revoked_at"] or row["expires_at"] <= now:
        return None
    idle = idle_seconds()
    if idle and row["last_seen_at"] + idle <= now:
        revoke(row["id"], "idle")
        return None
    _touch(row["id"], now)
    return row["id"]


def public_id_of(secret: str) -> Optional[str]:
    row = _row_for(secret)
    return row["id"] if row is not None else None


def _touch(public_id: str, now: int) -> None:
    with _touch_lock:
        if now - _last_touch.get(public_id, 0) < TOUCH_INTERVAL_SECONDS:
            return
        _last_touch[public_id] = now
        if len(_last_touch) > 10000:
            _last_touch.clear()
    try:
        conn = _conn()
        try:
            with conn:
                conn.execute("UPDATE sessions SET last_seen_at = ? WHERE id = ? AND last_seen_at < ?", (now, public_id, now))
        finally:
            conn.close()
    except sqlite3.Error as exc:                    # a busy database must not fail the request that is being served
        logger.debug(f"session touch skipped: {exc}")


def revoke(public_id: str, reason: str = "signed out", user_id: Optional[str] = None) -> bool:
    """Ends one session. With `user_id` only when it belongs to that user (an account holder ending their own session)."""
    conn = _conn()
    try:
        with conn:
            sql = "UPDATE sessions SET revoked_at = ?, revoked_reason = ? WHERE id = ? AND revoked_at IS NULL"
            args: List[Any] = [int(time.time()), reason[:60], public_id]
            if user_id is not None:
                sql += " AND user_id = ?"
                args.append(user_id)
            return conn.execute(sql, args).rowcount > 0
    finally:
        conn.close()


def revoke_secret(secret: str, reason: str = "signed out") -> bool:
    row = _row_for(secret)
    return revoke(row["id"], reason) if row is not None else False


def revoke_all(user_id: str, reason: str, except_id: Optional[str] = None) -> int:
    """Ends every live session of a user (optionally keeping one: "sign out my other sessions"). Returns how many ended."""
    conn = _conn()
    try:
        with conn:
            sql = "UPDATE sessions SET revoked_at = ?, revoked_reason = ? WHERE user_id = ? AND revoked_at IS NULL"
            args: List[Any] = [int(time.time()), reason[:60], user_id]
            if except_id:
                sql += " AND id != ?"
                args.append(except_id)
            return conn.execute(sql, args).rowcount
    finally:
        conn.close()


def revoke_saml(name_id: str, session_index: Optional[str] = None, reason: str = "SAML single logout") -> int:
    """IdP-initiated Single Logout: ends the sessions that this SAML NameID (and SessionIndex, when the IdP names one) started."""
    if not name_id:
        return 0
    conn = _conn()
    try:
        with conn:
            sql = "UPDATE sessions SET revoked_at = ?, revoked_reason = ? WHERE saml_name_id = ? AND revoked_at IS NULL"
            args: List[Any] = [int(time.time()), reason[:60], name_id]
            if session_index:
                sql += " AND saml_session_index = ?"
                args.append(session_index)
            return conn.execute(sql, args).rowcount
    finally:
        conn.close()


def list_for(user_id: str, current_id: Optional[str] = None, include_ended: bool = False) -> List[Dict[str, Any]]:
    """The user's sessions, newest first, without any secret. `current` marks the one making the request."""
    now = int(time.time())
    idle = idle_seconds()
    conn = _conn()
    try:
        rows = conn.execute("SELECT id, created_at, expires_at, last_seen_at, revoked_at, revoked_reason, ip, user_agent, method "
                            "FROM sessions WHERE user_id = ? ORDER BY created_at DESC LIMIT 200", (user_id,)).fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        d = dict(r)
        if d["revoked_at"]:
            state = "revoked"
        elif d["expires_at"] <= now:
            state = "expired"
        elif idle and d["last_seen_at"] + idle <= now:
            state = "idle"
        else:
            state = "active"
        if state != "active" and not include_ended:
            continue
        d["state"] = state
        d["current"] = d["id"] == current_id
        out.append(d)
    return out


def _maybe_prune() -> None:
    global _last_prune
    now = time.time()
    if now - _last_prune < 3600:
        return
    _last_prune = now
    cutoff = int(now) - KEEP_ENDED_SECONDS
    try:
        conn = _conn()
        try:
            with conn:
                conn.execute("DELETE FROM sessions WHERE expires_at < ? OR (revoked_at IS NOT NULL AND revoked_at < ?)", (cutoff, cutoff))
        finally:
            conn.close()
    except sqlite3.Error as exc:
        logger.debug(f"session prune skipped: {exc}")
