"""WebAuthn: passkeys and security keys, as a second factor AND for passwordless sign-in.

Two uses of the same credentials (table `webauthn_credentials` in auth.db, created by auth.init_auth_db):
  * second factor   password first, then the account's registered key / passkey (`/api/auth/login/webauthn*`), an alternative to a TOTP code;
  * passwordless    "Sign in with a passkey" (`/api/auth/passkey/*`): discoverable credentials, no username, and user verification (PIN / biometric)
                    is REQUIRED, so the sign-in itself is possession + verification (multi-factor) and skips the password.
Both count as "enrolled" for the organisation's MFA policy (web/mfa_policy.py). Accounts of external identity providers (OIDC, SAML) are never
covered: their provider owns the second factor, exactly like TOTP.

Verification is done by the `webauthn` package (py_webauthn): challenge, origin, RP id hash, flags, signature and the signature-counter rule
(a counter that does not increase = a cloned authenticator = refused; passkeys that always report 0 are accepted). This module adds what the package
does not: single-use server-side challenges, the relying-party / origin decision, storage, ownership checks (the user handle of a passwordless
assertion must be the account that owns the credential), limits, and audit.

Attestation policy (table `webauthn_policy`, administrators; `/api/webauthn/policy`): `none` (default; nothing is asked for), `record` (authenticators are asked for
attestation; the model (AAGUID) and format are stored, and the certificate chain is verified against the trust roots the administrator pasted, if any),
`require` (a registration must carry an attestation whose chain verifies against those roots and, when an allowlist of models is set, come from one of them).
Only a chain that was verified against configured roots counts as "attested"; synced passkeys (Apple, Google, password managers) give no attestation,
so `require` excludes them on purpose. There is no built-in vendor trust store and no FIDO MDS lookup: the roots are whatever you trust.

RP id and origin: WEBAUTHN_RP_ID (default: the request's Host without the port) and WEBAUTHN_ORIGINS (comma list; default: the request's Origin,
accepted only when it is the same host as the Host header, so it works behind a reverse proxy that keeps the Host). Browsers need https, except on
localhost. WEBAUTHN_PASSWORDLESS=off keeps passkeys as a second factor but switches "Sign in with a passkey" off.
"""
import base64
import json
import logging
import os
import secrets
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger("localspark.webauthn")

RP_NAME = "Data Kiln Works"
CHALLENGE_TTL = 300
MAX_CREDENTIALS = 10
MAX_OPEN_CHALLENGES = 2000
_PURPOSES = ("register", "second_factor", "passwordless")


class WebAuthnError(Exception):
    """A refused request; the message is safe to show to the user."""


def available() -> bool:
    try:
        import webauthn  # noqa: F401
        return True
    except Exception:
        return False


def passwordless_enabled() -> bool:
    return available() and os.getenv("WEBAUTHN_PASSWORDLESS", "on").strip().lower() not in ("off", "false", "0", "no")


def _b64u(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _unb64u(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _conn():
    from web.auth import get_db_connection
    return get_db_connection()


def _now() -> int:
    return int(time.time())


# ---------------------------------------------------------------- relying party

def relying_party(headers: Dict[str, str]) -> Tuple[str, List[str]]:
    """(rp_id, accepted origins) for a request."""
    h = {k.lower(): v for k, v in headers.items()}
    host = (h.get("host") or "").strip().lower()
    hostname = host.split("]")[0].lstrip("[") if host.startswith("[") else host.split(":")[0]
    rp_id = os.getenv("WEBAUTHN_RP_ID", "").strip().lower() or hostname
    env = [o.strip().rstrip("/") for o in os.getenv("WEBAUTHN_ORIGINS", "").split(",") if o.strip()]
    if env:
        return rp_id, env
    origin = (h.get("origin") or "").strip().rstrip("/")
    if origin and urlparse(origin).netloc.lower() == host:
        return rp_id, [origin]
    return rp_id, ["http://" + host] if host else []


# ---------------------------------------------------------------- challenges (single use, server side)

def _new_challenge(purpose: str, user_id: Optional[str]) -> Tuple[str, bytes]:
    assert purpose in _PURPOSES
    challenge = secrets.token_bytes(32)
    cid = secrets.token_urlsafe(24)
    c = _conn()
    try:
        with c:
            c.execute("DELETE FROM webauthn_challenges WHERE expires < ?", (_now(),))
            if c.execute("SELECT COUNT(*) FROM webauthn_challenges").fetchone()[0] >= MAX_OPEN_CHALLENGES:
                raise WebAuthnError("Too many sign-ins are in progress. Try again in a minute.")
            c.execute("INSERT INTO webauthn_challenges (id, challenge, purpose, user_id, expires) VALUES (?,?,?,?,?)", (cid, _b64u(challenge), purpose, user_id, _now() + CHALLENGE_TTL))
    finally:
        c.close()
    return cid, challenge


def _take_challenge(cid: str, purpose: str, user_id: Optional[str]) -> bytes:
    """Consumes a challenge (it works once): it must exist, be unexpired, be for this purpose and, when bound, this user."""
    c = _conn()
    try:
        with c:
            row = c.execute("SELECT challenge, purpose, user_id, expires FROM webauthn_challenges WHERE id = ?", (cid or "",)).fetchone()
            c.execute("DELETE FROM webauthn_challenges WHERE id = ?", (cid or "",))
    finally:
        c.close()
    if not row or row["purpose"] != purpose or row["expires"] < _now() or (row["user_id"] and row["user_id"] != user_id):
        raise WebAuthnError("This request expired or was already used. Start again.")
    return _unb64u(row["challenge"])


# ---------------------------------------------------------------- credentials

def _view(r) -> Dict[str, Any]:
    aaguid = r["aaguid"] if "aaguid" in r.keys() else None
    model = None
    if aaguid:
        model = next((m["label"] for m in get_policy()["allowed"] if m["aaguid"] == aaguid), None)
    return {"id": r["id"], "name": r["name"], "created_at": r["created_at"], "last_used_at": r["last_used_at"], "transports": json.loads(r["transports"] or "[]"),
            "device_type": r["device_type"], "backed_up": bool(r["backed_up"]), "user_verified": bool(r["uv"]),
            "aaguid": aaguid if aaguid and aaguid != _ZERO_AAGUID else None, "model": model, "attestation_fmt": r["attestation_fmt"], "attested": bool(r["attested"])}


# ---------------------------------------------------------------- attestation policy

_ZERO_AAGUID = "00000000-0000-0000-0000-000000000000"
POLICY_MODES = ("none", "record", "require")
_UUID = __import__("re").compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def _parse_roots(pem: str) -> List[Any]:
    from cryptography import x509
    try:
        return list(x509.load_pem_x509_certificates((pem or "").encode()))
    except Exception:
        raise WebAuthnError("The trust roots are not valid PEM certificates (-----BEGIN CERTIFICATE-----).")


def _roots_summary(pem: str) -> List[Dict[str, Any]]:
    from cryptography.hazmat.primitives import hashes
    out = []
    for c in (_parse_roots(pem) if (pem or "").strip() else []):
        out.append({"subject": c.subject.rfc4514_string()[:160], "not_after": c.not_valid_after_utc.strftime("%Y-%m-%d"), "expired": c.not_valid_after_utc.timestamp() < time.time(),
                    "sha256": c.fingerprint(hashes.SHA256()).hex()})
    return out


def get_policy() -> Dict[str, Any]:
    c = _conn()
    try:
        r = c.execute("SELECT mode, allowed, roots_pem, updated_by, updated_at FROM webauthn_policy WHERE id = 1").fetchone()
    finally:
        c.close()
    if not r:
        return {"mode": "none", "allowed": [], "roots_pem": "", "updated_by": None, "updated_at": None}
    return {"mode": r["mode"], "allowed": json.loads(r["allowed"] or "[]"), "roots_pem": r["roots_pem"] or "", "updated_by": r["updated_by"], "updated_at": r["updated_at"]}


def policy_view() -> Dict[str, Any]:
    p = get_policy()
    return {"mode": p["mode"], "allowed": p["allowed"], "roots_pem": p["roots_pem"], "roots": _roots_summary(p["roots_pem"]), "updated_by": p["updated_by"], "updated_at": p["updated_at"]}


def set_policy(mode: str, allowed: Any, roots_pem: str, actor: str) -> Dict[str, Any]:
    mode = (mode or "none").strip().lower()
    if mode not in POLICY_MODES:
        raise WebAuthnError("The mode is none, record or require.")
    models: List[Dict[str, str]] = []
    if isinstance(allowed, str):
        allowed = [{"aaguid": p[0], "label": " ".join(p[1:])} for p in (ln.split() for ln in allowed.splitlines()) if p]
    if not isinstance(allowed, list) or len(allowed) > 200:
        raise WebAuthnError("The list of approved models holds at most 200 entries.")
    for m in allowed:
        g = str((m or {}).get("aaguid") or "").strip().lower()
        if not _UUID.match(g):
            raise WebAuthnError(f"'{g}' is not an AAGUID (a UUID such as 2fc0579f-8113-47ea-b116-bb5a8db9202a).")
        if g not in [x["aaguid"] for x in models]:
            models.append({"aaguid": g, "label": str((m or {}).get("label") or "").strip()[:60] or g})
    pem = (roots_pem or "").strip()
    if len(pem) > 100_000:
        raise WebAuthnError("The trust roots are too large (100 KB at most).")
    certs = _parse_roots(pem) if pem else []
    if pem and not certs:
        raise WebAuthnError("The trust roots contain no certificate.")
    if len(certs) > 30:
        raise WebAuthnError("At most 30 trust root certificates.")
    if mode == "require" and not certs:
        raise WebAuthnError("'Require' needs trust roots: without them no authenticator could ever be verified and nobody could register a passkey.")
    if pem:
        from cryptography.hazmat.primitives import serialization
        pem = "".join(c.public_bytes(serialization.Encoding.PEM).decode() for c in certs)
    c = _conn()
    try:
        with c:
            c.execute("UPDATE webauthn_policy SET mode = ?, allowed = ?, roots_pem = ?, updated_by = ?, updated_at = ? WHERE id = 1", (mode, json.dumps(models), pem, actor, _now()))
    finally:
        c.close()
    return policy_view()


def list_credentials(user_id: str) -> List[Dict[str, Any]]:
    c = _conn()
    try:
        return [_view(r) for r in c.execute("SELECT * FROM webauthn_credentials WHERE user_id = ? ORDER BY created_at", (user_id,))]
    finally:
        c.close()


def count(user_id: str) -> int:
    c = _conn()
    try:
        return c.execute("SELECT COUNT(*) FROM webauthn_credentials WHERE user_id = ?", (user_id,)).fetchone()[0]
    finally:
        c.close()


def _get(credential_id: str):
    c = _conn()
    try:
        return c.execute("SELECT * FROM webauthn_credentials WHERE id = ?", (credential_id,)).fetchone()
    finally:
        c.close()


def rename(user_id: str, credential_id: str, name: str) -> None:
    name = (name or "").strip()[:60]
    if not name:
        raise WebAuthnError("Give the passkey a name.")
    c = _conn()
    try:
        with c:
            n = c.execute("UPDATE webauthn_credentials SET name = ? WHERE id = ? AND user_id = ?", (name, credential_id, user_id)).rowcount
    finally:
        c.close()
    if not n:
        raise WebAuthnError("That passkey does not exist.")


def delete(user_id: str, credential_id: str) -> bool:
    c = _conn()
    try:
        with c:
            return c.execute("DELETE FROM webauthn_credentials WHERE id = ? AND user_id = ?", (credential_id, user_id)).rowcount > 0
    finally:
        c.close()


def delete_all(user_id: str) -> int:
    c = _conn()
    try:
        with c:
            return c.execute("DELETE FROM webauthn_credentials WHERE user_id = ?", (user_id,)).rowcount
    finally:
        c.close()


def _descriptors(user_id: str):
    from webauthn.helpers.structs import PublicKeyCredentialDescriptor
    return [PublicKeyCredentialDescriptor(id=_unb64u(v["id"])) for v in list_credentials(user_id)]


# ---------------------------------------------------------------- registration

def _enum_str(x) -> Optional[str]:
    """The package returns some fields as enums and some as plain strings."""
    return None if x is None else str(getattr(x, "value", x))


def pol_mode() -> str:
    try:
        return get_policy()["mode"]
    except Exception:
        return "none"


def registration_options(user: Dict[str, Any], headers: Dict[str, str], require_uv: bool = False) -> Dict[str, Any]:
    if not available():
        raise WebAuthnError("Passkeys are not available on this server (the webauthn package is not installed).")
    from webauthn import generate_registration_options, options_to_json
    from webauthn.helpers.structs import AttestationConveyancePreference, AuthenticatorSelectionCriteria, ResidentKeyRequirement, UserVerificationRequirement
    if count(user["id"]) >= MAX_CREDENTIALS:
        raise WebAuthnError(f"An account can have at most {MAX_CREDENTIALS} passkeys or security keys.")
    rp_id, _ = relying_party(headers)
    if not rp_id or rp_id.replace(".", "").isdigit():
        raise WebAuthnError("Passkeys need the studio to be opened by a host name (not an IP address).")
    cid, challenge = _new_challenge("register", user["id"])
    opts = generate_registration_options(
        rp_id=rp_id, rp_name=RP_NAME, user_id=user["id"].encode(), user_name=user["username"], user_display_name=user.get("display_name") or user["username"],
        challenge=challenge, exclude_credentials=_descriptors(user["id"]),
        attestation=AttestationConveyancePreference.NONE if get_policy()["mode"] == "none" else AttestationConveyancePreference.DIRECT,
        authenticator_selection=AuthenticatorSelectionCriteria(resident_key=ResidentKeyRequirement.PREFERRED,
                                                               user_verification=UserVerificationRequirement.REQUIRED if require_uv else UserVerificationRequirement.PREFERRED))
    return {"challenge_id": cid, "options": json.loads(options_to_json(opts))}


def _verify_registration(credential, expected, rp_id, origins, require_uv: bool):
    """(verified, attested, policy): `attested` = a certificate chain was verified against the administrator's trust roots."""
    from webauthn import verify_registration_response
    from webauthn.helpers import parse_attestation_object
    from webauthn.helpers.structs import AttestationFormat
    pol = get_policy()
    roots = [c for c in (_parse_roots(pol["roots_pem"]) if pol["roots_pem"].strip() else [])]
    from cryptography.hazmat.primitives import serialization
    pem_roots = [c.public_bytes(serialization.Encoding.PEM) for c in roots]
    by_fmt = {f: pem_roots for f in (AttestationFormat.PACKED, AttestationFormat.TPM, AttestationFormat.APPLE, AttestationFormat.ANDROID_KEY,
                                      AttestationFormat.ANDROID_SAFETYNET, AttestationFormat.FIDO_U2F)} if pem_roots else None
    kw = dict(credential=credential, expected_challenge=expected, expected_rp_id=rp_id, expected_origin=origins, require_user_verification=require_uv)
    verified = False
    try:
        v = verify_registration_response(**kw, pem_root_certs_bytes_by_fmt=by_fmt)
        verified = True
    except Exception as exc:
        if pol["mode"] != "record" or not by_fmt:
            raise
        logger.info(f"attestation chain not trusted, recorded without trust: {exc}")
        v = verify_registration_response(**kw)               # `record` never turns an authenticator away: what failed is stored as unattested
    attested = False
    if verified and by_fmt:
        try:
            att = parse_attestation_object(v.attestation_object)
            attested = bool(getattr(att.att_stmt, "x5c", None))
        except Exception:
            attested = False
    return v, attested, pol


def finish_registration(user: Dict[str, Any], challenge_id: str, credential: Dict[str, Any], name: str, headers: Dict[str, str], require_uv: bool = False,
                        purpose: str = "register") -> Dict[str, Any]:
    expected = _take_challenge(challenge_id, purpose, user["id"])
    rp_id, origins = relying_party(headers)
    try:
        v, attested, pol = _verify_registration(credential, expected, rp_id, origins, require_uv)
    except Exception as exc:
        logger.info(f"passkey registration for {user.get('username')} refused: {exc}")
        raise WebAuthnError("The passkey could not be verified. Try again, or use another authenticator." if pol_mode() != "require" else
                            "This authenticator could not be verified. Your organisation only accepts authenticators that prove their make and model (attestation).")
    aaguid = str(getattr(v, "aaguid", "") or "").lower()
    if pol["mode"] == "require":
        if not attested:
            raise WebAuthnError("Your organisation only accepts authenticators that prove their make and model (attestation). This one did not: synced passkeys and some authenticators cannot. Use an approved security key.")
        if pol["allowed"] and aaguid not in [m["aaguid"] for m in pol["allowed"]]:
            raise WebAuthnError(f"This authenticator model ({aaguid}) is not approved by your administrator.")
    cid = _b64u(v.credential_id)
    if _get(cid):
        raise WebAuthnError("That passkey is already registered.")
    if count(user["id"]) >= MAX_CREDENTIALS:
        raise WebAuthnError(f"An account can have at most {MAX_CREDENTIALS} passkeys or security keys.")
    transports = []
    try:
        transports = [str(t) for t in (credential.get("response", {}).get("transports") or [])][:8]
    except Exception:
        pass
    device = _enum_str(getattr(v, "credential_device_type", None)) or "single_device"
    c = _conn()
    try:
        with c:
            c.execute("INSERT INTO webauthn_credentials (id, user_id, public_key, sign_count, name, transports, device_type, backed_up, uv, created_at, aaguid, attestation_fmt, attested) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (cid, user["id"], _b64u(v.credential_public_key), int(v.sign_count), (name or "").strip()[:60] or "Passkey", json.dumps(transports), device,
                       1 if getattr(v, "credential_backed_up", False) else 0, 1 if getattr(v, "user_verified", False) else 0, _now(),
                       aaguid, _enum_str(getattr(v, "fmt", None)), 1 if attested else 0))
    finally:
        c.close()
    return next(x for x in list_credentials(user["id"]) if x["id"] == cid)


# ---------------------------------------------------------------- authentication

def authentication_options(headers: Dict[str, str], user_id: Optional[str] = None) -> Dict[str, Any]:
    """user_id given = second factor for that account (its credentials are named); None = passwordless (discoverable credentials, UV required)."""
    if not available():
        raise WebAuthnError("Passkeys are not available on this server.")
    from webauthn import generate_authentication_options, options_to_json
    from webauthn.helpers.structs import UserVerificationRequirement
    rp_id, _ = relying_party(headers)
    if user_id is None and not passwordless_enabled():
        raise WebAuthnError("Signing in with a passkey is switched off on this server.")
    cid, challenge = _new_challenge("second_factor" if user_id else "passwordless", user_id)
    opts = generate_authentication_options(rp_id=rp_id, challenge=challenge, allow_credentials=_descriptors(user_id) if user_id else None,
                                           user_verification=UserVerificationRequirement.PREFERRED if user_id else UserVerificationRequirement.REQUIRED)
    return {"challenge_id": cid, "options": json.loads(options_to_json(opts))}


def finish_authentication(challenge_id: str, credential: Dict[str, Any], headers: Dict[str, str], user_id: Optional[str] = None) -> str:
    """Verifies an assertion and returns the id of the account it proves. `user_id` (second factor) pins the account; without it (passwordless) the
    account is the credential's owner and the response's user handle must name it."""
    from webauthn import verify_authentication_response
    purpose = "second_factor" if user_id else "passwordless"
    expected = _take_challenge(challenge_id, purpose, user_id)
    try:
        raw_id = str(credential.get("id") or credential.get("rawId") or "")
    except AttributeError:
        raise WebAuthnError("The response is not a passkey assertion.")
    row = _get(raw_id) if raw_id else None
    if not row or (user_id and row["user_id"] != user_id):
        raise WebAuthnError("That passkey is not registered for this account.")
    if not user_id:
        handle = (credential.get("response") or {}).get("userHandle")
        try:
            handle_ok = bool(handle) and _unb64u(handle).decode() == row["user_id"]
        except Exception:
            handle_ok = False
        if not handle_ok:
            raise WebAuthnError("The passkey did not identify an account. Use your username and password instead.")
    rp_id, origins = relying_party(headers)
    try:
        v = verify_authentication_response(credential=credential, expected_challenge=expected, expected_rp_id=rp_id, expected_origin=origins,
                                           credential_public_key=_unb64u(row["public_key"]), credential_current_sign_count=int(row["sign_count"] or 0),
                                           require_user_verification=not user_id)
    except Exception as exc:
        logger.info(f"passkey assertion refused ({purpose}): {exc}")
        raise WebAuthnError("The passkey could not be verified." + (" Its signature counter went backwards: it may have been cloned." if "sign count" in str(exc).lower() else ""))
    c = _conn()
    try:
        with c:
            c.execute("UPDATE webauthn_credentials SET sign_count = ?, last_used_at = ?, backed_up = ? WHERE id = ?",
                      (int(v.new_sign_count), _now(), 1 if getattr(v, "credential_backed_up", False) else 0, raw_id))
    finally:
        c.close()
    return row["user_id"]
