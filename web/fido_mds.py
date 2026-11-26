"""FIDO Metadata Service (MDS3) cache for WebAuthn passkeys and security keys (`web/webauthn_auth.py`).

The FIDO Alliance publishes one signed JSON blob (a JWS: `header.payload.signature`) listing every certified authenticator model by AAGUID, with
its certification level, its human-readable description and its **status history** -- including `REVOKED`, which means a real, disclosed security
issue in that model. This module fetches that blob (`FIDO_MDS_URL`, default the real service), verifies it, and caches one row per AAGUID
(`fido_mds_entries` in auth.db) so `webauthn_auth` can show a model's name and certification status next to a credential, and refuse to REGISTER a
new credential of a model the Alliance has revoked -- independent of the administrator's own attestation policy (`webauthn_policy`), since a
revocation is a fact about the hardware, not a local trust decision.

**Trust is explicit, like the attestation policy's own trust roots**: there is no vendor root baked into this module. An administrator pastes the
FIDO Alliance's MDS root certificate (`fido_mds_config.root_pem`, public information, published at https://mds.fidoalliance.org/Root.cer) through
`/api/webauthn/mds/config`; without it, `refresh()` does nothing (there is nothing to verify the blob's signing certificate against). Verification
is a real X.509 chain walk (`_verify_chain`): the blob's JWS header carries its signing certificate chain (`x5c`, leaf first per RFC 7515), each
certificate's validity window is checked, and each certificate's signature is checked against the next certificate in the chain, ending at a
configured root by subject-name match followed by an actual signature check (a forged certificate with a matching subject but the wrong key fails
that final step) -- then the JWS signature itself is checked against the leaf's public key (PyJWT). None of this depends on the `webauthn` package's
own attestation verification code (`_verify_registration`); it is a separate, generic JWS + X.509 check.

A background loop (`mds_refresh_loop`, started in `startup_event`) refreshes at most once an hour, and only when the cached blob's own
`nextUpdate` date has passed (or, if that is unknown, once a day); `refresh(force=True)` (the admin "Refresh now" button, and automatically once
after saving a new root) ignores that and fetches immediately. A failed refresh (network, an expired or untrusted chain, a bad signature) is kept
as `last_error` and never clears the existing cache: a stale cache that still refuses known-revoked models is safer than an empty one.
"""
import base64
import datetime
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("localspark.fido_mds")

DEFAULT_URL = "https://mds3.fidoalliance.org/"
MAX_ENTRIES = 20000
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class MdsError(ValueError):
    """A refused configuration or a failed refresh; the message is safe to show to an administrator."""


def _conn():
    from web.auth import get_db_connection
    return get_db_connection()


def init_db() -> None:
    c = _conn()
    try:
        with c:
            c.execute("""CREATE TABLE IF NOT EXISTS fido_mds_config (id INTEGER PRIMARY KEY CHECK (id = 1), url TEXT NOT NULL DEFAULT '',
                root_pem TEXT NOT NULL DEFAULT '', updated_by TEXT, updated_at INTEGER)""")
            c.execute("INSERT OR IGNORE INTO fido_mds_config (id, url, root_pem) VALUES (1, '', '')")
            c.execute("""CREATE TABLE IF NOT EXISTS fido_mds_state (id INTEGER PRIMARY KEY CHECK (id = 1), last_refresh_at INTEGER,
                last_error TEXT, entry_count INTEGER NOT NULL DEFAULT 0, next_update TEXT, serial_no INTEGER)""")
            c.execute("INSERT OR IGNORE INTO fido_mds_state (id) VALUES (1)")
            c.execute("""CREATE TABLE IF NOT EXISTS fido_mds_entries (aaguid TEXT PRIMARY KEY, description TEXT, status TEXT,
                revoked INTEGER NOT NULL DEFAULT 0, status_reports TEXT NOT NULL DEFAULT '[]', updated_at INTEGER NOT NULL)""")
    finally:
        c.close()


# ---------------------------------------------------------------- configuration (root of trust, URL)

def _parse_roots(pem: str) -> List[Any]:
    from cryptography import x509
    try:
        return list(x509.load_pem_x509_certificates((pem or "").encode()))
    except Exception:
        raise MdsError("The trust root is not a valid PEM certificate (-----BEGIN CERTIFICATE-----).")


def _roots_summary(pem: str) -> List[Dict[str, Any]]:
    from cryptography.hazmat.primitives import hashes
    out = []
    for c in (_parse_roots(pem) if (pem or "").strip() else []):
        out.append({"subject": c.subject.rfc4514_string()[:160], "not_after": c.not_valid_after_utc.strftime("%Y-%m-%d"),
                    "expired": c.not_valid_after_utc.timestamp() < time.time(), "sha256": c.fingerprint(hashes.SHA256()).hex()})
    return out


def get_config() -> Dict[str, Any]:
    c = _conn()
    try:
        r = c.execute("SELECT url, root_pem, updated_by, updated_at FROM fido_mds_config WHERE id = 1").fetchone()
    finally:
        c.close()
    if not r:
        return {"url": "", "root_pem": "", "updated_by": None, "updated_at": None}
    return {"url": r["url"] or "", "root_pem": r["root_pem"] or "", "updated_by": r["updated_by"], "updated_at": r["updated_at"]}


def config_view() -> Dict[str, Any]:
    cfg = get_config()
    return {**cfg, "roots": _roots_summary(cfg["root_pem"]), "effective_url": cfg["url"].strip() or DEFAULT_URL}


def set_config(url: str, root_pem: str, actor: str) -> Dict[str, Any]:
    url = (url or "").strip()
    if url and not (url.startswith("https://") or url.startswith("http://")):
        raise MdsError("The URL must be http(s).")
    if len(url) > 500:
        raise MdsError("The URL is too long.")
    pem = (root_pem or "").strip()
    if len(pem) > 100_000:
        raise MdsError("The trust root is too large (100 KB at most).")
    certs = _parse_roots(pem) if pem else []
    if pem and not certs:
        raise MdsError("The trust root contains no certificate.")
    if len(certs) > 10:
        raise MdsError("At most 10 trust root certificates.")
    if pem:
        from cryptography.hazmat.primitives import serialization
        pem = "".join(c.public_bytes(serialization.Encoding.PEM).decode() for c in certs)
    c = _conn()
    try:
        with c:
            c.execute("UPDATE fido_mds_config SET url = ?, root_pem = ?, updated_by = ?, updated_at = ? WHERE id = 1", (url, pem, actor, int(time.time())))
    finally:
        c.close()
    return config_view()


# ---------------------------------------------------------------- state / lookup

def _save_state(last_refresh_at: Optional[int] = None, last_error: Any = "__keep__", entry_count: Optional[int] = None,
                next_update: Any = "__keep__", serial_no: Any = "__keep__") -> None:
    c = _conn()
    try:
        row = c.execute("SELECT last_refresh_at, last_error, entry_count, next_update, serial_no FROM fido_mds_state WHERE id = 1").fetchone()
        with c:
            c.execute("UPDATE fido_mds_state SET last_refresh_at = ?, last_error = ?, entry_count = ?, next_update = ?, serial_no = ? WHERE id = 1", (
                last_refresh_at if last_refresh_at is not None else (row["last_refresh_at"] if row else None),
                last_error if last_error != "__keep__" else (row["last_error"] if row else None),
                entry_count if entry_count is not None else (row["entry_count"] if row else 0),
                next_update if next_update != "__keep__" else (row["next_update"] if row else None),
                serial_no if serial_no != "__keep__" else (row["serial_no"] if row else None)))
    finally:
        c.close()


def status() -> Dict[str, Any]:
    cfg = get_config()
    c = _conn()
    try:
        r = c.execute("SELECT last_refresh_at, last_error, entry_count, next_update, serial_no FROM fido_mds_state WHERE id = 1").fetchone()
        n = c.execute("SELECT COUNT(*) FROM fido_mds_entries").fetchone()[0]
    finally:
        c.close()
    return {"configured": bool(cfg["root_pem"].strip()), "url": cfg["url"], "effective_url": cfg["url"].strip() or DEFAULT_URL,
            "last_refresh_at": (r["last_refresh_at"] if r else None), "last_error": (r["last_error"] if r else None),
            "entry_count": n, "next_update": (r["next_update"] if r else None), "serial_no": (r["serial_no"] if r else None)}


def lookup(aaguid: str) -> Optional[Dict[str, Any]]:
    """The cached model info for an AAGUID, or None (unknown model, or the cache has never been refreshed)."""
    a = (aaguid or "").strip().lower()
    if not a:
        return None
    c = _conn()
    try:
        r = c.execute("SELECT aaguid, description, status, revoked, status_reports, updated_at FROM fido_mds_entries WHERE aaguid = ?", (a,)).fetchone()
    finally:
        c.close()
    if not r:
        return None
    return {"aaguid": r["aaguid"], "description": r["description"], "status": r["status"], "revoked": bool(r["revoked"]),
            "status_reports": json.loads(r["status_reports"] or "[]"), "updated_at": r["updated_at"]}


def _store_entries(entries: List[Dict[str, Any]]) -> None:
    now = int(time.time())
    rows = []
    for e in entries:
        aaguid = str((e or {}).get("aaguid") or "").strip().lower()
        if not aaguid or not _UUID.match(aaguid):
            continue                      # U2F-only entries (keyed by a key identifier, not an AAGUID) do not apply to WebAuthn passkeys
        reports = e.get("statusReports") or []
        latest = reports[-1] if reports else {}
        current_status = str(latest.get("status") or "UNKNOWN")
        stmt = e.get("metadataStatement") or {}
        description = str(stmt.get("description") or e.get("description") or "")[:200]
        rows.append((aaguid, description, current_status, 1 if current_status == "REVOKED" else 0, json.dumps(reports)[:20000], now))
    c = _conn()
    try:
        with c:
            c.execute("DELETE FROM fido_mds_entries")
            c.executemany("INSERT INTO fido_mds_entries (aaguid, description, status, revoked, status_reports, updated_at) VALUES (?,?,?,?,?,?)", rows)
    finally:
        c.close()


# ---------------------------------------------------------------- fetch, verify, refresh

def _b64u_decode(seg: str) -> bytes:
    return base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4))


def _leaf_and_chain(header: Dict[str, Any]) -> List[Any]:
    from cryptography import x509
    x5c = header.get("x5c")
    if not x5c or not isinstance(x5c, list):
        raise MdsError("The metadata blob's JWS header carries no certificate chain (x5c).")
    try:
        return [x509.load_der_x509_certificate(base64.b64decode(c)) for c in x5c]
    except Exception:
        raise MdsError("The metadata blob's certificate chain could not be parsed.")


def _verify_signed_by(cert, issuer) -> None:
    from cryptography.hazmat.primitives.asymmetric import padding, rsa, ec
    pub = issuer.public_key()
    if isinstance(pub, rsa.RSAPublicKey):
        pub.verify(cert.signature, cert.tbs_certificate_bytes, padding.PKCS1v15(), cert.signature_hash_algorithm)
    elif isinstance(pub, ec.EllipticCurvePublicKey):
        pub.verify(cert.signature, cert.tbs_certificate_bytes, ec.ECDSA(cert.signature_hash_algorithm))
    else:
        raise MdsError("The certificate chain uses an unsupported key type.")


def _verify_chain(chain: List[Any], roots: List[Any], now: datetime.datetime) -> None:
    """Each certificate in `chain` (leaf first, RFC 7515 x5c order) must be currently valid and be signed by the next one in the
    chain; the last one must be signed by one of the configured trust roots (matched by subject, then an actual signature check --
    a forged certificate sharing a root's subject name but not its key fails that check)."""
    if not roots:
        raise MdsError("No trust root is configured for the FIDO Metadata Service.")
    for cert in chain:
        if not (cert.not_valid_before_utc <= now <= cert.not_valid_after_utc):
            raise MdsError("A certificate in the metadata blob's chain is not currently valid.")
    for i, current in enumerate(chain):
        issuer = chain[i + 1] if i + 1 < len(chain) else next((r for r in roots if r.subject == current.issuer), None)
        if issuer is None:
            raise MdsError("The metadata blob's certificate chain does not lead to the configured trust root.")
        try:
            _verify_signed_by(current, issuer)
        except MdsError:
            raise
        except Exception:
            raise MdsError("The metadata blob's certificate chain does not verify.")


def _verify_jws_payload(blob: str, leaf) -> Dict[str, Any]:
    import jwt as pyjwt
    from cryptography.hazmat.primitives import serialization
    pub_pem = leaf.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    header = pyjwt.get_unverified_header(blob)
    alg = header.get("alg")
    if alg not in ("RS256", "RS384", "RS512", "ES256", "ES384", "ES512"):
        raise MdsError(f"Unsupported signature algorithm '{alg}'.")
    try:
        return pyjwt.decode(blob, key=pub_pem, algorithms=[alg], options={"verify_exp": False, "verify_nbf": False, "verify_iat": False, "verify_aud": False})
    except Exception as exc:
        raise MdsError(f"The metadata blob's signature did not verify: {exc}")


def _due(st: Dict[str, Any]) -> bool:
    if st.get("next_update"):
        try:
            nu = datetime.datetime.fromisoformat(st["next_update"])
            if nu.tzinfo is None:
                nu = nu.replace(tzinfo=datetime.timezone.utc)
            return datetime.datetime.now(datetime.timezone.utc) >= nu
        except Exception:
            return True
    if st.get("last_refresh_at"):
        return (time.time() - st["last_refresh_at"]) > 86400
    return True


def refresh(force: bool = False) -> Dict[str, Any]:
    """Fetches and verifies the MDS blob and replaces the cache. A no-op (returns the current status) when no trust root is
    configured, or (unless `force`) when the cache is not due yet. A failure is recorded as `last_error` and never clears the
    existing cache."""
    cfg = get_config()
    if not cfg["root_pem"].strip():
        return status()
    st = status()
    if not force and not _due(st):
        return st
    url = cfg["url"].strip() or DEFAULT_URL
    try:
        import requests
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        blob = resp.text.strip()
        parts = blob.split(".")
        if len(parts) != 3:
            raise MdsError("The response is not a JWS (header.payload.signature).")
        header = json.loads(_b64u_decode(parts[0]))
        chain = _leaf_and_chain(header)
        roots = _parse_roots(cfg["root_pem"])
        _verify_chain(chain, roots, datetime.datetime.now(datetime.timezone.utc))
        payload = _verify_jws_payload(blob, chain[0])
        entries = payload.get("entries")
        if not isinstance(entries, list):
            raise MdsError("The metadata blob has no entries list.")
        entries = entries[:MAX_ENTRIES]
        _store_entries(entries)
        _save_state(last_refresh_at=int(time.time()), last_error=None, entry_count=len(entries), next_update=payload.get("nextUpdate"), serial_no=payload.get("no"))
    except Exception as exc:
        logger.warning(f"FIDO MDS refresh failed: {exc}")
        _save_state(last_refresh_at=int(time.time()), last_error=str(exc)[:500])
    return status()


async def mds_refresh_loop() -> None:
    """Started in `startup_event`; checks hourly but only actually fetches when the cache is due (see `_due`)."""
    import asyncio
    while True:
        try:
            await asyncio.to_thread(refresh, False)
        except Exception:
            logger.exception("FIDO MDS refresh loop iteration failed")
        await asyncio.sleep(3600)
