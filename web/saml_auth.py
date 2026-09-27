"""SAML 2.0 single sign-on (this studio is the Service Provider; the customer's IdP: Entra ID, Okta, Keycloak, ADFS, Shibboleth...).

Flow: `GET /api/auth/saml/login` builds an AuthnRequest (HTTP-Redirect binding) and remembers it server-side under a random RelayState;
the IdP authenticates the person and POSTs a signed response to `POST /api/auth/saml/acs` (HTTP-POST binding), where it is validated by
python3-saml in *strict* mode (issuer, audience, destination, InResponseTo, NotBefore/NotOnOrAfter, XML signature over the assertion or the
response against the configured IdP certificate, deprecated algorithms rejected, signature-wrapping checks) and provisioned like an OIDC user.

What this module adds on top of the library, because a correct signature alone is not enough:
  * **Solicited only** (default): the response must answer a request this studio made (InResponseTo = the stored request id, single use,
    10 minutes); an unsolicited IdP-initiated response is refused unless `allow_idp_initiated` is set.
  * **Replay protection**: every accepted assertion id is remembered until it expires; presenting it again is refused.
  * **Browser binding** (https deployments): a SameSite=None;Secure cookie ties the response to the browser that started the login
    (the ACS is a cross-site POST, so a Lax cookie would never arrive). On plain http (development) the binding is skipped.
  * **No account takeover**: an existing local/LDAP/OIDC username, a deleted or deactivated account is refused, exactly like OIDC.
  * The role comes from the group attribute (`admin_value` / `power_user_value`, else `default_role`); when the assertion carries the group
    attribute, platform groups mapped to SAML values follow it (web/groups.py), and an assertion without it never strips anyone.
Only assertions signed by the configured IdP certificate are accepted (`want_assertions_signed`, on by default).

**Signed AuthnRequests**: this studio auto-generates its own RSA keypair and self-signed certificate the first time one is
needed (`.metadata/saml_sp.key` mode 0600, `.metadata/saml_sp.crt`; stable across restarts, never regenerated unless
`rotate_sp_key()` is called explicitly), so there is nothing for an administrator to procure. `GET /api/auth/saml/metadata`
always advertises it; `sign_authn_requests` (off by default, since most IdPs don't require it) turns on `authnRequestsSigned`
once the certificate is registered with the IdP.

**Encrypted assertions**: decrypting an `<EncryptedAssertion>` with the SP's private key works automatically whenever the
IdP sends one, regardless of any setting (python3-saml decrypts unconditionally when a private key is configured).
`require_encrypted_assertions` only controls whether an *unencrypted* assertion is refused (`wantAssertionsEncrypted`).

**Single Logout (SLO)**, HTTP-Redirect binding only (the SAML profile's own restriction for SLO): `GET /api/auth/saml/logout`
(SP-initiated: builds and signs a LogoutRequest, redirects to `slo_url`, clears the local session immediately regardless of
what the IdP does) and `GET /api/auth/saml/sls` (this studio's SingleLogoutService: an IdP-initiated LogoutRequest is
validated, answered with a signed LogoutResponse and clears the local session too; the return leg of an SP-initiated logout
is a LogoutResponse validated against the LogoutRequest this studio sent). Incoming SLO messages must be signed
(`wantMessagesSigned`, on only for the SLO code path -- see `sp_settings`'s docstring for why it cannot be a blanket
setting: HTTP-Redirect SLO has no assertion-level signature to fall back on, unlike the login response); this studio's
own LogoutRequest/LogoutResponse messages are always signed too. SLO is only offered to the
frontend when `slo_url` is configured; the SLS endpoint itself is always live (an IdP can still send an IdP-initiated
LogoutRequest without this studio ever having sent one).
"""
import hashlib
import logging
import os
import re
import secrets
import time
from hmac import compare_digest
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger("localspark.saml")

STATE_COOKIE = "dkw_saml"
STATE_TTL_SECONDS = 600
_USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9._@+-]{0,127}$")
NAMEID_UNSPECIFIED = "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
_USERNAME_FALLBACK_ATTRS = ("username", "uid", "preferred_username", "sAMAccountName", "email", "mail",
                            "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress")
WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")


class SamlError(Exception):
    """A sign-in problem with a message that is safe to show to the person signing in (details go to the log)."""


def _cfg_str(cfg: Dict[str, Any], key: str, default: str = "") -> str:
    v = cfg.get(key)
    return v.strip() if isinstance(v, str) and v.strip() else default


def is_configured(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("enabled")) and bool(_cfg_str(cfg, "sso_url")) and bool(_cfg_str(cfg, "x509_cert")) and bool(_cfg_str(cfg, "idp_entity_id"))


def _pem(text: str) -> str:
    """The base64 body of a PEM block without its header/footer/whitespace (what python3-saml wants for x509cert and
    privateKey alike): verified directly that python3-saml accepts a stripped body for both."""
    return re.sub(r"-----(BEGIN|END) [A-Z0-9 ]+-----|\s+", "", text or "")


def _sp_key_paths() -> Tuple[str, str]:
    return os.path.join(METADATA_DIR, "saml_sp.key"), os.path.join(METADATA_DIR, "saml_sp.crt")


def _generate_sp_keypair(key_path: str, cert_path: str):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    import datetime as _dt
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "datakilnworks-saml-sp")])
    now = _dt.datetime.now(_dt.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - _dt.timedelta(days=1))
            .not_valid_after(now + _dt.timedelta(days=3650)).sign(key, hashes.SHA256()))
    key_pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    os.makedirs(os.path.dirname(key_path), exist_ok=True)
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(key_pem)
    with open(cert_path, "wb") as f:
        f.write(cert_pem)


def sp_cert_and_key() -> Tuple[str, str]:
    """(certificate body, private key body) of this studio's own SAML signing/decryption identity, generated once on
    first use and reused forever after (a stable identity an IdP administrator can register). Never regenerated
    except by an explicit `rotate_sp_key()` call."""
    key_path, cert_path = _sp_key_paths()
    if not (os.path.exists(key_path) and os.path.exists(cert_path)):
        try:
            _generate_sp_keypair(key_path, cert_path)
        except FileExistsError:
            pass                # another worker/request won the race; fall through and read what it wrote
    with open(cert_path, "r") as f:
        cert = f.read()
    with open(key_path, "r") as f:
        key = f.read()
    return _pem(cert), _pem(key)


def sp_certificate_pem() -> str:
    """The SP's own certificate, PEM-formatted, for an administrator to register with the IdP (never the private key)."""
    cert, _key = sp_cert_and_key()
    return "-----BEGIN CERTIFICATE-----\n" + cert + "\n-----END CERTIFICATE-----\n"


def rotate_sp_key():
    """Discards the current SP signing/decryption keypair; the next use generates a fresh one. An IdP that has the
    old certificate registered will reject signed requests / stop being able to send encrypted assertions until it
    is updated with the new one from `sp_certificate_pem()`."""
    key_path, cert_path = _sp_key_paths()
    for p in (key_path, cert_path):
        try:
            os.remove(p)
        except FileNotFoundError:
            pass


def _idp_certs(text: str) -> Dict[str, Any]:
    """One certificate, or several (an IdP rolling its signing key publishes both): any of them may sign."""
    blocks = re.findall(r"-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----", text or "", re.S)
    bodies = [re.sub(r"\s+", "", b) for b in blocks] or ([_pem(text)] if _pem(text) else [])
    if len(bodies) > 1:
        return {"x509certMulti": {"signing": bodies}}
    return {"x509cert": bodies[0] if bodies else ""}


def _db():
    from web.auth import get_db_connection
    conn = get_db_connection()
    conn.execute("CREATE TABLE IF NOT EXISTS saml_requests (token TEXT PRIMARY KEY, request_id TEXT NOT NULL, browser_hash TEXT, expires INTEGER NOT NULL)")
    conn.execute("CREATE TABLE IF NOT EXISTS saml_assertions (assertion_id TEXT PRIMARY KEY, expires INTEGER NOT NULL)")
    # `kind` ('login' | 'logout'): the same solicited-request table now also tracks SP-initiated LogoutRequests, so an
    # incoming LogoutResponse can be matched back to InResponseTo the same way an AuthnResponse already is.
    if "kind" not in {r[1] for r in conn.execute("PRAGMA table_info(saml_requests)").fetchall()}:
        conn.execute("ALTER TABLE saml_requests ADD COLUMN kind TEXT NOT NULL DEFAULT 'login'")
    conn.execute("DELETE FROM saml_requests WHERE expires < ?", (int(time.time()),))
    conn.execute("DELETE FROM saml_assertions WHERE expires < ?", (int(time.time()),))
    return conn


def sp_settings(cfg: Dict[str, Any], sp_base: str, script_name: str = "/api/auth/saml/acs",
                want_messages_signed: bool = False) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """(python3-saml settings, the request dict describing the endpoint currently processing a message). Both derive from
    `sp_base` (the configured public URL of this studio, else the request's), never from headers the browser controls
    beyond that, so the Destination check is deterministic. `script_name` must be the endpoint actually being processed
    (the ACS for a login response, the SLS for a logout message) since Destination validation compares against it.
    `want_messages_signed` is scoped to SLO on purpose (see `begin_logout`/`complete_logout`): the security flag it maps
    to (`wantMessagesSigned`) is dual-purpose in python3-saml, ALSO requiring the outer `<samlp:Response>` element of an
    ordinary login response to be signed (not just the assertion inside it) -- verified directly: turning it on
    unconditionally broke sign-in against any IdP that only signs the assertion, which is the common case (Okta, Entra,
    Keycloak all default to it). SLO's own HTTP-Redirect messages have no assertion-level signature to fall back on, so
    they need this flag; ordinary login does not, and must never have it forced on by this function."""
    base = (_cfg_str(cfg, "sp_base_url") or sp_base).rstrip("/")
    u = urlparse(base)
    if u.scheme not in ("http", "https") or not u.hostname:
        raise SamlError("The studio's public URL is not valid (set 'SP base URL' in the SAML settings).")
    acs = f"{base}/api/auth/saml/acs"
    sp_cert, sp_key = sp_cert_and_key()
    encrypted = bool(cfg.get("require_encrypted_assertions"))
    idp_slo = _cfg_str(cfg, "slo_url")
    settings = {
        "strict": True, "debug": False,
        "sp": {"entityId": _cfg_str(cfg, "entity_id") or f"{base}/api/auth/saml/metadata",
               "assertionConsumerService": {"url": acs, "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"},
               "singleLogoutService": {"url": f"{base}/api/auth/saml/sls", "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"},
               "NameIDFormat": _cfg_str(cfg, "name_id_format", NAMEID_UNSPECIFIED),
               "x509cert": sp_cert, "privateKey": sp_key},
        "idp": {"entityId": _cfg_str(cfg, "idp_entity_id"),
                "singleSignOnService": {"url": _cfg_str(cfg, "sso_url"), "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"},
                **({"singleLogoutService": {"url": idp_slo, "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"}} if idp_slo else {}),
                **_idp_certs(_cfg_str(cfg, "x509_cert"))},
        "security": {"authnRequestsSigned": bool(cfg.get("sign_authn_requests")), "wantAssertionsSigned": cfg.get("want_assertions_signed", True) is not False,
                     "wantMessagesSigned": want_messages_signed, "wantAssertionsEncrypted": encrypted, "wantNameId": True,
                     "wantNameIdEncrypted": encrypted, "rejectDeprecatedAlgorithm": True, "requestedAuthnContext": False,
                     "logoutRequestSigned": True, "logoutResponseSigned": True,
                     "signatureAlgorithm": "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",
                     "digestAlgorithm": "http://www.w3.org/2001/04/xmlenc#sha256", "allowRepeatAttributeName": True,
                     "allowSingleLabelDomains": True},    # http://localhost:8891 and in-cluster host names are legitimate deployments
    }
    port = u.port
    req = {"https": "on" if u.scheme == "https" else "off", "http_host": u.hostname + (f":{port}" if port else ""),
           "server_port": str(port or (443 if u.scheme == "https" else 80)), "script_name": script_name, "get_data": {}, "post_data": {}}
    return settings, req


def _auth(settings: Dict[str, Any], req: Dict[str, Any]):
    from onelogin.saml2.auth import OneLogin_Saml2_Auth
    try:
        return OneLogin_Saml2_Auth(req, old_settings=settings)
    except Exception as exc:
        logger.warning(f"SAML settings rejected: {exc}")
        raise SamlError("The SAML configuration is incomplete or invalid. Ask an administrator to check it.")


def begin_login(cfg: Dict[str, Any], sp_base: str, https: bool) -> Tuple[str, str]:
    """(URL to send the browser to, browser-binding cookie value)."""
    if not is_configured(cfg):
        raise SamlError("SAML sign-in is not enabled.")
    settings, req = sp_settings(cfg, sp_base)
    auth = _auth(settings, req)
    token = secrets.token_urlsafe(24)
    url = auth.login(return_to=token)
    request_id = auth.get_last_request_id()
    browser = secrets.token_urlsafe(24) if https else ""
    conn = _db()
    try:
        conn.execute("INSERT INTO saml_requests (token, request_id, browser_hash, expires, kind) VALUES (?,?,?,?,'login')",
                     (token, request_id, hashlib.sha256(browser.encode()).hexdigest() if browser else None, int(time.time()) + STATE_TTL_SECONDS))
        conn.commit()
    finally:
        conn.close()
    return url, browser


def begin_logout(cfg: Dict[str, Any], sp_base: str, name_id: str, session_index: Optional[str], name_id_format: Optional[str]) -> str:
    """SP-initiated Single Logout: URL to send the browser to. Raises SamlError if SLO isn't configured (no `slo_url`)
    or the IdP declines to support it. `name_id`/`session_index` come from this session's own login (stored in the
    session token), never guessed, so the LogoutRequest names exactly the session the IdP itself issued."""
    if not is_configured(cfg):
        raise SamlError("SAML sign-in is not enabled.")
    if not _cfg_str(cfg, "slo_url"):
        raise SamlError("Single Logout is not configured for this identity provider.")
    settings, req = sp_settings(cfg, sp_base, script_name="/api/auth/saml/sls", want_messages_signed=True)
    auth = _auth(settings, req)
    token = secrets.token_urlsafe(24)
    try:
        url = auth.logout(return_to=token, name_id=name_id or None, session_index=session_index or None,
                          name_id_format=name_id_format or None)
    except Exception as exc:
        logger.warning(f"SAML logout could not be started: {exc}")
        raise SamlError("Single Logout could not be started.")
    request_id = auth.get_last_request_id()
    conn = _db()
    try:
        conn.execute("INSERT INTO saml_requests (token, request_id, browser_hash, expires, kind) VALUES (?,?,NULL,?,'logout')",
                     (token, request_id, int(time.time()) + STATE_TTL_SECONDS))
        conn.commit()
    finally:
        conn.close()
    return url


def complete_logout(cfg: Dict[str, Any], sp_base: str, query: Dict[str, str]) -> Dict[str, Any]:
    """Processes an incoming SLO message at the SingleLogoutService endpoint (HTTP-Redirect binding only, the SAML
    profile's own restriction): a LogoutRequest (IdP-initiated: validated, answered with a signed LogoutResponse the
    caller must redirect the browser to) or a LogoutResponse (the return leg of an SP-initiated logout this studio
    started: validated against the LogoutRequest it sent). Returns {"redirect_url": str|None, "error": bool}; the
    caller clears the local session cookie regardless (this endpoint is reached only by front-channel browser
    redirects, so the ambient session cookie, if any, belongs to the browser SLO concerns)."""
    if not is_configured(cfg):
        return {"redirect_url": None, "error": True}
    relay_state = query.get("RelayState") or ""
    request_id = None
    if relay_state and len(relay_state) < 200:
        conn = _db()
        try:
            row = conn.execute("SELECT request_id FROM saml_requests WHERE token = ? AND kind = 'logout'", (relay_state,)).fetchone()
            if row:
                conn.execute("DELETE FROM saml_requests WHERE token = ?", (relay_state,))
                conn.commit()
                request_id = row["request_id"]
        finally:
            conn.close()
    settings, req = sp_settings(cfg, sp_base, script_name="/api/auth/saml/sls", want_messages_signed=True)
    req["get_data"] = dict(query)
    auth = _auth(settings, req)
    try:
        url = auth.process_slo(keep_local_session=True, request_id=request_id)
    except Exception as exc:
        logger.warning(f"SAML SLO message could not be processed: {exc}")
        return {"redirect_url": None, "error": True}
    errors = auth.get_errors()
    if errors:
        logger.warning(f"SAML SLO message rejected: {errors} {auth.get_last_error_reason()}")
        return {"redirect_url": None, "error": True}
    return {"redirect_url": url, "error": False}


def _names(attrs: Dict[str, List[str]], *wanted: str) -> List[str]:
    for w in wanted:
        if w and attrs.get(w):
            return [str(v) for v in attrs[w]]
    return []


def _username(cfg: Dict[str, Any], attrs: Dict[str, List[str]], nameid: str) -> str:
    candidates = _names(attrs, _cfg_str(cfg, "attribute_username"), *_USERNAME_FALLBACK_ATTRS) or ([nameid] if nameid else [])
    for c in candidates:
        c = c.strip().lower()
        if _USERNAME_RE.match(c):
            return c
    raise SamlError("The identity provider did not supply a username this studio can use.")


def _map_role(cfg: Dict[str, Any], values: List[str]) -> str:
    have = {v.lower() for v in values}
    admin, power = _cfg_str(cfg, "admin_value").lower(), _cfg_str(cfg, "power_user_value").lower()
    if admin and admin in have:
        return "admin"
    if power and power in have:
        return "power_user"
    default = _cfg_str(cfg, "default_role", "user")
    return default if default in ("admin", "power_user", "user") else "user"


def _provision(cfg: Dict[str, Any], username: str, display_name: str, role: str) -> Dict[str, Any]:
    from web.auth import get_user_by_username, upsert_external_user
    existing = get_user_by_username(username)
    if existing:
        if (existing.get("auth_source") or "local") != "saml":
            raise SamlError("An account with this username already exists and is not managed by this identity provider; "
                            "an administrator must resolve this before it can sign in here.")
        if existing.get("deleted_at"):
            raise SamlError("This account was deleted; an administrator must restore it before it can sign in again.")
        if not existing.get("is_active"):
            raise SamlError("Account is deactivated. Contact an administrator.")
    try:
        return upsert_external_user(username=username, display_name=display_name or username, role=role, auth_source="saml")
    except ValueError as exc:
        raise SamlError(str(exc)) from exc


def complete_login(cfg: Dict[str, Any], sp_base: str, saml_response: str, relay_state: str, browser_cookie: Optional[str], https: bool) -> Dict[str, Any]:
    """Validates the POSTed response and returns the provisioned local user. Raises SamlError with a user-safe message."""
    if not is_configured(cfg):
        raise SamlError("SAML sign-in is not enabled.")
    if not saml_response:
        raise SamlError("The identity provider sent no sign-in response.")
    settings, req = sp_settings(cfg, sp_base)
    request_id = None
    solicited = relay_state and len(relay_state) < 200
    if solicited:
        conn = _db()
        try:
            row = conn.execute("SELECT request_id, browser_hash FROM saml_requests WHERE token = ? AND kind = 'login'", (relay_state,)).fetchone()
            if row:
                conn.execute("DELETE FROM saml_requests WHERE token = ?", (relay_state,))     # single use, whatever the outcome
                conn.commit()
        finally:
            conn.close()
        if row:
            request_id = row["request_id"]
            if row["browser_hash"]:
                got = hashlib.sha256((browser_cookie or "").encode()).hexdigest()
                if not browser_cookie or not compare_digest(got, row["browser_hash"]):
                    raise SamlError("The sign-in response did not come from the browser that started it. Please try again.")
    if request_id is None and not cfg.get("allow_idp_initiated"):
        raise SamlError("The sign-in attempt expired or was not started here. Please start it again from the sign-in page.")
    req["post_data"] = {"SAMLResponse": saml_response}
    auth = _auth(settings, req)
    try:
        auth.process_response(request_id=request_id)
    except Exception as exc:
        logger.warning(f"SAML response could not be processed: {exc}")
        raise SamlError("The identity provider's response could not be verified.")
    errors = auth.get_errors()
    if errors or not auth.is_authenticated():
        logger.warning(f"SAML response rejected: {errors} {auth.get_last_error_reason()}")
        raise SamlError("The identity provider's response could not be verified.")
    # Replay: an assertion is accepted once.
    assertion_id = auth.get_last_assertion_id()
    if not assertion_id:
        raise SamlError("The identity provider's response carried no assertion id.")
    expires = int(auth.get_last_assertion_not_on_or_after() or (time.time() + 600)) + 300
    conn = _db()
    try:
        try:
            conn.execute("INSERT INTO saml_assertions (assertion_id, expires) VALUES (?, ?)", (assertion_id, expires))
            conn.commit()
        except Exception:
            raise SamlError("This sign-in response was already used.")
    finally:
        conn.close()

    attrs = auth.get_attributes() or {}
    username = _username(cfg, attrs, auth.get_nameid() or "")
    group_attr = _cfg_str(cfg, "attribute_groups", "groups")
    group_values = _names(attrs, group_attr)
    display = (_names(attrs, _cfg_str(cfg, "attribute_display_name"), "displayName", "name", "cn", "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name") or [""])[0]
    user = _provision(cfg, username, display, _map_role(cfg, group_values))
    if group_attr in attrs:                       # only when the IdP actually sent the attribute (absent is not "no groups")
        try:
            from web import groups
            groups.sync_external_memberships(user["id"], "saml", group_values)
        except Exception as exc:
            logger.warning(f"SAML group sync for {username} failed: {exc}")
    # Carried in the session token so a later Single Logout names exactly the session the IdP itself issued.
    user["_saml_session"] = {"name_id": auth.get_nameid() or "", "session_index": auth.get_session_index() or "",
                              "name_id_format": auth.get_nameid_format() or ""}
    return user


def metadata_xml(cfg: Dict[str, Any], sp_base: str) -> str:
    settings, req = sp_settings(cfg, sp_base)
    from onelogin.saml2.settings import OneLogin_Saml2_Settings
    s = OneLogin_Saml2_Settings(settings, sp_validation_only=True)
    xml = s.get_sp_metadata()
    errors = s.validate_metadata(xml)
    if errors:
        raise SamlError("The service-provider metadata is not valid: " + ", ".join(errors))
    return xml.decode("utf-8") if isinstance(xml, bytes) else xml


def import_idp_metadata(url: str = "", xml: str = "") -> Dict[str, str]:
    """Reads an IdP's entity id, SSO URL, SLO URL (Redirect binding) and signing certificate from its metadata (for the admin's form)."""
    from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser
    try:
        if xml.strip():
            data = OneLogin_Saml2_IdPMetadataParser.parse(xml.strip())
        else:
            u = urlparse(url.strip())
            if u.scheme not in ("http", "https") or not u.hostname:
                raise SamlError("The metadata URL must be http(s).")
            data = OneLogin_Saml2_IdPMetadataParser.parse_remote(url.strip(), timeout=10)
    except SamlError:
        raise
    except Exception as exc:
        raise SamlError(f"The IdP metadata could not be read ({type(exc).__name__}).")
    idp = (data or {}).get("idp") or {}
    if not idp.get("entityId") or not (idp.get("singleSignOnService") or {}).get("url"):
        raise SamlError("The metadata has no entity id or single sign-on service.")
    cert = idp.get("x509cert") or (idp.get("x509certMulti") or {}).get("signing", [""])[0]
    return {"idp_entity_id": idp["entityId"], "sso_url": idp["singleSignOnService"]["url"],
            "slo_url": (idp.get("singleLogoutService") or {}).get("url", ""),
            "x509_cert": "-----BEGIN CERTIFICATE-----\n" + cert + "\n-----END CERTIFICATE-----" if cert else ""}
