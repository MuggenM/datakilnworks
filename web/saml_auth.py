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
Only assertions signed by the configured IdP certificate are accepted (`want_assertions_signed`, on by default). AuthnRequests are not signed and
assertions are not encrypted in this version (most IdPs do not require either).
"""
import hashlib
import logging
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


class SamlError(Exception):
    """A sign-in problem with a message that is safe to show to the person signing in (details go to the log)."""


def _cfg_str(cfg: Dict[str, Any], key: str, default: str = "") -> str:
    v = cfg.get(key)
    return v.strip() if isinstance(v, str) and v.strip() else default


def is_configured(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("enabled")) and bool(_cfg_str(cfg, "sso_url")) and bool(_cfg_str(cfg, "x509_cert")) and bool(_cfg_str(cfg, "idp_entity_id"))


def _pem(cert: str) -> str:
    """The certificate body without header/footer/whitespace (what python3-saml wants)."""
    return re.sub(r"-----(BEGIN|END) CERTIFICATE-----|\s+", "", cert or "")


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
    conn.execute("DELETE FROM saml_requests WHERE expires < ?", (int(time.time()),))
    conn.execute("DELETE FROM saml_assertions WHERE expires < ?", (int(time.time()),))
    return conn


def sp_settings(cfg: Dict[str, Any], sp_base: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """(python3-saml settings, the request dict describing the ACS URL). Both derive from `sp_base` (the configured public URL of this
    studio, else the request's), never from headers the browser controls beyond that, so the Destination check is deterministic."""
    base = (_cfg_str(cfg, "sp_base_url") or sp_base).rstrip("/")
    u = urlparse(base)
    if u.scheme not in ("http", "https") or not u.hostname:
        raise SamlError("The studio's public URL is not valid (set 'SP base URL' in the SAML settings).")
    acs = f"{base}/api/auth/saml/acs"
    settings = {
        "strict": True, "debug": False,
        "sp": {"entityId": _cfg_str(cfg, "entity_id") or f"{base}/api/auth/saml/metadata",
               "assertionConsumerService": {"url": acs, "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"},
               "NameIDFormat": _cfg_str(cfg, "name_id_format", NAMEID_UNSPECIFIED)},
        "idp": {"entityId": _cfg_str(cfg, "idp_entity_id"),
                "singleSignOnService": {"url": _cfg_str(cfg, "sso_url"), "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"},
                **_idp_certs(_cfg_str(cfg, "x509_cert"))},
        "security": {"authnRequestsSigned": False, "wantAssertionsSigned": cfg.get("want_assertions_signed", True) is not False,
                     "wantMessagesSigned": False, "wantAssertionsEncrypted": False, "wantNameId": True,
                     "wantNameIdEncrypted": False, "rejectDeprecatedAlgorithm": True, "requestedAuthnContext": False,
                     "signatureAlgorithm": "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",
                     "digestAlgorithm": "http://www.w3.org/2001/04/xmlenc#sha256", "allowRepeatAttributeName": True,
                     "allowSingleLabelDomains": True},    # http://localhost:8891 and in-cluster host names are legitimate deployments
    }
    port = u.port
    req = {"https": "on" if u.scheme == "https" else "off", "http_host": u.hostname + (f":{port}" if port else ""),
           "server_port": str(port or (443 if u.scheme == "https" else 80)), "script_name": "/api/auth/saml/acs", "get_data": {}, "post_data": {}}
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
        conn.execute("INSERT INTO saml_requests (token, request_id, browser_hash, expires) VALUES (?,?,?,?)",
                     (token, request_id, hashlib.sha256(browser.encode()).hexdigest() if browser else None, int(time.time()) + STATE_TTL_SECONDS))
        conn.commit()
    finally:
        conn.close()
    return url, browser


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
            row = conn.execute("SELECT request_id, browser_hash FROM saml_requests WHERE token = ?", (relay_state,)).fetchone()
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
    """Reads an IdP's entity id, SSO URL (Redirect binding) and signing certificate from its metadata (for the admin's form)."""
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
            "x509_cert": "-----BEGIN CERTIFICATE-----\n" + cert + "\n-----END CERTIFICATE-----" if cert else ""}
