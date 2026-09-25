"""
Real OpenID Connect login (authorization-code flow with PKCE) and provisioning of the local `users` row
(`auth_source='oidc'`) for accounts that sign in through an external identity provider.

`web/auth_frameworks.py` only stores the configuration and does a bare discovery-document fetch
(`test_oidc_discovery`); this module is what actually signs someone in. The routes live in `web/app.py`
(`/api/auth/oidc/login`, `/api/auth/oidc/callback`).

Flow:
  1. `begin_login`: fetch (and cache) the discovery document, check its `issuer` matches the configured issuer,
     generate `state`, `nonce` and a PKCE verifier, and return the authorization URL plus a short-lived *signed*
     cookie value carrying those three secrets. The cookie is the only server-side memory of the attempt, so
     nothing is held in process memory (survives `--reload` and more than one worker).
  2. The browser returns to the callback with `code` + `state`. `complete_login` refuses unless the state matches
     the cookie, exchanges the code for tokens (with the PKCE verifier and, if configured, the client secret),
     and validates the ID token: signature against the provider's JWKS with an explicit asymmetric-algorithm
     allow-list (never `none`/HS*), `iss`, `aud`/`azp`, `exp`/`iat`, and the `nonce` from step 1.
  3. Username and role come from claims (`username_claim`; `admin_claim` with `admin_value`/`power_user_value`,
     falling back to the userinfo endpoint when the claim is not in the ID token). The local account is created
     or updated through `web.auth.upsert_external_user`.

Account-takeover rules, stricter than LDAP's because there is no bind to prove anything: an existing username
that is local, belongs to another provider (ldap), was deleted, or was deactivated by an administrator is always
refused -- an OIDC login can never take over, or reactivate, an account it did not create.
"""

import logging
import re
import secrets
import time
from base64 import urlsafe_b64encode
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from hmac import compare_digest
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import jwt
import requests

logger = logging.getLogger("localspark.oidc")

STATE_COOKIE = "dkw_oidc"
STATE_TTL_SECONDS = 600
ALLOWED_ALGS = ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512"]
_USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9._@+-]{0,127}$")
_HTTP_TIMEOUT = 5

_discovery_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_jwk_clients: Dict[str, "jwt.PyJWKClient"] = {}


class OidcError(Exception):
    """A failed sign-in. The message is safe to show the user; details go to the log."""


def _cfg_str(cfg: Dict[str, Any], key: str, default: str = "") -> str:
    v = cfg.get(key)
    return v.strip() if isinstance(v, str) and v.strip() else default


def is_configured(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("enabled")) and bool(_cfg_str(cfg, "issuer_url")) and bool(_cfg_str(cfg, "client_id"))


def discover(cfg: Dict[str, Any]) -> Dict[str, Any]:
    issuer = _cfg_str(cfg, "issuer_url").rstrip("/")
    cached = _discovery_cache.get(issuer)
    if cached and time.time() - cached[0] < 600:
        return cached[1]
    try:
        resp = requests.get(f"{issuer}/.well-known/openid-configuration", timeout=_HTTP_TIMEOUT)
        resp.raise_for_status()
        doc = resp.json()
    except Exception as exc:
        logger.warning(f"OIDC discovery failed for {issuer}: {exc}")
        raise OidcError("The identity provider is not reachable right now.") from exc
    # Mix-up protection (RFC 8414 §3.3 / OIDC Discovery §4.3): the document must describe the issuer we asked for.
    if str(doc.get("issuer", "")).rstrip("/") != issuer:
        logger.warning(f"OIDC discovery issuer mismatch: configured {issuer!r}, document says {doc.get('issuer')!r}")
        raise OidcError("The identity provider's discovery document does not match the configured issuer.")
    for key in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
        if not doc.get(key):
            raise OidcError(f"The identity provider's discovery document has no {key}.")
    _discovery_cache[issuer] = (time.time(), doc)
    return doc


def _signing_secret() -> str:
    from web.auth import JWT_SECRET_KEY
    return JWT_SECRET_KEY


def begin_login(cfg: Dict[str, Any], redirect_uri: str) -> Tuple[str, str]:
    """Returns (authorization_url, signed_state_cookie_value)."""
    if not is_configured(cfg):
        raise OidcError("OpenID Connect sign-in is not enabled.")
    doc = discover(cfg)
    state, nonce, verifier = secrets.token_urlsafe(24), secrets.token_urlsafe(24), secrets.token_urlsafe(48)
    challenge = urlsafe_b64encode(sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    params = {
        "response_type": "code",
        "client_id": _cfg_str(cfg, "client_id"),
        "redirect_uri": redirect_uri,
        "scope": _cfg_str(cfg, "scopes", "openid email profile"),
        "state": state,
        "nonce": nonce,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    sep = "&" if "?" in doc["authorization_endpoint"] else "?"
    cookie = jwt.encode({"st": state, "nn": nonce, "cv": verifier, "ru": redirect_uri, "purpose": "oidc-state",
                         "exp": datetime.now(timezone.utc) + timedelta(seconds=STATE_TTL_SECONDS)},
                        _signing_secret(), algorithm="HS256")
    return doc["authorization_endpoint"] + sep + urlencode(params), cookie


def _exchange_code(cfg: Dict[str, Any], doc: Dict[str, Any], code: str, verifier: str, redirect_uri: str) -> Dict[str, Any]:
    client_id, secret = _cfg_str(cfg, "client_id"), cfg.get("client_secret") or ""
    data = {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri, "code_verifier": verifier}
    auth = None
    if secret:
        methods = doc.get("token_endpoint_auth_methods_supported") or ["client_secret_basic"]
        if "client_secret_basic" in methods:
            auth = (client_id, secret)
        else:
            data.update(client_id=client_id, client_secret=secret)
    else:
        data["client_id"] = client_id                       # public client: PKCE alone protects the code
    try:
        resp = requests.post(doc["token_endpoint"], data=data, auth=auth, timeout=_HTTP_TIMEOUT,
                             headers={"Accept": "application/json"})
        body = resp.json()
    except Exception as exc:
        logger.warning(f"OIDC token request failed: {exc}")
        raise OidcError("The identity provider did not answer the token request.") from exc
    if resp.status_code != 200 or "id_token" not in body:
        logger.warning(f"OIDC token endpoint refused the code: HTTP {resp.status_code} {str(body)[:300]}")
        raise OidcError("The identity provider rejected the sign-in.")
    return body


def _validate_id_token(cfg: Dict[str, Any], doc: Dict[str, Any], id_token: str, nonce: str) -> Dict[str, Any]:
    client_id = _cfg_str(cfg, "client_id")
    try:
        alg = jwt.get_unverified_header(id_token).get("alg")
        if alg not in ALLOWED_ALGS:
            raise OidcError("The identity provider signed the token with an unsupported algorithm.")
        client = _jwk_clients.setdefault(doc["jwks_uri"], jwt.PyJWKClient(doc["jwks_uri"], timeout=_HTTP_TIMEOUT))
        key = client.get_signing_key_from_jwt(id_token).key
        claims = jwt.decode(id_token, key, algorithms=ALLOWED_ALGS, audience=client_id, issuer=doc["issuer"],
                            leeway=60, options={"require": ["exp", "iat", "iss", "aud", "sub"]})
    except OidcError:
        raise
    except Exception as exc:
        logger.warning(f"OIDC ID token rejected: {exc}")
        raise OidcError("The identity provider's token could not be verified.") from exc
    if isinstance(claims.get("aud"), list) and len(claims["aud"]) > 1 and claims.get("azp") != client_id:
        raise OidcError("The identity provider's token was issued to a different client.")
    if not compare_digest(str(claims.get("nonce", "")), nonce):
        raise OidcError("The sign-in response did not match this browser session (nonce).")
    return claims


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value]
    return [str(value)]


def _map_role(cfg: Dict[str, Any], claims: Dict[str, Any]) -> str:
    claim = _cfg_str(cfg, "admin_claim", "groups")
    values = {v.lower() for v in _as_list(claims.get(claim))}
    admin_value, power_value = _cfg_str(cfg, "admin_value").lower(), _cfg_str(cfg, "power_user_value").lower()
    if admin_value and admin_value in values:
        return "admin"
    if power_value and power_value in values:
        return "power_user"
    default = _cfg_str(cfg, "default_role", "user")
    return default if default in ("admin", "power_user", "user") else "user"


def _username(cfg: Dict[str, Any], claims: Dict[str, Any]) -> str:
    wanted = _cfg_str(cfg, "username_claim", "preferred_username")
    for claim in (wanted, "email"):
        value = claims.get(claim)
        if not isinstance(value, str) or not value.strip():
            continue
        if claim == "email" and claims.get("email_verified") is False:
            continue                                        # an unverified address proves nothing about who this is
        candidate = value.strip().lower()
        if _USERNAME_RE.match(candidate):
            return candidate
        raise OidcError("Your identity provider account has a username this studio cannot use.")
    raise OidcError("The identity provider did not supply a usable username.")


def _provision(cfg: Dict[str, Any], claims: Dict[str, Any]) -> Dict[str, Any]:
    from web.auth import get_user_by_username, upsert_external_user
    username = _username(cfg, claims)
    existing = get_user_by_username(username)
    if existing:
        source = existing.get("auth_source") or "local"
        if source != "oidc":
            raise OidcError("An account with this username already exists and is not managed by this identity provider; "
                            "an administrator must resolve this before it can sign in here.")
        if existing.get("deleted_at"):
            raise OidcError("This account was deleted; an administrator must restore it before it can sign in again.")
        if not existing.get("is_active"):
            raise OidcError("Account is deactivated. Contact an administrator.")
    name = claims.get("name") if isinstance(claims.get("name"), str) else ""
    try:
        return upsert_external_user(username=username, display_name=name or username, role=_map_role(cfg, claims),
                                    auth_source="oidc")
    except ValueError as exc:
        raise OidcError(str(exc)) from exc


def complete_login(cfg: Dict[str, Any], code: str, state: str, state_cookie: Optional[str],
                   redirect_uri: str) -> Dict[str, Any]:
    """Validates the callback and returns the provisioned local user. Raises OidcError on any failure."""
    if not is_configured(cfg):
        raise OidcError("OpenID Connect sign-in is not enabled.")
    if not state_cookie or not code or not state:
        raise OidcError("The sign-in attempt expired or was not started from this browser. Please try again.")
    try:
        saved = jwt.decode(state_cookie, _signing_secret(), algorithms=["HS256"], options={"require": ["exp"]})
    except Exception:
        raise OidcError("The sign-in attempt expired or was not started from this browser. Please try again.")
    if saved.get("purpose") != "oidc-state" or not compare_digest(str(saved.get("st", "")), state):
        raise OidcError("The sign-in response did not match this browser session (state).")
    if saved.get("ru") != redirect_uri:
        raise OidcError("The sign-in response did not match this browser session.")
    doc = discover(cfg)
    tokens = _exchange_code(cfg, doc, code, saved["cv"], redirect_uri)
    claims = _validate_id_token(cfg, doc, tokens["id_token"], saved["nn"])
    claim_name = _cfg_str(cfg, "admin_claim", "groups")
    if claim_name not in claims and tokens.get("access_token") and doc.get("userinfo_endpoint"):
        try:
            info = requests.get(doc["userinfo_endpoint"], timeout=_HTTP_TIMEOUT,
                                headers={"Authorization": f"Bearer {tokens['access_token']}"}).json()
            if info.get("sub") == claims["sub"]:            # OIDC Core §5.3.2: never trust userinfo for another subject
                for k, v in info.items():
                    claims.setdefault(k, v)
        except Exception as exc:
            logger.warning(f"OIDC userinfo request failed (continuing without it): {exc}")
    user = _provision(cfg, claims)
    # Group sync: only when the identity provider actually sent the groups claim (an absent claim is not "no groups": it would strip access).
    if claim_name in claims:
        try:
            from web import groups
            groups.sync_external_memberships(user["id"], "oidc", _as_list(claims.get(claim_name)))
        except Exception as exc:
            logger.warning(f"OIDC group sync for {user.get('username')} failed: {exc}")
    return user
