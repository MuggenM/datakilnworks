"""OAuth 2.0 client-credentials grant (RFC 6749 section 4.4) for `http` connections: machine-to-machine access to REST APIs (Entra ID app registrations,
Okta / Auth0 / Keycloak service clients, cloud vendor APIs).

The connection stores the token endpoint, the client id, optional scope and extra token parameters, and the client SECRET (encrypted like every
connection secret). Access tokens are fetched here, kept in memory only (never written to disk, never returned by an API, never logged) and refreshed a
little before they expire; a server that answers 401 to a token it just issued or that was revoked triggers ONE re-fetch and retry (see
`autoloader_conn._http`). The client secret goes to the token endpoint only, the access token only to the connection's base URL (the origin check of
`_http` still applies), redirects from the token endpoint are refused, responses are size-limited and validated (a token with whitespace or control
characters would allow header injection), and no message contains the secret or a token.
"""
import hashlib
import json
import re
import threading
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

MAX_RESPONSE = 64 * 1024
MAX_TOKEN_LEN = 8192
DEFAULT_LIFETIME = 300               # seconds, when the server does not say
RESERVED = {"grant_type", "client_id", "client_secret", "scope"}
_TOKEN_RE = re.compile(r"^[A-Za-z0-9\-._~+/=]+$")           # RFC 6750 b64token (JWTs and opaque tokens fit)

_cache: Dict[str, Dict[str, Any]] = {}
_lock = threading.Lock()


class OAuthError(Exception):
    """The token could not be obtained; the message is safe to show (no secret, no token)."""


def _key(conn: Dict[str, Any]) -> str:
    cfg, secret = conn["config"], conn.get("secret") or {}
    raw = json.dumps([cfg.get("token_url"), cfg.get("client_id"), cfg.get("scope"), cfg.get("client_auth"), cfg.get("extra_params"), secret.get("client_secret", "")], sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()               # a new secret or scope is a new entry; the secret itself is never a key


def invalidate(conn: Dict[str, Any]) -> None:
    with _lock:
        _cache.pop(_key(conn), None)


def clear() -> None:
    with _lock:
        _cache.clear()


def _describe(r) -> str:
    """The token endpoint's own error (RFC 6749 5.2: error and error_description), shortened; nothing else of the body."""
    try:
        j = r.json()
        code = str(j.get("error") or "")[:60]
        desc = str(j.get("error_description") or "").split("\n")[0][:140]
        return f"{code}{': ' + desc if desc else ''}".strip()
    except Exception:
        return ""


def _fetch(conn: Dict[str, Any]) -> Dict[str, Any]:
    import requests
    cfg, secret = conn["config"], conn.get("secret") or {}
    csecret = secret.get("client_secret", "")
    if not csecret:
        raise OAuthError("The client secret is missing from this connection.")
    form = {"grant_type": "client_credentials"}
    if cfg.get("scope"):
        form["scope"] = cfg["scope"]
    for k, v in (cfg.get("extra_params") or {}).items():
        if k not in RESERVED:
            form[k] = v
    auth = None
    if cfg.get("client_auth") == "body":
        form.update(client_id=cfg["client_id"], client_secret=csecret)
    else:
        from requests.auth import HTTPBasicAuth
        from urllib.parse import quote
        auth = HTTPBasicAuth(quote(cfg["client_id"], safe=""), quote(csecret, safe=""))          # RFC 6749 2.3.1: the credentials are form-urlencoded first
    try:
        r = requests.post(cfg["token_url"], data=form, auth=auth, timeout=(min(10, cfg.get("timeout_seconds", 30)), cfg.get("timeout_seconds", 30)),
                          allow_redirects=False, headers={"Accept": "application/json", "User-Agent": "DataKilnWorks-AutoLoader/1"}, stream=True)
    except requests.exceptions.SSLError:
        raise OAuthError("TLS to the token endpoint failed (certificate check).")
    except requests.RequestException as exc:
        raise OAuthError(f"The token endpoint could not be reached ({type(exc).__name__}).")
    try:
        body = r.raw.read(MAX_RESPONSE + 1, decode_content=True) or b""
        r._content = body[:MAX_RESPONSE]
        if r.is_redirect or r.is_permanent_redirect:
            raise OAuthError("The token endpoint redirected the request; the redirect was not followed (the client secret must not leave the configured URL).")
        if r.status_code >= 400:
            why = _describe(r)
            raise OAuthError(f"The token endpoint refused the client credentials (HTTP {r.status_code}{': ' + why if why else ''}).")
        if len(body) > MAX_RESPONSE:
            raise OAuthError("The token endpoint's answer is too large.")
        try:
            j = json.loads(body.decode("utf-8"))
        except Exception:
            raise OAuthError("The token endpoint did not answer with JSON.")
    finally:
        r.close()
    tok = j.get("access_token") if isinstance(j, dict) else None
    if not isinstance(tok, str) or not tok:
        raise OAuthError("The token endpoint's answer has no access_token.")
    if len(tok) > MAX_TOKEN_LEN or not _TOKEN_RE.match(tok):
        raise OAuthError("The access token has an unexpected format (it must be a plain token of letters, digits and - . _ ~ + / =).")
    ttype = str(j.get("token_type") or "Bearer")
    if ttype.lower() != "bearer":
        raise OAuthError(f"The token type '{ttype[:20]}' is not supported (Bearer only).")
    try:
        lifetime = int(j.get("expires_in")) if j.get("expires_in") is not None else DEFAULT_LIFETIME
    except (TypeError, ValueError):
        lifetime = DEFAULT_LIFETIME
    lifetime = max(1, min(lifetime, 24 * 3600))
    skew = min(60.0, lifetime / 2)                                   # refresh a little early
    return {"token": tok, "type": "Bearer", "expires_at": time.time() + lifetime - skew, "lifetime": lifetime}


def get_token(conn: Dict[str, Any], force: bool = False) -> Tuple[str, str]:
    """(access token, type) for a connection with auth 'oauth2': from the in-memory cache, or freshly fetched."""
    k = _key(conn)
    with _lock:
        e = _cache.get(k)
        if e and not force and e["expires_at"] > time.time():
            return e["token"], e["type"]
        e = _fetch(conn)                                             # under the lock: concurrent callers share one fetch
        _cache[k] = e
        return e["token"], e["type"]


def test(conn: Dict[str, Any]) -> Dict[str, Any]:
    """For the Test button: fetches a fresh token and reports how long it lives (never the token)."""
    e = _fetch(conn)
    with _lock:
        _cache[_key(conn)] = e
    return {"ok": True, "lifetime": e["lifetime"]}
