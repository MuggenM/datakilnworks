"""
Studio side of the notebook sandbox (see sandbox/worker.py).

The sandbox is a separate container that runs the kernels of users a masking policy applies to. It has no mount of the
warehouse, so the only way those kernels see data is the governed endpoint /api/sandbox/sql, called with a short-lived
token bound to one user. This module holds what the studio needs to use it:

    call(...)                  authenticated request to the sandbox worker (worker token, never given to kernels)
    mint_kernel_token(user)    the token a kernel presents to /api/sandbox/sql
    verify_kernel_token(tok)   -> username, or raises
    is_sandbox_peer(ip)        True for requests coming from the sandbox container (they may only use /api/sandbox/*)

SANDBOX_URL            worker address (default http://notebook-sandbox:8000); empty disables the sandbox
SANDBOX_TOKEN          worker token; when unset it is read from SANDBOX_TOKEN_FILE (default /run/sandbox/token), which
                       the worker generates on a volume shared read-only with the studio
SANDBOX_GATEWAY_URL    how kernels reach the studio (default http://datakilnworks-studio:8000)
SANDBOX_TOKEN_TTL      kernel token lifetime in seconds (default 900; renewed on every cell execution)
"""

import datetime
import os
import socket
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import jwt

from web import secrets_store

DEFAULT_URL = "http://notebook-sandbox:8000"
AUDIENCE = "dkw-sandbox-gateway"


def sandbox_url() -> str:
    return os.getenv("SANDBOX_URL", DEFAULT_URL).strip().rstrip("/")


def gateway_url() -> str:
    return os.getenv("SANDBOX_GATEWAY_URL", "http://datakilnworks-studio:8000").strip().rstrip("/")


def worker_token() -> Optional[str]:
    token = os.getenv("SANDBOX_TOKEN", "").strip()
    if token:
        return token
    try:
        with open(os.getenv("SANDBOX_TOKEN_FILE", "/run/sandbox/token")) as f:
            return f.read().strip() or None
    except OSError:
        return None


def configured() -> bool:
    return bool(sandbox_url()) and bool(worker_token())


_health = {"at": 0.0, "ok": False}


def available() -> bool:
    """Configured and the worker answers (cached for a few seconds so status endpoints stay cheap)."""
    if not configured():
        return False
    if time.time() - _health["at"] < 5:
        return _health["ok"]
    try:
        import httpx
        r = httpx.get(f"{sandbox_url()}/health", timeout=2.0)
        _health["ok"] = r.status_code == 200
    except Exception:
        _health["ok"] = False
    _health["at"] = time.time()
    return _health["ok"]


def call(method: str, path: str, body: Optional[Dict[str, Any]] = None, timeout: float = 30.0) -> Dict[str, Any]:
    import httpx
    token = worker_token()
    if not sandbox_url() or not token:
        raise RuntimeError("The notebook sandbox is not configured (no worker token). Start the notebook-sandbox service.")
    try:
        resp = httpx.request(method, f"{sandbox_url()}{path}", json=body, timeout=timeout,
                             headers={"Authorization": f"Bearer {token}"})
    except httpx.HTTPError as exc:
        raise RuntimeError(f"The notebook sandbox is not reachable: {exc}") from exc
    if resp.status_code >= 400:
        try:
            detail = resp.json().get("detail", resp.text)
        except Exception:
            detail = resp.text
        raise RuntimeError(f"Notebook sandbox error ({resp.status_code}): {detail}")
    return resp.json()


# ---------------------------------------------------------------------------------------------- kernel tokens

def _key() -> str:
    return secrets_store.load_or_create_secret("sandbox_gateway_key")


def token_ttl() -> int:
    try:
        return max(60, int(os.getenv("SANDBOX_TOKEN_TTL", "900")))
    except ValueError:
        return 900


def mint_kernel_token(username: str) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    return jwt.encode({"sub": username, "aud": AUDIENCE, "iat": now, "exp": now + datetime.timedelta(seconds=token_ttl())},
                      _key(), algorithm="HS256")


def verify_kernel_token(token: str) -> str:
    """Username the token was minted for; raises jwt.PyJWTError when it is forged, expired or for another audience."""
    claims = jwt.decode(token, _key(), algorithms=["HS256"], audience=AUDIENCE, options={"require": ["exp", "sub", "aud"]})
    return str(claims["sub"])


# ---------------------------------------------------------------------------------------- network isolation

_peers = {"at": 0.0, "ips": frozenset()}
_peers_lock = threading.Lock()


def _sandbox_ips() -> frozenset:
    if time.time() - _peers["at"] < 30:
        return _peers["ips"]
    with _peers_lock:
        host = urlparse(sandbox_url()).hostname if sandbox_url() else None
        ips = set()
        if host:
            try:
                ips = {info[4][0] for info in socket.getaddrinfo(host, None)}
            except OSError:
                ips = set()
        _peers.update(at=time.time(), ips=frozenset(ips))
    return _peers["ips"]


def is_sandbox_peer(client_ip: Optional[str]) -> bool:
    """
    True when the request comes from the sandbox container. Requests without credentials are the local admin in the
    default single-user mode, so everything a kernel could reach on the studio besides /api/sandbox/* is refused.
    """
    if not client_ip or not sandbox_url():
        return False
    return client_ip in _sandbox_ips()
