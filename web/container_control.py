"""
Studio side of the container controller (see controller/controller.py): the studio never holds the Docker socket, it asks the
controller to start / stop / pause / unpause the compute-node containers behind SQL warehouses.

CONTROLLER_URL       default http://container-controller:8000; empty disables container control
CONTROLLER_TOKEN     the token; when unset it is read from CONTROLLER_TOKEN_FILE (default /run/controller/token, written by the
                     controller on a volume the studio mounts read-only)
WAREHOUSE_SUSPEND_MODE   `stop` (default: frees the container's memory; resume takes a few seconds) or `pause` (freezes it:
                     memory stays allocated, resume is instant)
"""

import os
import threading
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx

DEFAULT_URL = "http://container-controller:8000"


class ControllerError(Exception):
    """The controller could not do what was asked (unreachable, refused, or Docker failed); the message is user-safe."""


def controller_url() -> str:
    return os.getenv("CONTROLLER_URL", DEFAULT_URL).strip().rstrip("/")


def token() -> Optional[str]:
    t = os.getenv("CONTROLLER_TOKEN", "").strip()
    if t:
        return t
    try:
        with open(os.getenv("CONTROLLER_TOKEN_FILE", "/run/controller/token")) as f:
            return f.read().strip() or None
    except OSError:
        return None


def suspend_mode() -> str:
    return "pause" if os.getenv("WAREHOUSE_SUSPEND_MODE", "stop").strip().lower() == "pause" else "stop"


def configured() -> bool:
    return bool(controller_url()) and bool(token())


def _request(method: str, path: str, http_timeout: float = 45.0, **params) -> Dict[str, Any]:
    if not configured():
        raise ControllerError("Container control is not configured (no container-controller service or token).")
    try:
        r = httpx.request(method, f"{controller_url()}{path}", headers={"X-Controller-Token": token()}, params=params or None, timeout=http_timeout)
    except httpx.HTTPError as exc:
        raise ControllerError(f"The container controller is not reachable: {exc}") from exc
    if r.status_code >= 400:
        try:
            detail = r.json().get("detail", r.text)
        except Exception:
            detail = r.text
        raise ControllerError(f"The container controller refused: {str(detail)[:200]}")
    return r.json()


_cache: Dict[str, Any] = {"at": 0.0, "data": None}
_cache_lock = threading.Lock()


def list_containers(max_age: float = 5.0) -> List[Dict[str, Any]]:
    """State of every service the controller may manage (cached briefly so status endpoints stay cheap)."""
    with _cache_lock:
        if _cache["data"] is not None and time.time() - _cache["at"] < max_age:
            return _cache["data"]
    data = _request("GET", "/containers", http_timeout=8.0)["containers"]
    with _cache_lock:
        _cache["at"], _cache["data"] = time.time(), data
    return data


def available() -> bool:
    try:
        list_containers()
        return True
    except ControllerError:
        return False


def invalidate():
    with _cache_lock:
        _cache["at"], _cache["data"] = 0.0, None


def service_for_endpoint(endpoint: Optional[str]) -> Optional[str]:
    """The compose service behind a worker endpoint (http://compute-node-01:8001 -> compute-node-01), if the controller manages it."""
    host = urlparse(endpoint or "").hostname
    if not host:
        return None
    try:
        return host if any(c["service"] == host and c.get("state") != "missing" for c in list_containers()) else None
    except ControllerError:
        return None


def status(service: str) -> Dict[str, Any]:
    return _request("GET", f"/containers/{service}", http_timeout=8.0)


def act(service: str, action: str) -> Dict[str, Any]:
    invalidate()
    out = _request("POST", f"/containers/{service}/{action}", http_timeout=45.0, **({"timeout": 10} if action == "stop" else {}))
    invalidate()
    return out


def wait_healthy(endpoint: str, timeout: float = 60.0, interval: float = 0.5) -> bool:
    """Polls the worker's /health until it answers 200 (a started container needs a few seconds to import DuckDB)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"{endpoint.rstrip('/')}/health", timeout=2.0).status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(interval)
    return False
