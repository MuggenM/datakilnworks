"""
Container controller: the only component that holds the Docker socket, and it exposes almost nothing of it.

The studio can suspend idle compute-node containers and resume them on demand (see web/warehouse_lifecycle.py), but it
must not itself hold the Docker socket: that is root on the host, and the studio runs users' notebooks and SQL. This tiny
service (`container-controller` in docker-compose.yml) is that trust boundary. It has no shell, no image / exec / create /
remove / network calls and never touches anything but the allow-listed services:

    GET   /health                              public
    GET   /containers                          state of every allowed service
    GET   /containers/{service}
    POST  /containers/{service}/start|stop|pause|unpause      (stop takes ?timeout=1..30)

Rules, all enforced here rather than trusted from the caller:
  * every route except /health needs the controller token (X-Controller-Token), generated on first start on a volume the
    studio mounts read-only;
  * a `{service}` must be in CONTROLLER_ALLOWED_SERVICES (default the three compute nodes), and the container must belong
    to the same compose project as this controller and be the only replica; anything else is a 404, so it is not even
    possible to learn what else runs on the host;
  * start/stop/pause/unpause are idempotent (already in that state = success).
Reachable only on the internal `control-net`, which only the studio shares.
"""

import hmac
import json
import logging
import os
import re
import secrets
import time
from typing import Any, Dict, List, Optional

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

logger = logging.getLogger("controller")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

SOCKET = os.getenv("DOCKER_SOCKET", "/var/run/docker.sock")
TOKEN_FILE = os.getenv("CONTROLLER_TOKEN_FILE", "/run/controller/token")
ALLOWED = [s.strip() for s in os.getenv("CONTROLLER_ALLOWED_SERVICES", "compute-node-01,compute-node-02,compute-node-03").split(",") if s.strip()]
SERVICE_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$")
PROJECT_LABEL, SERVICE_LABEL = "com.docker.compose.project", "com.docker.compose.service"
ACTIONS = ("start", "stop", "pause", "unpause")

app = FastAPI(title="Data Kiln Works container controller", docs_url=None, redoc_url=None, openapi_url=None)
_project: Optional[str] = None


@app.exception_handler(httpx.HTTPError)
def docker_unreachable(_request, exc: httpx.HTTPError):
    """Most often a wrong socket path (rootless Docker keeps it at $XDG_RUNTIME_DIR/docker.sock, see DOCKER_SOCKET_PATH)."""
    logger.error(f"Cannot talk to the Docker socket {SOCKET}: {exc}")
    return JSONResponse(status_code=503, content={"detail": "The controller cannot reach the Docker socket (check DOCKER_SOCKET_PATH in .env)."})


def _load_token() -> str:
    env = os.getenv("CONTROLLER_TOKEN", "").strip()
    if env:
        return env
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    try:
        fd = os.open(TOKEN_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)     # the studio (another container) reads it
        with os.fdopen(fd, "w") as f:
            f.write(secrets.token_urlsafe(32))
    except FileExistsError:
        pass
    for _ in range(50):
        with open(TOKEN_FILE) as f:
            value = f.read().strip()
        if value:
            return value
        time.sleep(0.02)
    raise RuntimeError("controller token file is empty")


TOKEN = _load_token()


def _docker() -> httpx.Client:
    return httpx.Client(transport=httpx.HTTPTransport(uds=SOCKET), base_url="http://docker", timeout=40.0)


def require_token(x_controller_token: str = Header(default="")):
    if not x_controller_token or not hmac.compare_digest(x_controller_token, TOKEN):
        raise HTTPException(status_code=401, detail="Invalid or missing controller token")


def own_project() -> str:
    """The compose project this controller runs in (from its own labels): it may only touch containers of that project."""
    global _project
    if _project:
        return _project
    project = os.getenv("CONTROLLER_PROJECT", "").strip()
    if not project:
        with _docker() as d:
            r = d.get(f"/containers/{os.getenv('HOSTNAME', '')}/json")
            r.raise_for_status()
            project = (r.json().get("Config", {}).get("Labels") or {}).get(PROJECT_LABEL, "")
    if not project:
        raise HTTPException(status_code=503, detail="Cannot determine the compose project of this controller")
    _project = project
    return project


def _find(service: str) -> Dict[str, Any]:
    if not SERVICE_RE.match(service) or service not in ALLOWED:
        raise HTTPException(status_code=404, detail="Unknown service")
    flt = json.dumps({"label": [f"{PROJECT_LABEL}={own_project()}", f"{SERVICE_LABEL}={service}"]})
    with _docker() as d:
        r = d.get("/containers/json", params={"all": "true", "filters": flt})
        r.raise_for_status()
        found = r.json()
    if len(found) != 1:
        raise HTTPException(status_code=404, detail="Unknown service" if not found else "Ambiguous service (several replicas)")
    return found[0]


def _summary(service: str, c: Dict[str, Any]) -> Dict[str, Any]:
    return {"service": service, "container": (c.get("Names") or ["?"])[0].lstrip("/"), "state": c.get("State"), "status": c.get("Status"),
            "running": c.get("State") in ("running", "paused"), "paused": c.get("State") == "paused"}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/containers", dependencies=[Depends(require_token)])
def list_containers() -> Dict[str, List[Dict[str, Any]]]:
    out = []
    for service in ALLOWED:
        try:
            out.append(_summary(service, _find(service)))
        except HTTPException:
            out.append({"service": service, "state": "missing", "running": False, "paused": False})
    return {"containers": out}


@app.get("/containers/{service}", dependencies=[Depends(require_token)])
def get_container(service: str):
    return _summary(service, _find(service))


@app.post("/containers/{service}/{action}", dependencies=[Depends(require_token)])
def act(service: str, action: str, timeout: int = Query(default=10, ge=1, le=30)):
    if action not in ACTIONS:
        raise HTTPException(status_code=404, detail="Unknown action")
    c = _find(service)
    with _docker() as d:
        r = d.post(f"/containers/{c['Id']}/{action}", params={"t": timeout} if action == "stop" else None)
    if r.status_code not in (204, 304):                     # 304 = already in that state
        logger.warning(f"{action} {service} failed: HTTP {r.status_code} {r.text[:200]}")
        raise HTTPException(status_code=502, detail=f"Docker refused to {action} {service}: {r.text[:200] or r.status_code}")
    logger.info(f"{action} {service}: {'already' if r.status_code == 304 else 'done'}")
    return _summary(service, _find(service))
