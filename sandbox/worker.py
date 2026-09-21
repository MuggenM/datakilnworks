"""
Notebook sandbox worker: runs the IPython kernels of users a masking policy applies to.

Runs in its own container (service `notebook-sandbox` in docker-compose.yml) which has NO mount of the warehouse, the
studio's metadata or the other users' notebooks. Isolation, from the outside in:

  container   no warehouse volume, cap_drop ALL (+ what uid switching needs), read-only root, pids/memory limits, an
              internal network that only the studio is on (no internet, no compute nodes, no Ray)
  OS user     every studio user gets a separate uid; each kernel drops to it (sandbox/launch_kernel.py) before any user
              code runs, so kernels of different users cannot read each other's memory, files or connection keys
  data        kernels read data only through the studio's governed endpoint /api/sandbox/sql with a token bound to the
              user (sandbox/shim.py); the token file is readable only by that user's uid
  worker      this API needs the worker token (generated at first start on a volume the studio mounts read-only). It is
              removed from the environment here and never reaches a kernel.

API (all but /health need `Authorization: Bearer <worker token>`):
    POST   /kernels/{kid}/execute   {owner, notebook, code, timeout, token}
    POST   /kernels/{kid}/restart   {owner, notebook, token}
    GET    /kernels/{kid}/status
    DELETE /kernels/{kid}
"""

import hmac
import json
import logging
import os
import re
import secrets
import sys
import threading
import time
from typing import Dict, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.getenv("SANDBOX_RUNNER_DIR", "/opt/runner"))    # web/notebook_runner.py, mounted read-only

STATE = os.getenv("SANDBOX_STATE", "/sandbox/state")
HOMES = os.path.join(STATE, "home")
TOKENS = os.path.join(STATE, "tokens")
JUPYTER_DIR = os.path.join(STATE, "jupyter")
TOKEN_FILE = os.getenv("SANDBOX_TOKEN_FILE", "/run/sandbox/token")
UID_BASE = 20000
MAX_KERNELS = int(os.getenv("SANDBOX_MAX_KERNELS", "32"))
IDLE_SECONDS = int(os.getenv("SANDBOX_IDLE_SECONDS", "3600"))
GATEWAY_URL = os.getenv("SANDBOX_GATEWAY_URL", "http://datakilnworks-studio:8000").rstrip("/")
MAX_ROWS = os.getenv("SANDBOX_MAX_ROWS", "1000000")

logger = logging.getLogger("sandbox")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

os.makedirs(HOMES, mode=0o755, exist_ok=True)
os.makedirs(TOKENS, mode=0o755, exist_ok=True)
os.environ["JUPYTER_PATH"] = JUPYTER_DIR


def _load_worker_token() -> str:
    """The worker token: SANDBOX_TOKEN when set, else generated once into TOKEN_FILE (0600). Removed from the environment."""
    token = os.environ.pop("SANDBOX_TOKEN", "").strip()
    if token:
        return token
    try:
        os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
        fd = os.open(TOKEN_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(secrets.token_hex(32))
    except FileExistsError:
        pass
    with open(TOKEN_FILE) as f:
        return f.read().strip()


WORKER_TOKEN = _load_worker_token()


def _write_kernelspec() -> None:
    spec_dir = os.path.join(JUPYTER_DIR, "kernels", "sandbox")
    os.makedirs(spec_dir, exist_ok=True)
    with open(os.path.join(spec_dir, "kernel.json"), "w") as f:
        json.dump({"argv": [sys.executable, os.path.join(HERE, "launch_kernel.py"), "{connection_file}"],
                   "display_name": "Python 3 (sandbox)", "language": "python"}, f)


_write_kernelspec()

from notebook_runner import KernelSession  # noqa: E402  (mounted read-only from web/)

_uid_lock = threading.Lock()


def uid_for(owner: str) -> int:
    """A stable, unique OS uid per studio user (persisted, so home directories keep their owner across restarts)."""
    path = os.path.join(STATE, "uids.json")
    with _uid_lock:
        try:
            with open(path) as f:
                table = json.load(f)
        except (OSError, ValueError):
            table = {}
        if owner not in table:
            table[owner] = UID_BASE + len(table)
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(table, f)
            os.replace(tmp, path)
        return table[owner]


def write_token(uid: int, token: str) -> str:
    """Drops the user's current gateway token where only that uid can read it (atomic replace in a root-owned directory)."""
    path = os.path.join(TOKENS, str(uid))
    tmp = f"{path}.{secrets.token_hex(4)}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
    with os.fdopen(fd, "w") as f:
        f.write(token)
    os.chown(tmp, uid, uid)
    os.replace(tmp, path)
    return path


class SandboxKernel(KernelSession):
    kernel_name = "sandbox"

    def __init__(self, notebook: str, owner: str):
        super().__init__(notebook, owner)
        self.uid = uid_for(owner)
        self.home = os.path.join(HOMES, str(self.uid))

    def _start_kwargs(self):
        # A complete, minimal environment: nothing of the worker's (its token is gone anyway) reaches user code.
        env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8", "HOME": self.home, "TMPDIR": os.path.join(self.home, "tmp"),
            "MPLCONFIGDIR": os.path.join(self.home, ".mpl"), "IPYTHONDIR": os.path.join(self.home, ".ipython"),
            "SANDBOX_UID": str(self.uid), "SANDBOX_SHIM": os.path.join(HERE, "shim.py"),
            "DKW_GATEWAY_URL": GATEWAY_URL, "DKW_TOKEN_FILE": os.path.join(TOKENS, str(self.uid)), "DKW_MAX_ROWS": MAX_ROWS,
            "PYTHONUNBUFFERED": "1",
        }
        return {"env": env}


KERNELS: Dict[str, SandboxKernel] = {}
KERNELS_LOCK = threading.Lock()
KID_RE = re.compile(r"^[0-9a-f]{32}$")

app = FastAPI(title="Data Kiln Works notebook sandbox", docs_url=None, redoc_url=None, openapi_url=None)


def require_token(authorization: Optional[str] = Header(None)) -> None:
    supplied = authorization[7:].strip() if authorization and authorization.lower().startswith("bearer ") else ""
    if not supplied or not hmac.compare_digest(supplied, WORKER_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid worker token.")


def _kernel(kid: str, owner: str, notebook: str, create: bool) -> Optional[SandboxKernel]:
    if not KID_RE.match(kid):
        raise HTTPException(status_code=400, detail="Bad kernel id.")
    with KERNELS_LOCK:
        kernel = KERNELS.get(kid)
        if kernel is not None and kernel.owner != owner and owner:
            raise HTTPException(status_code=403, detail="Kernel belongs to another user.")
        if kernel is None and create:
            if len(KERNELS) >= MAX_KERNELS:
                raise HTTPException(status_code=503, detail=f"The sandbox is full ({MAX_KERNELS} kernels). Try again later.")
            kernel = KERNELS[kid] = SandboxKernel(notebook, owner)
        return kernel


class ExecBody(BaseModel):
    owner: str
    notebook: str
    code: str = ""
    timeout: int = 120
    token: str


@app.get("/health")
def health():
    return {"status": "ok", "kernels": len(KERNELS)}


@app.post("/kernels/{kid}/execute", dependencies=[Depends(require_token)])
def execute(kid: str, body: ExecBody):
    kernel = _kernel(kid, body.owner, body.notebook, create=True)
    write_token(kernel.uid, body.token)
    return kernel.execute_code(body.code, timeout=min(max(body.timeout, 1), 3600))


@app.post("/kernels/{kid}/restart", dependencies=[Depends(require_token)])
def restart(kid: str, body: ExecBody):
    kernel = _kernel(kid, body.owner, body.notebook, create=True)
    write_token(kernel.uid, body.token)
    kernel.restart()
    return {"success": True, "status": kernel.status}


@app.get("/kernels/{kid}/status", dependencies=[Depends(require_token)])
def status(kid: str):
    kernel = _kernel(kid, "", "", create=False)
    alive = bool(kernel and kernel.is_alive())
    return {"is_alive": alive, "status": kernel.status if alive else "stopped", "execution_count": kernel.execution_count if kernel else 0}


@app.delete("/kernels/{kid}", dependencies=[Depends(require_token)])
def delete(kid: str):
    with KERNELS_LOCK:
        kernel = KERNELS.pop(kid, None) if KID_RE.match(kid) else None
    if kernel is not None:
        kernel.shutdown()
    return {"success": True}


def _reaper() -> None:
    while True:
        time.sleep(60)
        cutoff = time.time() - IDLE_SECONDS
        with KERNELS_LOCK:
            idle = [k for k, v in KERNELS.items() if v.last_active < cutoff and v.status != "busy"]
            victims = [KERNELS.pop(k) for k in idle]
        for kernel in victims:
            logger.info(f"Shutting down idle sandbox kernel of {kernel.owner}")
            kernel.shutdown()


threading.Thread(target=_reaper, daemon=True, name="sandbox-reaper").start()
