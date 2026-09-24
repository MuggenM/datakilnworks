"""
Real SQL-warehouse lifecycle: suspending idle warehouses and resuming them on demand by stopping / starting the compute-node
container behind them (through the container controller, see controller/controller.py), instead of only flipping a flag.

  suspend(wh_id, reason)     stop (or pause) the container, scale its Ray pool to 0, mark the warehouse STOPPED
  resume(wh_id)              start / unpause it, wait until the worker answers /health, mark RUNNING
  ensure_running(wh_id)      what a query calls first: resumes a suspended (or externally stopped) warehouse, and makes
                             concurrent queries wait for that one resume instead of racing it
  reconcile()                the container is the truth: align each warehouse's state flag with it
  autosuspend_loop(...)      every AUTOSUSPEND_INTERVAL_SECONDS: warehouses idle for `auto_stop_mins` are suspended, unless
                             a query is running on them (studio side or, asked of the worker, on the node)

A warehouse whose endpoint the controller does not manage (a custom endpoint, or no controller running) keeps the old
flag-only behaviour and is reported as not controllable, so nothing pretends to be enforced that is not.

AUTOSUSPEND_INTERVAL_SECONDS   check period (default 30)
AUTOSUSPEND_TIME_SCALE         multiplies the idle timeout (default 1.0; tests use 0.05 so "1 minute" is 3 seconds)
WAREHOUSE_RESUME_TIMEOUT       seconds to wait for a resumed worker to answer (default 60)
"""

import asyncio
import datetime
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from web import container_control, warehouses

logger = logging.getLogger("localspark.warehouse_lifecycle")

INTERVAL = float(os.getenv("AUTOSUSPEND_INTERVAL_SECONDS", "30"))
TIME_SCALE = float(os.getenv("AUTOSUSPEND_TIME_SCALE", "1.0"))
RESUME_TIMEOUT = float(os.getenv("WAREHOUSE_RESUME_TIMEOUT", "60"))
_TS = "%Y-%m-%d %H:%M:%S"

_locks: Dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock(wh_id: str) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(wh_id, threading.Lock())


def _now() -> str:
    return datetime.datetime.now().strftime(_TS)


def service_of(wh: Dict[str, Any]) -> Optional[str]:
    return container_control.service_for_endpoint(wh.get("endpoint"))


def describe(wh: Dict[str, Any]) -> Dict[str, Any]:
    """What the UI shows about a warehouse's container: whether auto-stop / start / stop are real for it."""
    service = service_of(wh)
    state = None
    if service:
        try:
            state = next((c.get("state") for c in container_control.list_containers() if c["service"] == service), None)
        except container_control.ControllerError:
            pass
    return {"controllable": bool(service), "service": service, "container_state": state, "suspend_mode": container_control.suspend_mode()}


def _scale_ray(wh_id: str, workers: int):
    try:
        from web.ray_engine import ray_manager, RAY_INSTALLED
        if RAY_INSTALLED:
            ray_manager.scale_warehouse(wh_id, workers)
    except Exception as exc:
        logger.warning(f"Could not scale the Ray pool of {wh_id} to {workers}: {exc}")


def _suspend_locked(wh_id: str, reason: str) -> Dict[str, Any]:
    wh = warehouses.get_sql_warehouse(wh_id)
    if not wh:
        return {"ok": False, "error": "Warehouse not found"}
    service = service_of(wh)
    if service:
        mode = container_control.suspend_mode()
        try:
            container_control.act(service, "pause" if mode == "pause" else "stop")
        except container_control.ControllerError as exc:
            logger.error(f"Could not suspend warehouse {wh_id} ({service}): {exc}")
            return {"ok": False, "error": str(exc), "warehouse": wh}
    _scale_ray(wh_id, 0)
    updated = warehouses.mutate_sql_warehouse(wh_id, {"state": "STOPPED", "suspended_at": _now(), "suspend_reason": reason,
                                                      "suspend_mode": container_control.suspend_mode() if service else None})
    logger.info(f"Warehouse {wh_id} suspended ({reason}){' via container ' + service if service else ' (flag only)'}")
    return {"ok": True, "container": bool(service), "warehouse": updated}


def suspend(wh_id: str, reason: str = "manual") -> Dict[str, Any]:
    """Suspends a warehouse for real when its container is managed, else only flips the flag (legacy)."""
    with _lock(wh_id):
        return _suspend_locked(wh_id, reason)


def _resume_locked(wh: Dict[str, Any]) -> Dict[str, Any]:
    wh_id, service, started = wh["id"], service_of(wh), time.time()
    warning = None
    if service:
        try:
            st = container_control.status(service)
            if st.get("state") == "paused":
                container_control.act(service, "unpause")
            elif st.get("state") != "running":
                container_control.act(service, "start")
            if wh.get("endpoint") and not container_control.wait_healthy(wh["endpoint"], RESUME_TIMEOUT):
                warning = f"the worker did not answer within {int(RESUME_TIMEOUT)}s"
        except container_control.ControllerError as exc:
            logger.error(f"Could not resume warehouse {wh_id} ({service}): {exc}")
            return {"ok": False, "error": str(exc)}
    ms = round((time.time() - started) * 1000)
    updated = warehouses.mutate_sql_warehouse(wh_id, {"state": "RUNNING", "last_active_at": _now(), "suspended_at": None,
                                                      "suspend_reason": None, "last_resume_ms": ms})
    if warning:
        logger.warning(f"Warehouse {wh_id} resumed but {warning}")
    else:
        logger.info(f"Warehouse {wh_id} resumed in {ms} ms")
    return {"ok": True, "container": bool(service), "resume_ms": ms, "warning": warning, "warehouse": updated}


def resume(wh_id: str) -> Dict[str, Any]:
    with _lock(wh_id):
        wh = warehouses.get_sql_warehouse(wh_id)
        if not wh:
            return {"ok": False, "error": "Warehouse not found"}
        return _resume_locked(wh)


def ensure_running(warehouse_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """Called before a query. Returns {"resume_ms": ...} when it had to bring the warehouse back (the caller can tell the user
    why the query was slow), {"error": ...} if it could not, None when nothing was needed. Never raises."""
    try:
        wh = warehouses.resolve_sql_warehouse(warehouse_id)
        if not wh or not service_of(wh):
            return None
        with _lock(wh["id"]):                                   # waits for a resume already in flight
            wh = warehouses.get_sql_warehouse(wh["id"]) or wh
            state = container_control.status(service_of(wh))["state"]
            if wh.get("state") != "STOPPED" and state == "running":
                warehouses.mark_sql_warehouse_active(wh["id"])   # the idle clock restarts now, so a tick cannot suspend it under this query
                return None
            res = _resume_locked(wh)
        return {"resume_ms": res["resume_ms"], "warning": res.get("warning")} if res["ok"] else {"error": res["error"]}
    except Exception as exc:
        logger.error(f"ensure_running failed: {exc}")
        return {"error": str(exc)}


def reconcile() -> List[str]:
    """Aligns each managed warehouse's flag with its container (a `docker stop` by hand, a compose restart, ...)."""
    changed: List[str] = []
    try:
        containers = {c["service"]: c for c in container_control.list_containers(max_age=0)}
    except container_control.ControllerError:
        return changed
    for wh in warehouses.load_sql_warehouses():
        c = containers.get(urlparse(wh.get("endpoint") or "").hostname or "")
        if not c or c.get("state") == "missing":
            continue
        running = c.get("state") == "running"
        if running and wh.get("state") == "STOPPED":
            warehouses.mutate_sql_warehouse(wh["id"], {"state": "RUNNING", "suspended_at": None, "suspend_reason": None})
            changed.append(wh["id"])
        elif not running and wh.get("state") == "RUNNING":
            warehouses.mutate_sql_warehouse(wh["id"], {"state": "STOPPED", "suspended_at": _now(),
                                                       "suspend_reason": "external" if c.get("state") != "paused" else "paused"})
            changed.append(wh["id"])
    return changed


def _idle_seconds(wh: Dict[str, Any], now: Optional[float] = None) -> float:
    try:
        last = datetime.datetime.strptime(wh.get("last_active_at") or "", _TS).timestamp()
    except ValueError:
        return 0.0
    return (time.time() if now is None else now) - last


def _worker_busy(wh: Dict[str, Any]) -> bool:
    """True when the compute node itself reports a running query."""
    try:
        import httpx
        from web.compute_auth import compute_headers
        r = httpx.get(f"{wh['endpoint'].rstrip('/')}/api/compute/status", headers=compute_headers(), timeout=3.0)
        return r.status_code == 200 and int(r.json().get("queries_active", 0)) > 0
    except Exception:
        return False


def autosuspend_tick(busy_check: Callable[[str], bool] = lambda _id: False, now: Optional[float] = None) -> List[str]:
    """One pass: reconcile, then suspend every managed, running warehouse idle for its `auto_stop_mins`."""
    suspended: List[str] = []
    reconcile()
    for wh in warehouses.load_sql_warehouses():
        mins = int(wh.get("auto_stop_mins") or 0)
        if wh.get("state") != "RUNNING" or mins <= 0 or not service_of(wh):
            continue
        limit = mins * 60 * TIME_SCALE
        if _idle_seconds(wh, now) < limit or busy_check(wh["id"]) or _worker_busy(wh):
            continue
        with _lock(wh["id"]):                                   # re-check under the lock: a query may just have resumed / used it
            fresh = warehouses.get_sql_warehouse(wh["id"]) or wh
            if fresh.get("state") != "RUNNING" or _idle_seconds(fresh, now) < limit or busy_check(wh["id"]):
                continue
            if _suspend_locked(wh["id"], "idle").get("ok"):
                suspended.append(wh["id"])
    return suspended


async def autosuspend_loop(busy_check: Callable[[str], bool]):
    logger.info(f"Warehouse auto-suspend loop started (every {INTERVAL:g}s).")
    warned = False
    while True:
        try:
            if container_control.configured():
                warned = False
                await asyncio.to_thread(autosuspend_tick, busy_check)
            elif not warned:
                logger.warning("Container control is not configured (no container-controller): warehouse auto-stop is NOT enforced.")
                warned = True
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(f"Auto-suspend pass failed: {exc}", exc_info=True)
        await asyncio.sleep(INTERVAL)
