"""Remembers, for a short while, which compute-node endpoints could not be connected to.

execute_sql tries a warehouse's compute node first and falls back to Ray / in-process. Without a node (every test studio,
a node that is down) each query paid the 3 s connect timeout before that fallback. An endpoint whose CONNECT failed is
now skipped for `COMPUTE_UNREACHABLE_TTL` seconds (default 30, 0 = off); afterwards the next query tries it again.

Only a failure to connect marks an endpoint: a node that answered (any status, or a read timeout on a slow query) is
reachable. A successful resume of the warehouse's container (warehouse_lifecycle._resume_locked, which waits for the
worker's /health) forgets the endpoint at once, so a node that is brought back is used by the very next query.
"""
import logging
import os
import threading
import time
from typing import Dict, Iterable, Optional

logger = logging.getLogger("localspark.compute_reachability")

_lock = threading.Lock()
_down: Dict[str, float] = {}          # endpoint -> time.monotonic() until which it is skipped


def ttl() -> float:
    """Read on every call (a default argument would freeze it at import)."""
    try:
        return max(0.0, float(os.environ.get("COMPUTE_UNREACHABLE_TTL", "30")))
    except ValueError:
        return 30.0


def _key(endpoint: str) -> str:
    return (endpoint or "").strip().rstrip("/")


def is_skipped(endpoint: str) -> bool:
    key = _key(endpoint)
    with _lock:
        until = _down.get(key)
        if until is None:
            return False
        if time.monotonic() >= until:
            del _down[key]
            return False
        return True


def mark_unreachable(endpoint: str) -> None:
    seconds = ttl()
    if seconds <= 0:
        return
    key = _key(endpoint)
    with _lock:
        known = key in _down
        _down[key] = time.monotonic() + seconds
    if not known:
        logger.info(f"Compute node {key} could not be connected to; queries skip it for {seconds:g} s")


def mark_reachable(endpoint: str) -> None:
    with _lock:
        _down.pop(_key(endpoint), None)


def forget(endpoints: Iterable[Optional[str]]) -> None:
    with _lock:
        for ep in endpoints:
            if ep:
                _down.pop(_key(ep), None)


def is_connect_failure(exc: BaseException) -> bool:
    """True only when no connection was made (refused, unresolvable, connect timeout)."""
    import httpx
    return isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout))
