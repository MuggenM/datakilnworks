"""
Brute-force protection for password sign-in (`/api/auth/login`).

Failures are counted per (username, client address) and per client address, in a sliding window. Past the limit, the next attempt is
refused with 429 + Retry-After until a backoff has passed since the last failure (doubling with every further failure, capped at the
window). There is deliberately no hard per-account lockout: that would let anyone lock a known username out; a pair limit only slows
the address that is guessing, and the wider per-address limit catches password spraying across many usernames.

The client address comes from `ip_allowlist.resolve_client` (trusted-proxy rules apply). State is in process memory: the studio runs
as one process (Helm: one replica). A failure counts for local and LDAP accounts alike; a successful sign-in clears its pair.

Env: LOGIN_THROTTLE=off disables it; LOGIN_THROTTLE_WINDOW_SECONDS (900), LOGIN_THROTTLE_PER_USER (5), LOGIN_THROTTLE_PER_IP (30),
LOGIN_THROTTLE_BASE_SECONDS (30).
"""

import os
import threading
import time
from collections import deque
from typing import Deque, Dict, Optional, Tuple

_lock = threading.Lock()
_pairs: Dict[Tuple[str, str], Deque[float]] = {}
_ips: Dict[str, Deque[float]] = {}
MAX_KEYS = 20000


def _cfg():
    return {"on": os.getenv("LOGIN_THROTTLE", "on").strip().lower() not in ("off", "0", "false", "no"),
            "window": float(os.getenv("LOGIN_THROTTLE_WINDOW_SECONDS", "900")),
            "per_user": int(os.getenv("LOGIN_THROTTLE_PER_USER", "5")),
            "per_ip": int(os.getenv("LOGIN_THROTTLE_PER_IP", "30")),
            "base": float(os.getenv("LOGIN_THROTTLE_BASE_SECONDS", "30"))}


def _trim(q: Deque[float], now: float, window: float) -> None:
    while q and now - q[0] > window:
        q.popleft()


def _wait(q: Optional[Deque[float]], limit: int, now: float, c) -> float:
    if not q or len(q) < limit:
        return 0.0
    delay = min(c["base"] * (2 ** (len(q) - limit)), c["window"])
    return max(0.0, q[-1] + delay - now)


def retry_after(username: str, ip: str) -> int:
    """Seconds the caller must wait before this attempt is even checked (0 = go ahead)."""
    c = _cfg()
    if not c["on"]:
        return 0
    now = time.time()
    key = ((username or "").strip().lower(), ip or "")
    with _lock:
        pq, iq = _pairs.get(key), _ips.get(key[1])
        for q in (pq, iq):
            if q is not None:
                _trim(q, now, c["window"])
        w = max(_wait(pq, c["per_user"], now, c), _wait(iq, c["per_ip"], now, c))
    return int(w) + (1 if w > int(w) else 0)


def record_failure(username: str, ip: str) -> None:
    c = _cfg()
    if not c["on"]:
        return
    now = time.time()
    key = ((username or "").strip().lower(), ip or "")
    with _lock:
        if len(_pairs) > MAX_KEYS or len(_ips) > MAX_KEYS:
            _prune(now, c["window"])
        _pairs.setdefault(key, deque()).append(now)
        _ips.setdefault(key[1], deque()).append(now)


def record_success(username: str, ip: str) -> None:
    with _lock:
        _pairs.pop(((username or "").strip().lower(), ip or ""), None)


def _prune(now: float, window: float) -> None:
    for d in (_pairs, _ips):
        for k in [k for k, q in d.items() if not q or now - q[-1] > window]:
            d.pop(k, None)


def reset() -> None:
    """Tests only."""
    with _lock:
        _pairs.clear()
        _ips.clear()
