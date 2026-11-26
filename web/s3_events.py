"""S3 bucket event notifications as an Auto-Loader trigger: an S3-compatible server (MinIO, Garage, AWS through an HTTP subscriber...) POSTs "an object
was created" to the studio, and the matching pipeline runs at once instead of waiting for its next poll.

Receiver  POST /hooks/s3-events, authenticated by a bearer token created under Auto-Loader > S3 events (shown once, only its SHA-256 is stored, revocable;
          MinIO's `auth_token` setting sends exactly this). It accepts the standard S3 event JSON (`{"Records": [{"eventName": "s3:ObjectCreated:Put",
          "s3": {"bucket": {"name": ..}, "object": {"key": ..}}}]}`), also MinIO's wrapper (`EventName`, `Key`, `Records`) and lists of those.
What it does  An event only WAKES a pipeline; it is never trusted as data. The pipeline lists its prefix and reads objects with its own storage mount
          credentials, exactly as in a poll (exactly-once identities unchanged). So a forged or duplicated event can at worst cause one extra listing,
          which is why events are debounced (`S3_EVENTS_DEBOUNCE`, default 2 s: a burst of uploads is one run) and never run two cycles of a pipeline at
          once (an event during a run schedules one more run afterwards).
Which pipelines  Only enabled pipelines with an s3:// source whose `s3_events` setting is on, whose bucket and prefix match, and whose file pattern and the
          scan's ignore rules (hidden names, .tmp / .part, _quarantine) accept the key; ObjectCreated events only (removals are ignored).
Safety net  Events can be lost (server restart, network). A pipeline with `s3_events` therefore still rescans every `watch_sweep_seconds` (default 5 min)
          by itself, like the file-watch mode of local volumes.
"""
import hashlib
import hmac
import json
import logging
import os
import secrets
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote_plus

logger = logging.getLogger("localspark.s3events")

MAX_BODY = 1_000_000
MAX_RECORDS = 1000
DEBOUNCE_SECONDS = float(os.getenv("S3_EVENTS_DEBOUNCE", "2"))


class EventError(Exception):
    """An invalid request; the message is safe to show."""


def _db():
    from web import autoloader
    conn = autoloader.get_db()
    conn.execute("""CREATE TABLE IF NOT EXISTS s3_event_tokens (id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE, prefix TEXT,
        created_by TEXT, created_at TEXT, last_used_at TEXT, revoked_at TEXT)""")
    conn.commit()
    return conn


def _now() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _audit(actor: str, action: str, target: str, detail: Dict[str, Any]) -> None:
    try:
        from web.governance import store
        store.init_governance_db()
        c = store.get_db()
        try:
            store.write_audit(c, actor, action, target, detail)
            c.commit()
        finally:
            c.close()
    except Exception as exc:
        logger.warning(f"could not audit {action}: {exc}")


# ---------------------------------------------------------------- tokens

def create_token(name: str, actor: str) -> Dict[str, Any]:
    name = (name or "").strip()
    if not 1 <= len(name) <= 60:
        raise EventError("Give the token a name (1-60 characters), for example 'MinIO production'.")
    token = "dkw_s3ev_" + secrets.token_hex(24)
    tid = f"s3t_{uuid.uuid4().hex[:8]}"
    c = _db()
    try:
        c.execute("INSERT INTO s3_event_tokens (id, name, token_hash, prefix, created_by, created_at) VALUES (?,?,?,?,?,?)",
                  (tid, name, hashlib.sha256(token.encode()).hexdigest(), token[:13] + "...", actor, _now()))
        c.commit()
    finally:
        c.close()
    _audit(actor, "S3EVENTS_TOKEN_CREATE", f"s3-events-token:{name}", {"token_id": tid})
    return {"id": tid, "name": name, "token": token}


def list_tokens() -> List[Dict[str, Any]]:
    c = _db()
    try:
        return [dict(r) for r in c.execute("SELECT id, name, prefix, created_by, created_at, last_used_at, revoked_at FROM s3_event_tokens ORDER BY created_at DESC")]
    finally:
        c.close()


def revoke_token(token_id: str, actor: str) -> None:
    c = _db()
    try:
        r = c.execute("SELECT name FROM s3_event_tokens WHERE id = ?", (token_id,)).fetchone()
        if not r:
            raise LookupError("Token not found.")
        c.execute("UPDATE s3_event_tokens SET revoked_at = COALESCE(revoked_at, ?) WHERE id = ?", (_now(), token_id))
        c.commit()
    finally:
        c.close()
    _audit(actor, "S3EVENTS_TOKEN_REVOKE", f"s3-events-token:{r['name']}", {"token_id": token_id})


def authenticate(header: Optional[str]) -> Dict[str, Any]:
    """The token row for `Authorization: Bearer <token>` (MinIO may send the bare token). Raises EventError on anything else."""
    if not header:
        raise EventError("A bearer token is required.")
    token = header[7:].strip() if header.lower().startswith("bearer ") else header.strip()
    if not token.startswith("dkw_s3ev_") or len(token) > 200:
        raise EventError("The token is not valid.")
    digest = hashlib.sha256(token.encode()).hexdigest()
    c = _db()
    try:
        row = next((r for r in c.execute("SELECT * FROM s3_event_tokens WHERE token_hash = ?", (digest,)) if hmac.compare_digest(r["token_hash"], digest)), None)
        if not row or row["revoked_at"]:
            raise EventError("The token is not valid or was revoked.")
        c.execute("UPDATE s3_event_tokens SET last_used_at = ? WHERE id = ?", (_now(), row["id"]))
        c.commit()
        return dict(row)
    finally:
        c.close()


# ---------------------------------------------------------------- parsing and matching

def parse_records(payload: Any) -> List[Tuple[str, str, str]]:
    """[(event name, bucket, key)] from the S3 event notification shapes; the key is URL-decoded (S3 sends it encoded, spaces as '+')."""
    items = payload if isinstance(payload, list) else [payload]
    out: List[Tuple[str, str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            raise EventError("The body is not an S3 event notification.")
        recs = it.get("Records")
        if recs is None and "s3" in it:
            recs = [it]
        if recs is None:
            if it.get("Event") == "s3:TestEvent" or it.get("Service") == "Amazon S3":
                continue                                            # AWS's connectivity test message
            raise EventError("No 'Records' in the body: this is not an S3 event notification.")
        if not isinstance(recs, list):
            raise EventError("'Records' must be a list.")
        for r in recs:
            if not isinstance(r, dict):
                continue
            s3 = r.get("s3") or {}
            name = str(r.get("eventName") or it.get("EventName") or "")
            bucket = str(((s3.get("bucket") or {}).get("name")) or "")
            key = unquote_plus(str(((s3.get("object") or {}).get("key")) or ""))
            if bucket and key:
                out.append((name, bucket, key))
            if len(out) > MAX_RECORDS:
                raise EventError(f"At most {MAX_RECORDS} records per request.")
    return out


def matching_pipelines(events: List[Tuple[str, str, str]]) -> List[str]:
    """Ids of the pipelines an ObjectCreated event should wake."""
    from web import autoloader, autoloader_s3
    created = [(b, k) for n, b, k in events if n.replace("s3:", "").startswith("ObjectCreated") or not n]
    if not created:
        return []
    woken: List[str] = []
    for p in autoloader.list_pipelines():
        if not p.get("enabled", 1) or not p.get("s3_events") or not autoloader_s3.is_s3_path(p.get("source_volume_path")) or p.get("cron_schedule"):
            continue
        try:
            bucket, prefix = autoloader_s3.parse_s3_path(p["source_volume_path"])
        except autoloader_s3.S3SourceError:
            continue
        pattern = p.get("file_pattern") or "*"
        for b, k in created:
            if b == bucket and k.startswith(prefix) and not autoloader_s3.is_ignored_key(k[len(prefix):], pattern):
                woken.append(p["id"])
                break
    return woken


# ---------------------------------------------------------------- waking pipelines (debounced, one cycle at a time)

class Waker:
    def __init__(self):
        self._lock = threading.Lock()
        self._state: Dict[str, Dict[str, Any]] = {}
        self.stats: Dict[str, Dict[str, Any]] = {}
        self.recent: List[Dict[str, Any]] = []

    def record(self, token_name: str, n_events: int, woken: List[str]) -> None:
        now = int(time.time())
        with self._lock:
            self.recent.insert(0, {"at": now, "token": token_name, "events": n_events, "woke": list(woken)})
            del self.recent[100:]
            for pid in woken:
                s = self.stats.setdefault(pid, {"events": 0, "last_event_at": None, "last_run_at": None})
                s["events"] += 1
                s["last_event_at"] = now

    def wake(self, pipeline_id: str, run=None) -> None:
        run = run or _run_cycle
        with self._lock:
            st = self._state.setdefault(pipeline_id, {"timer": None, "running": False, "dirty": False})
            if st["running"]:
                st["dirty"] = True                                  # an event during a run: one more run afterwards
                return
            if st["timer"] is not None:
                return                                              # already scheduled: the burst is one run
            t = threading.Timer(DEBOUNCE_SECONDS, self._fire, args=(pipeline_id, run))
            t.daemon = True
            st["timer"] = t
            t.start()

    def _fire(self, pipeline_id: str, run) -> None:
        while True:
            with self._lock:
                st = self._state[pipeline_id]
                st["timer"], st["running"], st["dirty"] = None, True, False
            try:
                run(pipeline_id)
                with self._lock:
                    self.stats.setdefault(pipeline_id, {"events": 0, "last_event_at": None, "last_run_at": None})["last_run_at"] = int(time.time())
            except Exception as exc:
                logger.error(f"S3-event run of {pipeline_id} failed: {exc}", exc_info=True)
            with self._lock:
                st = self._state[pipeline_id]
                st["running"] = False
                if not st["dirty"]:
                    return


def _run_cycle(pipeline_id: str) -> None:
    from web import autoloader
    autoloader.run_pipeline_cycle(pipeline_id)


WAKER = Waker()


def handle(payload: Any, token_name: str) -> Dict[str, Any]:
    events = parse_records(payload)
    woken = matching_pipelines(events)
    woken = list(dict.fromkeys(woken))
    WAKER.record(token_name, len(events), woken)
    for pid in woken:
        WAKER.wake(pid)
    return {"received": len(events), "woke": woken}


def status() -> Dict[str, Any]:
    with WAKER._lock:
        return {"pipelines": {k: dict(v) for k, v in WAKER.stats.items()}, "recent": list(WAKER.recent[:50])}


# ---------------------------------------------------------------- HTTP route

import asyncio
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()


@router.post("/hooks/s3-events")
async def receive_s3_events(request: Request):
    try:
        tok = await asyncio.to_thread(authenticate, request.headers.get("authorization"))
    except EventError as exc:
        return JSONResponse(status_code=401, content={"detail": str(exc)}, headers={"WWW-Authenticate": 'Bearer realm="s3-events"'})
    raw = await request.body()
    if len(raw) > MAX_BODY:
        return JSONResponse(status_code=413, content={"detail": "The request body is too large."})
    try:
        payload = json.loads(raw.decode("utf-8") or "{}")
        result = await asyncio.to_thread(handle, payload, tok["name"])
    except (ValueError, UnicodeDecodeError):
        return JSONResponse(status_code=400, content={"detail": "The body is not valid JSON."})
    except EventError as exc:
        return JSONResponse(status_code=400, content={"detail": str(exc)})
    return result
