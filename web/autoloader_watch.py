"""
File-watch-based triggering for Auto-Loader pipelines: a step up from polling.

A pipeline with `watch_enabled` is woken by the kernel (inotify) the moment a file lands in its volume folder,
instead of scanning on a timer. Ingestion itself is unchanged (`run_pipeline_cycle` scans, fingerprints and loads),
so exactly-once behaviour, schema policies and quarantine are identical; only *when* a cycle starts differs.

Design:
  * Linux inotify through ctypes: no new dependency. Where it is unavailable (another OS, a locked-down container,
    the per-user watch limit) the pipeline reports `fallback` and keeps running on its polling interval.
  * Only "the writer is finished" events count: `IN_CLOSE_WRITE` (a file written and closed) and `IN_MOVED_TO`
    (a file, or a whole directory, renamed into place). `IN_CREATE` is deliberately ignored for files: it fires
    before any data exists, and a half-written file must not be ingested (its fingerprint changes when it grows,
    so it would be loaded twice). Names the scan ignores (hidden, `.tmp`, `.part`, `_quarantine`, non-matching
    patterns) never trigger anything.
  * Events are debounced and coalesced: a burst of files becomes one cycle a moment after the burst goes quiet (or
    after at most MAX_WAIT_SECONDS under a continuous stream). A cycle never overlaps itself (see the per-pipeline
    lock in `run_pipeline_cycle`), and events arriving during one trigger a follow-up cycle.
  * inotify can miss things (queue overflow, network or FUSE filesystems that emit no events, files that arrived
    while the studio was down, hard links), so a low-frequency reconciliation sweep (`watch_sweep_seconds`)
    still runs; the watcher start itself also triggers one immediate catch-up cycle.
"""

import ctypes
import ctypes.util
import errno
import fnmatch
import logging
import os
import select
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger("localspark.autoloader.watch")

IN_CLOSE_WRITE, IN_MOVED_TO, IN_CREATE = 0x00000008, 0x00000080, 0x00000100
IN_DELETE_SELF, IN_MOVE_SELF = 0x00000400, 0x00000800
IN_Q_OVERFLOW, IN_IGNORED = 0x00004000, 0x00008000
IN_ONLYDIR, IN_ISDIR = 0x01000000, 0x40000000
IN_NONBLOCK, IN_CLOEXEC = 0o4000, 0o2000000
_DIR_MASK = IN_CLOSE_WRITE | IN_MOVED_TO | IN_CREATE | IN_DELETE_SELF | IN_MOVE_SELF | IN_ONLYDIR
_EVENT_HEADER = struct.Struct("iIII")

DEBOUNCE_SECONDS = float(os.getenv("AUTOLOADER_WATCH_DEBOUNCE", "1.0"))
MAX_WAIT_SECONDS = float(os.getenv("AUTOLOADER_WATCH_MAX_WAIT", "10.0"))
DEFAULT_SWEEP_SECONDS = 300
MIN_SWEEP_SECONDS = 30

_libc = None


def _load_libc():
    global _libc
    if _libc is None:
        name = ctypes.util.find_library("c")
        _libc = ctypes.CDLL(name, use_errno=True) if name else False
    return _libc or None


def inotify_available() -> bool:
    if not os.name == "posix" or not hasattr(os, "uname") or os.uname().sysname != "Linux":
        return False
    libc = _load_libc()
    return bool(libc and hasattr(libc, "inotify_init1"))


def is_ignored_name(name: str, pattern: str) -> bool:
    """True for files the pipeline scan would skip (must stay in step with run_pipeline_cycle's discovery)."""
    if name.startswith(".") or name.endswith(".tmp") or name.endswith(".part"):
        return True
    return not (pattern == "*" or fnmatch.fnmatch(name.lower(), pattern.lower()))


class WatchUnavailable(Exception):
    """inotify cannot be used for this directory; the caller falls back to polling."""


class InotifyWatcher:
    """Recursive inotify watch of one directory tree. `on_event(path)` is called from the watcher thread."""

    def __init__(self, root: str, pattern: str, on_event: Callable[[str], None]):
        libc = _load_libc()
        if not inotify_available():
            raise WatchUnavailable("inotify is only available on Linux")
        self.root, self.pattern, self.on_event = os.path.abspath(root), pattern or "*", on_event
        self._libc = libc
        self._fd = libc.inotify_init1(IN_NONBLOCK | IN_CLOEXEC)
        if self._fd < 0:
            err = ctypes.get_errno()
            hint = " (the per-user inotify instance limit, fs.inotify.max_user_instances, is exhausted; it is retried every few seconds)" if err == errno.EMFILE else ""
            raise WatchUnavailable(f"inotify_init1 failed: {os.strerror(err)}{hint}")
        self._wd_to_path: Dict[int, str] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self.dead = False
        self.events = 0
        self.last_event_at: Optional[float] = None
        self.detail = ""
        if not self._add_tree(self.root, raise_on_root=True):
            os.close(self._fd)
            raise WatchUnavailable(self.detail or "could not watch the directory")
        self._thread = threading.Thread(target=self._run, name=f"autoloader-watch-{os.path.basename(self.root)}", daemon=True)
        self._thread.start()

    @property
    def watch_count(self) -> int:
        with self._lock:
            return len(self._wd_to_path)

    def _add_dir(self, path: str) -> bool:
        wd = self._libc.inotify_add_watch(self._fd, os.fsencode(path), _DIR_MASK)
        if wd < 0:
            err = ctypes.get_errno()
            if err == errno.ENOSPC:
                self.detail = "inotify watch limit reached (fs.inotify.max_user_watches); some sub-folders rely on the rescan"
            elif err != errno.ENOENT:
                self.detail = f"could not watch {path}: {os.strerror(err)}"
            return False
        with self._lock:
            self._wd_to_path[wd] = path
        return True

    def _add_tree(self, top: str, raise_on_root: bool = False) -> bool:
        ok = True
        for dirpath, dirs, _files in os.walk(top, followlinks=False):
            dirs[:] = [d for d in dirs if not d.startswith(".") and d != "_quarantine" and not os.path.islink(os.path.join(dirpath, d))]
            if not self._add_dir(dirpath):
                ok = False
                if dirpath == top and raise_on_root:
                    return False
        return ok

    def _handle(self, wd: int, mask: int, name: str):
        if mask & IN_Q_OVERFLOW:
            self.detail = "event queue overflowed; a rescan was triggered"
            self._fire(self.root)
            return
        with self._lock:
            base = self._wd_to_path.get(wd)
        if mask & IN_IGNORED:
            with self._lock:
                self._wd_to_path.pop(wd, None)
            if base == self.root:
                self.dead = True
            return
        if base is None:
            return
        if mask & (IN_DELETE_SELF | IN_MOVE_SELF):
            if base == self.root:
                self.dead = True                      # the manager notices and re-creates the watcher
            return
        full = os.path.join(base, name)
        if mask & IN_ISDIR:
            if mask & (IN_CREATE | IN_MOVED_TO) and not name.startswith(".") and name != "_quarantine":
                self._add_tree(full)
                self._fire(full)                      # files already inside (renamed in) or written before the watch was added emit no event
            return
        if mask & (IN_CLOSE_WRITE | IN_MOVED_TO) and not is_ignored_name(name, self.pattern):
            self._fire(full)

    def _fire(self, path: str):
        self.events += 1
        self.last_event_at = time.time()
        try:
            self.on_event(path)
        except Exception as exc:                     # a callback bug must never kill the watcher
            logger.error(f"Auto-Loader watch callback failed: {exc}", exc_info=True)

    def _run(self):
        try:
            while not self._stop.is_set():
                ready, _, _ = select.select([self._fd], [], [], 0.5)
                if not ready:
                    continue
                try:
                    data = os.read(self._fd, 65536)
                except BlockingIOError:
                    continue
                pos = 0
                while pos + _EVENT_HEADER.size <= len(data):
                    wd, mask, _cookie, length = _EVENT_HEADER.unpack_from(data, pos)
                    name = data[pos + _EVENT_HEADER.size: pos + _EVENT_HEADER.size + length].split(b"\0", 1)[0]
                    pos += _EVENT_HEADER.size + length
                    self._handle(wd, mask, os.fsdecode(name))
        except Exception as exc:
            logger.error(f"Auto-Loader watcher for {self.root} stopped: {exc}", exc_info=True)
            self.detail = f"watcher stopped: {exc}"
        finally:
            self.dead = True
            try:
                os.close(self._fd)
            except OSError:
                pass

    def stop(self):
        self._stop.set()
        if self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(timeout=2)


class WatchManager:
    """Owns one InotifyWatcher per watch-enabled pipeline and turns their events into (debounced) pipeline cycles."""

    def __init__(self, run_cycle: Callable[[str], Dict[str, Any]], resolve_dir: Callable[[str], str]):
        self._run_cycle, self._resolve_dir = run_cycle, resolve_dir
        self._watchers: Dict[str, InotifyWatcher] = {}
        self._keys: Dict[str, tuple] = {}
        self._problem: Dict[str, str] = {}
        self._state: Dict[str, Dict[str, Any]] = {}
        self._cv = threading.Condition()
        self._pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="autoloader-watch-run")
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._dispatch, name="autoloader-watch-dispatch", daemon=True)
        self._thread.start()

    # ---- events -> cycles
    def notify(self, pipeline_id: str):
        now = time.time()
        with self._cv:
            st = self._state.setdefault(pipeline_id, {"pending": False, "first": 0.0, "last": 0.0, "running": False, "cycles": 0})
            if not st["pending"]:
                st["first"] = now
            st["pending"], st["last"] = True, now
            self._cv.notify()

    def _dispatch(self):
        while not self._stop.is_set():
            with self._cv:
                due, wait = [], 1.0
                now = time.time()
                for pid, st in self._state.items():
                    if st["pending"] and not st["running"]:
                        ready_at = min(st["last"] + DEBOUNCE_SECONDS, st["first"] + MAX_WAIT_SECONDS)
                        if ready_at <= now:
                            st["pending"], st["running"] = False, True
                            due.append(pid)
                        else:
                            wait = min(wait, ready_at - now)
                if not due:
                    self._cv.wait(timeout=max(0.05, wait))
            for pid in due:
                self._pool.submit(self._run_one, pid)

    def _run_one(self, pid: str):
        try:
            result = self._run_cycle(pid) or {}
            if result.get("skipped"):                    # another trigger was mid-cycle: it may have scanned before our file
                self.notify(pid)
        except Exception as exc:
            logger.error(f"Auto-Loader watch-triggered cycle for {pid} failed: {exc}", exc_info=True)
        finally:
            with self._cv:
                st = self._state.get(pid)
                if st:
                    st["running"] = False
                    st["cycles"] += 1
                    if st["pending"]:
                        self._cv.notify()

    # ---- watcher lifecycle
    def sync(self, pipelines: List[Dict[str, Any]]):
        """Reconcile running watchers with the current pipeline configuration."""
        wanted: Dict[str, tuple] = {}
        invalid: Dict[str, str] = {}
        for p in pipelines:
            if p.get("enabled", 1) and p.get("watch_enabled") and not p.get("cron_schedule"):
                try:
                    wanted[p["id"]] = (self._resolve_dir(p["source_volume_path"]), p.get("file_pattern") or "*")
                except Exception as exc:
                    invalid[p["id"]] = f"invalid source volume path: {exc}"
        for pid in list(self._watchers):
            w = self._watchers[pid]
            if pid not in wanted or self._keys.get(pid) != wanted[pid] or w.dead:
                w.stop()
                self._watchers.pop(pid, None)
                self._keys.pop(pid, None)
        for pid in list(self._problem):
            if pid not in wanted:
                self._problem.pop(pid, None)
        self._problem.update(invalid)
        for pid, (path, pattern) in wanted.items():
            if pid in self._watchers:
                continue
            try:
                os.makedirs(path, exist_ok=True)
                self._watchers[pid] = InotifyWatcher(path, pattern, lambda _p, pid=pid: self.notify(pid))
                self._keys[pid] = (path, pattern)
                self._problem.pop(pid, None)
                logger.info(f"Auto-Loader pipeline {pid}: watching {path} ({self._watchers[pid].watch_count} directories)")
                self.notify(pid)                          # catch files that arrived while nobody was watching
            except WatchUnavailable as exc:
                if self._problem.get(pid) != str(exc):
                    logger.warning(f"Auto-Loader pipeline {pid}: file watching unavailable ({exc}); using polling")
                self._problem[pid] = str(exc)
            except Exception as exc:
                self._problem[pid] = str(exc)
                logger.error(f"Auto-Loader pipeline {pid}: could not start watcher: {exc}", exc_info=True)

    def is_watching(self, pipeline_id: str) -> bool:
        w = self._watchers.get(pipeline_id)
        return bool(w and not w.dead)

    def status(self, pipeline: Dict[str, Any]) -> Dict[str, Any]:
        if not pipeline.get("watch_enabled"):
            return {"mode": "off"}
        if pipeline.get("cron_schedule"):
            return {"mode": "off", "detail": "a cron schedule takes precedence over file watching"}
        pid = pipeline["id"]
        w = self._watchers.get(pid)
        if w and not w.dead:
            return {"mode": "watching", "directories": w.watch_count, "events": w.events, "detail": w.detail or None,
                    "last_event_at": datetime.utcfromtimestamp(w.last_event_at).strftime("%Y-%m-%d %H:%M:%S") if w.last_event_at else None,
                    "sweep_seconds": sweep_seconds(pipeline)}
        return {"mode": "fallback", "detail": self._problem.get(pid) or "starting", "poll_seconds": pipeline.get("poll_interval_seconds")}

    def stop(self):
        self._stop.set()
        with self._cv:
            self._cv.notify_all()
        for w in list(self._watchers.values()):
            w.stop()
        self._watchers.clear()
        self._pool.shutdown(wait=False)


def sweep_seconds(pipeline: Dict[str, Any]) -> int:
    try:
        return max(MIN_SWEEP_SECONDS, int(pipeline.get("watch_sweep_seconds") or DEFAULT_SWEEP_SECONDS))
    except (TypeError, ValueError):
        return DEFAULT_SWEEP_SECONDS
