#!/usr/bin/env python3
"""
File-watch-based Auto-Loader triggering (web/autoloader_watch.py), on a throwaway WAREHOUSE_DIR. No daemon runs: only
the inotify watcher + dispatcher, so anything ingested here was triggered by a filesystem event.
Tests: event latency vs no timer; half-written files are not ingested until the writer finishes (exactly once);
.tmp/.part -> rename; pattern and hidden-file filtering; sub-folders (created later) and renamed-in folders;
bursts coalesce into few cycles; catch-up of files present before watching; the quarantine folder does not retrigger;
cycles never overlap; cron and watch are exclusive; fallback status; watcher lifecycle (disable, delete, root removed).
"""
import os
import shutil
import sys
import tempfile
import threading
import time

TMP_WAREHOUSE = tempfile.mkdtemp(prefix="autoloader_watch_")
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
os.environ["AUTOLOADER_WATCH_DEBOUNCE"] = "0.5"
os.environ["AUTOLOADER_WATCH_MAX_WAIT"] = "4"
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from deltalake import DeltaTable
from web import autoloader, autoloader_watch, volumes

FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def wait_for(cond, timeout=8.0, step=0.1):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return cond()


def write_csv(path, rows, start=0):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("id,val\n" + "\n".join(f"{start + i},{i}" for i in range(rows)) + "\n")


def table_rows(table):
    p = os.path.join(TMP_WAREHOUSE, "dbo", table)
    return DeltaTable(p).to_pyarrow_table().num_rows if os.path.isdir(os.path.join(p, "_delta_log")) else 0


def pipe(name, table, **kw):
    volumes.create_volume("warehouse", "raw", name)
    d = volumes.resolve_volume_posix_path(f"/Volumes/warehouse/raw/{name}")
    p = autoloader.create_pipeline({"name": name, "source_volume_path": f"/Volumes/warehouse/raw/{name}", "file_pattern": "*.csv",
                                    "target_table": table, "watch_enabled": True, **kw})
    return p, d


def main():
    try:
        mgr = autoloader.get_watch_manager()
        cycles = []
        real_run = mgr._run_cycle

        def counting_run(pid):
            cycles.append(pid)
            return real_run(pid)
        mgr._run_cycle = counting_run

        print("1. Availability and configuration")
        check("inotify is available here", autoloader_watch.inotify_available())
        check("cron and watch are alternatives", _raises(lambda: autoloader.create_pipeline({"name": "x", "source_volume_path": "/Volumes/warehouse/raw/x", "target_table": "x", "watch_enabled": True, "cron_schedule": "*/5 * * * *"})))
        check("a sweep below 30s is clamped", autoloader._normalize_sweep(5) == 30 and autoloader._normalize_sweep(None) == 300)

        print("\n2. Event-driven ingestion (no timer, no daemon)")
        p, d = pipe("w1", "t_w1")
        check("the pipeline reports it is being watched", wait_for(lambda: autoloader.get_pipeline(p["id"])["watch"]["mode"] == "watching"), autoloader.get_pipeline(p["id"])["watch"])
        wait_for(lambda: len(cycles) >= 1)                          # the initial catch-up cycle
        base_cycles = len(cycles)
        t0 = time.time()
        write_csv(os.path.join(d, "a.csv"), 5)
        check("a dropped file is ingested by the event", wait_for(lambda: table_rows("t_w1") == 5), table_rows("t_w1"))
        check("...within a couple of seconds (debounce 0.5s), far below any poll interval", time.time() - t0 < 5, time.time() - t0)
        st = autoloader.get_pipeline(p["id"])["watch"]
        check("status shows events seen", st["events"] >= 1 and st["last_event_at"] and st["directories"] >= 1, st)

        print("\n3. Half-written files are never ingested")
        path = os.path.join(d, "big.csv")
        f = open(path, "w")
        f.write("id,val\n" + "\n".join(f"{100 + i},{i}" for i in range(50)) + "\n")
        f.flush()
        time.sleep(2.5)
        check("a file still open for writing triggers nothing", table_rows("t_w1") == 5)
        f.write("\n".join(f"{200 + i},{i}" for i in range(50)) + "\n")
        f.close()
        check("it is ingested once, complete, when the writer closes it", wait_for(lambda: table_rows("t_w1") == 105), table_rows("t_w1"))
        time.sleep(2)
        check("...and not again afterwards (exactly once)", table_rows("t_w1") == 105)
        write_csv(os.path.join(d, "up.csv.part"), 3, 500)
        time.sleep(1.5)
        check("a .part file is ignored", table_rows("t_w1") == 105)
        os.rename(os.path.join(d, "up.csv.part"), os.path.join(d, "up.csv"))
        check("renaming it into place ingests it", wait_for(lambda: table_rows("t_w1") == 108), table_rows("t_w1"))

        print("\n4. Filtering")
        n = len(cycles)
        write_csv(os.path.join(d, "notes.txt"), 3, 900)
        write_csv(os.path.join(d, ".hidden.csv"), 3, 910)
        write_csv(os.path.join(d, "x.tmp"), 3, 920)
        time.sleep(2)
        check("non-matching, hidden and .tmp files start no cycle", len(cycles) == n and table_rows("t_w1") == 108, (len(cycles) - n, table_rows("t_w1")))

        print("\n5. Sub-folders")
        write_csv(os.path.join(d, "2026", "09", "deep.csv"), 4, 1000)
        check("a sub-folder created after the watch started is watched", wait_for(lambda: table_rows("t_w1") == 112), table_rows("t_w1"))
        write_csv(os.path.join(d, "2026", "09", "deeper.csv"), 2, 1100)
        check("...including files added to it later", wait_for(lambda: table_rows("t_w1") == 114), table_rows("t_w1"))
        stage = os.path.join(TMP_WAREHOUSE, "staging_dir")
        write_csv(os.path.join(stage, "moved.csv"), 6, 2000)
        os.rename(stage, os.path.join(d, "moved_in"))
        check("a folder renamed in with files already inside is picked up", wait_for(lambda: table_rows("t_w1") == 120), table_rows("t_w1"))

        print("\n6. Bursts coalesce")
        n = len(cycles)
        for i in range(40):
            write_csv(os.path.join(d, f"burst_{i}.csv"), 1, 3000 + i)
        check("40 files in a burst are all ingested", wait_for(lambda: table_rows("t_w1") == 160, 20), table_rows("t_w1"))
        time.sleep(1.5)
        check("...by far fewer than 40 cycles", 1 <= len(cycles) - n <= 8, len(cycles) - n)

        print("\n7. Quarantine does not retrigger")
        n = len(cycles)
        with open(os.path.join(d, "corrupt.csv"), "wb") as fh:
            fh.write(b"\x00\x01\x02 not a csv \xff\xfe")
        wait_for(lambda: os.path.isdir(os.path.join(d, "_quarantine")) and os.listdir(os.path.join(d, "_quarantine")), 10)
        time.sleep(3)
        check("the bad file is quarantined by one cycle, not a loop", os.path.isdir(os.path.join(d, "_quarantine")) and 1 <= len(cycles) - n <= 2, len(cycles) - n)

        print("\n8. Cycles never overlap")
        overlap, active = [], []
        orig_impl = autoloader._run_pipeline_cycle_impl

        def slow_impl(pid):
            active.append(1)
            if len(active) > 1:
                overlap.append(1)
            time.sleep(1.2)
            try:
                return orig_impl(pid)
            finally:
                active.pop()
        autoloader._run_pipeline_cycle_impl = slow_impl
        try:
            r = []
            ts = [threading.Thread(target=lambda: r.append(autoloader.run_pipeline_cycle(p["id"]))) for _ in range(3)]
            [t.start() for t in ts]; [t.join() for t in ts]
        finally:
            autoloader._run_pipeline_cycle_impl = orig_impl
        check("three simultaneous triggers run one cycle and skip the others", sum(1 for x in r if x.get("skipped")) == 2 and not overlap, (r, overlap))

        print("\n9. Catch-up of files that arrived while nobody was watching")
        autoloader.update_pipeline(p["id"], {"watch_enabled": False})
        check("switching watching off stops the watcher", wait_for(lambda: not mgr.is_watching(p["id"])) and autoloader.get_pipeline(p["id"])["watch"]["mode"] == "off")
        write_csv(os.path.join(d, "offline.csv"), 7, 5000)
        time.sleep(2)
        check("...and nothing is ingested without a trigger", table_rows("t_w1") == 160)
        autoloader.update_pipeline(p["id"], {"watch_enabled": True})
        check("re-enabling it runs an immediate catch-up cycle", wait_for(lambda: table_rows("t_w1") == 167), table_rows("t_w1"))

        print("\n10. Lifecycle and fallback")
        shutil.rmtree(d)
        check("removing the watched folder marks the watcher dead", wait_for(lambda: mgr._watchers[p["id"]].dead if p["id"] in mgr._watchers else True))
        autoloader.sync_watchers()
        check("sync re-creates the folder and the watcher", wait_for(lambda: (autoloader.sync_watchers() or True) and mgr.is_watching(p["id"])) and os.path.isdir(d))
        write_csv(os.path.join(d, "after.csv"), 2, 6000)
        check("ingestion works again after that", wait_for(lambda: table_rows("t_w1") == 169), table_rows("t_w1"))
        real = autoloader_watch.inotify_available
        autoloader_watch.inotify_available = lambda: False
        try:
            p2, d2 = pipe("w2", "t_w2")
            st = autoloader.get_pipeline(p2["id"])["watch"]
        finally:
            autoloader_watch.inotify_available = real
        check("without inotify a watch pipeline reports fallback (it keeps polling)", st["mode"] == "fallback" and "inotify" in (st.get("detail") or "").lower(), st)
        autoloader.delete_pipeline(p["id"])
        check("deleting a pipeline stops its watcher", wait_for(lambda: p["id"] not in mgr._watchers))
        mgr.stop()
        time.sleep(0.6)
        leftover = [t.name for t in threading.enumerate() if t.name.startswith("autoloader-watch")]
        check("no watcher threads are left after stop()", not leftover, leftover)
    finally:
        shutil.rmtree(TMP_WAREHOUSE, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Auto-Loader file-watch checks passed.")


def _raises(fn):
    try:
        fn()
    except ValueError:
        return True
    return False


if __name__ == "__main__":
    main()
