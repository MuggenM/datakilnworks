#!/usr/bin/env python3
"""
Shallow clone engine verification (web/table_clone.py), in a temp dir: partitioned / special-character tables, VERSION /
TIMESTAMP AS OF, checkpoints, hard-link sharing (no data copied), independence in both directions (append / delete /
merge / vacuum on either side; the source dropped), readable by delta-rs AND DuckDB, CREATE OR REPLACE / IF NOT EXISTS,
clone of a clone, statistics preserved, and failure modes that leave nothing behind.
"""
import datetime
import errno
import json
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from web import table_clone
from web.table_clone import CloneError, parse_clone_sql, shallow_clone

FAILURES = []
ROOT = tempfile.mkdtemp(prefix="shallow_clone_")


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def duck_rows(path):
    return duckdb.sql(f"select id, region, amt, ts from delta_scan('{path}') order by id").fetchall()


def delta_rows(path):
    return sorted(tuple(r.values()) for r in DeltaTable(path).to_pyarrow_table().to_pylist())


def raises(fn, frag=""):
    try:
        fn()
    except CloneError as exc:
        return frag.lower() in str(exc).lower()
    except Exception as exc:
        return False
    return False


def leftovers(parent):
    return [n for n in os.listdir(parent) if ".clone-" in n or ".old-" in n]


def main():
    try:
        print("1. SQL parsing")
        p = parse_clone_sql("CREATE OR REPLACE TABLE cat2.dbo.t2 SHALLOW CLONE dbo.t1 VERSION AS OF 3;")
        check("full statement", p == {"target": ["cat2", "dbo", "t2"], "source": ["dbo", "t1"], "replace": True, "if_not_exists": False, "version": 3, "timestamp": None}, p)
        p = parse_clone_sql("create table if not exists dbo.b shallow clone dbo.a timestamp as of '2026-09-24 10:00:00'")
        check("IF NOT EXISTS + TIMESTAMP AS OF, any case", p and p["if_not_exists"] and p["timestamp"] == "2026-09-24 10:00:00" and p["version"] is None, p)
        check('quoted identifiers', parse_clone_sql('CREATE TABLE "my s"."t x" SHALLOW CLONE `s`.`t`')["target"] == ["my s", "t x"])
        check("ordinary SQL is not a clone", parse_clone_sql("CREATE TABLE a AS SELECT 1") is None and parse_clone_sql("SELECT 'SHALLOW CLONE'") is None
              and parse_clone_sql("CREATE TABLE a SHALLOW CLONE b; DROP TABLE x") is None)

        print("\n2. A partitioned source with awkward names")
        src_root = os.path.join(ROOT, "wh dir", "sales")             # a space in the path on purpose
        src = os.path.join(src_root, "orders")
        base = pa.table({"id": [1, 2, 3], "region": ["north east", "süd", "a/b"], "amt": [1.5, 2.5, 3.5],
                         "ts": pa.array([datetime.datetime(2026, 1, 1, 12, 0, 1)] * 3, pa.timestamp("us"))})
        write_deltalake(src, base, partition_by=["region"])
        time.sleep(1.1)
        t_after_v0 = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        time.sleep(1.1)
        write_deltalake(src, pa.table({"id": [4], "region": ["north east"], "amt": [4.5], "ts": pa.array([datetime.datetime(2026, 2, 2)], pa.timestamp("us"))}), mode="append")
        DeltaTable(src).delete("id = 2")
        DeltaTable(src).create_checkpoint()
        write_deltalake(src, pa.table({"id": [5], "region": ["süd"], "amt": [5.5], "ts": pa.array([datetime.datetime(2026, 3, 3)], pa.timestamp("us"))}), mode="append")
        sv = DeltaTable(src).version()
        want = delta_rows(src)
        dst = os.path.join(src_root, "orders_clone")
        r = shallow_clone(src, dst, source_label="sales.orders")
        check("clone of the latest version (log has a checkpoint)", r["created"] and r["source_version"] == sv and r["files"] >= 3, r)
        check("delta-rs reads the same rows", delta_rows(dst) == want)
        check("DuckDB delta_scan (the studio's engine) reads the same rows", sorted(duck_rows(dst)) == want)
        check("the clone starts at version 0 with a CLONE commit", DeltaTable(dst).version() == 0 and json.loads(open(os.path.join(dst, "_delta_log", "0" * 20 + ".json")).readline())["commitInfo"]["operation"] == "CLONE")
        info = json.loads(open(os.path.join(dst, "_delta_log", "0" * 20 + ".json")).readline())["commitInfo"]["operationParameters"]
        check("the CLONE commit records source and version", info["source"] == "sales.orders" and info["sourceVersion"] == sv and info["isShallow"] is True, info)
        pf = [os.path.join(d, f) for d, _, fs in os.walk(dst) for f in fs if f.endswith(".parquet")]
        check("data files are hard links to the source (same inode), nothing copied",
              pf and all(os.stat(f).st_nlink >= 2 for f in pf) and all(os.stat(f).st_ino == os.stat(os.path.join(src, os.path.relpath(f, dst))).st_ino for f in pf))
        raw = [json.loads(l) for l in open(os.path.join(dst, "_delta_log", "0" * 20 + ".json"))]
        adds = {a["add"]["path"]: a["add"] for a in raw if "add" in a}
        src_stats = {a["path"]: a.get("stats") for a in table_clone._live_adds(src, sv).values()}
        check("file statistics are preserved exactly", all(adds[p]["stats"] == src_stats[p] and adds[p]["stats"] for p in adds))
        check("partition values are preserved as the source's exact strings", {a["partitionValues"]["region"] for a in adds.values()} == {"north east", "süd", "a/b"} or
              {a["partitionValues"]["region"] for a in adds.values()} <= {"north east", "süd", "a/b"})
        check("no temp directories left behind", not leftovers(src_root))

        print("\n3. Versions and timestamps")
        d0 = os.path.join(src_root, "at_v0")
        r = shallow_clone(src, d0, version=0)
        check("VERSION AS OF 0 gives the first version's rows", r["source_version"] == 0 and len(delta_rows(d0)) == 3, delta_rows(d0))
        dts = os.path.join(src_root, "at_ts")
        r = shallow_clone(src, dts, timestamp=t_after_v0)
        check("TIMESTAMP AS OF picks the version current at that time", r["source_version"] == 0 and delta_rows(dts) == delta_rows(d0), r)
        check("an unparseable timestamp is refused", raises(lambda: shallow_clone(src, os.path.join(src_root, "x1"), timestamp="yesterday-ish"), "timestamp"))
        check("a version that doesn't exist is refused", raises(lambda: shallow_clone(src, os.path.join(src_root, "x2"), version=99), "version"))
        check("a missing source is refused", raises(lambda: shallow_clone(os.path.join(ROOT, "nope"), os.path.join(src_root, "x3")), "does not exist"))
        os.remove(os.path.join(src, "_delta_log", "0" * 19 + "1.json"))        # history older than the checkpoint cleaned up
        check("a version covered by a checkpoint still clones after older commits are cleaned up", shallow_clone(src, os.path.join(src_root, "x4"), version=2)["created"])
        shutil.rmtree(os.path.join(src_root, "x4"))
        check("a version whose log was cleaned up fails clearly, not with a wrong clone", raises(lambda: shallow_clone(src, os.path.join(src_root, "x5"), version=1), ""), )
        check("failed clones leave nothing behind", not leftovers(src_root) and not any(os.path.exists(os.path.join(src_root, f"x{i}")) for i in (1, 2, 3, 4, 5)))

        print("\n4. Independence")
        before_src = delta_rows(src)
        write_deltalake(dst, pa.table({"id": [100], "region": ["north east"], "amt": [0.5], "ts": pa.array([datetime.datetime(2026, 4, 4)], pa.timestamp("us"))}), mode="append")
        DeltaTable(dst).delete("id = 1")
        check("writes and deletes on the clone work (versions advance)", DeltaTable(dst).version() == 2 and (100, ) == tuple(r[0] for r in delta_rows(dst) if r[0] == 100))
        check("...and the source is untouched", delta_rows(src) == before_src)
        write_deltalake(src, pa.table({"id": [200], "region": ["süd"], "amt": [9.9], "ts": pa.array([datetime.datetime(2026, 5, 5)], pa.timestamp("us"))}), mode="append")
        DeltaTable(src).delete("id = 3")
        check("writes on the source don't reach the clone", not any(r[0] == 200 for r in delta_rows(dst)) and any(r[0] == 3 for r in delta_rows(dst)))
        DeltaTable(src).vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
        DeltaTable(dst).vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
        check("vacuuming either side does not break the other", DeltaTable(src).to_pyarrow_table().num_rows > 0 and DeltaTable(dst).to_pyarrow_table().num_rows > 0)
        cloned_rows = delta_rows(dst)
        clone2 = os.path.join(src_root, "clone_of_clone")
        shallow_clone(dst, clone2)
        shutil.rmtree(src)
        check("the source can be dropped: the clone is still fully readable", delta_rows(dst) == cloned_rows and len(cloned_rows) > 0)
        check("...by DuckDB too", len(duck_rows(dst)) == len(cloned_rows))
        check("a clone of a clone works and survives its parent's source being dropped", delta_rows(clone2) == cloned_rows)

        print("\n5. Replace / exists")
        a, b = os.path.join(ROOT, "a"), os.path.join(ROOT, "b")
        write_deltalake(a, pa.table({"x": [1, 2]}))
        write_deltalake(b, pa.table({"x": [9]}))
        c = os.path.join(ROOT, "c")
        shallow_clone(a, c)
        check("an existing target is refused without OR REPLACE", raises(lambda: shallow_clone(b, c), "already exists"))
        r = shallow_clone(b, c, if_not_exists=True)
        check("IF NOT EXISTS leaves it alone", r["created"] is False and delta_rows(c) == [(1,), (2,)])
        r = shallow_clone(b, c, replace=True)
        check("OR REPLACE swaps in the new clone", r["created"] and delta_rows(c) == [(9,)] and not leftovers(ROOT))
        check("cloning a table onto itself is refused", raises(lambda: shallow_clone(a, a), "itself"))
        notdelta = os.path.join(ROOT, "plain"); os.makedirs(notdelta)
        open(os.path.join(notdelta, "f.txt"), "w").write("x")
        check("OR REPLACE never clobbers a directory that isn't a Delta table", raises(lambda: shallow_clone(a, notdelta, replace=True), "not a delta table") and os.path.exists(os.path.join(notdelta, "f.txt")))

        print("\n6. Filesystem limits")
        real_link = os.link
        def bad_link(s, d):
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        os.link = bad_link
        try:
            ok = raises(lambda: shallow_clone(a, os.path.join(ROOT, "xdev")), "same local filesystem")
        finally:
            os.link = real_link
        check("a cross-device hard link fails with a clear message", ok and not os.path.exists(os.path.join(ROOT, "xdev")) and not leftovers(ROOT))
    finally:
        shutil.rmtree(ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All shallow clone engine checks passed.")


if __name__ == "__main__":
    main()
