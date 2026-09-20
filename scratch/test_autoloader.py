#!/usr/bin/env python3
"""
Verification script for the Volume Auto-Loader (web/volumes.py + web/autoloader.py).
Runs against a throwaway WAREHOUSE_DIR, so it never touches real data.
Tests:
1. File fingerprinting and checkpoint state transitions.
2. Ingestion: 10 CSV files -> row count and Delta version increments, idempotent re-run.
3. Schema evolution: a CSV with 2 extra columns adds columns without data loss.
4. Quarantine: a corrupt file is routed to _quarantine/ and later files still load.
5. Reset checkpoints re-ingests files.
"""

import os
import sys
import time
import shutil
import tempfile

TMP_WAREHOUSE = tempfile.mkdtemp(prefix="autoloader_test_")
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from deltalake import DeltaTable
from web import volumes, autoloader

FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def drop_csv(vol_dir, name, rows, extra_cols=False, mtime=None):
    header = "device_id,temperature" + (",humidity,site" if extra_cols else "")
    lines = [header] + [f"dev_{name}_{i},{20 + i}" + (f",{40 + i},plant_a" if extra_cols else "") for i in range(rows)]
    path = os.path.join(vol_dir, name)
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    if mtime:
        os.utime(path, (mtime, mtime))
    return path


def make_pipeline(vol_name, table, **overrides):
    volumes.create_volume("warehouse", "raw", vol_name)
    vol_dir = volumes.resolve_volume_posix_path(f"/Volumes/warehouse/raw/{vol_name}")
    pipe = autoloader.create_pipeline({
        "name": f"test_{vol_name}",
        "source_volume_path": f"/Volumes/warehouse/raw/{vol_name}",
        "file_pattern": "*",
        "target_table": table,
        "ingest_mode": "append",
        "schema_evolution": "addNewColumns",
        **overrides,
    })
    return pipe, vol_dir


def delta_path(table):
    return os.path.join(TMP_WAREHOUSE, "dbo", table)


def test_fingerprint():
    print("\n1. Fingerprinting")
    d = tempfile.mkdtemp(dir=TMP_WAREHOUSE)
    a = drop_csv(d, "a.csv", 3, mtime=1_700_000_000)
    fp1 = autoloader.compute_file_fingerprint(a)
    check("fingerprint is deterministic", fp1 == autoloader.compute_file_fingerprint(a))
    with open(a, "a") as f:
        f.write("dev_new,99\n")
    check("fingerprint changes when content changes", fp1 != autoloader.compute_file_fingerprint(a))
    b = drop_csv(d, "b.csv", 3, mtime=1_700_000_000)
    c = drop_csv(d, "c.csv", 3, mtime=1_700_000_000)
    check("same-size, same-mtime files with different content differ",
          autoloader.compute_file_fingerprint(b) != autoloader.compute_file_fingerprint(c))


def test_ingest_and_idempotency():
    print("\n2. Ingestion of 10 CSV files")
    pipe, vol_dir = make_pipeline("ingest_vol", "bronze_ingest")
    now = time.time()
    for i in range(10):
        drop_csv(vol_dir, f"batch_{i:02d}.csv", 5, mtime=now - 100 + i)

    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("10 files ingested", res.get("files_ingested") == 10, res)
    check("50 rows reported", res.get("rows_ingested") == 50, res)

    dt = DeltaTable(delta_path("bronze_ingest"))
    check("target table holds 50 rows", dt.to_pyarrow_table().num_rows == 50)
    check("one Delta commit per file (version 9)", dt.version() == 9, f"version={dt.version()}")

    res2 = autoloader.run_pipeline_cycle(pipe["id"])
    check("re-run ingests nothing (exactly-once)", res2.get("files_ingested") == 0, res2)
    check("table version unchanged after re-run", DeltaTable(delta_path("bronze_ingest")).version() == 9)

    hist = autoloader.get_pipeline_history(pipe["id"], limit=50)
    check("history records 10 SUCCESS rows", len(hist) == 10 and all(h["status"] == "SUCCESS" for h in hist))
    stored = autoloader.get_pipeline(pipe["id"])
    check("pipeline counters updated", stored["total_files_ingested"] == 10 and stored["total_rows_ingested"] == 50)

    print("\n5. Reset checkpoints")
    autoloader.reset_pipeline_checkpoints(pipe["id"])
    res3 = autoloader.run_pipeline_cycle(pipe["id"])
    check("reset makes all files eligible again", res3.get("files_ingested") == 10, res3)


def test_schema_evolution():
    print("\n3. Schema evolution")
    pipe, vol_dir = make_pipeline("evol_vol", "bronze_evol")
    now = time.time()
    drop_csv(vol_dir, "v1.csv", 4, mtime=now - 10)
    autoloader.run_pipeline_cycle(pipe["id"])
    drop_csv(vol_dir, "v2.csv", 3, extra_cols=True, mtime=now)
    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("file with new columns ingested", res.get("files_ingested") == 1, res)

    tbl = DeltaTable(delta_path("bronze_evol")).to_pyarrow_table()
    check("new columns added to the table", {"humidity", "site"} <= set(tbl.column_names), tbl.column_names)
    check("no data lost (7 rows)", tbl.num_rows == 7, tbl.num_rows)
    old_rows = [r for r in tbl.column("device_id").to_pylist() if r.startswith("dev_v1")]
    check("old rows kept with NULL in new columns",
          len(old_rows) == 4 and tbl.column("humidity").null_count == 4)


def test_quarantine():
    print("\n4. Quarantine of corrupt files")
    pipe, vol_dir = make_pipeline("quar_vol", "bronze_quar")
    now = time.time()
    bad = os.path.join(vol_dir, "corrupt.parquet")
    with open(bad, "wb") as f:
        f.write(b"this is definitely not a parquet file")
    os.utime(bad, (now - 20, now - 20))
    drop_csv(vol_dir, "good_after.csv", 6, mtime=now)

    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("corrupt file quarantined", res.get("files_quarantined") == 1, res)
    check("pipeline continued with the healthy file", res.get("files_ingested") == 1, res)
    check("corrupt file moved out of the volume root", not os.path.exists(bad))
    qdir = os.path.join(vol_dir, "_quarantine")
    check("file present in _quarantine/", os.path.isdir(qdir) and len(os.listdir(qdir)) == 1)
    check("healthy rows landed in the table", DeltaTable(delta_path("bronze_quar")).to_pyarrow_table().num_rows == 6)

    statuses = {h["status"] for h in autoloader.get_pipeline_history(pipe["id"])}
    check("history has QUARANTINED and SUCCESS", statuses == {"QUARANTINED", "SUCCESS"}, statuses)

    res2 = autoloader.run_pipeline_cycle(pipe["id"])
    check("quarantined file is not rescanned", res2.get("files_quarantined") == 0 and res2.get("files_ingested") == 0, res2)


def test_fail_on_new_columns():
    print("\n6. failOnNewColumns policy")
    pipe, vol_dir = make_pipeline("fail_vol", "bronze_fail", schema_evolution="failOnNewColumns")
    now = time.time()
    drop_csv(vol_dir, "ok1.csv", 4, mtime=now - 20)
    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("matching-schema file ingested", res.get("files_ingested") == 1, res)
    drop_csv(vol_dir, "drift.csv", 3, extra_cols=True, mtime=now - 10)
    drop_csv(vol_dir, "ok2.csv", 2, mtime=now)
    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("drifted file is not ingested", res.get("files_ingested") == 1, res)
    tbl = DeltaTable(delta_path("bronze_fail")).to_pyarrow_table()
    check("target schema unchanged and 6 rows", set(tbl.column_names) == {"device_id", "temperature"} and tbl.num_rows == 6)
    failed = [h for h in autoloader.get_pipeline_history(pipe["id"]) if h["status"] == "FAILED"]
    check("drifted file flagged FAILED with a schema-mismatch reason",
          len(failed) == 1 and "Schema mismatch" in (failed[0]["error_message"] or ""), failed)
    check("UI alias 'fail' accepted", autoloader.normalize_schema_evolution("fail") == "failOnNewColumns")
    try:
        autoloader.normalize_schema_evolution("bogus")
        check("unknown policy rejected", False)
    except ValueError:
        check("unknown policy rejected", True)


def test_rescue():
    print("\n7. rescue policy")
    pipe, vol_dir = make_pipeline("rescue_vol", "bronze_rescue", schema_evolution="rescue")
    now = time.time()
    drop_csv(vol_dir, "r1.csv", 3, mtime=now - 10)
    autoloader.run_pipeline_cycle(pipe["id"])
    drop_csv(vol_dir, "r2.csv", 2, extra_cols=True, mtime=now)
    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("file with unknown columns ingested", res.get("files_ingested") == 1, res)
    tbl = DeltaTable(delta_path("bronze_rescue")).to_pyarrow_table()
    check("schema not widened", set(tbl.column_names) == {"device_id", "temperature", "_rescued_data"}, tbl.column_names)
    check("5 rows total", tbl.num_rows == 5)
    rescued = [r for r in tbl.column("_rescued_data").to_pylist() if r]
    check("unknown columns captured as JSON for 2 rows", len(rescued) == 2 and '"site": "plant_a"' in rescued[0], rescued)
    check("clean rows have NULL _rescued_data", tbl.column("_rescued_data").null_count == 3)


def test_streaming_large_file():
    print("\n8. Streaming ingestion")
    autoloader.BATCH_ROWS = 1000
    pipe, vol_dir = make_pipeline("stream_vol", "bronze_stream")
    now = time.time()
    drop_csv(vol_dir, "big.csv", 10_500, mtime=now - 10)
    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("large file ingested", res.get("rows_ingested") == 10_500, res)
    dt = DeltaTable(delta_path("bronze_stream"))
    check("all rows landed in one atomic Delta commit", dt.to_pyarrow_table().num_rows == 10_500 and dt.version() == 0,
          f"version={dt.version()}")

    # Bad value far past the CSV sniffing sample: fails mid-stream, must leave the table untouched.
    path = drop_csv(vol_dir, "midbad.csv", 30_000, mtime=now)
    with open(path, "a") as f:
        f.write("dev_bad,not_a_number\n")
    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("mid-stream corrupt file quarantined", res.get("files_quarantined") == 1, res)
    check("no partial rows committed", DeltaTable(delta_path("bronze_stream")).to_pyarrow_table().num_rows == 10_500)
    check("bad file moved to _quarantine/", not os.path.exists(path))

    weird = drop_csv(vol_dir, "o'brien.csv", 2, mtime=now + 1)
    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("file names containing quotes are ingested", res.get("files_ingested") == 1, res)
    autoloader.BATCH_ROWS = int(os.getenv("AUTOLOADER_BATCH_ROWS", "100000"))


def test_merge_mode():
    print("\n9. Merge (upsert) mode")
    pipe, vol_dir = make_pipeline("merge_vol", "bronze_merge", ingest_mode="merge", merge_keys="device_id")
    now = time.time()
    with open(os.path.join(vol_dir, "m1.csv"), "w") as f:
        f.write("device_id,temperature\na,1\nb,2\n")
    os.utime(os.path.join(vol_dir, "m1.csv"), (now - 10, now - 10))
    autoloader.run_pipeline_cycle(pipe["id"])
    with open(os.path.join(vol_dir, "m2.csv"), "w") as f:
        f.write("device_id,temperature\nb,20\nc,3\n")
    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("upsert file ingested", res.get("files_ingested") == 1, res)
    rows = {r["device_id"]: r["temperature"] for r in DeltaTable(delta_path("bronze_merge")).to_pyarrow_table().to_pylist()}
    check("existing key updated and new key inserted", rows == {"a": 1, "b": 20, "c": 3}, rows)


def test_merge_key_safety():
    print("\n10. Merge key validation")
    for bad in (dict(ingest_mode="merge"), dict(ingest_mode="merge", merge_keys="  ,"), dict(ingest_mode="upsert")):
        try:
            make_pipeline(f"badcfg_{abs(hash(str(bad)))}", "t_badcfg", **bad)
            check(f"rejects config {bad}", False)
        except ValueError:
            check(f"rejects config {bad}", True)

    pipe, vol_dir = make_pipeline("mkey_vol", "bronze_mkey", ingest_mode="merge", merge_keys="DEVICE_ID")
    now = time.time()
    with open(os.path.join(vol_dir, "k1.csv"), "w") as f:
        f.write("device_id,temperature\na,1\nb,2\n")
    os.utime(os.path.join(vol_dir, "k1.csv"), (now - 20, now - 20))
    autoloader.run_pipeline_cycle(pipe["id"])
    with open(os.path.join(vol_dir, "k2.csv"), "w") as f:
        f.write("device_id,temperature,humidity\nb,20,55\nc,3,60\n")
    os.utime(os.path.join(vol_dir, "k2.csv"), (now - 10, now - 10))
    res = autoloader.run_pipeline_cycle(pipe["id"])
    tbl = DeltaTable(delta_path("bronze_mkey")).to_pyarrow_table()
    check("case-insensitive key + new column merged in one file", res.get("files_ingested") == 1 and "humidity" in tbl.column_names, res)
    check("upsert result correct", {r["device_id"]: r["temperature"] for r in tbl.to_pylist()} == {"a": 1, "b": 20, "c": 3})

    autoloader.update_pipeline(pipe["id"], {"merge_keys": "device_id = source.device_id OR 1=1 --"})
    with open(os.path.join(vol_dir, "k3.csv"), "w") as f:
        f.write("device_id,temperature,humidity\na,99,1\n")
    res = autoloader.run_pipeline_cycle(pipe["id"])
    failed = [h for h in autoloader.get_pipeline_history(pipe["id"]) if h["status"] == "FAILED"]
    check("injection-style key rejected as a non-column", res.get("files_ingested") == 0 and len(failed) == 1, res)
    tbl = DeltaTable(delta_path("bronze_mkey")).to_pyarrow_table()
    check("table untouched by rejected key", {r["device_id"]: r["temperature"] for r in tbl.to_pylist()}["a"] == 1)

    pipe2, vol2 = make_pipeline("mkey_space_vol", "bronze_mkey2", ingest_mode="merge", merge_keys="my id")
    for i, body in enumerate(["my id,val\n1,x\n2,y\n", "my id,val\n2,z\n"]):
        path = os.path.join(vol2, f"s{i}.csv")
        with open(path, "w") as f:
            f.write(body)
        os.utime(path, (now - 10 + i, now - 10 + i))
        autoloader.run_pipeline_cycle(pipe2["id"])
    rows = {r["my id"]: r["val"] for r in DeltaTable(delta_path("bronze_mkey2")).to_pyarrow_table().to_pylist()}
    check("keys with spaces are quoted correctly", rows == {1: "x", 2: "z"}, rows)


def test_cron_schedule():
    print("\n11. Cron scheduling")
    from datetime import datetime
    import sqlite3
    check("cron normalized", autoloader.normalize_cron("  */15   * * * * ") == "*/15 * * * *")
    check("empty cron -> None", autoloader.normalize_cron("  ") is None)
    for bad in ("not a cron", "* * * *", "61 * * * *"):
        try:
            autoloader.normalize_cron(bad)
            check(f"rejects '{bad}'", False)
        except ValueError:
            check(f"rejects '{bad}'", True)

    last = "2026-09-20 10:00:00"
    check("not due before next tick", not autoloader.cron_is_due("*/15 * * * *", last, datetime(2026, 9, 20, 10, 14, 59)))
    check("due at next tick", autoloader.cron_is_due("*/15 * * * *", last, datetime(2026, 9, 20, 10, 15, 0)))
    check("missed ticks catch up once", autoloader.cron_is_due("0 * * * *", last, datetime(2026, 9, 20, 15, 0, 0)))

    pipe, _ = make_pipeline("cron_vol", "bronze_cron", cron_schedule="0 2 * * *")
    check("cron stored on the pipeline", pipe["cron_schedule"] == "0 2 * * *")
    upd = autoloader.update_pipeline(pipe["id"], {"name": "renamed"})
    check("update without cron keeps it", upd["cron_schedule"] == "0 2 * * *")
    upd = autoloader.update_pipeline(pipe["id"], {"cron_schedule": ""})
    check("empty cron switches back to interval polling", upd["cron_schedule"] is None)
    try:
        autoloader.update_pipeline(pipe["id"], {"cron_schedule": "nope"})
        check("update rejects invalid cron", False)
    except ValueError:
        check("update rejects invalid cron", True)

    # Databases created before cron support get the column added in place
    legacy_path = os.path.join(TMP_WAREHOUSE, "legacy_autoloader.db")
    legacy = sqlite3.connect(legacy_path)
    legacy.execute("CREATE TABLE autoloader_pipelines (id TEXT PRIMARY KEY, name TEXT)")
    legacy.commit()
    legacy.close()
    real_path = autoloader.DB_PATH
    autoloader.DB_PATH = legacy_path
    try:
        autoloader.init_autoloader_db()
        conn = sqlite3.connect(legacy_path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(autoloader_pipelines)")}
        conn.close()
    finally:
        autoloader.DB_PATH = real_path
    check("legacy table migrated with cron_schedule column", "cron_schedule" in cols, cols)


def main():
    autoloader.init_autoloader_db()
    try:
        test_fingerprint()
        test_ingest_and_idempotency()
        test_schema_evolution()
        test_quarantine()
        test_fail_on_new_columns()
        test_rescue()
        test_streaming_large_file()
        test_merge_mode()
        test_merge_key_safety()
        test_cron_schedule()
    finally:
        shutil.rmtree(TMP_WAREHOUSE, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Auto-Loader checks passed.")


if __name__ == "__main__":
    main()
