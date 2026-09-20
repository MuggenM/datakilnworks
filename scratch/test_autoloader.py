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


def main():
    autoloader.init_autoloader_db()
    try:
        test_fingerprint()
        test_ingest_and_idempotency()
        test_schema_evolution()
        test_quarantine()
    finally:
        shutil.rmtree(TMP_WAREHOUSE, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Auto-Loader checks passed.")


if __name__ == "__main__":
    main()
