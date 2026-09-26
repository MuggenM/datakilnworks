#!/usr/bin/env python3
"""
S3 sources for Auto-Loader (web/autoloader_s3.py) against an in-process moto S3 server (a real S3 HTTP API; boto3 and
DuckDB httpfs both talk to it). It is a mock, not Garage/MinIO/AWS: provider quirks are not covered. Needs
`pip install "moto[server]"` (test-only); skips cleanly without it. Throwaway WAREHOUSE_DIR, never a real bucket.
Tests: validation; discovery filters; exactly-once (same bytes not reloaded, replaced object reloaded); csv/json/parquet;
schema evolution; pagination; quarantine (copy + delete, and when delete is refused); access/network errors are FAILED
(retried) not quarantined; unreachable endpoint / missing bucket / missing mount surface as pipeline errors; mount
selection; lineage node; file events refused for S3.
"""
import io
import json
import os
import shutil
import sys
import tempfile
import time

TMP = tempfile.mkdtemp(prefix="autoloader_s3_")
os.environ["WAREHOUSE_DIR"] = TMP
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from moto.server import ThreadedMotoServer
except ImportError:
    print('moto is not installed (pip install "moto[server]") -- skipping the S3 Auto-Loader test.')
    shutil.rmtree(TMP, ignore_errors=True)
    sys.exit(0)

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable

from web import autoloader, autoloader_s3, mounts

FAILURES = []
PORT = 18932
ENDPOINT = f"127.0.0.1:{PORT}"
BUCKET = "landing"


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def rows(table):
    p = os.path.join(TMP, "dbo", table)
    return DeltaTable(p).to_pyarrow_table().num_rows if os.path.isdir(os.path.join(p, "_delta_log")) else 0


def csv(n, start=0, extra=False):
    head = "id,val" + (",extra" if extra else "")
    return (head + "\n" + "\n".join(f"{start + i},{i}" + (",x" if extra else "") for i in range(n)) + "\n").encode()


def main():
    import logging
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    server = ThreadedMotoServer(port=PORT, verbose=False)
    server.start()
    try:
        s3 = boto3.client("s3", endpoint_url=f"http://{ENDPOINT}", aws_access_key_id="k", aws_secret_access_key="s", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        put = lambda key, body: s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        keys = lambda prefix="": sorted(o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []))

        def mount(mid="m1", bucket=BUCKET, endpoint=ENDPOINT):
            return {"id": mid, "type": "s3", "catalog_name": mid, "name": mid,
                    "config": {"bucket": bucket, "endpoint": endpoint, "key_id": "k", "secret": "s", "region": "us-east-1", "url_style": "path", "use_ssl": False}}

        def pipe(name, table, path, **kw):
            return autoloader.create_pipeline({"name": name, "source_volume_path": path, "file_pattern": "*.csv", "target_table": table, **kw})

        print("1. Validation and configuration")
        mounts.save_mounts([])
        p0 = pipe("nomount", "t_nomount", f"s3://{BUCKET}/in/")
        r = autoloader.run_pipeline_cycle(p0["id"])
        check("no S3 mount configured -> a clear pipeline error, not a crash", "error" in r and "mount" in r["error"].lower() and autoloader.get_pipeline(p0["id"])["status"] == "ERROR", r)
        mounts.save_mounts([mount()])
        check("bad s3 URLs are refused at creation", all(_raises(lambda u=u: pipe("bad", "t_bad", u)) for u in ("s3://", "s3:///x", "s3://b/../x")))
        check("file events are refused for S3 sources", _raises(lambda: pipe("w", "t_w", f"s3://{BUCKET}/x/", watch_enabled=True)))
        check("a storage mount only applies to s3:// sources", _raises(lambda: pipe("m", "t_m", "/Volumes/warehouse/raw/x", source_mount_id="m1")))
        p = pipe("main", "t_main", f"s3://{BUCKET}/in")
        check("the path is normalised with a trailing slash", p["source_volume_path"] == f"s3://{BUCKET}/in/")
        p_bad_mount = pipe("badmount", "t_bm", f"s3://{BUCKET}/in/", source_mount_id="nope")
        r = autoloader.run_pipeline_cycle(p_bad_mount["id"])
        check("an unknown mount id is reported", "nope" in r.get("error", ""), r)
        autoloader.delete_pipeline(p_bad_mount["id"]); autoloader.delete_pipeline(p0["id"])

        print("\n2. Discovery filters")
        put("in/a.csv", csv(3))
        put("in/sub/dir/b.csv", csv(4, 100))
        put("in/notes.txt", b"x")
        put("in/.hidden.csv", csv(2, 900))
        put("in/.h/c.csv", csv(2, 910))
        put("in/up.csv.part", csv(2, 920))
        put("in/x.tmp", csv(2, 930))
        put("in/_quarantine/old.csv", csv(2, 940))
        put("in/emptydir/", b"")
        put("other/z.csv", csv(2, 950))                            # outside the prefix
        found = [o.rel_key for o in autoloader_s3.list_objects(p)]
        check("only real, matching objects under the prefix are considered", found == ["a.csv", "sub/dir/b.csv"] or sorted(found) == ["a.csv", "sub/dir/b.csv"], found)

        print("\n3. Ingestion and exactly-once")
        r = autoloader.run_pipeline_cycle(p["id"])
        check("both objects are ingested from S3 (streamed, nothing downloaded)", r["files_ingested"] == 2 and rows("t_main") == 7, r)
        hist = {h["file_path"] for h in autoloader.get_pipeline_history(p["id"])}
        check("history records keys relative to the prefix", hist == {"a.csv", "sub/dir/b.csv"}, hist)
        r = autoloader.run_pipeline_cycle(p["id"])
        check("a second cycle loads nothing", r["files_ingested"] == 0 and rows("t_main") == 7, r)
        put("in/a.csv", csv(3))
        r = autoloader.run_pipeline_cycle(p["id"])
        check("re-uploading identical bytes is not reloaded (same ETag)", r["files_ingested"] == 0 and rows("t_main") == 7, r)
        put("in/a.csv", csv(5, 500))
        r = autoloader.run_pipeline_cycle(p["id"])
        check("a replaced object (new ETag) is loaded again", r["files_ingested"] == 1 and rows("t_main") == 12, (r, rows("t_main")))
        put("in/c.csv", csv(2, 700, extra=True))
        r = autoloader.run_pipeline_cycle(p["id"])
        check("schema evolution works (a new column appears)", r["files_ingested"] == 1 and "extra" in [f.name for f in DeltaTable(os.path.join(TMP, "dbo", "t_main")).schema().fields], r)
        st = autoloader.get_pipeline(p["id"])
        check("pipeline counters and status", st["status"] == "IDLE" and st["total_files_ingested"] == 4 and not st["last_error"], st)

        print("\n4. Other formats and modes")
        buf = io.BytesIO(); pq.write_table(pa.table({"id": [1, 2, 3], "val": [1, 2, 3]}), buf)
        put("pq/p1.parquet", buf.getvalue())
        put("pq/j1.json", json.dumps([{"id": 9, "val": 9}, {"id": 10, "val": 10}]).encode())
        pp = autoloader.create_pipeline({"name": "pq", "source_volume_path": f"s3://{BUCKET}/pq/", "file_pattern": "*", "target_table": "t_pq"})
        r = autoloader.run_pipeline_cycle(pp["id"])
        check("parquet and JSON objects are read", r["files_ingested"] == 2 and rows("t_pq") == 5, r)
        put("mg/1.csv", csv(3)); 
        pm = autoloader.create_pipeline({"name": "mg", "source_volume_path": f"s3://{BUCKET}/mg/", "file_pattern": "*.csv", "target_table": "t_mg", "ingest_mode": "merge", "merge_keys": "id"})
        autoloader.run_pipeline_cycle(pm["id"]); put("mg/2.csv", csv(3, 2))
        autoloader.run_pipeline_cycle(pm["id"])
        check("merge mode upserts on the key across S3 files", rows("t_mg") == 5, rows("t_mg"))

        print("\n5. Pagination")
        for i in range(1105):
            s3.put_object(Bucket=BUCKET, Key=f"big/f{i:04d}.csv", Body=b"id,val\n1,1\n")
        pb = autoloader.create_pipeline({"name": "big", "source_volume_path": f"s3://{BUCKET}/big/", "file_pattern": "*.csv", "target_table": "t_big"})
        check("more than 1000 objects are all listed (paginated)", len(autoloader_s3.list_objects(pb)) == 1105)
        autoloader.delete_pipeline(pb["id"])

        print("\n6. Quarantine")
        put("q/good1.csv", csv(3))
        put("q/bad.csv", b"\x00\x01 not a csv \xff\xfe")
        put("q/good2.csv", csv(2, 50))
        pq_ = autoloader.create_pipeline({"name": "q", "source_volume_path": f"s3://{BUCKET}/q/", "file_pattern": "*.csv", "target_table": "t_q"})
        r = autoloader.run_pipeline_cycle(pq_["id"])
        ks = keys("q/")
        check("the corrupt object is quarantined and the good ones still load", r["files_quarantined"] == 1 and r["files_ingested"] == 2 and rows("t_q") == 5, r)
        check("...it was copied under _quarantine/ and the original removed", "q/bad.csv" not in ks and any(k.startswith("q/_quarantine/bad.csv.") and k.endswith(".bad") for k in ks), ks)
        put("q2/bad2.csv", b"\x00\x01 \xff\xfe garbage")
        pq2 = autoloader.create_pipeline({"name": "q2", "source_volume_path": f"s3://{BUCKET}/q2/", "file_pattern": "*.csv", "target_table": "t_q2"})
        real_delete = boto3.client("s3").__class__
        orig = autoloader_s3.make_client

        def no_delete_client(conn):
            c = orig(conn)
            def refuse(**kw):
                raise Exception("AccessDenied: delete not allowed")
            c.delete_object = refuse
            return c
        autoloader_s3.make_client = no_delete_client
        try:
            r1 = autoloader.run_pipeline_cycle(pq2["id"])
            r2 = autoloader.run_pipeline_cycle(pq2["id"])
        finally:
            autoloader_s3.make_client = orig
        check("when the credentials cannot delete, the object stays but is recorded and never retried", r1["files_quarantined"] == 1 and "q2/bad2.csv" in keys("q2/") and r2["files_quarantined"] == 0 and r2["files_ingested"] == 0, (r1, r2))

        print("\n7. Access and network problems are not the object's fault")
        put("io/fine.csv", csv(4))
        pio = autoloader.create_pipeline({"name": "io", "source_volume_path": f"s3://{BUCKET}/io/", "file_pattern": "*.csv", "target_table": "t_io"})
        real_cfg = autoloader_s3.configure_duckdb

        def dead_endpoint(duck, conn):
            real_cfg(duck, dict(conn, endpoint="127.0.0.1:1"))
        autoloader_s3.configure_duckdb = dead_endpoint
        try:
            r = autoloader.run_pipeline_cycle(pio["id"])
        finally:
            autoloader_s3.configure_duckdb = real_cfg
        check("an unreachable endpoint mid-read FAILs the file (retried), never quarantines it", r["files_quarantined"] == 0 and r["details"][0]["status"] == "FAILED" and "io/fine.csv" in keys("io/"), r)
        check("the pipeline shows the error", autoloader.get_pipeline(pio["id"])["status"] == "ERROR" and autoloader.get_pipeline(pio["id"])["last_error"])
        r = autoloader.run_pipeline_cycle(pio["id"])
        check("the next cycle retries and succeeds, clearing the error", r["files_ingested"] == 1 and rows("t_io") == 4 and autoloader.get_pipeline(pio["id"])["status"] == "IDLE" and not autoloader.get_pipeline(pio["id"])["last_error"], r)
        mounts.save_mounts([mount(endpoint="127.0.0.1:1")])
        pdead = autoloader.create_pipeline({"name": "dead", "source_volume_path": f"s3://{BUCKET}/io/", "file_pattern": "*.csv", "target_table": "t_dead"})
        r = autoloader.run_pipeline_cycle(pdead["id"])
        check("an unreachable endpoint at listing time is a pipeline error", "Could not list" in r.get("error", "") and autoloader.get_pipeline(pdead["id"])["status"] == "ERROR", r)
        mounts.save_mounts([mount()])
        pnb = autoloader.create_pipeline({"name": "nobucket", "source_volume_path": "s3://no-such-bucket/x/", "file_pattern": "*.csv", "target_table": "t_nb"})
        r = autoloader.run_pipeline_cycle(pnb["id"])
        check("a missing bucket says so", "bucket does not exist" in r.get("error", ""), r)
        check("I/O-looking errors are told apart from malformed data", autoloader._looks_like_remote_io_error(Exception("IO Error: HTTP GET error 403")) and not autoloader._looks_like_remote_io_error(Exception("Invalid Input Error: CSV Error on Line: 1")))

        print("\n8. Mount selection and lineage")
        mounts.save_mounts([mount("other", bucket="elsewhere"), mount("m_landing", bucket=BUCKET)])
        check("a mount whose bucket matches is preferred", autoloader_s3.resolve_connection({"source_volume_path": f"s3://{BUCKET}/x/"})["mount"] == "m_landing")
        check("an explicit source_mount_id wins", autoloader_s3.resolve_connection({"source_volume_path": f"s3://{BUCKET}/x/", "source_mount_id": "other"})["mount"] == "other")
        from web import lineage
        check("the lineage node id for an S3 source", autoloader._volume_lineage_id(f"s3://{BUCKET}/in/") == f"volume:s3://{BUCKET}/in")
        check("sync_pipeline_lineage does not choke on an S3 source", autoloader.sync_pipeline_lineage(p) is None)
    finally:
        server.stop()
        shutil.rmtree(TMP, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Auto-Loader S3 checks passed.")


def _raises(fn):
    try:
        fn()
    except ValueError:
        return True
    return False


if __name__ == "__main__":
    main()
