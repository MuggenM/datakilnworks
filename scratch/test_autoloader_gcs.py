#!/usr/bin/env python3
"""Google Cloud Storage as an Auto-Loader source (web/autoloader_gcs.py, web/autoloader_preview.py, web/mounts.py "gcs" mount
type), throwaway WAREHOUSE_DIR, against a real throwaway Deuxfleurs Garage standing in for GCS's own S3-compatible
interoperability endpoint (see web/autoloader_gcs.py's module docstring for why; not real GCS, and not in `ci/plan.json` for
that reason: Google-specific quirks of the interoperability API beyond the ENDPOINT/SCOPE one below are unverified). Run on
the same docker network as Garage:
  eval "$(scratch/garage_up.sh)"   # prints/exports GARAGE_ENDPOINT, GARAGE_KEY_ID, GARAGE_SECRET_KEY
  docker run --rm --network <the network garage_up.sh used> -e TEST_S3_ENDPOINT="$GARAGE_ENDPOINT" -e TEST_S3_ACCESS_KEY="$GARAGE_KEY_ID" \
     -e TEST_S3_SECRET_KEY="$GARAGE_SECRET_KEY" -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch \
     localspark-lakehouse-notebook python /workspace/scratch/test_autoloader_gcs.py
Covers: mount resolution (explicit id, by-bucket match, first mount), discovery filters (hidden, .tmp/.part, _quarantine),
exactly-once via ETag, schema evolution (a new column), quarantine of a genuinely corrupt object, watch_enabled refused
(inotify has nothing to watch on GCS), the DuckDB ENDPOINT/SCOPE quirk documented in autoloader_gcs.py (a bucket-scoped
`gcs` secret honours a custom ENDPOINT; an unscoped one silently talks to real Google instead), and the preview endpoint.
"""
import io
import os
import shutil
import sys
import tempfile
import time

TMP = tempfile.mkdtemp(prefix="al_gcs_")
os.environ["WAREHOUSE_DIR"] = TMP
sys.path.insert(0, "/workspace")

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from web import autoloader, autoloader_gcs as gcs, autoloader_preview as pv, mounts

FAIL = []


def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c:
        FAIL.append(n)


def err(fn, *a, **k):
    try:
        fn(*a, **k)
        return None
    except pv.PreviewError as e:
        return str(e)


EP = os.environ.get("TEST_S3_ENDPOINT", "127.0.0.1:9000")
KEY = os.environ.get("TEST_S3_ACCESS_KEY", "")
SECRET = os.environ.get("TEST_S3_SECRET_KEY", "")
REGION = os.environ.get("TEST_S3_REGION", "garage")           # Garage's own s3_region; real GCS uses "auto"


def mount(mid, bucket, **kw):
    return {"id": mid, "type": "gcs", "catalog_name": mid, "name": mid,
            "config": {"bucket": bucket, "key_id": KEY, "secret": SECRET, "endpoint": EP, "region": REGION, "use_ssl": False, **kw}}


s3 = boto3.client("s3", endpoint_url=f"http://{EP}", aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=REGION)
for _ in range(30):
    try:
        s3.list_buckets()
        break
    except Exception:
        time.sleep(1)


def make_bucket(name):
    try:
        s3.create_bucket(Bucket=name)
    except Exception:
        pass


def put(bucket, key, body):
    s3.put_object(Bucket=bucket, Key=key, Body=body if isinstance(body, bytes) else body.encode())


try:
    make_bucket("landing")
    autoloader.init_autoloader_db()
    check("is_gcs_path recognises the scheme (not the more common gs://, DuckDB's own name for this backend)",
          gcs.is_gcs_path("gcs://landing/in/") and not gcs.is_gcs_path("gs://landing/in/") and not gcs.is_gcs_path("s3://x/y"))
    check("parse_gcs_path splits bucket/prefix", gcs.parse_gcs_path("gcs://landing/in/sub") == ("landing", "in/sub/"))
    check("normalize_path always ends the prefix with /", gcs.normalize_path("gcs://landing/in") == "gcs://landing/in/")

    mounts.save_mounts([])
    check("no mount configured: a clear message", "mount" in (err(pv.preview, "gcs://landing/in/", "*.csv") or "").lower())
    mounts.save_mounts([mount("gm1", "landing")])

    put("landing", "in/a.csv", "id,name,amount\n" + "\n".join(f"{i},n{i},{i * 1.5}" for i in range(20)) + "\n")
    time.sleep(1.1)
    put("landing", "in/b.csv", "id,name,amount\n" + "\n".join(f"{i},n{i},{i * 1.5}" for i in range(100, 120)) + "\n")
    put("landing", "in/.hidden.csv", "id\n1\n")
    put("landing", "in/x.csv.tmp", "id\n1\n")
    put("landing", "in/_quarantine/q.csv", "id\n1\n")
    put("landing", "in/sub/c.csv", "id,name,amount\n" + "\n".join(f"{i},n{i},{i * 1.5}" for i in range(900, 903)) + "\n")
    put("landing", "in/readme.md", "hi")

    r = pv.preview("gcs://landing/in/", "*.csv")
    check("a preview reads the oldest matching object in place, discovery filters applied",
          r["source"] == "gcs" and r["sample"] == "a.csv" and sorted(f["path"] for f in r["files"]) == ["a.csv", "b.csv", "sub/c.csv"], r)
    check("columns match what the loader would infer", [(c["name"], c["type"]) for c in r["columns"]] == [("id", "int64"), ("name", "string"), ("amount", "double")], r["columns"])
    r2 = pv.preview("gcs://landing/in/", "*.csv", None, "sub/c.csv")
    check("a specific object can be chosen", r2["sample"] == "sub/c.csv" and r2["rows"][0][0] == 900)
    check("an object outside the listing is refused", "not one of the files" in (err(pv.preview, "gcs://landing/in/", "*.csv", None, "../readme.md") or ""))

    p = autoloader.create_pipeline({"name": "gcs test", "source_volume_path": "gcs://landing/in/", "file_pattern": "*.csv",
                                    "target_catalog": "warehouse", "target_schema": "dbo", "target_table": "gcs_events"})
    check("the source is normalised and no watcher / mount error at creation", p["source_volume_path"] == "gcs://landing/in/")
    try:
        autoloader.create_pipeline({"name": "bad", "source_volume_path": "gcs://landing/in/", "watch_enabled": True, "target_table": "x"})
        ok = False
    except ValueError as e:
        ok = "polled" in str(e)
    check("file-event watching is refused for a GCS source (inotify has nothing to watch there)", ok)

    r1 = autoloader.run_pipeline_cycle(p["id"])
    check("first cycle ingests all matching objects (sub-folders included), oldest first, filters applied",
          r1.get("files_ingested") == 3 and r1.get("rows_ingested") == 43, r1)
    r2 = autoloader.run_pipeline_cycle(p["id"])
    check("exactly-once: a second cycle loads nothing new", r2.get("files_ingested") == 0, r2)

    put("landing", "in/c.csv", "id,name,amount,extra\n1,x,1.0,zeta\n")
    r3 = autoloader.run_pipeline_cycle(p["id"])
    check("a new object with an extra column evolves the schema (addNewColumns)", r3.get("files_ingested") == 1, r3)

    import duckdb
    d = duckdb.connect()
    t = d.execute(f"SELECT * FROM delta_scan('{os.path.join(TMP, 'dbo', 'gcs_events')}')").fetchdf()
    check("44 rows total and the new column present, null for every earlier row", len(t) == 44 and "extra" in t.columns and t["extra"].isna().sum() == 43, (len(t), list(t.columns)))

    put("landing", "in/d.parquet", b"not a parquet file at all")
    p2 = autoloader.update_pipeline(p["id"], {"file_pattern": "*.parquet"})
    r4 = autoloader.run_pipeline_cycle(p["id"])
    quarantined = sorted(o["Key"] for o in s3.list_objects_v2(Bucket="landing", Prefix="in/_quarantine/").get("Contents", []))
    check("a genuinely unreadable object (no magic bytes) is quarantined, copied+deleted server-side, not retried",
          any("d.parquet" in k for k in quarantined) and not any(o["Key"] == "in/d.parquet" for o in s3.list_objects_v2(Bucket="landing", Prefix="in/").get("Contents", [])), (r4, quarantined))
    r5 = autoloader.run_pipeline_cycle(p["id"])
    check("the quarantined object is not retried on the next cycle", r5.get("files_ingested") == 0 and r5.get("files_quarantined") == 0, r5)

    # mount resolution: explicit id, by-bucket match, first mount
    make_bucket("otherbucket")
    put("otherbucket", "z.csv", "id\n1\n")
    mounts.save_mounts([mount("m_other", "otherbucket"), mount("m_landing", "landing")])
    conn = gcs.resolve_connection({"source_volume_path": "gcs://otherbucket/", "source_mount_id": ""})
    check("mount is chosen by bucket match when not given explicitly", conn["mount"] == "m_other")
    conn2 = gcs.resolve_connection({"source_volume_path": "gcs://otherbucket/", "source_mount_id": "m_landing"})
    check("an explicit mount id wins even if it doesn't match the bucket (credentials may still work)", conn2["mount"] == "m_landing")
    try:
        gcs.resolve_connection({"source_volume_path": "gcs://otherbucket/", "source_mount_id": "ghost"})
        ok = False
    except gcs.GcsSourceError as e:
        ok = "does not exist" in str(e)
    check("an unknown mount id is refused", ok)

    # the ENDPOINT/SCOPE quirk this module works around: a bucket-scoped secret must be able to read through a
    # deliberately wrong endpoint failing (proving ENDPOINT really is being applied, not silently defaulting to Google)
    duck = duckdb.connect()
    conn3 = gcs.resolve_connection({"source_volume_path": "gcs://landing/", "source_mount_id": "m_landing"})
    bad_conn = dict(conn3, endpoint="no-such-host.invalid")
    try:
        gcs.configure_duckdb(duck, bad_conn, "landing")
        duck.execute("SELECT * FROM glob('gcs://landing/**')").fetchall()
        ok = False
    except Exception as e:
        ok = "no-such-host" in str(e).lower() or "resolve" in str(e).lower()
    check("configure_duckdb's bucket-scoped secret really does honour a custom ENDPOINT (a wrong one fails to resolve)", ok, str(e) if not ok else "")
    duck.close()

    mounts.save_mounts([mount("gm1", "landing")])
    check("a bad gcs:// URL is refused", all(err(pv.preview, u) for u in ("gcs://", "gcs:///x")))
    check("a different bucket the pipeline never reads is untouched", [o["Key"] for o in s3.list_objects_v2(Bucket="otherbucket").get("Contents", [])] == ["z.csv"])

finally:
    shutil.rmtree(TMP, ignore_errors=True)

print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS")
sys.exit(1 if FAIL else 0)
