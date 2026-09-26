#!/usr/bin/env python3
"""Auto-Loader target catalogs (web/autoloader.py resolve_target / target_catalogs), throwaway WAREHOUSE_DIR, in-process moto S3 (mock,
not Garage/MinIO/AWS). Run in a throwaway container:
  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook \
     sh -c 'pip install -q "moto[server]" && python /workspace/scratch/test_autoloader_target.py'
Prefer a real S3 server: with a throwaway MinIO on the same docker network and TEST_S3_ENDPOINT=<host>:9000 (delta-rs stalls against moto's threaded server)
Covers: the dropdown source lists local + writable S3 catalogs only; a local source loads into a catalog that is an S3 mount
(append, merge, schema evolution, exactly-once); read-only and non-Delta mounts and unknown catalogs are refused up front."""
import os, shutil, sys, tempfile
TMP = tempfile.mkdtemp(prefix="al_target_"); os.environ["WAREHOUSE_DIR"] = TMP
sys.path.insert(0, "/workspace")
EXTERNAL = os.getenv("TEST_S3_ENDPOINT")        # e.g. a throwaway MinIO container (a real S3 implementation): host:port, keys minioadmin/minioadmin
try:
    from moto.server import ThreadedMotoServer
except ImportError:
    ThreadedMotoServer = None
import boto3
from deltalake import DeltaTable
from web import autoloader, mounts
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
import io
import pyarrow as pa, pyarrow.parquet as pq
PORT, BUCKET = 18933, "lake"
EP = EXTERNAL or f"127.0.0.1:{PORT}"
KEY = "minioadmin" if EXTERNAL else "k"
def mount(mid, **kw):
    return {"id": mid, "type": kw.pop("type", "s3"), "catalog_name": mid, "name": mid, "read_only": kw.pop("read_only", False),
            "config": {"bucket": BUCKET, "endpoint": EP, "key_id": KEY, "secret": KEY, "region": "us-east-1", "url_style": "path", "use_ssl": False}}
server = None if EXTERNAL else ThreadedMotoServer(port=PORT, verbose=False)
if server: server.start()
try:
    s3 = boto3.client("s3", endpoint_url=f"http://{EP}", aws_access_key_id=KEY, aws_secret_access_key=KEY, region_name="us-east-1"); s3.create_bucket(Bucket=BUCKET)
    autoloader.init_autoloader_db()
    so = mounts.get_s3_storage_options(mount("x")["config"])
    mounts.save_mounts([mount("lake_s3"), mount("ro_s3", read_only=True), mount("pg", type="postgres")])
    ids = {c["id"]: c for c in autoloader.target_catalogs()}
    check("dropdown lists the default catalog", ids.get("warehouse", {}).get("kind") == "local", list(ids))
    check("dropdown lists a writable S3 mount, marked S3", ids.get("lake_s3", {}).get("kind") == "s3", list(ids))
    check("read-only and non-S3 mounts are not offered", "ro_s3" not in ids and "pg" not in ids, list(ids))

    def mk(name, **kw):
        return autoloader.create_pipeline({"name": name, "source_volume_path": src, "file_pattern": "*.csv", **kw})
    src = os.path.join(TMP, "landing"); os.makedirs(src)
    for bad, why in (("ro_s3", "read-only"), ("pg", "postgres"), ("nope", "does not exist")):
        try: mk("bad", target_catalog=bad, target_table="t"); ok = False
        except ValueError as e: ok = why.split()[0].lower() in str(e).lower() or "only local" in str(e)
        check(f"target '{bad}' refused at creation", ok)
    p = mk("to s3", target_catalog="lake_s3", target_schema="bronze", target_table="events")
    open(os.path.join(src, "a.csv"), "w").write("id,val\n1,a\n2,b\n")
    r = autoloader.run_pipeline_cycle(p["id"]); check("first cycle ingests into the S3 catalog", r.get("files_ingested") == 1 and r.get("rows_ingested") == 2, r)
    def read(uri):
        """Active data files of the S3 Delta table read through boto3 (moto's threaded server stalls under delta-rs' parallel reads)."""
        dt = DeltaTable(uri, storage_options=so)
        parts = [pq.read_table(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=f.split(f"s3://{BUCKET}/", 1)[1])["Body"].read())) for f in dt.file_uris()]
        return pa.concat_tables(parts, promote_options="default") if parts else None
    uri = f"s3://{BUCKET}/bronze/events"
    check("the Delta table is at s3://bucket/schema/table", read(uri).num_rows == 2)
    check("nothing was written to local storage for it", not os.path.exists(os.path.join(TMP, "bronze")) and not os.path.exists(os.path.join(TMP, "catalogs", "lake_s3")))
    open(os.path.join(src, "b.csv"), "w").write("id,val,extra\n3,c,x\n")
    r = autoloader.run_pipeline_cycle(p["id"]); check("append with a new column (schema evolution) on S3", r.get("files_ingested") == 1, r)
    t = read(uri)
    check("3 rows and the new column", t.num_rows == 3 and "extra" in t.column_names, (t.num_rows, t.column_names))
    r = autoloader.run_pipeline_cycle(p["id"]); check("exactly-once: a second cycle loads nothing", r.get("files_ingested") == 0, r)
    m = mk("merge to s3", target_catalog="lake_s3", target_schema="bronze", target_table="merged", ingest_mode="merge", merge_keys="id")
    autoloader.run_pipeline_cycle(m["id"])
    open(os.path.join(src, "c.csv"), "w").write("id,val\n1,changed\n9,new\n")
    autoloader.run_pipeline_cycle(m["id"])
    mt = read(f"s3://{BUCKET}/bronze/merged").to_pydict()
    check("merge/upsert works on S3", sorted(mt["id"]) == [1, 2, 3, 9] and mt["val"][mt["id"].index(1)] == "changed", mt)
    # local catalogs are unchanged
    loc = mk("local", target_table="local_t"); autoloader.run_pipeline_cycle(loc["id"])
    check("the default catalog still lands on local disk", os.path.isdir(os.path.join(TMP, "dbo", "local_t", "_delta_log")))
finally:
    (server.stop() if server else None); shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
