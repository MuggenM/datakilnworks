#!/usr/bin/env python3
"""Azure Blob Storage as an Auto-Loader source (web/autoloader_azure.py, web/autoloader_preview.py, web/mounts.py "azure" mount
type), throwaway WAREHOUSE_DIR, against a real throwaway Azurite (Microsoft's official Storage emulator; not real Azure, and not
in `ci/plan.json` for that reason). Run on the same docker network as Azurite:
  docker network create aztest_net
  docker run -d --name aztest_azurite --network aztest_net mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck
  docker run --rm --network aztest_net -e AZURITE_HOST=aztest_azurite -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch \
     localspark-lakehouse-notebook python /workspace/scratch/test_autoloader_azure.py
`--skipApiVersionCheck` is required: a recent azure-storage-blob release sends an API version Azurite does not know yet, and
every request is refused (`InvalidHeaderValue`) without the flag.
Covers: mount resolution (explicit id, by-container match, first mount), discovery filters (hidden, .tmp/.part, _quarantine),
exactly-once via ETag, schema evolution (a new column), quarantine of a genuinely corrupt blob (verified server-side copy +
delete, unlike GCS/S3 no bucket-scope quirk applies here), watch_enabled refused (inotify has nothing to watch on Azure),
and the preview endpoint (web/autoloader_preview.py) reading a blob in place through DuckDB's azure extension.
"""
import io
import os
import shutil
import sys
import tempfile
import time

TMP = tempfile.mkdtemp(prefix="al_azure_")
os.environ["WAREHOUSE_DIR"] = TMP
sys.path.insert(0, "/workspace")

from azure.storage.blob import BlobServiceClient
import pyarrow as pa
import pyarrow.parquet as pq

from web import autoloader, autoloader_azure as az, autoloader_preview as pv, mounts

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


ACCOUNT = "devstoreaccount1"
ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
HOST = os.environ.get("AZURITE_HOST", "127.0.0.1")
ACCOUNT_URL = f"http://{HOST}:10000/{ACCOUNT}"
CS = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={ACCOUNT_KEY};BlobEndpoint={ACCOUNT_URL};"


def mount(mid, container, **kw):
    return {"id": mid, "type": "azure", "catalog_name": mid, "name": mid,
            "config": {"container": container, "account_name": ACCOUNT, "account_key": ACCOUNT_KEY, "account_url": ACCOUNT_URL, **kw}}


svc = BlobServiceClient.from_connection_string(CS)
for _ in range(30):
    try:
        list(svc.list_containers())
        break
    except Exception:
        time.sleep(1)


def make_container(name):
    try:
        svc.create_container(name)
    except Exception:
        pass
    return svc.get_container_client(name)


def put(cc, name, body):
    cc.upload_blob(name, body if isinstance(body, bytes) else body.encode(), overwrite=True)


try:
    landing = make_container("landing")
    autoloader.init_autoloader_db()
    check("is_azure_path recognises the scheme", az.is_azure_path("azure://landing/in/") and not az.is_azure_path("s3://x/y") and not az.is_azure_path(""))
    check("parse_azure_path splits container/prefix", az.parse_azure_path("azure://landing/in/sub") == ("landing", "in/sub/"))
    check("normalize_path always ends the prefix with /", az.normalize_path("azure://landing/in") == "azure://landing/in/")

    mounts.save_mounts([])
    check("no mount configured: a clear message", "mount" in (err(pv.preview, "azure://landing/in/", "*.csv") or "").lower())
    mounts.save_mounts([mount("am1", "landing")])

    put(landing, "in/a.csv", "id,name,amount\n" + "\n".join(f"{i},n{i},{i * 1.5}" for i in range(20)) + "\n")
    time.sleep(1.1)
    put(landing, "in/b.csv", "id,name,amount\n" + "\n".join(f"{i},n{i},{i * 1.5}" for i in range(100, 120)) + "\n")
    put(landing, "in/.hidden.csv", "id\n1\n")
    put(landing, "in/x.csv.tmp", "id\n1\n")
    put(landing, "in/_quarantine/q.csv", "id\n1\n")
    put(landing, "in/sub/c.csv", "id,name,amount\n" + "\n".join(f"{i},n{i},{i * 1.5}" for i in range(900, 903)) + "\n")
    put(landing, "in/readme.md", "hi")

    r = pv.preview("azure://landing/in/", "*.csv")
    check("a preview reads the oldest matching blob in place, discovery filters applied",
          r["source"] == "azure" and r["sample"] == "a.csv" and sorted(f["path"] for f in r["files"]) == ["a.csv", "b.csv", "sub/c.csv"], r)
    check("columns match what the loader would infer", [(c["name"], c["type"]) for c in r["columns"]] == [("id", "int64"), ("name", "string"), ("amount", "double")], r["columns"])
    r2 = pv.preview("azure://landing/in/", "*.csv", None, "sub/c.csv")
    check("a specific blob can be chosen", r2["sample"] == "sub/c.csv" and r2["rows"][0][0] == 900)
    check("an object outside the listing is refused", "not one of the files" in (err(pv.preview, "azure://landing/in/", "*.csv", None, "../readme.md") or ""))

    p = autoloader.create_pipeline({"name": "azure test", "source_volume_path": "azure://landing/in/", "file_pattern": "*.csv",
                                    "target_catalog": "warehouse", "target_schema": "dbo", "target_table": "az_events"})
    check("the source is normalised and no watcher / mount error at creation", p["source_volume_path"] == "azure://landing/in/")
    try:
        autoloader.create_pipeline({"name": "bad", "source_volume_path": "azure://landing/in/", "watch_enabled": True, "target_table": "x"})
        ok = False
    except ValueError as e:
        ok = "polled" in str(e)
    check("file-event watching is refused for an Azure source (inotify has nothing to watch there)", ok)

    r1 = autoloader.run_pipeline_cycle(p["id"])
    check("first cycle ingests all matching blobs (sub-folders included), oldest first, filters applied",
          r1.get("files_ingested") == 3 and r1.get("rows_ingested") == 43, r1)
    r2 = autoloader.run_pipeline_cycle(p["id"])
    check("exactly-once: a second cycle loads nothing new", r2.get("files_ingested") == 0, r2)

    put(landing, "in/c.csv", "id,name,amount,extra\n1,x,1.0,zeta\n")
    r3 = autoloader.run_pipeline_cycle(p["id"])
    check("a new blob with an extra column evolves the schema (addNewColumns)", r3.get("files_ingested") == 1, r3)

    import duckdb
    d = duckdb.connect()
    t = d.execute(f"SELECT * FROM delta_scan('{os.path.join(TMP, 'dbo', 'az_events')}')").fetchdf()
    check("44 rows total and the new column present, null for every earlier row", len(t) == 44 and "extra" in t.columns and t["extra"].isna().sum() == 43, (len(t), list(t.columns)))

    buf = io.BytesIO()
    pq.write_table(pa.table({"x": [1, 2, 3]}), buf)
    put(landing, "in/bad.csv", b"this is not really csv at all but duckdb may still parse a line of it")
    put(make_container("elsewhere"), "in/other.csv", "id\n1\n")  # a second container: never touched by this pipeline
    put(landing, "in/d.parquet", b"not a parquet file")
    p2 = autoloader.update_pipeline(p["id"], {"file_pattern": "*"})
    r4 = autoloader.run_pipeline_cycle(p["id"])
    quarantined_names = sorted(b.name for b in landing.list_blobs(name_starts_with="in/_quarantine/"))
    check("a genuinely unreadable blob (no magic bytes) is quarantined, moved server-side, not retried", any("d.parquet" in n for n in quarantined_names), quarantined_names)
    r5 = autoloader.run_pipeline_cycle(p["id"])
    check("the quarantined blob is not retried on the next cycle", not any("d.parquet" in (d.get("file_path") or "") and d["status"] != "QUARANTINED" for d in r5.get("details", [])), r5)

    # mount resolution: explicit id, by-container match, first mount
    other = make_container("otherbucket")
    put(other, "z.csv", "id\n1\n")
    mounts.save_mounts([mount("m_other", "otherbucket"), mount("m_landing", "landing")])
    conn = az.resolve_connection({"source_volume_path": "azure://otherbucket/", "source_mount_id": ""})
    check("mount is chosen by container match when not given explicitly", conn["mount"] == "m_other")
    conn2 = az.resolve_connection({"source_volume_path": "azure://otherbucket/", "source_mount_id": "m_landing"})
    check("an explicit mount id wins even if it doesn't match the container (credentials/account may still work)", conn2["mount"] == "m_landing")
    try:
        az.resolve_connection({"source_volume_path": "azure://otherbucket/", "source_mount_id": "ghost"})
        ok = False
    except az.AzureSourceError as e:
        ok = "does not exist" in str(e)
    check("an unknown mount id is refused", ok)

    mounts.save_mounts([mount("am1", "landing")])
    check("a bad azure:// URL is refused", all(err(pv.preview, u) for u in ("azure://", "azure:///x")))
    check("a different container the pipeline never reads is untouched", [b.name for b in make_container("elsewhere").list_blobs()] == ["in/other.csv"])

finally:
    shutil.rmtree(TMP, ignore_errors=True)

print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS")
sys.exit(1 if FAIL else 0)
