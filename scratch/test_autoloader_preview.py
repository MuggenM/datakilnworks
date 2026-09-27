#!/usr/bin/env python3
"""Preview of local-folder and s3:// Auto-Loader sources (web/autoloader_preview.py) against a throwaway warehouse and a THROWAWAY Deuxfleurs Garage (a real S3):
  eval "$(scratch/garage_up.sh pvnet pv-garage)"    # prints/exports GARAGE_ENDPOINT, GARAGE_KEY_ID, GARAGE_SECRET_KEY
  docker run --rm --network pvnet -e S3_ENDPOINT="$GARAGE_ENDPOINT" -e S3_ACCESS_KEY="$GARAGE_KEY_ID" -e S3_SECRET_KEY="$GARAGE_SECRET_KEY" \
     -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_autoloader_preview.py
Garage stands in for MinIO here (MinIO's Docker Hub image can no longer be pulled without a login); see scratch/garage_up.sh for why one test still uses
MinIO. Container names must not use underscores: botocore's endpoint validation rejects them in a hostname."""
import io, json, os, shutil, sys, tempfile, time
TMP = tempfile.mkdtemp(prefix="alpv_"); os.environ["WAREHOUSE_DIR"] = TMP
sys.path.insert(0, "/workspace")
import pyarrow as pa, pyarrow.parquet as pq
from web import autoloader_preview as pv, autoloader, mounts
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def err(fn, *a, **k):
    try: fn(*a, **k); return None
    except pv.PreviewError as e: return str(e)
def write(rel, data, mtime=None, base=TMP):
    p = os.path.join(base, rel); os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb" if isinstance(data, bytes) else "w") as f: f.write(data)
    if mtime: os.utime(p, (mtime, mtime))
    return p
NOW = time.time()
def csv(n, start=0): return "id,name,amount\n" + "\n".join(f"{start + i},name{start + i},{(start + i) * 1.5}" for i in range(n)) + "\n"
D = os.path.join(TMP, "landing")

print("local folders")
write("landing/b_new.csv", csv(30, 100), NOW - 10); write("landing/a_old.csv", csv(30, 0), NOW - 1000)
r = pv.preview(D, "*.csv"); check("the oldest file is the sample (what the pipeline loads first), listing shows both", r["source"] == "local" and r["sample"] == "a_old.csv" and [f["path"] for f in r["files"]] == ["a_old.csv", "b_new.csv"] and r["file_count"] == 2, r["sample"])
check("columns and types are those the loader would infer", [(c["name"], c["type"]) for c in r["columns"]] == [("id", "int64"), ("name", "string"), ("amount", "double")], r["columns"])
check("10 rows by default, more exist", len(r["rows"]) == 10 and r["truncated"] is True and r["rows"][0] == [0, "name0", 0.0])
check("a limit is honoured (max 50)", len(pv.preview(D, "*.csv", limit=3)["rows"]) == 3 and len(pv.preview(D, "*.csv", limit=500)["rows"]) == 30)
r = pv.preview(D, "*.csv", sample_file="b_new.csv"); check("a specific file can be chosen from the listing", r["sample"] == "b_new.csv" and r["rows"][0][0] == 100)
check("a file that is not in the discovered list is refused (no path can be smuggled in)", "not one of the files" in (err(pv.preview, D, "*.csv", None, "../../etc/passwd") or "") and "not one of the files" in (err(pv.preview, D, "*.csv", None, "nope.csv") or ""))
write("landing/.hidden.csv", csv(3)); write("landing/x.csv.tmp", csv(3)); write("landing/y.csv.part", csv(3)); write("landing/_quarantine/bad.csv", csv(3)); write("landing/.sub/z.csv", csv(3)); write("landing/deep/c.csv", csv(3, 500), NOW - 5000)
r = pv.preview(D, "*.csv"); check("the pipeline's filters apply: hidden, .tmp, .part, _quarantine and hidden folders are ignored; sub-folders are included", sorted(f["path"] for f in r["files"]) == ["a_old.csv", "b_new.csv", "deep/c.csv"] and r["sample"] == "deep/c.csv", [f["path"] for f in r["files"]])
write("landing/data.json", json.dumps([{"a": 1, "nested": {"k": [1, 2]}, "s": "é✓"}]), NOW - 9000); write("landing/lines.jsonl", "".join(json.dumps({"i": i, "t": "x"}) + "\n" for i in range(5)), NOW - 8000)
r = pv.preview(D, "data.json"); check("JSON with nested values and unicode", r["columns"][0]["name"] == "a" and r["rows"][0][2] == "é✓" and isinstance(r["rows"][0][1], dict), r["rows"])
r = pv.preview(D, "lines.jsonl"); check("JSON Lines", len(r["rows"]) == 5 and r["truncated"] is False)
pq.write_table(pa.table({"x": list(range(100)), "y": [f"v{i}" for i in range(100)], "ts": pa.array([1] * 100, pa.timestamp("us"))}), os.path.join(TMP, "landing", "p.parquet"))
r = pv.preview(D, "*.parquet"); check("Parquet, with a timestamp column shown as text", [c["name"] for c in r["columns"]] == ["x", "y", "ts"] and len(r["rows"]) == 10 and isinstance(r["rows"][0][2], str), r["rows"][:1])
write("landing/readme.md", "# hi\n"); write("landing/notes.bin", b"\x00\x01")
r = pv.preview(D, "*"); check("with pattern * unsupported formats are noted, and the sample is the oldest readable file", r["sample"] not in ("readme.md", "notes.bin") and any("format the loader does not read" in n for n in r["notes"]) and any(not f["readable"] for f in r["files"]), r["notes"])
check("only unsupported files: a clear message", "none has a format" in (err(pv.preview, D, "readme.*") or ""))
write("landing/corrupt.parquet", b"not a parquet file at all", NOW - 20000); write("landing/broken.csv", 'a,b\n1,"unterminated\n', NOW - 30000)
e = err(pv.preview, D, "corrupt.parquet"); check("a corrupt file gives a readable error, not a stack trace", e and "PARQUET" in e and len(e) < 300 and "Traceback" not in e, e)
try: pv.preview(D, "broken.csv"); msg = "read"
except pv.PreviewError as ex_: msg = str(ex_)
check("a malformed CSV is either read as far as DuckDB can, or reported in one plain line", "\n" not in msg and len(msg) < 300, msg)
check("no matching file: says which pattern", "No file" in (err(pv.preview, D, "*.xlsx") or "") and "'*.xlsx'" in err(pv.preview, D, "*.xlsx"))
missing = os.path.join(TMP, "not_yet")
e = err(pv.preview, missing, "*"); check("a folder that does not exist yet is reported, and NOT created (the pipeline creates it itself)", "does not exist yet" in e and not os.path.exists(missing), e)
check("a path outside the volumes / warehouse is refused", "Invalid volume path" in (err(pv.preview, "/etc") or ""), err(pv.preview, "/etc"))
write(".metadata/storage_mounts.json", json.dumps([{"config": {"secret": "TOPSECRET"}}])); write(".metadata/x.csv", csv(2))
e = err(pv.preview, os.path.join(TMP, ".metadata"), "*"); check("a hidden folder (.metadata holds credentials) can never be previewed", e and "Hidden folders" in e, e)
e = err(pv.preview, os.path.join(TMP, "landing", "..", ".metadata"), "*"); check("...also not through .. or a symlink", e is not None and "TOPSECRET" not in e)
os.symlink(os.path.join(TMP, ".metadata"), os.path.join(TMP, "landing", "link")); e = err(pv.preview, os.path.join(TMP, "landing", "link"), "*"); check("a symlink into a hidden folder is refused", e and "Hidden" in e, e)
check("an empty path and a mount on a local path are refused", "Enter the source path" in (err(pv.preview, "  ") or "") and "only applies to s3://" in (err(pv.preview, D, "*", "m1") or ""))
try: autoloader._validate_source(os.path.join(TMP, ".metadata"), 0, None); ok = False
except ValueError as ex: ok = "Hidden" in str(ex)
check("creating a pipeline on a hidden folder is refused too", ok)
for i in range(MAX := 5100): write(f"many/f{i:05d}.csv", "a\n1\n", NOW - i)
r = pv.preview(os.path.join(TMP, "many"), "*.csv"); check("a huge folder is scanned only up to a limit and says so", r["truncated_listing"] is True and r["file_count"] == pv.MAX_LOCAL_SCAN and r["notes"][0].startswith(f"{pv.MAX_LOCAL_SCAN}+"), r["notes"][0])
check("the listing is capped", len(pv.preview(D, "*")["files"]) <= pv.LISTED_FILES)
check("no pipeline, checkpoint or table exists after all of this", not os.path.exists(missing) and not os.path.exists(os.path.join(TMP, ".metadata", "autoloader.db")))

EP = os.environ.get("S3_ENDPOINT")
if not EP:
    print("no S3_ENDPOINT: skipping the S3 section")
else:
    print("s3:// (Garage)")
    AK = os.environ.get("S3_ACCESS_KEY", "minioadmin"); SK = os.environ.get("S3_SECRET_KEY", "minioadmin")  # defaults kept for an old MinIO-based run
    import boto3
    s3 = boto3.client("s3", endpoint_url=f"http://{EP}", aws_access_key_id=AK, aws_secret_access_key=SK, region_name="garage")
    for _ in range(30):
        try: s3.list_buckets(); break
        except Exception: time.sleep(1)
    s3.create_bucket(Bucket="landing")
    put = lambda k, b: s3.put_object(Bucket="landing", Key=k, Body=b if isinstance(b, bytes) else b.encode())
    def mount(mid="m1", bucket="landing", secret=SK, key=AK, endpoint=EP):
        return {"id": mid, "type": "s3", "catalog_name": mid, "name": mid, "config": {"bucket": bucket, "endpoint": endpoint, "key_id": key, "secret": secret, "region": "garage", "url_style": "path", "use_ssl": False}}
    mounts.save_mounts([])
    check("no mount configured: a clear message", "mount" in (err(pv.preview, "s3://landing/in/", "*.csv") or "").lower())
    mounts.save_mounts([mount()])
    put("in/a.csv", csv(30)); time.sleep(1.1); put("in/b.csv", csv(30, 100)); put("in/.hidden.csv", csv(2)); put("in/x.csv.tmp", csv(2)); put("in/_quarantine/q.csv", csv(2)); put("in/sub/c.csv", csv(3, 900)); put("in/readme.md", "x")
    buf = io.BytesIO(); pq.write_table(pa.table({"x": list(range(50)), "y": ["a"] * 50}), buf); put("pq/p.parquet", buf.getvalue()); put("in/data.jsonl", "".join(json.dumps({"k": i}) + "\n" for i in range(3)))
    r = pv.preview("s3://landing/in/", "*.csv")
    check("an S3 prefix previews the oldest matching object, read in place", r["source"] == "s3" and r["sample"] == "a.csv" and len(r["rows"]) == 10 and r["truncated"] and r["columns"][0]["name"] == "id", r["notes"])
    check("the pipeline's filters apply to keys (hidden, .tmp, _quarantine)", sorted(f["path"] for f in r["files"]) == ["a.csv", "b.csv", "sub/c.csv"], [f["path"] for f in r["files"]])
    r = pv.preview("s3://landing/in/", "*.csv", None, "sub/c.csv"); check("a chosen object is read", r["sample"] == "sub/c.csv" and r["rows"][0][0] == 900)
    check("an object outside the listing is refused", "not one of the files" in (err(pv.preview, "s3://landing/in/", "*.csv", None, "../pq/p.parquet") or ""))
    r = pv.preview("s3://landing/pq/", "*.parquet"); check("Parquet on S3", [c["name"] for c in r["columns"]] == ["x", "y"] and len(r["rows"]) == 10)
    r = pv.preview("s3://landing/in/", "*.jsonl"); check("JSON Lines on S3", r["rows"] == [[0], [1], [2]])
    check("no object matches: says so", "No object" in (err(pv.preview, "s3://landing/in/", "*.xlsx") or ""))
    check("an empty prefix is reported the same way", "No object" in (err(pv.preview, "s3://landing/nothing/", "*") or ""))
    check("bad URLs are refused", all(err(pv.preview, u) for u in ("s3://", "s3:///x", "s3://landing/../x")))
    e = err(pv.preview, "s3://nobucket/x/", "*"); check("a missing bucket is reported in plain words", e and "bucket" in e.lower() and "Traceback" not in e, e)
    mounts.save_mounts([mount(secret="wrong")]); e = err(pv.preview, "s3://landing/in/", "*.csv"); check("wrong credentials: access denied, and the secret is never in the message", e and ("denied" in e.lower() or "credential" in e.lower() or "signature" in e.lower()) and "wrong" not in e, e)
    mounts.save_mounts([mount(endpoint="127.0.0.1:1")]); e = err(pv.preview, "s3://landing/in/", "*.csv"); check("an unreachable endpoint fails fast with a short message", e and len(e) < 300, e)
    mounts.save_mounts([mount("other", bucket="elsewhere"), mount("m_landing")])
    check("the mount is chosen by bucket, or explicitly", pv.preview("s3://landing/in/", "*.csv")["sample"] == "a.csv" and pv.preview("s3://landing/in/", "*.csv", "m_landing")["sample"] == "a.csv")
    check("an unknown mount is refused", "does not exist" in (err(pv.preview, "s3://landing/in/", "*.csv", "ghost") or ""))
    mounts.save_mounts([mount()])
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(16) as ex: list(ex.map(lambda i: put(f"big/o{i:05d}.csv", "a\n1\n"), range(3300)))
    r = pv.preview("s3://landing/big/", "*.csv"); check("a huge prefix is scanned only up to a limit and says so", r["truncated_listing"] is True and "only the first" in r["notes"][0] and r["file_count"] <= pv.MAX_S3_SCAN + 1000, r["notes"][0])
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
