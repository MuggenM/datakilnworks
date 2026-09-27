#!/usr/bin/env python3
"""Delta Sharing server (web/delta_sharing.py + /api/sharing) in a throwaway warehouse, including the REAL `delta-sharing` client library against a live
server thread. Needs `pip install delta-sharing`:
docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook sh -c "pip install -q delta-sharing && python /workspace/scratch/test_delta_sharing.py"
The S3-mount section (pre-signed URLs straight to the mount's endpoint, real client included) only runs with a throwaway Garage container
(`scratch/garage_up.sh`) and TEST_S3_ENDPOINT / TEST_S3_ACCESS_KEY / TEST_S3_SECRET_KEY set; it is skipped, not failed, without them."""
import json, os, sys, tempfile, threading, time, shutil
TMP = tempfile.mkdtemp(prefix="dsh_"); os.environ["WAREHOUSE_DIR"] = TMP + "/warehouse"; os.makedirs(TMP + "/warehouse/.metadata")
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
os.environ["DELTA_SHARING_URL_TTL"] = "3"
sys.path.insert(0, "/workspace")
import pandas as pd, requests, uvicorn
from deltalake import write_deltalake, DeltaTable
from fastapi.testclient import TestClient
from web import app as app_module, auth, delta_sharing as ds
from web.governance import policies, row_filters, tags
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
W = TMP + "/warehouse"
os.makedirs(W + "/sales"); os.makedirs(W + "/hr")
df = pd.DataFrame({"id": list(range(1, 7)), "region": ["EMEA", "APAC", "EMEA", "AMER", "APAC", "EMEA"], "amount": [10.5, 20.0, None, 40.25, 50.0, 60.0]})
write_deltalake(W + "/sales/orders", df, partition_by=["region"]); write_deltalake(W + "/sales/orders", pd.DataFrame({"id": [7], "region": ["EMEA"], "amount": [70.0]}), mode="append")
write_deltalake(W + "/hr/people", pd.DataFrame({"id": [1, 2], "name": ["Ann", "Bo"], "ssn": ["111", "222"]}))
os.makedirs(W + "/misc/notdelta"); open(W + "/misc/notdelta/x.txt", "w").write("x")
with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0")
admin = TestClient(app_module.app); admin.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"})
anon = TestClient(app_module.app)
J = lambda r: r.json() if r.content else {}

print("administration")
check("only admins may use it", anon.get("/api/sharing").status_code in (401, 403))
check("an empty overview", J(admin.get("/api/sharing"))["shares"] == [] and J(admin.get("/api/sharing"))["endpoint"].endswith("/delta-sharing"))
check("a share is created; bad and duplicate names are refused", admin.post("/api/sharing/shares", json={"name": "partner_share", "comment": "for ACME"}).status_code == 200 and admin.post("/api/sharing/shares", json={"name": "bad name!"}).status_code == 400 and admin.post("/api/sharing/shares", json={"name": "partner_share"}).status_code == 409)
add = lambda src, **k: admin.post("/api/sharing/shares/partner_share/tables", json={"source": src, "history": True, **k})
check("a partitioned local Delta table is added", add("warehouse.sales.orders").status_code == 200)
check("...a table can be added under another name; duplicates are refused", add("warehouse.sales.orders", schema_alias="public", table_alias="sales_orders", history=False).status_code == 200 and add("warehouse.sales.orders").status_code == 409)
check("missing tables, non-Delta folders, malformed sources and unknown shares are refused", add("warehouse.sales.nope").status_code == 400 and add("warehouse.misc.notdelta").status_code == 400 and add("orders").status_code == 400 and add("warehouse.sales.orders; drop").status_code == 400 and admin.post("/api/sharing/shares/ghost/tables", json={"source": "warehouse.sales.orders"}).status_code == 404)

print("governance: raw files cannot be masked or filtered")
tags.set_tag(catalog="warehouse", schema_name="hr", table_name="people", column_name="ssn", tag_key="pii", tag_value="ssn")
r = add("warehouse.hr.people"); check("with no policy the table is shareable (tags alone do not matter)", r.status_code == 200, r.text)
admin.delete("/api/sharing/shares/partner_share/tables/hr/people")
pol = policies.create_policy({"name": "Mask PII", "tag_key": "pii", "mask_type": "redact", "except_roles": ["admin"]})
r = add("warehouse.hr.people"); check("a masking policy that applies to non-exempt principals refuses it, even though the admin is exempt", r.status_code == 400 and "masking policy" in r.text, r.text)
policies.delete_policy(pol["id"])
tags.create_definition("region_scoped", "Row filter scope by region")
tags.set_tag(catalog="warehouse", schema_name="sales", table_name="orders", tag_key="region_scoped", tag_value="")
rp = row_filters.create_row_policy({"name": "Region filter", "tag_key": "region_scoped", "filter_column": "region", "filter_mode": "attribute", "attribute_key": "region", "except_roles": ["admin"]})
r = add("warehouse.sales.orders", table_alias="filtered"); check("a row filter policy refuses it too", r.status_code == 400 and "row filter" in r.text, r.text)
check("the refusal is audited", any("SHARING_TABLE_REFUSED" in str(x) for x in __import__("sqlite3").connect(W + "/.metadata/governance.db").execute("SELECT action FROM governance_audit").fetchall()))
row_filters.delete_row_policy(rp["id"]); tags.unset_tag(catalog="warehouse", schema_name="sales", table_name="orders", tag_key="region_scoped")

print("recipients")
r = admin.post("/api/sharing/recipients", json={"name": "ACME Corp", "shares": ["partner_share"], "expires_in_days": 30}).json()
tok = r["token"]; prof = r["profile"]
check("a recipient gets a token and a profile file (shown once)", tok.startswith("dkw_dsh_") and prof["shareCredentialsVersion"] == 1 and prof["bearerToken"] == tok and prof["endpoint"].endswith("/delta-sharing") and prof["expirationTime"].endswith("Z"), r)
ov = json.dumps(admin.get("/api/sharing").json())
check("the token is never shown again and only its hash is stored", tok not in ov and tok not in json.dumps([dict(x) for x in ds._db().execute("SELECT * FROM recipients")]) and ds._hash(tok) in json.dumps([dict(x) for x in ds._db().execute("SELECT * FROM recipients")]))
check("bad recipient names, duplicates, unknown shares and silly expiries are refused", admin.post("/api/sharing/recipients", json={"name": "<x>"}).status_code == 400 and admin.post("/api/sharing/recipients", json={"name": "ACME Corp"}).status_code == 409 and admin.post("/api/sharing/recipients", json={"name": "Z", "shares": ["ghost"]}).status_code == 404 and admin.post("/api/sharing/recipients", json={"name": "Z2", "expires_in_days": 99999}).status_code == 400)
rid = r["recipient"]["id"]

print("protocol, raw HTTP")
H = {"Authorization": f"Bearer {tok}"}
check("no token, a wrong token and an admin session are all 401", anon.get("/delta-sharing/shares").status_code == 401 and anon.get("/delta-sharing/shares", headers={"Authorization": "Bearer nope"}).status_code == 401 and admin.get("/delta-sharing/shares").status_code == 401)
check("the error body follows the protocol", anon.get("/delta-sharing/shares").json()["errorCode"] == "UNAUTHENTICATED")
check("list shares", anon.get("/delta-sharing/shares", headers=H).json() == {"items": [{"name": "partner_share", "id": anon.get("/delta-sharing/shares", headers=H).json()["items"][0]["id"]}]})
check("get share, schemas, tables and all-tables", anon.get("/delta-sharing/shares/partner_share", headers=H).json()["share"]["name"] == "partner_share"
      and [i["name"] for i in anon.get("/delta-sharing/shares/partner_share/schemas", headers=H).json()["items"]] == ["public", "sales"]
      and [i["name"] for i in anon.get("/delta-sharing/shares/partner_share/schemas/sales/tables", headers=H).json()["items"]] == ["orders"]
      and len(anon.get("/delta-sharing/shares/partner_share/all-tables", headers=H).json()["items"]) == 2)
p1 = anon.get("/delta-sharing/shares/partner_share/all-tables?maxResults=1", headers=H).json()
p2 = anon.get("/delta-sharing/shares/partner_share/all-tables?maxResults=1&pageToken=" + p1["nextPageToken"], headers=H).json()
check("pagination", len(p1["items"]) == 1 and "nextPageToken" in p1 and len(p2["items"]) == 1 and "nextPageToken" not in p2 and p1["items"][0]["name"] != p2["items"][0]["name"])
check("unknown shares, schemas and tables are 404 with a protocol error", all(anon.get(u, headers=H).status_code == 404 for u in ("/delta-sharing/shares/ghost", "/delta-sharing/shares/partner_share/schemas/nope/tables", "/delta-sharing/shares/partner_share/schemas/sales/tables/nope/version")))
v = anon.get("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/version", headers=H)
check("version header", v.status_code == 200 and v.headers["delta-table-version"] == "1", dict(v.headers))
m = anon.get("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/metadata", headers=H)
lines = [json.loads(x) for x in m.text.strip().split("\n")]
check("metadata is NDJSON: protocol 1 then metaData in the Parquet format", m.headers["content-type"].startswith("application/x-ndjson") and lines[0] == {"protocol": {"minReaderVersion": 1}} and lines[1]["metaData"]["format"] == {"provider": "parquet"} and lines[1]["metaData"]["partitionColumns"] == ["region"] and "amount" in lines[1]["metaData"]["schemaString"], m.text[:300])
q = anon.post("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/query", json={}, headers=H)
ql = [json.loads(x) for x in q.text.strip().split("\n")]; files = [x["file"] for x in ql if "file" in x]
check("query returns protocol, metaData and one file line per data file with a signed URL, partition values, size and stats", q.headers["delta-table-version"] == "1" and "protocol" in ql[0] and "metaData" in ql[1] and len(files) >= 4 and all(f["url"].startswith("http://testserver/delta-sharing/files/") and f["size"] > 0 and "partitionValues" in f and json.loads(f["stats"])["numRecords"] >= 1 for f in files), q.text[:400])
check("the total number of records in the stats is the table's", sum(json.loads(f["stats"])["numRecords"] for f in files) == 7)
q0 = [json.loads(x) for x in anon.post("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/query", json={"version": 0}, headers=H).text.strip().split("\n")]
check("time travel: version 0 has the first 6 rows", sum(json.loads(x["file"]["stats"])["numRecords"] for x in q0 if "file" in x) == 6)
check("a change data feed / version range request is refused clearly", anon.post("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/query", json={"startingVersion": 0}, headers=H).status_code == 400 and anon.get("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/changes", headers=H).status_code == 400)
check("a bad body is 400", anon.post("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/query", content=b"[1]", headers=H).status_code == 400)
furl = files[0]["url"].replace("http://testserver", "")
r = anon.get(furl); check("the file link needs no credentials and serves the Parquet file", r.status_code == 200 and r.content[:4] == b"PAR1", r.status_code)
tokpart = furl.rsplit("/", 1)[1]
b, s = tokpart.split(".")
check("a tampered or malformed link is 401", anon.get("/delta-sharing/files/" + b + "." + s[:-2] + "AA").status_code == 401 and anon.get("/delta-sharing/files/garbage").status_code == 401 and anon.get("/delta-sharing/files/a.b").status_code == 401)
import base64
def forge(**kw):
    p = json.loads(base64.urlsafe_b64decode(b + "==")); p.update(kw)
    body = base64.urlsafe_b64encode(json.dumps(p).encode()).decode().rstrip("=")
    return body + "." + s
check("a payload changed after signing is refused (path traversal, other file)", anon.get("/delta-sharing/files/" + forge(f="../../hr/people/x.parquet")).status_code == 401)
import hashlib, hmac
def signed(**kw):
    p = {"r": rid, "t": ds._db().execute("SELECT id FROM share_tables LIMIT 1").fetchone()[0], "f": "x.parquet", "e": int(time.time()) + 60}; p.update(kw)
    body = base64.urlsafe_b64encode(json.dumps(p).encode()).decode().rstrip("=")
    return body + "." + base64.urlsafe_b64encode(hmac.new(ds._key(), body.encode(), hashlib.sha256).digest()).decode().rstrip("=")
check("even a correctly signed link cannot leave the table directory or name a non-Parquet file", all(anon.get("/delta-sharing/files/" + signed(f=f)).status_code == 404 for f in ("../../hr/people/_delta_log/00000000000000000000.json", "/etc/passwd", "_delta_log/00000000000000000000.json", "..\\x.parquet", "")))
time.sleep(3.5)
check("a link expires (TTL 3 s in this test)", anon.get(furl).status_code == 401 and "expired" in anon.get(furl).text)

print("live server and the real delta-sharing client")
cfg = uvicorn.Config(app_module.app, host="127.0.0.1", port=8931, log_level="warning"); srv = uvicorn.Server(cfg); threading.Thread(target=srv.run, daemon=True).start()
for _ in range(100):
    try: requests.get("http://127.0.0.1:8931/healthz", timeout=1); break
    except Exception: time.sleep(0.2)
import delta_sharing
os.environ["DELTA_SHARING_URL_TTL"] = "900"; ds.URL_TTL = 900
pf = TMP + "/acme.share"; json.dump({**prof, "endpoint": "http://127.0.0.1:8931/delta-sharing"}, open(pf, "w"))
client = delta_sharing.SharingClient(pf)
check("the client lists shares and tables", [s.name for s in client.list_shares()] == ["partner_share"] and sorted(t.name for t in client.list_all_tables()) == ["orders", "sales_orders"])
pdf = delta_sharing.load_as_pandas(pf + "#partner_share.sales.orders").sort_values("id").reset_index(drop=True)
check("load_as_pandas returns exactly the table (7 rows, partition column, nulls)", list(pdf["id"]) == [1, 2, 3, 4, 5, 6, 7] and list(pdf["region"]) == ["EMEA", "APAC", "EMEA", "AMER", "APAC", "EMEA", "EMEA"] and pdf["amount"].isna().sum() == 1 and abs(pdf["amount"].sum() - 250.75) < 1e-9, str(pdf))
try:                                                    # the Rust kernel parses our Delta-format lines; fetching a URL that is not on S3/Azure/GCS is what it cannot do, see verify_delta_sharing_https.py
    kd = delta_sharing.load_as_pandas(pf + "#partner_share.sales.orders", use_delta_format=True)
    ok, why = sorted(kd["id"]) == [1, 2, 3, 4, 5, 6, 7], str(kd)
except Exception as exc:
    ok, why = "Object at location /delta-sharing/files/" in repr(exc), repr(exc)[:500]
check("the client's Delta-format path (delta-kernel-rust) accepts protocol, metadata and file lines (it reads the table, or only fails at fetching a file URL on a non-cloud-storage host)", ok, why)
check("the aliased table gives the same data", len(delta_sharing.load_as_pandas(pf + "#partner_share.public.sales_orders")) == 7)
check("time travel through the client (version 0 = 6 rows)", len(delta_sharing.load_as_pandas(pf + "#partner_share.sales.orders", version=0)) == 6)
check("the client can read the table version", delta_sharing.get_table_version(pf + "#partner_share.sales.orders") == 1)
check("bytes served are counted for the recipient", [x for x in admin.get("/api/sharing").json()["recipients"] if x["id"] == rid][0]["bytes_served"] > 0)
log = admin.get("/api/sharing").json()["log"]; check("the activity log shows the recipient's queries", any(x["action"] == "QUERY" and x["recipient"] == "ACME Corp" for x in log))

print("history sharing is opt-in")
rq = lambda tbl, body, hdr=None, h=H: anon.post(f"/delta-sharing/shares/partner_share/schemas/{tbl}/query", json=body, headers={**h, **(hdr or {})})
check("without history only the latest version can be read (version 0 and old timestamps are 403)", rq("public/tables/sales_orders", {"version": 0}).status_code == 403 and rq("public/tables/sales_orders", {"version": 1}).status_code == 200 and rq("public/tables/sales_orders", {"timestamp": "2000-01-01T00:00:00Z"}).status_code in (400, 403))
check("...and the change feed is refused", anon.get("/delta-sharing/shares/partner_share/schemas/public/tables/sales_orders/changes?startingVersion=0", headers=H).status_code == 403)
check("an unknown version is a clear 400", rq("sales/tables/orders", {"version": 99}).status_code == 400)
admin.put("/api/sharing/shares/partner_share/tables/public/sales_orders/history", json={"history": True})
check("an administrator can turn history sharing on (and it is audited)", rq("public/tables/sales_orders", {"version": 0}).status_code == 200 and any("SHARING_TABLE_HISTORY" in str(x) for x in __import__("sqlite3").connect(W + "/.metadata/governance.db").execute("SELECT action FROM governance_audit").fetchall()))
check("the overview shows the flag", [t["history"] for x in admin.get("/api/sharing").json()["shares"] for t in x["tables"] if t["table_name"] == "sales_orders"] == [True])
admin.put("/api/sharing/shares/partner_share/tables/public/sales_orders/history", json={"history": False})
check("...and off again", rq("public/tables/sales_orders", {"version": 0}).status_code == 403)

print("stats")
def files_of(body, hdr=None, tbl="sales/tables/orders"):
    r = rq(tbl, body, hdr); ls = [json.loads(x) for x in r.text.strip().split("\n")]
    return r, ls, [x["file"] for x in ls if "file" in x]
r, ls, fl = files_of({})
st = [json.loads(f["stats"]) for f in fl]
check("integer columns carry min/max, exactly the log's; floats and partition columns do not", all(set(x.get("minValues", {})) <= {"id"} and set(x.get("maxValues", {})) <= {"id"} and "amount" not in x.get("minValues", {}) for x in st) and any("minValues" in x for x in st), st[:2])
check("nullCount is there for data columns (amount has one null)", sum(x["nullCount"].get("amount", 0) for x in st) == 1, st)
def rows_of(files):
    import pyarrow.parquet as pq, io
    out = []
    for f in files:
        rr = anon.get(f["url"].replace("http://testserver", "")); out += pq.read_table(io.BytesIO(rr.content)).to_pylist()
    return out
allrows = rows_of(fl); check("(sanity: the files hold the 7 rows; partition values are in the file line, not the data)", len(allrows) == 7)

print("predicate and limit hints")
def ids_from(body):
    r, ls, fl2 = files_of(body); return len(fl2), sorted(x["id"] for x in rows_of(fl2)), fl2
n_all = len(fl)
n, got, _ = ids_from({"predicateHints": ["region = 'APAC'"]})
check("a partition predicate keeps only that partition's files (and every matching row)", n < n_all and {2, 5} <= set(got) and n == 1, (n, got))
n, got, _ = ids_from({"predicateHints": ["region = 'NOWHERE'"]}); check("a value that no partition holds gives no files", n == 0)
n, got, _ = ids_from({"predicateHints": ["region <> 'EMEA'"]}); check("<> works on partitions", {2, 4, 5} <= set(got) and 1 not in got, got)
n, got, _ = ids_from({"predicateHints": ["id > 6"]}); check("a range predicate prunes by min/max but never drops a matching row", 7 in got and n < n_all, (n, n_all, got))
n, got, _ = ids_from({"predicateHints": ["id >= 1 AND id <= 1"]}); check("AND of two terms", 1 in got and n <= n_all, got)
n, got, _ = ids_from({"predicateHints": ["id = 4"]}); check("equality on a stats column", 4 in got and n < n_all, (n, got))
n, got, _ = ids_from({"predicateHints": ["id < 0"]}); check("a range nothing satisfies gives no files", n == 0)
n, got, _ = ids_from({"predicateHints": ["amount > 1000"]}); check("a float column is never pruned by stats (no bounds are sent)", n == n_all)
n, got, _ = ids_from({"predicateHints": ["upper(region) = 'X'", "id +", "1 = 1", "region LIKE 'E%'"]}); check("hints that are not understood keep every file", n == n_all)
n, got, _ = ids_from({"predicateHints": ["region IS NULL"]}); check("IS NULL on a partition (no nulls) gives no files", n == 0)
n, got, _ = ids_from({"predicateHints": ["amount IS NULL"]}); check("IS NULL on a data column uses the null counts (only the file with the null)", 3 in got and n < n_all, (n, got))
J = lambda o: json.dumps(o)
col = lambda nme, t="STRING": {"op": "column", "name": nme, "valueType": t}; lit = lambda v, t="STRING": {"op": "literal", "value": v, "valueType": t}
n, got, _ = ids_from({"jsonPredicateHints": J({"op": "equal", "children": [col("region"), lit("APAC")]})}); check("jsonPredicateHints: equal on a partition", n == 1 and {2, 5} <= set(got))
n, got, _ = ids_from({"jsonPredicateHints": J({"op": "and", "children": [{"op": "greaterThan", "children": [col("id", "LONG"), lit("5", "LONG")]}, {"op": "equal", "children": [col("region"), lit("EMEA")]}]})}); check("jsonPredicateHints: AND, numeric literal as a string, EMEA rows above 5 are kept", {6, 7} <= set(got), got)
n, got, _ = ids_from({"jsonPredicateHints": J({"op": "or", "children": [{"op": "equal", "children": [col("region"), lit("APAC")]}, {"op": "equal", "children": [col("region"), lit("AMER")]}]})}); check("jsonPredicateHints: OR", {2, 4, 5} <= set(got) and 1 not in got, got)
n, got, _ = ids_from({"jsonPredicateHints": J({"op": "lessThan", "children": [lit("3", "LONG"), col("id", "LONG")]})}); check("jsonPredicateHints: literal on the left is flipped (3 < id)", 4 in got and 7 in got, got)
n, got, _ = ids_from({"jsonPredicateHints": J({"op": "not", "children": [{"op": "equal", "children": [col("region"), lit("EMEA")]}]})}); check("jsonPredicateHints: NOT is not reasoned about (all files kept)", n == n_all)
n, got, _ = ids_from({"jsonPredicateHints": "{not json"}); check("broken JSON hints are ignored", n == n_all)
n, got, _ = ids_from({"limitHint": 1}); check("limitHint 1 returns just enough files", n == 1 and n < n_all, n)
n, got, _ = ids_from({"limitHint": 3}); check("limitHint 3 returns files with at least 3 rows in total", 1 <= n < n_all and len(got) >= 3, (n, got))
n, got, _ = ids_from({"limitHint": 1000}); check("a limit above the table size returns everything", n == n_all)
n, got, _ = ids_from({"predicateHints": ["region = 'EMEA'"], "limitHint": 1}); check("limit after an exact (partition) predicate", n == 1)
n, got, _ = ids_from({"predicateHints": ["id > 0"], "limitHint": 1}); check("limit is NOT applied after an inexact (stats) predicate", n == n_all)
check("a bad limit is ignored", ids_from({"limitHint": "many"})[0] == n_all and ids_from({"limitHint": -5})[0] == n_all)

print("response formats")
CAP = lambda v: {"delta-sharing-capabilities": v}
r = rq("sales/tables/orders", {}, CAP("responseformat=delta")); dl = [json.loads(x) for x in r.text.strip().split("\n")]
check("Delta format: deltaProtocol, deltaMetadata (with version, size, numFiles) and deltaSingleAction add files with signed URLs", r.headers["delta-sharing-capabilities"] == "responseformat=delta" and dl[0]["protocol"]["deltaProtocol"]["minReaderVersion"] == 1 and "minWriterVersion" in dl[0]["protocol"]["deltaProtocol"]
      and dl[1]["metaData"]["deltaMetadata"]["format"]["provider"] == "parquet" and dl[1]["metaData"]["numFiles"] == len(dl) - 2 and dl[1]["metaData"]["version"] == 1 and dl[1]["metaData"]["size"] > 0
      and all(x["file"]["deltaSingleAction"]["add"]["path"].startswith("http://testserver/delta-sharing/files/") and x["file"]["deltaSingleAction"]["add"]["dataChange"] is True and x["file"]["expirationTimestamp"] > time.time() * 1000 for x in dl[2:]), r.text[:500])
check("its files download", anon.get(dl[2]["file"]["deltaSingleAction"]["add"]["path"].replace("http://testserver", "")).content[:4] == b"PAR1")
r = rq("sales/tables/orders", {}, CAP("responseformat=delta,parquet")); check("'delta,parquet' (what the client can read, not a preference): parquet", "deltaProtocol" not in r.text and r.headers["delta-sharing-capabilities"] == "responseformat=parquet")
r = rq("sales/tables/orders", {}, CAP("responseformat=parquet,delta")); check("'parquet,delta': parquet", '"minReaderVersion":1}' in r.text and "deltaProtocol" not in r.text and r.headers["delta-sharing-capabilities"] == "responseformat=parquet")
check("unknown or no capabilities: parquet", "deltaProtocol" not in rq("sales/tables/orders", {}, CAP("responseformat=avro;readerfeatures=deletionvectors")).text and "deltaProtocol" not in rq("sales/tables/orders", {}).text)
check("readerfeatures in the header do not change what is served (tables with reader features stay refused)", "deltaProtocol" in rq("sales/tables/orders", {}, CAP("responseformat=delta;readerfeatures=deletionvectors,columnmapping")).text)
m = anon.get("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/metadata", headers={**H, **CAP("responseformat=delta")}); ml = [json.loads(x) for x in m.text.strip().split("\n")]
check("metadata honours the format too", "deltaProtocol" in ml[0]["protocol"] and "deltaMetadata" in ml[1]["metaData"])
n, got, _ = ids_from({"predicateHints": ["region = 'APAC'"]}); r = rq("sales/tables/orders", {"predicateHints": ["region = 'APAC'"]}, CAP("responseformat=delta")); check("hints work in the Delta format as well", len(r.text.strip().split("\n")) - 2 == n)

print("change data feed")
os.makedirs(W + "/sales", exist_ok=True)
write_deltalake(W + "/sales/events", pd.DataFrame({"id": [1, 2, 3], "v": [1, 2, 3]}), configuration={"delta.enableChangeDataFeed": "true"})
dte = DeltaTable(W + "/sales/events"); dte.update({"v": "10"}, predicate="id = 1"); dte.delete("id = 2")
write_deltalake(W + "/sales/events", pd.DataFrame({"id": [4], "v": [4]}), mode="append")
admin.post("/api/sharing/shares", json={"name": "feed_share"})
fa = admin.post("/api/sharing/shares/feed_share/tables", json={"source": "warehouse.sales.events", "history": True}); check("a CDF table is added with history", fa.status_code == 200, fa.text)
admin.post("/api/sharing/shares/feed_share/tables", json={"source": "warehouse.sales.orders", "schema_alias": "sales", "table_alias": "plain", "history": True})
admin.post("/api/sharing/shares/feed_share/tables", json={"source": "warehouse.sales.orders", "schema_alias": "sales", "table_alias": "nohist"})
rf = admin.post("/api/sharing/recipients", json={"name": "Feed Co", "shares": ["feed_share"]}).json(); HF = {"Authorization": "Bearer " + rf["token"]}
CH = lambda tbl, qs, hdr=None: anon.get(f"/delta-sharing/shares/feed_share/schemas/sales/tables/{tbl}/changes?{qs}", headers={**HF, **(hdr or {})})
r = CH("events", "startingVersion=0&endingVersion=3"); cl = [json.loads(x) for x in r.text.strip().split("\n")]
acts = [(list(x)[0], x[list(x)[0]]["version"]) for x in cl[2:]]
check("the feed starts with protocol + metaData, then per version: add for the create, cdf for the update and the delete, add for the append", "protocol" in cl[0] and "metaData" in cl[1] and acts[0] == ("add", 0) and {a for a in acts if a[1] == 1} == {("cdf", 1)} and {a for a in acts if a[1] == 2} == {("cdf", 2)} and ("add", 3) in acts, acts)
check("every change line has a signed url, size, partition values and the commit timestamp", all(x[list(x)[0]]["url"].startswith("http://testserver/delta-sharing/files/") and x[list(x)[0]]["size"] > 0 and x[list(x)[0]]["timestamp"] > 1.5e12 and "partitionValues" in x[list(x)[0]] for x in cl[2:]))
import pyarrow.parquet as pq, io
cdf_rows = []
for x in cl[2:]:
    if "cdf" in x: cdf_rows += pq.read_table(io.BytesIO(anon.get(x["cdf"]["url"].replace("http://testserver", "")).content)).to_pylist()
kinds = sorted((r_["_change_type"], r_["id"]) for r_ in cdf_rows)
check("the change-data files carry the row-level changes (update pre/post image, delete)", ("update_preimage", 1) in kinds and ("update_postimage", 1) in kinds and ("delete", 2) in kinds, kinds)
r = CH("events", "startingVersion=1&endingVersion=1"); c1 = [json.loads(x) for x in r.text.strip().split("\n")][2:]; check("a single-version range", c1 and all(list(x)[0] == "cdf" for x in c1))
r = CH("events", "startingVersion=2"); c2 = [json.loads(x) for x in r.text.strip().split("\n")][2:]; check("no endingVersion means up to the latest", {x[list(x)[0]]["version"] for x in c2} == {2, 3})
ts_all = [int(json.loads(l)["commitInfo"]["timestamp"]) for l in [open(W + f"/sales/events/_delta_log/{v:020d}.json").readline() for v in range(4)]]
iso = lambda ms: datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).isoformat().replace("+00:00", "Z")
import datetime
r = CH("events", f"startingTimestamp={iso(ts_all[1])}&endingTimestamp={iso(ts_all[2])}"); c3 = [json.loads(x) for x in r.text.strip().split("\n")][2:]
check("timestamps are resolved to versions (start = first commit at or after, end = last commit at or before)", {x[list(x)[0]]["version"] for x in c3} == {1, 2}, r.text[:300])
check("errors: no start, a start beyond the latest, end before start, bad numbers, too wide", CH("events", "").status_code == 400 and CH("events", "startingVersion=99").status_code == 400 and CH("events", "startingVersion=2&endingVersion=1").status_code == 400 and CH("events", "startingVersion=x").status_code == 400 and CH("events", "startingVersion=0&endingVersion=99").status_code == 400)
ds.MAX_CHANGE_VERSIONS = 2; check("a range wider than MAX_CHANGE_VERSIONS is refused", CH("events", "startingVersion=0&endingVersion=3").status_code == 400); ds.MAX_CHANGE_VERSIONS = 1000
check("a table shared WITHOUT history has no change feed (403)", CH("nohist", "startingVersion=0").status_code == 403)
r = CH("plain", "startingVersion=0"); pl = [json.loads(x) for x in r.text.strip().split("\n")][2:]
check("a table without CDF: add actions per commit (dataChange only), no cdf", r.status_code == 200 and all(list(x)[0] == "add" for x in pl) and {x["add"]["version"] for x in pl} == {0, 1})
check("...their stats travel with the add (numRecords)", all(json.loads(x["add"]["stats"])["numRecords"] >= 1 for x in pl))
r = CH("events", "startingVersion=0&endingVersion=1", CAP("responseformat=delta")); dcl = [json.loads(x) for x in r.text.strip().split("\n")]
check("the change feed in Delta format: deltaSingleAction add / cdc with version and timestamp", "deltaProtocol" in dcl[0]["protocol"] and dcl[2]["file"]["deltaSingleAction"].keys() == {"add"} and any("cdc" in x["file"]["deltaSingleAction"] and x["file"]["version"] == 1 for x in dcl[2:]))
r = CH("plain", "startingVersion=0&endingVersion=1"); os.makedirs(W + "/x", exist_ok=True)
# a commit that removes a file (overwrite) yields remove actions for tables without CDF
write_deltalake(W + "/sales/plain_src", pd.DataFrame({"id": [1]})); write_deltalake(W + "/sales/plain_src", pd.DataFrame({"id": [2]}), mode="overwrite")
admin.post("/api/sharing/shares/feed_share/tables", json={"source": "warehouse.sales.plain_src", "history": True})
r = CH("plain_src", "startingVersion=0"); ps = [json.loads(x) for x in r.text.strip().split("\n")][2:]
check("an overwrite without CDF yields add and remove actions (remove url is signed)", {list(x)[0] for x in ps if x[list(x)[0]]["version"] == 1} == {"add", "remove"} and all("url" in x[list(x)[0]] for x in ps))
DeltaTable(W + "/sales/events").create_checkpoint()
for v_ in (0, 1): os.remove(W + f"/sales/events/_delta_log/{v_:020d}.json")
check("a version whose log file was cleaned up (behind a checkpoint) is reported, not guessed", "no longer available" in CH("events", "startingVersion=0&endingVersion=1").text and CH("events", "startingVersion=2").status_code == 200)
check("the change requests are logged", any(x["action"] == "CHANGES" for x in admin.get("/api/sharing").json()["log"]))
tags.create_definition("gate", "x") if False else None

print("the real client reads the change feed")
pf2 = TMP + "/feed.share"; json.dump({**json.loads(json.dumps(rf["profile"])), "endpoint": "http://127.0.0.1:8931/delta-sharing"}, open(pf2, "w"))
write_deltalake(W + "/sales/events2", pd.DataFrame({"id": [1, 2, 3], "v": [1, 2, 3]}), configuration={"delta.enableChangeDataFeed": "true"})
d2 = DeltaTable(W + "/sales/events2"); d2.update({"v": "10"}, predicate="id = 1"); d2.delete("id = 2")
admin.post("/api/sharing/shares/feed_share/tables", json={"source": "warehouse.sales.events2", "history": True})
try:
    ch = delta_sharing.load_table_changes_as_pandas(pf2 + "#feed_share.sales.events2", starting_version=0, ending_version=2)
    kinds = sorted(zip(ch["_change_type"], ch["id"], ch["_commit_version"])); check("load_table_changes_as_pandas returns inserts, update pre/post images and the delete", ("insert", 1, 0) in kinds and ("update_preimage", 1, 1) in kinds and ("update_postimage", 1, 1) in kinds and ("delete", 2, 2) in kinds, kinds)
except Exception as exc:
    check("load_table_changes_as_pandas", False, repr(exc))
try:
    check("load_as_pandas with limit and the hints path works through the real client", len(delta_sharing.load_as_pandas(pf2 + "#feed_share.sales.plain", limit=1)) == 1)
except Exception as exc:
    check("load_as_pandas with a limit", False, repr(exc))
for tbl in ("events", "plain", "nohist", "plain_src", "events2"):
    pass
admin.delete(f"/api/sharing/recipients/{rf['recipient']['id']}"); admin.delete("/api/sharing/shares/feed_share")

print("per-recipient IP rules")
rip = admin.post("/api/sharing/recipients", json={"name": "Office Only", "shares": ["partner_share"]}).json(); HI = {"Authorization": "Bearer " + rip["token"]}
c_in, c_out = TestClient(app_module.app, client=("10.1.2.3", 50000)), TestClient(app_module.app, client=("203.0.113.9", 50000))
check("without rules every address works", c_out.get("/delta-sharing/shares", headers=HI).status_code == 200)
v = admin.put(f"/api/sharing/recipients/{rip['recipient']['id']}/ips", json={"allowed_cidrs": ["10.0.0.0/8", "192.0.2.5"]}); check("rules are stored (a single address becomes /32) and shown", v.status_code == 200 and v.json()["allowed_cidrs"] == ["10.0.0.0/8", "192.0.2.5/32"], v.text)
check("an address inside is served, outside is 403 with the protocol error", c_in.get("/delta-sharing/shares", headers=HI).status_code == 200 and c_out.get("/delta-sharing/shares", headers=HI).status_code == 403 and c_out.get("/delta-sharing/shares", headers=HI).json()["errorCode"] == "PERMISSION_DENIED")
furl_in = [json.loads(x)["file"]["url"] for x in c_in.post("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/query", json={}, headers=HI).text.strip().split("\n") if '"file"' in x][0].replace("http://testserver", "")
check("a file link works from an allowed address only (a leaked link is useless elsewhere)", c_in.get(furl_in).status_code == 200 and c_out.get(furl_in).status_code == 403)
check("X-Forwarded-For from an untrusted peer is not believed", c_out.get("/delta-sharing/shares", headers={**HI, "X-Forwarded-For": "10.1.2.3"}).status_code == 403)
check("the refusal is logged", any(x["action"] == "REFUSED" and x["recipient"] == "Office Only" for x in admin.get("/api/sharing").json()["log"]))
check("garbage, /0 and too many entries are refused", all(admin.put(f"/api/sharing/recipients/{rip['recipient']['id']}/ips", json={"allowed_cidrs": bad}).status_code == 400 for bad in (["not-an-ip"], ["0.0.0.0/0"], ["::/0"], [f"10.0.{i}.0/24" for i in range(51)])))
v = admin.put(f"/api/sharing/recipients/{rip['recipient']['id']}/ips", json={"allowed_cidrs": []}); check("clearing the list removes the restriction", v.json()["allowed_cidrs"] == [] and c_out.get("/delta-sharing/shares", headers=HI).status_code == 200)
admin.delete(f"/api/sharing/recipients/{rip['recipient']['id']}")

print("things that change while a recipient is connected")
tags.set_tag(catalog="warehouse", schema_name="sales", table_name="orders", tag_key="region_scoped", tag_value="")
rp = row_filters.create_row_policy({"name": "Late filter", "tag_key": "region_scoped", "filter_column": "region", "filter_mode": "attribute", "attribute_key": "region", "except_roles": ["admin"]})
r = anon.post("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/query", json={}, headers=H)
check("a row filter policy created AFTER sharing cuts the table off at once (403, with the reason)", r.status_code == 403 and "row filter" in r.text, r.text)
check("...an already issued file link is refused too", anon.get(furl.replace(furl.rsplit('/',1)[1], forge_ok := ds.sign_file(rid, ds._db().execute("SELECT id FROM share_tables WHERE table_name='orders'").fetchone()[0], files[0]["url"].rsplit("/", 1)[1] and json.loads(base64.urlsafe_b64decode(furl.rsplit('/',1)[1].split('.')[0] + '=='))["f"]))).status_code == 403)
check("the overview flags the table that is no longer shareable", any(t.get("problem") for s_ in admin.get("/api/sharing").json()["shares"] for t in s_["tables"] if t["table_name"] == "orders"))
row_filters.delete_row_policy(rp["id"]); tags.unset_tag(catalog="warehouse", schema_name="sales", table_name="orders", tag_key="region_scoped")
check("and works again once the policy is gone", anon.post("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/query", json={}, headers=H).status_code == 200)
write_deltalake(W + "/sales/orders", pd.DataFrame({"id": [8], "region": ["AMER"], "amount": [1.0]}), mode="append")
check("new data is visible at the next request (version 2)", len(delta_sharing.load_as_pandas(pf + "#partner_share.sales.orders")) == 8 and delta_sharing.get_table_version(pf + "#partner_share.sales.orders") == 2)
admin.delete("/api/sharing/shares/partner_share/tables/sales/orders")
check("removing a table from the share hides it and kills its links", anon.get("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/version", headers=H).status_code == 404 and anon.get(files[0]["url"].replace("http://testserver", "")).status_code in (401, 404))
add("warehouse.sales.orders")
check("(re-added)", anon.get("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/version", headers=H).status_code == 200)

print("recipient lifecycle")
r2 = admin.post("/api/sharing/recipients", json={"name": "Other Org"}).json(); H2 = {"Authorization": "Bearer " + r2["token"]}
check("a recipient without shares sees none, and cannot reach another recipient's share", anon.get("/delta-sharing/shares", headers=H2).json()["items"] == [] and anon.get("/delta-sharing/shares/partner_share", headers=H2).status_code == 404 and anon.post("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/query", json={}, headers=H2).status_code == 404)
admin.put(f"/api/sharing/recipients/{r2['recipient']['id']}/shares", json={"shares": ["partner_share"]})
check("granting a share makes it visible", len(anon.get("/delta-sharing/shares", headers=H2).json()["items"]) == 1)
furl2 = [json.loads(x)["file"]["url"] for x in anon.post("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/query", json={}, headers=H2).text.strip().split("\n") if '"file"' in x][0].replace("http://testserver", "")
admin.put(f"/api/sharing/recipients/{r2['recipient']['id']}/shares", json={"shares": []})
check("taking the share away also invalidates that recipient's file links", anon.get(furl2).status_code == 404)
admin.put(f"/api/sharing/recipients/{r2['recipient']['id']}/shares", json={"shares": ["partner_share"]})
furl2 = [json.loads(x)["file"]["url"] for x in anon.post("/delta-sharing/shares/partner_share/schemas/sales/tables/orders/query", json={}, headers=H2).text.strip().split("\n") if '"file"' in x][0].replace("http://testserver", "")
admin.post(f"/api/sharing/recipients/{r2['recipient']['id']}/revoke")
check("revoking stops the API and the file links at once", anon.get("/delta-sharing/shares", headers=H2).status_code == 401 and anon.get(furl2).status_code == 401 and "revoked" in anon.get("/delta-sharing/shares", headers=H2).text)
rot = admin.post(f"/api/sharing/recipients/{r2['recipient']['id']}/rotate", json={}).json()
check("rotating gives a new token that works (and reactivates); the old one does not", anon.get("/delta-sharing/shares", headers={"Authorization": "Bearer " + rot["token"]}).status_code == 200 and anon.get("/delta-sharing/shares", headers=H2).status_code == 401 and rot["profile"]["bearerToken"] == rot["token"])
ds._db().execute("UPDATE recipients SET expires_at = '2000-01-01 00:00:00' WHERE name = 'Other Org'").connection.commit()
check("an expired token is refused", "expired" in anon.get("/delta-sharing/shares", headers={"Authorization": "Bearer " + rot["token"]}).text)
admin.delete(f"/api/sharing/recipients/{r2['recipient']['id']}")
check("deleting a recipient removes it", len(admin.get("/api/sharing").json()["recipients"]) == 1)
admin.delete("/api/sharing/shares/partner_share")
check("deleting a share removes it from the recipient", anon.get("/delta-sharing/shares", headers=H).json()["items"] == [] and admin.get("/api/sharing").json()["recipients"][0]["shares"] == [])
acts = {x[0] for x in __import__("sqlite3").connect(W + "/.metadata/governance.db").execute("SELECT action FROM governance_audit")}
check("all administration is audited", {"SHARING_SHARE_CREATE", "SHARING_TABLE_ADD", "SHARING_TABLE_REMOVE", "SHARING_RECIPIENT_CREATE", "SHARING_RECIPIENT_REVOKE", "SHARING_RECIPIENT_ROTATE", "SHARING_RECIPIENT_DELETE", "SHARING_SHARE_DELETE"} <= acts, acts)
print("S3-mount tables (real Garage, optional)")
S3_EP = os.environ.get("TEST_S3_ENDPOINT")
if not S3_EP:
    print("  (skipped: set TEST_S3_ENDPOINT/TEST_S3_ACCESS_KEY/TEST_S3_SECRET_KEY to a throwaway Garage container -- see scratch/garage_up.sh -- to run this section)")
else:
    import boto3
    from web import mounts
    S3_KEY, S3_SECRET, S3_REGION = os.environ.get("TEST_S3_ACCESS_KEY", ""), os.environ.get("TEST_S3_SECRET_KEY", ""), os.environ.get("TEST_S3_REGION", "garage")
    s3cfg = {"bucket": "dshbucket", "endpoint": S3_EP, "key_id": S3_KEY, "secret": S3_SECRET, "region": S3_REGION, "use_ssl": False}
    s3 = boto3.client("s3", endpoint_url=f"http://{S3_EP}", aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET, region_name=S3_REGION)
    for _ in range(30):
        try: s3.list_buckets(); break
        except Exception: time.sleep(1)
    try: s3.create_bucket(Bucket="dshbucket")
    except Exception: pass
    mounts.save_mounts([{"id": "dshm1", "type": "s3", "catalog_name": "dshcat", "name": "dshcat", "config": s3cfg}])
    from web.mounts import get_s3_storage_options
    write_deltalake("s3://dshbucket/dbo/items", pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]}), storage_options=get_s3_storage_options(s3cfg))
    admin.post("/api/sharing/shares", json={"name": "s3_share"})
    r3a = admin.post("/api/sharing/shares/s3_share/tables", json={"source": "dshcat.dbo.items", "history": True})
    check("a table in an S3 mount can be added to a share", r3a.status_code == 200, r3a.text)
    r3 = admin.post("/api/sharing/recipients", json={"name": "S3 Partner", "shares": ["s3_share"]}).json()
    H3 = {"Authorization": "Bearer " + r3["token"]}
    q3 = anon.post("/delta-sharing/shares/s3_share/schemas/dbo/tables/items/query", json={}, headers=H3)
    ql3 = [json.loads(x) for x in q3.text.strip().split("\n")]
    files3 = [x["file"] for x in ql3 if "file" in x]
    check("query returns pre-signed URLs straight to the S3-compatible endpoint, not our own file proxy",
          q3.status_code == 200 and len(files3) >= 1 and all(S3_EP in f["url"] and "/delta-sharing/files/" not in f["url"] for f in files3), q3.text[:500])
    check("the pre-signed URL is fetchable on its own (a plain HTTP GET, no bearer token)", requests.get(files3[0]["url"]).status_code == 200)
    pf3 = TMP + "/s3.share"; json.dump({**r3["profile"], "endpoint": "http://127.0.0.1:8931/delta-sharing"}, open(pf3, "w"))
    try:
        real = delta_sharing.load_as_pandas(pf3 + "#s3_share.dbo.items")
        check("the REAL delta-sharing client reads an S3-mount table end to end via the pre-signed URL", sorted(real["id"].tolist()) == [1, 2, 3], real["id"].tolist())
    except Exception as exc:
        check("the real client reads an S3-mount table", False, repr(exc))
    chg = anon.get("/delta-sharing/shares/s3_share/schemas/dbo/tables/items/changes?startingVersion=0", headers=H3)
    check("the change feed is refused for an S3-mount table, with a clear reason", chg.status_code == 403 and "S3 mount" in chg.text, chg.text)
    write_deltalake("s3://dshbucket/dbo/items", pd.DataFrame({"id": [4], "val": ["d"]}), mode="append", storage_options=get_s3_storage_options(s3cfg))
    q3b = anon.post("/delta-sharing/shares/s3_share/schemas/dbo/tables/items/query", json={"version": 0}, headers=H3)
    n0 = sum(json.loads(x["file"]["stats"])["numRecords"] for x in [json.loads(l) for l in q3b.text.strip().split("\n")] if "file" in x)
    check("time travel (an older version) still works for an S3-mount table", n0 == 3, q3b.text[:400])
    admin.delete(f"/api/sharing/recipients/{r3['recipient']['id']}"); admin.delete("/api/sharing/shares/s3_share")

os.environ["DELTA_SHARING"] = "off"
check("DELTA_SHARING=off switches the protocol off", anon.get("/delta-sharing/shares", headers=H).status_code == 404)
srv.should_exit = True; shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
