#!/usr/bin/env python3
"""
Phase 4 verification for tag-based masking: every data-egress path goes through the gateway.
Runs against a throwaway WAREHOUSE_DIR, so it never touches real data.
Tests (each as an exempt admin and as a masked user):
1. SQL editor: masked results, masked_columns, blocked statements, history keeps the user's text, workers get rewritten SQL.
2. Dashboards: widget query, filters, preview, export; the result cache never mixes masked and unmasked viewers.
3. Table preview, version preview and version diff; parquet export; query profile; history profile.
4. Owner-scoped background work: jobs, alerts, scheduled exports run as their owner.
5. Genie: LLM prompt samples are masked for everyone; generated SQL runs as the asker.
6. Features that cannot be masked are refused for masked users (dbt, OneLake, distributed scans); upload id traversal.
7. A compute worker runs the rewritten SQL.
"""

import datetime
import glob
import io
import json
import os
import shutil
import sys
import tempfile

TMP_ROOT = tempfile.mkdtemp(prefix="governance_p4_")
TMP_WAREHOUSE = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(TMP_WAREHOUSE)
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_NOTEBOOK_EXECUTION", "JWT_SECRET_KEY", "COMPUTE_TOKEN", "GOVERNANCE_ENFORCEMENT"):
    os.environ.pop(var, None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import jwt
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth
from web.governance import gateway, policies, store, tags

FAILURES = []
RAW = ["ada@example.com", "bob@corp.io", "cy@example.com", "123-45-6789", "987-65-4321", "555-12-3456", "dee@example.com", "111-22-3333"]


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:1500]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def leaks(obj):
    text = json.dumps(obj, default=str) if not isinstance(obj, str) else obj
    return [v for v in RAW if v in text]


def cookie_for(username):
    user = auth.get_user_by_username(username)
    token = jwt.encode({"sub": user["id"], "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)},
                       auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)
    return {auth.COOKIE_NAME: token}


ADMIN, BOB, LEAD = {}, {}, {}
WORKER_CALLS = []


def setup():
    """Delta tables exist before the studio connects, so they are registered the way the app registers them."""
    d = os.path.join(TMP_WAREHOUSE, "hr", "delta_emp")
    write_deltalake(d, pa.table({"id": [1, 2, 3], "email": ["ada@example.com", "bob@corp.io", "cy@example.com"],
                                 "ssn": ["123-45-6789", "987-65-4321", "555-12-3456"], "dept": ["eng", "ops", "eng"]}))
    write_deltalake(d, pa.table({"id": [4], "email": ["dee@example.com"], "ssn": ["111-22-3333"], "dept": ["ops"]}), mode="append")

    con = app_module.get_duckrun_conn().con
    con.execute("CREATE SCHEMA IF NOT EXISTS warehouse.hr")
    if not con.execute("SELECT 1 FROM duckdb_views() WHERE view_name = 'delta_emp' AND database_name = 'warehouse'").fetchone():
        con.execute(f"CREATE VIEW warehouse.hr.delta_emp AS SELECT * FROM delta_scan('{d}')")
    con.execute("CREATE OR REPLACE TABLE warehouse.hr.employees (id INTEGER, email VARCHAR, ssn VARCHAR, dept VARCHAR)")
    con.execute("INSERT INTO warehouse.hr.employees VALUES (1,'ada@example.com','123-45-6789','eng'),(2,'bob@corp.io','987-65-4321','ops'),(3,'cy@example.com','555-12-3456','eng')")

    for key, vals in (("pii", ["email", "ssn"]),):
        if key not in [t["tag_key"] for t in tags.list_definitions()]:
            tags.create_definition(key, "personal", vals)
    for tbl in ("employees", "delta_emp"):
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name=tbl, column_name="email", tag_key="pii", tag_value="email")
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name=tbl, column_name="ssn", tag_key="pii", tag_value="ssn")
    policies.create_policy({"name": "Email masking", "tag_key": "pii", "tag_value": "email", "mask_type": "email", "priority": 50})
    policies.create_policy({"name": "PII partial", "tag_key": "pii", "mask_type": "partial", "priority": 100})
    ADMIN.update(cookie_for("admin"))
    BOB.update(cookie_for("analyst_bob"))
    LEAD.update(cookie_for("lead_engineer"))
    return d


def install_worker_spy():
    """Captures what the studio would send to a compute worker, without reaching the real containers."""
    import httpx
    app_module.RAY_INSTALLED = False          # the dispatcher would otherwise start an embedded Ray cluster in this process
    real_post = httpx.Client.post

    def spy(self, url, *a, **kw):
        if "/api/compute/execute" in str(url) and str(url).startswith("http"):   # absolute URL = studio -> worker; TestClient uses relative paths
            WORKER_CALLS.append(kw.get("json", {}))
            raise httpx.ConnectError("worker spy: not connecting to real workers in tests")
        return real_post(self, url, *a, **kw)
    httpx.Client.post = spy


def sql(client, query, who, **extra):
    r = client.post("/api/sql/execute", json={"query": query, "catalog": "warehouse", **extra}, cookies=who)
    return r.json()


def test_sql_editor(client):
    print("\n1. SQL editor")
    q = "SELECT id, email, ssn FROM warehouse.hr.employees ORDER BY id"
    b, a = sql(client, q, BOB), sql(client, q, ADMIN)
    check("masked user gets masked rows", b.get("success") and not leaks(b["rows"]) and b["rows"][0]["email"] == "a***@example.com", b)
    check("...and the response says which columns were masked", {m["column"] for m in b.get("masked_columns", [])} == {"email", "ssn"}, b.get("masked_columns"))
    check("exempt admin gets raw rows and no mask list", a.get("success") and a["rows"][0]["email"] == "ada@example.com" and not a.get("masked_columns"), a)
    hist = client.get("/api/history?limit=20", cookies=ADMIN).json()
    texts = json.dumps(hist, default=str)
    check("history stores the user's own SQL, not the rewritten SQL", q in texts and "gov_mask" not in texts, texts[:200])
    rec = next((h for h in (hist.get("history") or hist.get("queries") or hist.get("items") or []) if h.get("query_text") == q and h.get("user") == "analyst_bob"), None)
    check("history records how many columns were masked", rec is not None and rec.get("masked_columns") == 2, rec)
    bob_only = "SELECT email FROM warehouse.hr.employees WHERE dept = 'eng' ORDER BY id"
    n_before = len(WORKER_CALLS)
    sql(client, bob_only, BOB)
    sent = [c["query"] for c in WORKER_CALLS[n_before:]]
    check("the SQL dispatched to a worker for a masked user is the rewritten SQL", sent and all("gov_mask" in q and q != bob_only for q in sent), sent)
    n_before = len(WORKER_CALLS)
    sql(client, "SELECT email FROM warehouse.hr.employees WHERE dept = 'ops' ORDER BY id", ADMIN)
    check("an exempt admin's SQL is dispatched untouched", [c["query"] for c in WORKER_CALLS[n_before:]][0].startswith("SELECT email FROM warehouse.hr.employees WHERE dept = 'ops'"))
    check("what the masked user's session sent to workers contains no raw values", not leaks(sent))

    for label, query in {"query()": "SELECT * FROM query('SELECT * FROM warehouse.hr.employees')", "gov_ function": "SELECT gov_mask_hash('123-45-6789')",
                         "macro redefinition": "CREATE OR REPLACE MACRO memory.main.gov_mask_email(v) AS v",
                         "read platform secrets": f"SELECT * FROM read_text('{TMP_WAREHOUSE}/.metadata/jwt_secret')",
                         "UPDATE masked table": "UPDATE warehouse.hr.employees SET dept='x' WHERE ssn='123-45-6789'"}.items():
        r = sql(client, query, BOB)
        check(f"blocked for a masked user: {label}", r.get("success") is False and r.get("governance_blocked") is True, r)
    check("the same statements are not gateway-blocked for an admin (secrets read)",
          not sql(client, f"SELECT length(content) FROM read_text('{TMP_WAREHOUSE}/.metadata/jwt_secret')", ADMIN).get("governance_blocked"))
    r = sql(client, "SELECT count(*) AS n FROM warehouse.hr.employees WHERE ssn = '123-45-6789'", BOB)
    check("no filter oracle through the API", r["rows"][0]["n"] == 0, r)
    r = client.post("/api/sql/execute", json={"query": q}, cookies={auth.COOKIE_NAME: "garbage"})
    check("bad session is 401, never admin", r.status_code == 401, r.status_code)


def test_dashboards(client):
    print("\n2. Dashboards and the result cache")
    r = client.post("/api/dashboards", json={"name": "HR"}, cookies=ADMIN)
    dash = r.json()["id"]
    widget_q = "SELECT email, ssn, dept FROM warehouse.hr.employees ORDER BY id"
    r = client.post(f"/api/dashboards/{dash}/widgets", json={"title": "People", "type": "table", "query": widget_q}, cookies=ADMIN)
    check("widget created (its preview ran as the admin)", r.status_code == 200, r.text[:200])
    widget_id = r.json().get("id") or r.json().get("widget", {}).get("id")

    ra = client.post(f"/api/dashboards/{dash}/query", json={"parameters": {}}, cookies=ADMIN).json()
    rb = client.post(f"/api/dashboards/{dash}/query", json={"parameters": {}}, cookies=BOB).json()
    rows_a, rows_b = ra["widgets"][0]["result"]["rows"], rb["widgets"][0]["result"]["rows"]
    check("admin sees raw, masked user sees masked, from the same warm cache key", rows_a[0]["email"] == "ada@example.com"
          and rows_b[0]["email"] == "a***@example.com" and not leaks(rows_b), (rows_a[:1], rows_b[:1]))
    rb2 = client.post(f"/api/dashboards/{dash}/query", json={"parameters": {}}, cookies=BOB).json()
    check("the masked user's second view is served from cache, still masked", rb2["widgets"][0]["result"].get("from_cache") and not leaks(rb2["widgets"][0]["result"]["rows"]))
    ra2 = client.post(f"/api/dashboards/{dash}/query", json={"parameters": {}}, cookies=ADMIN).json()
    check("...and the admin's cached copy is still raw (no poisoning either way)", ra2["widgets"][0]["result"]["rows"][0]["email"] == "ada@example.com")
    check("masked_columns is attached to the widget result", rb["widgets"][0]["result"].get("masked_columns"), rb["widgets"][0]["result"].keys())

    r = client.post("/api/dashboards/preview-widget", json={"query": widget_q}, cookies=BOB)
    check("preview-widget is masked", r.status_code == 200 and not leaks(r.json()), r.text[:200])
    r = client.get(f"/api/dashboards/{dash}/widgets/{widget_id}/export?format=csv", cookies=BOB)
    check("widget export is masked", r.status_code == 200 and not leaks(r.text), r.text[:200])
    r = client.get(f"/api/dashboards/{dash}/widgets/{widget_id}/export?format=csv", cookies=ADMIN)
    check("...and raw for the admin", "ada@example.com" in r.text)

    # a dashboard filter whose dimension is a masked column must not list raw values
    dashboards = app_module.load_dashboards_store()
    for d in dashboards:
        if d["id"] == dash:
            d["filters"] = [{"key": "who", "label": "Who", "table": "warehouse.hr.employees", "dimension": "ssn"}]
    app_module.save_dashboards_store(dashboards)
    fo = client.get(f"/api/dashboards/{dash}/filters", cookies=BOB).json()
    check("filter dropdown values are masked", not leaks(fo) and len(fo["options"]["who"]) > 1, fo)
    fo = client.get(f"/api/dashboards/{dash}/filters", cookies=ADMIN).json()
    check("...and raw for the admin", "123-45-6789" in fo["options"]["who"])


def test_previews_and_exports(client, delta_dir):
    print("\n3. Previews, version diff, export, profile")
    b = client.get("/api/table/hr/delta_emp/preview?catalog=warehouse&limit=10", cookies=BOB)
    a = client.get("/api/table/hr/delta_emp/preview?catalog=warehouse&limit=10", cookies=ADMIN)
    check("table preview is masked for a masked user", b.status_code == 200 and not leaks(b.json()) and b.json()["rows"], b.text[:200])
    check("...and raw for the admin", a.status_code == 200 and "ada@example.com" in json.dumps(a.json()))
    b = client.get("/api/table/hr/delta_emp/preview?catalog=warehouse&limit=10&version=0", cookies=BOB)
    check("time-travel preview (version param) is masked", b.status_code == 200 and not leaks(b.json()), b.text[:200])

    b = client.get("/api/table/hr/delta_emp/version/0/preview?catalog=warehouse", cookies=BOB)
    a = client.get("/api/table/hr/delta_emp/version/0/preview?catalog=warehouse", cookies=ADMIN)
    check("version preview endpoint is masked", b.status_code == 200 and not leaks(b.json()) and b.json()["sample_rows"], b.text[:200])
    check("...and raw for the admin", "ada@example.com" in json.dumps(a.json()))
    b = client.get("/api/table/hr/delta_emp/diff?v1=0&v2=1&catalog=warehouse", cookies=BOB)
    a = client.get("/api/table/hr/delta_emp/diff?v1=0&v2=1&catalog=warehouse", cookies=ADMIN)
    check("version diff samples are masked", b.status_code == 200 and not leaks(b.json()) and b.json()["added_rows_sample"], b.text[:300])
    check("...while counts stay real and the admin sees raw", b.json()["added_count"] == 1 and "dee@example.com" in json.dumps(a.json()), b.text[:300])

    r = client.post("/api/sql/export/parquet", json={"query": "SELECT email, ssn FROM warehouse.hr.employees"}, cookies=BOB)
    table = pq.read_table(io.BytesIO(r.content)).to_pylist() if r.status_code == 200 else r.text
    check("parquet export of a query is masked", r.status_code == 200 and not leaks(table), table)
    r = client.post("/api/sql/export/parquet", json={"query": "SELECT * FROM query('select 1')"}, cookies=BOB)
    check("...and refuses blocked SQL", r.status_code == 403, r.status_code)
    r = client.post("/api/sql/export/parquet", json={"query": "SELECT email FROM warehouse.hr.employees"}, cookies=ADMIN)
    check("admin export is raw", "ada@example.com" in json.dumps(pq.read_table(io.BytesIO(r.content)).to_pylist()))

    r = client.post("/api/sql/profile", json={"query": "SELECT email, ssn FROM warehouse.hr.employees", "catalog": "warehouse"}, cookies=BOB).json()
    check("profile returns masked rows", r.get("success") and not leaks(r.get("rows")), r.get("error") or r)
    r = client.post("/api/sql/profile", json={"query": "SELECT * FROM query('select 1')", "catalog": "warehouse"}, cookies=BOB).json()
    check("profile refuses blocked SQL", r.get("success") is False and r.get("governance_blocked"), r)

    qid = sql(client, "SELECT email FROM warehouse.hr.employees", ADMIN).get("query_id")
    r = client.get(f"/api/history/{qid}/profile", cookies=BOB)
    check("history profile re-runs under the requesting user's rights and does not return rows", r.status_code == 200 and not leaks(r.json()), r.text[:200])
    check("history profile needs a valid session", client.get(f"/api/history/{qid}/profile", cookies={auth.COOKIE_NAME: "garbage"}).status_code == 401)


def read_delta(table):
    from deltalake import DeltaTable
    return DeltaTable(os.path.join(TMP_WAREHOUSE, "hr", table)).to_pyarrow_table().to_pylist()


def test_background_work(client):
    print("\n4. Jobs, alerts and scheduled exports run as their owner")
    job = {"id": "job_bob_copy", "name": "bob copy", "created_by": "admin", "schedule_cron": "", "enabled": False,
           "tasks": [{"id": "t1", "name": "copy", "type": "sql", "depends_on": [],
                      "parameters": {"query": "CREATE OR REPLACE TABLE warehouse.hr.copy_bob AS SELECT * FROM warehouse.hr.delta_emp"}}]}
    r = client.post("/api/jobs", json=job, cookies=BOB)
    check("a job is created", r.status_code == 200, r.text[:200])
    check("the owner is set server-side (a client cannot claim to be admin)", r.json()["created_by"] == "analyst_bob", r.json())
    r = client.post("/api/jobs/job_bob_copy/run", cookies=ADMIN)
    check("running a job (even triggered by an admin) executes it as the owner", r.status_code == 200 and r.json().get("status") == "SUCCESS", r.text[:900])
    raw = read_delta("copy_bob")
    check("the table the masked owner's job created holds only masked values", raw and not leaks(raw), raw)
    check("another non-admin cannot modify or delete it", client.delete("/api/jobs/job_bob_copy", cookies=LEAD).status_code == 403
          and client.post("/api/jobs", json={**job, "name": "hijack"}, cookies=LEAD).status_code == 403)

    admin_job = {**job, "id": "job_admin_copy", "name": "admin copy", "tasks": [{"id": "t1", "name": "copy", "type": "sql", "depends_on": [],
                 "parameters": {"query": "CREATE OR REPLACE TABLE warehouse.hr.copy_admin AS SELECT * FROM warehouse.hr.delta_emp"}}]}
    client.post("/api/jobs", json=admin_job, cookies=ADMIN)
    client.post("/api/jobs/job_admin_copy/run", cookies=BOB)
    raw = read_delta("copy_admin")
    check("an admin-owned job keeps raw data even when a masked user triggers it", "ada@example.com" in str(raw), raw)

    from web import workflow
    legacy = {"id": "job_legacy", "name": "legacy", "enabled": False, "schedule_cron": "", "tasks": [
        {"id": "t1", "name": "copy", "type": "sql", "depends_on": [], "parameters": {"query": "CREATE OR REPLACE TABLE warehouse.hr.copy_legacy AS SELECT * FROM warehouse.hr.delta_emp"}}]}
    workflow.create_or_update_job(legacy)
    workflow.run_pipeline("job_legacy", trigger="MANUAL")
    raw = read_delta("copy_legacy")
    check("a legacy job with no recorded owner runs with least privilege", not leaks(raw), raw)
    nb = {"id": "job_nb", "name": "nb", "created_by": "analyst_bob", "enabled": False, "schedule_cron": "", "tasks": [
        {"id": "t1", "name": "nb", "type": "notebook", "depends_on": [], "parameters": {"notebook_path": "x.ipynb"}}]}
    workflow.create_or_update_job(nb)
    run = workflow.run_pipeline("job_nb", trigger="MANUAL")
    detail = json.dumps(run, default=str)
    check("notebook tasks are refused for a masked owner", "not available while masking" in detail or "FAILED" in detail, detail[:300])

    from web import alerts
    alerts.init_alerts_db()
    body = {"name": "max ssn", "custom_query": "SELECT max(ssn) AS m FROM warehouse.hr.delta_emp", "target_column": "m", "operator": "!=", "threshold_value": "zzz"}
    import asyncio

    async def make(data, owner):          # create_alert schedules a first evaluation on the running loop
        return alerts.create_alert(data, user_id=owner)
    a_bob = asyncio.run(make(body, "analyst_bob"))
    a_adm = asyncio.run(make({**body, "name": "max ssn admin"}, "admin"))
    ev_bob, ev_adm = alerts.execute_alert_check(a_bob["id"]), alerts.execute_alert_check(a_adm["id"])
    check("an alert owned by a masked user evaluates masked values", ev_bob["error"] is None and not leaks(ev_bob), ev_bob)
    check("an alert owned by an exempt admin evaluates raw values", "987-65-4321" in str(ev_adm["observed_value"]) or "111-22-3333" in str(ev_adm["observed_value"]), ev_adm)

    from web import scheduled_exports as se
    dash = client.post("/api/dashboards", json={"name": "Exports"}, cookies=ADMIN).json()["id"]
    client.post(f"/api/dashboards/{dash}/widgets", json={"title": "Emp", "type": "table", "query": "SELECT email, ssn FROM warehouse.hr.delta_emp"}, cookies=ADMIN)
    try:
        sid = se.create_schedule(dash, "bob export", "daily", format="csv", created_by="analyst_bob")
        se.execute_scheduled_export(sid)
        print("      schedule state:", {k: se.SCHEDULED_EXPORTS[sid].get(k) for k in ("last_run_status", "last_run_files", "last_export_dir")}, "EXPORTS_DIR", se.EXPORTS_DIR)
        files = glob.glob(os.path.join(se.EXPORTS_DIR, sid, "*", "*.csv"))
        text = open(files[0]).read() if files else ""
        check("a scheduled export owned by a masked user writes masked files", files and not leaks(text) and "a***@example.com" in text, (files, text[:120]))
        sid2 = se.create_schedule(dash, "admin export", "daily", format="csv", created_by="admin")
        se.execute_scheduled_export(sid2)
        files = glob.glob(os.path.join(se.EXPORTS_DIR, sid2, "*", "*.csv"))
        check("...and raw files for an exempt admin owner", files and "ada@example.com" in open(files[0]).read())
    except Exception as exc:
        check("scheduled export scenario ran", False, repr(exc))


def test_genie(client):
    print("\n5. Genie")
    from web import genie
    conn = app_module.get_duckrun_conn()
    ctx = genie.extract_schema_context(conn)
    emp = [t for t in ctx["tables"] if t["name"] in ("employees", "delta_emp")]
    check("LLM prompt context found the tagged tables", len(emp) == 2, [t["name"] for t in ctx["tables"]][:8])
    check("sample rows in the prompt context are masked (even though an admin could ask)", not leaks(ctx["schema_summary"]) and not leaks(ctx["tables"]),
          [t["sample_rows"] for t in emp])
    check("...but still useful: masked samples are present", any(t["sample_rows"] for t in emp))

    res_b = genie.execute_genie_sql("SELECT email, ssn FROM warehouse.hr.delta_emp ORDER BY id", principal={"username": "analyst_bob", "role": "user"})
    res_a = genie.execute_genie_sql("SELECT email, ssn FROM warehouse.hr.delta_emp ORDER BY id", principal={"username": "admin", "role": "admin"})
    res_n = genie.execute_genie_sql("SELECT email FROM warehouse.hr.delta_emp")
    check("generated SQL runs as the asker: masked for bob", res_b["success"] and not leaks(res_b["rows"]), res_b)
    check("...raw for the admin", res_a["success"] and "ada@example.com" in json.dumps(res_a["rows"], default=str))
    check("...and with no identity it runs with least privilege", res_n["success"] and not leaks(res_n["rows"]), res_n)
    res = genie.execute_genie_sql("SELECT * FROM query('select 1')", principal={"username": "analyst_bob", "role": "user"})
    check("LLM-written SQL cannot use gated statements", res["success"] is False and "governance" in res["error"].lower(), res)


def test_refused_features(client):
    print("\n6. Features that cannot be masked, and upload ids")
    for label, method, url, kw in (
        ("dbt model preview", "get", "/api/dbt/preview/anything", {}),
        ("dbt CTE preview", "get", "/api/dbt/cte-preview/m/c", {}),
        ("dbt source preview", "get", "/api/dbt/sources/s/t/preview", {}),
        ("dbt run", "post", "/api/dbt/run", {"json": {"action": "run"}}),
        ("OneLake direct query", "post", "/api/catalogs/onelake/x/query", {"json": {"table_name": "t", "sql": "select 1"}}),
        ("distributed Delta scan", "post", "/api/compute/warehouses/wh_starter/distributed-query", {"json": {"table_path": "hr/delta_emp"}}),
    ):
        r = getattr(client, method)(url, cookies=BOB, **kw)
        check(f"{label} is refused for a masked user", r.status_code == 403, (r.status_code, r.text[:120]))
        if label not in ("dbt run", "distributed Delta scan"):       # these have heavy side effects (a real dbt run / a Ray cluster)
            r = getattr(client, method)(url, cookies=ADMIN, **kw)
            check(f"{label} is not refused for the exempt admin", r.status_code != 403, (r.status_code, r.text[:120]))
    r = client.post("/api/compute/warehouses/wh_starter/distributed-query", json={"table_path": "/etc/passwd"}, cookies=LEAD)
    check("a plain scan of a non-table path is refused even without masking", r.status_code == 403)
    r = client.post("/api/ingest/create", json={"file_id": "../warehouse/hr/delta_emp/part.parquet", "table_name": "stolen"}, cookies=BOB)
    check("ingest upload id must be one the server issued (path traversal)", r.status_code == 400, (r.status_code, r.text[:120]))
    r = client.post("/api/ingest/create", json={"file_id": "/etc/passwd.csv", "table_name": "stolen"}, cookies=BOB)
    check("...including absolute paths", r.status_code == 400)


def test_worker(client, delta_dir):
    print("\n7. A compute worker executes the rewritten SQL")
    from web import compute_worker
    from web.compute_auth import compute_headers
    workers = TestClient(compute_worker.app)
    rewritten = gateway.govern_sql("SELECT email, ssn FROM warehouse.hr.delta_emp ORDER BY id", {"username": "analyst_bob", "role": "user"}).sql
    r = workers.post("/api/compute/execute", json={"query": rewritten, "warehouse_id": "wh_starter", "catalog": "warehouse"}, headers=compute_headers())
    body = r.json()
    check("the worker returns masked rows for the rewritten SQL", r.status_code == 200 and body.get("success") and not leaks(body.get("rows")) and body["rows"], body)
    check("the worker reports masks installed", workers.get("/api/compute/status", headers=compute_headers()).json().get("governance_masks_installed") is True)


def main():
    try:
        delta_dir = setup()
        install_worker_spy()
        client = TestClient(app_module.app)
        test_sql_editor(client)
        test_dashboards(client)
        test_previews_and_exports(client, delta_dir)
        test_background_work(client)
        test_genie(client)
        test_refused_features(client)
        test_worker(client, delta_dir)
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Governance Phase 4 checks passed.")


if __name__ == "__main__":
    main()
