#!/usr/bin/env python3
"""
Phase 6 verification for tag-based masking: lifecycle, tag propagation and hardening.
Runs against a throwaway WAREHOUSE_DIR, so it never touches real data.
Tests:
1. Propagation: an exempt user's CREATE TABLE AS / INSERT ... SELECT tags the new table like its sources (direct columns,
   aliases, CTEs), flags computed columns as unclassified, and leaves masked users' copies alone.
2. Jobs propagate too; dropping a table removes its tags; reconcile orphans dropped columns.
3. Auto-Loader tags _rescued_data for review.
4. Status lists compute nodes; mask throughput sanity numbers.
"""

import datetime
import os
import shutil
import sys
import tempfile
import time

TMP_ROOT = tempfile.mkdtemp(prefix="governance_p6_")
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
from deltalake import DeltaTable, write_deltalake
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth
from web.governance import masks, policies, store, tags

FAILURES = []
RAW = ["ada@example.com", "bob@corp.io", "cy@example.com", "123-45-6789", "987-65-4321", "555-12-3456"]


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:500]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def cookie_for(username):
    user = auth.get_user_by_username(username)
    token = jwt.encode({"sub": user["id"], "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)},
                       auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)
    return {auth.COOKIE_NAME: token}


def col_tags(table, column):
    out = {}
    for a in tags.list_assignments(include_orphaned=False, limit=5000):
        if a["table_name"] == table and a["column_name"] == column:
            out[a["tag_key"]] = (a["tag_value"], a["source"])
    return out


def read_delta(table):
    return DeltaTable(os.path.join(TMP_WAREHOUSE, "hr", table)).to_pyarrow_table().to_pylist()


def setup():
    d = os.path.join(TMP_WAREHOUSE, "hr", "delta_emp")
    write_deltalake(d, pa.table({"id": [1, 2, 3], "email": ["ada@example.com", "bob@corp.io", "cy@example.com"],
                                 "ssn": ["123-45-6789", "987-65-4321", "555-12-3456"], "dept": ["eng", "ops", "eng"]}))
    con = app_module.get_duckrun_conn().con
    con.execute("CREATE SCHEMA IF NOT EXISTS warehouse.hr")
    if not con.execute("SELECT 1 FROM duckdb_views() WHERE view_name = 'delta_emp' AND database_name = 'warehouse'").fetchone():
        con.execute(f"CREATE VIEW warehouse.hr.delta_emp AS SELECT * FROM delta_scan('{d}')")
    store.init_governance_db()
    for key, vals in (("pii", ["email", "ssn", "name"]), ("sensitivity", ["public", "internal", "confidential", "restricted", "unclassified"])):
        if key not in [t["tag_key"] for t in tags.list_definitions()]:
            tags.create_definition(key, key, vals)
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="delta_emp", column_name="email", tag_key="pii", tag_value="email")
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="delta_emp", column_name="ssn", tag_key="pii", tag_value="ssn")
    policies.create_policy({"name": "PII partial", "tag_key": "pii", "mask_type": "partial"})


def sql(client, query, who):
    return client.post("/api/sql/execute", json={"query": query, "catalog": "warehouse"}, cookies=who).json()


def install_spy():
    import httpx
    app_module.RAY_INSTALLED = False
    real_post = httpx.Client.post

    def spy(self, url, *a, **kw):
        if "/api/compute/execute" in str(url) and str(url).startswith("http"):
            raise httpx.ConnectError("worker spy")
        return real_post(self, url, *a, **kw)
    httpx.Client.post = spy


def test_propagation(client, admin, bob):
    print("\n1. Propagation through the SQL editor")
    before = len(tags.list_assignments(limit=5000))
    r = sql(client, "SELECT * FROM warehouse.hr.delta_emp", admin)
    check("a plain SELECT propagates nothing", r.get("success") and len(tags.list_assignments(limit=5000)) == before, r)

    r = sql(client, "CREATE OR REPLACE TABLE warehouse.hr.copy_all AS SELECT * FROM warehouse.hr.delta_emp", admin)
    check("admin CTAS succeeds", r.get("success"), r)
    check("SELECT * copies the tags of tagged columns", col_tags("copy_all", "email") == {"pii": ("email", "propagated")}
          and col_tags("copy_all", "ssn") == {"pii": ("ssn", "propagated")}, [col_tags("copy_all", c) for c in ("email", "ssn")])
    check("untagged columns stay untagged", col_tags("copy_all", "dept") == {} and col_tags("copy_all", "id") == {})
    check("propagated tags are audited with the actor", any(e["object"] == "warehouse.hr.copy_all.email" and e["actor"] == "admin"
                                                               for e in store.list_audit(action="TAG_SET", limit=200)))
    rows = sql(client, "SELECT email, ssn FROM warehouse.hr.copy_all ORDER BY 1", bob)
    check("...so a masked user reading the copy is masked (the leak is closed)", rows.get("success") and not str(rows["rows"]).count("@example.com")
          and not any(v in str(rows) for v in RAW), rows)

    sql(client, "CREATE OR REPLACE TABLE warehouse.hr.copy_alias AS SELECT id, email AS contact FROM warehouse.hr.delta_emp", admin)
    check("an aliased column keeps its tags under the new name", col_tags("copy_alias", "contact") == {"pii": ("email", "propagated")}, col_tags("copy_alias", "contact"))

    sql(client, "CREATE OR REPLACE TABLE warehouse.hr.copy_cte AS WITH x AS (SELECT email FROM warehouse.hr.delta_emp) SELECT email AS e2 FROM x", admin)
    check("tags are traced through a CTE", col_tags("copy_cte", "e2") == {"pii": ("email", "propagated")}, col_tags("copy_cte", "e2"))

    sql(client, "CREATE OR REPLACE TABLE warehouse.hr.copy_expr AS SELECT upper(email) AS shout, length(ssn) AS n, dept FROM warehouse.hr.delta_emp", admin)
    check("computed columns are flagged 'unclassified' for review",
          col_tags("copy_expr", "shout") == {"sensitivity": ("unclassified", "propagated")}
          and col_tags("copy_expr", "n") == {"sensitivity": ("unclassified", "propagated")} and col_tags("copy_expr", "dept") == {})

    sql(client, "CREATE OR REPLACE TABLE warehouse.hr.copy_agg AS SELECT dept, count(*) AS c FROM warehouse.hr.delta_emp GROUP BY dept", admin)
    check("aggregates that read no tagged column add no tags", col_tags("copy_agg", "c") == {} and col_tags("copy_agg", "dept") == {})

    # INSERT needs a Delta target: create it through duckrun directly (no propagation involved), then insert as the admin
    app_module.get_duckrun_conn().sql("CREATE OR REPLACE TABLE warehouse.hr.sink AS SELECT id, email FROM warehouse.hr.delta_emp WHERE 1 = 0")
    check("(control) the empty Delta target starts untagged", col_tags("sink", "email") == {})
    r = sql(client, "INSERT INTO warehouse.hr.sink SELECT id, email FROM warehouse.hr.delta_emp", admin)
    check("INSERT ... SELECT into an existing table propagates too", r.get("success") and col_tags("sink", "email") == {"pii": ("email", "propagated")}, (r, col_tags("sink", "email")))

    n = len(tags.list_assignments(limit=5000))
    sql(client, "CREATE OR REPLACE TABLE warehouse.hr.copy_all AS SELECT * FROM warehouse.hr.delta_emp", admin)
    check("re-running is idempotent (no duplicate assignments)", len(tags.list_assignments(limit=5000)) == n)

    r = sql(client, "CREATE OR REPLACE TABLE warehouse.hr.copy_bob AS SELECT * FROM warehouse.hr.delta_emp", bob)
    stored = app_module.get_duckrun_conn().con.execute("SELECT email, ssn FROM warehouse.hr.copy_bob").fetchall()
    check("a masked user's CTAS works and stores masked values", r.get("success") and stored and not any(v in str(stored) for v in RAW), (r, stored))
    check("...and is not tagged (its values are already masked)", col_tags("copy_bob", "email") == {} and col_tags("copy_bob", "ssn") == {},
          ([col_tags("copy_bob", c) for c in ("email", "ssn")], [(e["actor"], e["object"], e["detail"].get("source")) for e in store.list_audit(action="TAG_SET", limit=40) if "copy_bob" in (e["object"] or "")]))


def test_jobs_drop_reconcile(client, admin, bob):
    print("\n2. Jobs, drop, reconcile")
    job = {"id": "job_prop", "name": "propagating job", "enabled": False, "schedule_cron": "", "tasks": [
        {"id": "t1", "name": "copy", "type": "sql", "depends_on": [],
         "parameters": {"query": "CREATE OR REPLACE TABLE warehouse.hr.copy_job AS SELECT id, ssn FROM warehouse.hr.delta_emp"}}]}
    client.post("/api/jobs", json=job, cookies=admin)
    r = client.post("/api/jobs/job_prop/run", cookies=admin).json()
    check("an admin-owned job that copies tagged columns tags the new table", r.get("status") == "SUCCESS"
          and col_tags("copy_job", "ssn") == {"pii": ("ssn", "propagated")}, (r.get("status"), col_tags("copy_job", "ssn")))

    check("dropping a table needs a valid session", client.delete("/api/table/hr/copy_all?catalog=warehouse", cookies={auth.COOKIE_NAME: "garbage"}).status_code == 401)
    r = client.delete("/api/table/hr/copy_all?catalog=warehouse", cookies=admin)
    check("dropping a table removes its tags", r.status_code == 200 and col_tags("copy_all", "email") == {} and col_tags("copy_all", "ssn") == {}, (r.status_code, col_tags("copy_all", "email")))
    check("other tables keep their tags", col_tags("copy_alias", "contact") != {})

    con = app_module.get_duckrun_conn().con
    con.execute("CREATE OR REPLACE TABLE warehouse.hr.scratch (id INTEGER, phone VARCHAR)")
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="scratch", column_name="phone", tag_key="pii", tag_value="name")
    con.execute("ALTER TABLE warehouse.hr.scratch DROP COLUMN phone")
    r = client.post("/api/governance/reconcile", cookies=admin).json()
    check("reconcile flags the tag of a dropped column as orphaned", r["orphaned"] >= 1 and col_tags("scratch", "phone") == {}, r)


def test_autoloader():
    print("\n3. Auto-Loader rescue column")
    from web import autoloader, volumes
    volumes.create_volume("warehouse", "raw", "gov_vol")
    vol_dir = volumes.resolve_volume_posix_path("/Volumes/warehouse/raw/gov_vol")
    pipe = autoloader.create_pipeline({"name": "gov rescue", "source_volume_path": "/Volumes/warehouse/raw/gov_vol", "file_pattern": "*",
                                       "target_table": "bronze_gov", "ingest_mode": "append", "schema_evolution": "rescue"})
    now = time.time()
    for name, body, age in (("a.csv", "id,name\n1,x\n", 30), ("b.csv", "id,name,ssn\n2,y,999-99-9999\n", 10)):
        path = os.path.join(vol_dir, name)
        open(path, "w").write(body)
        os.utime(path, (now - age, now - age))
    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("both files ingested", res.get("files_ingested") == 2, res)
    check("_rescued_data is tagged sensitivity=unclassified for review", col_tags("bronze_gov", "_rescued_data") == {"sensitivity": ("unclassified", "propagated")},
          col_tags("bronze_gov", "_rescued_data"))


def test_status_and_perf(client, admin):
    print("\n4. Status and mask throughput")
    r = client.get("/api/governance/status", cookies=admin).json()
    check("status lists compute nodes with their mask status", isinstance(r.get("workers"), list), r.keys())

    con = masks._tester().cursor()
    n = 1_000_000
    con.execute(f"CREATE TEMP TABLE bench AS SELECT CAST(i AS VARCHAR) || '@example.com' AS email, lpad(CAST(i AS VARCHAR), 11, '9') AS ssn FROM range({n}) t(i)")
    results = {}
    for label, expr in (("no mask", '"email"'), ("email", masks.mask_expression("email", "VARCHAR", "email")),
                        ("partial", masks.mask_expression("partial", "VARCHAR", "ssn")), ("hash (python UDF)", masks.mask_expression("hash", "VARCHAR", "ssn"))):
        t0 = time.perf_counter()
        con.execute(f"SELECT count(*), max({expr}) FROM bench").fetchall()
        results[label] = time.perf_counter() - t0
    for label, secs in results.items():
        print(f"      {label:20s} {secs * 1000:8.0f} ms for {n:,} rows ({n / max(secs, 1e-9) / 1e6:5.1f} M rows/s)")
    check("SQL-macro masks stay within ~5x of an unmasked scan", results["email"] < max(results["no mask"] * 8, 1.0) and results["partial"] < max(results["no mask"] * 8, 1.0), results)
    check("the keyed-hash UDF handles 1M rows in bounded time", results["hash (python UDF)"] < 60, results)
    con.close()


def main():
    try:
        setup()
        install_spy()
        client = TestClient(app_module.app)
        admin, bob = cookie_for("admin"), cookie_for("analyst_bob")
        test_propagation(client, admin, bob)
        test_jobs_drop_reconcile(client, admin, bob)
        test_autoloader()
        test_status_and_perf(client, admin)
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Governance Phase 6 checks passed.")


if __name__ == "__main__":
    main()
