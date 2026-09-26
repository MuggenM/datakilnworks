#!/usr/bin/env python3
"""
Shallow clone through the studio: REST endpoint and SQL editor statement, on a throwaway WAREHOUSE_DIR.
Tests: clone works and is queryable; SQL syntax (VERSION AS OF, OR REPLACE, IF NOT EXISTS); governance -- a user a
masking policy applies to cannot clone (it would expose raw files), and the clone carries the source's tags, including
ones the source only inherited from its schema, so masking still applies to it; permissions; history + lineage;
the clone survives the source being dropped through the API.
"""
import datetime
import os
import shutil
import sys
import tempfile

TMP_ROOT = tempfile.mkdtemp(prefix="clone_api_")
TMP_WAREHOUSE = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(TMP_WAREHOUSE)
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_NOTEBOOK_EXECUTION", "JWT_SECRET_KEY", "COMPUTE_TOKEN", "GOVERNANCE_ENFORCEMENT"):
    os.environ.pop(var, None)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import jwt
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth
from web.governance import policies, tags

app_module.RAY_INSTALLED = False
import httpx
_real_post = httpx.Client.post


def _spy(self, url, *a, **kw):
    if "/api/compute/execute" in str(url) and str(url).startswith("http"):
        raise httpx.ConnectError("no real workers in tests")
    return _real_post(self, url, *a, **kw)


httpx.Client.post = _spy
auth.create_user("analyst_bob", "userpassword123", "Bob", role="user")
with auth.get_db_connection() as _c:
    _c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")

FAILURES = []
RAW = ["ada@example.com", "bob@corp.io", "cy@example.com"]


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def cookie_for(username):
    user = auth.get_user_by_username(username)
    now = datetime.datetime.now(datetime.timezone.utc)
    return {auth.COOKIE_NAME: jwt.encode({"sub": user["id"], "iat": int(now.timestamp()), "exp": now + datetime.timedelta(hours=1)},
                                         auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)}


def main():
    try:
        client = TestClient(app_module.app)
        admin, bob = cookie_for("admin"), cookie_for("analyst_bob")
        for schema in ("hr", "analytics"):
            os.makedirs(os.path.join(TMP_WAREHOUSE, schema), exist_ok=True)
        emp = os.path.join(TMP_WAREHOUSE, "hr", "employees")
        write_deltalake(emp, pa.table({"id": [1, 2, 3], "email": RAW, "dept": ["eng", "eng", "ops"]}))
        write_deltalake(emp, pa.table({"id": [4], "email": ["dee@example.com"], "dept": ["ops"]}), mode="append")
        app_module.get_duckrun_conn().refresh()
        if "pii" not in [t["tag_key"] for t in tags.list_definitions()]:
            tags.create_definition("pii", "personal", ["email"])
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="email", tag_key="pii", tag_value="email")
        if "domain" not in [t["tag_key"] for t in tags.list_definitions()]:
            tags.create_definition("domain", "business domain", [])
        tags.set_tag(catalog="warehouse", schema_name="hr", tag_key="domain", tag_value="people")           # inherited by hr.employees
        policies.create_policy({"name": "Mask PII", "tag_key": "pii", "mask_type": "email", "except_roles": ["admin"]})

        def sql(q, cookies=admin, catalog="warehouse"):
            return client.post("/api/sql/execute", json={"query": q, "catalog": catalog}, cookies=cookies).json()

        def emails(table, cookies):
            r = sql(f"SELECT email FROM {table} ORDER BY id", cookies)
            return [row["email"] for row in r.get("rows", [])], r

        print("1. REST clone")
        r = client.post("/api/table/hr/employees/clone", json={"target_table": "emp_copy"}, cookies=admin)
        check("admin clones a table", r.status_code == 200 and r.json()["created"] and r.json()["target"] == "warehouse.hr.emp_copy", r.text)
        check("data files are shared, not copied", all(os.stat(os.path.join(d, f)).st_nlink >= 2 for d, _, fs in os.walk(os.path.join(TMP_WAREHOUSE, "hr", "emp_copy")) for f in fs if f.endswith(".parquet")))
        got, raw = emails("warehouse.hr.emp_copy", admin)
        check("the clone is queryable in the SQL editor", got == RAW + ["dee@example.com"], raw)
        check("a second clone into another schema works", client.post("/api/table/hr/employees/clone", json={"target_table": "emp_a", "target_schema": "analytics"}, cookies=admin).status_code == 200)
        check("an existing target is a 400", client.post("/api/table/hr/employees/clone", json={"target_table": "emp_copy"}, cookies=admin).status_code == 400)
        check("a missing source is a 404", client.post("/api/table/hr/ghost/clone", json={"target_table": "g2"}, cookies=admin).status_code == 404)
        check("a missing target schema is a 404", client.post("/api/table/hr/employees/clone", json={"target_table": "z", "target_schema": "nope"}, cookies=admin).status_code == 404)
        check("no session is 401", client.post("/api/table/hr/employees/clone", json={"target_table": "q"}, cookies={auth.COOKIE_NAME: "garbage"}).status_code in (401, 403))

        print("\n2. Governance")
        check("a user a masking policy applies to cannot clone", client.post("/api/table/hr/employees/clone", json={"target_table": "bob_copy"}, cookies=bob).status_code == 403
              and not os.path.exists(os.path.join(TMP_WAREHOUSE, "hr", "bob_copy")))
        got, _ = emails("warehouse.hr.employees", bob)
        check("(control) bob sees masked emails on the source", got and all(e not in RAW for e in got), got)
        got, _ = emails("warehouse.hr.emp_copy", bob)
        check("the clone is masked for bob too: the column tag was copied", got and all(e not in RAW and e != "dee@example.com" for e in got), got)
        eff_src = tags.effective_tags("warehouse", "hr", "employees", ["email"])["email"]
        eff_dst = tags.effective_tags("warehouse", "analytics", "emp_a", ["email"])["email"]
        check("effective tags equal the source's, including the schema-inherited one, in another schema", eff_dst == {k: {"value": v["value"], "level": eff_dst[k]["level"]} for k, v in eff_src.items()} and "domain" in eff_dst and "pii" in eff_dst, (eff_src, eff_dst))
        got, _ = emails("warehouse.analytics.emp_a", bob)
        check("masked for bob in the other schema as well", got and all(e not in RAW for e in got), got)

        print("\n3. SQL syntax")
        r = sql("CREATE TABLE hr.emp_v0 SHALLOW CLONE hr.employees VERSION AS OF 0")
        check("CREATE TABLE ... SHALLOW CLONE ... VERSION AS OF works", r.get("success") is True and "version 0" in r.get("message", ""), r)
        check("...and holds the old version's rows", len(sql("SELECT * FROM warehouse.hr.emp_v0")["rows"]) == 3 and len(sql("SELECT * FROM warehouse.hr.employees")["rows"]) == 4)
        r = sql("CREATE TABLE hr.emp_v0 SHALLOW CLONE hr.employees")
        check("an existing target errors in SQL", r.get("success") is False and "already exists" in r.get("error", ""), r)
        check("IF NOT EXISTS is a no-op", sql("CREATE TABLE IF NOT EXISTS hr.emp_v0 SHALLOW CLONE hr.employees").get("success") is True and len(sql("SELECT * FROM warehouse.hr.emp_v0")["rows"]) == 3)
        check("OR REPLACE replaces it", sql("CREATE OR REPLACE TABLE hr.emp_v0 SHALLOW CLONE hr.employees").get("success") is True and len(sql("SELECT * FROM warehouse.hr.emp_v0")["rows"]) == 4)
        check("three-part names work", sql("CREATE TABLE warehouse.hr.emp_3 SHALLOW CLONE warehouse.hr.employees", catalog="other").get("success") is True)
        r = sql("CREATE TABLE emp_bare SHALLOW CLONE employees")
        check("a bare table name asks for a schema", r.get("success") is False and "schema" in r.get("error", "").lower(), r)
        r = sql("CREATE TABLE hr.bob_sql SHALLOW CLONE hr.employees", bob)
        check("a masked user is refused in SQL too, with a governance message", r.get("success") is False and "governance" in r.get("error", "").lower(), r)
        hist = client.get("/api/history?limit=50", cookies=admin).json()
        rows = hist.get("history", hist) if isinstance(hist, dict) else hist
        check("clone statements are recorded in query history", any("SHALLOW CLONE" in (h.get("query_text") or h.get("query") or "") for h in rows), str(rows)[:200])

        print("\n3b. Across catalogs")
        from web import warehouses
        warehouses.create_catalog("Sandbox", "sandbox_cat")
        warehouses.create_catalog_schema("sandbox_cat", "scratch")
        r = client.post("/api/table/hr/emp_copy/clone", json={"target_table": "emp_x", "target_schema": "scratch", "target_catalog": "sandbox_cat"}, cookies=admin)
        check("clone into another catalog", r.status_code == 200 and r.json()["target"] == "sandbox_cat.scratch.emp_x", r.text)
        check("...with the source's tags on it", "pii" in tags.effective_tags("sandbox_cat", "scratch", "emp_x", ["email"])["email"])
        check("clone from another catalog back", client.post("/api/table/scratch/emp_x/clone", json={"catalog": "sandbox_cat", "target_table": "emp_back", "target_schema": "hr", "target_catalog": "warehouse"}, cookies=admin).status_code == 200)
        check("an unknown target catalog is a 404", client.post("/api/table/hr/emp_copy/clone", json={"target_table": "z", "target_catalog": "nope"}, cookies=admin).status_code == 404)

        print("\n4. Replace prunes stale tags; source can be dropped")
        tags.set_tag(catalog="warehouse", schema_name="analytics", table_name="emp_a", column_name="dept", tag_key="pii", tag_value="email")
        r = client.post("/api/table/hr/employees/clone", json={"target_table": "emp_a", "target_schema": "analytics", "replace": True}, cookies=admin)
        stale = [a for a in tags.list_assignments(catalog="warehouse") if a["table_name"] == "emp_a" and a.get("column_name") == "dept"]
        check("OR REPLACE succeeds and drops a tag the new table shouldn't have", r.status_code == 200 and not stale, (r.text, stale))
        check("...while keeping the copied ones", "pii" in tags.effective_tags("warehouse", "analytics", "emp_a", ["email"])["email"])
        r = client.delete("/api/table/hr/employees", cookies=admin)
        check("dropping the source through the API works", r.status_code == 200 and not os.path.exists(emp))
        check("the clone still reads fully afterwards", len(sql("SELECT * FROM warehouse.hr.emp_copy")["rows"]) == 4 and len(DeltaTable(os.path.join(TMP_WAREHOUSE, "hr", "emp_copy")).to_pyarrow_table()) == 4)
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All shallow clone API checks passed.")


if __name__ == "__main__":
    main()
