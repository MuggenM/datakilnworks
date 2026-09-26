#!/usr/bin/env python3
"""
dbt output is closed by default, opened deliberately, and keeps its sources' governance (web/dbt_governance.py).
Run inside the studio container; temp warehouse + temp dbt project only (never the real ones).
Tests: dbt writes to <warehouse>/<schema>/<model> and the schema is tagged access=closed BEFORE the first table exists;
the deny-all policy is created once; exempt roles read, other roles see no rows; only an admin can open / close a table;
opening keeps masking (source tags reach the output through renames, aggregates and CTEs); removing a source tag removes the
derived one on the next run; a model in a new schema (+schema) is closed automatically; an explicitly open schema is not
overwritten; DBT_CLOSED_BY_DEFAULT=false switches it all off.
"""
import datetime
import json
import os
import shutil
import sys
import tempfile

TMP = tempfile.mkdtemp(prefix="dbt_gov_")
WH, PROJ = os.path.join(TMP, "warehouse"), os.path.join(TMP, "dbt_project")
os.makedirs(os.path.join(WH, "dbo"))
os.environ.update({"WAREHOUSE_DIR": WH, "DBT_PROJECT_DIR": PROJ, "INIT_ADMIN_USERNAME": "admin",
                   "INIT_ADMIN_PASSWORD_HASH": "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"})
for var in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "DBT_CLOSED_BY_DEFAULT"):
    os.environ.pop(var, None)
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, REPO)

import jwt
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth, dbt_governance
from web.governance import policies, row_filters, store, tags

app_module.RAY_INSTALLED = False          # the SQL editor would otherwise start an embedded Ray cluster in this process
import httpx
_real_post = httpx.Client.post


def _spy(self, url, *a, **kw):            # no real compute workers in tests: the studio executes locally
    if "/api/compute/execute" in str(url) and str(url).startswith("http"):
        raise httpx.ConnectError("no real workers in tests")
    return _real_post(self, url, *a, **kw)


httpx.Client.post = _spy

FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:400]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def cookie_for(username):
    user = auth.get_user_by_username(username)
    now = datetime.datetime.now(datetime.timezone.utc)
    return {auth.COOKIE_NAME: jwt.encode({"sub": user["id"], "iat": int(now.timestamp()), "exp": now + datetime.timedelta(hours=1)},
                                         auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)}


def main():
    try:
        for d in ("models", "macros", "seeds", "tests"):
            if os.path.isdir(os.path.join(REPO, "dbt_project", d)):
                shutil.copytree(os.path.join(REPO, "dbt_project", d), os.path.join(PROJ, d))
        for f in ("dbt_project.yml", "profiles.yml"):
            shutil.copy(os.path.join(REPO, "dbt_project", f), PROJ)
        for base, _, files in os.walk(PROJ):
            for f in files:
                if f.endswith((".yml", ".sql")):
                    p = os.path.join(base, f)
                    t = open(p).read()
                    if "/workspace/warehouse" in t:
                        open(p, "w").write(t.replace("/workspace/warehouse", WH))
        # one more model: a renamed, aggregated pii-derived column, materialised as a table
        open(os.path.join(PROJ, "models", "marts", "fct_names.sql"), "w").write(
            "{{ config(materialized='table') }}\nselect department, max(employee_name) as top_person, count(*) as n from {{ ref('stg_employees') }} group by department\n")

        w = lambda t, **c: write_deltalake(os.path.join(WH, "dbo", t), pa.table(c))
        w("silver_employees", name=["ann", "bob", "cy", "di"], department=["eng", "eng", "ops", "ops"], salary=[100.0, 120.0, 80.0, 90.0],
          hire_date=[datetime.date(2020, 1, 1)] * 4, bonus_estimate=[1.0, 2.0, 3.0, 4.0])
        w("dim_products", product_id=[1, 2, 3], product_name=["x", "y", "z"], category=["c1", "c1", "c2"], price=[10.0, 20.0, 30.0], stock_qty=[10, 40, 100])
        w("nyse_tickers", ticker=["A", "B"], cap=[1, 2])
        w("gold_telemetry_kpis", device=["d1"], v=[1.0])
        app_module.get_duckrun_conn().refresh()

        auth.create_user("pat", "patpassword1", "Pat", role="power_user")
        auth.create_user("uma", "umapassword1", "Uma", role="user")
        with auth.get_db_connection() as c:
            c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
        admin, pat, uma = cookie_for("admin"), cookie_for("pat"), cookie_for("uma")
        client = TestClient(app_module.app)

        store.init_governance_db()
        # a governed source: silver_employees.name is pii (masked for everyone but admin), the table carries a table-level tag
        tags.set_tag(catalog="warehouse", schema_name="dbo", table_name="silver_employees", column_name="name", tag_key="pii", tag_value="name")
        tags.set_tag(catalog="warehouse", schema_name="dbo", table_name="silver_employees", tag_key="sensitivity", tag_value="confidential")
        policies.create_policy({"name": "Mask PII", "tag_key": "pii", "mask_type": "redact", "except_roles": ["admin"]})

        def sql(q, cookies):
            return client.post("/api/sql/execute", json={"query": q, "catalog": "warehouse"}, cookies=cookies).json()

        def tag_of(schema, table, column=""):
            return {k: v["value"] for k, v in tags.effective_tags("warehouse", schema, table, [column]).get(column, {}).items()} if column else \
                   {k: v["value"] for k, v in tags.effective_table_tags("warehouse", schema, table).items()}

        print("1. Before anything runs")
        check("no access tag and no closing policy exist yet", not any(p["name"] == dbt_governance.POLICY_NAME for p in row_filters.list_row_policies()))

        print("\n2. A dbt run writes into the lakehouse, closed from the first moment")
        seen = {}
        real_run = app_module.subprocess.run if hasattr(app_module, "subprocess") else None
        import subprocess as sp
        orig = sp.run

        def spy(cmd, *a, **kw):
            if isinstance(cmd, list) and cmd[:2] == ["dbt", "run"] and "before" not in seen:
                seen["before"] = tags.effective_table_tags("warehouse", "dbt", "fct_department_payroll").get("access")
                seen["table_existed"] = os.path.isdir(os.path.join(WH, "dbt", "fct_department_payroll"))
            return orig(cmd, *a, **kw)
        sp.run = spy
        try:
            r = client.post("/api/dbt/run", json={"action": "run"}, cookies=admin)
        finally:
            sp.run = orig
        run = r.json()
        check("the run succeeds", r.status_code == 200 and run["status"] == "SUCCESS", (r.status_code, str(run)[:400]))
        check("tables are Delta under <warehouse>/dbt/<model> (the profile's schema)", all(os.path.isdir(os.path.join(WH, "dbt", m, "_delta_log")) for m in ("fct_department_payroll", "fct_inventory_health", "fct_names")))
        check("the schema was tagged access=closed BEFORE dbt created the first table", seen.get("before", {}) and seen["before"]["value"] == "closed" and seen["table_existed"] is False, seen)
        check("one deny-all row policy was created for the tag", [p["name"] for p in row_filters.list_row_policies()].count(dbt_governance.POLICY_NAME) == 1)
        pol = next(p for p in row_filters.list_row_policies() if p["name"] == dbt_governance.POLICY_NAME)
        check("...exempting admin and power_user only", pol["except_roles"] == ["admin", "power_user"] and pol["tag_value"] == "closed", pol)
        check("the run record says what governance did", run["governance"]["before"].get("schemas_closed") == ["dbt"] and "tables" in run["governance"]["after"], run["governance"])
        r2 = client.post("/api/dbt/run", json={"action": "run"}, cookies=admin).json()
        check("a second run does not create a second policy or re-close", r2["governance"]["before"]["schemas_closed"] == [] and [p["name"] for p in row_filters.list_row_policies()].count(dbt_governance.POLICY_NAME) == 1, r2["governance"]["before"])

        print("\n3. Who sees what while closed")
        rows = lambda who, table="fct_department_payroll": len(sql(f"SELECT * FROM warehouse.dbt.{table}", who).get("rows", []))
        check("admin reads the rows", rows(admin) == 2, sql("SELECT * FROM warehouse.dbt.fct_department_payroll", admin))
        check("power_user (exempt) reads them too", rows(pat) == 2)
        check("a plain user sees the table but no rows", rows(uma) == 0, sql("SELECT * FROM warehouse.dbt.fct_department_payroll", uma))
        check("...in every dbt table", rows(uma, "fct_inventory_health") == 0 and rows(uma, "fct_names") == 0)

        print("\n4. Source tags follow the data")
        names = tag_of("dbt", "fct_names", "top_person")
        check("a renamed, aggregated column derived from a pii column carries pii", names.get("pii") == "name", names)
        check("count(*) with no source column stays untagged", not tag_of("dbt", "fct_names", "n").get("pii"))
        check("a column that only groups on department is untagged", "pii" not in tag_of("dbt", "fct_names", "department"))
        check("the source table's table-level tag reaches the output table", tag_of("dbt", "fct_names").get("sensitivity") == "confidential", tag_of("dbt", "fct_names"))
        check("an output table not derived from tagged data gets no derived tags", "sensitivity" not in tag_of("dbt", "fct_inventory_health") and "pii" not in tag_of("dbt", "fct_inventory_health", "category"))

        print("\n5. Deliberate opening")
        check("a plain user cannot open a table", client.post("/api/dbt/models/fct_names/open", cookies=uma).status_code == 403)
        check("a power_user cannot either", client.post("/api/dbt/models/fct_names/open", cookies=pat).status_code == 403)
        listing = {m["name"]: m for m in client.get("/api/dbt/models", cookies=admin).json()["models"]}
        check("the model list shows each table's access state", listing["fct_names"]["access"]["access"] == "closed" and listing["fct_names"]["lakehouse_table"] == "warehouse.dbt.fct_names" and "access" not in listing["stg_employees"], listing["fct_names"].get("access"))
        r = client.post("/api/dbt/models/fct_names/open", cookies=admin)
        check("an admin opens one table", r.status_code == 200 and r.json()["access"] == "open" and r.json()["table"] == "warehouse.dbt.fct_names", r.text)
        check("...and reports which columns carry source tags", "top_person" in r.json()["columns_with_source_tags"], r.json())
        res = sql("SELECT department, top_person, n FROM warehouse.dbt.fct_names ORDER BY department", uma)
        check("the plain user now sees rows, with the pii-derived column still masked", len(res.get("rows", [])) == 2 and all(row["top_person"] not in ("ann", "bob", "cy", "di") for row in res["rows"]), res)
        check("only that table opened: its siblings stay closed", rows(uma) == 0 and rows(uma, "fct_inventory_health") == 0)
        check("admin still sees the real values", {row["top_person"] for row in sql("SELECT top_person FROM warehouse.dbt.fct_names", admin)["rows"]} == {"bob", "di"})
        client.post("/api/dbt/run", json={"action": "run"}, cookies=admin)
        check("re-running dbt keeps a deliberately opened table open", rows(uma, "fct_names") == 2)
        r = client.post("/api/dbt/models/fct_names/close", cookies=admin)
        check("an admin closes it again", r.status_code == 200 and rows(uma, "fct_names") == 0)
        check("opening a view model or an unknown model is refused", client.post("/api/dbt/models/stg_employees/open", cookies=admin).status_code == 400 and client.post("/api/dbt/models/nope/open", cookies=admin).status_code == 404)

        print("\n6. Derived tags are refreshed, not left behind")
        tags.unset_tag(catalog="warehouse", schema_name="dbo", table_name="silver_employees", column_name="name", tag_key="pii")
        client.post("/api/dbt/run", json={"action": "run"}, cookies=admin)
        check("removing the source tag removes the derived one on the next run", "pii" not in tag_of("dbt", "fct_names", "top_person"), tag_of("dbt", "fct_names", "top_person"))

        print("\n7. A new schema is closed automatically; an explicit open is respected")
        open(os.path.join(PROJ, "models", "marts", "fct_names.sql"), "w").write(
            "{{ config(materialized='table', schema='marts') }}\nselect department, count(*) as n from {{ ref('stg_employees') }} group by department\n")
        r = client.post("/api/dbt/run", json={"action": "run"}, cookies=admin).json()
        check("a model configured with +schema lands in dbt_marts and that schema was closed first", os.path.isdir(os.path.join(WH, "dbt_marts", "fct_names")) and "dbt_marts" in r["governance"]["before"]["schemas_closed"], r["governance"]["before"])
        check("...so users see no rows there", rows(uma, "fct_names") == 0 or len(sql("SELECT * FROM warehouse.dbt_marts.fct_names", uma).get("rows", [])) == 0)
        tags.set_tag(catalog="warehouse", schema_name="dbt", tag_key="access", tag_value="open")
        r = client.post("/api/dbt/run", json={"action": "run"}, cookies=admin).json()
        check("a schema an admin explicitly opened is not closed again by the next run", "dbt" not in r["governance"]["before"]["schemas_closed"] and tags.effective_table_tags("warehouse", "dbt", "x")["access"]["value"] == "open", r["governance"]["before"])

        print("\n8. The switch")
        os.environ["DBT_CLOSED_BY_DEFAULT"] = "false"
        check("DBT_CLOSED_BY_DEFAULT=false disables closing and propagation", dbt_governance.before_run() == {"enabled": False} and dbt_governance.after_run() == {"enabled": False})
        os.environ.pop("DBT_CLOSED_BY_DEFAULT")
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All dbt governance checks passed.")


if __name__ == "__main__":
    main()
