#!/usr/bin/env python3
"""
Row-level security phase 2: the /api/governance/row-policies and /api/governance/attributes REST routes.
Runs against a throwaway WAREHOUSE_DIR. Tests: admin-only CRUD, validation errors, RBAC on every route,
preview-as and /effective/... reporting row filter effects, and coverage reporting row policies.
"""

import datetime
import os
import shutil
import sys
import tempfile

TMP_ROOT = tempfile.mkdtemp(prefix="governance_rls_p2_")
TMP_WAREHOUSE = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(TMP_WAREHOUSE)
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
# Bootstrap-only admin account (see web/auth.py::_init_admin_from_env); precomputed hash for "adminpassword123"
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
os.environ["INIT_ADMIN_DISPLAY_NAME"] = "System Administrator"
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_NOTEBOOK_EXECUTION", "JWT_SECRET_KEY", "COMPUTE_TOKEN", "GOVERNANCE_ENFORCEMENT"):
    os.environ.pop(var, None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import jwt
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth

# lead_engineer / analyst_bob are no longer auto-seeded (bootstrap-only seeds just the admin account);
# create them explicitly so this test sees the same accounts the old hardcoded seed provided.
auth.create_user("lead_engineer", "powerpassword123", "Lead Data Engineer", role="power_user")
auth.create_user("analyst_bob", "userpassword123", "Bob the Analyst", role="user")

# Bootstrap sets must_change_password=1 for the admin account; clear it (as if admin already
# completed the forced first-login change) so the rest of this test behaves as before.
with auth.get_db_connection() as _c:
    _c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
from web.governance import row_filters, store, tags

FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:400]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def cookie_for(username):
    user = auth.get_user_by_username(username)
    token = jwt.encode({"sub": user["id"], "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)},
                       auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)
    return {auth.COOKIE_NAME: token}


def main():
    try:
        con = app_module.get_duckrun_conn().con
        con.execute("CREATE SCHEMA IF NOT EXISTS warehouse.hr")
        con.execute("CREATE OR REPLACE TABLE warehouse.hr.employees (id INTEGER, region VARCHAR, owner VARCHAR)")
        con.execute("INSERT INTO warehouse.hr.employees VALUES (1,'EMEA','analyst_bob'),(2,'US','lead_engineer')")
        store.init_governance_db()
        tags.create_definition("region_scoped2", "row filter scope")
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", tag_key="region_scoped2", tag_value="")

        client = TestClient(app_module.app)
        admin, bob = cookie_for("admin"), cookie_for("analyst_bob")

        print("\n1. Row policy CRUD is admin-only")
        body = {"name": "RLS route test", "tag_key": "region_scoped2", "filter_column": "region", "filter_mode": "owner"}
        r = client.post("/api/governance/row-policies", json=body, cookies=bob)
        check("a non-admin cannot create a row policy", r.status_code == 403, r.text)
        r = client.get("/api/governance/row-policies", cookies=bob)
        check("a non-admin can still list row policies (read)", r.status_code == 200, r.text)
        r = client.post("/api/governance/row-policies", json=body, cookies=admin)
        check("an admin can create one", r.status_code == 200, r.text)
        created = r.json()
        pid = created["id"]
        check("the response echoes what was stored", created["filter_mode"] == "owner" and created["filter_column"] == "region", created)

        print("\n2. Validation")
        bad = {"name": "bad", "tag_key": "region_scoped2", "filter_column": "region", "filter_mode": "attribute"}
        r = client.post("/api/governance/row-policies", json=bad, cookies=admin)
        check("attribute mode without attribute_key is rejected", r.status_code == 400, r.text)
        bad2 = {"name": "bad2", "tag_key": "region_scoped2", "filter_column": "region", "filter_mode": "custom",
                "filter_expr": "DROP TABLE employees"}
        r = client.post("/api/governance/row-policies", json=bad2, cookies=admin)
        check("an unsafe custom expression is rejected", r.status_code == 400, r.text)
        r = client.post("/api/governance/row-policies/validate", json={"filter_expr": "{col} = {user}"}, cookies=admin)
        check("the validate endpoint approves a safe expression", r.status_code == 200 and r.json()["ok"] is True, r.text)
        r = client.post("/api/governance/row-policies/validate", json={"filter_expr": "1=1; DROP TABLE x"}, cookies=admin)
        check("...and rejects an unsafe one, without creating anything", r.json()["ok"] is False, r.text)
        r = client.post("/api/governance/row-policies/validate", json={"filter_expr": "{col} = {user}"}, cookies=bob)
        check("validate is admin-only", r.status_code == 403)

        print("\n3. Update / delete")
        r = client.put(f"/api/governance/row-policies/{pid}", json={"enabled": False}, cookies=bob)
        check("update is admin-only", r.status_code == 403)
        r = client.put(f"/api/governance/row-policies/{pid}", json={"enabled": False}, cookies=admin)
        check("an admin can update", r.status_code == 200 and r.json()["enabled"] is False, r.text)
        r = client.delete(f"/api/governance/row-policies/{pid}", cookies=bob)
        check("delete is admin-only", r.status_code == 403)
        r = client.get(f"/api/governance/row-policies", cookies=admin)
        check("the disabled policy is still listed", any(p["id"] == pid for p in r.json()["policies"]))

        print("\n4. Principal attributes")
        r = client.post("/api/governance/attributes", json={"principal_type": "user", "principal_value": "analyst_bob",
                                                             "attribute_key": "region", "values": ["EMEA", "APAC"]}, cookies=bob)
        check("setting attributes is admin-only", r.status_code == 403)
        r = client.post("/api/governance/attributes", json={"principal_type": "user", "principal_value": "analyst_bob",
                                                             "attribute_key": "region", "values": ["EMEA", "APAC"]}, cookies=admin)
        check("an admin can assign attribute values", r.status_code == 200 and sorted(r.json()["values"]) == ["APAC", "EMEA"], r.text)
        r = client.post("/api/governance/attributes", json={"principal_type": "role", "principal_value": "power_user",
                                                             "attribute_key": "region", "values": ["US"]}, cookies=admin)
        check("...and to a role", r.status_code == 200, r.text)
        r = client.post("/api/governance/attributes", json={"principal_type": "role", "principal_value": "not-a-role",
                                                             "attribute_key": "region", "values": ["US"]}, cookies=admin)
        check("an unknown role is rejected", r.status_code == 400, r.text)
        r = client.get("/api/governance/attributes", cookies=bob)
        check("listing attributes is admin-only", r.status_code == 403)
        r = client.get("/api/governance/attributes", params={"principal_type": "user", "principal_value": "analyst_bob"}, cookies=admin)
        check("attributes list by filter", any(a["attribute_key"] == "region" for a in r.json()["attributes"]), r.text)
        r = client.delete("/api/governance/attributes", params={"principal_type": "user", "principal_value": "analyst_bob",
                                                                 "attribute_key": "region"}, cookies=admin)
        check("an admin can delete an assignment", r.status_code == 200, r.text)
        r = client.delete("/api/governance/attributes", params={"principal_type": "user", "principal_value": "analyst_bob",
                                                                 "attribute_key": "region"}, cookies=admin)
        check("deleting a nonexistent assignment is 404", r.status_code == 404, r.text)
        client.post("/api/governance/attributes", json={"principal_type": "user", "principal_value": "analyst_bob",
                                                         "attribute_key": "region", "values": ["EMEA"]}, cookies=admin)

        print("\n5. Live effect: preview-as, /effective/..., /coverage")
        client.put(f"/api/governance/row-policies/{pid}", json={"enabled": True}, cookies=admin)
        r = client.post("/api/governance/preview-as", json={"sql": "SELECT * FROM warehouse.hr.employees",
                                                             "as_user": "analyst_bob"}, cookies=admin)
        check("preview-as reports the row filter that would apply", r.status_code == 200 and r.json()["row_filters_applied"], r.text)
        r = client.get("/api/governance/effective/warehouse/hr/employees", params={"as_user": "analyst_bob"}, cookies=admin)
        check("/effective/... reports the table as row-filtered for that user", r.status_code == 200 and r.json()["row_filtered"] is True, r.text)
        r = client.get("/api/governance/effective/warehouse/hr/employees", params={"as_user": "admin"}, cookies=admin)
        check("...and not for the exempt admin", r.json()["row_filtered"] is False, r.text)
        r = client.get("/api/governance/coverage", cookies=admin)
        check("coverage lists the row policy and its matches", r.status_code == 200
              and any(p["name"] == "RLS route test" for p in r.json()["row_policies"]), r.text)
        r = client.get("/api/governance/coverage", cookies=bob)
        check("coverage is admin-only", r.status_code == 403)
        r = client.get("/api/governance/status", cookies=admin)
        check("status counts row policies", r.json()["row_policies_total"] >= 1 and r.json()["row_policies_enabled"] >= 1, r.text)

        row_filters.delete_row_policy(pid)
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All row-level security route checks passed.")


if __name__ == "__main__":
    main()
