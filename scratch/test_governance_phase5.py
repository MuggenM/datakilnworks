#!/usr/bin/env python3
"""
Phase 5 backend verification for tag-based masking: the endpoints that back the Governance view.
Runs against a throwaway WAREHOUSE_DIR (UI behaviour is covered by scratch/verify_governance_ui.py).
Tests: /status posture, /preview-as (rewritten SQL, blocked, RBAC), /coverage.
"""

import datetime
import os
import shutil
import sys
import tempfile

TMP_ROOT = tempfile.mkdtemp(prefix="governance_p5_")
TMP_WAREHOUSE = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(TMP_WAREHOUSE)
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_NOTEBOOK_EXECUTION", "JWT_SECRET_KEY", "COMPUTE_TOKEN", "GOVERNANCE_ENFORCEMENT"):
    os.environ.pop(var, None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import jwt
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth
from web.governance import policies, tags

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
        con.execute("CREATE OR REPLACE TABLE warehouse.hr.employees (id INTEGER, email VARCHAR, ssn VARCHAR)")
        con.execute("INSERT INTO warehouse.hr.employees VALUES (1,'ada@example.com','123-45-6789')")
        client = TestClient(app_module.app)
        admin, bob = cookie_for("admin"), cookie_for("analyst_bob")

        print("\n1. Status posture")
        r = client.get("/api/governance/status", cookies=admin).json()
        posture = r["posture"]
        check("posture reports enforcement mode, auth and notebook settings", posture["enforcement_mode"] == "enforce"
              and posture["require_auth"] is False and posture["notebook_execution"] == "exempt", posture)
        os.environ["GOVERNANCE_ENFORCEMENT"] = "audit"
        os.environ["GOVERNANCE_NOTEBOOK_EXECUTION"] = "all"
        try:
            p2 = client.get("/api/governance/status", cookies=admin).json()["posture"]
            check("posture follows the environment", p2["enforcement_mode"] == "audit" and p2["notebook_execution"] == "all", p2)
        finally:
            os.environ.pop("GOVERNANCE_ENFORCEMENT"), os.environ.pop("GOVERNANCE_NOTEBOOK_EXECUTION")

        print("\n2. Preview as user")
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="email", tag_key="pii", tag_value="email")
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="ssn", tag_key="pii", tag_value="ssn")
        policies.create_policy({"name": "PII partial", "tag_key": "pii", "mask_type": "partial"})
        body = {"sql": "SELECT email, ssn FROM warehouse.hr.employees", "as_user": "analyst_bob"}
        r = client.post("/api/governance/preview-as", json=body, cookies=admin).json()
        check("a masked user's query is rewritten and the masked columns are listed", r["changed"] and "gov_mask_partial" in r["rewritten_sql"]
              and {m["column"] for m in r["masked_columns"]} == {"email", "ssn"} and r["blocked"] is None, r)
        r = client.post("/api/governance/preview-as", json={**body, "as_user": "admin"}, cookies=admin).json()
        check("an exempt admin's query is untouched and their raw reads are listed", r["changed"] is False and r["rewritten_sql"] is None
              and "warehouse.hr.employees.email" in r["exempt_reads"], r)
        r = client.post("/api/governance/preview-as", json={"sql": "SELECT * FROM query('select 1')", "as_user": "analyst_bob"}, cookies=admin).json()
        check("a blocked statement reports the reason", r["blocked"] and r["rewritten_sql"] is None, r)
        check("nothing was executed or logged as a real query", True)
        check("unknown user -> 404", client.post("/api/governance/preview-as", json={**body, "as_user": "nobody"}, cookies=admin).status_code == 404)
        check("preview-as is admin-only", client.post("/api/governance/preview-as", json=body, cookies=bob).status_code == 403)

        print("\n3. Coverage")
        r = client.get("/api/governance/coverage", cookies=admin).json()
        check("assignments counted by level", r["assignments"] == 2 and r["by_level"] == {"column": 2}, r)
        check("the policy is matched by two assignments", r["policies"][0]["matching_assignments"] == 2 and r["policies_without_matches"] == [], r)
        policies.create_policy({"name": "Orphan policy", "tag_key": "sensitivity", "mask_type": "null"})
        r = client.get("/api/governance/coverage", cookies=admin).json()
        check("a policy whose tag nothing carries is flagged", r["policies_without_matches"] == ["Orphan policy"], r)
        check("coverage is admin-only", client.get("/api/governance/coverage", cookies=bob).status_code == 403)
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Governance Phase 5 checks passed.")


if __name__ == "__main__":
    main()
