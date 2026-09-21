#!/usr/bin/env python3
"""
Phase 2 verification for tag-based masking: mask library, custom expressions, policies and resolution.
Runs against a throwaway WAREHOUSE_DIR, so it never touches real data.
Tests:
1. Mask primitives: values, NULL handling, type preservation for every (mask x type), keyed hash.
2. Secrets: the hash key is not visible through EXPLAIN or duckdb_functions(); a missing mask fails closed.
3. Custom expressions: static AST validation and dry runs per type family.
4. Policy CRUD and validation, audit trail.
5. Resolution: tag/value match, type filter, exemptions, priority, restrictiveness ties, cache invalidation.
6. REST API: RBAC, validate endpoint, effective view (as_user), status.
7. Integration: masks work from cursors in other catalogs on the real studio and worker connections.
"""

import datetime
import hashlib
import os
import shutil
import sys
import tempfile

TMP_ROOT = tempfile.mkdtemp(prefix="governance_p2_")
TMP_WAREHOUSE = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(TMP_WAREHOUSE)
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_NOTEBOOK_EXECUTION", "JWT_SECRET_KEY", "COMPUTE_TOKEN"):
    os.environ.pop(var, None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import duckdb
import jwt
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth
from web.governance import macros, masks, policies, store, tags
from web.governance.policies import Principal

FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def raises(exc_type, fn, *a, **kw):
    try:
        fn(*a, **kw)
    except exc_type as e:
        return str(e) or True
    except Exception:
        return False
    return False


def cookie_for(username):
    user = auth.get_user_by_username(username)
    token = jwt.encode({"sub": user["id"], "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)},
                       auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)
    return {auth.COOKIE_NAME: token}


def mask(mask_type, data_type, value, expr=None):
    res = masks.run_mask(mask_type, data_type, value, expr)
    return res["masked"] if res["ok"] else f"ERROR: {res['error']}"


def test_primitives():
    print("\n1. Mask primitives")
    check("email keeps first char and domain", mask("email", "VARCHAR", "ada@example.com") == "a***@example.com")
    check("email without a usable '@' is fully redacted", mask("email", "VARCHAR", "not-an-email") == "****" and mask("email", "VARCHAR", "@x.com") == "****")
    check("email NULL stays NULL", mask("email", "VARCHAR", None) is None)
    check("partial keeps the last 4 of an SSN", mask("partial", "VARCHAR", "123-45-6789") == "*******6789")
    check("partial never reveals short values", mask("partial", "VARCHAR", "ab") == "**" and mask("partial", "VARCHAR", "abcde") == "***de")
    check("partial 3 chars keeps only one", mask("partial", "VARCHAR", "abc") == "**c")
    check("generalize string keeps 3 chars", mask("generalize", "VARCHAR", "Alexander") == "Ale***" and mask("generalize", "VARCHAR", "ab") == "***")
    check("generalize numeric -> one significant figure (rounded down)",
          mask("generalize", "DECIMAL(12,2)", "12345.67") == "10000.00" and mask("generalize", "INTEGER", "-987") == "-900"
          and mask("generalize", "INTEGER", "0") == "0" and mask("generalize", "INTEGER", "5") == "5")
    check("generalize DATE/TIMESTAMP -> year", mask("generalize", "DATE", "1990-05-17") == "1990-01-01"
          and mask("generalize", "TIMESTAMP", "1990-05-17 08:30:00") == "1990-01-01 00:00:00")
    check("generalize TIME has no meaningful bucket -> NULL", mask("generalize", "TIME", "08:30:00") is None)
    check("redact: '****' for text, NULL otherwise (hides NULL-ness of text)",
          mask("redact", "VARCHAR", "x") == "****" and mask("redact", "VARCHAR", None) == "****" and mask("redact", "INTEGER", "7") is None)
    check("null mask", mask("null", "VARCHAR", "x") is None and mask("null", "DATE", "2020-01-01") is None)
    check("hash on a non-text column falls back to NULL", mask("hash", "INTEGER", "42") is None)
    check("partial/email on non-text fall back to NULL", mask("partial", "INTEGER", "42") is None and mask("email", "DATE", "2020-01-01") is None)

    h1, h2 = mask("hash", "VARCHAR", "123-45-6789"), mask("hash", "VARCHAR", "123-45-6788")
    check("hash is deterministic (joins keep working) and distinguishes values", h1 == mask("hash", "VARCHAR", "123-45-6789") and h1 != h2 and len(h1) == 32)
    check("hash of NULL is NULL", mask("hash", "VARCHAR", None) is None)
    key = bytes.fromhex(open(os.path.join(TMP_WAREHOUSE, ".metadata", "governance_salt")).read().strip())[:32]
    check("hash is a BLAKE2b MAC under the per-install key",
          h1 == hashlib.blake2b(b"123-45-6789", key=key, digest_size=16).hexdigest())
    check("hash is not a plain digest (no offline dictionary attack)",
          h1 not in (hashlib.md5(b"123-45-6789").hexdigest(), hashlib.sha256(b"123-45-6789").hexdigest()[:32]))

    con = masks._tester()
    bad = []
    types = ["VARCHAR", "INTEGER", "BIGINT", "DECIMAL(10,2)", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN", "TIME"]
    for mt in ("redact", "hash", "partial", "email", "null", "generalize"):
        for t in types:
            expr = masks.mask_expression(mt, t, "v")
            got = con.execute(f'SELECT typeof({expr}) FROM (SELECT CAST(NULL AS {t}) AS "v")').fetchone()[0]
            if got.upper() != t.upper():
                bad.append((mt, t, got))
    check("every mask x type keeps the column's type", not bad, bad[:5])
    check("type families", [masks.type_family(t) for t in ("VARCHAR", "DECIMAL(10,2)", "TIMESTAMP WITH TIME ZONE", "BOOLEAN", "INTEGER[]", "UUID", "STRUCT(a INTEGER)")]
          == ["string", "numeric", "temporal", "boolean", "other", "other", "other"])


def test_secrets_and_fail_closed():
    print("\n2. Secrets and fail-closed behaviour")
    salt = open(os.path.join(TMP_WAREHOUSE, ".metadata", "governance_salt")).read().strip()
    key_hex = bytes.fromhex(salt)[:32].hex()
    con = masks._tester()
    plan = str(con.execute("EXPLAIN SELECT gov_mask_hash(CAST('x' AS VARCHAR))").fetchall())
    check("EXPLAIN does not reveal the hash key", salt[:16] not in plan and key_hex[:16] not in plan)
    defs = " ".join(str(r[0]) for r in con.execute("SELECT macro_definition FROM duckdb_functions() WHERE function_name LIKE 'gov_mask_%'").fetchall())
    check("no mask definition contains the key", salt[:16] not in defs and key_hex[:16] not in defs)
    from stat import S_IMODE
    check("salt file is private (0600)", S_IMODE(os.stat(os.path.join(TMP_WAREHOUSE, ".metadata", "governance_salt")).st_mode) == 0o600)

    fresh = duckdb.connect(":memory:")
    check("a connection without masks reports macros_installed=False", macros.macros_installed(fresh) is False)
    failed = False
    try:
        fresh.execute("SELECT memory.main.gov_mask_email('a@b.com')")
    except Exception:
        failed = True
    check("masked SQL on an un-provisioned connection fails instead of returning raw data", failed)
    macros.install_governance_macros(fresh)
    check("install is idempotent", macros.macros_installed(fresh) and (macros.install_governance_macros(fresh) or macros.macros_installed(fresh)))


def test_custom_expressions():
    print("\n3. Custom expressions")
    good = ["left({col}, 2) || '***'", "regexp_replace({col}, '[0-9]', 'x')",
            "CASE WHEN length({col}) > 4 THEN substr({col}, 1, 2) ELSE '*' END", "coalesce(nullif({col}, ''), 'n/a')",
            "upper(left({col}, 1)) || repeat('*', 4)"]
    for e in good:
        v = masks.validate_custom_expression(e, ["string"])
        check(f"accepts {e[:40]}...", v["ok"], v["errors"])
    bad = {"no placeholder": "'constant'", "subquery": "(SELECT 1) || {col}", "other column": "other_col || {col}",
           "aggregate": "count({col})", "window": "sum(length({col})) OVER ()", "table function": "query('select 1') || {col}",
           "file function": "read_csv('x') || {col}", "oracle": "gov_mask_hash({col})", "multi statement": "{col}; DROP TABLE x",
           "syntax error": "md5({col}", "star": "{col} || *", "extra braces": "{col} || '{x}'"}
    for label, e in bad.items():
        v = masks.validate_custom_expression(e, ["string"])
        check(f"rejects {label}", not v["ok"] and v["errors"], v)
    v = masks.validate_custom_expression("{col} + 1", None)
    check("type-unsafe expression is rejected without a type filter", not v["ok"] and "string" in " ".join(v["errors"]), v)
    v = masks.validate_custom_expression("{col} + 1", ["numeric"])
    check("...but accepted when restricted to numeric columns", v["ok"], v)
    check("custom mask runs and preserves type", mask("custom", "VARCHAR", "abcdef", "left({col}, 2) || '***'") == "ab***")


def test_policy_crud():
    print("\n4. Policy CRUD & validation")
    store.init_governance_db()
    tags.create_definition("pii", "personal", ["email", "phone", "ssn", "name", "dob"], actor="admin") if not [d for d in tags.list_definitions() if d["tag_key"] == "pii"] else None
    base = {"name": "PII partial", "tag_key": "pii", "mask_type": "partial", "priority": 100}
    p = policies.create_policy(base, actor="admin")
    check("create returns a parsed record", p["id"].startswith("pol_") and p["enabled"] is True and p["except_roles"] == ["admin"]
          and p["applies_to_types"] is None and p["tag_value"] is None, p)
    check("duplicate name rejected", raises(ValueError, policies.create_policy, base))
    for label, patch in {"unknown tag": {"tag_key": "ghost"}, "bad mask type": {"mask_type": "shred"}, "bad tag value": {"tag_value": "banana"},
                         "bad role": {"except_roles": ["root"]}, "bad priority": {"priority": 99999}, "empty name": {"name": ""},
                         "bad family": {"applies_to_types": ["blob"]}, "custom without expr": {"mask_type": "custom"},
                         "custom with bad expr": {"mask_type": "custom", "mask_expr": "count({col})"}}.items():
        check(f"rejects {label}", raises(ValueError, policies.create_policy, {**base, "name": f"x {label}", **patch}) is not False)
    p2 = policies.create_policy({**base, "name": "Email masking", "tag_value": "email", "mask_type": "email", "priority": 50,
                                 "except_users": ["lead_engineer"], "applies_to_types": ["string"]}, actor="admin")
    check("optional fields round-trip", p2["tag_value"] == "email" and p2["except_users"] == ["lead_engineer"] and p2["applies_to_types"] == ["string"], p2)

    up = policies.update_policy(p2["id"], {"enabled": False, "priority": 10}, actor="admin")
    check("partial update keeps the rest", up["enabled"] is False and up["priority"] == 10 and up["mask_type"] == "email")
    check("update validates the merged record", raises(ValueError, policies.update_policy, p2["id"], {"mask_type": "custom"}) is not False)
    check("rename to an existing name rejected", raises(ValueError, policies.update_policy, p2["id"], {"name": "PII partial"}) is not False)
    check("unknown policy -> NotFound", raises(policies.NotFound, policies.get_policy, "pol_nope"))
    policies.update_policy(p2["id"], {"enabled": True, "priority": 50})
    actions = [e["action"] for e in store.list_audit(limit=200)]
    check("audit has POLICY_CREATE / POLICY_UPDATE", {"POLICY_CREATE", "POLICY_UPDATE"} <= set(actions))
    tmp = policies.create_policy({**base, "name": "temp policy"})
    policies.delete_policy(tmp["id"], actor="admin")
    check("delete works and is audited", raises(policies.NotFound, policies.get_policy, tmp["id"]) and "POLICY_DELETE" in [e["action"] for e in store.list_audit(limit=50)])


def test_resolution():
    print("\n5. Resolution")
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="email", tag_key="pii", tag_value="email", actor="admin")
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="ssn", tag_key="pii", tag_value="ssn", actor="admin")
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="salary", tag_key="sensitivity", tag_value="confidential", actor="admin")
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="dob", tag_key="sensitivity", tag_value="confidential", actor="admin")
    policies.create_policy({"name": "Salary generalize", "tag_key": "sensitivity", "tag_value": "confidential", "mask_type": "generalize",
                            "applies_to_types": ["numeric"], "priority": 100}, actor="admin")
    disabled = policies.create_policy({"name": "Disabled null", "tag_key": "pii", "mask_type": "null", "priority": 1, "enabled": False})

    cols = [{"column": "id", "type": "INTEGER"}, {"column": "email", "type": "VARCHAR"}, {"column": "ssn", "type": "VARCHAR"},
            {"column": "salary", "type": "DECIMAL(10,2)"}, {"column": "dob", "type": "DATE"}]
    bob = Principal(username="analyst_bob", role="user")
    admin = Principal(username="admin", role="admin")
    lead = Principal(username="lead_engineer", role="power_user")

    def resolve(principal):
        return {m.column: m for m in policies.masks_for_table("warehouse", "hr", "employees", cols, principal)}

    r = resolve(bob)
    check("user: email gets the more specific, lower-priority-number policy", r["email"].policy_name == "Email masking" and r["email"].mask_type == "email", r.get("email"))
    check("user: ssn falls to the general pii policy", r["ssn"].policy_name == "PII partial", r.get("ssn"))
    check("user: numeric salary generalised by the sensitivity policy", r["salary"].mask_type == "generalize", r.get("salary"))
    check("type filter: temporal 'confidential' column is not masked by a numeric-only policy", "dob" not in r)
    check("untagged columns are visible", "id" not in r)
    check("disabled policies are ignored", all(m.policy_id != disabled["id"] for m in r.values()))
    check("mask spec carries a ready SQL expression", 'memory.main.gov_mask_email("email")' in r["email"].expression, r["email"].expression)

    check("admin is exempt (default except_roles)", resolve(admin) == {})
    check("system principal is exempt", resolve(Principal.system()) == {})
    rl = resolve(lead)
    check("per-user exemption drops that policy and the next one applies", rl["email"].policy_name == "PII partial", rl.get("email"))

    tie = policies.create_policy({"name": "PII redact", "tag_key": "pii", "mask_type": "redact", "priority": 100}, actor="admin")
    r = resolve(bob)
    check("equal priority -> more restrictive mask wins and the tie is reported",
          r["ssn"].mask_type == "redact" and r["ssn"].conflicts == ["PII partial"], r.get("ssn"))
    check("a lower priority number beats restrictiveness", r["email"].mask_type == "email")
    policies.delete_policy(tie["id"])
    check("cache follows policy changes immediately", resolve(bob)["ssn"].mask_type == "partial")

    tags.unset_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="ssn", tag_key="pii")
    check("cache follows tag changes immediately", "ssn" not in resolve(bob))
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="ssn", tag_key="pii", tag_value="ssn")

    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", tag_key="pii", tag_value="name", actor="admin")
    r = resolve(bob)
    check("a table-level tag masks every column it covers (inheritance) unless a column overrides it",
          r["id"].policy_name == "PII partial" and r["id"].mask_type == "partial" and r["email"].mask_type == "email"
          and r["ssn"].policy_name == "PII partial", {k: v.policy_name for k, v in r.items()})
    tags.unset_tag(catalog="warehouse", schema_name="hr", table_name="employees", tag_key="pii")
    check("with no enabled policies nothing is masked", [policies.delete_policy(p["id"]) for p in policies.list_policies()] is not None
          and policies.masks_for_table("warehouse", "hr", "employees", cols, bob) == [])


def test_api():
    print("\n6. REST API")
    client = TestClient(app_module.app)
    admin, bob, lead = cookie_for("admin"), cookie_for("analyst_bob"), cookie_for("lead_engineer")

    body = {"name": "API pii", "tag_key": "pii", "mask_type": "partial", "priority": 90}
    check("plain user cannot create a policy", client.post("/api/governance/masking-policies", json=body, cookies=bob).status_code == 403)
    check("power user cannot create a policy", client.post("/api/governance/masking-policies", json=body, cookies=lead).status_code == 403)
    r = client.post("/api/governance/masking-policies", json=body, cookies=admin)
    pid = r.json().get("id")
    check("admin creates a policy", r.status_code == 200 and pid, r.text[:200])
    check("duplicate -> 400", client.post("/api/governance/masking-policies", json=body, cookies=admin).status_code == 400)
    check("unknown tag -> 404", client.post("/api/governance/masking-policies", json={**body, "name": "n2", "tag_key": "ghost"}, cookies=admin).status_code == 404)
    check("any user can list policies", client.get("/api/governance/masking-policies", cookies=bob).json()["policies"][0]["name"] == "API pii")
    r = client.put(f"/api/governance/masking-policies/{pid}", json={"enabled": False, "priority": 5}, cookies=admin)
    check("PUT patches only the given fields", r.status_code == 200 and r.json()["enabled"] is False and r.json()["priority"] == 5 and r.json()["mask_type"] == "partial", r.text[:200])
    check("PUT is admin-only; unknown id -> 404", client.put(f"/api/governance/masking-policies/{pid}", json={"enabled": True}, cookies=bob).status_code == 403
          and client.put("/api/governance/masking-policies/pol_none", json={"enabled": True}, cookies=admin).status_code == 404)
    client.put(f"/api/governance/masking-policies/{pid}", json={"enabled": True}, cookies=admin)

    r = client.post("/api/governance/masking-policies/validate", json={"mask_type": "email", "data_type": "VARCHAR", "value": "ada@example.com"}, cookies=admin)
    check("validate returns per-type behaviour and a sample", r.status_code == 200 and r.json()["sample"]["masked"] == "a***@example.com"
          and r.json()["behaviour"]["numeric"] == "NULL", r.text[:250])
    r = client.post("/api/governance/masking-policies/validate", json={"mask_type": "custom", "mask_expr": "count({col})"}, cookies=admin)
    check("validate reports custom-expression errors", r.status_code == 200 and r.json()["custom"]["ok"] is False, r.text[:250])
    r = client.post("/api/governance/masking-policies/validate", json={"mask_type": "custom", "mask_expr": "left({col}, 1) || '***'", "applies_to_types": ["string"], "value": "secret"}, cookies=admin)
    check("validate runs a valid custom expression", r.json()["custom"]["ok"] and r.json()["sample"]["masked"] == "s***", r.text[:250])
    check("validate is admin-only", client.post("/api/governance/masking-policies/validate", json={"mask_type": "null"}, cookies=bob).status_code == 403)

    r = client.get("/api/governance/effective/warehouse/hr/employees", cookies=bob)
    data = r.json()
    check("effective view: plain user sees masked columns flagged", r.status_code == 200 and "ssn" in data["masked_columns"] and "id" not in data["masked_columns"], data.get("masked_columns"))
    ssn = next(c for c in data["columns"] if c["column"] == "ssn")
    check("effective view names the winning policy", ssn["policy"]["name"] == "API pii" and ssn["family"] == "string", ssn)
    check("admin sees nothing masked", client.get("/api/governance/effective/warehouse/hr/employees", cookies=admin).json()["masked_columns"] == [])
    r = client.get("/api/governance/effective/warehouse/hr/employees?as_user=analyst_bob", cookies=admin)
    check("admin can preview as another user", r.status_code == 200 and r.json()["as_user"] == "analyst_bob" and "ssn" in r.json()["masked_columns"], r.text[:200])
    check("as_user is admin-only; unknown user -> 404", client.get("/api/governance/effective/warehouse/hr/employees?as_user=admin", cookies=bob).status_code == 403
          and client.get("/api/governance/effective/warehouse/hr/employees?as_user=nobody", cookies=admin).status_code == 404)
    check("unknown table -> 404", client.get("/api/governance/effective/warehouse/hr/ghost", cookies=bob).status_code == 404)

    r = client.get("/api/governance/status", cookies=admin)
    st = r.json()
    check("status reports masks installed on the studio connection", r.status_code == 200 and st["masks_installed"] is True and st["policies_enabled"] >= 1, st)
    check("status is admin-only", client.get("/api/governance/status", cookies=bob).status_code == 403)
    check("delete works", client.delete(f"/api/governance/masking-policies/{pid}", cookies=admin).status_code == 200
          and client.delete(f"/api/governance/masking-policies/{pid}", cookies=admin).status_code == 404)


def test_integration():
    print("\n7. Integration on real connections")
    con = app_module.get_duckrun_conn().con
    check("studio connection has every mask primitive", macros.macros_installed(con))
    cur = con.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse.other")
    cur.execute("USE warehouse.other")
    expr_email = masks.mask_expression("email", "VARCHAR", "email")
    expr_hash = masks.mask_expression("hash", "VARCHAR", "ssn")
    expr_sal = masks.mask_expression("generalize", "DECIMAL(10,2)", "salary")
    rows = cur.execute(f'SELECT id, email, ssn, salary FROM (SELECT * REPLACE ({expr_email} AS "email", {expr_hash} AS "ssn", {expr_sal} AS "salary") '
                       f"FROM warehouse.hr.employees) AS employees").fetchall()
    check("REPLACE-style masking works from a cursor whose current schema differs",
          rows[0][1] == "a***@example.com" and rows[0][2] not in ("123-45-6789", None) and len(rows[0][2]) == 32 and str(rows[0][3]) == "100000.00", rows)
    plan = str(cur.execute(f'EXPLAIN SELECT * REPLACE ({expr_hash} AS "ssn") FROM warehouse.hr.employees').fetchall())
    salt = open(os.path.join(TMP_WAREHOUSE, ".metadata", "governance_salt")).read().strip()
    check("the plan of a masked query does not leak the key", salt[:16] not in plan)

    from web import compute_worker
    from web.compute_auth import compute_headers
    wclient = TestClient(compute_worker.app)
    r = wclient.get("/api/compute/status", headers=compute_headers())
    check("compute worker status reports masks installed", r.status_code == 200 and r.json().get("governance_masks_installed") is True, r.text[:200])


def main():
    try:
        con = app_module.get_duckrun_conn().con
        con.execute("CREATE SCHEMA IF NOT EXISTS warehouse.hr")
        con.execute("""CREATE OR REPLACE TABLE warehouse.hr.employees (
            id INTEGER, first_name VARCHAR, email VARCHAR, ssn VARCHAR, salary DECIMAL(10,2), dob DATE)""")
        con.execute("INSERT INTO warehouse.hr.employees VALUES (1,'Ada','ada@example.com','123-45-6789',100000,'1990-01-02')")
        test_primitives()
        test_secrets_and_fail_closed()
        test_custom_expressions()
        test_policy_crud()
        test_resolution()
        test_api()
        test_integration()
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Governance Phase 2 checks passed.")


if __name__ == "__main__":
    main()
