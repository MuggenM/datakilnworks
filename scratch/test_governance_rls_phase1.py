#!/usr/bin/env python3
"""
Row-level security (RLS) verification: row filter policies through the governance gateway.
Runs against a throwaway WAREHOUSE_DIR, so it never touches real data. Mirrors scratch/test_governance_phase3.py's
approach (run SQL through enforce.govern(), execute the rewritten SQL, check what came back) but for row filtering.

Tests:
1. The three filter modes (owner, attribute, custom) across query shapes: star, alias, CTE, subquery, join, UNION, view.
2. Multiple applicable policies combine with AND; exemptions; a masking policy and a row policy on the same table together.
3. Fail-closed: a missing filter_column, an unresolvable table name, an unassigned attribute.
4. Statement gating for principals subject to a row policy (DML, COPY, CREATE VIEW, computed paths, table functions).
5. Audit trail (ROW_FILTER_APPLIED / ROW_FILTER_EXEMPT_READ) and the cache fingerprint distinguishing different predicates.
6. mask_arrow() row-filters an Arrow table already in Python.
7. is_subject()/deny_if_subject() cover row policies too (what notebook execution gating relies on).
8. Tag propagation: an exempt CTAS over a row-filtered table tags the destination the same way.
"""

import os
import shutil
import sys
import tempfile

TMP_ROOT = tempfile.mkdtemp(prefix="governance_rls_")
TMP_WAREHOUSE = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(TMP_WAREHOUSE)
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
# Bootstrap-only admin account (see web/auth.py::_init_admin_from_env); precomputed hash for "adminpassword123"
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
os.environ["INIT_ADMIN_DISPLAY_NAME"] = "System Administrator"
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_NOTEBOOK_EXECUTION", "JWT_SECRET_KEY", "COMPUTE_TOKEN",
            "GOVERNANCE_ENFORCEMENT", "GOVERNANCE_ALLOWED_PATHS"):
    os.environ.pop(var, None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import pyarrow as pa

from web import app as app_module
from web import auth

# Bootstrap sets must_change_password=1 for the admin account; clear it (as if admin already
# completed the forced first-login change) so the rest of this test behaves as before.
with auth.get_db_connection() as _c:
    _c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
from web.governance import enforce, gateway, policies, propagate, row_filters, store, tags
from web.governance.policies import Principal

FAILURES = []
ADMIN, BOB, LEAD, SYSTEM = (Principal("admin", "admin"), Principal("analyst_bob", "user"),
                            Principal("lead_engineer", "power_user"), Principal.system())


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:400]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


class Env:
    con = None


def run(sql, principal=BOB, home="warehouse.hr", **kw):
    cur = Env.con.cursor()
    try:
        if home:
            cur.execute(f"USE {home}")
        res = enforce.govern(sql, principal, cur, **kw)
        if res.blocked:
            return None, res
        try:
            rows = cur.execute(res.sql).fetchall()
        except Exception as exc:
            res.blocked = None
            return f"ENGINE ERROR: {exc}", res
        return rows, res
    finally:
        cur.close()


def setup():
    con = app_module.get_duckrun_conn().con
    Env.con = con
    con.execute("CREATE SCHEMA IF NOT EXISTS warehouse.hr")
    con.execute("""CREATE OR REPLACE TABLE warehouse.hr.employees (
        id INTEGER, name VARCHAR, email VARCHAR, region VARCHAR, owner VARCHAR, dept VARCHAR)""")
    con.execute("""INSERT INTO warehouse.hr.employees VALUES
        (1,'Ada','ada@example.com','EMEA','analyst_bob','eng'),
        (2,'Bob','bob@corp.io','US','lead_engineer','ops'),
        (3,'Cy','cy@example.com','APAC','analyst_bob','eng'),
        (4,'Dee','dee@example.com','US','someone_else','sales')""")
    con.execute("CREATE OR REPLACE TABLE warehouse.hr.badges (id INTEGER, region VARCHAR, badge VARCHAR)")
    con.execute("INSERT INTO warehouse.hr.badges VALUES (1,'EMEA','gold'),(2,'US','silver'),(3,'APAC','bronze'),(4,'US','silver')")
    con.execute("CREATE OR REPLACE VIEW warehouse.hr.v_employees AS SELECT * FROM warehouse.hr.employees")
    con.execute("CREATE OR REPLACE TABLE warehouse.hr.no_region_col (id INTEGER, note VARCHAR)")
    con.execute("INSERT INTO warehouse.hr.no_region_col VALUES (1,'x'),(2,'y')")
    store.init_governance_db()


def ids(rows, idx=0):
    return sorted(r[idx] for r in rows)


def main():
    try:
        setup()

        print("\n1. attribute mode across query shapes")
        if "region_scoped" not in [t["tag_key"] for t in tags.list_definitions()]:
            tags.create_definition("region_scoped", "Row filter scope by region")
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", tag_key="region_scoped", tag_value="")
        pol = row_filters.create_row_policy({"name": "Region filter", "tag_key": "region_scoped", "filter_column": "region",
                                             "filter_mode": "attribute", "attribute_key": "region", "except_roles": ["admin"]})
        row_filters.set_attribute_values("user", "analyst_bob", "region", ["EMEA", "APAC"])
        rows, res = run("SELECT * FROM employees ORDER BY id", BOB)
        check("attribute mode: star selects only assigned regions", ids(rows) == [1, 3], (rows, res.blocked))
        check("the result is marked row-filtered", res.row_filtered and res.row_filtered[0].table == "warehouse.hr.employees")
        rows, res = run("SELECT e.id FROM employees e ORDER BY e.id", BOB)
        check("aliased table is filtered too", ids(rows) == [1, 3], rows)
        rows, res = run("SELECT id FROM (SELECT * FROM employees) sub ORDER BY id", BOB)
        check("subquery wrapping a filtered table stays filtered", ids(rows) == [1, 3], rows)
        rows, res = run("WITH e AS (SELECT * FROM employees) SELECT id FROM e ORDER BY id", BOB)
        check("CTE over a filtered table stays filtered", ids(rows) == [1, 3], rows)
        rows, res = run("SELECT id FROM employees WHERE region = 'US' ORDER BY id", BOB)
        check("no oracle: filtering by an unassigned region finds nothing (not an error)", rows == [], rows)
        rows, res = run("SELECT id FROM employees UNION SELECT id FROM employees ORDER BY id", BOB)
        check("UNION over the same filtered table stays filtered on both sides", ids(rows) == [1, 3], rows)
        rows, res = run("SELECT id FROM v_employees ORDER BY id", BOB)
        check("a view over the filtered table is filtered through its definition", ids(rows) == [1, 3], rows)
        rows, res = run("SELECT e.id, b.badge FROM employees e JOIN badges b ON e.region = b.region ORDER BY e.id", BOB)
        check("joins only see the caller's rows on the filtered side", ids(rows) == [1, 3], rows)

        print("\n2. owner mode, custom mode, combining policies, exemptions")
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", tag_key="region_scoped", tag_value="owner-scope")
        tags.create_definition("owner_scoped", "Row filter: only rows you own") if "owner_scoped" not in [t["tag_key"] for t in tags.list_definitions()] else None
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", tag_key="owner_scoped", tag_value="")
        owner_pol = row_filters.create_row_policy({"name": "Owner filter", "tag_key": "owner_scoped", "filter_column": "owner",
                                                    "filter_mode": "owner", "except_roles": ["admin"]})
        rows, res = run("SELECT id FROM employees ORDER BY id", BOB)
        check("owner AND the earlier attribute filter combine (bob owns 1,3; both are in his assigned regions)",
              ids(rows) == [1, 3], rows)
        custom_pol = row_filters.create_row_policy({"name": "Region literal filter", "tag_key": "owner_scoped",
                                                     "filter_mode": "custom", "filter_column": "region",
                                                     "filter_expr": "{col} <> 'APAC' OR {user} = 'lead_engineer'",
                                                     "except_roles": ["admin"]})
        rows, res = run("SELECT id FROM employees ORDER BY id", BOB)
        check("adding a custom filter narrows further (excludes bob's APAC row)", ids(rows) == [1], rows)
        rows, res = run("SELECT id FROM employees ORDER BY id", LEAD)
        check("lead_engineer has no assigned region attribute yet, so the attribute policy denies everything",
              rows == [], rows)
        rows, res = run("SELECT id FROM employees ORDER BY id", ADMIN)
        check("an exempt admin (except_roles) is unrestricted", ids(rows) == [1, 2, 3, 4], rows)
        row_filters.update_row_policy(custom_pol["id"], {"enabled": False})

        print("\n3. Fail-closed defaults")
        rows, res = run("SELECT id FROM no_region_col ORDER BY id", BOB)
        check("an untagged table is unaffected by the region policy", ids(rows) == [1, 2], rows)
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name="no_region_col", tag_key="region_scoped", tag_value="")
        rows, res = run("SELECT id FROM no_region_col ORDER BY id", BOB)
        check("a table tagged region_scoped without the filter column denies all rows, not left unfiltered",
              rows == [], (rows, res.blocked))
        tags.unset_tag(catalog="warehouse", schema_name="hr", table_name="no_region_col", tag_key="region_scoped")
        row_filters.set_attribute_values("user", "analyst_bob", "region", [])
        rows, res = run("SELECT id FROM employees ORDER BY id", BOB)
        check("clearing bob's assigned attribute values leaves nothing visible, not everything", rows == [], rows)
        row_filters.set_attribute_values("user", "analyst_bob", "region", ["EMEA", "APAC"])
        rows, res = run("SELECT * FROM nope_does_not_exist", BOB)
        # An unresolvable name that isn't heuristically "sensitive" (no matching tagged name, no broad catalog/schema
        # tag) falls through to DuckDB's own catalog error rather than a governance block -- same as masking's existing
        # behavior. Either way is safe: what matters is that no rows come back.
        check("an unresolvable name never returns real rows", not isinstance(rows, list) or rows == [], (rows, res.blocked))

        print("\n4. Statement gating for row-subject principals")
        _, res = run("DELETE FROM employees WHERE id = 1", BOB)
        check("DELETE on a row-filtered table is refused", res.blocked is not None, res.blocked)
        _, res = run("UPDATE employees SET name = 'x' WHERE id = 1", BOB)
        check("UPDATE on a row-filtered table is refused", res.blocked is not None, res.blocked)
        _, res = run("CREATE VIEW v_leak AS SELECT * FROM employees", BOB)
        check("CREATE VIEW over a row-filtered table is refused", res.blocked is not None, res.blocked)
        _, res = run("COPY employees TO '/tmp/x.csv'", BOB)
        check("COPY ... TO is refused for a row-subject principal", res.blocked is not None, res.blocked)
        _, res = run("SELECT * FROM read_parquet(?)", BOB, home="warehouse.hr")
        check("a computed file path is refused for a row-subject principal", res.blocked is not None, res.blocked)

        print("\n5. Audit trail and cache fingerprint")
        rows, res = run("SELECT id FROM employees ORDER BY id", BOB)
        check("ROW_FILTER_APPLIED shows up in the audit log", any(a["action"] == "ROW_FILTER_APPLIED" for a in store.list_audit(limit=20)))
        rows, res = run("SELECT id FROM employees ORDER BY id", ADMIN)
        check("ROW_FILTER_EXEMPT_READ is recorded for the exempt admin's raw read",
              any(a["action"] == "ROW_FILTER_EXEMPT_READ" for a in store.list_audit(limit=20)))
        _, res_bob = run("SELECT id FROM employees ORDER BY id", BOB)
        row_filters.set_attribute_values("user", "lead_engineer", "region", ["US"])
        _, res_lead = run("SELECT id FROM employees ORDER BY id", LEAD)
        fp_bob, fp_lead = gateway.fingerprint(res_bob), gateway.fingerprint(res_lead)
        check("two users with different resolved predicates get different cache fingerprints",
              fp_bob and fp_lead and fp_bob != fp_lead, (fp_bob, fp_lead))
        _, res_bob2 = run("SELECT id FROM employees ORDER BY id", BOB)
        check("the same user's fingerprint is stable across calls", gateway.fingerprint(res_bob2) == fp_bob)

        print("\n6. mask_arrow() row-filters data already in Python")
        table = pa.table({"id": [1, 2, 3, 4], "region": ["EMEA", "US", "APAC", "US"], "owner": ["analyst_bob", "lead_engineer", "analyst_bob", "x"]})
        out = gateway.mask_arrow(table, "warehouse", "hr", "employees", {"username": "analyst_bob", "role": "user", "id": "u1"})
        check("mask_arrow applies the same row filters (owner mode: bob owns 1 and 3)", sorted(out.column("id").to_pylist()) == [1, 3], out.to_pydict())
        out_admin = gateway.mask_arrow(table, "warehouse", "hr", "employees", {"username": "admin", "role": "admin", "id": "a1"})
        check("mask_arrow leaves an exempt principal's data untouched", sorted(out_admin.column("id").to_pylist()) == [1, 2, 3, 4])

        print("\n7. is_subject()/deny_if_subject() cover row policies")
        check("gateway.is_subject is true for a row-filtered, non-exempt user", gateway.is_subject({"username": "analyst_bob", "role": "user", "id": "u1"}))
        check("...and false for the exempt admin", not gateway.is_subject({"username": "admin", "role": "admin", "id": "a1"}))
        try:
            gateway.deny_if_subject({"username": "analyst_bob", "role": "user", "id": "u1"}, "Notebook tasks")
            check("deny_if_subject raises for a row-filtered user", False)
        except Exception as exc:
            check("deny_if_subject raises for a row-filtered user", "GovernanceBlocked" in type(exc).__name__, type(exc).__name__)

        print("\n8. Tag propagation from a row-filtered source")
        Env.con.execute("DROP TABLE IF EXISTS warehouse.hr.employees_copy")
        # Fully qualified so propagate_after's own metadata lookup (a fresh cursor, no USE context) resolves the
        # destination the same way the original statement's cursor did.
        ctas_sql = "CREATE TABLE warehouse.hr.employees_copy AS SELECT * FROM warehouse.hr.employees"
        _, res = run(ctas_sql, ADMIN)
        check("the CTAS by an exempt admin succeeds", res.blocked is None, res.blocked)
        applied = propagate.propagate_after(ctas_sql, ADMIN, res, Env.con.cursor(), default_catalog="warehouse")
        check("propagation tags the destination table with the row-filter tags",
              any(a["tag"] in ("region_scoped", "owner_scoped") for a in applied), applied)
        rows, res2 = run("SELECT id FROM employees_copy ORDER BY id", BOB)
        # Both propagated tags apply (owner_scoped + region_scoped, combined with AND, matching the source table); the
        # disabled custom policy was never applicable so it does not narrow this further. What matters is it is NOT
        # all four rows -- an unfiltered copy of a row-filtered table.
        check("...so the copy is filtered for a masked user too, not a full leak of all rows", ids(rows) == [1, 3], (rows, applied))
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All row-level security checks passed.")


if __name__ == "__main__":
    main()
