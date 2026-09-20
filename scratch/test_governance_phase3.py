#!/usr/bin/env python3
"""
Phase 3 verification for tag-based masking: the enforcement gateway (web/governance/enforce.py).
Runs against a throwaway WAREHOUSE_DIR, so it never touches real data.
Tests:
1. Masking through every query shape: star, aliases, qualified names, USE, CTEs (incl. shadowing), subqueries, joins,
   set operations, views, multi-statement, EXPLAIN.
2. No oracles: filters, LIKE, GROUP BY, ORDER BY over masked columns see masked values.
3. Path-based scans map back to the table (delta_scan / read_parquet / string tables) and are sandboxed.
4. Statement gating: query(), gov_* calls, macro redefinition, COPY, SUMMARIZE, DML on masked tables, settings.
5. Non-admin guarantees without any policy (.metadata, outside paths, macro tampering).
6. Exemptions, audit trail, enforcement modes, masked_relation, fidelity, randomized leak check, performance.
"""

import os
import random
import shutil
import statistics
import sys
import tempfile
import time

TMP_ROOT = tempfile.mkdtemp(prefix="governance_p3_")
TMP_WAREHOUSE = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(TMP_WAREHOUSE)
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_RESTRICT_NOTEBOOKS", "JWT_SECRET_KEY", "COMPUTE_TOKEN",
            "GOVERNANCE_ENFORCEMENT", "GOVERNANCE_ALLOWED_PATHS"):
    os.environ.pop(var, None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import pyarrow as pa
from deltalake import write_deltalake

from web import app as app_module
from web.governance import enforce, policies, store, tags
from web.governance.policies import Principal

FAILURES = []
RAW = ["ada@example.com", "bob@corp.io", "cy@example.com", "123-45-6789", "987-65-4321", "555-12-3456"]
ADMIN, BOB, LEAD, SYSTEM = (Principal("admin", "admin"), Principal("analyst_bob", "user"),
                            Principal("lead_engineer", "power_user"), Principal.system())


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


class Env:
    con = None


def run(sql, principal=BOB, home="warehouse.hr", **kw):
    """Gateway + execution. Returns (rows | None, RewriteResult). Never executes a blocked statement.
    Cursors start in memory.main (as in the studio's local path), so each run starts in `home` like a session that ran USE."""
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


def leaks(rows):
    text = str(rows)
    return [v for v in RAW if v in text]


def rows_ok(rows):
    return isinstance(rows, list)


def setup():
    # a Delta table written before the studio connects, so it is registered the way the app registers Delta tables
    delta_dir = os.path.join(TMP_WAREHOUSE, "hr", "delta_emp")
    write_deltalake(delta_dir, pa.table({"id": [1, 2], "email": ["ada@example.com", "bob@corp.io"], "ssn": ["123-45-6789", "987-65-4321"],
                                         "dept": ["eng", "ops"]}))
    os.makedirs(os.path.join(TMP_WAREHOUSE, "volumes", "warehouse", "raw", "inbox"), exist_ok=True)
    with open(os.path.join(TMP_WAREHOUSE, "volumes", "warehouse", "raw", "inbox", "f.csv"), "w") as f:
        f.write("a,b\n1,2\n")

    con = app_module.get_duckrun_conn().con
    Env.con = con
    con.execute("CREATE SCHEMA IF NOT EXISTS warehouse.hr")
    con.execute("CREATE SCHEMA IF NOT EXISTS warehouse.dbo")
    con.execute("""CREATE OR REPLACE TABLE warehouse.hr.employees (
        id INTEGER, first_name VARCHAR, email VARCHAR, ssn VARCHAR, salary DECIMAL(10,2), dob DATE, dept VARCHAR)""")
    con.execute("""INSERT INTO warehouse.hr.employees VALUES
        (1,'Ada','ada@example.com','123-45-6789',100000.00,'1990-01-02','eng'),
        (2,'Bob','bob@corp.io','987-65-4321',85000.00,'1985-07-30','ops'),
        (3,'Cy','cy@example.com','555-12-3456',120000.00,'1979-11-11','eng')""")
    con.execute("CREATE OR REPLACE TABLE warehouse.hr.badges (id INTEGER, ssn VARCHAR, badge VARCHAR)")
    con.execute("INSERT INTO warehouse.hr.badges VALUES (1,'123-45-6789','gold'),(2,'987-65-4321','silver')")
    con.execute("CREATE OR REPLACE VIEW warehouse.hr.v_employees AS SELECT * FROM warehouse.hr.employees")
    con.execute("CREATE OR REPLACE VIEW warehouse.hr.v_deep AS SELECT id, ssn AS secret, dept FROM warehouse.hr.v_employees")
    con.execute("CREATE OR REPLACE TABLE warehouse.hr.plain (id INTEGER, note VARCHAR)")
    con.execute("INSERT INTO warehouse.hr.plain VALUES (1,'hello'),(2,'world')")
    if not con.execute("SELECT 1 FROM duckdb_views() WHERE view_name = 'delta_emp' AND database_name = 'warehouse'").fetchone():
        con.execute(f"CREATE VIEW warehouse.hr.delta_emp AS SELECT * FROM delta_scan('{delta_dir}')")

    for key, desc, vals in (("pii", "personal", ["email", "ssn"]), ("sensitivity", "sens", None)):
        if key not in [d["tag_key"] for d in tags.list_definitions()]:
            tags.create_definition(key, desc, vals)
    for tbl in ("employees", "delta_emp"):
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name=tbl, column_name="email", tag_key="pii", tag_value="email")
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name=tbl, column_name="ssn", tag_key="pii", tag_value="ssn")
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="badges", column_name="ssn", tag_key="pii", tag_value="ssn")
    for col in ("salary", "dob"):
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name=col, tag_key="sensitivity", tag_value="confidential")
    policies.create_policy({"name": "Email masking", "tag_key": "pii", "tag_value": "email", "mask_type": "email", "priority": 50})
    policies.create_policy({"name": "PII partial", "tag_key": "pii", "mask_type": "partial", "priority": 100})
    policies.create_policy({"name": "Salary generalize", "tag_key": "sensitivity", "mask_type": "generalize", "applies_to_types": ["numeric"]})
    return delta_dir


def test_basic_shapes():
    print("\n1. Masking through every query shape")
    rows, res = run("SELECT * FROM warehouse.hr.employees ORDER BY id")
    check("SELECT * masks email, ssn, salary for a plain user", rows_ok(rows) and rows[0][2] == "a***@example.com" and rows[0][3] == "*******6789"
          and str(rows[0][4]) == "100000.00" and not leaks(rows), (rows, res.blocked))
    check("...and reports the masked columns", {m.column for m in res.masked} == {"email", "ssn", "salary"}, res.masked)
    check("salary generalised to one significant figure, dob untouched (numeric-only policy)",
          str(rows[1][4]) == "80000.00" and str(rows[1][5]) == "1985-07-30", rows[1])
    rows, res = run("SELECT * FROM warehouse.hr.employees ORDER BY id", ADMIN)
    check("admin (exempt) sees raw data and the SQL is untouched", rows[0][2] == "ada@example.com" and res.changed is False
          and res.sql == "SELECT * FROM warehouse.hr.employees ORDER BY id", (rows[:1], res.changed))
    rows, _ = run("SELECT * FROM warehouse.hr.employees ORDER BY id", SYSTEM)
    check("system principal sees raw data", rows[0][2] == "ada@example.com")

    for label, sql in {
        "alias": "SELECT e.email, e.ssn FROM warehouse.hr.employees e",
        "3-part qualified columns": "SELECT warehouse.hr.employees.email, warehouse.hr.employees.ssn FROM warehouse.hr.employees",
        "2-part qualified columns": "SELECT hr.employees.email, hr.employees.ssn FROM hr.employees",
        "bare table name after USE": "USE warehouse.hr; SELECT email, ssn FROM employees",
        "quoted/mixed-case identifiers": 'SELECT "EMAIL", "SSN" FROM "Warehouse"."HR"."Employees"',
        "FROM-first syntax": "FROM warehouse.hr.employees SELECT email, ssn",
        "star + exclude": "SELECT * EXCLUDE (id) FROM warehouse.hr.employees",
        "table star": "SELECT e.* FROM warehouse.hr.employees e",
        "columns() expression": "SELECT COLUMNS('email|ssn') FROM warehouse.hr.employees",
        "sample clause": "SELECT email, ssn FROM warehouse.hr.employees USING SAMPLE 100%",
        "column alias": "SELECT email AS contact, ssn AS gov_id FROM warehouse.hr.employees",
        "cast to text": "SELECT CAST(ssn AS VARCHAR) || '' AS x FROM warehouse.hr.employees",
    }.items():
        rows, res = run(sql)
        check(f"masks through {label}", rows_ok(rows) and rows and not leaks(rows), (rows if not rows_ok(rows) else rows[:1], res.blocked, res.sql[:200]))

    rows, res = run("SELECT count(*) FROM warehouse.hr.employees")
    check("aggregates over untouched columns still work", rows == [(3,)], rows)
    rows, res = run("SELECT id, first_name, dept FROM warehouse.hr.employees ORDER BY id")
    check("a query that ignores the masked columns still returns the real values of the others", [r[1] for r in rows] == ["Ada", "Bob", "Cy"], rows)
    admin_rows, _ = run("SELECT id, first_name, dept FROM warehouse.hr.employees ORDER BY id", ADMIN)
    check("non-sensitive columns are identical for masked and exempt users", rows == admin_rows)


def test_composition():
    print("\n2. CTEs, subqueries, joins, set operations, views")
    cases = {
        "CTE": "WITH c AS (SELECT * FROM warehouse.hr.employees) SELECT email, ssn FROM c",
        "nested CTEs": "WITH a AS (SELECT * FROM warehouse.hr.employees), b AS (SELECT * FROM a) SELECT email, ssn FROM b",
        "subquery in FROM": "SELECT email, ssn FROM (SELECT * FROM warehouse.hr.employees) x",
        "IN subquery": "SELECT id FROM warehouse.hr.plain WHERE id IN (SELECT id FROM warehouse.hr.employees WHERE ssn LIKE '123%') ",
        "scalar subquery": "SELECT (SELECT max(ssn) FROM warehouse.hr.employees) AS m",
        "EXISTS correlated": "SELECT p.id FROM warehouse.hr.plain p WHERE EXISTS (SELECT 1 FROM warehouse.hr.employees e WHERE e.id = p.id AND e.email = 'ada@example.com')",
        "self join": "SELECT a.email, b.ssn FROM warehouse.hr.employees a JOIN warehouse.hr.employees b ON a.id = b.id",
        "join on masked key": "SELECT e.email, b.badge, b.ssn FROM warehouse.hr.employees e JOIN warehouse.hr.badges b ON e.ssn = b.ssn",
        "UNION": "SELECT ssn FROM warehouse.hr.employees UNION ALL SELECT ssn FROM warehouse.hr.badges",
        "INTERSECT": "SELECT ssn FROM warehouse.hr.employees INTERSECT SELECT ssn FROM warehouse.hr.badges",
        "window function": "SELECT ssn, row_number() OVER (ORDER BY ssn) FROM warehouse.hr.employees",
        "lateral/unnest": "SELECT unnest([email, ssn]) FROM warehouse.hr.employees",
        "view": "SELECT * FROM warehouse.hr.v_employees",
        "view over view with alias": "SELECT secret FROM warehouse.hr.v_deep",
        "view select *": "SELECT * FROM warehouse.hr.v_deep",
        "multi-statement": "SELECT 1; SELECT email, ssn FROM warehouse.hr.employees",
        "DESCRIBE query": "DESCRIBE SELECT email FROM warehouse.hr.employees",
        "derived table alias list": "SELECT a, b FROM (SELECT email, ssn FROM warehouse.hr.employees) AS t(a, b)",
    }
    for label, sql in cases.items():
        rows, res = run(sql)
        ok = (rows_ok(rows) or (rows is None and res.blocked)) and not leaks(rows if rows_ok(rows) else [])
        check(f"{label}: no raw value reaches the user", ok, (rows, res.blocked, res.sql[:220]))
    rows, res = run("SELECT e.email, b.badge FROM warehouse.hr.employees e JOIN warehouse.hr.badges b ON e.ssn = b.ssn ORDER BY e.id")
    check("joins on a consistently masked key still work (partial mask is deterministic)", rows_ok(rows) and len(rows) == 2, rows)

    # CTE shadowing: a CTE with a table's name is not the table; the real table outside the CTE scope must still be masked
    rows, _ = run("WITH employees AS (SELECT 'plain@x.com' AS email) SELECT email FROM employees")
    check("a CTE named like a table is not masked (it is not the table)", rows == [("plain@x.com",)], rows)
    rows, res = run("SELECT * FROM (WITH employees AS (SELECT 'y' AS email) SELECT * FROM employees) a, hr.employees e ORDER BY e.id")
    check("shadowing CTE in one scope does not exempt the real table in another", rows_ok(rows) and not leaks(rows) and rows[0][0] == "y", (rows, res.blocked))
    rows, res = run("WITH employees AS (SELECT 1 AS id) SELECT (SELECT max(ssn) FROM hr.employees), (SELECT id FROM employees)")
    check("CTE + qualified real table in the same statement", rows_ok(rows) and not leaks(rows), (rows, res.blocked))

    rows, res = run("EXPLAIN SELECT email, ssn FROM warehouse.hr.employees")
    check("EXPLAIN is rewritten (plan of the masked query)", rows_ok(rows) and res.changed and res.sql.upper().startswith("EXPLAIN "), (rows, res.blocked, res.sql[:80]))
    rows, res = run("EXPLAIN ANALYZE SELECT email FROM warehouse.hr.employees")
    check("EXPLAIN ANALYZE is rewritten and does not leak", rows_ok(rows) and not leaks(rows), (rows, res.blocked))


def test_context_independence():
    print("\n2b. Rewritten SQL does not depend on the executing session's default catalog")
    rows, res = run("SELECT email, ssn FROM employees")
    check("an unqualified name is pinned to its resolved table", rows_ok(rows) and "warehouse.hr.employees" in res.sql.replace('"', ""), (rows, res.blocked, res.sql))
    for home in (None, "warehouse.dbo", "warehouse.hr", "memory.main"):
        cur = Env.con.cursor()
        try:
            if home:
                cur.execute(f"USE {home}")
            got = cur.execute(res.sql).fetchall()
            check(f"the same rewritten SQL is masked when executed from {home or 'a fresh cursor'}", not leaks(got) and got[0][0] == "a***@example.com", got)
        finally:
            cur.close()
    rows, res = run("SELECT * FROM employees", home=None)
    check("from a session where the name cannot be resolved, a tagged name is refused instead of guessed", res.blocked and rows is None, (rows, res.sql))
    rows, res = run("SELECT count(*) FROM information_schema.tables")
    check("information_schema is left alone", rows_ok(rows) and res.blocked is None, (rows, res.blocked))


def test_no_oracles():
    print("\n3. No filter / ordering oracles")
    for label, sql in {
        "= raw ssn": "SELECT count(*) FROM warehouse.hr.employees WHERE ssn = '123-45-6789'",
        "LIKE prefix": "SELECT count(*) FROM warehouse.hr.employees WHERE ssn LIKE '123%'",
        "= raw email": "SELECT count(*) FROM warehouse.hr.employees WHERE email = 'ada@example.com'",
        "HAVING": "SELECT ssn FROM warehouse.hr.employees GROUP BY ssn HAVING ssn = '987-65-4321'",
        "CASE probe": "SELECT sum(CASE WHEN ssn = '555-12-3456' THEN 1 ELSE 0 END) FROM warehouse.hr.employees",
        "IN list": "SELECT count(*) FROM warehouse.hr.employees WHERE ssn IN ('123-45-6789','987-65-4321')",
        "regexp": "SELECT count(*) FROM warehouse.hr.employees WHERE regexp_matches(email, '^ada@')",
        "substr probe": "SELECT count(*) FROM warehouse.hr.employees WHERE substr(ssn, 1, 3) = '123'",
        "join probe": "SELECT count(*) FROM warehouse.hr.employees e JOIN (SELECT '123-45-6789' AS ssn) k ON e.ssn = k.ssn",
    }.items():
        rows, res = run(sql)
        check(f"{label}: a raw guess matches nothing for a masked user", rows_ok(rows) and (rows == [] or rows[0][0] in (0, None, 0.0)), (rows, res.blocked))
    rows, _ = run("SELECT count(*) FROM warehouse.hr.employees WHERE ssn = '123-45-6789'", ADMIN)
    check("...while the exempt admin gets the real match (control)", rows == [(1,)], rows)
    rows, _ = run("SELECT count(DISTINCT ssn) FROM warehouse.hr.employees")
    check("masked values still group/count", rows == [(3,)], rows)


def test_paths(delta_dir):
    print("\n4. Path-based scans")
    rows, res = run(f"SELECT * FROM delta_scan('{delta_dir}')")
    check("delta_scan of a tagged table's directory is masked", rows_ok(rows) and not leaks(rows) and res.changed, (rows, res.blocked))
    rows, res = run(f"SELECT * FROM delta_scan('{delta_dir}') d WHERE d.ssn = '123-45-6789'")
    check("...with an alias and a probing filter", rows_ok(rows) and rows == [], (rows, res.blocked))
    rows, res = run(f"SELECT email, ssn FROM read_parquet('{delta_dir}/*.parquet')")
    check("read_parquet of the table's data files is masked", rows_ok(rows) and not leaks(rows), (rows, res.blocked))
    rows, res = run(f"SELECT email FROM '{delta_dir}/part-00000.parquet'" if False else f"SELECT * FROM read_parquet(['{delta_dir}/x.parquet'])")
    check("list-of-paths form is resolved (missing file is an engine error, not a leak)", rows is None or not leaks([rows]), (rows, res.blocked))
    rows, res = run("SELECT * FROM warehouse.hr.delta_emp")
    check("the registered Delta view is masked", rows_ok(rows) and rows[0][1] == "a***@example.com" and rows[0][2] == "*******6789", (rows, res.blocked))
    check("...exactly once (its own tags and its delta_scan path resolve to the same table)",
          res.sql.count("gov_mask_email") == 1 and res.sql.count("gov_mask_partial") == 1, res.sql)
    rows, res = run(f"SELECT * FROM delta_scan('{delta_dir}')", home=None)
    check("path scans are attributed to the warehouse catalog even from a session that starts in memory.main",
          rows_ok(rows) and not leaks(rows) and res.changed, (rows, res.blocked, res.sql[:160]))
    check("computed path is refused", run("SELECT * FROM read_parquet('/tmp/' || 'x.parquet')")[1].blocked)
    check("read_text on a masked table's files is refused", run(f"SELECT * FROM read_text('{delta_dir}/_delta_log/00000000000000000000.json')")[1].blocked)
    check("query() is refused", run("SELECT * FROM query('SELECT * FROM warehouse.hr.employees')")[1].blocked)
    check("query_table() is refused", run("SELECT * FROM query_table('warehouse.hr.employees')")[1].blocked)
    check("unknown table functions are refused while masking applies", run("SELECT * FROM glob('/etc/*')")[1].blocked)
    check("allowed table functions still work", run("SELECT count(*) FROM range(5)")[0] == [(5,)])

    meta = os.path.join(TMP_WAREHOUSE, ".metadata", "jwt_secret")
    for label, sql in {"read_text": f"SELECT * FROM read_text('{meta}')", "read_csv": f"SELECT * FROM read_csv('{meta}')",
                       "relative path": "SELECT * FROM read_text('warehouse/.metadata/jwt_secret')",
                       "string table": f"SELECT * FROM '{meta}'", "dot-dot traversal": f"SELECT * FROM read_text('{TMP_WAREHOUSE}/hr/../.metadata/jwt_secret')",
                       "glob": f"SELECT * FROM read_text('{TMP_WAREHOUSE}/.meta*/*')", "COPY": f"COPY (SELECT 1) TO '{TMP_WAREHOUSE}/.metadata/x.csv'"}.items():
        check(f"platform secrets are unreachable via {label}", run(sql)[1].blocked, run(sql)[1].sql)
    check("outside the warehouse is refused", run("SELECT * FROM read_text('/etc/passwd')")[1].blocked)
    check("a symlink into .metadata is still metadata", (os.symlink(os.path.join(TMP_WAREHOUSE, ".metadata"), os.path.join(TMP_WAREHOUSE, "exports_link")) or True)
          and run(f"SELECT * FROM read_text('{TMP_WAREHOUSE}/exports_link/jwt_secret')")[1].blocked)
    rows, res = run(f"SELECT * FROM read_csv('{TMP_WAREHOUSE}/volumes/warehouse/raw/inbox/f.csv')")
    check("volume files stay readable", rows_ok(rows) and rows == [(1, 2)], (rows, res.blocked))
    check("admin may still read anything", run(f"SELECT * FROM read_text('{meta}')", ADMIN)[1].blocked is None)


def test_gating():
    print("\n5. Statement gating for masked principals")
    blocked = {
        "gov_ function call": "SELECT gov_mask_hash('123-45-6789')",
        "qualified mask macro call": "SELECT memory.main.gov_mask_email('ada@example.com')",
        "redefine a mask macro": "CREATE OR REPLACE MACRO memory.main.gov_mask_email(v) AS v",
        "drop a mask macro": "DROP MACRO memory.main.gov_mask_email",
        "create any macro": "CREATE MACRO m(x) AS x",
        "SUMMARIZE": "SUMMARIZE warehouse.hr.employees",
        "COPY table TO": "COPY warehouse.hr.employees TO '/tmp/out.csv'",
        "COPY query TO": "COPY (SELECT * FROM warehouse.hr.employees) TO '/tmp/out.csv'",
        "UPDATE masked table": "UPDATE warehouse.hr.employees SET dept = 'x' WHERE ssn = '123-45-6789'",
        "DELETE masked table": "DELETE FROM warehouse.hr.employees WHERE email = 'ada@example.com'",
        "CREATE VIEW over masked": "CREATE VIEW warehouse.hr.leak AS SELECT * FROM warehouse.hr.employees",
        "PIVOT": "PIVOT warehouse.hr.employees ON dept USING count(*)",
        "SET engine option": "SET threads = 1",
        "PRAGMA": "PRAGMA database_list",
        "ATTACH": "ATTACH '/tmp/x.db' AS x",
        "INSTALL": "INSTALL httpfs",
        "LOAD": "LOAD httpfs",
        "CALL": "CALL pragma_version()",
    }
    for label, sql in blocked.items():
        rows, res = run(sql)
        check(f"blocked: {label}", res.blocked and rows is None, (rows, res.sql))
    check("mask macros were not damaged by the attempts", run("SELECT memory.main.gov_mask_email('ada@example.com')", SYSTEM)[0] == [("a***@example.com",)])

    rows, res = run("SELECT * FROM warehouse.hr.plain ORDER BY id")
    check("tables without tagged columns return the same rows (and are pinned to their resolved name)",
          rows == [(1, "hello"), (2, "world")] and not res.masked and "warehouse.hr.plain" in res.sql, (rows, res.blocked, res.sql))
    check("CREATE TABLE / INSERT VALUES on unrelated tables still work",
          run("CREATE TABLE warehouse.hr.scratch (x INT)")[1].blocked is None and run("INSERT INTO warehouse.hr.scratch VALUES (1)")[1].blocked is None)
    run("DROP TABLE IF EXISTS warehouse.hr.scratch")

    rows, res = run("CREATE OR REPLACE TABLE warehouse.hr.copy_masked AS SELECT * FROM warehouse.hr.employees")
    dump, _ = run("SELECT * FROM warehouse.hr.copy_masked ORDER BY id", SYSTEM)
    check("CTAS stores the masked values, never the raw ones", res.blocked is None and not leaks(dump) and dump[0][2] == "a***@example.com", (dump, res.blocked))
    run("CREATE OR REPLACE TABLE warehouse.hr.ins_target (id INT, email VARCHAR)")
    _, res = run("INSERT INTO warehouse.hr.ins_target SELECT id, email FROM warehouse.hr.employees")
    dump, _ = run("SELECT * FROM warehouse.hr.ins_target ORDER BY id", SYSTEM)
    check("INSERT ... SELECT stores masked values", res.blocked is None and not leaks(dump) and dump[0][1] == "a***@example.com", (dump, res.blocked))
    run("DROP TABLE warehouse.hr.copy_masked"); run("DROP TABLE warehouse.hr.ins_target")

    # unparseable SQL that mentions a tagged table is refused; unrelated unparseable SQL is left to the engine
    rows, res = run("SELECT * FROM warehouse.hr.employees FOR SYSTEM_VERSION AS OF 3")
    check("unparseable SQL that mentions a tagged table is refused (fail closed)", res.blocked and rows is None, (rows, res.blocked))
    rows, res = run("SELECT * FROM warehouse.hr.plain FOR SYSTEM_VERSION AS OF 3")
    check("unparseable SQL about untagged data is passed through to the engine", res.blocked is None, res.blocked)
    check("...and admins are never blocked on parse failures", run("SELECT * FROM warehouse.hr.employees FOR SYSTEM_VERSION AS OF 3", ADMIN)[1].blocked is None)
    check("a table that cannot be resolved but shares a tagged name is refused (conservative)",
          run("SELECT * FROM other_schema.employees")[1].blocked is not None)
    check("an unresolvable name that is not tagged is left to the engine (it will report the error)",
          run("SELECT * FROM no_such_table")[1].blocked is None)


def test_without_policies():
    print("\n6. Non-admin guarantees with NO masking policies")
    saved = policies.list_policies()
    for p in saved:
        policies.delete_policy(p["id"])
    try:
        rows, res = run("SELECT * FROM warehouse.hr.employees ORDER BY id")
        check("without policies nothing is masked and SQL is untouched", rows[0][2] == "ada@example.com" and res.changed is False and res.sql.startswith("SELECT *"), rows[:1])
        meta = os.path.join(TMP_WAREHOUSE, ".metadata", "jwt_secret")
        check(".metadata is unreachable for a plain user", run(f"SELECT * FROM read_text('{meta}')")[1].blocked)
        check(".metadata is unreachable through a computed path", run("SELECT * FROM read_text('.meta' || 'data/jwt_secret')")[1].blocked)
        check("/etc/passwd is unreachable for a plain user", run("SELECT * FROM read_text('/etc/passwd')")[1].blocked)
        check("query() is refused for a plain user (it would hide a file read)", run("SELECT * FROM query('SELECT 1')")[1].blocked)
        check("mask macros cannot be redefined even before any policy exists", run("CREATE OR REPLACE MACRO memory.main.gov_mask_email(v) AS v")[1].blocked)
        check("ordinary macros are fine", run("CREATE OR REPLACE MACRO my_double(x) AS x * 2")[1].blocked is None)
        check("admin is unaffected", run(f"SELECT * FROM read_text('{meta}')", ADMIN)[1].blocked is None)
        check("plain users can still read volumes", run(f"SELECT * FROM read_csv('{TMP_WAREHOUSE}/volumes/warehouse/raw/inbox/f.csv')")[0] == [(1, 2)])
    finally:
        for p in saved:
            policies.create_policy({k: p[k] for k in ("name", "description", "tag_key", "tag_value", "mask_type", "mask_expr", "applies_to_types",
                                                      "except_roles", "except_users", "priority", "enabled")})


def test_exemptions_audit_modes():
    print("\n7. Exemptions, audit trail, modes")
    since = store.utcnow()
    run("SELECT email FROM warehouse.hr.employees", BOB)
    run("SELECT email FROM warehouse.hr.employees", ADMIN)
    run("SELECT * FROM query('SELECT 1')", BOB)
    ev = store.list_audit(since=since, limit=100)
    actions = {e["action"] for e in ev}
    check("audit has MASK_APPLIED, EXEMPT_READ and QUERY_BLOCKED", {"MASK_APPLIED", "EXEMPT_READ", "QUERY_BLOCKED"} <= actions, actions)
    applied = next(e for e in ev if e["action"] == "MASK_APPLIED")
    check("MASK_APPLIED lists tables, columns and policies but no values or SQL",
          "warehouse.hr.employees.email" in applied["detail"]["columns"] and "Email masking" in applied["detail"]["policies"]
          and not leaks([applied["detail"]]) and "SELECT" not in str(applied["detail"]), applied)
    ex = next(e for e in ev if e["action"] == "EXEMPT_READ")
    check("EXEMPT_READ names the admin and the columns read raw", ex["actor"] == "admin" and "warehouse.hr.employees.email" in ex["detail"]["columns"], ex)

    ep = policies.create_policy({"name": "Lead sees email", "tag_key": "pii", "tag_value": "email", "mask_type": "null", "priority": 1,
                                 "except_users": ["lead_engineer"]})
    rows, _ = run("SELECT email FROM warehouse.hr.employees ORDER BY id", LEAD)
    check("per-user exemption: lead is not nulled by the priority-1 policy but is still partial-masked by the next", rows[0][0] == "a***@example.com", rows)
    rows, _ = run("SELECT email FROM warehouse.hr.employees ORDER BY id", BOB)
    check("...while everyone else is nulled by it", rows[0][0] is None, rows)
    policies.delete_policy(ep["id"])

    os.environ["GOVERNANCE_ENFORCEMENT"] = "audit"
    try:
        rows, res = run("SELECT email FROM warehouse.hr.employees ORDER BY id")
        check("audit mode: results are unchanged (raw) but the would-be masking is computed", rows[0][0] == "ada@example.com" and res.sql.startswith("SELECT email"), rows)
        rows, res = run("SELECT * FROM query('SELECT 1')")
        check("audit mode never blocks", res.blocked is None, res.blocked)
    finally:
        os.environ.pop("GOVERNANCE_ENFORCEMENT", None)
    os.environ["GOVERNANCE_ENFORCEMENT"] = "off"
    try:
        rows, res = run("SELECT email FROM warehouse.hr.employees ORDER BY id")
        check("off mode: gateway is inert", rows[0][0] == "ada@example.com" and res.changed is False)
    finally:
        os.environ.pop("GOVERNANCE_ENFORCEMENT", None)

    con = Env.con.cursor()
    rel = enforce.masked_relation("warehouse", "hr", "employees", BOB, con)
    check("masked_relation returns a masked subquery for a plain user", rel.strip().startswith("(") and "gov_mask" in rel, rel[:120])
    check("...that yields masked data when used in a FROM clause", not leaks(con.execute(f"SELECT * FROM {rel}").fetchall()))
    check("...and the plain identifier for an exempt principal", enforce.masked_relation("warehouse", "hr", "employees", ADMIN, con) == '"warehouse"."hr"."employees"')
    check("masked_relation on a nonexistent table is passed through (the engine reports it)",
          enforce.masked_relation("warehouse", "hr", "ghost_table", BOB, con).endswith('"ghost_table"'))
    con.close()


def test_fuzz_and_perf():
    print("\n8. Randomised leak check and performance")
    rnd = random.Random(20260920)
    tables = ["warehouse.hr.employees", "hr.employees", "warehouse.hr.v_employees", "warehouse.hr.v_deep", "warehouse.hr.badges", "warehouse.hr.delta_emp"]
    cols = {"warehouse.hr.employees": ["id", "first_name", "email", "ssn", "salary", "dob", "dept"], "hr.employees": ["id", "email", "ssn", "dept"],
            "warehouse.hr.v_employees": ["id", "email", "ssn", "dept"], "warehouse.hr.v_deep": ["id", "secret", "dept"],
            "warehouse.hr.badges": ["id", "ssn", "badge"], "warehouse.hr.delta_emp": ["id", "email", "ssn", "dept"]}

    def proj(t):
        pick = rnd.sample(cols[t], k=rnd.randint(1, len(cols[t])))
        shapes = [lambda c: c, lambda c: f"upper(CAST({c} AS VARCHAR))", lambda c: f"{c} AS x_{c}", lambda c: f"length(CAST({c} AS VARCHAR))",
                  lambda c: f"coalesce(CAST({c} AS VARCHAR), 'n')"]
        return ", ".join(rnd.choice(shapes)(c) for c in pick)

    def pred(t):
        c = rnd.choice(cols[t])
        return rnd.choice([f"{c} IS NOT NULL", f"CAST({c} AS VARCHAR) LIKE '%1%'", "1=1", f"CAST({c} AS VARCHAR) <> 'zzz'"])

    generators = [
        lambda: (lambda t: f"SELECT * FROM {t}")(rnd.choice(tables)),
        lambda: (lambda t: f"SELECT {proj(t)} FROM {t}")(rnd.choice(tables)),
        lambda: (lambda t: f"SELECT {proj(t)} FROM {t} WHERE {pred(t)}")(rnd.choice(tables)),
        lambda: (lambda t: f"SELECT * FROM (SELECT {proj(t)} FROM {t}) q")(rnd.choice(tables)),
        lambda: (lambda t: f"WITH w AS (SELECT * FROM {t}) SELECT * FROM w")(rnd.choice(tables)),
        lambda: (lambda a, b: f"SELECT x.*, y.* FROM {a} x JOIN {b} y ON x.id = y.id")(rnd.choice(tables), rnd.choice(tables)),
        lambda: (lambda t: f"SELECT * FROM {t} UNION ALL SELECT * FROM {t}")(rnd.choice(tables[:1] + tables[2:3])),
        lambda: (lambda t: f"SELECT count(*), max(CAST({rnd.choice(cols[t])} AS VARCHAR)) FROM {t}")(rnd.choice(tables)),
        lambda: (lambda t: f"SELECT * FROM {t} t WHERE EXISTS (SELECT 1 FROM {t} u WHERE u.id = t.id)")(rnd.choice(tables)),
        lambda: (lambda t: f"SELECT list(CAST({rnd.choice(cols[t])} AS VARCHAR)) FROM {t}")(rnd.choice(tables)),
    ]
    leaked, errored, ran, blocked = [], 0, 0, 0
    for _ in range(400):
        sql = rnd.choice(generators)()
        rows, res = run(sql)
        if res.blocked:
            blocked += 1
        elif not rows_ok(rows):
            errored += 1                                     # engine error (e.g. type mismatch in a random query): must not leak either
            if leaks([rows]):
                leaked.append(sql)
        else:
            ran += 1
            if leaks(rows):
                leaked.append(sql)
    check("400 random queries: zero raw sensitive values in any result", not leaked, leaked[:3])
    print(f"      ({ran} ran, {errored} engine errors, {blocked} blocked)")
    check("the fuzz actually exercised the masking path", ran > 250, ran)

    cur = Env.con.cursor()
    sql = ("WITH d AS (SELECT dept, count(*) c FROM warehouse.hr.employees GROUP BY dept) "
           "SELECT e.first_name, e.email, b.badge, d.c FROM warehouse.hr.employees e JOIN warehouse.hr.badges b ON e.id = b.id "
           "JOIN d ON d.dept = e.dept WHERE e.ssn IS NOT NULL ORDER BY e.id")
    samples = []
    for _ in range(200):
        t0 = time.perf_counter()
        enforce.rewrite_for_principal(sql, BOB, cur)
        samples.append((time.perf_counter() - t0) * 1000)
    samples.sort()
    p50, p95 = statistics.median(samples), samples[int(len(samples) * 0.95)]
    print(f"      rewrite latency: p50={p50:.2f} ms  p95={p95:.2f} ms")
    check("rewrite p95 latency stays under 25 ms", p95 < 25, p95)
    t0 = time.perf_counter()
    for _ in range(200):
        enforce.rewrite_for_principal(sql, ADMIN, cur)
    check("exempt principals pay little", (time.perf_counter() - t0) / 200 * 1000 < 25)
    cur.close()


def main():
    try:
        delta_dir = setup()
        test_basic_shapes()
        test_composition()
        test_context_independence()
        test_no_oracles()
        test_paths(delta_dir)
        test_gating()
        test_without_policies()
        test_exemptions_audit_modes()
        test_fuzz_and_perf()
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Governance Phase 3 checks passed.")


if __name__ == "__main__":
    main()
