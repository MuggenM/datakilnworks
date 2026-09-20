#!/usr/bin/env python3
"""
Phase 1 verification for tag-based masking: tag store, inheritance, classifier and REST API.
Runs against a throwaway WAREHOUSE_DIR, so it never touches real data.
Tests:
1. Store: idempotent init, one-time seeding, definition validation, versioned cache.
2. Assignments: hierarchy rules, allowed values, upsert, audit trail.
3. Inheritance: catalog -> schema -> table -> column, most specific wins, unset falls back.
4. Lifecycle: orphaning on column drop, restore, rename, drop.
5. Classifier: column-name heuristics (names only, never values).
6. API: RBAC, validation against live catalogs, bulk apply, suggestions, audit, delete-in-use.
"""

import datetime
import os
import shutil
import sqlite3
import sys
import tempfile

# The default catalog is named after the warehouse directory, so mirror the real layout (.../warehouse).
TMP_ROOT = tempfile.mkdtemp(prefix="governance_p1_")
TMP_WAREHOUSE = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(TMP_WAREHOUSE)
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_RESTRICT_NOTEBOOKS", "JWT_SECRET_KEY"):
    os.environ.pop(var, None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import jwt
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth
from web.governance import catalog_meta, classify, store, tags

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
    except Exception as e:  # wrong exception type
        return False
    return False


def cookie_for(username):
    user = auth.get_user_by_username(username)
    token = jwt.encode({"sub": user["id"], "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)},
                       auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)
    return {auth.COOKIE_NAME: token}


def setup_fixture():
    con = app_module.get_duckrun_conn().con
    con.execute("CREATE SCHEMA IF NOT EXISTS warehouse.hr")
    con.execute("""CREATE OR REPLACE TABLE warehouse.hr.employees (
        id INTEGER, first_name VARCHAR, email VARCHAR, ssn VARCHAR, salary DECIMAL(10,2), dob DATE, notes VARCHAR)""")
    con.execute("INSERT INTO warehouse.hr.employees VALUES (1,'Ada','ada@example.com','123-45-6789',100000,'1990-01-02','x')")
    return con


def test_store_and_definitions():
    print("\n1. Store & definitions")
    store.init_governance_db()
    store.init_governance_db()
    keys = [d["tag_key"] for d in tags.list_definitions()]
    check("init is idempotent and seeds pii + sensitivity once", keys == ["pii", "sensitivity"], keys)
    tags.delete_definition("pii", force=True)
    store.init_governance_db()
    check("a deleted seed definition is not re-seeded", "pii" not in [d["tag_key"] for d in tags.list_definitions()])
    tags.create_definition("pii", "Personal data", ["email", "phone", "ssn", "name", "dob"], actor="admin")

    check("rejects bad tag keys", all(raises(ValueError, tags.create_definition, k) for k in ("", "Bad Key", "-x", "a" * 65, "x/y")))
    check("rejects duplicates", raises(ValueError, tags.create_definition, "pii"))
    d = tags.create_definition("Owner_Team", "Owning team", actor="admin")
    check("keys are normalised to lower case; free-form values", d["tag_key"] == "owner_team" and d["allowed_values"] is None, d)
    check("unknown definition -> NotFound", raises(tags.NotFound, tags.get_definition, "nope"))

    v0 = store.get_version()
    tags.create_definition("temp_tag")
    check("writes bump the governance version", store.get_version() == v0 + 1)
    tags.delete_definition("temp_tag")


def test_assignments_and_inheritance():
    print("\n2. Assignments & inheritance")
    check("column tag needs a table", raises(ValueError, tags.set_tag, catalog="warehouse", schema_name="hr", column_name="ssn", tag_key="pii", tag_value="ssn"))
    check("table tag needs a schema", raises(ValueError, tags.set_tag, catalog="warehouse", table_name="employees", tag_key="pii", tag_value="ssn"))
    check("undefined tag rejected", raises(tags.NotFound, tags.set_tag, catalog="warehouse", tag_key="ghost"))
    check("value outside allowed_values rejected", raises(ValueError, tags.set_tag, catalog="warehouse", schema_name="hr",
                                                          table_name="employees", column_name="ssn", tag_key="pii", tag_value="banana"))
    check("over-long value rejected", raises(ValueError, tags.set_tag, catalog="warehouse", tag_key="owner_team", tag_value="x" * 200))

    tags.set_tag(catalog="warehouse", tag_key="sensitivity", tag_value="internal", actor="admin")
    tags.set_tag(catalog="warehouse", schema_name="hr", tag_key="sensitivity", tag_value="confidential", actor="admin")
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", tag_key="owner_team", tag_value="hr-platform", actor="admin")
    tags.set_tag(catalog="Warehouse", schema_name="HR", table_name="Employees", column_name="SSN", tag_key="pii", tag_value="ssn", actor="admin")
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="salary", tag_key="sensitivity",
                 tag_value="restricted", actor="admin")

    eff = tags.effective_tags("warehouse", "hr", "employees", ["id", "ssn", "salary"])
    check("plain column inherits schema over catalog (most specific wins)", eff["id"]["sensitivity"] == {"value": "confidential", "level": "schema"}, eff["id"])
    check("table tag is inherited by every column", eff["id"]["owner_team"] == {"value": "hr-platform", "level": "table"})
    check("column tag applies (case-insensitive names)", eff["ssn"]["pii"] == {"value": "ssn", "level": "column"}, eff["ssn"])
    check("column tag overrides the same key from a higher level", eff["salary"]["sensitivity"] == {"value": "restricted", "level": "column"})
    check("other columns unaffected by a sibling's tag", "pii" not in eff["id"])

    other = tags.effective_tags("warehouse", "finance", "ledger", ["amount"])
    check("catalog tag reaches other schemas; schema tag does not", other["amount"]["sensitivity"]["level"] == "catalog"
          and "owner_team" not in other["amount"], other)

    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="salary", tag_key="sensitivity",
                 tag_value="confidential", actor="admin")
    check("re-tagging updates in place (upsert)", tags.effective_tags("warehouse", "hr", "employees", ["salary"])["salary"]["sensitivity"]["value"] == "confidential")
    check("unset returns True once", tags.unset_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="salary", tag_key="sensitivity", actor="admin")
          and not tags.unset_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="salary", tag_key="sensitivity"))
    check("unset falls back to the inherited value", tags.effective_tags("warehouse", "hr", "employees", ["salary"])["salary"]["sensitivity"]["level"] == "schema")

    # cache follows the version even when another process writes
    other_conn = sqlite3.connect(store.GOV_DB_PATH)
    other_conn.execute("INSERT INTO object_tags (catalog, schema_name, table_name, column_name, tag_key, tag_value, created_by, created_at) "
                       "VALUES ('warehouse','hr','employees','notes','owner_team','ext','other-proc','now')")
    other_conn.execute("UPDATE governance_meta SET value = CAST(value AS INTEGER) + 1 WHERE key = 'version'")
    other_conn.commit()
    other_conn.close()
    check("cache is invalidated by a write from another process",
          tags.effective_tags("warehouse", "hr", "employees", ["notes"])["notes"]["owner_team"]["value"] == "ext")
    tags.unset_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="notes", tag_key="owner_team")

    check("table_has_any_tags sees inherited tags", tags.table_has_any_tags("warehouse", "hr", "employees")
          and tags.table_has_any_tags("warehouse", "brand_new", "table"))

    actions = [e["action"] for e in store.list_audit(limit=500)]
    check("audit trail recorded defines, sets and unsets", {"TAG_DEFINE", "TAG_SET", "TAG_UNSET"} <= set(actions), sorted(set(actions)))
    ev = [e for e in store.list_audit(action="TAG_SET") if e["object"] == "warehouse.hr.employees.ssn"]
    check("audit detail has tag, value and level", ev and ev[0]["detail"]["tag"] == "pii" and ev[0]["detail"]["level"] == "column", ev[:1])
    check("audit filters work", all(e["actor"] == "admin" for e in store.list_audit(actor="admin")))


def test_lifecycle(con):
    print("\n3. Lifecycle")
    res = tags.reconcile(con)
    check("reconcile with everything present flags nothing", res["orphaned"] == 0, res)

    con.execute("ALTER TABLE warehouse.hr.employees DROP COLUMN ssn")
    res = tags.reconcile(con)
    check("dropping a tagged column orphans its tag", res["orphaned"] == 1, res)
    check("orphaned tags are ignored by resolution", "pii" not in tags.effective_tags("warehouse", "hr", "employees", ["id"])["id"])
    listing = tags.list_assignments(tag_key="pii")
    check("orphaned tags stay listed for review", listing and listing[0]["orphaned"] == 1, listing)
    check("definitions report orphaned counts", [d for d in tags.list_definitions() if d["tag_key"] == "pii"][0]["orphaned_assignments"] == 1)

    con.execute("ALTER TABLE warehouse.hr.employees ADD COLUMN ssn VARCHAR")
    res = tags.reconcile(con)
    check("re-adding the column restores the tag", res["restored"] == 1 and
          tags.effective_tags("warehouse", "hr", "employees", ["ssn"])["ssn"]["pii"]["value"] == "ssn", res)

    moved = tags.rename_table("warehouse", "hr", "employees", "staff")
    check("rename re-keys table and column tags", moved >= 2 and
          tags.effective_tags("warehouse", "hr", "staff", ["ssn"])["ssn"]["pii"]["value"] == "ssn")
    tags.rename_table("warehouse", "hr", "staff", "employees")
    removed = tags.drop_object("warehouse", "hr", "employees", "ssn")
    check("drop_object removes column tags", removed == 1 and "pii" not in tags.effective_tags("warehouse", "hr", "employees", ["ssn"])["ssn"])
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="ssn", tag_key="pii", tag_value="ssn")


def test_classifier():
    print("\n4. Classifier")
    cases = {
        "email": ("pii", "email"), "CustomerEmail": ("pii", "email"), "contact-email": ("pii", "email"),
        "phone_number": ("pii", "phone"), "ssn": ("pii", "ssn"), "first_name": ("pii", "name"), "lastName": ("pii", "name"),
        "dob": ("pii", "dob"), "dateOfBirth": ("pii", "dob"), "ip_address": ("pii", "ip"), "zip_code": ("pii", "address"),
        "iban": ("pii", "financial"), "credit_card_number": ("pii", "financial"), "password": ("sensitivity", "restricted"),
        "api_key": ("sensitivity", "restricted"), "salary": ("sensitivity", "confidential"),
    }
    for col, (key, value) in cases.items():
        hit = classify.classify_column(col, "VARCHAR")
        check(f"'{col}' -> {key}={value}", hit and (hit["tag_key"], hit["tag_value"]) == (key, value), hit)
    for col in ("id", "description", "created_at", "username", "recipient_count", "shipping_method", "chip_id"):
        check(f"'{col}' is not flagged", classify.classify_column(col, "VARCHAR") is None, classify.classify_column(col))
    generic = classify.classify_column("name", "VARCHAR")
    check("generic 'name' is low confidence", generic and generic["confidence"] < 0.5, generic)
    check("non-text columns are dampened", classify.classify_column("email", "INTEGER")["confidence"] < classify.classify_column("email", "VARCHAR")["confidence"])
    check("normalize handles camel case and separators", classify.normalize_column_name("HTTPServerIPAddress") == "http_server_ip_address")

    cols = [{"catalog": "warehouse", "schema": "hr", "table": "t", "column": "email", "type": "VARCHAR"},
            {"catalog": "warehouse", "schema": "hr", "table": "t", "column": "name", "type": "VARCHAR"}]
    sugg = classify.suggest_for_columns(cols, {("warehouse", "hr", "t", "email"): {"pii"}})
    check("already-tagged columns are not suggested again; low confidence filtered", sugg == [], sugg)


def test_api(con):
    print("\n5. REST API")
    client = TestClient(app_module.app)
    admin, bob = cookie_for("admin"), cookie_for("analyst_bob")
    lead = cookie_for("lead_engineer")

    r = client.get("/api/governance/tags", cookies=bob)
    check("any signed-in user can list definitions", r.status_code == 200 and len(r.json()["tags"]) >= 2, r.text[:150])
    r = client.post("/api/governance/tags", json={"tag_key": "region", "allowed_values": ["eu", "us"]}, cookies=bob)
    check("non-admin cannot create definitions", r.status_code == 403, r.status_code)
    r = client.post("/api/governance/tags", json={"tag_key": "region", "allowed_values": ["eu", "us"]}, cookies=admin)
    check("admin creates a definition", r.status_code == 200 and r.json()["allowed_values"] == ["eu", "us"], r.text[:150])
    r = client.post("/api/governance/tags", json={"tag_key": "region"}, cookies=admin)
    check("duplicate -> 400", r.status_code == 400, r.status_code)

    body = {"set": [{"tag_key": "pii", "tag_value": "email"}], "unset": []}
    url = "/api/governance/objects/warehouse/hr/employees/columns/email/tags"
    check("plain user cannot tag", client.put(url, json=body, cookies=bob).status_code == 403)
    check("power user who does not own the catalog cannot tag", client.put(url, json=body, cookies=lead).status_code == 403)
    r = client.put(url, json=body, cookies=admin)
    check("admin tags a column", r.status_code == 200 and r.json()["applied"][0]["object"] == "warehouse.hr.employees.email", r.text[:200])
    r = client.put("/api/governance/objects/warehouse/hr/employees/columns/nope/tags", json=body, cookies=admin)
    check("unknown column -> 400 with a precise message", r.status_code == 400 and "does not exist" in r.json()["detail"], r.text[:200])
    r = client.put("/api/governance/objects/warehouse/hr/ghost/tags", json=body, cookies=admin)
    check("unknown table -> 400", r.status_code == 400, r.status_code)
    r = client.put("/api/governance/objects/ghost_catalog/tags", json=body, cookies=admin)
    check("unknown catalog -> 400", r.status_code == 400, r.status_code)
    r = client.put(url, json={"set": [{"tag_key": "pii", "tag_value": "banana"}]}, cookies=admin)
    check("disallowed value -> 400", r.status_code == 400 and "allowed" in r.json()["detail"].lower(), r.text[:200])
    r = client.put("/api/governance/objects/warehouse/hr/tags", json={"set": [{"tag_key": "sensitivity", "tag_value": "internal"}]}, cookies=admin)
    check("schema-level tag works", r.status_code == 200, r.text[:150])

    r = client.get("/api/governance/objects/warehouse/hr/employees/tags", cookies=bob)
    data = r.json()
    email = next(c for c in data["columns"] if c["column"] == "email")
    check("plain user can read effective tags of a public catalog", r.status_code == 200 and email["effective"]["pii"]["level"] == "column", r.text[:200])
    check("effective view includes inherited schema tags", email["effective"]["sensitivity"]["level"] == "schema", email)
    check("direct vs inherited are separated", set(email["direct"]) == {"pii"} and data["schema_tags"].get("sensitivity") == "internal", data["schema_tags"])
    check("unknown table read -> 404", client.get("/api/governance/objects/warehouse/hr/ghost/tags", cookies=bob).status_code == 404)

    from web.permissions import get_all_catalog_ids, can_user_access_catalog
    bob_user = auth.get_user_by_username("analyst_bob")
    blocked = [c for c in get_all_catalog_ids() if not can_user_access_catalog(bob_user, c)]
    if blocked:
        cat = blocked[0]
        r = client.get(f"/api/governance/objects/{cat}/dbo/x/tags", cookies=bob)
        check(f"reading tags of a catalog the user cannot access ({cat}) -> 403", r.status_code == 403, r.status_code)
    else:
        print("  [SKIP] no catalog is inaccessible to analyst_bob in this fixture")

    tags.unset_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="ssn", tag_key="pii")
    r = client.get("/api/governance/suggestions?catalog=warehouse&schema_name=hr", cookies=admin)
    sugg = r.json()["suggestions"]
    names = {s["column_name"] for s in sugg}
    check("suggestions list untagged sensitive columns", r.status_code == 200 and {"ssn", "salary", "dob", "first_name"} <= names and "email" not in names, names)
    check("suggestions need catalog management rights", client.get("/api/governance/suggestions?catalog=warehouse", cookies=bob).status_code == 403)

    items = [s for s in sugg if s["column_name"] in ("ssn", "dob")]
    payload = {"assignments": [{"catalog": s["catalog"], "schema_name": s["schema_name"], "table_name": s["table_name"],
                                "column_name": s["column_name"], "tag_key": s["tag_key"], "tag_value": s["tag_value"], "source": "suggested"} for s in items]
                             + [{"catalog": "warehouse", "schema_name": "hr", "table_name": "employees", "column_name": "ghost", "tag_key": "pii", "tag_value": "ssn"}]}
    r = client.post("/api/governance/tags/apply", json=payload, cookies=admin)
    res = r.json()
    check("bulk apply: valid items applied, invalid reported per item", r.status_code == 200 and res["applied"] == len(items) and res["failed"] == 1, res)
    assign = [a for a in tags.list_assignments(tag_key="pii") if a["column_name"] == "ssn"]
    check("accepted suggestions are stored with source='suggested'", assign and assign[0]["source"] == "suggested", assign)
    check("bulk apply is also permission-checked", client.post("/api/governance/tags/apply", json=payload, cookies=bob).json()["applied"] == 0)

    r = client.get("/api/governance/tags/pii/usage", cookies=admin)
    check("usage lists assignments", r.status_code == 200 and len(r.json()["assignments"]) >= 2, r.text[:150])
    check("audit is admin-only", client.get("/api/governance/audit", cookies=bob).status_code == 403
          and client.get("/api/governance/audit", cookies=admin).status_code == 200)
    check("reconcile is admin-only", client.post("/api/governance/reconcile", cookies=bob).status_code == 403
          and client.post("/api/governance/reconcile", cookies=admin).status_code == 200)

    r = client.delete("/api/governance/tags/pii", cookies=admin)
    check("deleting a tag in use is refused", r.status_code == 400 and "in use" in r.json()["detail"], r.text[:200])
    check("delete needs admin", client.delete("/api/governance/tags/region", cookies=bob).status_code == 403)
    r = client.delete("/api/governance/tags/region?force=true", cookies=admin)
    check("unused/forced delete succeeds", r.status_code == 200, r.text[:150])
    check("delete of an unknown tag -> 404", client.delete("/api/governance/tags/never_existed", cookies=admin).status_code == 404)
    check("unauthenticated requests still resolve (local mode) but bad sessions are 401",
          client.get("/api/governance/tags", cookies={auth.COOKIE_NAME: "garbage"}).status_code == 401)


def main():
    try:
        con = setup_fixture()
        test_store_and_definitions()
        test_assignments_and_inheritance()
        test_lifecycle(con)
        test_classifier()
        test_api(con)
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Governance Phase 1 checks passed.")


if __name__ == "__main__":
    main()
