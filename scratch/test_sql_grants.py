#!/usr/bin/env python3
"""SQL GRANT / REVOKE / SHOW GRANTS (web/sql_grants.py): the parser on many spellings and malformed input, and the effect on the real grants
(catalog ACLs, table / schema grants) with the real enforcement. Throwaway WAREHOUSE_DIR.
  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_sql_grants.py"""
import os, shutil, sys, tempfile
TMP = tempfile.mkdtemp(prefix="sqlgrants_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
sys.path.insert(0, "/workspace")
import pandas as pd
from deltalake import write_deltalake
from fastapi import HTTPException
from web import auth, groups, permissions, sql_grants as sg, table_access, warehouses
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def perr(sql):
    try: sg.parse(sql); return None
    except sg.GrantSqlError as e: return str(e)
def rerr(sql, user, cat="warehouse"):
    try: sg.run(sg.parse(sql), sql, user, cat); return None
    except sg.GrantSqlError as e: return str(e)

for t in ("orders", "customers"): write_deltalake(f"{TMP}/catalogs/sales/dbo/{t}", pd.DataFrame({"id": [1]}))
write_deltalake(f"{TMP}/catalogs/sales/pub/items", pd.DataFrame({"id": [1]}))
warehouses.create_catalog("Sales", "sales", "", None, False, "admin")
ADMIN = auth.get_user_by_username("admin")
alice = auth.create_user("alice", "alicepass123", "Alice", "user"); bob = auth.create_user("bob", "bobpass1234", "Bob", "user")
owner = auth.create_user("pat", "patpass12345", "Pat", "power_user"); warehouses.create_catalog("Owned", "owned", "", None, False, "pat")
G = groups.create_group("Analysts", "", "admin"); H = groups.create_group("PII readers", "", "admin"); groups.add_members(G["id"], [bob["id"]], "admin")
U = lambda u: {"id": u["id"], "username": u["username"], "role": u["role"]}
def can_sql(u, q):
    try: permissions.enforce_sql_permissions(q, U(u), "READ"); return True
    except HTTPException: return False

print("parsing")
P = sg.parse
check("not a grant statement", P("select * from t") is None and P("show tables") is None and P("grant_table_x") is None)
s = P("grant select on table sales.dbo.orders to group analysts;")
check("basic GRANT", s["op"] == "grant" and s["privileges"] == ["SELECT"] and s["target"] == {"kind": "table", "parts": ["sales", "dbo", "orders"]} and s["principals"] == [{"kind": "group", "name": "analysts"}], s)
s = P("GRANT SELECT, MODIFY ON SCHEMA sales.dbo TO USER alice, USER bob")
check("several privileges and principals", s["privileges"] == ["SELECT", "MODIFY"] and [p["name"] for p in s["principals"]] == ["alice", "bob"], s)
check("multi-word privileges", P("grant all privileges on catalog sales to alice")["privileges"] == ["ALL PRIVILEGES"] and P("grant use catalog on catalog sales to alice")["privileges"] == ["USE CATALOG"])
check("FUTURE / ALL TABLES IN SCHEMA is a schema target", P("grant select on future tables in schema sales.dbo to group analysts")["target"] == {"kind": "schema", "parts": ["sales", "dbo"], "future": "FUTURE"})
check("a bare three-part name is a table", P("grant select on sales.dbo.orders to alice")["target"]["kind"] == "table")
check("quoted names (with spaces, dots and escaped quotes)", P('grant select on table "sales"."dbo"."my.table" to group "PII readers"')["target"]["parts"] == ["sales", "dbo", "my.table"] and P('grant select on catalog `sales` to group "PII readers"')["principals"][0]["name"] == "PII readers")
check("REVOKE ... FROM", P("revoke modify on table sales.dbo.orders from group analysts")["op"] == "revoke")
check("SHOW GRANTS forms", P("show grants")["target"] is None and P("SHOW GRANTS ON TABLE sales.dbo.orders")["target"]["kind"] == "table" and P("show grants to user alice")["principal"] == {"kind": "user", "name": "alice"} and P("show grants alice")["principal"]["name"] == "alice")
check("keywords are case-insensitive and whitespace is free", P("  GrAnT\n SELECT\tON  TABLE sales.dbo.orders  TO  alice ;  ")["op"] == "grant")
for label, q, frag in (("WITH GRANT OPTION", "grant select on table sales.dbo.orders to alice with grant option", "GRANT OPTION"),
                       ("REVOKE GRANT OPTION FOR", "revoke grant option for select on table sales.dbo.orders from alice", "GRANT OPTION"),
                       ("column-level", "grant select (email) on table sales.dbo.orders to alice", "Column-level"),
                       ("two statements", "grant select on table a.b.c to alice; grant select on table a.b.d to bob", "one"),
                       ("missing ON", "grant select to alice", "ON"),
                       ("missing TO", "grant select on table sales.dbo.orders alice", "TO"),
                       ("REVOKE with TO", "revoke select on table sales.dbo.orders to alice", "FROM"),
                       ("unknown object needs a keyword", "grant select on sales.dbo to alice", "Say what the object is"),
                       ("ALL TABLES IN CATALOG", "grant select on all tables in catalog sales to alice", "not supported"),
                       ("garbage", "grant select on table sales.dbo.orders to alice %%%", "Unexpected"),
                       ("trailing text", "show grants on table a.b.c extra words", "Unexpected")):
    e = perr(q); check(f"refused with a reason: {label}", e is not None and frag.lower() in e.lower(), e)

print("catalog grants")
check("an ordinary user cannot grant", "Only an administrator" in (rerr("grant select on catalog sales to alice", U(alice)) or ""))
r = sg.run(P("grant select on catalog sales to user alice"), "x", U(ADMIN))
check("GRANT SELECT ON CATALOG gives read", "Granted SELECT on catalog sales" in r["message"] and permissions.can_user_access_catalog(U(alice), "sales", "READ") and not permissions.can_user_access_catalog(U(alice), "sales", "WRITE"), r)
sg.run(P("grant modify on catalog sales to alice"), "x", U(ADMIN)); check("MODIFY raises it to write", permissions.can_user_access_catalog(U(alice), "sales", "WRITE"))
r = sg.run(P("grant select on catalog sales to alice"), "x", U(ADMIN)); check("granting a lower privilege never lowers", "already holds" in r["message"] and permissions.can_user_access_catalog(U(alice), "sales", "WRITE"), r)
sg.run(P("revoke modify on catalog sales from alice"), "x", U(ADMIN)); check("REVOKE MODIFY leaves read", permissions.can_user_access_catalog(U(alice), "sales", "READ") and not permissions.can_user_access_catalog(U(alice), "sales", "WRITE"))
sg.run(P("revoke select on catalog sales from alice"), "x", U(ADMIN)); check("REVOKE SELECT removes access", not permissions.can_user_access_catalog(U(alice), "sales", "READ"))
sg.run(P("grant all privileges on catalog sales to group analysts"), "x", U(ADMIN)); check("ALL PRIVILEGES on a catalog is ADMIN, for a group", permissions.can_user_access_catalog(U(bob), "sales", "ADMIN"))
sg.run(P("revoke all privileges on catalog sales from group analysts"), "x", U(ADMIN)); check("REVOKE ALL removes the whole grant", not permissions.can_user_access_catalog(U(bob), "sales", "READ"))
check("the catalog owner (a power user) may grant on their own catalog", rerr("grant select on catalog owned to alice", U(owner)) is None and permissions.can_user_access_catalog(U(alice), "owned", "READ"))
check("...but not on someone else's", "Only an administrator" in (rerr("grant select on catalog sales to alice", U(owner)) or ""))
check("an unknown catalog is refused", "does not exist" in (rerr("grant select on catalog nope to alice", U(ADMIN)) or ""))

print("table and schema grants (with real enforcement)")
check("before: bob (in Analysts) cannot query the table", not can_sql(bob, "select * from sales.dbo.orders"))
r = sg.run(P("grant select on table sales.dbo.orders to group analysts"), "x", U(ADMIN)); check("GRANT SELECT ON TABLE to a group", can_sql(bob, "select * from sales.dbo.orders") and not can_sql(bob, "select * from sales.dbo.customers"), r)
check("...the same grant is visible to the UI", any(g["name"] == "Analysts" for g in groups.list_grants("table", "sales.dbo.orders")))
sg.run(P("grant modify on table sales.dbo.orders to group analysts"), "x", U(ADMIN)); check("MODIFY raises it", table_access.can_access_table(U(bob), "sales", "dbo", "orders", "WRITE"))
sg.run(P("revoke modify on table sales.dbo.orders from group analysts"), "x", U(ADMIN)); check("REVOKE MODIFY leaves SELECT", can_sql(bob, "select * from sales.dbo.orders") and not table_access.can_access_table(U(bob), "sales", "dbo", "orders", "WRITE"))
sg.run(P("revoke all on table sales.dbo.orders from group analysts"), "x", U(ADMIN)); check("REVOKE ALL removes it", not can_sql(bob, "select * from sales.dbo.orders"))
sg.run(P("grant select on future tables in schema sales.pub to user alice"), "x", U(ADMIN))
check("a schema grant covers its tables", can_sql(alice, "select * from sales.pub.items") and not can_sql(alice, "select * from sales.dbo.orders"))
write_deltalake(f"{TMP}/catalogs/sales/pub/later", pd.DataFrame({"id": [1]})); check("...including tables created later", can_sql(alice, "select * from sales.pub.later"))
check("a two-part table name uses the current catalog", "Granted" in sg.run(P("grant select on table dbo.customers to bob"), "x", U(ADMIN), "sales")["message"] and can_sql(bob, "select * from sales.dbo.customers"))
check("a missing table is refused (typo protection)", "does not exist" in (rerr("grant select on table sales.dbo.ordres to bob", U(ADMIN)) or ""))
check("a missing schema is refused", "does not exist" in (rerr("grant select on schema sales.nope to bob", U(ADMIN)) or ""))
check("an ordinary user cannot grant on a table", "Only an administrator" in (rerr("grant select on table sales.dbo.orders to alice", U(bob)) or ""))
check("an unknown principal is refused, and nothing is half-applied", "No user or group named 'zed'" in (rerr("grant select on table sales.dbo.orders to alice, zed", U(ADMIN)) or "") and not can_sql(alice, "select * from sales.dbo.orders"))
check("a role is not a principal", "Roles" in (rerr("grant select on table sales.dbo.orders to role admin", U(ADMIN)) or ""))
groups.create_group("alice", "", "admin")
check("a name that is both a user and a group must be qualified", "both a user and a group" in (rerr("grant select on table sales.dbo.orders to alice", U(ADMIN)) or "") and rerr("grant select on table sales.dbo.orders to user alice", U(ADMIN)) is None)
check("revoking what is not held says so", "does not hold" in sg.run(P("revoke select on table sales.dbo.orders from user bob"), "x", U(ADMIN))["message"])

print("SHOW GRANTS")
def rows(sql, user): return sg.run(P(sql), sql, user)["rows"]
r = rows("show grants on table sales.dbo.orders", U(ADMIN))
check("on a table: its grants and the inherited catalog grants", any(x[0] == "alice" and x[2] == "table" and x[4] == "SELECT" for x in r), r)
check("columns are principal, type, object type, object, privilege, granted by, at", sg.run(P("show grants"), "x", U(ADMIN))["columns"][0]["name"] == "principal" and len(sg.COLUMNS) == 7)
sg.run(P("grant select on table sales.dbo.customers to group analysts"), "x", U(ADMIN))
r = rows("show grants to group analysts", U(ADMIN)); check("for a group", [(x[0], x[1], x[2], x[3], x[4]) for x in r] == [("Analysts", "group", "table", "sales.dbo.customers", "SELECT")], r)
r = rows("show grants to user alice", U(ADMIN)); check("for a user: every object (catalog, schema and table)", {x[2] for x in r} == {"catalog", "schema", "table"} and all(x[0] == "alice" for x in r), r)
check("with no arguments: your own grants", all(x[0] == "bob" for x in rows("show grants", U(bob))) and rows("show grants", U(bob)) != [])
check("listing another principal needs an administrator", "administrator" in (rerr("show grants to user alice", U(bob)) or ""))
check("listing an object's grants needs manage rights on its catalog", "Only an administrator" in (rerr("show grants on table sales.dbo.orders", U(bob)) or ""))
check("the owner may list their own catalog", rerr("show grants on catalog owned", U(owner)) is None)
r = rows("show grants on schema sales.pub", U(ADMIN)); check("on a schema", any(x[2] == "schema" and x[0] == "alice" for x in r), r)

print("audit")
import sqlite3
acts = [r[0] for r in sqlite3.connect(os.path.join(TMP, ".metadata", "governance.db")).execute("select action from governance_audit")]
check("every change is audited", "SQL_GRANT" in acts and "SQL_REVOKE" in acts and "GRANT_SET" in acts, set(acts))
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
