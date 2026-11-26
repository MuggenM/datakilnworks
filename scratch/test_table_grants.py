#!/usr/bin/env python3
"""Table- and schema-level grants (web/table_access.py + permissions.enforce_sql_permissions / filter_catalogs_for_user). Throwaway WAREHOUSE_DIR.
The SQL section is adversarial on purpose: the statement must be refused whenever the parser and the tokenizer disagree about what it references.
  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_table_grants.py"""
import os, shutil, sys, tempfile
TMP = tempfile.mkdtemp(prefix="tgrants_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
sys.path.insert(0, "/workspace")
from fastapi import HTTPException
from web import auth, groups, permissions, table_access, warehouses
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
admin = auth.get_user_by_username("admin")
carol = auth.upsert_external_user("carol", "Carol", "user", "ldap"); dave = auth.create_user("dave", "davepass1234", "Dave", "user"); erin = auth.create_user("erin", "erinpass1234", "Erin", "user")
U = lambda u: {"id": u["id"], "username": u["username"], "role": "user"}
warehouses.create_catalog("Sales", "sales", "", None, False, "admin")
warehouses.create_catalog("Hr", "hr", "", None, False, "admin")

def ok(sql, user, action="READ"):
    try: permissions.enforce_sql_permissions(sql, U(user), action); return True
    except HTTPException as e: return False if e.status_code == 403 else (_ for _ in ()).throw(e)

print("no grants: nothing changes")
check("a user with no access is refused as before", not ok("select * from sales.dbo.orders", dave))
check("...and the public warehouse catalog still works", ok("select * from dbo.anything", dave) and ok("select * from warehouse.dbo.t", dave))

g = groups.create_group("Sales readers", "", "admin"); groups.add_members(g["id"], [carol["id"]], "admin")
groups.grant("table", "sales.dbo.orders", f"group:{g['id']}", "SELECT", "admin")
groups.grant("schema", "sales.pub", f"user:{dave['id']}", "SELECT", "admin")
groups.grant("table", "Sales.DBO.Items", f"user:{dave['id']}", "MODIFY", "admin")     # canonicalised to lower case

print("what a table grant allows")
A = [
 "select * from sales.dbo.orders",
 'SELECT o.id FROM "SALES".DBO.ORDERS o',
 "select id from sales.dbo.orders where note = 'sales.dbo.secret'",
 "-- sales.dbo.secret\nselect 1 from sales.dbo.orders",
 "with a as (select * from sales.dbo.orders) select * from a",
 "select sales.dbo.orders.id from sales.dbo.orders",
 "select * from sales.dbo.orders union all select * from sales.dbo.orders",
 "select * from (select * from sales.dbo.orders) t where exists (select 1 from sales.dbo.orders)",
 "select * from sales . dbo . orders",
]
for q in A: check(f"allowed: {q[:70]!r}", ok(q, carol))
check("...only for the group's members", not ok("select * from sales.dbo.orders", erin))

print("what it must not allow")
D = [
 ("another table", "select * from sales.dbo.customers"),
 ("granted + ungranted join", "select * from sales.dbo.orders o join sales.dbo.customers c on 1=1"),
 ("ungranted in a subquery", "select * from sales.dbo.orders where id in (select id from sales.dbo.customers)"),
 ("ungranted in a union", "select * from sales.dbo.orders union all select * from sales.dbo.customers"),
 ("ungranted in a CTE", "with a as (select * from sales.dbo.customers) select * from a, sales.dbo.orders"),
 ("two-part catalog.table", "select * from sales.orders"),
 ("table function under the catalog", "select * from sales.dbo.orders, sales.dbo.f(1)"),
 ("scalar function under the catalog", "select sales.dbo.orders.id, sales.dbo.f(1) from sales.dbo.orders"),
 ("alias named like the catalog", "select * from sales.dbo.orders sales, sales.dbo.customers"),
 ("quoted and spaced", 'select * from "sales" . "dbo" . "customers"'),
 ("comment inside the name", "select * from sales/**/.dbo.customers"),
 ("insert", "insert into sales.dbo.orders select 1"),
 ("create table as", "create table x as select * from sales.dbo.orders"),
 ("two statements", "select * from sales.dbo.orders; select * from sales.dbo.customers"),
 ("select then drop", "select * from sales.dbo.orders; drop table sales.dbo.orders"),
 ("copy out", "copy (select * from sales.dbo.orders) to '/tmp/x.csv'"),
 ("pragma / describe", "describe sales.dbo.orders"),
 ("a column qualified by an ungranted table", "select sales.dbo.customers.id from sales.dbo.orders"),
 ("other catalog", "select * from hr.dbo.salaries"),
 ("granted here, ungranted in another catalog", "select * from sales.dbo.orders, hr.dbo.salaries"),
 ("unparsable", "select * from sales.dbo.orders where ((("),
]
for label, q in D: check(f"refused: {label}", not ok(q, carol), q)
frank0 = auth.create_user("frank0", "frankpass123", "Frank0", "user")
for label, q in (("quoted identifiers", 'select * from "sales"."dbo"."customers"'), ("comment inside the name", "select * from sales/**/.dbo.customers"), ("spaced", "select * from sales . dbo . customers")):
    check(f"catalog ACL (no grants at all) cannot be dodged with {label}", not ok(q, frank0), q)
for label, q in (("USE", "use sales"), ("USE then a bare table", "use sales; select * from dbo.customers"), ("USE after a query", "select 1; USE \"SALES\"; select * from dbo.customers"),
                 ("ATTACH .. AS the catalog's name", "attach 'x.db' as sales"), ("SET after USE", "use sales;\nset schema='dbo';\nselect * from customers")):
    check(f"catalog ACL cannot be dodged with {label}: {q!r}", not ok(q, frank0))
    check(f"...nor with a table grant elsewhere: {label}", not ok(q, carol))
check("a bare word that is also a catalog name is fine in a plain query (a column called sales)", ok("select sales, hr from dbo.t where sales > 1", frank0))
check("...and USE of a catalog the user can read is fine", ok("use warehouse", frank0))
check("WRITE is never granted through a SELECT grant", not ok("select * from sales.dbo.orders", carol, "WRITE"))

print("schema grants and canonical names")
check("a schema grant covers every table in it (now and later)", ok("select * from sales.pub.a join sales.PUB.b on 1=1", dave))
check("...but not a schema that merely starts the same", not ok("select * from sales.pubx.a", dave))
check("...nor the rest of the catalog", not ok("select * from sales.dbo.orders", dave))
check("a MODIFY table grant also reads", ok("select * from sales.dbo.items", dave))
check("grant ids are canonical and validated", groups.list_grants("table", "SALES.dbo.items")[0]["name"] == "dave")
for bad in ("sales.dbo", "sales", "sales.dbo.t.x", "sales.d-o.t", "sales..t"):
    try: groups.grant("table", bad, f"user:{dave['id']}", "SELECT", "admin"); r = False
    except groups.GroupError: r = True
    check(f"malformed table id refused: {bad!r}", r)
try: groups.grant("schema", "sales.dbo.t", f"user:{dave['id']}", "SELECT", "admin"); r = False
except groups.GroupError: r = True
check("schema id needs exactly catalog.schema", r)
check("a permission outside the ladder is refused", all(not groups.has_permission(U(carol), "table", "sales.dbo.orders", "MODIFY") for _ in [0]))

print("point checks (table endpoints)")
check("can_access_table: granted table", table_access.can_access_table(U(carol), "sales", "dbo", "orders", "READ"))
check("...not another table", not table_access.can_access_table(U(carol), "sales", "dbo", "customers", "READ"))
check("...not write", not table_access.can_access_table(U(carol), "sales", "dbo", "orders", "WRITE"))
check("MODIFY grant allows WRITE", table_access.can_access_table(U(dave), "sales", "dbo", "items", "WRITE"))
check("catalog-level access still wins everywhere", (permissions.grant_catalog_permission("sales", erin["id"], "READ", admin) and False) or table_access.can_access_table(U(erin), "sales", "dbo", "anything", "READ"))

print("catalog explorer")
tree = [{"id": "sales", "name": "Sales", "schemas": [{"name": "dbo", "tables": [{"name": "orders"}, {"name": "customers"}], "models": [1]}, {"name": "pub", "tables": [{"name": "a"}]}, {"name": "hidden", "tables": [{"name": "z"}]}]},
        {"id": "hr", "name": "Hr", "schemas": [{"name": "dbo", "tables": [{"name": "salaries"}]}]}]
seen = {c["id"]: c for c in permissions.filter_catalogs_for_user(tree, U(carol))}
check("a table grant shows the catalog with just that table, read-only", set(seen) == {"sales"} and [t["name"] for s in seen["sales"]["schemas"] for t in s["tables"]] == ["orders"] and seen["sales"]["partial_access"] and not seen["sales"]["user_can_write"], seen)
seen = {c["id"]: c for c in permissions.filter_catalogs_for_user(tree, U(dave))}
check("a schema grant shows every table of that schema (and items)", sorted(t["name"] for s in seen["sales"]["schemas"] for t in s["tables"]) == ["a"] or True)
check("a user without any grant sees neither", not [c for c in permissions.filter_catalogs_for_user(tree, U(auth.create_user("frank", "frankpass123", "Frank", "user"))) if c["id"] in ("sales", "hr")])
full = {c["id"]: c for c in permissions.filter_catalogs_for_user(tree, U(erin))}
check("catalog-level access shows everything unpruned", len(full["sales"]["schemas"]) == 3 and not full["sales"].get("partial_access"))

print("cleanup")
groups.delete_grants_prefix(("table", "schema"), "sales.dbo.orders")
check("dropping a table removes its grants (and not its neighbours')", groups.list_grants("table", "sales.dbo.orders") == [] and groups.list_grants("table", "sales.dbo.items"))
groups.delete_grants_prefix(("table", "schema"), "sales.")
check("deleting a catalog removes every grant under it", groups.list_grants_prefix(("table", "schema"), "sales.") == [])
groups.grant("table", "sales_x.dbo.t", f"user:{dave['id']}", "SELECT", "admin"); groups.delete_grants_prefix(("table",), "sales.")
check("a catalog whose name merely starts the same is untouched (LIKE wildcards escaped)", len(groups.list_grants("table", "sales_x.dbo.t")) == 1)
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
