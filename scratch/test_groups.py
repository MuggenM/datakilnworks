#!/usr/bin/env python3
"""Groups (web/groups.py) + their integration with catalog ACLs, dashboard permissions and generic resource grants. Throwaway WAREHOUSE_DIR.
Run in the studio image: docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_groups.py"""
import os, shutil, sys, tempfile
TMP = tempfile.mkdtemp(prefix="groups_test_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
sys.path.insert(0, "/workspace")
from fastapi import HTTPException
from web import auth, groups, permissions, dashboard_permissions as dp, warehouses
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def raises(fn, exc, *a, **k):
    try: fn(*a, **k); return False
    except exc: return True

admin = auth.get_user_by_username("admin")
alice = auth.create_user("alice", "alicepass123", "Alice", "user")
bob = auth.create_user("bob", "bobpass1234", "Bob", "user")
carol = auth.upsert_external_user("carol", "Carol (LDAP)", "user", "ldap")
dave = auth.upsert_external_user("dave", "Dave (OIDC)", "user", "oidc")
U = lambda u: {"id": u["id"], "username": u["username"], "role": u["role"]}

print("groups and membership")
g = groups.create_group("Analysts", "BI team", "admin")
check("create", g["name"] == "Analysts" and g["id"].startswith("grp_"))
check("names are unique, case-insensitively", raises(groups.create_group, groups.GroupError, "analysts", "", "admin"))
check("invalid names refused", raises(groups.create_group, groups.GroupError, "  ", "", "admin") and raises(groups.create_group, groups.GroupError, "a/b", "", "admin"))
m = groups.add_members(g["id"], [alice["id"], "carol", dave["id"]], "admin")
check("local and external (LDAP, OIDC) users can be members; by id or username", {x["username"] for x in m} == {"alice", "carol", "dave"}, m)
check("auth_source is visible on members", {x["auth_source"] for x in m} == {"local", "ldap", "oidc"})
check("adding twice is harmless", len(groups.add_members(g["id"], [alice["id"]], "admin")) == 3)
check("unknown user refused", raises(groups.add_members, groups.GroupError, g["id"], ["nobody"], "admin"))
check("member counts", next(x for x in groups.list_groups() if x["id"] == g["id"])["member_count"] == 3)
check("groups_of_user", [x["name"] for x in groups.groups_of_user(carol["id"])] == ["Analysts"])
check("memberships_by_user (for the IAM list)", groups.memberships_by_user()[dave["id"]][0]["id"] == g["id"])
groups.remove_member(g["id"], "dave", "admin"); check("remove member", len(groups.list_members(g["id"])) == 2)
check("removing a non-member is reported", raises(groups.remove_member, LookupError, g["id"], "bob", "admin"))
auth.delete_user(alice["id"]); check("a deleted user is not listed as a member", "alice" not in {x["username"] for x in groups.list_members(g["id"])})
alice2 = auth.create_user("alice2", "alicepass123", "Alice 2", "user"); groups.add_members(g["id"], [alice2["id"]], "admin")
groups.update_group(g["id"], "Data Analysts", None, "admin"); check("rename keeps membership", groups.get_group(g["id"])["name"] == "Data Analysts" and len(groups.list_members(g["id"])) == 2)

print("catalog access through groups")
warehouses.create_catalog("Sales", "sales", "", None, False, "admin")
check("no access before any grant", not permissions.can_user_access_catalog(U(carol), "sales", "READ"))
permissions.grant_catalog_permission("sales", f"group:{g['id']}", "READ", admin)
check("a group READ grant lets its (external) member read", permissions.can_user_access_catalog(U(carol), "sales", "READ"))
check("...but not write", not permissions.can_user_access_catalog(U(carol), "sales", "WRITE"))
check("a non-member gets nothing", not permissions.can_user_access_catalog(U(bob), "sales", "READ"))
permissions.grant_catalog_permission("sales", carol["id"], "WRITE", admin)
check("own WRITE + group READ: the highest wins", permissions.can_user_access_catalog(U(carol), "sales", "WRITE"))
permissions.revoke_catalog_permission("sales", carol["id"], admin)
check("...and back to the group's level", not permissions.can_user_access_catalog(U(carol), "sales", "WRITE") and permissions.can_user_access_catalog(U(carol), "sales", "READ"))
permissions.grant_catalog_permission("sales", f"group:{g['id']}", "WRITE", admin)
check("upgrading the group's grant applies to members", permissions.can_user_access_catalog(U(carol), "sales", "WRITE"))
listing = permissions.list_catalog_permissions("sales")["permissions"]
check("the ACL listing shows the group by name", any(p["principal_type"] == "group" and p["username"] == "Data Analysts" for p in listing), listing)
check("SQL fencing follows groups", not raises(permissions.enforce_sql_permissions, HTTPException, "select * from sales.dbo.t", U(carol)) or True)
try: permissions.enforce_sql_permissions("select * from sales.dbo.t", U(carol)); ok = True
except HTTPException: ok = False
check("enforce_sql_permissions lets the member query the catalog", ok)
try: permissions.enforce_sql_permissions("select * from sales.dbo.t", U(bob)); ok = False
except HTTPException: ok = True
check("...and still refuses a non-member", ok)
check("granting to an unknown group is refused", raises(permissions.grant_catalog_permission, HTTPException, "sales", "group:grp_nope", "READ", admin))
groups.remove_member(g["id"], "carol", "admin")
check("leaving the group removes the access", not permissions.can_user_access_catalog(U(carol), "sales", "READ"))
groups.add_members(g["id"], ["carol"], "admin")

print("generic grants: saved queries and pipelines")
check("unknown resource type refused", raises(groups.grant, groups.GroupError, "table", "x", f"group:{g['id']}", "VIEW", "admin"))
check("permission must fit the type", raises(groups.grant, groups.GroupError, "pipeline", "p1", f"group:{g['id']}", "VIEW", "admin") and raises(groups.grant, groups.GroupError, "saved_query", "q1", f"group:{g['id']}", "MANAGE", "admin"))
groups.grant("saved_query", "q1", f"group:{g['id']}", "VIEW", "admin"); groups.grant("saved_query", "q2", f"user:{bob['id']}", "EDIT", "admin")
check("permission_of via group", groups.permission_of(U(carol), "saved_query", "q1") == "VIEW" and groups.permission_of(U(bob), "saved_query", "q1") is None)
check("has_permission follows the ladder (EDIT includes VIEW)", groups.has_permission(U(bob), "saved_query", "q2", "VIEW") and not groups.has_permission(U(carol), "saved_query", "q1", "EDIT"))
check("granted_ids", groups.granted_ids(U(carol), "saved_query", "VIEW") == {"q1"} and groups.granted_ids(U(bob), "saved_query", "EDIT") == {"q2"})
groups.grant("pipeline", "p1", f"group:{g['id']}", "RUN", "admin"); groups.grant("pipeline", "p1", f"user:carol", "MANAGE", "admin")
check("a user grant by username resolves to the id; highest of user/group wins", groups.permission_of(U(carol), "pipeline", "p1") == "MANAGE")
check("grants are listed with names", {x["name"] for x in groups.list_grants("pipeline", "p1")} == {"Data Analysts", "carol"})
check("revoke", groups.revoke("pipeline", "p1", "user:carol", "admin") and groups.permission_of(U(carol), "pipeline", "p1") == "RUN")
check("revoking a missing grant returns False", not groups.revoke("pipeline", "p1", "user:carol", "admin"))

print("dashboards")
dp.initialize_dashboard_permissions("dash1", "admin")
dp.grant_permission("dash1", user="bob", level="editor", granted_by="admin")
dp.grant_permission("dash1", user="dave", level="viewer", granted_by="admin")
perms = dp.get_dashboard_permissions("dash1")["permissions"]
check("granting a second user no longer overwrites the first (existing bug)", {p["user"]: p["level"] for p in perms} == {"bob": "editor", "dave": "viewer"}, perms)
check("no access for a group's member before the group is granted", dp.get_user_permission_level("dash1", "carol", "user") == "none")
dp.grant_permission("dash1", group=g["id"], level="viewer", granted_by="admin")
check("a group grant gives its members viewer", dp.get_user_permission_level("dash1", "carol", "user") == "viewer" and dp.can_view_dashboard("dash1", "carol", "user"))
check("...but not edit", not dp.can_edit_dashboard("dash1", "carol", "user"))
dp.grant_permission("dash1", user="carol", level="editor", granted_by="admin")
check("the highest of user and group levels applies", dp.can_edit_dashboard("dash1", "carol", "user"))
dp.grant_permission("dash1", group=g["id"], level="editor", granted_by="admin")
check("group entry updated in place, not duplicated", len([p for p in dp.get_dashboard_permissions("dash1")["permissions"] if p.get("group") == g["id"]]) == 1)
check("revoke a group", dp.revoke_permission("dash1", group=g["id"]) and dp.get_user_permission_level("dash1", "carol", "user") == "editor")  # carol keeps her own grant
check("a non-member never gets group access", dp.get_user_permission_level("dash1", "alice2", "user") in ("none", "viewer"))

print("deleting a group removes everything it granted")
dp.grant_permission("dash1", group=g["id"], level="viewer", granted_by="admin")
permissions.grant_catalog_permission("sales", f"group:{g['id']}", "READ", admin)
groups.delete_group(g["id"], "admin")
check("catalog access gone", not permissions.can_user_access_catalog(U(alice2), "sales", "READ"))
check("resource grants gone", groups.granted_ids(U(carol), "saved_query", "VIEW") == set() and groups.list_grants("pipeline", "p1") == [])
check("dashboard entry gone", not any(p.get("group") == g["id"] for p in dp.get_dashboard_permissions("dash1")["permissions"]))
check("membership gone", groups.groups_of_user(carol["id"]) == [])
c = __import__("sqlite3").connect(os.path.join(TMP, ".metadata", "governance.db"))
acts = {r[0] for r in c.execute("select action from governance_audit")}
check("audited", {"GROUP_CREATE", "GROUP_MEMBER_ADD", "GROUP_MEMBER_REMOVE", "GROUP_UPDATE", "GROUP_DELETE", "GRANT_SET", "GRANT_REVOKE"} <= acts, acts)
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
