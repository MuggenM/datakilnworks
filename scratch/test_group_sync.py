#!/usr/bin/env python3
"""Directory group sync semantics (web/groups.py: set_mapping / sync_external_memberships), no directory needed. Throwaway WAREHOUSE_DIR.
  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_group_sync.py"""
import os, shutil, sys, tempfile
TMP = tempfile.mkdtemp(prefix="gsync_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
sys.path.insert(0, "/workspace")
from web import auth, groups
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def raises(fn, *a):
    try: fn(*a); return False
    except (groups.GroupError, LookupError): return True
A = auth.upsert_external_user("ann", "Ann", "user", "ldap"); B = auth.upsert_external_user("ben", "Ben", "user", "ldap"); L = auth.create_user("loc", "localpass123", "Local", "user")
DN = "cn=analysts,ou=groups,dc=example,dc=com"
g = groups.create_group("Analysts", "", "admin"); h = groups.create_group("Other", "", "admin")
members = lambda gr: {m["username"]: m["origin"] for m in groups.list_members(gr["id"])}

print("mapping")
m = groups.set_mapping(g["id"], "ldap", "  CN=Analysts, OU=Groups , DC=example,DC=com ", "admin")
check("mapping stored, the entered label is shown", m["source"] == "ldap" and m["external_ref"].startswith("  CN") or m["external_ref"].startswith("CN=Analysts"), m)
check("the same directory group cannot map to two groups (DN written differently)", raises(groups.set_mapping, h["id"], "ldap", DN, "admin"))
check("a reference is required", raises(groups.set_mapping, h["id"], "ldap", "  ", "admin"))
check("unknown source refused", raises(groups.set_mapping, h["id"], "kerberos", "x", "admin"))
check("unknown group", raises(groups.set_mapping, "grp_nope", "ldap", DN, "admin"))
groups.set_mapping(h["id"], "oidc", "Analysts", "admin"); check("the same text under another source is fine", groups.get_group(h["id"])["source"] == "oidc")

print("sync")
r = groups.sync_external_memberships(A["id"], "ldap", [DN.upper(), "cn=other,ou=groups,dc=example,dc=com"])
check("a directory group whose DN matches (case and spacing ignored) adds the user", r == {"added": ["Analysts"], "removed": []} and members(g) == {"ann": "sync"}, (r, members(g)))
check("an OIDC-mapped group is not touched by LDAP sync", members(h) == {})
check("syncing again changes nothing", groups.sync_external_memberships(A["id"], "ldap", [DN]) == {"added": [], "removed": []})
groups.add_members(g["id"], [B["id"]], "admin")
groups.sync_external_memberships(B["id"], "ldap", [])
check("a manual member is not removed by a sync that says they are in no group", members(g) == {"ann": "sync", "ben": "manual"}, members(g))
groups.add_members(g["id"], [A["id"]], "admin")
check("adding a synced member by hand keeps it a sync membership", members(g)["ann"] == "sync")
check("a synced member cannot be removed by hand", raises(groups.remove_member, g["id"], "ann", "admin"))
groups.remove_member(g["id"], "ben", "admin"); check("a manual member can", "ben" not in members(g))
r = groups.sync_external_memberships(A["id"], "ldap", ["cn=elsewhere,dc=x"])
check("leaving the directory group removes the synced membership", r["removed"] == ["Analysts"] and members(g) == {}, (r, members(g)))
groups.sync_external_memberships(A["id"], "ldap", [DN])
r = groups.set_mapping(g["id"], "ldap", "cn=other,ou=groups,dc=example,dc=com", "admin")
check("changing the mapping drops the members that came from the old one", members(g) == {}, members(g))
groups.sync_external_memberships(A["id"], "ldap", ["cn=other,ou=groups,dc=example,dc=com"]); groups.add_members(g["id"], [L["id"]], "admin")
groups.set_mapping(g["id"], "local", "", "admin")
check("clearing the mapping removes synced members and keeps manual ones", members(g) == {"loc": "manual"} and groups.get_group(g["id"])["source"] == "local", members(g))
groups.sync_external_memberships(A["id"], "ldap", [DN])
check("an unmapped group is not synced", members(g) == {"loc": "manual"})
check("sync only accepts a directory source", raises(groups.sync_external_memberships, A["id"], "local", []))

print("access follows the sync")
from web import permissions, warehouses
warehouses.create_catalog("Sales", "sales", "", None, False, "admin")
groups.set_mapping(g["id"], "ldap", DN, "admin"); permissions.grant_catalog_permission("sales", f"group:{g['id']}", "READ", auth.get_user_by_username("admin"))
U = lambda u: {"id": u["id"], "username": u["username"], "role": "user"}
check("no access before the directory says so", not permissions.can_user_access_catalog(U(A), "sales", "READ"))
groups.sync_external_memberships(A["id"], "ldap", [DN]); check("access appears when the directory group does", permissions.can_user_access_catalog(U(A), "sales", "READ"))
groups.sync_external_memberships(A["id"], "ldap", []); check("...and disappears when it goes", not permissions.can_user_access_catalog(U(A), "sales", "READ"))
import sqlite3
acts = [r[0] for r in sqlite3.connect(os.path.join(TMP, ".metadata", "governance.db")).execute("select action from governance_audit")]
check("mapping and sync changes are audited", "GROUP_MAPPING" in acts and "GROUP_SYNC" in acts, set(acts))
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
