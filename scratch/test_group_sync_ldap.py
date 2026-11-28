#!/usr/bin/env python3
"""LDAP group sync against a REAL directory (the lldap of the CI services stack, never the dev one): platform groups mapped to LDAP group
DNs follow sign-in, per-user sync, bulk discovery and full sync; manual members survive; a failed group search never strips anyone.
Fixtures seeded by the stack (profile `ldap`): users u1 (in dkw_analysts), u2, u3 with password 'Password123!', groups dkw_analysts / dkw_ops.
    CI_SERVICES=ldap ci/run_test.sh group_sync_ldap
Env: LLDAP_TEST_HOST (required, no default), and (defaults = the stack's dummy values) LLDAP_BIND_DN, LLDAP_BIND_PASSWORD, LLDAP_BASE_DN, LLDAP_USER_PASSWORD.
Without the directory it skips (exit 0), except with REQUIRE_SERVICES=1 (CI), where it fails."""
import json, os, shutil, sys, tempfile, urllib.request
TMP = tempfile.mkdtemp(prefix="gsync_ldap_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
sys.path.insert(0, "/workspace"); sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _services
HOST = os.getenv("LLDAP_TEST_HOST", "")          # no default: see _services.require_or_skip
if not _services.require_or_skip("lldap", HOST, 3890):
    shutil.rmtree(TMP, ignore_errors=True); sys.exit(0)
from web import auth, auth_frameworks, groups, ldap_auth
BASE = os.getenv("LLDAP_BASE_DN", "dc=example,dc=com"); BIND_PW = os.getenv("LLDAP_BIND_PASSWORD", "adminpassword")
USER_PW = os.getenv("LLDAP_USER_PASSWORD", "Password123!"); GROUPS_OU = f"ou=groups,{BASE}"; PEOPLE_OU = f"ou=people,{BASE}"
CFG = {"enabled": True, "server_host": HOST, "server_port": 3890, "encryption": "none", "base_dn": BASE, "bind_dn": os.getenv("LLDAP_BIND_DN", f"uid=admin,{PEOPLE_OU}"),
       "bind_password": BIND_PW, "user_search_base": PEOPLE_OU, "user_search_filter": "(uid={username})", "group_search_base": GROUPS_OU, "default_role": "user"}
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def req(url, body=None, token=None):
    r = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, headers={"Content-Type": "application/json"})
    if token: r.add_header("Authorization", f"Bearer {token}")
    return json.loads(urllib.request.urlopen(r).read())
TOKEN = req(f"http://{HOST}:17170/auth/simple/login", {"username": CFG["bind_dn"].split(",")[0].split("=", 1)[1], "password": BIND_PW})["token"]
def gql(q, v=None): return req(f"http://{HOST}:17170/api/graphql", {"query": q, "variables": v or {}}, TOKEN)["data"]
gid = lambda name: next(g["id"] for g in gql("{groups{id displayName}}")["groups"] if g["displayName"] == name)
def dir_add(user, group): gql("mutation($u:String!,$g:Int!){addUserToGroup(userId:$u,groupId:$g){ok}}", {"u": user, "g": gid(group)})
def dir_remove(user, group): gql("mutation($u:String!,$g:Int!){removeUserFromGroup(userId:$u,groupId:$g){ok}}", {"u": user, "g": gid(group)})
AN, OPS = f"cn=dkw_analysts,{GROUPS_OU}", f"cn=dkw_ops,{GROUPS_OU}"
auth_frameworks.save_config({"ldap": CFG})
members = lambda g: {m["username"]: m["origin"] for m in groups.list_members(g["id"])}

print("browsing the directory")
found = {g["name"]: g["dn"] for g in ldap_auth.list_directory_groups(CFG)}
check("directory groups are listed with their DNs", found.get("dkw_analysts") == AN and found.get("dkw_ops") == OPS, found)
check("a name filter narrows the list", [g["name"] for g in ldap_auth.list_directory_groups(CFG, "analysts")] == ["dkw_analysts"])
check("LDAP filter characters in the query are escaped, not interpreted", ldap_auth.list_directory_groups(CFG, "*)(uid=*") == [])

analysts = groups.create_group("Analysts", "", "admin"); ops = groups.create_group("Ops", "", "admin")
groups.set_mapping(analysts["id"], "ldap", AN, "admin"); groups.set_mapping(ops["id"], "ldap", OPS.upper(), "admin")

print("sign-in")
u, why = ldap_auth.authenticate("u1", USER_PW, CFG)
check("u1 (in dkw_analysts) signs in", u is not None, why)
check("...and is a synced member of Analysts only", members(analysts) == {"u1": "sync"} and members(ops) == {}, (members(analysts), members(ops)))
check("a wrong password changes nothing", ldap_auth.authenticate("u1", "nope", CFG)[0] is None and members(analysts) == {"u1": "sync"})

print("per-user sync")
dir_add("u1", "dkw_ops")
r = ldap_auth.sync_user("u1", CFG)
check("a group added in the directory shows up after sync_user", members(ops) == {"u1": "sync"}, (r, members(ops)))
groups.add_members(analysts["id"], [auth.get_user_by_username("u1")["id"]], "admin")   # already synced: stays sync
dir_remove("u1", "dkw_analysts")
ldap_auth.sync_user("u1", CFG)
check("leaving a directory group removes the synced membership", "u1" not in members(analysts) and members(ops) == {"u1": "sync"}, (members(analysts), members(ops)))

print("manual members survive")
groups.add_members(analysts["id"], [auth.get_user_by_username("u1")["id"]], "admin")
ldap_auth.sync_all(CFG, discover=False)
check("a manual member of a mapped group stays through a full sync", members(analysts) == {"u1": "manual"}, members(analysts))

print("bulk discovery and full sync")
dir_add("u2", "dkw_ops"); dir_add("u3", "dkw_analysts")
res = ldap_auth.sync_all(CFG, discover=True)
check("discovered users get their groups straight away", {"u2", "u3"} <= set(res["discovered_users"]) and members(ops).get("u2") == "sync" and members(analysts).get("u3") == "sync", (res["discovered_users"], members(ops), members(analysts)))

print("a directory that cannot be asked never strips access")
bad = {**CFG, "group_search_base": f"ou=nonexistent,{BASE}"}
before = (members(analysts), members(ops))
ldap_auth.sync_user("u3", bad); ldap_auth.sync_all(bad, discover=False)
check("a group search that fails leaves every membership as it was", (members(analysts), members(ops)) == before, (before, (members(analysts), members(ops))))
noconf = {**CFG, "group_search_base": "", "base_dn": ""}
ldap_auth.sync_user("u3", noconf)
check("no group search base configured: nothing is removed either", (members(analysts), members(ops)) == before)
dir_remove("u3", "dkw_analysts"); ldap_auth.sync_user("u3", CFG)
check("...but once the directory answers, the removal applies", "u3" not in members(analysts))

print("unmapping")
groups.set_mapping(ops["id"], "local", "", "admin")
check("clearing the mapping removes the synced members", members(ops) == {}, members(ops))
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
