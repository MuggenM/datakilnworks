#!/usr/bin/env python3
"""
Real LDAP authentication verification: bind, search, bind-as-user credential check, group-to-role mapping,
provisioning and sync -- against a REAL LDAP server (lldap), not a mock.

Needs the lldap of the CI services stack (profile `ldap`, ci/services/docker-compose.yml), which seeds the service bind
account `uid=admin,ou=people,dc=example,dc=com` / `adminpassword` and the users jdoe (member of dkw_engineers), asmith
(dkw_admins), bjones (dkw_analysts), all with password 'Password123!':

    CI_SERVICES=ldap ci/run_test.sh ldap_auth

Env: LLDAP_TEST_HOST (required, no default), and (defaults = the stack's dummy values) LLDAP_BIND_DN, LLDAP_BIND_PASSWORD, LLDAP_BASE_DN,
LLDAP_USER_PASSWORD. Without the directory it skips (exit 0), except with REQUIRE_SERVICES=1 (CI), where it fails.

Runs against a throwaway WAREHOUSE_DIR, so it never touches real accounts. Creates and cleans up its own
LDAP fixtures for the dynamic (group-change / deletion) tests.
"""

import datetime
import os
import shutil
import socket
import sys
import tempfile

TMP_ROOT = tempfile.mkdtemp(prefix="ldap_auth_")
TMP_WAREHOUSE = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(TMP_WAREHOUSE)
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
# Bootstrap-only admin account (see web/auth.py::_init_admin_from_env); precomputed hash for "adminpassword123"
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
os.environ["INIT_ADMIN_DISPLAY_NAME"] = "System Administrator"
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_NOTEBOOK_EXECUTION", "JWT_SECRET_KEY", "COMPUTE_TOKEN", "GOVERNANCE_ENFORCEMENT"):
    os.environ.pop(var, None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _services  # noqa: E402

LLDAP_HOST = os.getenv("LLDAP_TEST_HOST", "")          # no default: see _services.require_or_skip
BASE_DN = os.getenv("LLDAP_BASE_DN", "dc=example,dc=com")
BIND_PASSWORD = os.getenv("LLDAP_BIND_PASSWORD", "adminpassword")
USER_PASSWORD = os.getenv("LLDAP_USER_PASSWORD", "Password123!")
if not _services.require_or_skip("lldap", LLDAP_HOST, 3890):
    shutil.rmtree(TMP_ROOT, ignore_errors=True)
    sys.exit(0)

from fastapi.testclient import TestClient
from ldap3 import ALL, MODIFY_ADD, MODIFY_DELETE, SUBTREE, Connection, Server

from web import app as app_module
from web import auth, auth_frameworks, ldap_auth

# Bootstrap sets must_change_password=1 for the admin account; clear it (as if admin already
# completed the forced first-login change) so the rest of this test behaves as before.
with auth.get_db_connection() as _c:
    _c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")

FAILURES = []
PEOPLE_OU = f"ou=people,{BASE_DN}"
GROUPS_OU = f"ou=groups,{BASE_DN}"
ADMIN_DN = os.getenv("LLDAP_BIND_DN", f"uid=admin,{PEOPLE_OU}")
LDAP_CFG = {
    "enabled": True, "server_host": LLDAP_HOST, "server_port": 3890, "encryption": "none",
    "base_dn": BASE_DN, "bind_dn": ADMIN_DN, "bind_password": BIND_PASSWORD,
    "user_search_base": PEOPLE_OU, "user_search_filter": "(uid={username})",
    "group_search_base": GROUPS_OU,
    "admin_group": f"cn=dkw_admins,{GROUPS_OU}", "power_user_group": f"cn=dkw_engineers,{GROUPS_OU}",
    "default_role": "user",
}


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:400]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def admin_conn():
    return Connection(Server(LLDAP_HOST, port=3890, get_info=ALL), user=ADMIN_DN, password=BIND_PASSWORD, auto_bind=True)


def gql(query, variables=None, token=None):
    import json
    import urllib.request
    req = urllib.request.Request(f"http://{LLDAP_HOST}:17170/api/graphql",
                                 data=json.dumps({"query": query, "variables": variables or {}}).encode(),
                                 headers={"Content-Type": "application/json"})
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    return json.loads(urllib.request.urlopen(req).read())


def lldap_token():
    import json
    import urllib.request
    req = urllib.request.Request(f"http://{LLDAP_HOST}:17170/auth/simple/login",
                                 data=json.dumps({"username": ADMIN_DN.split(",")[0].split("=", 1)[1], "password": BIND_PASSWORD}).encode(),
                                 headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req).read())["token"]


def group_id(token, name):
    for g in gql("{groups{id displayName}}", token=token)["data"]["groups"]:
        if g["displayName"] == name:
            return g["id"]
    return None


RUN_ID = os.urandom(3).hex()
TMP_USER = f"ldaptest_{RUN_ID}"


def make_temp_user(username):
    # No password: lldap sets passwords through an OPAQUE PAKE handshake (the `lldap_set_password` CLI baked into
    # its own image implements the client side; nothing reachable over plain HTTP/LDAP does), so this fixture
    # can't authenticate. Sections needing deletion/discovery only need a directory entry that genuinely exists.
    c = admin_conn()
    try:
        c.add(f"uid={username},{PEOPLE_OU}", attributes={
            "objectClass": ["inetOrgPerson", "posixAccount", "mailAccount", "person"],
            "mail": f"{username}@example.com", "user_id": username, "first_name": "Temp", "last_name": "User",
            "display_name": "Temp User"})
    finally:
        c.unbind()


def delete_temp_user(username):
    c = admin_conn()
    try:
        c.delete(f"uid={username},{PEOPLE_OU}")
    except Exception:
        pass
    finally:
        c.unbind()


def main():
    try:
        auth_frameworks.save_config({"ldap": LDAP_CFG})
        auth.init_auth_db()

        print("\n1. Diagnostics")
        r = ldap_auth.test_bind_and_search(LDAP_CFG)
        check("service bind + search succeeds against the real directory", r["success"], r)
        bad_cfg = dict(LDAP_CFG, bind_password="wrong")
        r = ldap_auth.test_bind_and_search(bad_cfg)
        check("a wrong bind password is reported, not raised", r["success"] is False, r)
        # A real thing to answer, but not LDAP: lldap's own GraphQL/HTTP port. ldap3's BER decoder can raise a raw
        # KeyError (not LDAPException) on the garbage response -- every entry point here must turn that into a
        # clean result, never an uncaught exception (which reaches a FastAPI endpoint as a non-JSON 500).
        wrong_proto_cfg = dict(LDAP_CFG, server_port=17170)
        r = ldap_auth.test_bind_and_search(wrong_proto_cfg)
        check("a port that answers but isn't LDAP is reported cleanly, not raised", r["success"] is False, r)
        u, reason = ldap_auth.authenticate("jdoe", USER_PASSWORD, wrong_proto_cfg)
        check("authenticate() against the same wrong port fails cleanly instead of raising", u is None and bool(reason), reason)

        print("\n2. Real bind-as-user authentication + group-to-role mapping")
        user, reason = ldap_auth.authenticate("jdoe", USER_PASSWORD, LDAP_CFG)
        check("jdoe authenticates against the real directory", user is not None, reason)
        check("jdoe's group (dkw_engineers) maps to power_user", user and user["role"] == "power_user", user)
        check("the local account is provisioned with auth_source=ldap", user and user["auth_source"] == "ldap", user)
        user2, _ = ldap_auth.authenticate("asmith", USER_PASSWORD, LDAP_CFG)
        check("asmith (dkw_admins) maps to admin", user2 and user2["role"] == "admin", user2)
        user3, _ = ldap_auth.authenticate("bjones", USER_PASSWORD, LDAP_CFG)
        check("bjones (dkw_analysts, unmapped) gets the configured default_role", user3 and user3["role"] == "user", user3)

        print("\n3. Real failures (not mocked)")
        u, reason = ldap_auth.authenticate("jdoe", "wrong-password-entirely", LDAP_CFG)
        check("a real wrong password is rejected", u is None, reason)
        u, reason = ldap_auth.authenticate("nobody_like_this", "whatever", LDAP_CFG)
        check("an unknown username is rejected", u is None, reason)
        u, reason = ldap_auth.authenticate("*)(uid=*", "whatever", LDAP_CFG)
        check("LDAP filter metacharacters in the username do not become a filter injection", u is None, reason)

        print("\n4. Local accounts never fall through to LDAP")
        local = auth.create_user("localonly", "localpassword123", "Local Only User", "user")
        u, reason = ldap_auth.authenticate("localonly", "anything", LDAP_CFG)
        check("a local username is refused before any directory lookup happens", u is None and "local account" in (reason or ""), reason)

        print("\n5. Full HTTP login flow")
        client = TestClient(app_module.app)
        r = client.post("/api/auth/login", json={"username": "jdoe", "password": USER_PASSWORD})
        check("POST /api/auth/login succeeds for a real LDAP user", r.status_code == 200, r.text)
        check("...with the mapped role in the response", r.status_code == 200 and r.json()["user"]["role"] == "power_user", r.text[:200])
        check("...and a session cookie", auth.COOKIE_NAME in r.cookies)
        r2 = client.post("/api/auth/login", json={"username": "jdoe", "password": "still-wrong"})
        check("a wrong password for an already-provisioned LDAP account is rejected, not checked locally", r2.status_code == 401)
        r3 = client.get("/api/auth/me", cookies={auth.COOKIE_NAME: r.cookies.get(auth.COOKIE_NAME)})
        check("the session from an LDAP login works like any other", r3.json().get("authenticated") is True and r3.json()["user"]["username"] == "jdoe")

        print("\n6. Sync")
        s = ldap_auth.sync_user("jdoe", LDAP_CFG)
        check("syncing an unchanged account reports 'unchanged'", s["status"] == "unchanged", s)
        token = lldap_token()
        eng_id, admin_id = group_id(token, "dkw_engineers"), group_id(token, "dkw_admins")
        gql("mutation($u:String!,$g:Int!){removeUserFromGroup(userId:$u,groupId:$g){ok}}", {"u": "jdoe", "g": eng_id}, token=token)
        gql("mutation($u:String!,$g:Int!){addUserToGroup(userId:$u,groupId:$g){ok}}", {"u": "jdoe", "g": admin_id}, token=token)
        try:
            s = ldap_auth.sync_user("jdoe", LDAP_CFG)
            check("moving jdoe to dkw_admins and syncing picks up the new role", s["status"] == "updated" and s["role"] == "admin", s)
            check("the local record reflects the new role", auth.get_user_by_username("jdoe")["role"] == "admin")
        finally:
            gql("mutation($u:String!,$g:Int!){removeUserFromGroup(userId:$u,groupId:$g){ok}}", {"u": "jdoe", "g": admin_id}, token=token)
            gql("mutation($u:String!,$g:Int!){addUserToGroup(userId:$u,groupId:$g){ok}}", {"u": "jdoe", "g": eng_id}, token=token)
            ldap_auth.sync_user("jdoe", LDAP_CFG)

        print("\n7. Deprovisioning: an account removed from the directory")
        make_temp_user(TMP_USER)
        try:
            record = auth.upsert_external_user(username=TMP_USER, display_name="Temp User", role="user", auth_source="ldap")
            check("the fixture is a real directory entry with a matching local (LDAP-provisioned) account",
                  record is not None and ldap_auth.find_user(admin_conn(), LDAP_CFG, TMP_USER) is not None)
            delete_temp_user(TMP_USER)
            check("the directory entry is genuinely gone", ldap_auth.find_user(admin_conn(), LDAP_CFG, TMP_USER) is None)
            s = ldap_auth.sync_user(TMP_USER, LDAP_CFG)
            check("syncing after real directory deletion deactivates the local account", s["status"] == "deactivated", s)
            check("...and it is actually inactive", auth.get_user_by_username(TMP_USER)["is_active"] == 0)
            r4 = client.post("/api/auth/login", json={"username": TMP_USER, "password": "anything"})
            check("logging in after deletion fails cleanly (not found), not a 500", r4.status_code == 401, r4.text)
        finally:
            delete_temp_user(TMP_USER)

        print("\n8. sync_all")
        result = ldap_auth.sync_all(LDAP_CFG, discover=False)
        check("sync_all checks every LDAP-provisioned account", result["checked"] >= 3, result)
        check("...and reports no errors against a healthy directory", not result["errors"], result["errors"])

        print("\n9. Discovery: provisioning a directory user who has never logged in")
        TMP_USER2 = f"ldaptest2_{RUN_ID}"
        make_temp_user(TMP_USER2)
        try:
            token = lldap_token()
            gql("mutation($u:String!,$g:Int!){addUserToGroup(userId:$u,groupId:$g){ok}}",
               {"u": TMP_USER2, "g": group_id(token, "dkw_admins")}, token=token)
            check("the fixture has no local account yet", auth.get_user_by_username(TMP_USER2) is None)
            d = ldap_auth.discover_and_provision(LDAP_CFG)
            check("discovery finds the never-logged-in directory user", TMP_USER2 in d["discovered"], d)
            provisioned = auth.get_user_by_username(TMP_USER2)
            check("...and provisions it locally with the right role from its group", provisioned is not None
                  and provisioned["role"] == "admin" and provisioned["auth_source"] == "ldap", provisioned)
            d2 = ldap_auth.discover_and_provision(LDAP_CFG)
            check("running discovery again does not re-discover an already-provisioned user", TMP_USER2 not in d2["discovered"]
                  and TMP_USER2 in d2["skipped"], d2)

            existing_local = auth.create_user(f"clash_{RUN_ID}", "clashpassword123", "Local Clash User", "user")
            c = admin_conn()
            try:
                c.add(f"uid=clash_{RUN_ID},{PEOPLE_OU}", attributes={
                    "objectClass": ["inetOrgPerson", "posixAccount", "mailAccount", "person"],
                    "mail": f"clash_{RUN_ID}@example.com", "user_id": f"clash_{RUN_ID}",
                    "first_name": "Clash", "last_name": "User", "display_name": "Clash User"})
            finally:
                c.unbind()
            d3 = ldap_auth.discover_and_provision(LDAP_CFG)
            check("a directory user matching an existing local username is skipped, not overwritten",
                  f"clash_{RUN_ID}" in d3["skipped"] and f"clash_{RUN_ID}" not in d3["discovered"], d3)
            check("...and the local account is untouched", auth.get_user_by_username(f"clash_{RUN_ID}")["auth_source"] == "local")
        finally:
            delete_temp_user(TMP_USER2)
            c = admin_conn()
            try:
                c.delete(f"uid=clash_{RUN_ID},{PEOPLE_OU}")
            except Exception:
                pass
            finally:
                c.unbind()

        print("\n10. sync_all with discovery end to end")
        TMP_USER3 = f"ldaptest3_{RUN_ID}"
        make_temp_user(TMP_USER3)
        try:
            result = ldap_auth.sync_all(LDAP_CFG, discover=True)
            check("sync_all(discover=True) reports the newly discovered user", TMP_USER3 in result["discovered_users"], result)
            check("...and it is now also counted as checked/known", result["checked"] >= 4, result)
        finally:
            delete_temp_user(TMP_USER3)

        print("\n11. sync_group scopes login, discovery and re-sync; user_group maps the user role")
        ENG, ADM, ANA = (f"cn={g},{GROUPS_OU}" for g in ("dkw_engineers", "dkw_admins", "dkw_analysts"))
        cfg_g = dict(LDAP_CFG, sync_group=ENG)
        check("_in_sync_group: no sync_group means everyone", ldap_auth._in_sync_group(LDAP_CFG, []) is True)
        check("_in_sync_group: case-insensitive member / non-member", ldap_auth._in_sync_group(cfg_g, [ENG.upper()]) and not ldap_auth._in_sync_group(cfg_g, [ADM]))
        check("user_group maps to user even when default_role differs",
              ldap_auth._map_role(dict(LDAP_CFG, user_group=ANA, default_role="power_user"), [ANA]) == "user")
        check("no role group matched falls back to default_role, not a skip",
              ldap_auth._map_role(dict(LDAP_CFG, default_role="power_user"), [ANA]) == "power_user")
        u, why = ldap_auth.authenticate("bjones", USER_PASSWORD, cfg_g)
        check("login refused for a valid LDAP user outside sync_group", u is None and "not authorized" in (why or ""), (u, why))
        u, why = ldap_auth.authenticate("jdoe", USER_PASSWORD, cfg_g)
        check("login allowed for a member of sync_group", u is not None and u["username"] == "jdoe", (u, why))
        check("...and lands in the mapped role", u and u["role"] == "power_user", u)
        r = ldap_auth.sync_user("jdoe", dict(LDAP_CFG, sync_group=ADM))
        check("re-sync deactivates an account that left the sync group", r["status"] == "deactivated" and not auth.get_user_by_username("jdoe")["is_active"], r)
        r = ldap_auth.sync_user("jdoe", LDAP_CFG)
        check("...and reactivates it once back in scope", auth.get_user_by_username("jdoe")["is_active"] == 1, r)
        TMP_USER4 = f"ldaptest4_{RUN_ID}"
        make_temp_user(TMP_USER4)
        try:
            d = ldap_auth.discover_and_provision(cfg_g)
            check("discovery skips directory users outside sync_group", TMP_USER4 in d["skipped"] and TMP_USER4 not in d["discovered"]
                  and auth.get_user_by_username(TMP_USER4) is None, d)
        finally:
            delete_temp_user(TMP_USER4)
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All real LDAP authentication checks passed.")


if __name__ == "__main__":
    main()
