#!/usr/bin/env python3
"""Per-user / per-role network policies (web/ip_allowlist.py's network_policies table + the _network_policy_gate middleware),
on top of and independent of the global IP allowlist: validation, the user-over-role precedence, the self-lockout guard,
enforcement (blocked / allowed by address), loopback exemption, and that a blocked account can still see why and sign out."""
import os, shutil, sys, tempfile
TMP = tempfile.mkdtemp(prefix="netpol_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "IP_ALLOWLIST_OVERRIDE", "TRUSTED_PROXIES"): os.environ.pop(v, None)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from fastapi.testclient import TestClient
from web import app as app_module, auth, ip_allowlist as ipa

FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)


def from_(ip, **kw):
    return TestClient(app_module.app, client=(ip, 50000), **kw)


def put(c, ptype, pid, rules):
    return c.put(f"/api/network-policies/{ptype}/{pid}", json={"rules": [{"cidr": r} for r in rules]})


def delete(c, ptype, pid):
    return c.delete(f"/api/network-policies/{ptype}/{pid}")


API = "/api/autoloader/pipelines"      # any signed-in user may call it: a plain, ordinary /api/* endpoint to probe blocking with
                                        # (unlike /api/auth/me, which this gate deliberately always allows -- see the check below)


with auth.get_db_connection() as c:
    c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
alice_id = auth.create_user("alice", "alicepassword1", "Alice", role="user")["id"]
bob_id = auth.create_user("bob", "bobpassword1", "Bob", role="user")["id"]
OFFICE = "203.0.113.5"
HOME = "198.51.100.9"

admin = from_(OFFICE)
assert admin.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"}).status_code == 200
admin_id = auth.get_user_by_username("admin")["id"]

print("module-level validation")
try:
    ipa.set_network_policy("group", "x", [{"cidr": OFFICE}], {"id": admin_id, "username": "admin", "role": "admin"}); ok = False
except ipa.AllowlistError:
    ok = True
check("an unknown principal type is refused", ok)
try:
    ipa.set_network_policy("role", "superadmin", [{"cidr": OFFICE}], {"id": admin_id, "username": "admin", "role": "admin"}); ok = False
except ipa.AllowlistError:
    ok = True
check("an unknown role is refused", ok)
try:
    ipa.set_network_policy("user", "nobody", [{"cidr": OFFICE}], {"id": admin_id, "username": "admin", "role": "admin"}); ok = False
except ipa.AllowlistError:
    ok = True
check("a nonexistent user id is refused", ok)
try:
    ipa.set_network_policy("role", "user", [], {"id": admin_id, "username": "admin", "role": "admin"}); ok = False
except ipa.AllowlistError as e:
    ok = "at least one" in str(e)
check("an empty rule list is refused (delete instead of locking everyone out)", ok)
check("nothing was saved by the refusals", ipa.get_network_policy("role", "user") is None and ipa.get_network_policy("user", "nobody") is None)

print("HTTP endpoints are admin-only")
alice = from_(OFFICE); alice.post("/api/auth/login", json={"username": "alice", "password": "alicepassword1"})
check("a plain user cannot read, set or delete policies", alice.get("/api/network-policies").status_code == 403 and put(alice, "role", "user", [OFFICE]).status_code == 403 and delete(alice, "role", "user").status_code == 403)

print("self-lockout guard")
r = put(admin, "role", "admin", [HOME])
check("a role policy that would block the acting admin's own current address is refused", r.status_code == 400 and "lock" in r.text.lower(), r.text)
check("nothing changed", ipa.get_network_policy("role", "admin") is None)
r = put(admin, "role", "admin", [OFFICE])
check("...but one that includes it is accepted", r.status_code == 200 and r.json()["rules"] == [{"cidr": OFFICE + "/32", "note": ""}], r.text)
r = put(admin, "user", admin_id, [HOME])
check("a user-specific policy that would block the acting admin (targeting themselves) is refused the same way", r.status_code == 400 and "lock" in r.text.lower(), r.text)
r = delete(admin, "role", "admin")
check("removing the (still-set) role policy works", r.status_code == 200 and r.json().get("success") is True, r.text)
check("a repeat delete 404s", delete(admin, "role", "admin").status_code == 404)

print("enforcement: role policy, independent of the global allowlist (left off throughout)")
check("the global allowlist is off", ipa.get_config()["mode"] == "off")
r = put(admin, "role", "user", [OFFICE]); check("a role policy for 'user' is saved", r.status_code == 200, r.text)
check("alice (role=user) from the office address is let through", from_(OFFICE, cookies=alice.cookies).get(API).status_code == 200)
blocked = from_(HOME, cookies=alice.cookies).get(API)
check("...but from another address she is refused, with a clear reason and her address", blocked.status_code == 403 and blocked.json()["ip_blocked"] is True and blocked.json()["your_address"] == HOME, blocked.text)
check("a plain page (not /api/*) is not gated: only API calls are refused (the SPA shell can still load and explain)", from_(HOME, cookies=alice.cookies).get("/").status_code == 200)
me = from_(HOME, cookies=alice.cookies)
check("/api/auth/me and /api/auth/logout stay reachable even while blocked (so the account holder can see why and sign out)", me.get("/api/auth/me").status_code == 200 and me.post("/api/auth/logout").status_code == 200)
alice = from_(OFFICE); alice.post("/api/auth/login", json={"username": "alice", "password": "alicepassword1"})  # logging back in (the previous session was just logged out above)
bob = from_(OFFICE); bob.post("/api/auth/login", json={"username": "bob", "password": "bobpassword1"})
check("bob (role=user, no user-specific policy) is bound by the same role policy", from_(HOME, cookies=bob.cookies).get(API).status_code == 403 and from_(OFFICE, cookies=bob.cookies).get(API).status_code == 200)
check("admin is untouched (no policy on the admin role any more)", from_(HOME, cookies=admin.cookies).get(API).status_code == 200)
check("loopback without forwarding headers stays exempt, same as the global allowlist", from_("127.0.0.1", cookies=alice.cookies).get(API).status_code == 200)

print("enforcement: a user-specific policy overrides the role policy")
r = put(admin, "user", alice_id, [HOME]); check("alice gets her own, narrower policy", r.status_code == 200, r.text)
check("she is now allowed from HOME (her own policy), not the office (her role's policy) any more", from_(HOME, cookies=alice.cookies).get(API).status_code == 200 and from_(OFFICE, cookies=alice.cookies).get(API).status_code == 403)
check("bob, who has no user-specific policy, still follows the role policy (office only)", from_(OFFICE, cookies=bob.cookies).get(API).status_code == 200 and from_(HOME, cookies=bob.cookies).get(API).status_code == 403)
check("the list shows both the role and the user policy, with a readable label", any(p["principal_type"] == "user" and p["label"] == "alice" for p in admin.get("/api/network-policies").json()["policies"]) and any(p["principal_type"] == "role" and p["principal_id"] == "user" for p in admin.get("/api/network-policies").json()["policies"]))
check("deleting alice's own policy falls back to her role policy again", delete(admin, "user", alice_id).status_code == 200 and from_(OFFICE, cookies=alice.cookies).get(API).status_code == 200 and from_(HOME, cookies=alice.cookies).get(API).status_code == 403)
check("deleting the role policy too removes every restriction", delete(admin, "role", "user").status_code == 200 and from_(HOME, cookies=alice.cookies).get(API).status_code == 200)

print("changes are audited")
check("policy updates and deletions are audited", all(a_ in str(__import__("sqlite3").connect(TMP + "/warehouse/.metadata/governance.db").execute("SELECT action FROM governance_audit").fetchall()) for a_ in ("NETWORK_POLICY_UPDATE", "NETWORK_POLICY_DELETE")))

shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS")
sys.exit(1 if FAIL else 0)
