#!/usr/bin/env python3
"""Organisation-wide MFA policy (web/mfa_policy.py + the _mfa_policy_gate middleware) against a throwaway WAREHOUSE_DIR: policy guards, grace
periods, enforcement after the deadline (only enrolment allowed), exemptions, extensions, role scope, SSO accounts, stats, break-glass override."""
import base64, os, shutil, sqlite3, sys, tempfile, time
TMP_ROOT = tempfile.mkdtemp(prefix="mfapol_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP_ROOT, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "MFA_POLICY_OVERRIDE"): os.environ.pop(v, None)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from fastapi.testclient import TestClient
from web import app as app_module, auth, mfa, mfa_policy
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def db(sql, *p):
    with sqlite3.connect(os.path.join(os.environ["WAREHOUSE_DIR"], ".metadata", "auth.db")) as c:
        c.row_factory = sqlite3.Row; cur = c.execute(sql, p); c.commit(); return cur.fetchall()
DAY = 86400
def client_for(user, pw):
    c = TestClient(app_module.app); r = c.post("/api/auth/login", json={"username": user, "password": pw})
    assert r.status_code == 200 and r.json().get("success"), r.text; return c
def enrol(c):
    s = c.post("/api/auth/mfa/setup"); assert s.status_code == 200, s.text
    secret = base64.b32decode(s.json()["secret"]); r = c.post("/api/auth/mfa/enable", json={"code": mfa.totp(secret)}); assert r.status_code == 200, r.text
    return secret, r.json()["backup_codes"]
def set_policy(c, **kw): return c.put("/api/mfa/policy", json={"enabled": True, "roles": ["admin", "power_user", "user"], "grace_days": 7, **kw})
def age_policy(days): db("UPDATE mfa_policy SET enabled_at = ?", int(time.time()) - days * DAY); mfa_policy._invalidate()
def age_user(name, days): db("UPDATE users SET created_at = ? WHERE username = ?", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - days * DAY)), name)
def uid(name): return db("SELECT id FROM users WHERE username = ?", name)[0]["id"]

try:
    boot = TestClient(app_module.app)
    with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
    for n, role in (("alice", "user"), ("bob", "user"), ("carol", "power_user"), ("dave", "user"), ("erin", "user")): auth.create_user(n, n + "password1", n, role=role)
    admin = client_for("admin", "adminpassword123"); alice = client_for("alice", "alicepassword1")
    API = "/api/autoloader/pipelines"      # any signed-in user may call it

    print("defaults and guards")
    check("the policy is off by default and nobody is affected", admin.get("/api/mfa/policy").json()["enabled"] is False and alice.get(API).status_code == 200 and alice.get("/api/auth/me").json()["user"]["mfa_policy"]["state"] == "off")
    check("only administrators can read or change the policy and the stats", alice.get("/api/mfa/policy").status_code == 403 and alice.put("/api/mfa/policy", json={"enabled": True, "roles": ["user"], "grace_days": 1}).status_code == 403 and alice.get("/api/mfa/stats").status_code == 403)
    r = set_policy(admin); check("an administrator who is covered but has no second factor cannot turn the policy on (no self lock-out)", r.status_code == 400 and "your own account" in r.json()["detail"], r.text)
    check("a bad grace period and an empty role list are refused", set_policy(admin, grace_days=999).status_code == 400 and set_policy(admin, roles=[]).status_code == 400)
    admin_secret, admin_backup = enrol(admin)
    r = set_policy(admin); check("after enrolling themselves the administrator can turn it on", r.status_code == 200 and r.json()["enabled"] is True and r.json()["grace_days"] == 7, r.text)

    print("grace period")
    alice = client_for("alice", "alicepassword1")
    mp = alice.get("/api/auth/me").json()["user"]["mfa_policy"]
    check("a covered user without MFA is in grace with the days left", mp["state"] == "grace" and mp["required"] and mp["days_left"] == 7, mp)
    check("everything works during the grace period", alice.get(API).status_code == 200)
    age_policy(3); age_user("alice", 3)
    check("the days left count down", alice.get("/api/auth/me").json()["user"]["mfa_policy"]["days_left"] == 4)
    check("the login response carries the policy status", TestClient(app_module.app).post("/api/auth/login", json={"username": "alice", "password": "alicepassword1"}).json()["user"]["mfa_policy"]["state"] == "grace")

    print("enforcement after the deadline")
    age_policy(30); age_user("alice", 30)
    alice = client_for("alice", "alicepassword1")
    r = alice.get(API)
    check("an overdue user is refused everywhere (403, mfa_enrollment_required)", r.status_code == 403 and r.json().get("mfa_enrollment_required") is True, (r.status_code, r.text))
    check("...including writes and other API areas", alice.post("/api/sql/execute", json={"query": "select 1"}).status_code == 403 and alice.get("/api/history").status_code == 403)
    check("they can still sign in, see their status, and log out", alice.get("/api/auth/me").status_code == 200 and alice.get("/api/auth/me").json()["user"]["mfa_policy"]["state"] == "overdue" and alice.get("/api/auth/mfa/status").status_code == 200)
    check("a request without a session is not touched by the gate", "mfa_enrollment_required" not in TestClient(app_module.app).get(API).text)
    secret, backup = enrol(alice)
    check("enrolling lifts the block at once", alice.get(API).status_code == 200 and alice.get("/api/auth/me").json()["user"]["mfa_policy"]["state"] == "compliant")
    r = alice.post("/api/auth/mfa/disable", json={"code": mfa.totp(secret, at=time.time() + 30), "password": "alicepassword1"})
    check("a covered user cannot turn their MFA off while the policy is on (409)", r.status_code == 409 and "requires" in r.json()["detail"], r.text)
    check("the second sign-in step is still required for them", TestClient(app_module.app).post("/api/auth/login", json={"username": "alice", "password": "alicepassword1"}).json().get("mfa_required") is True)

    print("exemptions and extensions")
    age_user("bob", 30); bob = client_for("bob", "bobpassword1")
    check("bob is overdue", bob.get(API).status_code == 403)
    r = admin.post(f"/api/users/{uid('bob')}/mfa/exempt", json={"exempt": True, "reason": ""}); check("an exemption needs a reason", r.status_code == 400)
    r = admin.post(f"/api/users/{uid('bob')}/mfa/exempt", json={"exempt": True, "reason": "service account, hardware token pending"})
    check("an exempt user is not blocked and shows state exempt", r.status_code == 200 and bob.get(API).status_code == 200 and bob.get("/api/auth/me").json()["user"]["mfa_policy"]["state"] == "exempt")
    admin.post(f"/api/users/{uid('bob')}/mfa/exempt", json={"exempt": False, "reason": ""})
    check("removing the exemption blocks them again", bob.get(API).status_code == 403)
    r = admin.post(f"/api/users/{uid('bob')}/mfa/extend", json={"days": 3})
    mp = bob.get("/api/auth/me").json()["user"]["mfa_policy"]
    check("an extension gives one user more time", r.status_code == 200 and bob.get(API).status_code == 200 and mp["state"] == "grace" and mp["days_left"] == 3, mp)
    admin.post(f"/api/users/{uid('bob')}/mfa/extend", json={"days": 0})
    check("removing the extension blocks them again", bob.get(API).status_code == 403)
    check("bad days are refused, unknown users are 404", admin.post(f"/api/users/{uid('bob')}/mfa/extend", json={"days": 9999}).status_code == 400 and admin.post("/api/users/nope/mfa/extend", json={"days": 2}).status_code == 404)

    print("scope: roles, SSO, LDAP, inactive users")
    age_user("carol", 30); carol = client_for("carol", "carolpassword1")
    check("a power user is covered too", carol.get(API).status_code == 403)
    r = admin.put("/api/mfa/policy", json={"enabled": True, "roles": ["admin"], "grace_days": 7})
    check("narrowing the policy to admins frees the other roles at once", r.status_code == 200 and carol.get(API).status_code == 200 and bob.get(API).status_code == 200 and bob.get("/api/auth/me").json()["user"]["mfa_policy"]["state"] == "not_covered")
    set_policy(admin)
    check("...and widening it brings them back (their deadlines are unchanged)", carol.get(API).status_code == 403 and bob.get(API).status_code == 403)
    db("UPDATE users SET auth_source = 'oidc' WHERE username = 'carol'"); mfa_policy._invalidate()
    check("an SSO (OIDC) account is never asked for a second factor here", carol.get(API).status_code == 200 and carol.get("/api/auth/me").json()["user"]["mfa_policy"]["state"] == "sso")
    db("UPDATE users SET auth_source = 'saml' WHERE username = 'carol'")
    check("nor a SAML account", carol.get(API).status_code == 200)
    r = carol.post("/api/auth/mfa/setup"); check("SAML accounts cannot enrol a local TOTP either (their IdP owns it)", r.status_code == 403, r.text)
    db("UPDATE users SET auth_source = 'ldap' WHERE username = 'carol'")
    check("an LDAP account is covered (this studio checks their password)", carol.get(API).status_code == 403)
    db("UPDATE users SET auth_source = 'local' WHERE username = 'carol'")
    auth.create_user("frank", "frankpassword1", "frank", role="user"); age_user("frank", 30); db("UPDATE users SET is_active = 0 WHERE username = 'frank'")
    auth.create_user("gina", "ginapassword1", "gina", role="user"); age_user("gina", 30); db("UPDATE users SET deleted_at = '2026-01-01 00:00:00' WHERE username = 'gina'")

    print("new accounts")
    set_policy(admin, grace_days=0)
    auth.create_user("hank", "hankpassword1", "hank", role="user")
    hank = client_for("hank", "hankpassword1")
    check("with 0 grace days a new account must enrol at its first sign-in", hank.get(API).status_code == 403)
    set_policy(admin, grace_days=14)
    check("a longer grace period applies to a new account from its creation (and to accounts already in scope)", hank.get(API).status_code == 200 and hank.get("/api/auth/me").json()["user"]["mfa_policy"]["days_left"] in (13, 14))

    print("stats")
    age_policy(30); age_user("bob", 30); age_user("hank", 30)
    s = admin.get("/api/mfa/stats").json()
    names = {a["username"]: a for a in s["attention"]}
    check("stats count covered / enrolled / overdue and leave inactive and deleted users out", s["policy"]["enabled"] and "frank" not in names and "gina" not in names and s["compliant"] >= 2 and s["overdue"] >= 2, {k: s[k] for k in ("covered", "compliant", "grace", "overdue", "exempt")})
    check("coverage percent is compliant / covered", s["coverage_percent"] == round(100 * s["compliant"] / s["covered"], 1), s["coverage_percent"])
    check("the list names who needs attention, with state and deadline", names["bob"]["state"] == "overdue" and names["bob"]["deadline"] and names["hank"]["state"] == "overdue" and s["attention"][0]["state"] == "overdue")
    check("per-role breakdown adds up", sum(v["total"] for v in s["by_role"].values()) == s["covered"], s["by_role"])
    check("enrolments of the last weeks are counted", s["enrolled_per_week"][-1] >= 2 and len(s["enrolled_per_week"]) == 8, s["enrolled_per_week"])
    db("UPDATE users SET auth_source = 'oidc' WHERE username = 'erin'"); mfa_policy._invalidate()
    check("SSO accounts are counted separately", admin.get("/api/mfa/stats").json()["sso_accounts"] == 1)
    db("UPDATE users SET auth_source = 'local' WHERE username = 'erin'")

    print("break-glass and turning it off")
    check("bob is blocked", bob.get(API).status_code == 403)
    os.environ["MFA_POLICY_OVERRIDE"] = "off"
    check("MFA_POLICY_OVERRIDE=off suspends enforcement without touching the setting", bob.get(API).status_code == 200 and admin.get("/api/mfa/policy").json()["overridden"] is True and admin.get("/api/mfa/policy").json()["stored_enabled"] is True)
    del os.environ["MFA_POLICY_OVERRIDE"]
    check("...and removing it enforces again", bob.get(API).status_code == 403)
    r = admin.put("/api/mfa/policy", json={"enabled": False, "roles": ["user"], "grace_days": 5})
    check("turning the policy off frees everyone", r.status_code == 200 and bob.get(API).status_code == 200 and hank.get(API).status_code == 200)
    r = set_policy(admin)
    check("turning it on again restarts the grace period for everyone", bob.get(API).status_code == 200 and bob.get("/api/auth/me").json()["user"]["mfa_policy"]["days_left"] == 7)
    r = alice.post("/api/auth/mfa/disable", json={"code": mfa.totp(secret, at=time.time() + 60), "password": "alicepassword1"})
    admin.put("/api/mfa/policy", json={"enabled": False, "roles": ["user"], "grace_days": 5})
    r = alice.post("/api/auth/mfa/disable", json={"code": backup[0], "password": "alicepassword1"})
    check("with the policy off a user may turn MFA off again", r.status_code == 200, r.text)
    acts = [r["action"] for r in sqlite3.connect(os.path.join(os.environ["WAREHOUSE_DIR"], ".metadata", "governance.db")).execute("SELECT action FROM governance_audit").fetchall() and [dict(zip(["action"], x)) for x in sqlite3.connect(os.path.join(os.environ["WAREHOUSE_DIR"], ".metadata", "governance.db")).execute("SELECT action FROM governance_audit")]]
    check("policy changes, exemptions and extensions are audited", {"MFA_POLICY_UPDATE", "MFA_EXEMPT", "MFA_EXEMPT_REMOVED", "MFA_EXTEND"} <= set(acts), sorted(set(acts))[:12])
finally:
    shutil.rmtree(TMP_ROOT, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
