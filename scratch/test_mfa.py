#!/usr/bin/env python3
"""
TOTP MFA verification against a throwaway WAREHOUSE_DIR. Tests: RFC 6238 test vectors; two-step enrolment; secret
encrypted at rest and never returned by any user endpoint; two-phase login (mfa_token is not a session); wrong /
replayed / backup codes; lockout; token expiry/purpose/password-change invalidation; disable & regenerate need a
second factor (+ password for local accounts); admin reset; OIDC accounts excluded; no MFA state means unchanged login.
"""
import base64
import datetime
import os
import shutil
import sqlite3
import stat
import sys
import tempfile
import time

TMP_ROOT = tempfile.mkdtemp(prefix="mfa_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for var in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY"):
    os.environ.pop(var, None)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import jwt
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth, mfa

FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def db(sql, *params):
    with sqlite3.connect(os.path.join(os.environ["WAREHOUSE_DIR"], ".metadata", "auth.db")) as c:
        c.row_factory = sqlite3.Row
        cur = c.execute(sql, params)
        return cur.fetchall()


def main():
    try:
        client = TestClient(app_module.app)
        with auth.get_db_connection() as c:
            c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
        auth.create_user("alice", "alicepassword1", "Alice", role="user")
        auth.create_user("bob", "bobpassword1", "Bob", role="user")

        def login(user, pw):
            return client.post("/api/auth/login", json={"username": user, "password": pw})

        def session(user, pw):
            r = login(user, pw)
            return {auth.COOKIE_NAME: r.cookies.get(auth.COOKIE_NAME)}

        print("1. RFC 6238 test vectors (SHA-1, 6 digits)")
        key = b"12345678901234567890"
        for t, want in [(59, "287082"), (1111111109, "081804"), (1111111111, "050471"), (1234567890, "005924"), (2000000000, "279037"), (20000000000, "353130")]:
            check(f"T={t} -> {want}", mfa.totp(key, t) == want, mfa.totp(key, t))
        check("codes are accepted +-1 step and no further", mfa.match_step(key, mfa.totp(key, 59 + 30), 59) is not None and mfa.match_step(key, mfa.totp(key, 59 + 90), 59) is None)
        check("non-numeric / wrong-length input never matches", mfa.match_step(key, "abcdef") is None and mfa.match_step(key, "12345") is None)

        print("\n2. Enrolment")
        alice = session("alice", "alicepassword1")
        client.cookies.clear()
        check("setup needs a session", client.post("/api/auth/mfa/setup").status_code == 401)
        r = client.post("/api/auth/mfa/setup", cookies=alice)
        setup = r.json()
        check("setup returns a secret and an otpauth URI, and enables nothing", r.status_code == 200 and setup["otpauth_uri"].startswith("otpauth://totp/") and setup["secret"] in setup["otpauth_uri"]
              and not mfa.is_enabled(auth.get_user_by_username("alice")["id"]), r.text)
        secret = base64.b32decode(setup["secret"] + "=" * (-len(setup["secret"]) % 8))
        check("a wrong code does not enable MFA", client.post("/api/auth/mfa/enable", json={"code": "000000"}, cookies=alice).status_code == 400)
        check("login is unchanged until enrolment is confirmed", login("alice", "alicepassword1").json().get("success") is True)
        r = client.post("/api/auth/mfa/enable", json={"code": mfa.totp(secret)}, cookies=alice)
        codes = r.json().get("backup_codes", [])
        check("a valid code enables MFA and returns 10 backup codes once", r.status_code == 200 and len(codes) == 10 and len(set(codes)) == 10, r.text)
        check("status reflects it", client.get("/api/auth/mfa/status", cookies=alice).json() == {"enabled": True, "pending": False, "backup_codes_remaining": 10})
        check("setup while enabled is refused", client.post("/api/auth/mfa/setup", cookies=alice).status_code == 400)
        raw = db("select totp_secret, totp_backup from users where username='alice'")[0]
        check("the secret is encrypted at rest", setup["secret"] not in raw["totp_secret"] and secret not in raw["totp_secret"].encode())
        check("backup codes are stored hashed, not in clear", not any(c.replace("-", "") in raw["totp_backup"] for c in codes))
        keyfile = os.path.join(os.environ["WAREHOUSE_DIR"], ".metadata", "mfa.key")
        check("mfa.key is mode 0600", stat.S_IMODE(os.stat(keyfile).st_mode) == 0o600)
        me = client.get("/api/auth/me", cookies=alice).json()["user"]
        listed = next(u for u in client.get("/api/users", cookies=session("admin", "adminpassword123")).json()["users"] if u["username"] == "alice") if client.get("/api/users", cookies=session("admin", "adminpassword123")).status_code == 200 else {}
        leak = [k for k in list(me) + list(listed) if k.startswith("totp_") or k == "password_hash"]
        check("no user endpoint exposes secret material", not leak and me.get("mfa_enabled") is True, leak)

        print("\n3. Two-phase login")
        r = login("alice", "alicepassword1")
        body = r.json()
        check("the password step returns mfa_required and NO session", body.get("mfa_required") is True and body.get("success") is False and auth.COOKIE_NAME not in r.cookies and "user" not in body, body)
        tok = body["mfa_token"]
        client.cookies.clear()
        check("the mfa_token is not a session", client.get("/api/auth/me", cookies={auth.COOKIE_NAME: tok}).json().get("authenticated") is False
              and client.get("/api/workspace/files", cookies={auth.COOKIE_NAME: tok}).status_code == 401)
        check("a wrong password never yields an mfa_token", "mfa_token" not in login("alice", "wrong").json())
        check("a wrong code is 401 and gives no session", client.post("/api/auth/login/mfa", json={"mfa_token": tok, "code": "123456"}).status_code == 401)
        code = mfa.totp(secret, time.time() + 30)          # enrolment consumed the current step (replay protection)
        r = client.post("/api/auth/login/mfa", json={"mfa_token": tok, "code": code})
        check("the right code completes the login", r.status_code == 200 and r.json()["user"]["username"] == "alice" and auth.COOKIE_NAME in r.cookies and r.json()["user"]["mfa_enabled"] is True, r.text)
        r = client.post("/api/auth/login/mfa", json={"mfa_token": login("alice", "alicepassword1").json()["mfa_token"], "code": code})
        check("the same code cannot be replayed", r.status_code == 401, r.text)
        db("update users set totp_last_step = 0 where username='alice'")
        check("a code from the next step is accepted (clock drift)", client.post("/api/auth/login/mfa", json={"mfa_token": login("alice", "alicepassword1").json()["mfa_token"], "code": mfa.totp(secret, time.time() + 30)}).status_code == 200)
        bc = codes[0]
        r = client.post("/api/auth/login/mfa", json={"mfa_token": login("alice", "alicepassword1").json()["mfa_token"], "code": bc.lower().replace("-", " ")})
        check("a backup code logs in (any case / separators)", r.status_code == 200, r.text)
        r = client.post("/api/auth/login/mfa", json={"mfa_token": login("alice", "alicepassword1").json()["mfa_token"], "code": bc})
        check("...but only once", r.status_code == 401)
        check("and the count drops", client.get("/api/auth/mfa/status", cookies=alice).json()["backup_codes_remaining"] == 9)

        print("\n4. Token hygiene")
        user = auth.get_user_by_username("alice")
        check("a session JWT is not accepted as an mfa_token", client.post("/api/auth/login/mfa", json={"mfa_token": auth.create_access_token(user), "code": "000000"}).status_code == 401)
        expired = jwt.encode({"sub": user["id"], "purpose": "mfa", "pc": 0, "exp": int(time.time()) - 5}, auth.JWT_SECRET_KEY, algorithm="HS256")
        check("an expired mfa_token is refused", client.post("/api/auth/login/mfa", json={"mfa_token": expired, "code": mfa.totp(secret)}).status_code == 401)
        old = login("alice", "alicepassword1").json()["mfa_token"]
        time.sleep(1.1)
        auth.reset_user_password(user["id"], "newpassword99", chosen_by_self=True)
        check("an mfa_token dies if the password changes meanwhile", client.post("/api/auth/login/mfa", json={"mfa_token": old, "code": mfa.totp(secret, time.time() + 30)}).status_code == 401)
        auth.reset_user_password(user["id"], "alicepassword1", chosen_by_self=True)
        time.sleep(1.1)

        print("\n5. Lockout")
        db("update users set totp_last_step = 0, totp_failures = 0, totp_locked_until = 0 where username='alice'")
        t = login("alice", "alicepassword1").json()["mfa_token"]
        statuses = [client.post("/api/auth/login/mfa", json={"mfa_token": t, "code": "000000"}).status_code for _ in range(5)]
        check("5 wrong codes are refused", statuses == [401] * 5, statuses)
        r = client.post("/api/auth/login/mfa", json={"mfa_token": t, "code": mfa.totp(secret)})
        check("then even the right code is refused (429) while locked", r.status_code == 429, (r.status_code, r.text))
        db("update users set totp_locked_until = 0 where username='alice'")
        check("the lock expires", client.post("/api/auth/login/mfa", json={"mfa_token": t, "code": mfa.totp(secret)}).status_code == 200)

        print("\n6. Disable / regenerate need a second factor")
        alice = session("alice", "alicepassword1") if False else None
        t = login("alice", "alicepassword1").json()["mfa_token"]
        db("update users set totp_last_step = 0 where username='alice'")
        alice = {auth.COOKIE_NAME: client.post("/api/auth/login/mfa", json={"mfa_token": t, "code": mfa.totp(secret)}).cookies.get(auth.COOKIE_NAME)}
        db("update users set totp_last_step = 0 where username='alice'")
        check("disable without a code/password is refused", client.post("/api/auth/mfa/disable", json={"code": "000000"}, cookies=alice).status_code == 403)
        check("disable with a code but no password is refused (local account)", client.post("/api/auth/mfa/disable", json={"code": mfa.totp(secret)}, cookies=alice).status_code == 403)
        check("MFA is still on", mfa.is_enabled(user["id"]))
        db("update users set totp_last_step = 0 where username='alice'")
        r = client.post("/api/auth/mfa/backup-codes", json={"code": mfa.totp(secret), "password": "alicepassword1"}, cookies=alice)
        fresh = r.json().get("backup_codes", [])
        check("regenerate returns a fresh set and invalidates the old", r.status_code == 200 and len(fresh) == 10 and not set(fresh) & set(codes), r.text)
        db("update users set totp_last_step = 0 where username='alice'")
        check("password + code disables MFA", client.post("/api/auth/mfa/disable", json={"code": mfa.totp(secret), "password": "alicepassword1"}, cookies=alice).status_code == 200 and not mfa.is_enabled(user["id"]))
        check("login is single-step again", login("alice", "alicepassword1").json().get("success") is True)
        check("the old secret is gone", db("select totp_secret, totp_backup from users where username='alice'")[0]["totp_secret"] is None)

        print("\n7. Admin reset, and who is excluded")
        bob = session("bob", "bobpassword1")
        s2 = client.post("/api/auth/mfa/setup", cookies=bob).json()
        sec2 = base64.b32decode(s2["secret"] + "=" * (-len(s2["secret"]) % 8))
        client.post("/api/auth/mfa/enable", json={"code": mfa.totp(sec2)}, cookies=bob)
        bob_id = auth.get_user_by_username("bob")["id"]
        admin = session("admin", "adminpassword123")
        check("a non-admin cannot reset someone's MFA", client.post(f"/api/users/{bob_id}/mfa/reset", cookies=bob).status_code == 403)
        check("an admin cannot use the reset endpoint on themselves", client.post(f"/api/users/{auth.get_user_by_username('admin')['id']}/mfa/reset", cookies=admin).status_code == 400)
        check("an admin resets a lost device", client.post(f"/api/users/{bob_id}/mfa/reset", cookies=admin).status_code == 200 and not mfa.is_enabled(bob_id))
        check("reset of an unknown user is 404", client.post("/api/users/nope/mfa/reset", cookies=admin).status_code == 404)
        auth.upsert_external_user("oidcdave", "Dave", "user", "oidc")
        dave = {auth.COOKIE_NAME: auth.create_access_token(auth.get_user_by_username("oidcdave"))}
        check("an OIDC account cannot enrol (its IdP owns the second factor)", client.post("/api/auth/mfa/setup", cookies=dave).status_code == 403)
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All MFA checks passed.")


if __name__ == "__main__":
    main()
