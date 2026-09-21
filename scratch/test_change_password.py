#!/usr/bin/env python3
"""
Verification of self-service password change (local accounts only). Runs against a throwaway WAREHOUSE_DIR.
Tests: needs a session; needs the right current password; minimum length / must differ; only local accounts;
the new password logs in and the old one does not; older sessions stop working, the caller gets a fresh one;
an admin reset also ends old sessions; guest / X-User fallbacks cannot be used without the current password.
"""

import datetime
import os
import shutil
import sqlite3
import sys
import tempfile
import time

TMP_ROOT = tempfile.mkdtemp(prefix="change_pw_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(os.environ["WAREHOUSE_DIR"])
for var in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY"):
    os.environ.pop(var, None)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

import jwt
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth

FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def token_for(username, iat=None):
    user = auth.get_user_by_username(username)
    now = datetime.datetime.now(datetime.timezone.utc)
    return jwt.encode({"sub": user["id"], "iat": int(iat if iat is not None else now.timestamp()), "exp": now + datetime.timedelta(hours=1)},
                      auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)


def main():
    try:
        client = TestClient(app_module.app)
        change = lambda cookies, cur, new: client.post("/api/auth/change-password", json={"current_password": cur, "new_password": new}, cookies=cookies)
        login = lambda user, pw: client.post("/api/auth/login", json={"username": user, "password": pw}).status_code
        bob = {auth.COOKIE_NAME: token_for("analyst_bob")}
        old = "userpassword123"

        print("1. Access")
        check("no session: 401", change({}, old, "brandnew1").status_code == 401)
        check("a garbage session: 401", change({auth.COOKIE_NAME: "garbage"}, old, "brandnew1").status_code == 401)
        check("an X-User header alone changes nothing", client.post("/api/auth/change-password", headers={"X-User": "analyst_bob"},
              json={"current_password": "wrong", "new_password": "brandnew1"}).status_code in (401, 403))
        check("wrong current password: 403", change(bob, "not-the-password", "brandnew1").status_code == 403)
        check("...and the password is unchanged", login("analyst_bob", old) == 200)

        print("\n2. Validation")
        check("too short: 400", change(bob, old, "abc").status_code == 400)
        check("same as current: 400", change(bob, old, old).status_code == 400)
        check("still unchanged", login("analyst_bob", old) == 200)

        print("\n3. Change")
        older = {auth.COOKIE_NAME: token_for("analyst_bob", iat=time.time() - 60)}
        time.sleep(1.1)
        r = change(bob, old, "brandnew1")
        check("a correct change: 200", r.status_code == 200, r.text)
        check("the new password logs in", login("analyst_bob", "brandnew1") == 200)
        check("the old password no longer does", login("analyst_bob", old) == 401)
        check("a session from before the change is rejected", client.get("/api/auth/me", cookies=older).json().get("authenticated") is False
              and client.get("/api/workspace/files", cookies=older).status_code == 401)
        fresh = r.cookies.get(auth.COOKIE_NAME)
        check("the caller receives a fresh session that works", bool(fresh) and client.get("/api/auth/me", cookies={auth.COOKIE_NAME: fresh}).json()["authenticated"] is True)
        check("other users are unaffected", login("lead_engineer", "powerpassword123") == 200)

        print("\n4. Only local accounts")
        with sqlite3.connect(os.path.join(os.environ["WAREHOUSE_DIR"], ".metadata", "auth.db")) as c:
            c.execute("update users set auth_source = 'oidc' where username = 'lead_engineer'")
        lead = {auth.COOKIE_NAME: token_for("lead_engineer")}
        r = change(lead, "powerpassword123", "brandnew1")
        check("an externally managed account is refused", r.status_code == 403 and "identity provider" in r.text, r.text)
        check("/api/auth/me exposes auth_source for the UI", client.get("/api/auth/me", cookies=lead).json()["user"].get("auth_source") == "oidc")
        check("its password is untouched", login("lead_engineer", "powerpassword123") == 200)

        print("\n5. Admin reset ends sessions too")
        admin = {auth.COOKIE_NAME: token_for("admin")}
        victim = {auth.COOKIE_NAME: token_for("analyst_bob", iat=time.time())}
        time.sleep(1.1)
        uid = auth.get_user_by_username("analyst_bob")["id"]
        r = client.post(f"/api/users/{uid}/reset-password", json={"new_password": "resetbyadmin1"}, cookies=admin)
        check("the admin reset works", r.status_code == 200 and login("analyst_bob", "resetbyadmin1") == 200, r.text)
        check("the user's earlier session was ended", client.get("/api/workspace/files", cookies=victim).status_code == 401)
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All change-password checks passed.")


if __name__ == "__main__":
    main()
