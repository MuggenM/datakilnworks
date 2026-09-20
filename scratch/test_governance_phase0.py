#!/usr/bin/env python3
"""
Phase 0 verification for tag-based masking: security prerequisites.
Runs against a throwaway WAREHOUSE_DIR, so it never touches real data.
Tests:
1. Auth never fails open: bad credentials and errors are never admin (resolve_principal).
2. GOVERNANCE_REQUIRE_AUTH makes credential-less requests anonymous and ignores X-User.
3. The JWT signing key is a per-install secret, and tokens signed with the old public default are rejected.
4. Compute workers require X-Compute-Token; compose no longer publishes their ports.
5. Notebook role gating hands the Jupyter token only to allowed roles.
"""

import asyncio
import datetime
import os
import re
import shutil
import stat
import sys
import tempfile

TMP_WAREHOUSE = tempfile.mkdtemp(prefix="governance_p0_")
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
os.environ.pop("GOVERNANCE_REQUIRE_AUTH", None)
os.environ.pop("GOVERNANCE_RESTRICT_NOTEBOOKS", None)
os.environ.pop("JWT_SECRET_KEY", None)
os.environ.pop("COMPUTE_TOKEN", None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import jwt
from fastapi import HTTPException
from starlette.requests import Request

from web import auth

FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def make_request(headers=None, cookies=None):
    hdrs = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    if cookies:
        hdrs.append((b"cookie", "; ".join(f"{k}={v}" for k, v in cookies.items()).encode()))
    return Request({"type": "http", "method": "GET", "path": "/", "headers": hdrs, "query_string": b""})


def resolve(headers=None, cookies=None):
    """Returns (principal_dict, None) or (None, http_status)."""
    try:
        return asyncio.run(auth.resolve_principal(make_request(headers, cookies))), None
    except HTTPException as exc:
        return None, exc.status_code


def token_for(username, secret=None, expires_in=3600):
    user = auth.get_user_by_username(username)
    payload = {"sub": user["id"], "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=expires_in)}
    return jwt.encode(payload, secret or auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)


def test_auth_never_fails_open():
    print("\n1. resolve_principal never fails open")
    p, status = resolve()
    check("no credentials (local mode) -> local admin", p and p["role"] == "admin" and status is None, (p, status))

    good = token_for("analyst_bob")
    p, status = resolve(cookies={auth.COOKIE_NAME: good})
    check("valid session -> that user, not admin", p and p["username"] == "analyst_bob" and p["role"] != "admin", (p, status))

    p, status = resolve(headers={"Authorization": f"Bearer {good}"})
    check("valid bearer token -> that user", p and p["username"] == "analyst_bob", (p, status))

    p, status = resolve(cookies={auth.COOKIE_NAME: "garbage.token.value"})
    check("garbage cookie -> 401 (previously admin)", p is None and status == 401, (p, status))

    expired = token_for("admin", expires_in=-60)
    p, status = resolve(cookies={auth.COOKIE_NAME: expired})
    check("expired admin token -> 401 (previously admin)", p is None and status == 401, (p, status))

    p, status = resolve(headers={"X-User": "nobody_here"})
    check("unknown X-User -> 401", p is None and status == 401, (p, status))

    p, status = resolve(headers={"X-User": "analyst_bob"})
    check("known X-User still works in local mode", p and p["username"] == "analyst_bob", (p, status))

    real = auth.get_user_by_id
    auth.get_user_by_id = lambda _id: (_ for _ in ()).throw(RuntimeError("auth db unavailable"))
    try:
        asyncio.run(auth.resolve_principal(make_request(cookies={auth.COOKIE_NAME: good})))
        check("lookup failure does not become admin", False)
    except RuntimeError:
        check("lookup failure propagates instead of granting admin", True)
    except HTTPException as exc:
        check("lookup failure does not become admin", exc.status_code != 200)
    finally:
        auth.get_user_by_id = real

    offenders = []
    for name in os.listdir(os.path.join(BASE_DIR, "web")):
        if name.endswith(".py"):
            text = open(os.path.join(BASE_DIR, "web", name)).read()
            if re.search(r'except Exception:\s*\n\s*current_user = \{"role": "admin"', text):
                offenders.append(name)
    check("no endpoint keeps the 'except -> admin' fallback", not offenders, offenders)

    text = open(os.path.join(BASE_DIR, "web", "app.py")).read()
    swallowing = re.findall(r'await get_current_user\(request\)[^\n]*\n(?:[^\n]*\n){0,4}?\s*except Exception:\s*\n\s*(?:pass|user(?:_id)? = )', text)
    check("no endpoint swallows an auth failure (defaults to admin, None or a client-supplied user)", not swallowing, swallowing[:2])
    check("identity defaults of 'admin' are gone", not re.search(r'username = "admin"\n\s*is_admin = True\n\s*try:', text))


def test_require_auth_mode():
    print("\n2. GOVERNANCE_REQUIRE_AUTH")
    os.environ["GOVERNANCE_REQUIRE_AUTH"] = "true"
    try:
        p, status = resolve()
        check("no credentials -> anonymous least privilege", p and p["role"] == "user" and p["username"] == "anonymous", (p, status))
        p, status = resolve(headers={"X-User": "admin"})
        check("X-User: admin is ignored (no credential)", p and p["username"] == "anonymous", (p, status))
        p, status = resolve(cookies={auth.COOKIE_NAME: token_for("admin")})
        check("real admin session still admin", p and p["role"] == "admin", (p, status))
        p, status = resolve(cookies={auth.COOKIE_NAME: "garbage"})
        check("garbage cookie -> 401", status == 401, (p, status))
    finally:
        os.environ.pop("GOVERNANCE_REQUIRE_AUTH", None)


def test_jwt_secret():
    print("\n3. JWT secret")
    old_default = "localspark-super-secret-jwt-key-2026-secure"
    check("signing key is not the old public default", auth.JWT_SECRET_KEY != old_default)
    path = os.path.join(TMP_WAREHOUSE, ".metadata", "jwt_secret")
    check("secret persisted with mode 0600", os.path.exists(path) and stat.S_IMODE(os.stat(path).st_mode) == 0o600)
    check("secret is long and random", len(auth.JWT_SECRET_KEY) >= 64)
    forged = token_for("admin", secret=old_default)
    p, status = resolve(cookies={auth.COOKIE_NAME: forged})
    check("token forged with the old public key is rejected", p is None and status == 401, (p, status))
    from web.secrets_store import load_or_create_secret
    check("secret is stable across calls", load_or_create_secret("jwt_secret") == auth.JWT_SECRET_KEY)


def test_compute_worker_auth():
    print("\n4. Compute worker token")
    from fastapi.testclient import TestClient
    from web import compute_worker
    from web.compute_auth import compute_headers, get_compute_token

    client = TestClient(compute_worker.app)
    check("/health stays public", client.get("/health").status_code == 200)
    r = client.post("/api/compute/execute", json={"query": "SELECT 1", "warehouse_id": "wh_starter"})
    check("execute without token -> 401", r.status_code == 401, r.status_code)
    r = client.post("/api/compute/execute", json={"query": "SELECT 1", "warehouse_id": "wh_starter"},
                    headers={"X-Compute-Token": "wrong"})
    check("execute with wrong token -> 401", r.status_code == 401, r.status_code)
    check("status without token -> 401", client.get("/api/compute/status").status_code == 401)
    r = client.get("/api/compute/status", headers=compute_headers())
    check("status with the studio token -> 200", r.status_code == 200, r.status_code)
    r = client.post("/api/compute/execute", json={"query": "SELECT 41 + 1 AS answer", "warehouse_id": "wh_starter"},
                    headers=compute_headers())
    check("execute with the studio token runs SQL", r.status_code == 200 and "42" in r.text, r.text[:200])
    check("token is a per-install secret", len(get_compute_token()) >= 64)

    import yaml
    compose = yaml.safe_load(open(os.path.join(BASE_DIR, "docker-compose.yml")))
    workers = {k: v for k, v in compose["services"].items() if k.startswith("compute-node")}
    check("compose defines 3 workers", len(workers) == 3, list(workers))
    check("workers publish no host ports", all("ports" not in v for v in workers.values()))
    check("workers are exposed on the compose network", all(v.get("expose") for v in workers.values()))


def test_notebook_gating():
    print("\n5. Notebook role gating")
    from fastapi.testclient import TestClient
    from web import app as app_module
    client = TestClient(app_module.app)
    token = app_module.JUPYTER_TOKEN
    bob = {auth.COOKIE_NAME: token_for("analyst_bob")}

    r = client.get("/api/notebooks/access")
    check("default: open to everyone", r.status_code == 200 and r.json()["allowed"] and r.json()["token"] == token, r.text)

    os.environ["GOVERNANCE_RESTRICT_NOTEBOOKS"] = "true"
    try:
        r = client.get("/api/notebooks/access", cookies=bob)
        body = r.json()
        check("restricted: plain user denied, no token", not body["allowed"] and body["token"] == "" and body["port"] is None, body)
        r = client.get("/api/notebooks/access", cookies={auth.COOKIE_NAME: token_for("admin")})
        check("restricted: admin allowed with token", r.json()["allowed"] and r.json()["token"] == token, r.text)
        page = client.get("/").text
        check("restricted: token not embedded in the page", token not in page)
        check("restricted: UI starts with notebooks hidden", "notebooksAllowed: false" in page)
        status = client.get("/api/status", cookies=bob).json()
        check("restricted: /api/status hides the Jupyter URL", status.get("jupyter_url") is None, status)
        status = client.get("/api/status", cookies={auth.COOKIE_NAME: token_for("admin")}).json()
        check("restricted: /api/status shows it to admin", token in (status.get("jupyter_url") or ""), status)
        r = client.get("/api/notebooks/access", cookies={auth.COOKIE_NAME: "garbage"})
        check("restricted: bad session -> 401", r.status_code == 401, r.status_code)
    finally:
        os.environ.pop("GOVERNANCE_RESTRICT_NOTEBOOKS", None)
    check("unrestricted page embeds the token as before", token in client.get("/").text)


def main():
    try:
        test_auth_never_fails_open()
        test_require_auth_mode()
        test_jwt_secret()
        test_compute_worker_auth()
        test_notebook_gating()
    finally:
        shutil.rmtree(TMP_WAREHOUSE, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Governance Phase 0 checks passed.")


if __name__ == "__main__":
    main()
