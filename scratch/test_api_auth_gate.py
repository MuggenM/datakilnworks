#!/usr/bin/env python3
"""Default-deny for /api/* (`_api_auth_gate` in web/app.py) and GOVERNANCE_REQUIRE_AUTH on by default, against a throwaway
WAREHOUSE_DIR. About 170 routes used to answer without any credentials (warehouse create/delete, workspace upload, table restore,
dbt model edits, the MLflow API, ...), and every route that went through resolve_principal ran a credential-less request as the
local admin. Checks:
  - EVERY /api route in the OpenAPI schema (incl. the included routers) answers a credential-less request with the gate's 401,
    unless it is on the public list; the gate answers before the route runs, so nothing is changed by this sweep;
  - each public entry names a real route (a stale entry would hide a typo), and they are not refused by the gate;
  - X-User, guest mode, a garbage / revoked / purpose token do not get past it; a real session (cookie or bearer) does;
  - the default (variable unset) is require-auth; GOVERNANCE_REQUIRE_AUTH=false restores the single-user mode (gate off)."""
import os, re, sys, tempfile
TMP = tempfile.mkdtemp(prefix="apigate_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "IP_ALLOWLIST_OVERRIDE", "TRUSTED_PROXIES", "COOKIE_SECURE", "DKW_SECRETS_DIR", "CSRF_CHECK"):
    os.environ.pop(v, None)
os.environ["LOGIN_THROTTLE"] = "off"
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from fastapi.testclient import TestClient
from web import app as app_module, auth
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:600]}" if d and not c else ""))
    if not c: FAIL.append(n)
GATE_DETAIL = "Authentication required. Please log in."
def gated(r): return r.status_code == 401 and r.headers.get("content-type", "").startswith("application/json") and r.json().get("detail") == GATE_DETAIL

with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
auth.create_user("alice", "alicepassword1", "Alice", role="user")
anon = TestClient(app_module.app, client=("203.0.113.9", 50000))

print("the default")
check("GOVERNANCE_REQUIRE_AUTH unset means require auth", auth.governance_require_auth() is True)
for v in ("false", "0", "off", "no", "FALSE"):
    os.environ["GOVERNANCE_REQUIRE_AUTH"] = v
    check(f"'{v}' switches it off", auth.governance_require_auth() is False)
for v in ("true", "1", "yes", "on", "anything-else"):
    os.environ["GOVERNANCE_REQUIRE_AUTH"] = v
    check(f"'{v}' keeps it on", auth.governance_require_auth() is True)
os.environ.pop("GOVERNANCE_REQUIRE_AUTH")

print("every /api route without credentials")
schema = app_module.app.openapi()
ops = [(m.upper(), p) for p, item in schema["paths"].items() for m in item if m.upper() in ("GET", "POST", "PUT", "PATCH", "DELETE")]
api_ops = [(m, p) for m, p in ops if p.startswith("/api/")]
check("the schema lists the API (incl. routers)", len(api_ops) > 400 and any(p.startswith("/api/governance/") for _, p in api_ops)
      and any(p.startswith("/api/apps") for _, p in api_ops) and any(p.startswith("/api/dashboard-apps") for _, p in api_ops), len(api_ops))
open_routes, public_seen = [], set()
for m, p in api_ops:
    url = re.sub(r"\{[^}]+\}", "x", p)
    if app_module.is_public_api(m, url):
        public_seen.add((m, p)); continue
    r = anon.request(m, url, json={} if m in ("POST", "PUT", "PATCH") else None)
    if not gated(r):
        open_routes.append(f"{m} {p} -> {r.status_code}")
check(f"all {len(api_ops) - len(public_seen)} non-public routes are refused by the gate", not open_routes, open_routes[:20])

print("the public list")
paths_by_method = {(m, p) for m, p in api_ops}
def route_exists(m, path):
    return any(m == om and re.fullmatch(re.sub(r"\{[^}]+\}", "[^/]+", op), path) for om, op in paths_by_method)
stale = [f"{m} {p}" for m, p in app_module._PUBLIC_API if p not in ("/api/docs", "/api/docs/oauth2-redirect", "/api/redoc") and not route_exists(m, p)]
check("every exact public entry is a real route", not stale, stale)
for methods, rx in app_module._PUBLIC_API_PATTERNS:
    check(f"pattern {rx.pattern} matches a real route", any(m in methods and rx.fullmatch(re.sub(r"\{[^}]+\}", "x", p)) for m, p in api_ops))
check("the public list stays short", len(app_module._PUBLIC_API) + len(app_module._PUBLIC_API_PATTERNS) <= 30)
for m, p in [("GET", "/api/auth/me"), ("GET", "/api/auth/sso"), ("GET", "/api/brand/css"), ("GET", "/api/docs")]:
    r = anon.request(m, p)
    check(f"{m} {p} answers without a session", not gated(r) and r.status_code < 500, r.status_code)
r = anon.post("/api/apps/nope/query", json={"sql": "select 1"})
check("an app query reaches the route, which checks its own token", not gated(r), (r.status_code, r.text[:120]))
check("the docs pages stay public, the API spec under /api is not exposed beyond them", anon.get("/api/redoc").status_code == 200)

print("things that are not a session")
check("X-User: admin is refused", gated(anon.get("/api/dashboards", headers={"X-User": "admin"})))
check("a garbage cookie is refused", gated(TestClient(app_module.app, cookies={auth.COOKIE_NAME: "garbage"}).get("/api/dashboards")))
check("a garbage bearer token is refused", gated(anon.get("/api/dashboards", headers={"Authorization": "Bearer x.y.z"})))
alice = auth.get_user_by_username("alice")
mfa_like = auth.create_access_token(alice, extra_claims={"purpose": "mfa"})
check("a token with a purpose claim (MFA step, app data) is refused", gated(anon.get("/api/dashboards", headers={"Authorization": f"Bearer {mfa_like}"})))
with auth.get_db_connection() as c:
    c.execute("INSERT OR REPLACE INTO app_settings (key, value_json, updated_by, updated_at) VALUES ('allow_guest_mode', 'true', 'admin', '')")
check("guest mode does not open the API while auth is required", gated(anon.get("/api/dashboards")))
with auth.get_db_connection() as c:
    c.execute("UPDATE app_settings SET value_json = 'false' WHERE key = 'allow_guest_mode'")

print("a real session")
tok = auth.create_access_token(alice)
r = anon.get("/api/dashboards", headers={"Authorization": f"Bearer {tok}"})
check("a session bearer token passes", r.status_code == 200, (r.status_code, r.text[:200]))
c = TestClient(app_module.app, client=("203.0.113.9", 50001))
check("password sign-in works through the gate", c.post("/api/auth/login", json={"username": "alice", "password": "alicepassword1"}).status_code == 200)
check("the session cookie passes", c.get("/api/dashboards").status_code == 200)
check("role checks still apply behind the gate", c.get("/api/users").status_code == 403)
check("sign-out works", c.post("/api/auth/logout").status_code == 200)
check("after sign-out the cookie is refused", gated(c.get("/api/dashboards")))
check("a revoked bearer token is refused", (auth.end_session(tok, "test") or True) and gated(anon.get("/api/dashboards", headers={"Authorization": f"Bearer {tok}"})))

print("single-user mode (GOVERNANCE_REQUIRE_AUTH=false)")
os.environ["GOVERNANCE_REQUIRE_AUTH"] = "false"
r = anon.get("/api/dashboards")
check("the gate is off and the credential-less request is the local admin", r.status_code == 200, (r.status_code, r.text[:200]))
os.environ.pop("GOVERNANCE_REQUIRE_AUTH")
check("back on once the variable is unset", gated(anon.get("/api/dashboards")))

print("\nALL PASSED" if not FAIL else f"\n{len(FAIL)} FAILED: {FAIL}")
sys.exit(1 if FAIL else 0)
