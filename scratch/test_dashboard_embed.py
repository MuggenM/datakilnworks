#!/usr/bin/env python3
"""/embed/dashboard/{id} against a throwaway WAREHOUSE_DIR: the route used to return the whole dashboard definition (widget SQL
included) to anyone who knew an id, with no sign-in. Now it needs a real session with viewer rights, also when
GOVERNANCE_REQUIRE_AUTH is off (where a request without credentials is otherwise treated as the local admin), it never puts the
definition into the page, a dashboard the caller cannot see is a 404 like an unknown one, and the theme is a fixed choice."""
import os, re, sys, tempfile
TMP = tempfile.mkdtemp(prefix="embed_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "IP_ALLOWLIST_OVERRIDE", "TRUSTED_PROXIES", "COOKIE_SECURE", "DKW_SECRETS_DIR", "CSP_MODE"):
    os.environ.pop(v, None)
os.environ["LOGIN_THROTTLE"] = "off"
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from fastapi.testclient import TestClient
from web import app as app_module, auth
from web.dashboards import load_dashboards_store, save_dashboards_store
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def client(): return TestClient(app_module.app, client=("203.0.113.7", 50000))
def signed_in(user, pw):
    c = client()
    assert c.post("/api/auth/login", json={"username": user, "password": pw}).status_code == 200
    return c

SECRET_SQL = "SELECT 'embed-marker-7f3a' AS revenue_by_secret_region"
with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
auth.create_user("alice", "alicepassword1", "Alice", role="user")
auth.create_user("bob", "bobpassword1", "Bob", role="user")
alice, bob, admin = signed_in("alice", "alicepassword1"), signed_in("bob", "bobpassword1"), signed_in("admin", "adminpassword123")

r = alice.post("/api/dashboards", json={"name": "Alice's <b>private</b> board"})
check("alice creates a dashboard", r.status_code == 200, r.text)
did = r.json()["id"]
boards = load_dashboards_store()
for d in boards:
    if d["id"] == did:
        d["widgets"] = [{"id": "w1", "title": "Secret", "type": "table", "width": "col-span-4", "query": SECRET_SQL}]
save_dashboards_store(boards)

print("without a session")
anon = client()
r = anon.get(f"/embed/dashboard/{did}")
check("refused with 401 (GOVERNANCE_REQUIRE_AUTH unset: no local-admin fallback here)", r.status_code == 401, r.status_code)
check("the refusal carries no dashboard data", "embed-marker" not in r.text and "private" not in r.text)
check("an unknown id answers the same 401 (no existence oracle)", anon.get("/embed/dashboard/nope").status_code == 401)
anon.cookies.set(auth.COOKIE_NAME, "not-a-token")
check("a garbage cookie is refused too", anon.get(f"/embed/dashboard/{did}").status_code == 401)

print("with a session")
r = alice.get(f"/embed/dashboard/{did}")
check("the owner gets the page", r.status_code == 200, r.status_code)
check("the page does not contain the widget SQL", "embed-marker" not in r.text and "revenue_by_secret_region" not in r.text)
check("the name is escaped in the title", "&lt;b&gt;private&lt;/b&gt;" in r.text and "<b>private</b>" not in r.text)
csp = r.headers.get("content-security-policy", "")
nonce = re.search(r"'nonce-([^']+)'", csp)
check("the page has the embed CSP with a nonce", bool(nonce) and f'nonce="{nonce.group(1)}"' in r.text, csp)
check("its data comes from the governed dashboard API", "/api/dashboards/" in r.text and "/api/query/execute" not in r.text)
check("no innerHTML in the embed script", "innerHTML" not in r.text)
d = alice.get(f"/api/dashboards/{did}").json()
check("the API it calls returns the widget result for the owner",
      any(w.get("result", {}).get("rows") == [{"revenue_by_secret_region": "embed-marker-7f3a"}] for w in d.get("widgets", [])), d.get("widgets"))

print("a dashboard the caller may not see")
r = bob.get(f"/embed/dashboard/{did}")
check("another user gets 404, like an unknown id", r.status_code == 404, r.status_code)
check("and no data", "embed-marker" not in r.text and "private" not in r.text)
check("an unknown id is 404 for a signed-in user", bob.get("/embed/dashboard/nope").status_code == 404)
check("the admin gets the page", admin.get(f"/embed/dashboard/{did}").status_code == 200)

print("theme")
r = alice.get(f"/embed/dashboard/{did}", params={"theme": "dark"})
check("theme=dark is honoured", 'data-theme="dark"' in r.text)
r = alice.get(f"/embed/dashboard/{did}", params={"theme": "x';alert(1)//"})
check("any other theme becomes auto", 'data-theme="auto"' in r.text and "alert(1)" not in r.text)

print("signing out ends access")
alice.post("/api/auth/logout")
check("after logout the embed is refused", alice.get(f"/embed/dashboard/{did}").status_code == 401)

print("\nALL PASSED" if not FAIL else f"\n{len(FAIL)} FAILED: {FAIL}")
sys.exit(1 if FAIL else 0)
