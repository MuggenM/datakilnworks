#!/usr/bin/env python3
"""OpenID Connect sign-in (web/oidc_auth.py, /api/auth/oidc/*) against a REAL identity provider: the Keycloak of the CI services
stack (profile `idp`, realm dkw from ci/services/seed/keycloak-realm.json). The studio runs in-process (TestClient); `requests`
plays the browser at Keycloak: it follows the redirect, posts Keycloak's own login form and catches the redirect back.
    CI_SERVICES=idp ci/run_test.sh oidc_keycloak
Checks: discovery + PKCE S256 redirect, alice signed in with the admin role from the LakehouseAdmins group claim, group sync,
bob with the default role, a wrong password never reaches the studio, a tampered state is refused, a replayed code is refused.
KC_URL has no default (the stack's env file sets it), like every other services test."""
import html
import os
import re
import sys
import tempfile
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _services  # noqa: E402

KC_URL = os.environ.get("KC_URL", "").rstrip("/")
REALM = os.environ.get("KC_REALM", "dkw")
CLIENT_ID = os.environ.get("KC_OIDC_CLIENT_ID", "dkw-oidc")
CLIENT_SECRET = os.environ.get("KC_OIDC_CLIENT_SECRET", "")
if not _services.require_or_skip("Keycloak", *_services.host_port(KC_URL, 8080)):
    sys.exit(0)

TMP_ROOT = tempfile.mkdtemp(prefix="oidc_kc_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP_ROOT, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for var in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY"):
    os.environ.pop(var, None)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from web import app as app_module  # noqa: E402
from web import auth, auth_frameworks, groups, oidc_auth  # noqa: E402

FAIL = []
ISSUER = f"{KC_URL}/realms/{REALM}"


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:400]}" if detail and not cond else ""))
    if not cond:
        FAIL.append(name)


def at_keycloak(auth_url, username, password):
    """The browser's part: Keycloak's login page, its form, the redirect back. Returns (response of the form post, session)."""
    s = requests.Session()
    page = s.get(auth_url, timeout=20)
    m = re.search(r'<form[^>]*id="kc-form-login"[^>]*action="([^"]+)"', page.text) or re.search(r'action="([^"]*login-actions/authenticate[^"]*)"', page.text)
    if not m:
        raise AssertionError(f"no Keycloak login form (HTTP {page.status_code}): {page.text[:300]}")
    r = s.post(html.unescape(m.group(1)), data={"username": username, "password": password, "credentialId": ""},
               allow_redirects=False, timeout=20)
    return r, s


def start(client):
    """GET /api/auth/oidc/login -> (redirect URL to Keycloak, the state cookie)."""
    r = client.get("/api/auth/oidc/login", follow_redirects=False)
    return r, r.cookies.get(oidc_auth.STATE_COOKIE)


def callback(client, cookie, location, state=None):
    q = parse_qs(urlparse(location).query)
    client.cookies.clear()
    if cookie:
        client.cookies.set(oidc_auth.STATE_COOKIE, cookie, path="/api/auth/oidc")
    return client.get("/api/auth/oidc/callback", params={"code": q["code"][0], "state": state or q["state"][0]}, follow_redirects=False)


def sign_in(client, username, password):
    r, cookie = start(client)
    kc, _ = at_keycloak(r.headers["location"], username, password)
    return callback(client, cookie, kc.headers["location"]), kc, cookie


def refused(r, fragment=""):
    loc = requests.utils.unquote(r.headers.get("location", ""))
    return r.status_code == 302 and loc.startswith("/?sso_error=") and auth.COOKIE_NAME not in r.cookies and fragment.lower() in loc.lower()


def me(client, r):
    return client.get("/api/auth/me", cookies={auth.COOKIE_NAME: r.cookies.get(auth.COOKIE_NAME) or ""}).json()


client = TestClient(app_module.app)
with auth.get_db_connection() as c:
    c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
auth_frameworks.save_config({"oidc": {
    "enabled": True, "provider_name": "Keycloak", "issuer_url": ISSUER, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
    "scopes": "openid email profile", "username_claim": "preferred_username", "redirect_uri": "http://testserver/api/auth/oidc/callback",
    "admin_claim": "groups", "admin_value": "LakehouseAdmins", "default_role": "user"}})
oidc_auth._discovery_cache.clear()
gid = groups.create_group("Lakehouse admins", "", "admin")["id"]
groups.set_mapping(gid, "oidc", "LakehouseAdmins", "admin")

print("discovery and the redirect to Keycloak")
r, cookie = start(client)
loc = r.headers.get("location", "")
q = parse_qs(urlparse(loc).query)
check("the login start redirects to Keycloak's authorization endpoint", r.status_code == 302 and loc.startswith(f"{ISSUER}/protocol/openid-connect/auth"), (r.status_code, loc[:200]))
check("with the code flow and PKCE S256", q.get("response_type") == ["code"] and q.get("code_challenge_method") == ["S256"] and q.get("code_challenge"), q)
check("the client secret is not in the browser URL", CLIENT_SECRET and CLIENT_SECRET not in loc)
no_pkce = loc.split("?")[0] + "?" + "&".join(f"{k}={requests.utils.quote(v[0], safe='')}" for k, v in q.items() if not k.startswith("code_challenge"))
nr = requests.get(no_pkce, allow_redirects=False, timeout=20)
check("Keycloak itself requires PKCE for this client (a request without it is refused)",
      "error=invalid_request" in nr.headers.get("location", "") or (nr.status_code >= 400 and "kc-form-login" not in nr.text), (nr.status_code, nr.headers.get("location")))

print("alice (group LakehouseAdmins)")
r, kc, _ = sign_in(client, "alice", "alicepw")
check("Keycloak sends the browser back to the studio's callback", kc.status_code == 302 and "/api/auth/oidc/callback" in kc.headers.get("location", ""), (kc.status_code, kc.headers.get("location")))
m = me(client, r)
u = auth.get_user_by_username("alice")
check("alice_signed_in: a session cookie, the admin role from the LakehouseAdmins mapping",
      r.status_code == 302 and r.headers.get("location") == "/" and m.get("authenticated") is True and m["user"]["username"] == "alice"
      and u["role"] == "admin" and u["auth_source"] == "oidc", (r.status_code, r.headers.get("location"), m))
check("groups_synced: alice is in the mapped platform group (origin sync)",
      [(x["username"], x["origin"]) for x in groups.list_members(gid)] == [("alice", "sync")], groups.list_members(gid))

print("bob (no group)")
r, _, _ = sign_in(client, "bob", "bobpw")
m = me(client, r)
check("bob_default_role: bob signs in with the default role", m.get("authenticated") is True and m["user"]["username"] == "bob" and auth.get_user_by_username("bob")["role"] == "user", m)
check("...and is not in the mapped group", "bob" not in [x["username"] for x in groups.list_members(gid)])

print("refusals")
r, cookie = start(client)
kc, _ = at_keycloak(r.headers["location"], "bob", "WRONG")
check("a wrong password stays at Keycloak (its form again, no redirect to the studio)", kc.status_code == 200 and "kc-form-login" in kc.text, kc.status_code)
r, cookie = start(client)
kc, _ = at_keycloak(r.headers["location"], "alice", "alicepw")
r = callback(client, cookie, kc.headers["location"], state="tampered-" + parse_qs(urlparse(kc.headers["location"]).query)["state"][0])
check("state_tampered_refused: a callback with a changed state is refused", refused(r, "state"), (r.status_code, r.headers.get("location")))
r, cookie = start(client)
kc, _ = at_keycloak(r.headers["location"], "alice", "alicepw")
first = callback(client, cookie, kc.headers["location"])
again = callback(client, cookie, kc.headers["location"])
check("the first use of the code signs in", auth.COOKIE_NAME in first.cookies, (first.status_code, first.headers.get("location")))
check("code_replay_refused: the same code a second time is refused", refused(again), (again.status_code, again.headers.get("location")))

print()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS")
sys.stdout.flush()
os._exit(1 if FAIL else 0)
