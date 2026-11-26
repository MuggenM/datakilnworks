#!/usr/bin/env python3
"""Cross-site request refusal (`_csrf_origin_gate` / `csrf_verdict` in web/app.py) against a throwaway WAREHOUSE_DIR: a state-changing
request that carries the session cookie must come from the studio's own origin (Sec-Fetch-Site, then Origin, then Referer);
bearer-token and cookie-less requests are untouched; GET is untouched; CSRF_TRUSTED_ORIGINS and CSRF_CHECK=off."""
import os, shutil, sys, tempfile
TMP = tempfile.mkdtemp(prefix="csrf_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "CSRF_CHECK", "CSRF_TRUSTED_ORIGINS", "LOGIN_THROTTLE"):
    os.environ.pop(v, None)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from fastapi.testclient import TestClient
from web import app as app_module, auth
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
V = app_module.csrf_verdict
try:
    print("verdict")
    H = {"host": "dkw.example:8443"}
    check("same-origin fetch metadata passes", V("POST", "/api/x", {**H, "sec-fetch-site": "same-origin"}, True) is None)
    check("a user-typed / bookmarked request (none) passes", V("POST", "/api/x", {**H, "sec-fetch-site": "none"}, True) is None)
    check("cross-site fetch metadata is refused", V("POST", "/api/x", {**H, "sec-fetch-site": "cross-site", "origin": "https://evil.test"}, True))
    check("a sibling subdomain (same-site for SameSite=Lax) is refused", V("POST", "/api/x", {**H, "sec-fetch-site": "same-site", "origin": "https://other.example:8443"}, True))
    check("cross-site without Origin is still refused", V("DELETE", "/api/x", {**H, "sec-fetch-site": "cross-site"}, True))
    check("no fetch metadata: matching Origin passes", V("PUT", "/api/x", {**H, "origin": "https://dkw.example:8443"}, True) is None)
    check("no fetch metadata: another Origin is refused", V("PUT", "/api/x", {**H, "origin": "https://evil.test"}, True))
    check("Origin: null (sandboxed frame, data: URL) is refused", V("POST", "/api/x", {**H, "origin": "null"}, True))
    check("a port mismatch is another origin", V("POST", "/api/x", {**H, "origin": "https://dkw.example:9999"}, True))
    check("no Origin: matching Referer passes", V("PATCH", "/api/x", {**H, "referer": "https://dkw.example:8443/#sql"}, True) is None)
    check("no Origin: foreign Referer is refused", V("PATCH", "/api/x", {**H, "referer": "https://evil.test/page"}, True))
    check("no browser headers at all (curl, scripts) passes", V("POST", "/api/x", H, True) is None)
    check("GET is never checked", V("GET", "/api/x", {**H, "sec-fetch-site": "cross-site"}, True) is None)
    check("without the session cookie nothing is checked (bearer-token clients)", V("POST", "/scim/v2/Users", {**H, "sec-fetch-site": "cross-site"}, False) is None)
    check("the SAML ACS (the IdP's cross-site POST) is exempt", V("POST", "/api/auth/saml/acs", {**H, "sec-fetch-site": "cross-site"}, True) is None)
    os.environ["CSRF_TRUSTED_ORIGINS"] = "https://portal.example"
    check("CSRF_TRUSTED_ORIGINS adds an origin", V("POST", "/api/x", {**H, "origin": "https://portal.example"}, True) is None)
    os.environ.pop("CSRF_TRUSTED_ORIGINS")
    os.environ["CSRF_CHECK"] = "off"
    check("CSRF_CHECK=off disables it", V("POST", "/api/x", {**H, "sec-fetch-site": "cross-site", "origin": "https://evil.test"}, True) is None)
    os.environ.pop("CSRF_CHECK")

    print("through the app")
    with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
    c = TestClient(app_module.app)
    check("sign in", c.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"}).status_code == 200)
    EVIL = {"Origin": "https://evil.test", "Sec-Fetch-Site": "cross-site"}
    r = c.post("/api/auth/change-password", json={"current_password": "adminpassword123", "new_password": "pwned-by-csrf-123"}, headers=EVIL)
    check("a cross-site password change with the cookie is refused", r.status_code == 403 and r.json().get("csrf") is True, r.text)
    check("...and the password is unchanged", TestClient(app_module.app).post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"}).status_code == 200)
    r = c.post("/api/auth/logout", headers=EVIL)
    check("a cross-site logout is refused too", r.status_code == 403 and c.get("/api/auth/me").status_code == 200, r.text)
    check("a cross-site GET still works (reads are not state changes)", c.get("/api/auth/me", headers=EVIL).status_code == 200)
    r = c.post("/api/auth/logout", headers={"Origin": "http://testserver", "Sec-Fetch-Site": "same-origin"})
    check("the studio's own page can log out", r.status_code == 200, r.text)
finally:
    shutil.rmtree(TMP, ignore_errors=True)
print(f"\n{len(FAIL)} failure(s)" if FAIL else "\nALL PASSED")
sys.exit(1 if FAIL else 0)
