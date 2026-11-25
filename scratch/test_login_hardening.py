#!/usr/bin/env python3
"""Password sign-in hardening against a throwaway WAREHOUSE_DIR: brute-force throttle (web/login_throttle.py: per username+address
with backoff, per address against spraying, no lockout of the account itself), equal work for unknown and known usernames, the
session token only in the cookie, `Secure` on https / a trusted proxy's X-Forwarded-Proto / COOKIE_SECURE, and the security headers."""
import os, shutil, sys, tempfile, time
TMP = tempfile.mkdtemp(prefix="lhard_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "IP_ALLOWLIST_OVERRIDE", "TRUSTED_PROXIES", "COOKIE_SECURE", "LOGIN_THROTTLE"):
    os.environ.pop(v, None)
os.environ["LOGIN_THROTTLE_BASE_SECONDS"] = "2"
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from fastapi.testclient import TestClient
from web import app as app_module, auth, login_throttle
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def from_(ip, base_url="http://testserver"): return TestClient(app_module.app, client=(ip, 50000), base_url=base_url)
def login(c, user, pw, **h): return c.post("/api/auth/login", json={"username": user, "password": pw}, headers=h)
try:
    with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
    auth.create_user("alice", "alicepassword1", "Alice", role="user")
    auth.create_user("bob", "bobpassword1", "Bob", role="user")

    print("session token")
    c = from_("203.0.113.10"); r = login(c, "alice", "alicepassword1")
    check("a correct password signs in", r.status_code == 200 and r.json()["user"]["username"] == "alice", r.text)
    check("the session token is not in the response body", "token" not in r.json(), list(r.json()))
    check("the cookie still carries the session", c.get("/api/auth/me").status_code == 200)
    sc = r.headers.get("set-cookie", "")
    check("over plain http the cookie is HttpOnly and not Secure", "httponly" in sc.lower() and "secure" not in sc.lower(), sc)

    print("throttle")
    login_throttle.reset()
    a = from_("198.51.100.20")
    codes = [login(a, "alice", f"wrong{i}").status_code for i in range(5)]
    check("the first 5 wrong passwords answer 401", codes == [401] * 5, codes)
    r = login(a, "alice", "alicepassword1")
    check("then even the right password is refused with 429 + Retry-After", r.status_code == 429 and int(r.headers.get("retry-after", "0")) > 0, (r.status_code, r.headers.get("retry-after")))
    check("another address can still sign alice in (no account lockout)", login(from_("198.51.100.21"), "alice", "alicepassword1").status_code == 200)
    check("the same address can still sign in another user", login(a, "bob", "bobpassword1").status_code == 200)
    time.sleep(2.2)
    check("after the backoff the right password works again", login(a, "alice", "alicepassword1").status_code == 200)
    codes = [login(a, "alice", f"again{i}").status_code for i in range(5)]
    check("a success cleared the counter (5 more failures before throttling)", codes == [401] * 5, codes)
    login(a, "alice", "x"); time.sleep(2.2)
    r1 = login(a, "alice", "y"); r2 = login(a, "alice", "z")
    check("backoff grows with further failures", r1.status_code == 401 and r2.status_code == 429 and int(r2.headers["retry-after"]) >= 3, (r1.status_code, r2.status_code, r2.headers.get("retry-after")))
    login_throttle.reset()
    s = from_("192.0.2.50")
    codes = [login(s, f"user{i}", "Spring2026!").status_code for i in range(30)]
    check("password spraying: 30 unknown usernames from one address answer 401", set(codes) == {401}, set(codes))
    check("the 31st attempt from that address is throttled, whatever the username", login(s, "user99", "x").status_code == 429)
    check("...even for a valid account", login(s, "alice", "alicepassword1").status_code == 429)
    login_throttle.reset()
    os.environ["LOGIN_THROTTLE"] = "off"
    check("LOGIN_THROTTLE=off disables it", all(login(a, "alice", "bad").status_code == 401 for _ in range(8)))
    os.environ.pop("LOGIN_THROTTLE"); login_throttle.reset()

    print("timing")
    t = from_("198.51.100.99")
    def avg(user, n=6):
        ts = []
        for i in range(n):
            t0 = time.perf_counter(); login(t, user, f"nope{i}"); ts.append(time.perf_counter() - t0)
            login_throttle.reset()
        return sorted(ts)[n // 2]
    known, unknown = avg("alice"), avg("nobody-here")
    check("an unknown username takes about as long as a wrong password", unknown > known * 0.5, f"known={known*1000:.1f}ms unknown={unknown*1000:.1f}ms")

    print("Secure cookie and headers")
    r = login(from_("203.0.113.11", base_url="https://testserver"), "bob", "bobpassword1")
    check("over https the cookie is Secure", "secure" in r.headers.get("set-cookie", "").lower(), r.headers.get("set-cookie"))
    check("over https HSTS is sent", "max-age" in r.headers.get("strict-transport-security", ""))
    r = login(from_("10.9.9.9"), "bob", "bobpassword1", **{"X-Forwarded-Proto": "https", "X-Forwarded-For": "203.0.113.12"})
    check("X-Forwarded-Proto from an untrusted peer is ignored", "secure" not in r.headers.get("set-cookie", "").lower() and "strict-transport-security" not in r.headers)
    os.environ["TRUSTED_PROXIES"] = "10.9.9.0/24"
    r = login(from_("10.9.9.9"), "bob", "bobpassword1", **{"X-Forwarded-Proto": "https", "X-Forwarded-For": "203.0.113.12"})
    check("X-Forwarded-Proto https from a trusted proxy makes it Secure", "secure" in r.headers.get("set-cookie", "").lower(), r.headers.get("set-cookie"))
    os.environ.pop("TRUSTED_PROXIES")
    os.environ["COOKIE_SECURE"] = "true"
    check("COOKIE_SECURE=true forces it on plain http", "secure" in login(from_("203.0.113.13"), "bob", "bobpassword1").headers.get("set-cookie", "").lower())
    os.environ["COOKIE_SECURE"] = "false"
    check("COOKIE_SECURE=false turns it off even on https", "secure" not in login(from_("203.0.113.14", base_url="https://testserver"), "bob", "bobpassword1").headers.get("set-cookie", "").lower())
    os.environ.pop("COOKIE_SECURE")
    for path in ("/", "/api/auth/me", "/healthz"):
        h = from_("203.0.113.15").get(path).headers
        check(f"security headers on {path}", h.get("x-content-type-options") == "nosniff" and h.get("x-frame-options") == "SAMEORIGIN"
              and h.get("referrer-policy") == "same-origin" and "camera=()" in h.get("permissions-policy", ""), dict(h))
    check("no HSTS over plain http", "strict-transport-security" not in from_("203.0.113.15").get("/").headers)
finally:
    shutil.rmtree(TMP, ignore_errors=True)
print(f"\n{len(FAIL)} failure(s)" if FAIL else "\nALL PASSED")
sys.exit(1 if FAIL else 0)
