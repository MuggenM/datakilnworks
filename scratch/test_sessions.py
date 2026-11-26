#!/usr/bin/env python3
"""Server-side sessions (web/sessions.py) against a throwaway WAREHOUSE_DIR: every session JWT names a row in auth.db, so signing out,
"sign out everywhere else", an administrator's "end all sessions", a password change/reset, deactivation, deletion and an idle timeout
really end a session, also for a copy of the token taken before. Also: a token without `sid` (pre-upgrade) is refused, a token signed
with another key is refused even with a real sid, the gate middlewares see a revoked session as no session, nothing in the session
list is a credential, and only a hash of the session secret is stored."""
import os, shutil, sys, tempfile, time, datetime, hashlib
TMP = tempfile.mkdtemp(prefix="sess_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "IP_ALLOWLIST_OVERRIDE", "TRUSTED_PROXIES", "COOKIE_SECURE", "SESSION_IDLE_MINUTES", "DKW_SECRETS_DIR"):
    os.environ.pop(v, None)
os.environ["LOGIN_THROTTLE"] = "off"
os.environ["GOVERNANCE_REQUIRE_AUTH"] = "true"
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import jwt
from fastapi.testclient import TestClient
from web import app as app_module, auth, sessions
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def client(ua="pytest-browser-A"): return TestClient(app_module.app, client=("203.0.113.7", 50000), headers={"user-agent": ua})
def login(c, user, pw): return c.post("/api/auth/login", json={"username": user, "password": pw})
def _me(r): return 200 if r.status_code == 200 and r.json().get("authenticated") else 401     # /api/auth/me answers 200 either way
def bearer(tok): return _me(TestClient(app_module.app).get("/api/auth/me", headers={"Authorization": f"Bearer {tok}"}))
def me(c): return _me(c.get("/api/auth/me"))
def cookie_of(c): return c.cookies.get(auth.COOKIE_NAME)
try:
    with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
    auth.create_user("alice", "alicepassword1", "Alice", role="user")
    auth.create_user("bob", "bobpassword1", "Bob", role="user")

    print("a session is a server-side row")
    a1 = client("Mozilla/5.0 (X11; Linux x86_64) Chrome/130.0"); check("alice signs in", login(a1, "alice", "alicepassword1").status_code == 200)
    tok1 = cookie_of(a1); claims = jwt.decode(tok1, options={"verify_signature": False})
    check("the token carries a sid", bool(claims.get("sid")), claims)
    with auth.get_db_connection() as c:
        rows = c.execute("SELECT * FROM sessions WHERE user_id = ?", (claims["sub"],)).fetchall()
    check("one row for it", len(rows) == 1, len(rows))
    check("only a hash of the session secret is stored", rows[0]["secret_hash"] == hashlib.sha256(claims["sid"].encode()).hexdigest()
          and claims["sid"] not in [str(v) for v in dict(rows[0]).values()])
    check("address and user agent are recorded", rows[0]["ip"] == "203.0.113.7" and "Chrome" in rows[0]["user_agent"], dict(rows[0]))
    check("method is recorded", rows[0]["method"] == "password", rows[0]["method"])

    print("logout really ends the session")
    stolen = tok1
    check("a copy of the token works before logout", bearer(stolen) == 200)
    check("logout answers 200", a1.post("/api/auth/logout").status_code == 200)
    check("the copy no longer works after logout", bearer(stolen) == 401)
    a1.cookies.set(auth.COOKIE_NAME, stolen)
    check("nor as a cookie", me(a1) == 401)

    print("tokens that are not sessions")
    user = auth.get_user_by_username("alice")
    now = datetime.datetime.now(datetime.timezone.utc)
    legacy = jwt.encode({"sub": user["id"], "iat": int(now.timestamp()), "exp": now + datetime.timedelta(hours=1)}, auth.JWT_SECRET_KEY, algorithm="HS256")
    check("a token without sid (issued before server-side sessions) is refused", bearer(legacy) == 401)
    made_up = jwt.encode({"sub": user["id"], "sid": "made-up", "iat": int(now.timestamp()), "exp": now + datetime.timedelta(hours=1)}, auth.JWT_SECRET_KEY, algorithm="HS256")
    check("a token with an unknown sid is refused", bearer(made_up) == 401)
    real = auth.create_access_token(user)
    p = jwt.decode(real, auth.JWT_SECRET_KEY, algorithms=["HS256"])
    check("a real session token works", bearer(real) == 200)
    check("the same claims signed with another key are refused", bearer(jwt.encode(p, "not-the-key", algorithm="HS256")) == 401)
    bob = auth.get_user_by_username("bob")
    swapped = dict(p, sub=bob["id"])
    check("alice's sid under bob's subject is refused", bearer(jwt.encode(swapped, auth.JWT_SECRET_KEY, algorithm="HS256")) == 401)

    print("the session list and revoking one session")
    a2 = client("Mozilla/5.0 (Macintosh; Mac OS X 14_0) Safari/605.1"); login(a2, "alice", "alicepassword1")
    a3 = client("curl/8.5.0"); login(a3, "alice", "alicepassword1")
    r = a2.get("/api/auth/sessions"); lst = r.json()["sessions"]
    check("the list shows the live sessions only", r.status_code == 200 and len(lst) == 3, [s["id"] for s in lst])   # `real` above + a2 + a3
    check("exactly one is marked current", sum(1 for s in lst if s["current"]) == 1)
    check("nothing in the list is a credential", not any(k in s for s in lst for k in ("secret_hash", "sid", "token")), lst[0].keys())
    a3_id = next(s["id"] for s in lst if "curl" in (s["user_agent"] or ""))
    check("a session id alone is not a credential", bearer(a3_id) == 401)
    r = a2.post(f"/api/auth/sessions/{a3_id}/revoke")
    check("alice ends her curl session", r.status_code == 200 and r.json()["current"] is False, r.text)
    check("that session is signed out", me(a3) == 401)
    check("her current one still works", me(a2) == 200)
    b = client(); login(b, "bob", "bobpassword1")
    bob_id = b.get("/api/auth/sessions").json()["sessions"][0]["id"]
    check("alice cannot end bob's session (404)", a2.post(f"/api/auth/sessions/{bob_id}/revoke").status_code == 404)
    check("bob is still signed in", me(b) == 200)
    ended = a2.get("/api/auth/sessions?include_ended=true").json()["sessions"]
    check("include_ended shows the ended ones with a reason", any(s["state"] == "revoked" and s["revoked_reason"] for s in ended))

    print("sign out everywhere else")
    a4 = client("ua-4"); login(a4, "alice", "alicepassword1")
    r = a2.post("/api/auth/sessions/revoke-others")
    check("ends the others", r.status_code == 200 and r.json()["ended"] >= 2, r.text)
    check("the other browser is signed out", me(a4) == 401)
    check("the token made by create_access_token too", bearer(real) == 401)
    check("the current one stays", me(a2) == 200)
    r = a2.post(f"/api/auth/sessions/{a2.get('/api/auth/sessions').json()['sessions'][0]['id']}/revoke")
    check("ending the current session is a sign-out (cookie cleared)", r.status_code == 200 and r.json()["current"] is True and me(a2) == 401, r.text)

    print("administrator: end all sessions")
    adm = client(); login(adm, "admin", "adminpassword123")
    check("admin sees bob's sessions", len(adm.get(f"/api/users/{bob['id']}/sessions").json()["sessions"]) == 1)
    a5 = client(); login(a5, "alice", "alicepassword1")
    check("alice cannot list bob's sessions", a5.get(f"/api/users/{bob['id']}/sessions").status_code == 403)
    check("alice cannot end bob's sessions", a5.post(f"/api/users/{bob['id']}/sessions/revoke").status_code == 403)
    r = adm.post(f"/api/users/{bob['id']}/sessions/revoke")
    check("admin ends every session of bob", r.status_code == 200 and r.json()["ended"] == 1, r.text)
    check("bob is signed out", me(b) == 401)
    check("bob can sign in again", login(b, "bob", "bobpassword1").status_code == 200 and me(b) == 200)
    from web.governance import store
    acts = [r["action"] for r in store.list_audit(action=None, limit=500) if str(r.get("action", "")).startswith("SESSION_")]
    check("revocations are audited", {"SESSION_REVOKE", "SESSION_REVOKE_OTHERS", "SESSION_REVOKE_ALL"} <= set(acts), acts)

    print("password change / reset, deactivation, deletion")
    b2 = client(); login(b2, "bob", "bobpassword1")
    old_b = cookie_of(b)
    r = b2.post("/api/auth/change-password", json={"current_password": "bobpassword1", "new_password": "bobpassword2"})
    check("bob changes his password", r.status_code == 200, r.text)
    check("the browser that changed it stays signed in (fresh session)", me(b2) == 200)
    check("his other session ended", bearer(old_b) == 401)
    old_b2 = cookie_of(b2)
    auth.reset_user_password(bob["id"], "adminchosen1")
    check("an administrator's reset ends every session", bearer(old_b2) == 401)
    a6 = client(); login(a6, "alice", "alicepassword1"); tok6 = cookie_of(a6)
    auth.update_user(user["id"], is_active=False)
    with auth.get_db_connection() as c:
        live = c.execute("SELECT COUNT(*) FROM sessions WHERE user_id = ? AND revoked_at IS NULL", (user["id"],)).fetchone()[0]
    check("deactivation revokes the rows (not only the is_active check)", live == 0, live)
    auth.update_user(user["id"], is_active=True)
    check("reactivating does not bring the old session back", bearer(tok6) == 401)
    a7 = client(); login(a7, "alice", "alicepassword1"); tok7 = cookie_of(a7)
    auth.delete_user(user["id"]); auth.restore_user(user["id"])
    check("deleting (then restoring) ends the session", bearer(tok7) == 401)

    print("gate middlewares see a revoked session as no session")
    with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 1 WHERE username = 'alice'")
    a8 = client(); login(a8, "alice", "alicepassword1"); tok8 = cookie_of(a8)
    r = a8.get("/api/catalogs")
    check("live session + must_change_password: the gate answers 403", r.status_code == 403 and r.json().get("must_change_password"), r.status_code)
    a8.post("/api/auth/logout")
    a8.cookies.set(auth.COOKIE_NAME, tok8)
    r = a8.get("/api/catalogs")
    check("revoked session: the gate steps aside and the auth layer answers 401", r.status_code == 401, (r.status_code, r.text[:120]))
    with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'alice'")

    print("idle timeout")
    a9 = client(); login(a9, "alice", "alicepassword1"); tok9 = cookie_of(a9)
    sid9 = jwt.decode(tok9, options={"verify_signature": False})["sid"]
    with auth.get_db_connection() as c:
        c.execute("UPDATE sessions SET last_seen_at = last_seen_at - 7200 WHERE secret_hash = ?", (hashlib.sha256(sid9.encode()).hexdigest(),))
    check("without SESSION_IDLE_MINUTES an unused session stays valid", bearer(tok9) == 200)
    with auth.get_db_connection() as c:
        c.execute("UPDATE sessions SET last_seen_at = last_seen_at - 7200 WHERE secret_hash = ?", (hashlib.sha256(sid9.encode()).hexdigest(),))
    os.environ["SESSION_IDLE_MINUTES"] = "60"
    check("with SESSION_IDLE_MINUTES=60 a session unused for 2 h is refused", bearer(tok9) == 401)
    with auth.get_db_connection() as c:
        row = c.execute("SELECT revoked_reason FROM sessions WHERE secret_hash = ?", (hashlib.sha256(sid9.encode()).hexdigest(),)).fetchone()
    check("and recorded as idle", row and row[0] == "idle", row and row[0])
    a10 = client(); login(a10, "alice", "alicepassword1")
    check("a session in use stays valid under the idle timeout", me(a10) == 200)
    check("the list reports the idle timeout", a10.get("/api/auth/sessions").json()["idle_minutes"] == 60)
    os.environ.pop("SESSION_IDLE_MINUTES")

    print("expiry")
    short = auth.create_access_token(auth.get_user_by_username("alice"), expires_delta=datetime.timedelta(seconds=1))
    time.sleep(1.5)
    check("an expired session is refused", bearer(short) == 401)

    print("SAML Single Logout ends the matching sessions")
    saml_user = auth.get_user_by_username("alice")
    t1 = auth.create_access_token(saml_user, extra_claims={"saml_name_id": "alice@idp", "saml_session_index": "idx-1"}, method="saml")
    t2 = auth.create_access_token(saml_user, extra_claims={"saml_name_id": "alice@idp", "saml_session_index": "idx-2"}, method="saml")
    check("both SAML sessions work", bearer(t1) == 200 and bearer(t2) == 200)
    check("revoke_saml with a SessionIndex ends only that one", sessions.revoke_saml("alice@idp", "idx-1") == 1 and bearer(t1) == 401 and bearer(t2) == 200)
    check("revoke_saml without a SessionIndex ends every session of the NameID", sessions.revoke_saml("alice@idp") == 1 and bearer(t2) == 401)
finally:
    shutil.rmtree(TMP, ignore_errors=True)
print(f"\n{'ALL PASSED' if not FAIL else f'{len(FAIL)} FAILED: {FAIL}'}")
sys.exit(1 if FAIL else 0)
