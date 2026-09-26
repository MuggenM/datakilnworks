#!/usr/bin/env python3
"""
OpenID Connect login verification: the authorization-code + PKCE flow end to end against a mock identity provider
that speaks the real protocol (discovery, authorize, token with PKCE + client auth, JWKS, userinfo; RS256 ID tokens
signed with a real RSA key) on a local port, so the studio's real HTTP/JWKS/JWT code paths run. It is a mock, not
Keycloak/Entra/Okta -- provider quirks are not covered. Runs against a throwaway WAREHOUSE_DIR.
Tests: happy path; PKCE/state/nonce enforcement; ID token validation (signature, alg, iss, aud, exp); one-time codes;
role mapping (ID token claim and userinfo fallback); account-takeover refusals (local, ldap, deleted, deactivated).
"""
import base64
import hashlib
import os
import secrets
import shutil
import sys
import tempfile
import threading
import time
from urllib.parse import parse_qs, unquote, urlparse

TMP_ROOT = tempfile.mkdtemp(prefix="oidc_auth_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP_ROOT, "warehouse")
os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for var in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY"):
    os.environ.pop(var, None)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

import jwt
import requests
import uvicorn
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm

from web import app as app_module
from web import auth, auth_frameworks, oidc_auth

FAILURES = []
PORT = 18931
ISSUER = f"http://127.0.0.1:{PORT}"
CLIENT_ID, CLIENT_SECRET = "dkw-client", "s3cret"
REDIRECT = "http://testserver/api/auth/oidc/callback"


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def make_key():
    k = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return k


class Idp:
    key = make_key()
    other_key = make_key()
    kid = "k1"
    codes = {}
    user = {"sub": "u-1", "preferred_username": "oidcalice", "name": "Alice OIDC", "email": "alice@example.com", "groups": ["LakehouseAdmins"]}
    put_groups_in_id_token = True
    # per-test knobs applied when minting the id token
    sign_key = None
    alg = "RS256"
    iss = ISSUER
    aud = CLIENT_ID
    exp_delta = 300
    nonce_override = None
    discovery_issuer = ISSUER


idp = FastAPI()


@idp.get("/.well-known/openid-configuration")
def discovery():
    return {"issuer": Idp.discovery_issuer, "authorization_endpoint": f"{ISSUER}/authorize", "token_endpoint": f"{ISSUER}/token",
            "jwks_uri": f"{ISSUER}/jwks", "userinfo_endpoint": f"{ISSUER}/userinfo",
            "token_endpoint_auth_methods_supported": ["client_secret_basic"], "id_token_signing_alg_values_supported": ["RS256"]}


@idp.get("/jwks")
def jwks():
    import json
    jwk = json.loads(RSAAlgorithm.to_jwk(Idp.key.public_key()))
    jwk.update(kid=Idp.kid, use="sig", alg="RS256")
    return {"keys": [jwk]}


@idp.get("/authorize")
def authorize(client_id: str, redirect_uri: str, state: str, nonce: str, code_challenge: str, code_challenge_method: str, scope: str, response_type: str):
    assert response_type == "code" and code_challenge_method == "S256" and client_id == CLIENT_ID
    code = secrets.token_urlsafe(16)
    Idp.codes[code] = {"challenge": code_challenge, "nonce": nonce, "redirect_uri": redirect_uri}
    return RedirectResponse(f"{redirect_uri}?code={code}&state={state}", status_code=302)


@idp.post("/token")
async def token(request: Request, code: str = Form(...), code_verifier: str = Form(...), redirect_uri: str = Form(...), grant_type: str = Form(...),
                authorization: str = Header(None)):
    expected = "Basic " + base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    if authorization != expected:
        return JSONResponse({"error": "invalid_client"}, status_code=401)
    entry = Idp.codes.pop(code, None)                       # one-time use
    if not entry or entry["redirect_uri"] != redirect_uri:
        return JSONResponse({"error": "invalid_grant"}, status_code=400)
    digest = base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest()).rstrip(b"=").decode()
    if digest != entry["challenge"]:
        return JSONResponse({"error": "invalid_grant", "error_description": "PKCE"}, status_code=400)
    now = int(time.time())
    claims = {"iss": Idp.iss, "aud": Idp.aud, "sub": Idp.user["sub"], "iat": now, "exp": now + Idp.exp_delta,
              "nonce": Idp.nonce_override or entry["nonce"], **{k: v for k, v in Idp.user.items() if k != "sub"}}
    if not Idp.put_groups_in_id_token:
        claims.pop("groups", None)
    if Idp.alg == "HS256":
        tok = jwt.encode(claims, "anything", algorithm="HS256", headers={"kid": Idp.kid})
    else:
        tok = jwt.encode(claims, Idp.sign_key or Idp.key, algorithm=Idp.alg, headers={"kid": Idp.kid})
    return {"access_token": "at-" + code, "token_type": "Bearer", "id_token": tok}


@idp.get("/userinfo")
def userinfo(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer at-"):
        raise HTTPException(401)
    return {"sub": Idp.user["sub"], **({"groups": Idp.user["groups"]} if "groups" in Idp.user else {})}


def start_idp():
    server = uvicorn.Server(uvicorn.Config(idp, host="127.0.0.1", port=PORT, log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(50):
        try:
            requests.get(f"{ISSUER}/.well-known/openid-configuration", timeout=1)
            return server
        except Exception:
            time.sleep(0.1)
    sys.exit("mock IdP did not start")


def configure(**over):
    cfg = {"enabled": True, "provider_name": "MockIdP", "issuer_url": ISSUER, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
           "scopes": "openid email profile groups", "redirect_uri": REDIRECT, "username_claim": "preferred_username",
           "admin_claim": "groups", "admin_value": "LakehouseAdmins", "power_user_value": "DataEngineers", "default_role": "user"}
    cfg.update(over)
    auth_frameworks.save_config({"oidc": cfg})
    oidc_auth._discovery_cache.clear()


def reset_idp():
    Idp.sign_key, Idp.alg, Idp.iss, Idp.aud, Idp.exp_delta = None, "RS256", ISSUER, CLIENT_ID, 300
    Idp.nonce_override, Idp.discovery_issuer, Idp.put_groups_in_id_token = None, ISSUER, True
    Idp.user = {"sub": "u-1", "preferred_username": "oidcalice", "name": "Alice OIDC", "email": "alice@example.com", "groups": ["LakehouseAdmins"]}
    oidc_auth._discovery_cache.clear()


def attempt(client, tamper_state=None, drop_cookie=False):
    """Full browser round trip; returns the callback response (redirects not followed)."""
    r = client.get("/api/auth/oidc/login", follow_redirects=False)
    if r.status_code != 302 or not r.headers["location"].startswith(ISSUER):
        return r
    cookie = r.cookies.get(oidc_auth.STATE_COOKIE)
    a = requests.get(r.headers["location"], allow_redirects=False, timeout=5)
    cb = urlparse(a.headers["location"])
    q = parse_qs(cb.query)
    if tamper_state:
        q["state"] = [tamper_state]
    client.cookies.clear()
    if cookie and not drop_cookie:
        client.cookies.set(oidc_auth.STATE_COOKIE, cookie, path="/api/auth/oidc")
    return client.get(cb.path, params={"code": q["code"][0], "state": q["state"][0]}, follow_redirects=False)


def failed(r, fragment=""):
    loc = unquote(r.headers.get("location", ""))
    return r.status_code == 302 and loc.startswith("/?sso_error=") and auth.COOKIE_NAME not in r.cookies and fragment.lower() in loc.lower()


def main():
    server = start_idp()
    try:
        client = TestClient(app_module.app)
        with auth.get_db_connection() as c:
            c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")

        print("1. Configuration & login start")
        check("no SSO button while disabled", client.get("/api/auth/sso").json()["oidc"]["enabled"] is False)
        r = client.get("/api/auth/oidc/login", follow_redirects=False)
        check("disabled: login start fails cleanly to the UI", failed(r, "not enabled"), r.headers)
        configure()
        check("public /api/auth/sso announces the provider", client.get("/api/auth/sso").json()["oidc"] == {"enabled": True, "provider_name": "MockIdP"})
        r = client.get("/api/auth/oidc/login", follow_redirects=False)
        q = parse_qs(urlparse(r.headers["location"]).query)
        check("redirects to the IdP with code flow, S256 PKCE, state and nonce",
              r.status_code == 302 and q["response_type"] == ["code"] and q["code_challenge_method"] == ["S256"] and q["state"] and q["nonce"] and q["code_challenge"], q)
        check("the client secret never appears in the browser URL", CLIENT_SECRET not in r.headers["location"])
        check("the state cookie is HttpOnly and scoped to the OIDC paths", "httponly" in r.headers["set-cookie"].lower() and "path=/api/auth/oidc" in r.headers["set-cookie"].lower(), r.headers["set-cookie"])

        print("\n2. Happy path")
        reset_idp()
        r = attempt(client)
        check("callback redirects to the app with a session cookie", r.status_code == 302 and r.headers["location"] == "/" and auth.COOKIE_NAME in r.cookies, (r.status_code, r.headers))
        me = client.get("/api/auth/me", cookies={auth.COOKIE_NAME: r.cookies.get(auth.COOKIE_NAME)}).json()
        u = auth.get_user_by_username("oidcalice")
        check("account provisioned as auth_source=oidc with the mapped admin role", u and u["auth_source"] == "oidc" and u["role"] == "admin" and u["display_name"] == "Alice OIDC", u)
        check("the session works", me.get("authenticated") is True and me["user"]["username"] == "oidcalice", me)
        check("no local password can ever log this account in", client.post("/api/auth/login", json={"username": "oidcalice", "password": "anything"}).status_code == 401)
        check("the state cookie is cleared after use", "dkw_oidc" in r.headers.get("set-cookie", ""))

        print("\n3. Role mapping")
        Idp.user = {**Idp.user, "groups": ["DataEngineers"]}
        attempt(client)
        check("power user claim value maps to power_user", auth.get_user_by_username("oidcalice")["role"] == "power_user")
        Idp.user = {**Idp.user, "groups": ["Nobody"]}
        attempt(client)
        check("no matching value gets default_role", auth.get_user_by_username("oidcalice")["role"] == "user")
        Idp.user = {**Idp.user, "groups": ["LakehouseAdmins"]}
        Idp.put_groups_in_id_token = False
        attempt(client)
        check("groups missing from the ID token are read from userinfo", auth.get_user_by_username("oidcalice")["role"] == "admin")
        reset_idp()

        print("\n3b. Group sync (platform groups mapped to a value of the groups claim)")
        from web import groups
        g_eng = groups.create_group("Engineers", "", "admin"); g_ops = groups.create_group("Ops", "", "admin")
        groups.set_mapping(g_eng["id"], "oidc", "DataEngineers", "admin"); groups.set_mapping(g_ops["id"], "oidc", "Operators", "admin")
        uid = auth.get_user_by_username("oidcalice")["id"]
        member_names = lambda gid: {m["username"]: m["origin"] for m in groups.list_members(gid)}
        Idp.user = {**Idp.user, "groups": ["DataEngineers", "Other"]}
        attempt(client)
        check("a sign-in with the mapped claim value adds the user (origin sync)", member_names(g_eng["id"]) == {"oidcalice": "sync"}, member_names(g_eng["id"]))
        check("...and not to a group whose value they do not have", member_names(g_ops["id"]) == {})
        groups.add_members(g_ops["id"], [uid], "admin")
        Idp.user = {**Idp.user, "groups": ["Nobody"]}
        attempt(client)
        check("the next sign-in without the value removes the synced membership", member_names(g_eng["id"]) == {})
        check("a manually added membership is never removed by sync", member_names(g_ops["id"]) == {"oidcalice": "manual"})
        Idp.user = {**Idp.user, "groups": ["DataEngineers"]}
        attempt(client)
        Idp.user = {k: v for k, v in Idp.user.items() if k != "groups"}
        attempt(client)
        check("no groups claim at all is not 'in no groups': synced membership stays", member_names(g_eng["id"]) == {"oidcalice": "sync"}, member_names(g_eng["id"]))
        Idp.user = {**Idp.user, "groups": []}
        attempt(client)
        check("an explicitly empty claim does remove it", member_names(g_eng["id"]) == {})
        reset_idp()

        print("\n4. State / PKCE / nonce / replay")
        check("a tampered state is refused", failed(attempt(client, tamper_state="forged"), "state"))
        check("a missing state cookie (not this browser) is refused", failed(attempt(client, drop_cookie=True), "expired"))
        Idp.nonce_override = "attacker-nonce"
        check("a wrong nonce in the ID token is refused", failed(attempt(client), "nonce"))
        Idp.nonce_override = None
        r = client.get("/api/auth/oidc/login", follow_redirects=False)
        a = requests.get(r.headers["location"], allow_redirects=False, timeout=5)
        cb = urlparse(a.headers["location"]); qq = parse_qs(cb.query)
        client.cookies.clear(); client.cookies.set(oidc_auth.STATE_COOKIE, r.cookies.get(oidc_auth.STATE_COOKIE), path="/api/auth/oidc")
        first = client.get(cb.path, params={"code": qq["code"][0], "state": qq["state"][0]}, follow_redirects=False)
        client.cookies.clear(); client.cookies.set(oidc_auth.STATE_COOKIE, r.cookies.get(oidc_auth.STATE_COOKIE), path="/api/auth/oidc")
        replay = client.get(cb.path, params={"code": qq["code"][0], "state": qq["state"][0]}, follow_redirects=False)
        check("an authorization code works once, a replay is refused", first.headers["location"] == "/" and failed(replay, "rejected"), replay.headers)
        expired = jwt.encode({"st": "x", "nn": "y", "cv": "z", "ru": REDIRECT, "purpose": "oidc-state", "exp": int(time.time()) - 5}, auth.JWT_SECRET_KEY, algorithm="HS256")
        client.cookies.clear(); client.cookies.set(oidc_auth.STATE_COOKIE, expired, path="/api/auth/oidc")
        check("an expired state cookie is refused", failed(client.get("/api/auth/oidc/callback", params={"code": "c", "state": "x"}, follow_redirects=False), "expired"))
        forged = jwt.encode({"st": "x", "nn": "y", "cv": "z", "ru": REDIRECT, "purpose": "oidc-state", "exp": int(time.time()) + 60}, "not-the-secret", algorithm="HS256")
        client.cookies.clear(); client.cookies.set(oidc_auth.STATE_COOKIE, forged, path="/api/auth/oidc")
        check("a state cookie signed with another key is refused", failed(client.get("/api/auth/oidc/callback", params={"code": "c", "state": "x"}, follow_redirects=False), "expired"))
        client.cookies.clear()

        print("\n5. ID token validation")
        for label, setup, frag in [
            ("signed by a different key", lambda: setattr(Idp, "sign_key", Idp.other_key), "verified"),
            ("HS256 (algorithm confusion)", lambda: setattr(Idp, "alg", "HS256"), "algorithm"),
            ("wrong issuer", lambda: setattr(Idp, "iss", "http://evil.example"), "verified"),
            ("wrong audience", lambda: setattr(Idp, "aud", "someone-else"), "verified"),
            ("expired", lambda: setattr(Idp, "exp_delta", -3600), "verified"),
        ]:
            reset_idp(); setup()
            check(f"ID token {label} is refused", failed(attempt(client), frag), label)
        reset_idp()
        Idp.discovery_issuer = "http://evil.example"
        check("a discovery document for another issuer is refused", failed(attempt(client), "does not match"))
        reset_idp()
        check("and the flow still works after the failures", attempt(client).headers.get("location") == "/")

        print("\n6. Account takeover refusals")
        auth.create_user("localbob", "localpassword1", "Local Bob", role="user")
        Idp.user = {**Idp.user, "preferred_username": "localbob"}
        check("an existing LOCAL username is never taken over", failed(attempt(client), "already exists") and auth.get_user_by_username("localbob")["auth_source"] == "local")
        auth.upsert_external_user("ldapcarol", "Carol", "user", "ldap")
        Idp.user = {**Idp.user, "preferred_username": "ldapcarol"}
        check("an existing LDAP account is never taken over", failed(attempt(client), "already exists") and auth.get_user_by_username("ldapcarol")["auth_source"] == "ldap")
        Idp.user = {**Idp.user, "preferred_username": "oidcalice"}
        auth.update_user(auth.get_user_by_username("oidcalice")["id"], is_active=False)
        check("an admin-deactivated OIDC account is not reactivated by logging in", failed(attempt(client), "deactivated") and not auth.get_user_by_username("oidcalice")["is_active"])
        auth.update_user(auth.get_user_by_username("oidcalice")["id"], is_active=True)
        auth.delete_user(auth.get_user_by_username("oidcalice")["id"])
        check("a deleted OIDC account is not resurrected by logging in", failed(attempt(client), "deleted"))
        Idp.user = {**Idp.user, "preferred_username": "../evil"}
        check("a username that is not path-safe is refused", failed(attempt(client), "cannot use"))
        Idp.user = {**Idp.user, "preferred_username": "", "email": "mail.only@example.com"}
        check("falls back to the email claim when the username claim is empty", attempt(client).headers.get("location") == "/" and auth.get_user_by_username("mail.only@example.com") is not None)
        Idp.user = {**Idp.user, "preferred_username": "", "email": "unverified@example.com", "email_verified": False}
        check("an unverified email is never used as an identity", failed(attempt(client), "usable username"))

        print("\n7. Provider errors")
        reset_idp()
        r = client.get("/api/auth/oidc/callback", params={"error": "access_denied", "error_description": "user said no"}, follow_redirects=False)
        check("an error from the IdP returns to the login screen", failed(r, "did not complete"), r.headers)
        configure(issuer_url="http://127.0.0.1:1")
        check("an unreachable IdP fails cleanly, not with a 500", failed(client.get("/api/auth/oidc/login", follow_redirects=False), "not reachable"))
    finally:
        server.should_exit = True
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All OIDC login checks passed.")


if __name__ == "__main__":
    main()
