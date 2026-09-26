#!/usr/bin/env python3
"""SAML against a REAL identity provider (a throwaway Keycloak, /usr/bin/python3 + Playwright): metadata import through the settings UI, the login
button, the redirect to Keycloak's login page, its signed response, provisioning, role mapping and group sync. Prepared by the caller: Keycloak on
localhost:8180 (realm dkw, users alice/alicepw in group LakehouseAdmins and bob/bobpw, SAML client urn:dkw:keycloak-test with ACS
http://localhost:8117/api/auth/saml/acs, signed assertions, attributes username + groups) and a throwaway studio on localhost:8117:
    GIT_UI_URL=http://localhost:8117 KC_DESCRIPTOR=http://gtestkc:8080/realms/dkw/protocol/saml/descriptor python scratch/verify_saml_keycloak.py"""
import os, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/"); DESC = os.environ["KC_DESCRIPTOR"]
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
with sync_playwright() as p:
    b = p.chromium.launch()
    actx = b.new_context(viewport={"width": 1500, "height": 1300}); assert actx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok
    page = actx.new_page(); errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)

    print("settings: import the IdP's metadata")
    page.evaluate("async () => { const d = Alpine.$data(document.body); await d.openSettingsModal(); }"); time.sleep(1)
    page.locator("button:has-text('Authentication'):visible").first.click(); time.sleep(0.8)
    page.locator("button:has-text('SAML'):visible").first.click(); time.sleep(0.5)
    page.evaluate("() => { const d = Alpine.$data(document.body); d.authConfig.saml.idp_metadata_url = %r; d.authConfig.saml.entity_id = 'urn:dkw:keycloak-test'; d.authConfig.saml.sp_base_url = %r; }" % (DESC, BASE))
    time.sleep(0.5)
    btn = page.locator("[data-testid=saml-import]:visible"); btn.wait_for(timeout=8000); btn.click(); page.locator("[data-testid=saml-import-msg]:visible").wait_for(timeout=15000)
    cfg = page.evaluate("() => JSON.stringify(Alpine.$data(document.body).authConfig.saml)")
    import json; cfg = json.loads(cfg)
    check("entity id, SSO URL and certificate were filled from Keycloak's metadata", cfg["idp_entity_id"].endswith("/realms/dkw") and cfg["sso_url"].endswith("/realms/dkw/protocol/saml") and "BEGIN CERTIFICATE" in cfg["x509_cert"], {k: str(v)[:60] for k, v in cfg.items()})
    page.evaluate("() => { const d = Alpine.$data(document.body); Object.assign(d.authConfig.saml, { enabled: true, attribute_username: 'username', attribute_groups: 'groups', admin_value: 'LakehouseAdmins', provider_name: 'Keycloak' }); }")
    page.evaluate("async () => { await Alpine.$data(document.body).saveAuthConfig(); }"); time.sleep(1.5)
    check("the configuration is saved", actx.request.get(f"{BASE}/api/auth/sso").json()["saml"] == {"enabled": True, "provider_name": "Keycloak"})
    meta = actx.request.get(f"{BASE}/api/auth/saml/metadata")
    check("SP metadata is available for the IdP's administrator", meta.ok and "urn:dkw:keycloak-test" in meta.text() and f"{BASE}/api/auth/saml/acs" in meta.text(), meta.text()[:200])
    # a platform group mapped to the SAML value
    gid = actx.request.post(f"{BASE}/api/groups", data={"name": "Lakehouse admins"}).json()["id"]
    check("map a platform group to the SAML value", actx.request.put(f"{BASE}/api/groups/{gid}/mapping", data={"source": "saml", "external_ref": "LakehouseAdmins"}).ok)

    print("signing in through Keycloak")
    def sso_login(user, pw):
        ctx = b.new_context(viewport={"width": 1300, "height": 900}); pg = ctx.new_page()
        pg.goto(BASE, wait_until="networkidle"); time.sleep(1.5)
        return ctx, pg
    ctx, pg = sso_login("alice", "alicepw")
    check("the login page shows the SAML button with the configured label", pg.locator("[data-testid=saml-login]:visible").inner_text().strip() == "Sign in with Keycloak")
    pg.locator("[data-testid=saml-login]:visible").click(); pg.wait_for_selector("input[name=username]", timeout=20000)
    check("the browser was sent to Keycloak's login page", "localhost:8180" in pg.url and "SAMLRequest" in pg.url or "auth" in pg.url, pg.url[:120])
    pg.fill("input[name=username]", "alice"); pg.fill("input[name=password]", "alicepw"); pg.click("input[type=submit], button[type=submit]")
    pg.wait_for_url(f"{BASE}/**", timeout=30000); time.sleep(1.5)
    me = ctx.request.get(f"{BASE}/api/auth/me").json()
    check("Keycloak's signed response was accepted and a session started", me.get("authenticated") is True and me["user"]["username"] == "alice", me)
    check("the account is an SAML account with the role from the groups attribute", me["user"]["role"] == "admin" and next(u for u in actx.request.get(f"{BASE}/api/users").json()["users"] if u["username"] == "alice")["auth_source"] == "saml", me)
    members = actx.request.get(f"{BASE}/api/groups/{gid}/members").json()["members"]
    check("group sync: alice joined the mapped platform group (via directory)", [(m["username"], m["origin"]) for m in members] == [("alice", "sync")], members)
    check("the studio is usable (the page loaded without SSO error)", "sso_error" not in pg.url)
    ctx.close()

    print("a wrong password stays at the IdP; another user gets the default role")
    ctx, pg = sso_login("x", "x"); pg.locator("[data-testid=saml-login]:visible").click(); pg.wait_for_selector("input[name=username]", timeout=20000)
    pg.fill("input[name=username]", "bob"); pg.fill("input[name=password]", "WRONG"); pg.click("input[type=submit], button[type=submit]"); time.sleep(2)
    check("a wrong password never comes back to the studio", "localhost:8180" in pg.url and ctx.request.get(f"{BASE}/api/auth/me").json().get("authenticated") is not True or "localhost:8180" in pg.url)
    pg.fill("input[name=password]", "bobpw"); pg.click("input[type=submit], button[type=submit]"); pg.wait_for_url(f"{BASE}/**", timeout=30000); time.sleep(1.5)
    me = ctx.request.get(f"{BASE}/api/auth/me").json()
    check("bob (no group value) signs in with the default role", me.get("authenticated") is True and me["user"]["username"] == "bob" and me["user"]["role"] == "user", me)
    check("...and is not in the mapped group", "bob" not in [m["username"] for m in actx.request.get(f"{BASE}/api/groups/{gid}/members").json()["members"]])
    ctx.close()

    print("replaying a captured response")
    ctx, pg = sso_login("alice", "alicepw"); captured = {}
    def grab(req):
        if req.method == "POST" and req.url.endswith("/api/auth/saml/acs"): captured["form"] = req.post_data
    pg.on("request", grab)
    pg.locator("[data-testid=saml-login]:visible").click(); pg.wait_for_selector("input[name=username]", timeout=20000)
    pg.fill("input[name=username]", "alice"); pg.fill("input[name=password]", "alicepw"); pg.click("input[type=submit], button[type=submit]"); pg.wait_for_url(f"{BASE}/**", timeout=30000); time.sleep(1)
    check("the response was captured", "SAMLResponse" in (captured.get("form") or ""))
    import requests
    r = requests.post(f"{BASE}/api/auth/saml/acs", data=captured["form"], headers={"Content-Type": "application/x-www-form-urlencoded"}, allow_redirects=False)
    check("posting the captured response again is refused", r.status_code == 303 and "sso_error" in r.headers.get("location", ""), (r.status_code, r.headers.get("location")))
    ctx.close()
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
