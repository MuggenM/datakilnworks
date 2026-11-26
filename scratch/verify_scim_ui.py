#!/usr/bin/env python3
"""SCIM provisioning UI (Playwright, /usr/bin/python3) against a THROWAWAY studio container `scui` on :8117 (admin / adminpassword123):
  docker run -d --name scui -p 8117:8891 -v $PWD/web:/workspace/web -v $PWD/docs:/workspace/docs -w /workspace -e WAREHOUSE_DIR=/workspace/warehouse \
     -e INIT_ADMIN_USERNAME=admin -e INIT_ADMIN_PASSWORD_HASH='<hash of adminpassword123>' localspark-lakehouse-notebook python -m uvicorn web.app:app --host 0.0.0.0 --port 8891 --no-proxy-headers
  GIT_UI_URL=http://localhost:8117 python scratch/verify_scim_ui.py"""
import json, os, subprocess, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def ev(page, js):
    for i in range(3):
        try: return page.evaluate(js)
        except Exception as e:
            if "context was destroyed" not in str(e): raise
            time.sleep(2)
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1600, "height": 1200}); page = ctx.new_page(); errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("admin logs in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    subprocess.run(["docker", "exec", "scui", "python", "-c", "import sqlite3;c=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit()"], check=True, capture_output=True)
    page.goto(BASE, wait_until="networkidle"); time.sleep(5)
    ev(page, "() => { const d = Alpine.$data(document.body); d.showIamModal = true; d.iamTab = 'scim'; d.loadScim(); }"); time.sleep(2)
    check("the base URL to give the identity provider is shown", page.locator("[data-testid=scim-base-url]").inner_text().endswith("/scim/v2"))
    api = ctx.request
    check("SCIM is off: an API call with no token is refused", api.get(f"{BASE}/scim/v2/Users").status == 401)
    page.locator("[data-testid=scim-token-name]").fill("Entra ID"); page.locator("[data-testid=scim-token-create]").click(); page.locator("[data-testid=scim-secret]").wait_for(timeout=5000)
    token = page.locator("[data-testid=scim-secret]").inner_text()
    check("a token is created and shown once", token.startswith("dkw_scim_") and page.locator("[data-testid=scim-token-row]").count() == 1)
    H = {"Authorization": f"Bearer {token}", "Content-Type": "application/scim+json"}
    check("SCIM is still off: the token is refused with 403", api.get(f"{BASE}/scim/v2/Users", headers=H).status == 403)
    page.locator("[data-testid=scim-enabled]").check(); page.locator("[data-testid=scim-max-role]").select_option("admin"); page.locator("[data-testid=scim-default-role]").select_option("power_user"); page.locator("[data-testid=scim-max-role]").select_option("user"); page.locator("[data-testid=scim-save]").click(); time.sleep(1.5)
    check("a default role above the cap is refused with the reason", "cannot be higher" in page.locator("[data-testid=scim-error]").inner_text(), page.locator("[data-testid=scim-error]").inner_text())
    page.locator("[data-testid=scim-max-role]").select_option("power_user"); page.locator("[data-testid=scim-default-role]").select_option("user")
    page.locator("[data-testid=scim-add-group-role]").click(); page.locator("[data-testid=scim-group-name]").fill("Data Admins"); page.locator("[data-testid=scim-save]").click(); time.sleep(1.5)
    check("saved, and the token now works", page.locator("[data-testid=scim-error]").inner_text() == "" and api.get(f"{BASE}/scim/v2/Users", headers=H).status == 200)
    # an IdP provisions a user and a group
    pl = {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"], "userName": "ui.alice@corp.example", "displayName": "UI Alice", "active": True}
    u = api.post(f"{BASE}/scim/v2/Users", data=json.dumps(pl), headers=H).json()
    g = api.post(f"{BASE}/scim/v2/Groups", data=json.dumps({"displayName": "Data Admins", "members": [{"value": u["id"]}]}), headers=H).json()
    api.patch(f"{BASE}/scim/v2/Users/" + u["id"], data=json.dumps({"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"], "Operations": [{"op": "Replace", "path": "active", "value": "False"}]}), headers=H)
    page.locator("[data-testid=scim-refresh]").click(); time.sleep(1.5)
    check("the counts show the provisioned user (deactivated) and group", page.locator("[data-testid=scim-stat-users]").inner_text() == "1" and page.locator("[data-testid=scim-stat-inactive]").inner_text() == "1" and page.locator("[data-testid=scim-stat-groups]").inner_text() == "1")
    ev_txt = page.locator("[data-testid=scim-event-row]").first.inner_text()
    check("recent requests list the token's name, request and status", "Entra ID" in " ".join(page.locator("[data-testid=scim-event-row]").all_inner_texts()) and "PATCH" in " ".join(page.locator("[data-testid=scim-event-row]").all_inner_texts()), ev_txt)
    ev(page, "() => { const d = Alpine.$data(document.body); d.iamTab = 'users'; d.fetchUsers && d.fetchUsers(); }"); time.sleep(2)
    check("the users list shows a SCIM badge on the provisioned account only", page.locator("[data-testid=user-scim-badge]:visible").count() == 1)
    page.screenshot(path="/tmp/scim_ui.png")
    ev(page, "() => { const d = Alpine.$data(document.body); d.iamTab = 'scim'; d.loadScim(); }"); time.sleep(1.5)
    page.once("dialog", lambda d: d.accept()); page.locator("[data-testid=scim-token-revoke]").click(); time.sleep(1.5)
    check("revoking the token stops the identity provider at once", api.get(f"{BASE}/scim/v2/Users", headers=H).status == 401)
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
