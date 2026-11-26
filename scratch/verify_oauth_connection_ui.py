#!/usr/bin/env python3
"""OAuth 2.0 client-credentials connection form (Playwright, /usr/bin/python3) against a THROWAWAY studio `oaui` with the mock authorization server running
inside it (scratch/oauth_mock_server.py on 127.0.0.1:9911):
  docker run -d --name oaui -p 8117:8891 -v $PWD/web:/workspace/web -v $PWD/docs:/workspace/docs -w /workspace -e WAREHOUSE_DIR=/workspace/warehouse \
     -e INIT_ADMIN_USERNAME=admin -e INIT_ADMIN_PASSWORD_HASH='<hash of adminpassword123>' localspark-lakehouse-notebook python -m uvicorn web.app:app --host 0.0.0.0 --port 8891 --no-proxy-headers
  docker cp scratch/oauth_mock_server.py oaui:/tmp/ && docker exec -d oaui python /tmp/oauth_mock_server.py 9911
  GIT_UI_URL=http://localhost:8117 python scratch/verify_oauth_connection_ui.py"""
import os, subprocess, sys, time
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
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1500, "height": 1300}); page = ctx.new_page(); errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("login", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    subprocess.run(["docker", "exec", "oaui", "python", "-c", "import sqlite3;c=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit()"], check=True, capture_output=True)
    page.goto(BASE, wait_until="networkidle"); time.sleep(4)
    ev(page, "async () => { const d = Alpine.$data(document.body); d.currentView = 'autoloader'; await d.openConnections(); d.newConnection(); }"); time.sleep(1)
    page.locator("[data-testid=conn-name]").fill("oauth_api"); page.locator("[data-testid=conn-base-url]").fill("http://127.0.0.1:9911/api/")
    check("OAuth 2.0 is one of the authentication choices", "oauth2" in page.locator("[data-testid=conn-auth] option").evaluate_all("els => els.map(e => e.value)"))
    check("its fields are hidden until chosen", not page.locator("[data-testid=conn-oauth]").is_visible())
    page.locator("[data-testid=conn-auth]").select_option("oauth2"); time.sleep(0.5)
    check("choosing it shows token URL, client id, scope, method and extra parameters, and the secret is the client secret", page.locator("[data-testid=conn-oauth]").is_visible() and "client secret" in page.locator("[data-testid=conn-secret]").locator("xpath=..").inner_text().lower())
    page.locator("[data-testid=conn-token-url]").fill("http://127.0.0.1:9911/token"); page.locator("[data-testid=conn-client-id]").fill("my-client"); page.locator("[data-testid=conn-scope]").fill("api.read"); page.locator("[data-testid=conn-extra]").fill("audience=https://api.example")
    page.locator("[data-testid=conn-secret]").fill("s3cr3t-value-XYZ"); page.locator("[data-testid=conn-save]").click(); time.sleep(1.5)
    check("a token URL over plain http is refused with the reason until 'allow insecure' is ticked", "plain http" in page.locator("[data-testid=conn-error]").inner_text(), page.locator("[data-testid=conn-error]").inner_text())
    page.locator("text=Allow credentials over plain http").click()
    page.locator("[data-testid=conn-test]").click(); page.locator("text=Token obtained").first.wait_for(timeout=15000)
    check("Test fetches a token and reaches the API (no token or secret shown)", "valid for 3600 s" in page.locator("[data-testid=conn-form]").inner_text() and "s3cr3t" not in page.locator("[data-testid=conn-form]").inner_text() and "mock-token" not in page.locator("[data-testid=conn-form]").inner_text())
    page.locator("[data-testid=conn-save]").click(); page.locator("[data-testid=conn-row]").first.wait_for(timeout=8000)
    check("the connection is saved and listed", "oauth_api" in page.locator("[data-testid=conn-row]").first.inner_text())
    lst = ctx.request.get(f"{BASE}/api/connections").text()
    check("the API never returns the client secret", "s3cr3t" not in lst and '"has_secret":true' in lst.replace(" ", ""))
    page.locator("[data-testid=conn-edit]").first.click(); time.sleep(0.8)
    check("editing shows the saved settings; the secret stays hidden with a 'stored' hint", page.locator("[data-testid=conn-token-url]").input_value().endswith("/token") and page.locator("[data-testid=conn-client-id]").input_value() == "my-client" and page.locator("[data-testid=conn-scope]").input_value() == "api.read" and "audience=https://api.example" in page.locator("[data-testid=conn-extra]").input_value() and page.locator("[data-testid=conn-secret]").input_value() == "" and "stored" in (page.locator("[data-testid=conn-secret]").get_attribute("placeholder") or ""))
    page.locator("[data-testid=conn-test]").click(); page.locator("text=Token obtained").first.wait_for(timeout=15000)
    check("Test works with the stored secret (the form does not have to hold it)", "Token obtained" in page.locator("[data-testid=conn-form]").inner_text())
    page.locator("[data-testid=conn-client-id]").fill("someone-else"); page.locator("[data-testid=conn-test]").click(); time.sleep(3)
    check("a wrong client is reported with the server's reason, without the secret", "invalid_client" in page.locator("[data-testid=conn-form]").inner_text() and "s3cr3t" not in page.locator("[data-testid=conn-form]").inner_text(), page.locator("[data-testid=conn-form]").inner_text()[-200:])
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
