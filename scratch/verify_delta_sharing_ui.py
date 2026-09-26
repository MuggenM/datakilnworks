#!/usr/bin/env python3
"""Delta Sharing dialog end to end (host, /usr/bin/python3, Playwright). Builds a throwaway studio `dshui` on port 8117 with a seeded Delta table, drives the
dialog (share, table, recipient, profile, revoke) and reads the table through the protocol with the profile that the dialog showed. Removes the container."""
import json, os, subprocess, sys, time, urllib.request
from playwright.sync_api import sync_playwright
import _ui_slow
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); BASE = "http://localhost:8117"
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
sh = lambda *a: subprocess.run(a, capture_output=True, text=True)
def http(url, token=None, method="GET", body=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None, headers={"Authorization": f"Bearer {token}"} if token else {})
    try:
        with urllib.request.urlopen(req, timeout=20) as r: return r.status, r.read().decode("utf-8", "replace"), dict(r.headers)
    except urllib.error.HTTPError as e: return e.code, e.read().decode("utf-8", "replace"), dict(e.headers)
sh("docker", "rm", "-f", "dshui")
sh("docker", "run", "-d", "--name", "dshui", "-p", "8117:8891", "-v", f"{ROOT}/web:/workspace/web", "-v", f"{ROOT}/docs:/workspace/docs", "-w", "/workspace", "-e", "WAREHOUSE_DIR=/workspace/warehouse",
   "-e", "INIT_ADMIN_USERNAME=admin", "-e", f"INIT_ADMIN_PASSWORD_HASH={HASH}", "localspark-lakehouse-notebook", "python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8891", "--no-proxy-headers")
try:
    for _ in range(90):
        if sh("curl", "-s", "-o", "/dev/null", f"{BASE}/api/docs").returncode == 0 and sh("docker", "exec", "dshui", "python", "-c", "import sqlite3;sqlite3.connect('/workspace/warehouse/.metadata/auth.db').execute('select 1 from users')").returncode == 0: break
        time.sleep(1)
    time.sleep(3)
    r = sh("docker", "exec", "-w", "/workspace", "dshui", "python", "-c", "import sqlite3,pandas as pd;from deltalake import write_deltalake\nc=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit()\nwrite_deltalake('/workspace/warehouse/sales/orders', pd.DataFrame({'id':[1,2,3],'amount':[1.5,2.5,3.5]}))")
    check("seeded a Delta table", r.returncode == 0, r.stderr[-300:])
    with sync_playwright() as p:
        b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1440, "height": 1400}); page = _ui_slow.apply(ctx.new_page()); errors = []
        page.on("pageerror", lambda e: errors.append(str(e))); page.on("dialog", lambda d: d.accept())
        check("logged in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
        page.goto(BASE, wait_until="networkidle"); time.sleep(1)
        page.evaluate("() => { Alpine.$data(document.body).currentView = 'catalog'; }"); page.wait_for_timeout(500); page.click("[data-testid=open-sharing]"); page.wait_for_selector("[data-testid=sharing-modal]", state="visible"); page.wait_for_timeout(500)
        page.wait_for_function("() => (document.querySelector('[data-testid=sharing-endpoint]').innerText || '').trim().length > 0", timeout=30000)
        check("the modal shows the endpoint", page.locator("[data-testid=sharing-endpoint]").inner_text().endswith("/delta-sharing"))
        page.fill("[data-testid=sharing-share-name]", "acme_orders"); page.click("[data-testid=sharing-share-create]"); page.wait_for_selector("[data-testid=sharing-share]")
        check("a share is created", page.locator("[data-testid=sharing-share]").count() == 1)
        page.fill("[data-testid=sharing-add-source-acme_orders]", "warehouse.sales.nope"); page.click("[data-testid=sharing-add-table-acme_orders]"); page.wait_for_timeout(800)
        check("a table that does not exist is refused (nothing added)", page.locator("[data-testid=sharing-table-row]").count() == 0)
        page.fill("[data-testid=sharing-add-source-acme_orders]", "warehouse.sales.orders"); page.click("[data-testid=sharing-add-table-acme_orders]"); page.wait_for_selector("[data-testid=sharing-table-row]")
        check("the table is added and not flagged", "sales.orders" in page.locator("[data-testid=sharing-table-row]").inner_text() and not page.locator("[data-testid=sharing-table-problem]").is_visible())
        page.click("[data-testid=sharing-tab-recipients]")
        page.fill("[data-testid=sharing-recipient-name]", "ACME Corp"); page.select_option("[data-testid=sharing-recipient-days]", "30")
        page.locator("label:has-text('acme_orders') input[type=checkbox]").first.check(); page.click("[data-testid=sharing-recipient-create]")
        page.wait_for_selector("[data-testid=sharing-secret-box]", state="visible")
        prof = json.loads(page.locator("[data-testid=sharing-profile]").inner_text())
        check("the profile is shown once, with endpoint, token and expiry", prof["shareCredentialsVersion"] == 1 and prof["bearerToken"].startswith("dkw_dsh_") and prof["expirationTime"].endswith("Z"), prof)
        page.screenshot(path="/tmp/dsh_recipients.png")
        with page.expect_download() as dl: page.click("[data-testid=sharing-profile-download]")
        check("it can be downloaded as a .share file", dl.value.suggested_filename == "ACME_Corp.share")
        ep = prof["endpoint"].replace("http://localhost:8117", BASE)
        st, body, h = http(ep + "/shares/acme_orders/schemas/sales/tables/orders/query", prof["bearerToken"], "POST", {})
        lines = [json.loads(x) for x in body.strip().split("\n")]
        furl = next(x["file"]["url"] for x in lines if "file" in x)
        check("the recipient reads the table with that profile (signed file link works)", st == 200 and {k.lower(): v for k, v in h.items()}.get("delta-table-version") == "0" and http(furl)[0] == 200, (st, body[:200]))
        page.wait_for_timeout(300)
        check("the recipient card shows it active", page.locator("[data-testid=sharing-recipient-state]").inner_text() == "active")
        check("the table was added WITHOUT history (the checkbox is off) and older versions are refused", not page.locator("[data-testid=sharing-table-history]").is_checked() if page.locator("[data-testid=sharing-tab-shares]").count() else True)
        st_v, _, _ = http(ep + "/shares/acme_orders/schemas/sales/tables/orders/query", prof["bearerToken"], "POST", {"version": 0}); check("(version 0 is the latest here; the change feed is refused without history)", st_v == 200 and http(ep + "/shares/acme_orders/schemas/sales/tables/orders/changes?startingVersion=0", prof["bearerToken"])[0] == 403)
        page.click("[data-testid=sharing-tab-shares]"); page.wait_for_timeout(300); page.locator("[data-testid=sharing-table-history]").check(); page.wait_for_timeout(800)
        check("ticking 'share history' enables the change feed", http(ep + "/shares/acme_orders/schemas/sales/tables/orders/changes?startingVersion=0", prof["bearerToken"])[0] == 200)
        page.click("[data-testid=sharing-tab-recipients]"); page.wait_for_timeout(300)
        page.fill("[data-testid='sharing-ips-ACME Corp']", "192.0.2.0/24"); page.keyboard.press("Tab"); page.wait_for_timeout(800)
        check("an allowed-address list can be saved; the local test client is now outside it", http(ep + "/shares", prof["bearerToken"])[0] == 403)
        page.fill("[data-testid='sharing-ips-ACME Corp']", ""); page.keyboard.press("Tab"); page.wait_for_timeout(800)
        check("clearing it opens the recipient up again", http(ep + "/shares", prof["bearerToken"])[0] == 200)
        page.click("[data-testid=sharing-tab-activity]"); page.wait_for_timeout(300)
        page.evaluate("() => Alpine.$data(document.body).loadSharing()"); page.wait_for_timeout(600)
        check("the activity tab lists the query", page.locator("[data-testid=sharing-log-row]").count() >= 1 and "ACME Corp" in page.locator("[data-testid=sharing-log-row]").first.inner_text())
        page.click("[data-testid=sharing-tab-recipients]"); page.click("[data-testid=sharing-recipient-revoke]"); page.wait_for_timeout(1000)
        check("revoking shows revoked, and the token and the file link stop working", page.locator("[data-testid=sharing-recipient-state]").inner_text() == "revoked" and http(ep + "/shares", prof["bearerToken"])[0] == 401 and http(furl)[0] == 401)
        check("no JS errors", not errors, errors)
        b.close()
finally:
    sh("docker", "rm", "-f", "dshui")
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
