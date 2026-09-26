#!/usr/bin/env python3
"""Warm-start settings in the SQL warehouse dialog (Playwright, host /usr/bin/python3). Builds a throwaway studio `wsui` on port 8117 and removes it."""
import os, subprocess, sys, time
from playwright.sync_api import sync_playwright
import _ui_slow
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); BASE = "http://localhost:8117"
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
sh = lambda *a: subprocess.run(a, capture_output=True, text=True)
sh("docker", "rm", "-f", "wsui")
sh("docker", "run", "-d", "--name", "wsui", "-p", "8117:8891", "-v", f"{ROOT}/web:/workspace/web", "-v", f"{ROOT}/docs:/workspace/docs", "-w", "/workspace", "-e", "WAREHOUSE_DIR=/workspace/warehouse",
   "-e", "INIT_ADMIN_USERNAME=admin", "-e", f"INIT_ADMIN_PASSWORD_HASH={HASH}", "localspark-lakehouse-notebook", "python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8891", "--no-proxy-headers")
try:
    for _ in range(90):
        if sh("curl", "-s", "-o", "/dev/null", f"{BASE}/api/docs").returncode == 0 and sh("docker", "exec", "wsui", "python", "-c", "import sqlite3;sqlite3.connect('/workspace/warehouse/.metadata/auth.db').execute('select 1 from users')").returncode == 0: break
        time.sleep(1)
    time.sleep(3); sh("docker", "exec", "wsui", "python", "-c", "import sqlite3;c=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit()")
    with sync_playwright() as p:
        b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1440, "height": 1300}); page = _ui_slow.apply(ctx.new_page()); errors = []
        page.on("pageerror", lambda e: errors.append(str(e))); page.on("dialog", lambda d: (errors.append("dialog: " + d.message), d.accept()))
        check("logged in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
        page.goto(BASE, wait_until="networkidle"); time.sleep(1)
        act = lambda code: page.evaluate(f"async () => {{ const d = Alpine.$data(document.body); {code} }}")
        act("d.currentView = 'warehouses'; await d.fetchSqlWarehouses(); d.openCreateWarehouseModal();")
        page.wait_for_selector("[data-testid=warm-start-settings]", state="visible")
        check("the hold selector appears only for the warm (pause) mode", not page.locator("[data-testid=wh-warm-hold]").is_visible())
        page.select_option("[data-testid=wh-standby-mode]", "pause"); time.sleep(0.3)
        check("...and is shown after choosing it", page.locator("[data-testid=wh-warm-hold]").is_visible())
        page.select_option("[data-testid=wh-warm-hold]", "120")
        act("d.warehouseForm.name = 'Warm WH'; d.warehouseForm.warm_tables = 'warehouse.sales.orders, warehouse.sales.customers';")
        page.screenshot(path="/tmp/warm_form.png")
        page.locator("button:has-text('Create Warehouse'):visible").last.click()
        try:
            page.wait_for_function("() => (Alpine.$data(document.body).sqlWarehouses || []).some(x => x.name === 'Warm WH')", timeout=20000)      # a slow runner needs longer than a fixed sleep
        except Exception:
            pass
        w = page.evaluate("() => Alpine.$data(document.body).sqlWarehouses.find(x => x.name === 'Warm WH')")
        check("the warehouse is saved with its warm-start settings", w and w["standby_mode"] == "pause" and w["warm_hold_mins"] == 120 and w["warm_tables"] == ["warehouse.sales.orders", "warehouse.sales.customers"], w)
        act(f"d.editWarehouse(d.sqlWarehouses.find(x => x.name === 'Warm WH'));")
        page.wait_for_selector("[data-testid=wh-warm-tables]", state="visible")
        check("editing shows them again", page.input_value("[data-testid=wh-standby-mode]") == "pause" and page.input_value("[data-testid=wh-warm-hold]") == "120" and "customers" in page.input_value("[data-testid=wh-warm-tables]"))
        page.fill("[data-testid=wh-warm-tables]", "not-a-table"); page.locator("button:has-text('Update Warehouse'):visible").last.click(); page.wait_for_timeout(1500)
        check("a bad table name is refused with the reason", any("not a catalog.schema.table" in e for e in errors), errors)
        errors[:] = [e for e in errors if not e.startswith("dialog")]
        check("no JS errors", not errors, errors)
        b.close()
finally:
    sh("docker", "rm", "-f", "wsui")
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
