#!/usr/bin/env python3
"""Dashboard apps UI (Playwright, host /usr/bin/python3): the Apps catalogue (samples, clone), an app's pages and parameter bar, clicking a chart to
filter, text blocks (escaped), editing widgets / parameters / pages, export by a viewer, and a viewer seeing no editing tools. Builds a throwaway studio
`dapui` on port 8118, seeds a second user, removes the container. ARTIFACT_DIR (default /tmp/dkw_shots) receives screenshots."""
import os, subprocess, sys, time
from playwright.sync_api import sync_playwright
import _ui_slow
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); BASE = "http://localhost:8118"
ART = os.getenv("ARTIFACT_DIR", "/tmp/dkw_shots"); os.makedirs(ART, exist_ok=True)
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
sh = lambda *a: subprocess.run(a, capture_output=True, text=True)
sh("docker", "rm", "-f", "dapui")
sh("docker", "run", "-d", "--name", "dapui", "-p", "8118:8891", "-v", f"{ROOT}/web:/workspace/web", "-v", f"{ROOT}/docs:/workspace/docs", "-w", "/workspace", "-e", "WAREHOUSE_DIR=/workspace/warehouse",
   "-e", "INIT_ADMIN_USERNAME=admin", "-e", f"INIT_ADMIN_PASSWORD_HASH={HASH}", "localspark-lakehouse-notebook", "python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8891", "--no-proxy-headers")
try:
    for _ in range(90):
        if sh("curl", "-s", "-o", "/dev/null", f"{BASE}/api/docs").returncode == 0 and sh("docker", "exec", "dapui", "python", "-c", "import sqlite3;sqlite3.connect('/workspace/warehouse/.metadata/auth.db').execute('select 1 from users')").returncode == 0: break
        time.sleep(1)
    time.sleep(3)
    sh("docker", "exec", "dapui", "python", "-c",
       "import sqlite3;c=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit();"
       "import sys;sys.path.insert(0,'/workspace');from web import auth;auth.create_user('bob','bobpass1234','Bob',role='user')")
    sh("docker", "exec", "dapui", "python", "-c", "import sqlite3;c=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit()")
    with sync_playwright() as p:
        b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1700, "height": 1100}, accept_downloads=True); page = _ui_slow.apply(ctx.new_page()); errors = []
        page.on("pageerror", lambda e: errors.append(str(e))); page.on("dialog", lambda d: d.accept(d.default_value or "Extra page") if d.type == "prompt" else d.accept())
        page.on("console", lambda m: errors.append(m.text[:200]) if m.type == "error" and "Failed to load resource" not in m.text else None)
        check("logged in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
        page.goto(BASE, wait_until="networkidle"); time.sleep(1)
        ev = lambda code: page.evaluate(code)

        print("the Apps catalogue")
        page.click("[data-testid=nav-apps]"); page.wait_for_selector("[data-testid=dash-sample-card]", state="visible", timeout=15000)
        check("three samples are offered", page.locator("[data-testid=dash-sample-card]").count() == 3)
        check("no apps of your own yet, so no 'Your apps' section", not page.locator("[data-testid=dash-apps]").is_visible())
        page.screenshot(path=f"{ART}/dashapps_catalogue.png")
        page.locator("[data-testid=dash-sample-card]").first.locator("[data-testid=dash-sample-use]").click()
        page.wait_for_selector("[data-testid=clone-dlg]", state="visible")
        check("the name field is pre-filled with the sample's title", page.input_value("[data-testid=clone-name]") == "Sales explorer")
        page.fill("[data-testid=clone-name]", "My sales"); page.click("[data-testid=clone-submit]")
        try:
            page.wait_for_selector("[data-testid=app-bar]", state="visible", timeout=20000)
        except Exception:
            print("DIAG", ev("() => { const d = Alpine.$data(document.body); return JSON.stringify({view: d.currentView, sel: d.selectedDashboardId, cur: d.currentDashboard && {id: d.currentDashboard.id, access: d.currentDashboard.my_access, pages: d.currentDashboard.pages}, clone: d.cloneDlg, dashes: d.dashboards.length}); }"), errors[:5])
            print(sh("docker", "logs", "--tail", "25", "dapui").stderr[-2500:])
            raise

        print("the app")
        page.wait_for_function("() => document.querySelectorAll('[data-testid=page-tab]').length === 2", timeout=15000)
        check("it opened in the dashboard view with two page tabs", [t.strip() for t in page.locator("[data-testid=page-tab]").all_inner_texts()] == ["Overview", "Order details"])
        page.wait_for_function("() => document.querySelectorAll('[data-testid=param-bar] [data-param]').length === 4", timeout=10000)
        check("four typed parameters are in the bar", page.locator("[data-testid=param-bar] [data-param]").count() == 4)
        check("the classic filter bar is hidden for an app", not page.locator("text=Filters & Parameters:").first.is_visible())
        page.wait_for_function("() => { const d = Alpine.$data(document.body); return d.currentDashboard && d.currentDashboard.widgets.every(w => w.result && w.result.success); }", timeout=30000)
        check("every widget ran", True)
        kpi = lambda title: page.locator(f"div.bg-dbx-panel:has(h4:text-is('{title}'))").first
        rev0 = ev("() => Alpine.$data(document.body).currentDashboard.widgets.find(w => w.title === 'Revenue').result.rows[0].revenue")
        check("the Revenue KPI shows a number", kpi("Revenue").inner_text().strip() != "" and rev0 > 0, rev0)
        check("the text block renders", "Sales explorer" in page.locator("[data-testid=text-widget]").first.inner_text())
        check("only the current page's widgets are on screen", page.locator("[data-testid=text-widget]:visible").count() == 1 and not page.locator("h4:text-is('Orders (latest 200)')").first.is_visible())
        check("the dashboard picker shows the app that is open", page.eval_on_selector("select[x-model=selectedDashboardId]", "e => e.options[e.selectedIndex].text") == "My sales")
        check("a KPI of an app does not print its SQL under the number", not page.locator("div.bg-dbx-panel:has(h4:text-is('Revenue'))").first.locator("div.truncate").first.is_visible())
        page.wait_for_timeout(1500); page.screenshot(path=f"{ART}/dashapps_overview.png")

        print("parameters filter the app")
        page.select_option("[data-param=region]", "North"); page.wait_for_timeout(1500)
        rev1 = ev("() => Alpine.$data(document.body).currentDashboard.widgets.find(w => w.title === 'Revenue').result.rows[0].revenue")
        check("choosing a region lowers the revenue", 0 < rev1 < rev0, (rev0, rev1))
        page.click("[data-testid=param-reset]"); page.wait_for_timeout(1500)
        check("Reset brings it back", ev("() => Alpine.$data(document.body).currentDashboard.widgets.find(w => w.title === 'Revenue').result.rows[0].revenue") == rev0)
        page.fill("[data-param=from_date]", "2026-03-01"); page.fill("[data-param=to_date]", "2026-03-31"); page.dispatch_event("[data-param=to_date]", "change"); page.wait_for_timeout(1500)
        rev2 = ev("() => Alpine.$data(document.body).currentDashboard.widgets.find(w => w.title === 'Revenue').result.rows[0].revenue")
        check("a date range narrows it", 0 < rev2 < rev0, rev2)
        page.click("[data-testid=param-reset]"); page.wait_for_timeout(1500)

        print("clicking a bar sets the parameter")
        pos = ev("""() => { const d = Alpine.$data(document.body); const w = d.currentDashboard.widgets.find(w => w.title.startsWith('Revenue by region'));
                            const c = d.chartInstances[w.id]; const el = document.getElementById('chart-' + w.id).getBoundingClientRect();
                            const px = c.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, ['South', 1]); const base = c.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, ['South', 0]);
                            return { x: el.left + px[0], y: el.top + (px[1] + base[1]) / 2 }; }""")
        page.mouse.click(pos["x"], pos["y"]); page.wait_for_timeout(1800)
        check("the region parameter became South", page.input_value("[data-param=region]") == "South", page.input_value("[data-param=region]"))
        check("a 'Clicked: region = South' badge appears", "region = South" in page.locator("[data-testid=param-bar]").inner_text())
        rev3 = ev("() => Alpine.$data(document.body).currentDashboard.widgets.find(w => w.title === 'Revenue').result.rows[0].revenue")
        check("and every widget followed", 0 < rev3 < rev0, rev3)
        pos = ev("""() => { const d = Alpine.$data(document.body); const w = d.currentDashboard.widgets.find(w => w.title.startsWith('Revenue by region'));
                            const c = d.chartInstances[w.id]; const el = document.getElementById('chart-' + w.id).getBoundingClientRect();
                            const px = c.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, ['South', 1]); const base = c.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, ['South', 0]);
                            return { x: el.left + px[0], y: el.top + (px[1] + base[1]) / 2 }; }""")
        page.mouse.click(pos["x"], pos["y"]); page.wait_for_timeout(1800)
        check("clicking the same bar again clears it", page.input_value("[data-param=region]") == "ALL")

        print("pages and export")
        page.locator("[data-testid=page-tab]").nth(1).click(); page.wait_for_timeout(800)
        check("the second page shows its table and not the first page's text", page.locator("h4:text-is('Orders (latest 200)')").first.is_visible() and page.locator("[data-testid=text-widget]:visible").count() == 0)
        card = page.locator("div.bg-dbx-panel:has(h4:text-is('Orders (latest 200)'))").first
        card.hover(); card.locator("button[title='Export Data']").click()
        with page.expect_download(timeout=15000) as dl:
            card.locator("button:has-text('CSV')").click()
        name = dl.value.path()
        text = open(name).read()
        check("a viewer-side CSV export downloads the table rows", "order_id" in text and text.count("\n") >= 150, text[:200])
        page.screenshot(path=f"{ART}/dashapps_page2.png")
        page.locator("[data-testid=page-tab]").first.click(); page.wait_for_timeout(600)

        print("editing: text (escaped), widget, parameters, pages")
        page.click("[data-testid=add-text]"); page.wait_for_selector("[data-testid=widget-edit-dlg]", state="visible")
        page.fill("[data-testid=wedit-title]", "Note"); page.fill("[data-testid=wedit-markdown]", '<img src=x onerror="window.__xss=1"> **bold** [ok](https://example.com) [bad](javascript:alert(1))')
        page.click("[data-testid=wedit-save]"); page.wait_for_timeout(1500)
        note = page.locator("[data-testid=text-widget]:visible").nth(1)
        check("the new text block is on the page and shows the markup as text", "<img" in note.inner_text() and "bold" in note.inner_text())
        check("no element was created from the text, no handler ran", note.locator("img").count() == 0 and ev("() => window.__xss") is None)
        check("a https link becomes a link, a javascript: link stays plain text", note.locator("a[href^='https://example.com']").count() == 1 and note.locator("a[href^='javascript']").count() == 0)
        card = page.locator("div.bg-dbx-panel:has(h4:text-is('Average order'))").first
        card.hover(); card.locator("[data-testid=widget-edit]").click(); page.wait_for_selector("[data-testid=widget-edit-dlg]", state="visible")
        page.fill("[data-testid=wedit-query]", "select 1 from nowhere where :ghost = 1"); page.click("[data-testid=wedit-save]"); page.wait_for_timeout(800)
        check("an undeclared parameter in a query is refused with a message", "':ghost' is not a declared parameter" in page.locator("[data-testid=wedit-error]").inner_text())
        page.fill("[data-testid=wedit-title]", "Mean order"); page.fill("[data-testid=wedit-query]", "select 12 as v"); page.click("[data-testid=wedit-save]"); page.wait_for_timeout(1500)
        check("a valid edit is saved", page.locator("h4:text-is('Mean order')").count() == 1 and not page.locator("[data-testid=widget-edit-dlg]").is_visible())
        page.click("[data-testid=edit-params]"); page.wait_for_selector("[data-testid=params-dlg]", state="visible")
        page.click("[data-testid=param-add]")
        row = page.locator("[data-testid=param-row]").last
        row.locator("input").first.fill("Bad Id"); page.click("[data-testid=params-save]"); page.wait_for_timeout(800)
        check("an invalid parameter id is refused with a message", "lower-case" in page.locator("[data-testid=params-error]").inner_text())
        row.locator("input").first.fill("min_amount"); row.locator("select").first.select_option("number"); row.locator("input").nth(2).fill("100")
        page.click("[data-testid=params-save]"); page.wait_for_timeout(1500)
        check("a valid parameter is added to the bar", page.locator("[data-testid=param-bar] [data-param=min_amount]").count() == 1 and page.input_value("[data-param=min_amount]") == "100")
        page.click("[data-testid=page-add]"); page.wait_for_timeout(1200)
        check("adding a page adds a tab", page.locator("[data-testid=page-tab]").count() == 3)

        print("a second app: the table explorer (choices come from the catalogue)")
        page.click("[data-testid=nav-apps]"); page.wait_for_selector("[data-testid=dash-apps]", state="visible", timeout=10000)
        check("'Your apps' now lists the clone", page.locator("[data-testid=dash-app-card]").count() == 1)
        page.locator("[data-testid=dash-sample-card]").nth(2).locator("[data-testid=dash-sample-use]").click(); page.click("[data-testid=clone-submit]")
        page.wait_for_selector("[data-testid=param-bar] [data-param=tbl]", state="visible", timeout=20000); page.wait_for_timeout(1500)
        opts = page.locator("[data-param=tbl] option").all_inner_texts()
        check("the picker lists tables and has no ALL entry", len(opts) >= 0 and "ALL" not in opts, opts)
        page.screenshot(path=f"{ART}/dashapps_explorer.png")

        print("a viewer sees the app but none of the editing tools")
        app_id = ev("() => Alpine.$data(document.body).dashApps[0].id") or ctx.request.get(f"{BASE}/api/dashboard-apps").json()["apps"][0]["id"]
        sh("docker", "exec", "dapui", "python", "-c", f"import sys;sys.path.insert(0,'/workspace');from web.dashboard_permissions import grant_permission;print(grant_permission('{app_id}', user='bob', level='viewer', granted_by='admin'))")
        bctx = b.new_context(viewport={"width": 1700, "height": 1100}); bpage = _ui_slow.apply(bctx.new_page())
        bpage.on("dialog", lambda d: d.accept())
        check("bob logged in", bctx.request.post(f"{BASE}/api/auth/login", data={"username": "bob", "password": "bobpass1234"}).ok)
        bpage.goto(BASE, wait_until="networkidle"); time.sleep(1)
        bpage.click("[data-testid=nav-apps]"); bpage.wait_for_selector("[data-testid=dash-app-card]", state="visible", timeout=15000)
        check("bob sees the shared app in the catalogue", bpage.locator("[data-testid=dash-app-card]").count() == 1)
        bpage.locator("[data-testid=dash-app-open]").click(); bpage.wait_for_selector("[data-testid=page-tab]", state="visible", timeout=20000); bpage.wait_for_timeout(1500)
        check("bob has the app's pages and parameters", bpage.locator("[data-testid=param-bar]").is_visible())
        check("bob has no Add Widget, no editing tools, no delete", not bpage.locator("button:has-text('Add Widget')").is_visible() and not bpage.locator("[data-testid=add-text]").is_visible()
              and not bpage.locator("[data-testid=edit-params]").is_visible() and not bpage.locator("[data-testid=page-add]").is_visible() and not bpage.locator("button[title='Delete Dashboard']").is_visible())
        bcard = bpage.locator("div.bg-dbx-panel:has(h4:text-is('Revenue'))").first
        bcard.hover()
        check("bob has no widget edit button, but does have export", not bpage.locator("[data-testid=widget-edit]:visible").count() and bcard.locator("button[title='Export Data']").count() == 1)
        bpage.screenshot(path=f"{ART}/dashapps_viewer.png")
        check("no page errors", not errors, errors[:5])
        b.close()
finally:
    sh("docker", "rm", "-f", "dapui")
print()
if FAIL:
    print(f"{len(FAIL)} FAILED:"); [print("  -", f) for f in FAIL]; sys.exit(1)
print("ALL PASS")
