#!/usr/bin/env python3
"""Jobs & Pipelines overview (Playwright, host /usr/bin/python3): the workflows table (last-run status, sparkline of recent runs, schedule / next run,
owner, filter, status chips, sort, Run), the details block of a workflow, Pause / Resume, and the runs timeline with its statistics. Builds a throwaway
studio `ovui` on port 8117, seeds workflows and a run history through the API, removes the container. LIGHT=1 also takes light-theme screenshots."""
import json, os, subprocess, sys, time
from playwright.sync_api import sync_playwright
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); BASE = "http://localhost:8117"
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
sh = lambda *a: subprocess.run(a, capture_output=True, text=True)
def T(tid, deps=(), q="select 1"): return {"id": tid, "name": tid, "type": "sql", "depends_on": list(deps), "parameters": {"query": q}}
OK, BAD = [T("a"), T("b", ["a"])], [T("a"), T("b", ["a"], "select * from table_zz_missing")]
sh("docker", "rm", "-f", "ovui")
sh("docker", "run", "-d", "--name", "ovui", "-p", "8117:8891", "-v", f"{ROOT}/web:/workspace/web", "-v", f"{ROOT}/docs:/workspace/docs", "-w", "/workspace", "-e", "WAREHOUSE_DIR=/workspace/warehouse",
   "-e", "INIT_ADMIN_USERNAME=admin", "-e", f"INIT_ADMIN_PASSWORD_HASH={HASH}", "localspark-lakehouse-notebook", "python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8891", "--no-proxy-headers")
try:
    for _ in range(90):
        if sh("curl", "-s", "-o", "/dev/null", f"{BASE}/api/docs").returncode == 0 and sh("docker", "exec", "ovui", "python", "-c", "import sqlite3;sqlite3.connect('/workspace/warehouse/.metadata/auth.db').execute('select 1 from users')").returncode == 0: break
        time.sleep(1)
    time.sleep(3)
    sh("docker", "exec", "ovui", "python", "-c", "import sqlite3;c=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit()")
    with sync_playwright() as p:
        b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1600, "height": 1000}); page = ctx.new_page(); errors = []
        page.on("pageerror", lambda e: errors.append(str(e))); page.on("dialog", lambda d: d.accept())
        page.on("console", lambda m: errors.append(m.text[:200]) if m.type == "error" and "Failed to load resource" not in m.text else None)
        check("logged in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
        job = lambda jid, tasks, **kw: ctx.request.post(f"{BASE}/api/jobs", data={"id": jid, "name": jid, "description": kw.pop("description", "d " + jid), "enabled": kw.pop("enabled", True), "tasks": tasks, **kw})
        run = lambda jid: ctx.request.post(f"{BASE}/api/jobs/{jid}/run", data={"wait": True, "parameters": {}}, timeout=60000)
        job("alpha", OK, schedule_cron="0 3 * * *")
        for _ in range(3): run("alpha")
        job("alpha", BAD, schedule_cron="0 3 * * *")
        for _ in range(2): run("alpha")
        job("beta", OK, description="the manual one"); run("beta")
        job("gamma", OK, schedule_cron="*/5 * * * *", enabled=False, description="paused and never run")
        job("delta", OK, description="quarterly reconciliation")
        page.goto(BASE, wait_until="networkidle"); time.sleep(1)
        ev = lambda code: page.evaluate(code)
        ev("() => { const d = Alpine.$data(document.body); d.currentView = 'jobs'; d.fetchJobs(); }"); page.wait_for_timeout(1500)
        rows = lambda: page.locator("[data-testid=jobs-row]")
        row = lambda j: page.locator(f"[data-testid=jobs-row][data-job={j}]")
        bars = lambda j: row(j).locator("[data-testid=spark-bar]")

        print("the workflows table")
        check("with nothing opened the table is the page (no workflow is auto-selected)", page.locator("[data-testid=jobs-table-page]").is_visible() and ev("() => Alpine.$data(document.body).selectedJob") is None and rows().count() >= 4)
        check("the count line says how many workflows are shown", "of" in page.locator("[data-testid=jobs-count]").inner_text())
        st = lambda j: row(j).locator("[data-testid=jobs-dot]").get_attribute("data-status")
        check("last-run status per workflow (failed, succeeded, never run)", (st("alpha"), st("beta"), st("gamma")) == ("FAILED", "SUCCESS", "NEVER"), (st("alpha"), st("beta"), st("gamma")))
        check("the sparkline has one bar per recent run, oldest on the left, coloured by result", [bars("alpha").nth(i).get_attribute("data-status") for i in range(bars("alpha").count())] == ["SUCCESS"] * 3 + ["FAILED"] * 2 and bars("beta").count() == 1 and bars("gamma").count() == 0)
        hs = [float(bars("alpha").nth(i).evaluate("e => parseFloat(e.style.height)")) for i in range(5)]
        check("bar heights follow durations (the longest is tallest, none is invisible)", max(hs) > min(hs) and min(hs) >= 5 and max(hs) <= 24.5, hs)
        check("a workflow without runs says so", "no runs yet" in row("gamma").locator("[data-testid=jobs-spark]").inner_text())
        check("schedule and next run: cron workflows show the next tick, manual ones say Manual, paused ones say paused", "03:00" in row("alpha").locator("[data-testid=jobs-next]").inner_text() and "Manual" in row("beta").inner_text() and "paused" in row("gamma").locator("[data-testid=jobs-next]").inner_text().lower() and row("gamma").locator("[data-testid=jobs-paused]").is_visible(), row("alpha").locator("[data-testid=jobs-next]").inner_text())
        check("owner is shown", "admin" in row("alpha").inner_text())
        page.screenshot(path="/tmp/jobs_table.png")

        print("filter, chips, sort")
        page.fill("[data-testid=jobs-search]", "reconcil"); page.wait_for_timeout(300)
        check("the filter matches name, description, owner and id", rows().count() == 1 and row("delta").is_visible())
        page.fill("[data-testid=jobs-search]", ""); page.wait_for_timeout(200)
        chips = {k: page.locator(f"[data-testid=jobs-filter-{k}]").inner_text() for k in ("all", "SUCCESS", "FAILED", "NEVER")}
        check("the status chips show counts", chips["FAILED"].replace("\n", " ").endswith("1") and chips["SUCCESS"].replace("\n", " ").endswith("1") and chips["NEVER"].replace("\n", " ").endswith("4"), chips)
        page.click("[data-testid=jobs-filter-FAILED]"); page.wait_for_timeout(250)
        check("Failed shows only workflows whose last run failed", rows().count() == 1 and row("alpha").is_visible())
        page.click("[data-testid=jobs-filter-NEVER]"); page.wait_for_timeout(250)
        check("Never run shows the others (the two default workflows have never run either)", {"gamma", "delta"} <= {r.get_attribute("data-job") for r in rows().all()} and rows().count() == 4)
        page.click("[data-testid=jobs-filter-all]"); page.wait_for_timeout(250)
        names = lambda: [t.strip().lower() for t in page.locator("[data-testid=jobs-row] td:first-child span.font-semibold").all_inner_texts()]
        check("rows are sorted by name; clicking the header reverses it", names() == sorted(names()) and (page.click("[data-testid=jobs-sort-name]"), page.wait_for_timeout(250), names() == sorted(names(), reverse=True))[2])
        page.click("[data-testid=jobs-sort-name]"); page.wait_for_timeout(200)

        print("open a workflow: details")
        row("alpha").click(); page.wait_for_selector("[data-testid=job-details]", state="visible"); page.wait_for_timeout(600)
        d = page.locator("[data-testid=job-details]").inner_text()
        check("the details block shows id, owner, run as, schedule and next run", page.locator("[data-testid=job-detail-id]").inner_text() == "alpha" and "admin" in page.locator("[data-testid=job-detail-owner]").inner_text() and "admin" in page.locator("[data-testid=job-detail-runas]").inner_text() and "0 3 * * *" in d and "03:00" in page.locator("[data-testid=job-detail-next]").inner_text(), d)
        check("the Schedule button counts the schedule and triggers", page.locator("[data-testid=job-schedule-btn]").inner_text().strip() == "Schedule (1)", page.locator("[data-testid=job-schedule-btn]").inner_text())
        check("the sidebar with the other workflows is there for quick switching, the table page is gone", not page.locator("[data-testid=jobs-table-page]").is_visible() and page.locator("text=Configured Workflows").first.is_visible())
        page.locator("[data-testid=job-details-toggle]").click(); page.wait_for_timeout(200)
        check("the details can be collapsed (and the choice is remembered)", not page.locator("[data-testid=job-details]").is_visible() and ev("() => localStorage.getItem('dkwJobDetails')") == "closed")
        page.locator("[data-testid=job-details-toggle]").click(); page.wait_for_timeout(200)
        page.screenshot(path="/tmp/jobs_detail.png")

        print("pause and resume")
        page.locator("[data-testid=job-pause]").click(); page.wait_for_selector("[data-testid=job-paused-pill]", state="visible", timeout=5000); page.wait_for_timeout(500)
        check("Pause marks the workflow paused and removes its next run", page.locator("[data-testid=job-detail-next]").inner_text().strip() == "paused" and "Resume" in page.locator("[data-testid=job-pause]").inner_text())
        stored = json.loads(ctx.request.get(f"{BASE}/api/jobs/alpha").text()); stored = stored.get("job", stored)
        check("the stored workflow is disabled and does NOT contain the computed list fields", stored["enabled"] is False and not any(k in stored for k in ("recent_runs", "next_run", "run_as", "last_run")), [k for k in stored if k in ("recent_runs", "next_run", "run_as", "last_run")])
        page.locator("[data-testid=job-back]").click(); page.wait_for_timeout(700)
        check("back to the table: alpha is marked PAUSED", page.locator("[data-testid=jobs-table-page]").is_visible() and row("alpha").locator("[data-testid=jobs-paused]").is_visible())
        row("alpha").click(); page.wait_for_selector("[data-testid=job-details]", state="visible"); page.wait_for_timeout(400)
        page.locator("[data-testid=job-pause]").click(); page.wait_for_timeout(1000)
        check("Resume brings the next run back", "03:00" in page.locator("[data-testid=job-detail-next]").inner_text() and not page.locator("[data-testid=job-paused-pill]").is_visible())

        print("runs timeline")
        page.locator("button:has-text('Run History')").click(); page.wait_for_selector("[data-testid=runs-timeline]", state="visible"); page.wait_for_timeout(500)
        tb = page.locator("[data-testid=timeline-bar]")
        check("one bar per run, oldest left, coloured by result", tb.count() == 5 and [tb.nth(i).get_attribute("data-status") for i in range(5)] == ["SUCCESS"] * 3 + ["FAILED"] * 2, tb.count())
        check("statistics: 60% success over 5 finished runs, last success and last failure", page.locator("[data-testid=stat-rate]").inner_text() == "60%" and "5 finished" in page.locator("[data-testid=runs-stats]").inner_text() and page.locator("[data-testid=stat-last-ok]").inner_text() != "-" and page.locator("[data-testid=stat-last-fail]").inner_text() != "-" and page.locator("[data-testid=stat-avg]").inner_text() != "-", page.locator("[data-testid=runs-stats]").inner_text())
        page.screenshot(path="/tmp/jobs_timeline.png")
        tb.nth(4).click(); page.wait_for_selector("[data-testid=run-view]", state="visible", timeout=8000); page.wait_for_timeout(800)
        check("clicking a bar opens that run's page (the failed one)", page.locator("[data-testid=run-status]").inner_text().strip() == "FAILED")

        print("run from the table")
        page.locator("[data-testid=job-back]").click(); page.wait_for_timeout(500)
        before = bars("beta").count()
        row("beta").locator("[data-testid=jobs-run]").click(); page.wait_for_selector("[data-testid=run-view]", state="visible", timeout=8000)
        page.wait_for_function("() => document.querySelector('[data-testid=run-status]').innerText.trim() === 'SUCCESS'", timeout=30000)
        page.locator("[data-testid=job-back]").click(); page.wait_for_timeout(1200)
        check("Run on a row starts the workflow and shows its run page; back in the table its sparkline has grown", before == 1 and bars("beta").count() == 2, (before, bars("beta").count()))

        if os.getenv("LIGHT"):
            page.evaluate("() => { localStorage.setItem('dbx_theme', 'light'); }"); page.reload(wait_until="networkidle"); time.sleep(1)
            ev("() => { const d = Alpine.$data(document.body); d.currentView = 'jobs'; d.fetchJobs(); }"); page.wait_for_timeout(1500); page.screenshot(path="/tmp/jobs_table_light.png")
            row("alpha").click(); page.wait_for_timeout(800); page.locator("button:has-text('Run History')").click(); page.wait_for_timeout(600); page.screenshot(path="/tmp/jobs_timeline_light.png")
        check("no JS errors", not errors, errors)
        b.close()
finally:
    if FAIL: print(sh("docker", "logs", "--tail", "25", "ovui").stderr[-1500:])
    sh("docker", "rm", "-f", "ovui")
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
