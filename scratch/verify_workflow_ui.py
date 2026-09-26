#!/usr/bin/env python3
"""Workflow orchestration UI (Playwright, /usr/bin/python3) against a THROWAWAY studio (admin / adminpassword123):  GIT_UI_URL=http://localhost:8117 python scratch/verify_workflow_ui.py"""
import os, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1600, "height": 1100}); page = ctx.new_page()
    ctx.add_init_script("try { localStorage.setItem('dkwJobView', 'list'); } catch (e) {}")   # this test covers the classic list view and dialogs
    errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("admin logs in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    page.goto(BASE, wait_until="networkidle"); time.sleep(3)
    D = lambda js: page.evaluate("async () => { const d = Alpine.$data(document.body); " + js + " }")
    D("d.currentView = 'jobs'; await d.fetchJobs(); d.openCreateJobModal();")
    time.sleep(0.5)
    page.locator("input[placeholder*='Daily Medallion']").fill("UI orchestration job")
    page.locator("[data-testid=job-orchestration] >> text=+ parameter").click()
    pr = page.locator("[data-testid=job-orchestration] input[placeholder=name]").first; pr.fill("region")
    page.locator("[data-testid=job-orchestration] input[placeholder=default]").first.fill("EMEA")
    page.locator("[data-testid=job-orchestration] input[placeholder*='allowed values']").first.fill("EMEA, APAC")
    page.locator("[data-testid=job-orchestration] >> text=+ notification").click()
    page.locator("[data-testid=job-orchestration] input[placeholder*='a@x.org']").fill("ops@example.org")
    page.locator("[data-testid=job-orchestration] >> text=+ trigger").click()
    D("d.jobForm.triggers[0].type = 'table'; d.jobForm.triggers[0].table = 'dbo.watched';")
    page.locator("[data-testid=job-orchestration] input[type=number]").nth(1).fill("2")
    res = page.locator("[data-testid=task-resilience]").first
    res.locator("input").nth(0).fill("2"); res.locator("input").nth(1).fill("1")
    page.locator("textarea[placeholder*='CREATE OR REPLACE']").first.fill("select * from dbo.missing_table_for_ui, (select '{{ params.region }}')")
    page.locator("button:has-text('Save Pipeline')").click(); time.sleep(1.5)
    saved = [j for j in ctx.request.get(f"{BASE}/api/jobs").json()["jobs"] if j["name"] == "UI orchestration job"]
    j = saved[0] if saved else {}
    check("the job is saved with the orchestration fields", bool(saved) and j["max_concurrent_runs"] == 2 and j["parameters"][0]["allowed"] == ["EMEA", "APAC"] and j["tasks"][0]["retries"] == 2
          and j["triggers"] == [{"type": "table", "table": "dbo.watched"}] and j["notifications"][0]["on"] == ["failure"] and j["notifications"][0]["target"] == "ops@example.org", j)
    # invalid save is refused with the server message
    dlg = []; page.on("dialog", lambda d: (dlg.append(d.message), d.accept()))
    D("d.openCreateJobModal(); d.jobForm.name='bad'; d.jobForm.tasks[0].query=''; d.jobForm.tasks[0].parameters.query='select {{ params.nope }}'; await d.saveJob();")
    time.sleep(1); check("a job referencing an undeclared parameter is refused with the reason", any("declares no parameter" in m for m in dlg), dlg)
    D("d.showJobModal = false; await d.fetchJobs(); d.selectJob(d.jobs.find(j => j.name === 'UI orchestration job'));")
    time.sleep(0.5)
    page.locator("button[\@click*='runJobNow(selectedJob.id)']").click(); dlgbox = page.locator("[data-testid=run-params-dialog]"); dlgbox.wait_for(timeout=5000)
    check("Run Now asks for the declared parameters, prefilled with defaults", dlgbox.locator("select").first.input_value() == "EMEA")
    dlgbox.locator("select").first.select_option("APAC"); page.locator("[data-testid=run-params-start]").click()
    page.locator("[data-testid=repair-run]:visible").wait_for(timeout=45000)
    d = page.evaluate("() => Alpine.$data(document.body).selectedRunDetail")
    check("the run ended FAILED after 3 attempts and recorded the parameters", d["status"] == "FAILED" and d["tasks_detail"][0]["attempt_count"] == 3 and '"APAC"' in d["run_params"], (d["status"], d["run_params"]))
    check("attempts and parameters are visible", page.locator("[data-testid=task-attempts]:visible").count() == 1 and "region = APAC" in page.locator("[data-testid=run-params]").inner_text())
    check("the notification delivery is shown", page.locator("[data-testid=run-notifications]:visible").count() == 1)
    page.locator("[data-testid=repair-run]:visible").click(); time.sleep(3)
    pd = page.evaluate("() => Alpine.$data(document.body).selectedRunDetail.parent_run_id"); check("Repair starts a linked run", pd == d["run_id"], (pd, d["run_id"]))
    page.screenshot(path="/tmp/wf_ui.png")
    # cancel a long run from the UI
    r = ctx.request.post(f"{BASE}/api/jobs", data={"id": "long_ui", "name": "Long", "tasks": [{"id": "s", "name": "s", "type": "sql", "depends_on": [], "parameters": {"query": "select count(*) from range(100000000000)"}}]}); check("long job saved", r.ok, r.text())
    D("await d.fetchJobs(); d.selectJob(d.jobs.find(j => j.id === 'long_ui')); await d.runJobNow('long_ui');")
    page.locator("[data-testid=cancel-run]:visible").wait_for(timeout=15000); page.locator("[data-testid=cancel-run]:visible").click()
    for _ in range(30):
        st = page.evaluate("() => Alpine.$data(document.body).selectedRunDetail.status")
        if st != "RUNNING": break
        time.sleep(1)
    check("Cancel stops the run (CANCELLED)", st == "CANCELLED", st)
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
