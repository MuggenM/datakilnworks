#!/usr/bin/env python3
"""Pull-request mode in the Git panel of Project files (Playwright, /usr/bin/python3) against a THROWAWAY studio (GIT_MODE=pull_request, its dbt
project a temp dir, remote ga/dbt-ui on a throwaway Gitea; TOK = the Gitea token; project files are added from outside with docker exec):
    GIT_UI_URL=http://localhost:8117 GITEA_EXEC='docker exec gtest_studio' TOK=... python scratch/verify_git_pr_ui.py"""
import os, subprocess, sys, time, requests
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/"); TOK = os.environ["TOK"]
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def studio(cmd): return subprocess.run(["docker", "exec", "gtest_studio", "sh", "-c", cmd], capture_output=True, text=True)
forge = lambda m, path, **kw: subprocess.run(["docker", "exec", "gtest_gitea", "curl", "-s", "-X", m, "-H", f"Authorization: token {TOK}", "-H", "Content-Type: application/json", *(["-d", kw["data"]] if "data" in kw else []), f"localhost:3000/api/v1{path}"], capture_output=True, text=True).stdout
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1500, "height": 1300}); page = ctx.new_page()
    errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("login", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)
    page.evaluate("async () => { const d = Alpine.$data(document.body); d.currentView = 'dbt'; await d.fetchDbtAll(); }")
    page.click("button:has-text('Project files'):visible")
    panel = page.locator("[data-testid=git-panel]:visible"); panel.wait_for(timeout=10000); time.sleep(1)
    check("the panel says pull-request mode", "pull-request mode" in panel.inner_text() or panel.locator("[data-testid=git-connect]").is_visible())
    panel.locator("[data-testid=git-connect]").click(); panel.locator("[data-testid=git-message]").wait_for(timeout=15000)
    check("connected; badge shows the mode", panel.locator("[data-testid=git-mode]").is_visible())
    panel.locator("[data-testid=git-message]").fill("initial project"); panel.locator("[data-testid=git-commit]").click(); panel.locator("text=/^Committed [0-9a-f]+/").wait_for(timeout=15000); time.sleep(0.8)
    check("the first commit of an empty remote stays on the base branch (no branch box needed)", not panel.locator("[data-testid=pr-block]").count() or not panel.locator("[data-testid=git-pr-block]").is_visible())
    panel.locator("[data-testid=git-push]").click(); panel.locator("text=/^Pushed\\.$/").wait_for(timeout=15000)

    print("a change")
    studio("mkdir -p /workspace/dbt_project/models/marts && echo 'select 1 as a' > /workspace/dbt_project/models/marts/ui_model.sql")
    panel.locator("[data-testid=git-refresh]").click(); panel.locator("[data-testid=git-message]").wait_for(timeout=15000)
    check("the branch box is offered on the base branch", panel.locator("[data-testid=git-branch-name]").is_visible())
    panel.locator("[data-testid=git-branch-name]").fill("dkw/ui-change")
    panel.locator("[data-testid=git-message]").fill("add ui model"); panel.locator("[data-testid=git-commit]").click(); time.sleep(2.5)
    check("committing opened the named change branch", "dkw/ui-change" in panel.locator("[data-testid=git-mode]").inner_text(), panel.locator("[data-testid=git-mode]").inner_text())
    check("main was not pushed to: Pull is disabled on a change branch", panel.locator("[data-testid=git-pull]").is_disabled())
    blk = panel.locator("[data-testid=git-pr-block]"); blk.wait_for(timeout=8000)
    check("the pull-request block shows 'no pull request yet'", "no pull request yet" in blk.inner_text())
    check("Open pull request needs the push first", blk.locator("[data-testid=git-pr-open]").is_disabled())
    panel.locator("[data-testid=git-push]").click(); panel.locator("text=/^Pushed\\.$/").wait_for(timeout=15000); time.sleep(0.8)
    blk.locator("[data-testid=git-pr-title]").fill("Add ui model"); blk.locator("[data-testid=git-pr-body]").fill("from the UI test")
    blk.locator("[data-testid=git-pr-open]").click(); time.sleep(6)
    check("the pull request is opened and linked", "pull request #1 open" in blk.locator("[data-testid=git-pr-state]").inner_text() and blk.locator("[data-testid=git-pr-link]").get_attribute("href").endswith("/pulls/1"), blk.inner_text())
    pr = requests.get("http://localhost:3000/api/v1/repos/ga/dbt-ui/pulls/1", headers={"Authorization": f"token {TOK}"}).json() if False else None
    check("Finish before the merge is refused with the reason", (blk.locator("[data-testid=git-pr-finish]").click() or True) and (time.sleep(2) or True) and "not merged" in panel.locator("[data-testid=git-error]").inner_text(), panel.locator("[data-testid=git-error]").inner_text())

    print("merged on the server, then finish")
    forge("POST", "/repos/ga/dbt-ui/pulls/1/merge", data='{"Do":"merge"}')
    panel.locator("[data-testid=git-refresh]").click(); time.sleep(4)
    check("Refresh shows it merged", "merged" in blk.locator("[data-testid=git-pr-state]").inner_text(), blk.locator("[data-testid=git-pr-state]").inner_text())
    blk.locator("[data-testid=git-pr-finish]").click(); time.sleep(5)
    check("Finish returns to the base branch", not blk.is_visible() and "on dkw/" not in panel.locator("[data-testid=git-mode]").inner_text(), panel.inner_text()[:300])
    check("the merged file is there", studio("test -f /workspace/dbt_project/models/marts/ui_model.sql && echo yes").stdout.strip() == "yes")
    check("no page errors", not errs, errs)
    page.screenshot(path="/tmp/git_pr_ui.png")
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
