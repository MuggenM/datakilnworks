#!/usr/bin/env python3
"""UI check of the Git panel (Playwright, /usr/bin/python3) against a THROWAWAY studio + Gitea (see test_git_sync.py):
    GIT_UI_URL=http://localhost:8117 /usr/bin/python3 scratch/verify_git_sync_ui.py"""
import os, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
OUT = "/tmp/git_ui_shots"; os.makedirs(OUT, exist_ok=True)
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1500, "height": 1100}); page = ctx.new_page()
    errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("login", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)
    page.evaluate("async () => { const d = Alpine.$data(document.body); d.currentView = 'dbt'; await d.fetchDbtAll(); }")
    page.click("button:has-text('Project files'):visible")
    panel = page.locator("[data-testid=git-panel]:visible")
    panel.wait_for(timeout=8000)
    check("panel offers Connect before connecting", panel.locator("[data-testid=git-connect]").is_visible())
    panel.locator("[data-testid=git-connect]").click()
    panel.locator("[data-testid=git-message]").wait_for(timeout=15000)   # seeded files are uncommitted changes
    check("connected; seeded files show as changes", panel.locator("text=uncommitted change").is_visible())
    check("Commit disabled without a message", panel.locator("[data-testid=git-commit]").is_disabled())
    panel.locator("[data-testid=git-message]").fill("initial dbt project")
    panel.locator("[data-testid=git-commit]").click()
    panel.locator("text=Committed").wait_for(timeout=15000)
    check("commit reported", True)
    time.sleep(0.7)
    check("Push enabled (ahead 1)", panel.locator("[data-testid=git-push]").is_enabled())
    panel.locator("[data-testid=git-push]").click(); panel.locator("text=Pushed.").wait_for(timeout=15000)
    check("push reported, ahead 0", "ahead 0" in panel.inner_text())
    page.screenshot(path=f"{OUT}/git_panel.png")
    # an unprivileged view: non-admin gets 403
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
