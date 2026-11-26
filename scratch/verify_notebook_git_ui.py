#!/usr/bin/env python3
"""UI check of Workspace > Git (Playwright, /usr/bin/python3) against a THROWAWAY studio + Gitea:
    GIT_UI_URL=http://localhost:8117 /usr/bin/python3 scratch/verify_notebook_git_ui.py"""
import os, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1500, "height": 1000}); page = ctx.new_page()
    errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("login", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)
    page.evaluate("async () => { const d = Alpine.$data(document.body); d.currentView = 'workspace'; await d.fetchWorkspaceTree(); }")
    page.locator("[data-testid=nb-git-open]:visible").click()
    m = page.locator("[data-testid=nb-git-modal]:visible"); m.wait_for(timeout=8000)
    m.locator("[data-testid=nb-git-connect]").wait_for(timeout=8000)      # status is fetched after the modal opens
    check("Connect offered", True)
    m.locator("[data-testid=nb-git-connect]").click(); m.locator("[data-testid=nb-git-message]").wait_for(timeout=15000)
    check("Shared file shows as a change", "h.py" in m.inner_text())
    m.locator("[data-testid=nb-git-message]").fill("shared helpers"); time.sleep(0.3)
    m.locator("[data-testid=nb-git-commit]").click(); m.locator("text=/^Committed [0-9a-f]+/").wait_for(timeout=15000); time.sleep(0.7)
    m.locator("[data-testid=nb-git-push]").click(); m.locator("text=/^Pushed\\.$/").wait_for(timeout=15000)
    check("pushed, ahead 0", "ahead 0" in m.inner_text())
    page.screenshot(path="/tmp/nb_git.png")
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
