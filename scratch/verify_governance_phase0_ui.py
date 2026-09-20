#!/usr/bin/env python3
"""
UI verification for Governance Phase 0 (notebook role gating). Playwright, headless Chromium.
Point it at a THROWAWAY studio started with GOVERNANCE_RESTRICT_NOTEBOOKS=true:
    GOVERNANCE_UI_URL=http://localhost:8100 python3 scratch/verify_governance_phase0_ui.py
Tests:
1. A plain user never sees a JupyterLab entry point and never receives the token; an admin does.
2. No JS / Alpine errors on load or while switching users.
"""

import os
import sys
import time
from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("GOVERNANCE_UI_URL", "http://localhost:8100").rstrip("/")
OUT_DIR = os.getenv("GOVERNANCE_UI_OUT", "/tmp/governance_ui_shots")
os.makedirs(OUT_DIR, exist_ok=True)
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def session_page(browser, username, password):
    ctx = browser.new_context(viewport={"width": 1440, "height": 950})
    page = ctx.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    ok = ctx.request.post(f"{BASE_URL}/api/auth/login", data={"username": username, "password": password}).ok
    check(f"logged in as {username}", ok)
    page.goto(BASE_URL, wait_until="networkidle")
    time.sleep(1.5)
    return page, errors


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        print("\n1. Plain user")
        page, errors = session_page(browser, "analyst_bob", "userpassword123")
        link = page.locator("a:has-text('JupyterLab')").first
        check("header JupyterLab link hidden", not link.is_visible())
        state = page.evaluate("() => { const d = Alpine.$data(document.body); return {allowed: d.notebooksAllowed, token: d.jupyterToken, url: d.jupyterUrl}; }")
        check("UI state holds no Jupyter token", state["allowed"] is False and state["token"] == "" and state["url"] == "", state)
        check("token not in page source", "datakilnworks" not in page.content().replace("Data Kiln Works", ""))
        page.screenshot(path=f"{OUT_DIR}/p0_user.png")
        check("no JS errors (plain user)", not errors, errors[:3])

        print("\n2. Admin")
        page, errors = session_page(browser, "admin", "adminpassword123")
        check("header JupyterLab link visible", page.locator("a:has-text('JupyterLab')").first.is_visible())
        state = page.evaluate("() => { const d = Alpine.$data(document.body); return {allowed: d.notebooksAllowed, url: d.jupyterUrl}; }")
        check("UI state carries the Jupyter URL", state["allowed"] is True and "token=" in state["url"] and not state["url"].endswith("token="), state)
        page.screenshot(path=f"{OUT_DIR}/p0_admin.png")
        check("no JS errors (admin)", not errors, errors[:3])
        browser.close()

    print(f"\nScreenshots: {OUT_DIR}")
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Governance Phase 0 UI checks passed.")


if __name__ == "__main__":
    main()
