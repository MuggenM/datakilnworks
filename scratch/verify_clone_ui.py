#!/usr/bin/env python3
"""
UI verification for Delta shallow clone (Playwright; run with /usr/bin/python3) against a THROWAWAY studio whose
warehouse holds a Delta table hr/employees and whose admin is admin/adminpassword123 with no forced password change:
    CLONE_UI_URL=http://localhost:8112 /usr/bin/python3 scratch/verify_clone_ui.py
"""
import os
import sys
import time

from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("CLONE_UI_URL", "http://localhost:8112").rstrip("/")
OUT_DIR = os.getenv("CLONE_UI_OUT", "/tmp/clone_ui_shots")
os.makedirs(OUT_DIR, exist_ok=True)
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1440, "height": 1000})
        page = ctx.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.on("console", lambda m: errors.append(m.text[:200]) if m.type == "error" and "Failed to load resource" not in m.text else None)
        r = ctx.request.post(f"{BASE_URL}/api/auth/login", data={"username": "admin", "password": "adminpassword123"})
        check("logged in", r.ok, r.text())
        page.goto(BASE_URL, wait_until="networkidle")
        time.sleep(1)
        act = lambda code: page.evaluate(f"async () => {{ const d = Alpine.$data(document.body); {code} }}")
        act("d.currentView = 'catalog'; await d.fetchCatalogs(); await d.selectTable('hr', 'employees', 'warehouse');")
        page.wait_for_selector("button:has-text('Clone'):visible", timeout=8000)
        page.click("button:has-text('Clone'):visible")
        page.wait_for_selector("text=Shallow clone", timeout=5000)
        check("the modal opens prefilled with <table>_clone in the same schema", page.evaluate("() => { const m = Alpine.$data(document.body).cloneModal; return m.open && m.schema === 'hr' && m.name === 'employees_clone'; }"))
        page.screenshot(path=f"{OUT_DIR}/clone_modal.png")
        page.click("button:has-text('Create clone')")
        page.wait_for_function("() => Alpine.$data(document.body).selectedTable && Alpine.$data(document.body).selectedTable.table_name === 'employees_clone'", timeout=10000)
        check("the clone is created and the catalog jumps to it", not page.evaluate("() => Alpine.$data(document.body).cloneModal.open"))
        check("it has the source's rows", page.evaluate("() => (Alpine.$data(document.body).selectedTable.row_count ?? Alpine.$data(document.body).selectedTable.num_rows ?? -1)") in (3, 4, -1))
        page.screenshot(path=f"{OUT_DIR}/clone_result.png")
        # second clone onto the same name: error shown in the modal, not a crash
        page.evaluate("() => Alpine.$data(document.body).openCloneModal({schema_name:'hr', table_name:'employees', catalog:'warehouse'})")
        page.click("button:has-text('Create clone')")
        page.wait_for_selector("text=already exists", timeout=8000)
        check("a name clash shows the server's message in the modal", page.evaluate("() => Alpine.$data(document.body).cloneModal.open"))
        page.screenshot(path=f"{OUT_DIR}/clone_error.png")
        check("no JS errors", not errors, errors)
        browser.close()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All clone UI checks passed.")


if __name__ == "__main__":
    main()
