#!/usr/bin/env python3
"""
UI check of the dbt project-files editor (Playwright; /usr/bin/python3) against a THROWAWAY studio whose dbt project is at
/workspace/dbt_project (a copy of the repo's), admin/adminpassword123 without forced password change, and one S3 mount 'mount_lake':
    DBT_CFG_UI_URL=http://localhost:8117 /usr/bin/python3 scratch/verify_dbt_config_ui.py
"""
import os
import sys
import time

from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("DBT_CFG_UI_URL", "http://localhost:8117").rstrip("/")
OUT_DIR = os.getenv("DBT_CFG_UI_OUT", "/tmp/dbt_cfg_ui_shots")
os.makedirs(OUT_DIR, exist_ok=True)
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1500, "height": 1000})
        page = ctx.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        check("logged in", ctx.request.post(f"{BASE_URL}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
        page.goto(BASE_URL, wait_until="networkidle")
        time.sleep(1)
        act = lambda code: page.evaluate(f"async () => {{ const d = Alpine.$data(document.body); {code} }}")
        act("d.currentView = 'dbt'; await d.fetchDbtAll();")
        page.click("button:has-text('Project files'):visible")
        page.wait_for_selector("text=dbt project files >> visible=true", timeout=8000)
        modal = page.locator("div.fixed:has-text('dbt project files'):visible").last
        ta = modal.locator("textarea")
        check("the modal shows profiles.yml with the duckrun adapter", "type: duckrun" in ta.input_value())
        page.screenshot(path=f"{OUT_DIR}/editor.png")

        # an invalid edit: refused with a reason, nothing saved
        ta.fill("localspark_dbt:\n  target: dev\n")
        modal.locator("button:has-text('Save')").last.click()
        page.wait_for_selector("text=needs an `outputs:` mapping >> visible=true", timeout=8000)
        check("an invalid profile is refused with the reason shown", page.locator("text=needs an `outputs:` mapping").is_visible())
        modal.locator("button:has-text('Discard changes')").last.click()
        check("Discard restores the file", "type: duckrun" in ta.input_value())

        # a valid edit
        ta.fill(ta.input_value().replace("threads: 2", "threads: 3"))
        modal.locator("button:has-text('Validate')").last.click()
        page.wait_for_selector("text=Valid: dbt parsed it >> visible=true", timeout=30000)
        check("Validate runs dbt parse and says so", True)
        modal.locator("button:has-text('Save')").last.click()
        page.wait_for_selector("text=Saved. The previous version was kept. >> visible=true", timeout=30000)
        check("a valid edit saves, keeping the previous version", modal.locator("select:has(option:has-text('Previous versions'))").count() >= 1)
        page.screenshot(path=f"{OUT_DIR}/saved.png")

        # the guided settings edit the text (and keep the comments)
        check("the settings panel shows the current values", modal.locator("input[type=number]").input_value() == "3")
        comments_before = ta.input_value().count("#")
        modal.locator("input[type=text]").first.fill("analytics")
        modal.locator("input[type=number]").fill("5")
        modal.locator("select:has(option:has-text('Local warehouse'))").select_option("mount_lake")
        modal.locator("button:has-text('Apply to the files')").click()
        modal.locator("text=Applied to the files below").wait_for(timeout=8000)
        text = ta.input_value()
        check("'Apply' edits the YAML: schema, threads and the S3 mount with env-var credentials", "schema: analytics" in text and "threads: 5" in text and "s3://lake" in text and "DKW_MOUNT_MOUNT_LAKE_SECRET" in text and "hunter" not in text, text[:300])
        check("...and keeps every comment", text.count("#") == comments_before, (comments_before, text.count("#")))
        page.screenshot(path=f"{OUT_DIR}/settings_applied.png")
        modal.locator("button:has-text('Save')").last.click()
        modal.locator("text=Saved. The previous version was kept.").wait_for(timeout=30000)
        check("the applied settings save through the normal validation", True)
        check("after saving, the panel shows the saved values", modal.locator("input[type=number]").input_value() == "5")

        # dbt_project.yml tab
        modal.locator("button:has-text('dbt_project.yml')").first.click()
        check("the other tab shows dbt_project.yml", "profile:" in ta.input_value())
        check("no JS errors", not errors, errors)
        browser.close()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All dbt config UI checks passed.")


if __name__ == "__main__":
    main()
