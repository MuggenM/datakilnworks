#!/usr/bin/env python3
"""
UI check of an S3 Auto-Loader pipeline (Playwright; /usr/bin/python3). Point it at a THROWAWAY studio that has boto3, an S3
mount 'mount_s3' pointing at a moto/S3 endpoint, and a bucket `landing` holding in/a.csv (id,val; 3 rows):
    S3_UI_URL=http://localhost:8114 /usr/bin/python3 scratch/verify_autoloader_s3_ui.py
"""
import os
import sys
import time

from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("S3_UI_URL", "http://localhost:8114").rstrip("/")
OUT_DIR = os.getenv("S3_UI_OUT", "/tmp/s3_ui_shots")
os.makedirs(OUT_DIR, exist_ok=True)
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1440, "height": 1200})
        page = ctx.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.on("console", lambda m: errors.append(m.text[:200]) if m.type == "error" and "Failed to load resource" not in m.text else None)
        check("logged in", ctx.request.post(f"{BASE_URL}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
        page.goto(BASE_URL, wait_until="networkidle")
        time.sleep(1)
        act = lambda code: page.evaluate(f"async () => {{ const d = Alpine.$data(document.body); {code} }}")
        act("d.currentView = 'autoloader'; await d.initAutoLoader();")
        page.click("button:has-text('New Pipeline'):visible")
        page.wait_for_selector("text=Create Auto-Loader Pipeline >> visible=true", timeout=5000)
        check("the mount selector is hidden for a volume path", not page.locator("text=Storage Mount (endpoint & credentials) >> visible=true").count())
        act("Object.assign(d.newPipelineForm, {name: 'S3 demo', source_volume_path: 's3://landing/in', file_pattern: '*.csv', target_table: 's3_demo', poll_interval_seconds: 10});")
        page.wait_for_selector("text=Storage Mount (endpoint & credentials) >> visible=true", timeout=3000)
        check("typing an s3:// path reveals the storage mount selector", True)
        check("it lists the configured S3 mount", page.evaluate("() => [...document.querySelectorAll('select option')].some(o => o.textContent.includes('mount_s3') || o.value === 'mount_s3')"))
        page.select_option("select:has(option[value=mount_s3]):visible", "mount_s3")
        page.screenshot(path=f"{OUT_DIR}/s3_form.png")
        act("d.newPipelineForm.poll_interval_seconds = 'watch';")
        page.click("button:has-text('Create Pipeline'):visible")
        page.wait_for_selector("text=File events watch local volumes only", timeout=4000)
        check("choosing file events for an S3 path is refused with an explanation", not page.evaluate("() => Alpine.$data(document.body).autoloaderPipelines.some(p => p.name === 'S3 demo')"))
        act("d.newPipelineForm.poll_interval_seconds = 10;")
        page.click("button:has-text('Create Pipeline'):visible")
        page.wait_for_selector("text=S3 demo >> visible=true", timeout=8000)
        rows, pipe = 0, None
        deadline = time.time() + 40
        while time.time() < deadline:
            act("await d.fetchAutoloaderPipelines();")
            pipe = page.evaluate("() => Alpine.$data(document.body).autoloaderPipelines.find(p => p.name === 'S3 demo')")
            rows = pipe.get("total_rows_ingested") or 0
            if rows:
                break
            time.sleep(1.5)
        check("the daemon polls the bucket and ingests the object", rows == 3, pipe)
        check("the pipeline stored the normalised path and mount", pipe["source_volume_path"] == "s3://landing/in/" and pipe["source_mount_id"] == "mount_s3", pipe)
        page.screenshot(path=f"{OUT_DIR}/s3_card.png")
        check("no JS errors", not errors, errors)
        browser.close()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Auto-Loader S3 UI checks passed.")


if __name__ == "__main__":
    main()
