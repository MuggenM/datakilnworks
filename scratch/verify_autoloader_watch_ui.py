#!/usr/bin/env python3
"""
End-to-end check of file-watch Auto-Loader triggering in a real studio (daemon running), driven through the UI
(Playwright; /usr/bin/python3) against a THROWAWAY studio whose warehouse dir is bind-mounted at WATCH_UI_WAREHOUSE on the
host, so the file is dropped from OUTSIDE the container (proving host-side events reach the container's inotify):
    WATCH_UI_URL=http://localhost:8113 WATCH_UI_WAREHOUSE=/tmp/xyz/warehouse /usr/bin/python3 scratch/verify_autoloader_watch_ui.py
"""
import os
import sys
import time

from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("WATCH_UI_URL", "http://localhost:8113").rstrip("/")
WAREHOUSE = os.environ["WATCH_UI_WAREHOUSE"]
OUT_DIR = os.getenv("WATCH_UI_OUT", "/tmp/watch_ui_shots")
os.makedirs(OUT_DIR, exist_ok=True)
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1440, "height": 1100})
        page = ctx.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.on("console", lambda m: errors.append(m.text[:200]) if m.type == "error" and "Failed to load resource" not in m.text else None)
        r = ctx.request.post(f"{BASE_URL}/api/auth/login", data={"username": "admin", "password": "adminpassword123"})
        check("logged in", r.ok)
        page.goto(BASE_URL, wait_until="networkidle")
        time.sleep(1)
        act = lambda code: page.evaluate(f"async () => {{ const d = Alpine.$data(document.body); {code} }}")
        act("d.currentView = 'autoloader'; await d.initAutoLoader();")
        page.click("button:has-text('New Pipeline'):visible")
        page.wait_for_selector("text=Create Auto-Loader Pipeline >> visible=true", timeout=5000)
        act("Object.assign(d.newPipelineForm, {name: 'Watch demo', source_volume_path: '/Volumes/warehouse/raw/watchdemo', file_pattern: '*.csv', target_table: 'watch_demo'});")
        page.select_option("select:has(option[value=watch]):visible", "watch")
        page.wait_for_selector("text=Safety-net rescan >> visible=true", timeout=3000)
        check("choosing 'File events' reveals the safety-net rescan option", True)
        page.screenshot(path=f"{OUT_DIR}/watch_form.png")
        page.click("button:has-text('Create Pipeline'):visible")
        page.wait_for_selector("text=Watch demo >> visible=true", timeout=8000)
        deadline = time.time() + 30                           # the watcher may need a retry or two on a busy host
        while time.time() < deadline and not page.locator("text=watching >> visible=true").count():
            act("await d.fetchAutoloaderPipelines();")
            time.sleep(1)
        page.wait_for_selector("text=watching >> visible=true", timeout=3000)
        check("the pipeline card shows the watching badge", True)
        pipe = act_result = page.evaluate("() => Alpine.$data(document.body).autoloaderPipelines.find(p => p.name === 'Watch demo')")
        check("the API stored watch_enabled and the sweep", pipe and pipe["watch_enabled"] is True and pipe["watch_sweep_seconds"] == 300 and not pipe.get("cron_schedule"), pipe)

        vol = os.path.join(WAREHOUSE, "volumes", "warehouse", "raw", "watchdemo")
        t0 = time.time()
        tmp = os.path.join(vol, "data.csv.part")
        with open(tmp, "w") as f:
            f.write("id,val\n" + "\n".join(f"{i},{i * 2}" for i in range(25)) + "\n")
        os.rename(tmp, os.path.join(vol, "data.csv"))          # dropped from the host, atomically
        deadline = time.time() + 20
        rows = 0
        while time.time() < deadline:
            act("await d.fetchAutoloaderPipelines();")
            rows = page.evaluate("() => (Alpine.$data(document.body).autoloaderPipelines.find(p => p.name === 'Watch demo') || {}).total_rows_ingested || 0")
            if rows:
                break
            time.sleep(0.5)
        took = time.time() - t0
        check("a file dropped from the host is ingested", rows == 25, rows)
        check("...in a few seconds, without waiting for a poll tick", took < 12, round(took, 1))
        page.screenshot(path=f"{OUT_DIR}/watch_card.png")
        check("no JS errors", not errors, errors)
        browser.close()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Auto-Loader watch UI checks passed.")


if __name__ == "__main__":
    main()
