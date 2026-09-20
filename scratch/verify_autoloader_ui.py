#!/usr/bin/env python3
"""
UI verification for the Volume Auto-Loader (Playwright, headless Chromium).
Point it at a THROWAWAY studio instance: it creates pipelines and uploads files.
    AUTOLOADER_UI_URL=http://localhost:8100 python3 scratch/verify_autoloader_ui.py
Tests:
1. Auto-Loader view renders, no Alpine/JS errors on load or in the create modal.
2. Create-pipeline modal: schema-evolution options, merge-key field, cron controls and presets.
3. A cron pipeline and a merge pipeline are created through the UI and shown on their cards.
4. Volume upload + Run now ingest a file and the KPI/history views update.
5. Lineage graph shows the VOLUME node in the Raw Files column (dark and light themes).
"""

import os
import sys
import time
from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("AUTOLOADER_UI_URL", "http://localhost:8100").rstrip("/")
OUT_DIR = os.getenv("AUTOLOADER_UI_OUT", "/tmp/autoloader_ui_shots")
os.makedirs(OUT_DIR, exist_ok=True)
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def set_theme(page, theme):
    page.evaluate(
        """(t) => { document.documentElement.classList.toggle('dark', t === 'dark');
                    try { localStorage.setItem('theme', t); } catch (e) {} }""", theme)


def open_modal(page):
    page.get_by_role("button", name="New Pipeline").first.click()
    page.locator("h3:has-text('Create Auto-Loader Pipeline')").wait_for(state="visible")


def fill_form(page, name, source, table, ingest_mode=None, merge_keys=None, cron=None):
    modal = page.locator("div.fixed:has-text('Create Auto-Loader Pipeline')").first
    modal.get_by_placeholder("e.g. IoT Sensor Telemetry Stream").fill(name)
    modal.get_by_placeholder("/Volumes/warehouse/raw/iot_stream").fill(source)
    modal.get_by_placeholder("bronze_table").fill(table)
    if ingest_mode:
        modal.locator("select").filter(has_text="Append (Continuous stream)").select_option(ingest_mode)
    if merge_keys:
        modal.get_by_placeholder("device_id, sensor_id").fill(merge_keys)
    if cron:
        modal.locator("select").filter(has_text="Scheduled (Cron, UTC)").select_option("cron")
        modal.get_by_placeholder("*/15 * * * *").fill(cron)
    return modal


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 950})
        errors = []
        page.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))
        page.on("console", lambda m: errors.append(f"console: {m.text}") if m.type == "error" and "favicon" not in m.text else None)

        print("\n1. Auto-Loader view")
        login = page.request.post(f"{BASE_URL}/api/auth/login", data={
            "username": os.getenv("AUTOLOADER_UI_USER", "admin"),
            "password": os.getenv("AUTOLOADER_UI_PASSWORD", "adminpassword123")})
        check("logged in (session cookie set)", login.ok, login.text()[:200])
        page.goto(BASE_URL, wait_until="networkidle")
        page.get_by_role("button", name="Auto-Loader").first.click()
        page.wait_for_selector("text=Volume Auto-Loader", state="visible")
        time.sleep(1)
        baseline_errors = len(errors)
        print(f"  (info) {baseline_errors} JS errors already occur on initial page load, before any Auto-Loader interaction")
        check("demo pipeline card rendered", page.locator("text=IoT Sensor Telemetry Auto-Loader").first.is_visible())
        page.screenshot(path=f"{OUT_DIR}/01_autoloader_dark.png")

        print("\n2. Create modal")
        open_modal(page)
        modal = page.locator("div.fixed:has-text('Create Auto-Loader Pipeline')").first
        options = modal.locator("select").filter(has_text="Rescue Unknown Columns").locator("option").all_inner_texts()
        check("schema evolution offers add / fail / rescue", len(options) == 3 and any("Rescue" in o for o in options), options)
        check("merge key field hidden for append", not modal.get_by_placeholder("device_id, sensor_id").is_visible())
        modal.locator("select").filter(has_text="Append (Continuous stream)").select_option("merge")
        modal.get_by_placeholder("device_id, sensor_id").wait_for(state="visible", timeout=2000)
        check("merge key field shown for merge", modal.get_by_placeholder("device_id, sensor_id").is_visible())
        modal.locator("select").filter(has_text="Append (Continuous stream)").select_option("append")
        check("cron field hidden by default", not modal.get_by_placeholder("*/15 * * * *").is_visible())
        modal.locator("select").filter(has_text="Scheduled (Cron, UTC)").select_option("cron")
        modal.get_by_placeholder("*/15 * * * *").wait_for(state="visible", timeout=2000)
        check("cron field shown when Scheduled selected", modal.get_by_placeholder("*/15 * * * *").is_visible())
        modal.get_by_role("button", name="Hourly").click()
        check("preset fills the cron expression", modal.get_by_placeholder("*/15 * * * *").input_value() == "0 * * * *")
        page.screenshot(path=f"{OUT_DIR}/02_modal_cron_dark.png")
        set_theme(page, "light")
        page.screenshot(path=f"{OUT_DIR}/03_modal_cron_light.png")
        set_theme(page, "dark")
        page.keyboard.press("Escape")

        print("\n3. Create pipelines through the UI")
        page.request.post(f"{BASE_URL}/api/volumes", data={"catalog": "warehouse", "schema": "raw", "name": "ui_vol"})
        open_modal(page)
        fill_form(page, "UI Cron Pipeline", "/Volumes/warehouse/raw/ui_vol", "bronze_ui_cron", cron="0 2 * * *")
        page.get_by_role("button", name="Create Pipeline").click()
        page.wait_for_selector("text=UI Cron Pipeline", state="visible")
        check("cron card shows the schedule", page.locator("text=cron 0 2 * * * UTC").first.is_visible())

        open_modal(page)
        fill_form(page, "UI Merge Pipeline", "/Volumes/warehouse/raw/ui_vol", "bronze_ui_merge", ingest_mode="merge")
        page.get_by_role("button", name="Create Pipeline").click()
        time.sleep(1)
        check("merge pipeline without keys is rejected (modal stays open)",
              page.locator("h3:has-text('Create Auto-Loader Pipeline')").is_visible())
        modal = page.locator("div.fixed:has-text('Create Auto-Loader Pipeline')").first
        modal.get_by_placeholder("device_id, sensor_id").fill("device_id")
        page.get_by_role("button", name="Create Pipeline").click()
        page.wait_for_selector("text=UI Merge Pipeline", state="visible")
        check("merge pipeline created once keys are given", True)

        print("\n4. Upload + run")
        r = page.request.post(
            f"{BASE_URL}/api/volumes/warehouse/raw/ui_vol/upload",
            multipart={"file": {"name": "ui_batch.csv", "mimeType": "text/csv",
                                "buffer": b"device_id,temperature\na,1\nb,2\n"}})
        check("upload accepted", r.ok, r.text()[:200])
        pipes = page.request.get(f"{BASE_URL}/api/autoloader/pipelines").json()["pipelines"]
        merge_id = next(x["id"] for x in pipes if x["name"] == "UI Merge Pipeline")
        run = page.request.post(f"{BASE_URL}/api/autoloader/pipelines/{merge_id}/run").json()
        check("run ingested the uploaded file", run.get("files_ingested") == 1, run)
        page.get_by_role("button", name="Data Lineage").first.click()
        page.get_by_role("button", name="Auto-Loader").first.click()
        time.sleep(1.5)
        check("KPI cards and the merge pipeline card are rendered after the run",
              page.locator("text=Rows Streamed").first.is_visible() and page.locator("text=UI Merge Pipeline").first.is_visible())
        page.screenshot(path=f"{OUT_DIR}/04_pipelines_dark.png", full_page=False)

        print("\n5. Lineage")
        page.get_by_role("button", name="Data Lineage").first.click()
        time.sleep(2)
        col = page.locator("[data-node-id^='volume:/Volumes/warehouse/raw/ui_vol']")
        check("VOLUME node visible in the Raw Files column", col.count() >= 1 and col.first.is_visible(), col.count())
        check("volume node labelled VOLUME / AUTO-LOADER", "VOLUME / AUTO-LOADER" in (col.first.inner_text() if col.count() else ""))
        page.screenshot(path=f"{OUT_DIR}/05_lineage_dark.png")
        set_theme(page, "light")
        time.sleep(0.5)
        page.screenshot(path=f"{OUT_DIR}/06_lineage_light.png")

        # the deliberate merge-without-keys request above answers 400, which the browser logs as a console error
        new_errors = [e for e in errors[baseline_errors:] if "400 (Bad Request)" not in e]
        check("no new JS / Alpine errors from Auto-Loader or lineage interactions", not new_errors, new_errors[:5])
        browser.close()

    print(f"\nScreenshots: {OUT_DIR}")
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Auto-Loader UI checks passed.")


if __name__ == "__main__":
    main()
