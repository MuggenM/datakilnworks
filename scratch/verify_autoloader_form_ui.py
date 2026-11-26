#!/usr/bin/env python3
"""UI check of the Auto-Loader create dialog (Playwright, /usr/bin/python3) against a THROWAWAY studio with an S3 mount 'lake_s3'
and a read-only one 'ro':  GIT_UI_URL=http://localhost:8117 python scratch/verify_autoloader_form_ui.py"""
import os, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1500, "height": 1100}); page = ctx.new_page()
    errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("login", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)
    page.evaluate("async () => { const d = Alpine.$data(document.body); d.currentView = 'autoloader'; await d.fetchAutoloaderPipelines(); d.showCreatePipelineModal = true; }")
    cat = page.locator("[data-testid=pipeline-catalog]:visible"); cat.wait_for(timeout=8000); time.sleep(0.8)
    opts = cat.locator("option").all_inner_texts()
    check("catalog is a dropdown: local catalogs + the writable S3 mount (marked), not the read-only one", opts[0] == "warehouse" and "lake_s3 (S3)" in opts and not any(o.startswith("ro") for o in opts), opts)
    fmt = page.locator("[data-testid=pipeline-format]:visible")
    check("file format dropdown offers the supported formats", set(fmt.locator("option").all_inner_texts()) >= {"CSV (*.csv)", "Parquet (*.parquet)", "JSON (*.json)", "Custom pattern…"})
    fmt.select_option("*.parquet")
    check("choosing Parquet sets the glob", page.evaluate("() => Alpine.$data(document.body).newPipelineForm.file_pattern") == "*.parquet")
    check("custom input hidden for a standard format", not page.locator("[data-testid=pipeline-pattern]:visible").count())
    fmt.select_option("custom")
    page.locator("[data-testid=pipeline-pattern]:visible").fill("sensor_*.csv")
    check("custom pattern is used as typed", page.evaluate("() => Alpine.$data(document.body).newPipelineForm.file_pattern") == "sensor_*.csv")
    cat.select_option("lake_s3")
    check("S3 catalog selectable", page.evaluate("() => Alpine.$data(document.body).newPipelineForm.target_catalog") == "lake_s3")
    page.screenshot(path="/tmp/autoloader_form.png")
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
