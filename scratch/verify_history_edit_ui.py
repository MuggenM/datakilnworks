#!/usr/bin/env python3
"""UI check of editing a history query (Playwright, /usr/bin/python3) against a THROWAWAY studio seeded with a history row
'q_hist1' (unqualified names) and tables sales.orders / sales.customers:  GIT_UI_URL=http://localhost:8117 python scratch/verify_history_edit_ui.py"""
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
    r = ctx.request.post(f"{BASE}/api/sql/qualify", data={"sql": "select * from nope", "catalog": "warehouse"})
    check("qualify API reports unknown tables", r.ok and r.json()["unresolved"][0]["name"] == "nope", r.text())
    check("qualify API refuses unparsable SQL", ctx.request.post(f"{BASE}/api/sql/qualify", data={"sql": "select from from"}).status == 400)
    anon = p.request.new_context(); check("qualify API needs a session", anon.post(f"{BASE}/api/sql/qualify", data={"sql": "select 1"}).status == 401)
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)
    page.evaluate("async () => { const d = Alpine.$data(document.body); d.currentView = 'history'; await d.fetchHistory(); }")
    page.locator("span:has-text('select o.id from orders'):visible").first.click()
    ta = page.locator("[data-testid=hist-sql]:visible"); ta.wait_for(timeout=8000)
    check("history SQL is shown in an editable box", "from orders o" in ta.input_value())
    page.locator("[data-testid=hist-qualify]:visible").click()
    page.locator("[data-testid=hist-notes]:visible").wait_for(timeout=8000)
    v = ta.input_value()
    check("names completed to catalog.schema.table", "warehouse.sales.orders o" in v and "warehouse.sales.customers c" in v, v)
    check("comment and formatting kept", v.endswith("-- why"), v)
    ta.fill(v + "\n limit 5")
    page.locator("[data-testid=hist-save]:visible").click()
    page.locator("text=Save Query >> visible=true").first.wait_for(timeout=8000)
    modal_sql = page.evaluate("() => Alpine.$data(document.body).queryForm.query_text")
    check("Save dialog opens with the edited SQL", "limit 5" in modal_sql and "warehouse.sales.orders" in modal_sql, modal_sql)
    page.evaluate("() => { const d = Alpine.$data(document.body); d.queryForm.name = 'Qualified orders'; }")
    page.locator("button:has(span:text-is('Save Query')):visible").click()
    time.sleep(2)
    saved = ctx.request.get(f"{BASE}/api/queries").json()
    saved = saved.get("queries", saved) if isinstance(saved, dict) else saved
    check("saved as a query with the edited text", any(q["name"] == "Qualified orders" and "warehouse.sales.orders" in q["query_text"] for q in saved), saved)
    hist = ctx.request.get(f"{BASE}/api/history/q_hist1").json()
    check("the history record itself is unchanged", "from orders o" in hist["query_text"] and "warehouse." not in hist["query_text"], hist["query_text"])
    page.screenshot(path="/tmp/hist_edit.png")
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
