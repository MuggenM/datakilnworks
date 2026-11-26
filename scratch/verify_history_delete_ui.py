#!/usr/bin/env python3
"""Admin-only deletion of query history (Playwright, /usr/bin/python3) against a THROWAWAY studio with users admin and bob (role user,
password bobpassword123) and history rows q1..q5 (q1,q2 owned by bob):  GIT_UI_URL=http://localhost:8117 python scratch/verify_history_delete_ui.py"""
import os, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
with sync_playwright() as p:
    b = p.chromium.launch()
    # a normal user: append-only
    uctx = b.new_context(viewport={"width": 1500, "height": 1000}); u = uctx.new_page()
    check("bob logs in", uctx.request.post(f"{BASE}/api/auth/login", data={"username": "bob", "password": "bobpassword123"}).ok)
    check("user cannot delete entries (403)", uctx.request.post(f"{BASE}/api/history/delete", data={"query_ids": ["q1"]}).status == 403)
    check("user cannot clear the history (403)", uctx.request.delete(f"{BASE}/api/history").status == 403)
    u.goto(BASE, wait_until="networkidle"); time.sleep(1)
    u.evaluate("async () => { const d = Alpine.$data(document.body); d.currentView = 'history'; await d.fetchHistory(); }")
    time.sleep(0.5)
    check("user sees no checkboxes and no Clear History", u.locator("[data-testid=hist-row-select]:visible").count() == 0 and u.locator("button:has-text('Clear History'):visible").count() == 0)

    ctx = b.new_context(viewport={"width": 1500, "height": 1000}); page = ctx.new_page()
    errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("admin logs in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)
    page.evaluate("async () => { const d = Alpine.$data(document.body); d.currentView = 'history'; await d.fetchHistory(); }")
    rows = page.locator("[data-testid=hist-row-select]:visible"); rows.first.wait_for(timeout=8000)
    total = len(ctx.request.get(f"{BASE}/api/history?limit=100").json()["history"])   # the studio's own background queries are in there too
    check("admin sees a checkbox per entry", rows.count() == total >= 5, (rows.count(), total))
    check("Delete selected hidden until something is selected", page.locator("[data-testid=hist-delete-selected]:visible").count() == 0)
    for sql in ("select 1 from t", "select 2 from t"):
        page.locator(f"tr:has-text('{sql}'):visible [data-testid=hist-row-select]").first.check()
    btn = page.locator("[data-testid=hist-delete-selected]:visible"); btn.wait_for(timeout=3000)
    check("button counts the selection", "(2)" in btn.inner_text(), btn.inner_text())
    page.once("dialog", lambda d: d.accept()); btn.click(); time.sleep(1.5)
    left = ctx.request.get(f"{BASE}/api/history?limit=50").json()["history"]
    ids = sorted(h["query_id"] for h in left)
    check("only the two selected entries were removed", len(ids) == total - 2 and "q1" not in ids and "q2" not in ids and "q3" in ids, ids[:8])
    check("selection cleared and the list refreshed", page.locator("[data-testid=hist-row-select]:visible").count() == total - 2)
    # select all on the page
    page.locator("[data-testid=hist-select-all]:visible").check(); page.once("dialog", lambda d: d.accept())
    page.locator("[data-testid=hist-delete-selected]:visible").click(); time.sleep(1.5)
    check("select-all deletes the rest", not ctx.request.get(f"{BASE}/api/history?limit=50").json()["history"])
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
