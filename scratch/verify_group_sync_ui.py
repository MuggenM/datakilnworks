#!/usr/bin/env python3
"""UI check of directory group sync (Playwright, /usr/bin/python3) against a THROWAWAY studio + throwaway lldap (fixtures as in
test_group_sync_ldap.py: u1 in dkw_analysts, groups dkw_analysts / dkw_ops), the studio's LDAP configured for it:
    GIT_UI_URL=http://localhost:8117 python scratch/verify_group_sync_ui.py"""
import os, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1500, "height": 1200}); page = ctx.new_page()
    errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("login", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)
    page.evaluate("async () => { await Alpine.$data(document.body).openIamModal(); }")
    page.locator("[data-testid=iam-tab-groups]:visible").click()
    panel = page.locator("[data-testid=groups-panel]:visible"); panel.wait_for(timeout=8000)
    panel.locator("[data-testid=group-new-name]").fill("Analysts"); panel.locator("[data-testid=group-create]").click()
    panel.locator("[data-testid=group-directory]").wait_for(timeout=8000)
    panel.locator("[data-testid=group-map-source]").select_option("ldap")
    panel.locator("[data-testid=group-map-browse]").click(); panel.locator("[data-testid=group-map-options]").wait_for(timeout=10000)
    opts = panel.locator("[data-testid=group-map-options] option").all_inner_texts()
    check("Browse lists the directory's groups", any("dkw_analysts" in o for o in opts) and any("dkw_ops" in o for o in opts), opts)
    dn = next(o.split("—")[-1].strip() for o in opts if "dkw_analysts" in o)
    panel.locator("[data-testid=group-map-options]").select_option(dn)
    check("picking one fills the DN", panel.locator("[data-testid=group-map-ref]").input_value() == dn)
    panel.locator("[data-testid=group-map-save]").click(); panel.locator("[data-testid=group-map-msg]").wait_for(timeout=8000)
    check("mapping saved", "Mapping saved" in panel.locator("[data-testid=group-map-msg]").inner_text())
    panel.locator("[data-testid=group-map-sync]").click(); time.sleep(4)
    check("Sync LDAP now reports and loads the members", "Synced" in panel.locator("[data-testid=group-map-msg]").inner_text(), panel.locator("[data-testid=group-map-msg]").inner_text())
    rows = panel.locator("[data-testid=group-member]")
    check("u1 is a member, marked 'via directory'", rows.count() == 1 and "u1" in rows.first.inner_text() and panel.locator("[data-testid=member-synced]").count() == 1, rows.count())
    check("a synced member's Remove is disabled", panel.locator("[data-testid=group-remove]").first.is_disabled())
    api = ctx.request.get(f"{BASE}/api/groups").json()["groups"][0]
    check("the group list reports the mapping", api["source"] == "ldap" and api["external_ref"].lower() == dn.lower(), api)
    # a second group cannot claim the same directory group
    panel.locator("[data-testid=group-new-name]").fill("Copycat"); panel.locator("[data-testid=group-create]").click(); time.sleep(1)
    panel.locator("[data-testid=group-map-source]").select_option("ldap"); panel.locator("[data-testid=group-map-ref]").fill(dn.upper()); panel.locator("[data-testid=group-map-save]").click(); time.sleep(1)
    check("the same directory group cannot be mapped twice", "already mapped" in panel.locator("[data-testid=group-map-error]").inner_text(), panel.locator("[data-testid=group-map-error]").inner_text())
    # clearing the mapping
    panel.locator("[data-testid=group-row]:has-text('Analysts')").click(); time.sleep(1)
    panel.locator("[data-testid=group-map-source]").select_option("local"); page.once("dialog", lambda d: d.accept()); panel.locator("[data-testid=group-map-save]").click(); time.sleep(1.5)
    check("clearing the mapping removes the synced member", panel.locator("[data-testid=group-member]").count() == 0)
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
