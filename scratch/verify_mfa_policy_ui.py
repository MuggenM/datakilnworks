#!/usr/bin/env python3
"""MFA policy UI (Playwright, /usr/bin/python3) against a THROWAWAY studio container `mpui` (admin / adminpassword123), port 8117:
  docker run -d --name mpui -p 8117:8891 -v $PWD/web:/workspace/web -v $PWD/docs:/workspace/docs -w /workspace -e WAREHOUSE_DIR=/workspace/warehouse \
     -e INIT_ADMIN_USERNAME=admin -e INIT_ADMIN_PASSWORD_HASH='<hash of adminpassword123>' localspark-lakehouse-notebook uvicorn web.app:app --host 0.0.0.0 --port 8891
  GIT_UI_URL=http://localhost:8117 python scratch/verify_mfa_policy_ui.py"""
import base64, hashlib, hmac, os, struct, subprocess, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def totp(b32, at=None):
    key = base64.b32decode(b32); ctr = int((at or time.time()) // 30); h = hmac.new(key, struct.pack(">Q", ctr), hashlib.sha1).digest()
    o = h[-1] & 0xF; return "%06d" % ((struct.unpack(">I", h[o:o + 4])[0] & 0x7FFFFFFF) % 10 ** 6)
def ev(page, js, tries=4):
    for i in range(tries):
        try: return page.evaluate(js)
        except Exception as e:
            print("   (evaluate retry:", str(e)[:120].replace("\n", " "), page.url, ")")
            if "context was destroyed" not in str(e) or i == tries - 1: raise
            page.wait_for_load_state("networkidle"); time.sleep(3)
def sql(q):
    subprocess.run(["docker", "exec", "mpui", "python", "-c", f"import sqlite3;c=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute({q!r});c.commit()"], check=True, capture_output=True)
with sync_playwright() as p:
    b = p.chromium.launch()
    actx = b.new_context(viewport={"width": 1600, "height": 1100}); admin = actx.new_page(); errs = []; admin.on("pageerror", lambda e: errs.append(str(e))); admin.on("framenavigated", lambda f: print("   NAV admin", time.strftime("%X"), f.url)); admin.on("request", lambda r: print("   REQ", r.method, r.url) if r.is_navigation_request() else None)
    check("admin logs in", actx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    sql("UPDATE users SET must_change_password = 0")
    admin.goto(BASE, wait_until="networkidle"); time.sleep(5)
    admin.evaluate("() => { const d = Alpine.$data(document.body); d.showIamModal = true; d.iamTab = 'mfa'; d.loadMfaPolicy(); }"); time.sleep(1.5)
    admin.locator("[data-testid=mfa-policy-enabled]").check(); admin.locator("[data-testid=mfa-grace-days]").fill("7"); admin.locator("[data-testid=mfa-policy-save]").click(); time.sleep(1.5)
    check("saving is refused while the administrator has no second factor of their own", "your own account" in admin.locator("[data-testid=mfa-policy-error]").inner_text())
    s = actx.request.post(f"{BASE}/api/auth/mfa/setup").json(); secret = s["secret"]
    check("the administrator enrols", actx.request.post(f"{BASE}/api/auth/mfa/enable", data={"code": totp(secret)}).ok)
    admin.locator("[data-testid=mfa-policy-save]").click(); time.sleep(1.5)
    check("after that the policy saves and the statistics appear", admin.locator("[data-testid=mfa-stats]").is_visible() and admin.locator("[data-testid=mfa-policy-error]").inner_text() == "")
    check("the admin counts as enrolled", "1" in admin.locator("[data-testid=mfa-stat-enrolled]").inner_text())
    # a user under the policy
    check("a user is created", actx.request.post(f"{BASE}/api/users", data={"username": "ui_alice", "password": "alicepassword1", "display_name": "Alice", "role": "user"}).ok)
    uctx = b.new_context(viewport={"width": 1400, "height": 900}); user = uctx.new_page(); uerrs = []; user.on("pageerror", lambda e: uerrs.append(str(e)))
    check("alice logs in", uctx.request.post(f"{BASE}/api/auth/login", data={"username": "ui_alice", "password": "alicepassword1"}).ok)
    user.goto(BASE, wait_until="networkidle"); time.sleep(3)
    banner = user.locator("[data-testid=mfa-grace-banner]")
    check("during the grace period she sees a reminder with the days left", banner.is_visible() and "7 day" in banner.inner_text(), banner.inner_text() if banner.count() else "no banner")
    banner.locator("button:has-text('Later')").click(); time.sleep(0.5)
    check("the reminder can be dismissed for now", not banner.is_visible())
    ev(admin, "() => { Alpine.$data(document.body).loadMfaPolicy(); }"); time.sleep(1.5)
    check("the administrator sees her as 'in grace'", "grace" in admin.locator("[data-testid=mfa-attention-row]").first.inner_text())
    # the deadline passes
    sql("UPDATE mfa_policy SET enabled_at = strftime('%s','now') - 30*86400"); sql("UPDATE users SET created_at = '2020-01-01 00:00:00' WHERE username = 'ui_alice'"); time.sleep(3)
    user.reload(wait_until="networkidle"); time.sleep(3)
    forced = user.locator("[data-testid=mfa-forced-note]")
    check("after the deadline enrolment is forced: the notice appears", forced.is_visible())
    user.keyboard.press("Escape"); time.sleep(0.5)
    check("the forced dialog cannot be dismissed", user.locator("[data-testid=mfa-modal]").is_visible())
    check("the API refuses everything else meanwhile", uctx.request.get(f"{BASE}/api/autoloader/pipelines").status == 403)
    ev(admin, "() => { Alpine.$data(document.body).loadMfaPolicy(); }"); time.sleep(1.5)
    check("the administrator sees her as overdue", "overdue" in admin.locator("[data-testid=mfa-attention-row]").first.inner_text() and admin.locator("[data-testid=mfa-stat-overdue]").inner_text() == "1")
    # extend, then remove the extension, then exempt
    admin.once("dialog", lambda d: d.accept("5")); admin.locator("[data-testid=mfa-extend]").first.click(); time.sleep(1.5)
    check("an extension moves her back into the grace period", "grace" in admin.locator("[data-testid=mfa-attention-row]").first.inner_text() and "extended" in admin.locator("[data-testid=mfa-attention-row]").first.inner_text())
    admin.once("dialog", lambda d: d.accept("0")); admin.locator("[data-testid=mfa-extend]").first.click(); time.sleep(1.5)
    check("removing it blocks her again", "overdue" in admin.locator("[data-testid=mfa-attention-row]").first.inner_text())
    admin.once("dialog", lambda d: d.accept("shared kiosk account")); admin.locator("[data-testid=mfa-exempt]").first.click(); time.sleep(1.5)
    check("an exemption shows its reason and lifts the block", "shared kiosk account" in admin.locator("[data-testid=mfa-attention-row]").first.inner_text() and uctx.request.get(f"{BASE}/api/autoloader/pipelines").status == 200)
    admin.locator("[data-testid=mfa-unexempt]").first.click(); time.sleep(1.5)
    check("removing the exemption blocks her again", uctx.request.get(f"{BASE}/api/autoloader/pipelines").status == 403)
    # she enrols through the forced dialog
    user.reload(wait_until="networkidle"); time.sleep(3)
    user.locator("button:has-text('Set up')").first.click(); user.locator("text=Enter this key manually").wait_for(timeout=5000)
    secret2 = user.evaluate("() => Alpine.$data(document.body).mfa.setup.secret")
    user.locator("input[placeholder='123456']").last.fill(totp(secret2)); user.locator("button:has-text('Turn on')").click()
    user.locator("[data-testid=mfa-backups-saved]").wait_for(timeout=8000)
    check("she gets her backup codes", user.evaluate("() => Alpine.$data(document.body).mfa.backupCodes.length") == 10)
    user.locator("[data-testid=mfa-backups-saved]").click(); time.sleep(2)
    check("the dialog closes and the API works again", not user.locator("[data-testid=mfa-modal]").is_visible() and uctx.request.get(f"{BASE}/api/autoloader/pipelines").status == 200)
    ev(admin, "() => { Alpine.$data(document.body).loadMfaPolicy(); }"); time.sleep(1.5)
    check("the statistics now show two enrolled and nobody overdue", admin.locator("[data-testid=mfa-stat-overdue]").inner_text() == "0" and admin.locator("[data-testid=mfa-stat-enrolled]").inner_text().startswith("2") and "100" in admin.locator("[data-testid=mfa-stat-enrolled]").inner_text())
    admin.screenshot(path="/tmp/mfa_policy_ui.png")
    check("no page errors", not errs and not uerrs, (errs, uerrs))
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
