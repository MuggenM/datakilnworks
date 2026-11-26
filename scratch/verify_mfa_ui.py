#!/usr/bin/env python3
"""
UI verification for TOTP MFA (Playwright). Point it at a THROWAWAY studio bootstrapped with
INIT_ADMIN_USERNAME=admin and INIT_ADMIN_PASSWORD_HASH for `adminpassword123` (run with /usr/bin/python3):
    MFA_UI_URL=http://localhost:8110 /usr/bin/python3 scratch/verify_mfa_ui.py
Flow: forced first-login password change -> enrol MFA from the user menu (QR renders, backup codes shown once) ->
sign in again through the two-step login screen (wrong code is refused, right code succeeds) -> turn MFA off.
"""
import base64
import os
import sys
import time

from playwright.sync_api import sync_playwright

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("WAREHOUSE_DIR", "/tmp/unused_mfa_ui_warehouse")
from web import mfa  # only the pure TOTP helpers are used here

BASE_URL = os.getenv("MFA_UI_URL", "http://localhost:8110").rstrip("/")
OUT_DIR = os.getenv("MFA_UI_OUT", "/tmp/mfa_ui_shots")
os.makedirs(OUT_DIR, exist_ok=True)
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def data(page, expr):
    return page.evaluate(f"() => {{ const d = Alpine.$data(document.body); return {expr}; }}")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1440, "height": 1000})
        page = ctx.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.on("console", lambda m: errors.append(m.text[:200]) if m.type == "error" and "Failed to load resource" not in m.text else None)

        print("1. Forced password change, then MFA enrolment")
        page.goto(BASE_URL, wait_until="networkidle")
        page.fill("input[placeholder='Enter username']", "admin")
        page.fill("input[placeholder='Enter password']", "adminpassword123")
        page.click("form:has(input[placeholder='Enter password']) button[type=submit]")
        page.wait_for_selector("text=must be changed before you can continue", timeout=8000)
        check("first login forces the password-change modal (no close button)", data(page, "d.changePasswordForced") is True)
        boxes = page.locator("form:has-text('Repeat new password') input[type=password]")
        boxes.nth(0).fill("adminpassword123"); boxes.nth(1).fill("A-new-strong-pw1"); boxes.nth(2).fill("A-new-strong-pw1")
        page.click("form:has-text('Repeat new password') button[type=submit]")
        page.wait_for_function("() => !Alpine.$data(document.body).changePasswordOpen", timeout=8000)

        act = lambda code: page.evaluate(f"async () => {{ const d = Alpine.$data(document.body); {code} }}")
        act("await d.openMfa();")
        check("MFA modal opens in the off state", data(page, "d.mfa.open && !d.mfa.status.enabled"))
        page.click("button:has-text('Set up')")
        page.wait_for_selector("text=Scan this QR code", timeout=8000)
        time.sleep(0.5)
        check("a QR code (SVG) is rendered", page.locator("[x-ref=mfaQr] svg").count() == 1)
        secret_b32 = data(page, "d.mfa.setup.secret")
        secret = base64.b32decode(secret_b32 + "=" * (-len(secret_b32) % 8))
        page.screenshot(path=f"{OUT_DIR}/mfa_setup.png")
        page.fill("input[placeholder='123456']:visible", "000000")
        page.click("button:has-text('Turn on')")
        page.wait_for_selector("text=not valid", timeout=5000)
        check("a wrong confirmation code shows an error and does not enable MFA", data(page, "!d.mfa.status.enabled"))
        page.fill("input[placeholder='123456']:visible", mfa.totp(secret))
        page.click("button:has-text('Turn on')")
        page.wait_for_selector("text=They will not be shown again", timeout=8000)
        codes = data(page, "d.mfa.backupCodes")
        check("10 backup codes are shown once", isinstance(codes, list) and len(codes) == 10, codes)
        page.screenshot(path=f"{OUT_DIR}/mfa_backup_codes.png")
        page.click("button:has-text('I have saved them')")
        check("after dismissing, the codes are gone and MFA shows as on", data(page, "d.mfa.backupCodes === null && d.mfa.status.enabled"))

        print("\n2. Two-step sign-in")
        ctx.request.post(f"{BASE_URL}/api/auth/logout")
        ctx.clear_cookies()
        page.goto(BASE_URL, wait_until="networkidle")
        page.fill("input[placeholder='Enter username']", "admin")
        page.fill("input[placeholder='Enter password']", "A-new-strong-pw1")
        page.click("form:has(input[placeholder='Enter password']) button[type=submit]")
        page.wait_for_selector("text=Enter the 6-digit code", timeout=8000)
        check("the password step shows the code step, not a session", data(page, "!d.isAuthenticated && !!d.loginForm.mfaToken"))
        check("the SSO/password fields are hidden during the code step", not page.locator("input[placeholder='Enter password']").is_visible())
        page.screenshot(path=f"{OUT_DIR}/mfa_login_step.png")
        page.fill("input[placeholder='123456']:visible", "111111")
        page.click("button:has-text('Verify')")
        page.wait_for_selector("text=That code is not valid >> visible=true", timeout=5000)
        check("a wrong code is refused", data(page, "!d.isAuthenticated"))
        page.fill("input[placeholder='123456']:visible", mfa.totp(secret, time.time() + 30))   # enrolment used the current step
        page.click("button:has-text('Verify')")
        page.wait_for_function("() => Alpine.$data(document.body).isAuthenticated", timeout=8000)
        check("the right code signs in", data(page, "d.currentUser.username") == "admin")

        print("\n3. Turn MFA off")
        act("await d.openMfa();")
        page.click("button:has-text('Turn off')")
        page.fill("input[placeholder='Password']", "A-new-strong-pw1")
        page.fill("input[placeholder='Authenticator or backup code']", codes[0])
        page.click("form:has-text('Confirm to turn') button[type=submit]")
        page.wait_for_function("() => !Alpine.$data(document.body).mfa.status.enabled", timeout=8000)
        check("a backup code + password turn MFA off", True)
        print("  JS errors:", errors)
        check("no JS errors during the whole flow", not errors, errors)
        browser.close()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All MFA UI checks passed.")


if __name__ == "__main__":
    main()
