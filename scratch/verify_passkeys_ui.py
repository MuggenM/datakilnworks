#!/usr/bin/env python3
"""Passkeys in a REAL browser (Playwright + Chromium's virtual authenticator over CDP; host /usr/bin/python3): register a passkey in the security dialog, sign in
with it (passwordless), sign in with password + passkey as the second factor, the refusal without user verification, both methods offered together,
removal with the password. Builds a throwaway studio `pkui` on port 8117 (installs the webauthn package into that container) and removes it."""
import base64, os, subprocess, sys, time
from playwright.sync_api import sync_playwright
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); BASE = "http://localhost:8117"
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
sh = lambda *a: subprocess.run(a, capture_output=True, text=True)
sh("docker", "rm", "-f", "pkui")
sh("docker", "run", "-d", "--name", "pkui", "-p", "8117:8891", "-v", f"{ROOT}/web:/workspace/web", "-v", f"{ROOT}/docs:/workspace/docs", "-w", "/workspace", "-e", "WAREHOUSE_DIR=/workspace/warehouse",
   "-e", "INIT_ADMIN_USERNAME=admin", "-e", f"INIT_ADMIN_PASSWORD_HASH={HASH}", "localspark-lakehouse-notebook", "sh", "-c",
   "pip install -q webauthn >/dev/null 2>&1; python -m uvicorn web.app:app --host 0.0.0.0 --port 8891 --no-proxy-headers")
try:
    for _ in range(150):
        if sh("curl", "-s", "-o", "/dev/null", f"{BASE}/api/docs").returncode == 0 and sh("docker", "exec", "pkui", "python", "-c", "import sqlite3;sqlite3.connect('/workspace/warehouse/.metadata/auth.db').execute('select 1 from users')").returncode == 0: break
        time.sleep(1)
    time.sleep(3)
    r = sh("docker", "exec", "-w", "/workspace", "pkui", "python", "-c", "import sys;sys.path.insert(0,'/workspace');from web import auth\nauth.create_user('alice','alicepassword1','Alice','user')\nwith auth.get_db_connection() as c: c.execute('UPDATE users SET must_change_password=0')")
    check("seeded a user", r.returncode == 0, r.stderr[-300:])
    with sync_playwright() as p:
        b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1400, "height": 1000}); page = ctx.new_page(); errors = []
        page.on("pageerror", lambda e: errors.append(str(e) + " @ " + str(getattr(e, "stack", ""))[:500])); page.on("dialog", lambda d: d.accept())
        page.on("console", lambda m: errors.append(m.text[:200]) if m.type == "error" and "Failed to load resource" not in m.text else None)
        # Chromium cannot operate the autofill dropdown in automation: a wrapper records every conditional (autofill) request and lets the test "pick the passkey"
        ctx.add_init_script("""(() => { const orig = navigator.credentials.get.bind(navigator.credentials); window.__condCalls = [];
          navigator.credentials.get = function (opts) { if (opts && opts.mediation === 'conditional') { const rec = { aborted: false, signal: !!opts.signal };
            window.__condCalls.push(rec);
            return new Promise((resolve, reject) => { rec.resolve = () => orig({ publicKey: opts.publicKey }).then(resolve, reject);
              if (opts.signal) opts.signal.addEventListener('abort', () => { rec.aborted = true; reject(new DOMException('aborted', 'AbortError')); }); }); }
            return orig(opts); };
          try { PublicKeyCredential.isConditionalMediationAvailable = async () => true; } catch (e) {} })();""")
        cdp = ctx.new_cdp_session(page); cdp.send("WebAuthn.enable")
        auth = cdp.send("WebAuthn.addVirtualAuthenticator", {"options": {"protocol": "ctap2", "transport": "internal", "hasResidentKey": True, "hasUserVerification": True, "isUserVerified": True, "automaticPresenceSimulation": True}})["authenticatorId"]
        page.goto(BASE, wait_until="networkidle"); time.sleep(1)
        ev = lambda code: page.evaluate(code)
        def ui_login(user, pw):
            page.fill("input[placeholder='Enter username']", user); page.fill("input[placeholder='Enter password']", pw); page.press("input[placeholder='Enter password']", "Enter")
        who = lambda: ev("() => { const d = Alpine.$data(document.body); return d.isAuthenticated ? d.currentUser.username : null; }")
        def logout():
            ev("() => Alpine.$data(document.body).logout()"); page.wait_for_timeout(800)

        print("registration")
        check("the sign-in page offers a passkey button", page.locator("[data-testid=passkey-login]").is_visible())
        ui_login("alice", "alicepassword1"); page.wait_for_function("() => Alpine.$data(document.body).isAuthenticated"); page.wait_for_timeout(600)
        ev("() => Alpine.$data(document.body).openMfa()"); page.wait_for_selector("[data-testid=passkeys-section]", state="visible"); page.wait_for_timeout(500)
        check("the security dialog has a passkeys section with an Add button", page.locator("[data-testid=passkey-add]").is_visible())
        page.click("[data-testid=passkey-add]"); page.fill("[data-testid=passkey-name-input]", "Virtual laptop key")
        page.fill("[data-testid=passkey-password]", "wrong"); page.click("[data-testid=passkey-create]"); page.wait_for_timeout(1000)
        check("a wrong password is refused before the browser is asked", "incorrect" in page.locator("[data-testid=passkey-error]").inner_text() and page.locator("[data-testid=passkey-row]").count() == 0)
        page.fill("[data-testid=passkey-password]", "alicepassword1"); page.click("[data-testid=passkey-create]"); page.wait_for_selector("[data-testid=passkey-row]", timeout=10000)
        check("the passkey is created through the browser and listed", page.locator("[data-testid=passkey-name]").inner_text() == "Virtual laptop key")
        creds = cdp.send("WebAuthn.getCredentials", {"authenticatorId": auth})["credentials"]
        check("the (virtual) authenticator holds a resident credential for the studio", len(creds) == 1 and creds[0]["isResidentCredential"] and creds[0]["rpId"] == "localhost", creds)
        page.screenshot(path="/tmp/passkeys_dialog.png")
        page.keyboard.press("Escape"); ev("() => { Alpine.$data(document.body).mfa.open = false; }"); logout()

        print("passwordless sign-in")
        page.locator("[data-testid=passkey-login]").click(); page.wait_for_function("() => Alpine.$data(document.body).isAuthenticated", timeout=10000)
        check("'Sign in with a passkey' signs in as alice without a username or password", who() == "alice")
        logout()
        cdp.send("WebAuthn.setUserVerified", {"authenticatorId": auth, "isUserVerified": False})
        page.locator("[data-testid=passkey-login]").click(); page.wait_for_timeout(2500)
        check("an authenticator that does not verify the user (no PIN / biometric) is refused: one factor is not enough", who() is None and page.locator("text=could not be verified").first.is_visible() or who() is None)
        cdp.send("WebAuthn.setUserVerified", {"authenticatorId": auth, "isUserVerified": True})

        print("autofill (conditional) sign-in")
        page.reload(wait_until="networkidle"); time.sleep(1.5)
        calls = lambda: ev("() => window.__condCalls.length")
        check("the open sign-in dialog starts a conditional passkey request, and the username field asks the browser to offer passkeys", calls() == 1 and "webauthn" in page.get_attribute("[data-testid=login-username]", "autocomplete"), (calls(), page.get_attribute("[data-testid=login-username]", "autocomplete")))
        ev("() => window.__condCalls[window.__condCalls.length - 1].resolve()"); page.wait_for_function("() => Alpine.$data(document.body).isAuthenticated", timeout=10000)
        check("picking the passkey from the autofill list signs in without typing anything", who() == "alice")
        logout(); page.wait_for_timeout(1500)
        n = calls(); check("after signing out a fresh conditional request is started", n >= 2 and ev("() => !window.__condCalls[window.__condCalls.length - 1].aborted"), n)
        page.fill("input[placeholder='Enter username']", "alice"); page.fill("input[placeholder='Enter password']", "wrong-password"); page.press("input[placeholder='Enter password']", "Enter"); page.wait_for_timeout(1500)
        check("using the password form cancels the autofill request, and a wrong password renews it", ev("() => window.__condCalls[window.__condCalls.length - 2].aborted") is True and calls() >= n + 1 and ev("() => !window.__condCalls[window.__condCalls.length - 1].aborted"))
        page.locator("[data-testid=passkey-login]").click(); page.wait_for_function("() => Alpine.$data(document.body).isAuthenticated", timeout=10000)
        check("the explicit passkey button still works (it cancels the pending autofill request first)", who() == "alice" and ev("() => window.__condCalls.filter(c => !c.aborted).length") <= 1)
        logout()

        print("second factor")
        page.reload(wait_until="networkidle"); time.sleep(1)
        ui_login("alice", "alicepassword1"); page.wait_for_function("() => Alpine.$data(document.body).isAuthenticated", timeout=15000)
        check("password, then the passkey is used automatically as the second factor (nothing else to type)", who() == "alice")
        logout()
        print("both methods, and removal")
        r = sh("docker", "exec", "-w", "/workspace", "pkui", "python", "-c", "import sys;sys.path.insert(0,'/workspace');from web import auth, mfa\nu=auth.get_user_by_username('alice');s=mfa.begin_setup(u['id'],'alice');import base64;mfa.confirm_setup(u['id'], mfa.totp(base64.b32decode(s['secret']+'='*(-len(s['secret'])%8))))")
        check("(an authenticator app is added on the server)", r.returncode == 0, r.stderr[-300:])
        cdp.send("WebAuthn.removeVirtualAuthenticator", {"authenticatorId": auth})
        page.reload(wait_until="networkidle"); time.sleep(1); ui_login("alice", "alicepassword1"); page.wait_for_selector("[data-testid=mfa-use-passkey]", state="visible", timeout=8000)
        check("with both configured the second step offers the passkey AND the code field", page.locator("[data-testid=mfa-use-passkey]").is_visible() and page.locator("input[x-model='loginForm.mfaCode']").is_visible())
        page.screenshot(path="/tmp/passkeys_second_step.png")
        page.locator("[data-testid=mfa-use-passkey]").click(); page.wait_for_timeout(3500)
        check("with no authenticator available the browser prompt fails with a readable message and nobody is signed in", who() is None and page.locator("text=Passkey error").count() + page.locator("text=cancelled").count() > 0)
        # clean up on the server (the credential lived in the removed authenticator), then use the dialog again
        sh("docker", "exec", "-w", "/workspace", "pkui", "python", "-c", "import sys;sys.path.insert(0,'/workspace');from web import auth, mfa, webauthn_auth as w\nu=auth.get_user_by_username('alice');mfa.disable(u['id']);w.delete_all(u['id'])")
        auth = cdp.send("WebAuthn.addVirtualAuthenticator", {"options": {"protocol": "ctap2", "transport": "internal", "hasResidentKey": True, "hasUserVerification": True, "isUserVerified": True, "automaticPresenceSimulation": True}})["authenticatorId"]
        page.reload(wait_until="networkidle"); time.sleep(1); ui_login("alice", "alicepassword1"); page.wait_for_function("() => Alpine.$data(document.body).isAuthenticated", timeout=10000)
        ev("() => Alpine.$data(document.body).openMfa()"); page.wait_for_selector("[data-testid=passkeys-section]", state="visible"); page.wait_for_timeout(400)
        page.click("[data-testid=passkey-add]"); page.fill("[data-testid=passkey-name-input]", "Second key"); page.fill("[data-testid=passkey-password]", "alicepassword1"); page.click("[data-testid=passkey-create]"); page.wait_for_selector("[data-testid=passkey-row]", timeout=10000)
        page.click("[data-testid=passkey-remove]"); page.fill("[data-testid=passkey-remove-password]", "wrong"); page.click("[data-testid=passkey-remove-confirm]"); page.wait_for_timeout(800)
        check("removing needs the password: a wrong one leaves the passkey in place", page.locator("[data-testid=passkey-row]").count() == 1 and "incorrect" in page.locator("[data-testid=passkey-error]").inner_text())
        page.fill("[data-testid=passkey-remove-password]", "alicepassword1"); page.click("[data-testid=passkey-remove-confirm]"); page.wait_for_timeout(1000)
        check("with the right password it is removed", page.locator("[data-testid=passkey-row]").count() == 0)
        print("passkey policy (administrator)")
        ev("() => Alpine.$data(document.body).mfa.open = false"); logout()
        ui_login("admin", "adminpassword123"); page.wait_for_function("() => Alpine.$data(document.body).isAuthenticated", timeout=10000); page.wait_for_timeout(800)
        ev("() => { const d = Alpine.$data(document.body); d.showIamModal = true; d.iamTab = 'mfa'; d.loadMfaPolicy(); }"); page.wait_for_selector("[data-testid=passkey-policy-card]", state="visible"); page.wait_for_timeout(800)
        check("the two-factor policy tab has a passkey policy card, default 'accept any authenticator'", page.locator("[data-testid=wpol-mode-none]").is_checked() and not page.locator("[data-testid=wpol-roots]").is_visible())
        page.locator("[data-testid=wpol-mode-require]").check(); page.wait_for_timeout(200); page.click("[data-testid=wpol-save]"); page.wait_for_timeout(1000)
        check("'require' without trust roots is refused with the reason", "trust roots" in page.locator("[data-testid=wpol-error]").inner_text(), page.locator("[data-testid=wpol-error]").inner_text() if page.locator("[data-testid=wpol-error]").is_visible() else "no error")
        page.locator("[data-testid=wpol-mode-record]").check(); page.wait_for_timeout(200)
        page.fill("[data-testid=wpol-allowed]", "not-a-uuid  Some key"); page.click("[data-testid=wpol-save]"); page.wait_for_timeout(1000)
        check("a malformed AAGUID is refused", "AAGUID" in page.locator("[data-testid=wpol-error]").inner_text())
        page.fill("[data-testid=wpol-allowed]", "2fc0579f-8113-47ea-b116-bb5a8db9202a  YubiKey 5 NFC"); page.click("[data-testid=wpol-save]"); page.wait_for_timeout(1200)
        st = ctx.request.get(f"{BASE}/api/webauthn/policy").json()
        check("'record' with an approved model is saved", st["mode"] == "record" and st["allowed"] == [{"aaguid": "2fc0579f-8113-47ea-b116-bb5a8db9202a", "label": "YubiKey 5 NFC"}] and not page.locator("[data-testid=wpol-error]").is_visible(), st)
        page.screenshot(path="/tmp/passkey_policy.png")
        page.locator("[data-testid=wpol-mode-none]").check(); page.click("[data-testid=wpol-save]"); page.wait_for_timeout(800)
        print("passkey-only user")
        page.evaluate("() => { const d = Alpine.$data(document.body); d.showIamModal = true; d.iamTab = 'users'; d.fetchUsers(); }"); page.wait_for_timeout(1200)
        page.click("[data-testid=new-passkey-user]"); page.wait_for_selector("[data-testid=passkey-user-card]", state="visible")
        page.fill("[data-testid=pkuser-username]", "erin"); page.click("[data-testid=pkuser-create]"); page.wait_for_selector("[data-testid=passkey-user-link]", state="visible", timeout=8000)
        link = page.input_value("[data-testid=passkey-user-link]")
        check("an administrator creates a passkey-only user and gets a one-time enrolment link on screen", "/?enroll=" in link and link.startswith(BASE), link)
        page.screenshot(path="/tmp/passkey_only_admin.png")
        page.click("[data-testid=passkey-user-card] >> text=Done"); page.wait_for_timeout(800)
        page.evaluate("() => { Alpine.$data(document.body).fetchUsers(); }"); page.wait_for_timeout(1200)
        check("the user list marks the account as passkey-only", page.locator("[data-testid=passkey-only-badge]:visible").count() == 1 and page.locator("[data-testid=user-enroll-link]:visible").count() == 1)
        # the invited person opens the link in their own browser profile (own virtual authenticator)
        ctx2 = b.new_context(viewport={"width": 1300, "height": 900}); pg2 = ctx2.new_page(); errors2 = []
        pg2.on("pageerror", lambda e: errors2.append(str(e))); pg2.on("dialog", lambda d: d.accept())
        cdp2 = ctx2.new_cdp_session(pg2); cdp2.send("WebAuthn.enable")
        cdp2.send("WebAuthn.addVirtualAuthenticator", {"options": {"protocol": "ctap2", "transport": "internal", "hasResidentKey": True, "hasUserVerification": True, "isUserVerified": True, "automaticPresenceSimulation": True}})
        pg2.goto(link, wait_until="networkidle"); pg2.wait_for_selector("[data-testid=enroll-modal]", state="visible"); pg2.wait_for_timeout(800)
        who2 = lambda: pg2.evaluate("() => { const d = Alpine.$data(document.body); return d.isAuthenticated ? d.currentUser.username : null; }")
        check("the link opens the enrolment page for that account and the token is removed from the address bar", "Welcome" in pg2.locator("[data-testid=enroll-welcome]").inner_text() and "enroll" not in pg2.url, pg2.url)
        pg2.screenshot(path="/tmp/passkey_enroll.png")
        pg2.fill("[data-testid=enroll-name]", "Erin's laptop"); pg2.click("[data-testid=enroll-create]"); pg2.wait_for_function("() => Alpine.$data(document.body).isAuthenticated", timeout=15000)
        check("creating the passkey registers it and signs the new user in", who2() == "erin")
        check("the account shows no password features (no change-password entry, no authenticator app)", pg2.evaluate("() => Alpine.$data(document.body).currentUser.passwordless") is True)
        pg3 = ctx2.new_page(); pg3.goto(link, wait_until="networkidle"); pg3.wait_for_selector("[data-testid=enroll-modal]", state="visible"); pg3.wait_for_timeout(800)
        check("the same link is dead the second time", pg3.locator("[data-testid=enroll-error]").is_visible() and "not valid" in pg3.locator("[data-testid=enroll-error]").inner_text(), pg3.locator("[data-testid=enroll-error]").inner_text() if pg3.locator("[data-testid=enroll-error]").is_visible() else "")
        pg3.close()
        pg2.evaluate("() => Alpine.$data(document.body).openMfa()"); pg2.wait_for_selector("[data-testid=passkeys-section]", state="visible"); pg2.wait_for_timeout(500)
        check("in the passkey dialog there is no password field and no authenticator-app set-up", not pg2.locator("[data-testid=passkey-password]").is_visible() and not pg2.locator("button:has-text('Set up')").is_visible())
        pg2.click("[data-testid=passkey-add]"); pg2.fill("[data-testid=passkey-name-input]", "Erin phone"); pg2.click("[data-testid=passkey-create]"); pg2.wait_for_timeout(4000)
        check("adding another passkey asks for a passkey confirmation (no password) and reaches the browser's create step, where the same authenticator is rightly refused as already registered",
              "already registered" in pg2.locator("[data-testid=passkey-error]").inner_text() and pg2.locator("[data-testid=passkey-row]").count() == 1, pg2.locator("[data-testid=passkey-error]").inner_text() if pg2.locator("[data-testid=passkey-error]").is_visible() else "")
        pg2.click("[data-testid=passkey-remove]"); pg2.click("[data-testid=passkey-remove-confirm]"); pg2.wait_for_timeout(3000)
        check("removing the only passkey is confirmed with the passkey and then refused: the account would be locked out", pg2.locator("[data-testid=passkey-row]").count() == 1 and "lock" in pg2.locator("[data-testid=passkey-error]").inner_text(), pg2.locator("[data-testid=passkey-error]").inner_text() if pg2.locator("[data-testid=passkey-error]").is_visible() else "")
        pg2.screenshot(path="/tmp/passkey_only_dialog.png")
        check("no JS errors on the invited user's side", not [e for e in errors2 if "dialog" not in e], errors2)
        ctx2.close()
        errs = [e for e in errors if "dialog" not in e]
        check("no JS errors", not errs, errs)
        b.close()
finally:
    if FAIL: print(sh("docker", "logs", "--tail", "25", "pkui").stderr[-1500:])
    sh("docker", "rm", "-f", "pkui")
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
