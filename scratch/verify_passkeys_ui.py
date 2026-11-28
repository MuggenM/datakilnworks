#!/usr/bin/env python3
"""Passkeys in a REAL browser (Playwright + Chromium's virtual authenticator over CDP; host /usr/bin/python3): register a passkey in the security dialog, sign in
with it (passwordless), sign in with password + passkey as the second factor, the refusal without user verification, both methods offered together,
removal with the password. Builds a throwaway studio `pkui` on port 8117 (installs the webauthn package into that container) and removes it."""
import base64, os, re, subprocess, sys, time
from playwright.sync_api import sync_playwright
import _ui_slow
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); BASE = "http://localhost:8117"
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
FAIL = []
PENDING = {}   # url -> start time of every request the page has open (printed when a wait times out)
AUTH_LOG = []  # every /api/auth/ request the page sent, with its outcome (printed when a wait times out)
TIMELINE = []  # sign-in/out and workspace-tree requests and console errors of the main page, with times (printed when "no JS errors" fails)
T0 = time.monotonic()
def _tl(what): TIMELINE.append(f"{time.monotonic() - T0:7.2f}s {what}")
# sign-in/out, the workspace tree, and everything that can end or change a session on the server side (session revoke, account
# changes such as passkey-only conversion or a password reset, passkey add/remove), so a 401 can be tied to what caused it
_TL_PATHS = ("/api/auth/login", "/api/auth/logout", "/api/auth/passkey/login", "/api/auth/login/webauthn", "/api/workspace/tree",
             "/api/auth/sessions", "/api/auth/webauthn/", "/api/auth/enroll/", "/api/auth/me", "/api/users", "/api/notebooks/access")
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    _tl(f"check {'PASS' if c else 'FAIL'}: {n[:100]}")     # what the test was doing at that moment
    if not c: FAIL.append(n)


def print_error_windows(before=8.0, after=2.0, max_lines=160):
    """The timeline around each error (BEFORE seconds before it, AFTER after it), overlapping windows merged: the last 40
    entries used to be printed, and an error early in the run fell outside them (CI run 46086935122)."""
    times = [float(line.split("s", 1)[0]) for line in TIMELINE]
    marks = [t for t, line in zip(times, TIMELINE) if "console error:" in line or "page error:" in line]
    windows = []
    for t in marks:
        if windows and t - before <= windows[-1][1]:
            windows[-1][1] = t + after
        else:
            windows.append([t - before, t + after])
    printed = 0
    for i, (lo, hi) in enumerate(windows):
        print(f"  [FAIL] | --- timeline {lo:.2f}s .. {hi:.2f}s (error window {i + 1} of {len(windows)}) ---")
        for t, line in zip(times, TIMELINE):
            if lo <= t <= hi:
                if printed >= max_lines:
                    print("  [FAIL] | ... (more in passkeys_timeline.txt under ARTIFACT_DIR)"); return
                print("  [FAIL] | " + line); printed += 1
sh = lambda *a: subprocess.run(a, capture_output=True, text=True)
ERRORS = []   # page errors and console errors of the main page, printed by wait_or_explain

def wait_or_explain(page, selector, what, timeout=10000, function=None):
    """wait_for_selector (or wait_for_function when `function` is given), but a timeout prints what the page and the studio said
    (the open requests, the sign-in and passkey state, page errors, a screenshot, the studio log) first. The diagnostic lines carry
    "[FAIL] |" because ci/annotate.py puts [FAIL] lines into the CI annotation first; anything else is crowded out by the traceback."""
    try:
        if function: page.wait_for_function(function, timeout=timeout)
        else: page.wait_for_selector(selector, timeout=timeout)
    except Exception:
        err = page.locator("[data-testid=passkey-error]")
        now = time.monotonic(); busy = sorted(((now - t, u) for u, t in PENDING.items()), reverse=True)
        print(f"  [FAIL] | requests still open in the browser ({len(busy)}): " + "; ".join(u[:120] for _, u in busy[:12]))
        try: print("  [FAIL] | pk state: " + str(page.evaluate("() => { const p = Alpine.$data(document.body).pk; return { busy: p.busy, addOpen: p.addOpen, creds: (p.creds || []).length, error: p.error }; }")))
        except Exception as e: print(f"  [FAIL] | pk state: unreadable ({e})")
        try: print("  [FAIL] | sign-in state: " + str(page.evaluate("() => { const d = Alpine.$data(document.body); const f = d.loginForm || {}; return { authenticated: d.isAuthenticated, user: d.currentUser && d.currentUser.username, loginModal: d.showLoginModal, loading: f.loading, error: f.error, mfaToken: !!f.mfaToken, mfaMethods: f.mfaMethods, username: f.username }; }")))
        except Exception as e: print(f"  [FAIL] | sign-in state: unreadable ({e})")
        # why an Enter in the password field would not submit: an empty/invalid required field, a disabled submit button, focus elsewhere
        try: print("  [FAIL] | sign-in form: " + str(page.evaluate("""() => { const pw = document.querySelector("input[placeholder='Enter password']"); const f = pw && pw.form; const b = f && f.querySelector("button[type=submit]");
            return { pwModelLen: (Alpine.$data(document.body).loginForm.password || '').length, pwInputLen: pw ? pw.value.length : null, pwVisible: !!(pw && pw.offsetParent), formValid: f ? f.checkValidity() : null,
                     submitDisabled: b ? b.disabled : null, active: document.activeElement ? (document.activeElement.tagName + ' ' + (document.activeElement.getAttribute('placeholder') || document.activeElement.getAttribute('data-testid') || '')) : null }; }""")))
        except Exception as e: print(f"  [FAIL] | sign-in form: unreadable ({e})")
        print(f"  [FAIL] | /api/auth responses the page got: {len(AUTH_LOG)}, the last ones:"); [print("  [FAIL] |   " + a[:110]) for a in AUTH_LOG[-6:]]
        # what the page saw of the attempt: Enter keydowns (target, cancelled?), submit events, the page's own fetch() calls to /api/auth/login
        try: [print("  [FAIL] | page trace: " + t[:160]) for t in page.evaluate("() => (window.__dkwTrace || []).slice(-10)")]
        except Exception as e: print(f"  [FAIL] | page trace: unreadable ({e})")
        print(f"  [FAIL] | page/console errors so far ({len(ERRORS)}): " + " | ".join(e[:200] for e in ERRORS[-8:]))
        print(f"  [FAIL] {what}: {function or selector} did not {'become true' if function else 'appear'}; on-page error: {err.inner_text()[:300] if err.count() and err.first.is_visible() else '(none shown)'}")
        shot = os.path.join(os.getenv("ARTIFACT_DIR", "/tmp"), "passkeys_failure.png"); page.screenshot(path=shot); print(f"    screenshot: {shot}")
        r = sh("docker", "logs", "--tail", "40", "pkui"); print("    studio log (last 40 lines):"); [print("      " + ln[:300]) for ln in (r.stdout + r.stderr).splitlines()[-40:]]
        notable = [ln for ln in (r.stdout + r.stderr).splitlines() if re.search(r"ERROR|Traceback|Exception| [45]\d\d ", ln)]
        [print("  [FAIL] | studio: " + ln[:300]) for ln in notable[-6:]]
        r = sh("docker", "logs", "pkui"); [print("  [FAIL] | studio auth: " + ln[:150]) for ln in [ln for ln in (r.stdout + r.stderr).splitlines() if "/api/auth/" in ln][-4:]]
        raise
sh("docker", "rm", "-f", "pkui")
sh("docker", "run", "-d", *(["--network", os.environ["DOCKER_NETWORK"]] if os.getenv("DOCKER_NETWORK") else []), "--name", "pkui", "-p", "8117:8891", "-v", f"{ROOT}/web:/workspace/web", "-v", f"{ROOT}/docs:/workspace/docs", "-w", "/workspace", "-e", "WAREHOUSE_DIR=/workspace/warehouse",
   "-e", "INIT_ADMIN_USERNAME=admin", "-e", f"INIT_ADMIN_PASSWORD_HASH={HASH}", os.getenv("CI_IMAGE", "localspark-lakehouse-notebook"), "sh", "-c",
   "pip install -q webauthn >/dev/null 2>&1; python -m uvicorn web.app:app --host 0.0.0.0 --port 8891 --no-proxy-headers")
try:
    for _ in range(150):
        if sh("curl", "-s", "-o", "/dev/null", f"{BASE}/api/docs").returncode == 0 and sh("docker", "exec", "pkui", "python", "-c", "import sqlite3;sqlite3.connect('/workspace/warehouse/.metadata/auth.db').execute('select 1 from users')").returncode == 0: break
        time.sleep(1)
    time.sleep(3)
    r = sh("docker", "exec", "-w", "/workspace", "pkui", "python", "-c", "import sys;sys.path.insert(0,'/workspace');from web import auth\nauth.create_user('alice','alicepassword1','Alice','user')\nwith auth.get_db_connection() as c: c.execute('UPDATE users SET must_change_password=0')")
    check("seeded a user", r.returncode == 0, r.stderr[-300:])
    with sync_playwright() as p:
        b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1400, "height": 1000}); page = _ui_slow.apply(ctx.new_page()); errors = ERRORS
        page.on("pageerror", lambda e: (errors.append(str(e) + " @ " + str(getattr(e, "stack", ""))[:500]), _tl("page error: " + str(e)[:120]))); page.on("dialog", lambda d: d.accept())
        page.on("request", lambda r: PENDING.__setitem__(r.method + " " + r.url.replace(BASE, ""), time.monotonic()))
        for ev_name in ("requestfinished", "requestfailed"): page.on(ev_name, lambda r: PENDING.pop(r.method + " " + r.url.replace(BASE, ""), None))
        page.on("response", lambda r: AUTH_LOG.append(f"{r.request.method} {r.url.replace(BASE, '')} {r.status}") if "/api/auth/" in r.url else None)
        page.on("requestfailed", lambda r: AUTH_LOG.append(f"{r.method} {r.url.replace(BASE, '')} failed") if "/api/auth/" in r.url else None)
        page.on("console", lambda m: errors.append(m.text[:200]) if m.type == "error" and "Failed to load resource" not in m.text else None)
        page.on("request", lambda r: _tl(f"sent {r.method} {r.url.replace(BASE, '')}") if any(x in r.url for x in _TL_PATHS) else None)
        page.on("response", lambda r: _tl(f"answer {r.status} {r.request.method} {r.url.replace(BASE, '')}") if any(x in r.url for x in _TL_PATHS) else None)
        page.on("console", lambda m: _tl("console error: " + m.text[:120]) if m.type == "error" and "Failed to load resource" not in m.text else None)
        # Chromium cannot operate the autofill dropdown in automation: a wrapper records every conditional (autofill) request and lets the test "pick the passkey"
        # trace for a sign-in that does not happen: Enter keys, form submits and the page's fetch() of /api/auth/login (printed by wait_or_explain)
        ctx.add_init_script("""(() => { const T = window.__dkwTrace = []; const t0 = performance.now(); const at = () => Math.round(performance.now() - t0) + 'ms';
          const who = (el) => el ? (el.tagName + ' ' + (el.getAttribute('placeholder') || el.getAttribute('data-testid') || '')) : 'none';
          window.addEventListener('keydown', (e) => { if (e.key === 'Enter') T.push(at() + ' keydown Enter on ' + who(e.target)); }, true);
          window.addEventListener('keydown', (e) => { if (e.key === 'Enter') T.push(at() + ' keydown Enter after handlers: defaultPrevented=' + e.defaultPrevented); });
          document.addEventListener('submit', (e) => T.push(at() + ' submit event (form shown: ' + !!e.target.offsetParent + ')'), true);
          const f = window.fetch; window.fetch = function (u, o) { const s = String(u && u.url || u); if (s.indexOf('/api/auth/login') >= 0 || s.indexOf('/api/auth/logout') >= 0) T.push(at() + ' fetch ' + s);
            return f.apply(this, arguments); }; })();""")
        ctx.add_init_script("""(() => { const orig = navigator.credentials.get.bind(navigator.credentials); window.__condCalls = [];
          navigator.credentials.get = function (opts) { if (opts && opts.mediation === 'conditional') { const rec = { aborted: false, signal: !!opts.signal };
            window.__condCalls.push(rec);
            return new Promise((resolve, reject) => { rec.resolve = () => orig({ publicKey: opts.publicKey }).then(resolve, reject);
              if (opts.signal) opts.signal.addEventListener('abort', () => { rec.aborted = true; reject(new DOMException('aborted', 'AbortError')); }); }); }
            return orig(opts); };
          try { PublicKeyCredential.isConditionalMediationAvailable = async () => true; } catch (e) {} })();""")
        ctx.add_init_script("""(() => { const f = window.fetch; window.fetch = function (u, o) { const s = String(u && u.url || u); const h = window.__dkwHoldTree;
            if (!h || h.taken || s.indexOf('/api/workspace/tree') < 0) return f.apply(this, arguments);
            h.taken = true;
            return new Promise((r) => { h.send = r; }).then(() => f.call(window, u, o)).then((res) => { h.status = res.status; return new Promise((r) => { h.deliver = () => r(res); }); }); }; })();""")
        cdp = ctx.new_cdp_session(page); cdp.send("WebAuthn.enable")
        auth = cdp.send("WebAuthn.addVirtualAuthenticator", {"options": {"protocol": "ctap2", "transport": "internal", "hasResidentKey": True, "hasUserVerification": True, "isUserVerified": True, "automaticPresenceSimulation": True}})["authenticatorId"]
        page.goto(BASE, wait_until="networkidle"); time.sleep(1)
        ev = lambda code: page.evaluate(code)
        def ui_login(user, pw):
            page.fill("input[placeholder='Enter username']", user); page.fill("input[placeholder='Enter password']", pw); page.press("input[placeholder='Enter password']", "Enter")
        who = lambda: ev("() => { const d = Alpine.$data(document.body); return d.isAuthenticated ? d.currentUser.username : null; }")
        def logout():
            # logout() is async (POST /api/auth/logout, then the form reset and onUserChanged): wait for it to finish, not a fixed time
            ev("() => { window.__dkwLogoutDone = false; Alpine.$data(document.body).logout().finally(() => { window.__dkwLogoutDone = true; }); }")
            wait_or_explain(page, None, "signing out", timeout=20000, function="() => window.__dkwLogoutDone === true && !Alpine.$data(document.body).isAuthenticated"); page.wait_for_timeout(300)

        print("registration")
        check("the sign-in page offers a passkey button", page.locator("[data-testid=passkey-login]").is_visible())
        ui_login("alice", "alicepassword1"); page.wait_for_function("() => Alpine.$data(document.body).isAuthenticated"); page.wait_for_timeout(600)
        ev("() => { Alpine.$data(document.body).openMfa(); }"); page.wait_for_selector("[data-testid=passkeys-section]", state="visible"); page.wait_for_timeout(500)
        check("the security dialog has a passkeys section with an Add button", page.locator("[data-testid=passkey-add]").is_visible())
        page.click("[data-testid=passkey-add]"); page.fill("[data-testid=passkey-name-input]", "Virtual laptop key")
        page.fill("[data-testid=passkey-password]", "wrong"); page.click("[data-testid=passkey-create]"); page.wait_for_selector("[data-testid=passkey-error]", state="visible", timeout=20000)
        page.wait_for_function("() => !Alpine.$data(document.body).pk.busy", timeout=20000)
        check("a wrong password is refused before the browser is asked", "incorrect" in page.locator("[data-testid=passkey-error]").inner_text() and page.locator("[data-testid=passkey-row]").count() == 0)
        page.fill("[data-testid=passkey-password]", "alicepassword1"); page.click("[data-testid=passkey-create]")
        wait_or_explain(page, "[data-testid=passkey-row]", "adding a passkey in the dialog")
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
        ev("() => { Alpine.$data(document.body).openMfa(); }"); page.wait_for_selector("[data-testid=passkeys-section]", state="visible"); page.wait_for_timeout(400)
        page.click("[data-testid=passkey-add]"); page.fill("[data-testid=passkey-name-input]", "Second key"); page.fill("[data-testid=passkey-password]", "alicepassword1"); page.click("[data-testid=passkey-create]")
        wait_or_explain(page, "[data-testid=passkey-row]", "adding a passkey in the dialog")
        page.click("[data-testid=passkey-remove]"); page.fill("[data-testid=passkey-remove-password]", "wrong"); page.click("[data-testid=passkey-remove-confirm]"); page.wait_for_selector("[data-testid=passkey-error]", state="visible", timeout=15000); page.wait_for_function("() => !Alpine.$data(document.body).pk.busy", timeout=15000)
        check("removing needs the password: a wrong one leaves the passkey in place", page.locator("[data-testid=passkey-row]").count() == 1 and "incorrect" in page.locator("[data-testid=passkey-error]").inner_text())
        page.fill("[data-testid=passkey-remove-password]", "alicepassword1"); page.click("[data-testid=passkey-remove-confirm]"); page.wait_for_timeout(1000)
        check("with the right password it is removed", page.locator("[data-testid=passkey-row]").count() == 0)
        print("passkey policy (administrator)")
        ev("() => Alpine.$data(document.body).mfa.open = false"); logout()
        check("right after signing out the Sign In button is usable (it stayed disabled until the previous user's data had loaded, so Enter did nothing)",
              ev("() => !document.querySelector(\"input[placeholder='Enter password']\").form.querySelector('[type=submit]').disabled && !Alpine.$data(document.body).loginForm.loading"))
        ui_login("admin", "adminpassword123"); wait_or_explain(page, None, "the administrator signing in after alice signed out", function="() => Alpine.$data(document.body).isAuthenticated"); page.wait_for_timeout(800)
        # wait for the policy load itself: it overwrites the form when it answers, and after a sign-in the page is busy loading its data
        with page.expect_response(lambda r: r.url.endswith("/api/webauthn/policy") and r.request.method == "GET", timeout=20000):
            ev("() => { const d = Alpine.$data(document.body); d.showIamModal = true; d.iamTab = 'mfa'; d.loadMfaPolicy(); }")
        page.wait_for_selector("[data-testid=passkey-policy-card]", state="visible"); page.wait_for_timeout(800)
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
        ctx2 = b.new_context(viewport={"width": 1300, "height": 900}); pg2 = _ui_slow.apply(ctx2.new_page()); errors2 = []
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
        pg3 = _ui_slow.apply(ctx2.new_page()); pg3.goto(link, wait_until="networkidle"); pg3.wait_for_selector("[data-testid=enroll-modal]", state="visible"); pg3.wait_for_timeout(800)
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
        print("a load of the previous session answering after the next sign-in")
        # the CI failure "Failed to load workspace tree" (run #75): a tree load started at alice's sign-in is answered only after the
        # next user signed in. Its answer belongs to alice's session and must be dropped, whatever it is.
        ws = lambda: ev("() => { const d = Alpine.$data(document.body); return { home: d.userHomePath, alice: !!d.workspaceExpandedDirs['Users/alice'], tree: (d.workspaceTree || []).length }; }")
        def admin_after_held_alice_load(send_before_logout):
            ev("() => { Alpine.$data(document.body).showIamModal = false; }"); logout()
            ev("() => { window.__dkwHoldTree = {}; }")
            ui_login("alice", "alicepassword1"); page.wait_for_function("() => Alpine.$data(document.body).isAuthenticated && window.__dkwHoldTree.send", timeout=20000)
            if send_before_logout: ev("() => window.__dkwHoldTree.send()"); page.wait_for_function("() => window.__dkwHoldTree.deliver", timeout=20000)
            logout()
            if not send_before_logout: ev("() => window.__dkwHoldTree.send()"); page.wait_for_function("() => window.__dkwHoldTree.deliver", timeout=20000)
            ui_login("admin", "adminpassword123")
            page.wait_for_function("() => { const d = Alpine.$data(document.body); return d.isAuthenticated && d.currentUser.username === 'admin' && d.userHomePath === 'Users/admin' && (d.workspaceTree || []).length > 0; }", timeout=30000)
            before = len(errors); ev("() => window.__dkwHoldTree.deliver()"); page.wait_for_timeout(1500)
            return ev("() => window.__dkwHoldTree.status"), [e for e in errors[before:] if "dialog" not in e], ws()
        status, new_errs, state = admin_after_held_alice_load(send_before_logout=False)
        check("alice's tree request sent after her sign-out gets the 401, and answered after admin signed in it is dropped without an error",
              status == 401 and not new_errs and state["home"] == "Users/admin", (status, new_errs, state))
        status, new_errs, state = admin_after_held_alice_load(send_before_logout=True)
        check("alice's tree answered (200) only after admin signed in does not replace admin's workspace with alice's",
              status == 200 and not new_errs and state["home"] == "Users/admin" and not state["alice"], (status, new_errs, state))
        errs = [e for e in errors if "dialog" not in e]
        check("no JS errors", not errs, errs)
        if errs: print_error_windows()  # which sign-in/out, and which test step, a failing request belonged to
        if os.environ.get("ARTIFACT_DIR"):
            with open(os.path.join(os.environ["ARTIFACT_DIR"], "passkeys_timeline.txt"), "w") as f: f.write("\n".join(TIMELINE) + "\n")
        b.close()
finally:
    if FAIL: print(sh("docker", "logs", "--tail", "25", "pkui").stderr[-1500:])
    sh("docker", "rm", "-f", "pkui")
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
