#!/usr/bin/env python3
"""IP allowlist UI (Playwright, /usr/bin/python3) against a THROWAWAY studio `ipui` on a throwaway network `ipnet` (published on :8117), plus a
second container on that network as "somebody else" (a different source address):
  docker network create ipnet
  docker run -d --name ipui --network ipnet -p 8117:8891 -v $PWD/web:/workspace/web -v $PWD/docs:/workspace/docs -w /workspace -e WAREHOUSE_DIR=/workspace/warehouse \
     -e INIT_ADMIN_USERNAME=admin -e INIT_ADMIN_PASSWORD_HASH='<hash of adminpassword123>' localspark-lakehouse-notebook python -m uvicorn web.app:app --host 0.0.0.0 --port 8891 --no-proxy-headers
  GIT_UI_URL=http://localhost:8117 python scratch/verify_ip_allowlist_ui.py"""
import json, os, subprocess, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def other(path="/api/auth/me", headers=None):
    """A request from another container on the network (a different source address). Returns (status, body)."""
    code = ("import urllib.request,urllib.error,sys,json\nreq=urllib.request.Request('http://ipui:8891%s',headers=%r)\n"
            "try:\n r=urllib.request.urlopen(req,timeout=10);print(json.dumps([r.status,r.read().decode()[:300]]))\nexcept urllib.error.HTTPError as e:\n print(json.dumps([e.code,e.read().decode()[:300]]))\n" % (path, headers or {}))
    out = subprocess.run(["docker", "run", "--rm", "--network", "ipnet", "localspark-lakehouse-notebook", "python", "-c", code], capture_output=True).stdout.decode().strip().splitlines()
    return json.loads(out[-1])
def ev(page, js):
    for i in range(3):
        try: return page.evaluate(js)
        except Exception as e:
            if "context was destroyed" not in str(e): raise
            time.sleep(2)
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1600, "height": 1100}); page = ctx.new_page(); errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("admin logs in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    subprocess.run(["docker", "exec", "ipui", "python", "-c", "import sqlite3;c=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit()"], check=True, capture_output=True)
    page.goto(BASE, wait_until="networkidle"); time.sleep(5)
    ev(page, "() => { const d = Alpine.$data(document.body); d.showIamModal = true; d.iamTab = 'network'; d.loadIpAllow(); }"); time.sleep(2)
    mine = page.locator("[data-testid=ip-me-address]").inner_text()
    check("the tab shows the address the server sees for the administrator", mine.count(".") == 3, mine)
    page.locator("[data-testid=ip-mode]").select_option("enforce"); page.locator("[data-testid=ip-save]").click(); time.sleep(1.5)
    check("enforcing an empty list is refused with the reason", "block everybody" in page.locator("[data-testid=ip-error]").inner_text(), page.locator("[data-testid=ip-error]").inner_text())
    page.locator("[data-testid=ip-new-rule]").fill("198.51.100.0/24"); page.locator("[data-testid=ip-add-rule]").click(); page.locator("[data-testid=ip-save]").click(); time.sleep(1.5)
    check("a list that would lock the administrator out is refused and names their address", mine in page.locator("[data-testid=ip-error]").inner_text() and "would be blocked" in page.locator("[data-testid=ip-error]").inner_text())
    page.locator("[data-testid=ip-rule] button").first.click()          # remove the useless rule again
    page.locator("[data-testid=ip-add-mine]").click(); page.locator("[data-testid=ip-mode]").select_option("monitor"); page.locator("[data-testid=ip-save]").click(); time.sleep(1.5)
    check("monitor mode is saved with the administrator's own address as a rule", page.locator("[data-testid=ip-rule]").count() == 1 and mine in page.locator("[data-testid=ip-rule]").first.inner_text(), page.locator("[data-testid=ip-error]").inner_text())
    st, body = other()
    check("in monitor mode somebody else still gets through", st in (200, 401), (st, body))
    page.locator("[data-testid=ip-refresh]").click(); time.sleep(1.5)
    check("...and shows up as 'would block' in the activity list", page.locator("[data-testid=ip-activity-row]").count() >= 1 and "0" != page.locator("[data-testid=ip-activity-row]").first.locator("td").nth(2).inner_text())
    other_ip = page.locator("[data-testid=ip-activity-row]").first.locator("td").first.inner_text()
    page.locator("[data-testid=ip-check-input]").fill(other_ip); page.locator("[data-testid=ip-check]").click(); time.sleep(1)
    check("the address checker says it is not allowed", "not allowed" in page.locator("[data-testid=ip-check-result]").inner_text(), page.locator("[data-testid=ip-check-result]").inner_text())
    page.locator("[data-testid=ip-mode]").select_option("enforce"); page.locator("[data-testid=ip-save]").click(); time.sleep(1.5)
    st, body = other()
    check("enforced: somebody else gets 403 with their address", st == 403 and other_ip in body and "ip_blocked" in body, (st, body))
    check("the administrator still works", page.evaluate("async () => (await fetch('/api/auth/me')).status") == 200)
    st, body = other("/")
    check("the UI shell shows the denial page to others", st == 403 and "Access denied" in body)
    st, body = other("/api/auth/me", {"X-Forwarded-For": mine})
    check("...and X-Forwarded-For from an untrusted sender is ignored", st == 403)
    page.locator("[data-testid=ip-refresh]").click(); time.sleep(1.5)
    check("blocked requests are counted", page.locator("[data-testid=ip-activity-row]").first.locator("td").nth(1).inner_text() not in ("0", ""))
    page.locator("[data-testid=ip-new-rule]").fill(other_ip); page.locator("[data-testid=ip-add-rule]").click(); page.locator("[data-testid=ip-save]").click(); time.sleep(1.5)
    st, body = other()
    check("adding their address in the UI lets them in (no restart)", st in (200, 401), (st, body))
    page.locator("[data-testid=ip-new-proxy]").fill("10.0.0.0/8"); page.locator("[data-testid=ip-add-proxy]").click(); page.locator("[data-testid=ip-save]").click(); time.sleep(1.5)
    check("a trusted proxy can be added and is listed", page.locator("[data-testid=ip-proxy]").count() == 1 and page.locator("[data-testid=ip-error]").inner_text() == "")
    page.locator("[data-testid=ip-new-proxy]").fill("0.0.0.0/0"); page.locator("[data-testid=ip-add-proxy]").click(); page.locator("[data-testid=ip-save]").click(); time.sleep(1.5)
    check("trusting every address is refused", "allows every address" in page.locator("[data-testid=ip-error]").inner_text() or "/0" in page.locator("[data-testid=ip-error]").inner_text(), page.locator("[data-testid=ip-error]").inner_text())
    page.screenshot(path="/tmp/ip_ui.png")
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
