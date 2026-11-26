#!/usr/bin/env python3
"""
UI verification for the in-Studio notebook workspace (Playwright, headless Chromium).
Point it at a THROWAWAY studio started with NOTEBOOKS_DIR=<dir> containing
Users/{admin/admin_only,analyst_bob/bob_nb,lead_engineer/lead_nb}.ipynb and Shared/team.ipynb:
    NOTEBOOKS_UI_URL=http://localhost:8100 python3 scratch/verify_notebooks_ui.py
Tests:
1. No JupyterLab entry point anywhere; no JS errors.
2. A user's workspace tree shows only their own folder and Shared (an admin sees everyone's).
3. Cells run in the embedded runner and show their output.
4. Once a masking policy applies to a user, the notebook opens and is editable but cannot be run, with an explanation.
"""

import os
import sys
import time
from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("NOTEBOOKS_UI_URL", "http://localhost:8100").rstrip("/")
OUT_DIR = os.getenv("NOTEBOOKS_UI_OUT", "/tmp/notebooks_ui_shots")
os.makedirs(OUT_DIR, exist_ok=True)
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def session(browser, user, password):
    ctx = browser.new_context(viewport={"width": 1440, "height": 1000})
    page = ctx.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    ctx.request.post(f"{BASE_URL}/api/auth/login", data={"username": user, "password": password})
    page.goto(BASE_URL, wait_until="networkidle")
    time.sleep(1.2)
    return ctx, page, errors


def act(page, code):
    return page.evaluate(f"async () => {{ const d = Alpine.$data(document.body); {code} }}")


def open_workspace(page):
    page.get_by_role("button", name="Workspace").first.click()
    time.sleep(1.5)
    act(page, "d.workspaceExpandedDirs['Users'] = true; d.workspaceExpandedDirs['Shared'] = true;")
    time.sleep(0.5)


def tree_paths(page):
    return act(page, """
      const out = [];
      const walk = (nodes) => (nodes || []).forEach(n => { out.push(n.rel_path); walk(n.children); });
      walk(d.workspaceTree);
      return out;""")


def open_notebook(page, rel):
    page.evaluate("""async (rel) => {
      const d = Alpine.$data(document.body);
      const find = (nodes) => {
        for (const n of (nodes || [])) { if (n.rel_path === rel) return n; const c = find(n.children); if (c) return c; }
        return null;
      };
      const node = find(d.workspaceTree);
      if (node) { d.selectWorkspaceItem(node); }
    }""", rel)
    time.sleep(2)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        print("\n1. Admin")
        ctx, page, errors = session(browser, "admin", "adminpassword123")
        check("no JupyterLab link in the header", page.locator("a:has-text('JupyterLab')").count() == 0)
        open_workspace(page)
        paths = tree_paths(page)
        check("the admin's tree shows every user's notebooks", {"Users/admin/admin_only.ipynb", "Users/analyst_bob/bob_nb.ipynb", "Shared/team.ipynb"} <= set(paths), paths)
        check("no 'Full JupyterLab' switcher or 'Open in JupyterLab' action", page.locator("text=JupyterLab").count() == 0)
        open_notebook(page, "Users/admin/admin_only.ipynb")
        page.get_by_role("button", name="Run All Cells").click()
        page.locator("text=admin secret notebook").first.wait_for(state="visible", timeout=30000)
        check("cells run in the embedded runner and show their output", True)
        page.screenshot(path=f"{OUT_DIR}/n1_admin_notebook.png")
        check("no JS errors (admin)", not errors, errors[:3])
        ctx.close()

        print("\n2. A regular user")
        ctx, page, errors = session(browser, "analyst_bob", "userpassword123")
        open_workspace(page)
        paths = tree_paths(page)
        check("the tree shows the user's own notebook and Shared", "Users/analyst_bob/bob_nb.ipynb" in paths and "Shared/team.ipynb" in paths, paths)
        check("...and nothing of other users", not any(x.startswith("Users/admin") or x.startswith("Users/lead_engineer") for x in paths), paths)
        forbidden = ctx.request.get(f"{BASE_URL}/api/workspace/file?path=Users/admin/admin_only.ipynb")
        check("asking for another user's notebook directly is refused", forbidden.status == 403, forbidden.status)
        open_notebook(page, "Users/analyst_bob/bob_nb.ipynb")
        page.get_by_role("button", name="Run All Cells").click()
        page.locator("text=hello from bob").first.wait_for(state="visible", timeout=30000)
        check("the user can run their own notebook", not page.locator("text=Masking policies apply to you").first.is_visible())

        # a masking policy now applies to this user
        admin_ctx = browser.new_context()
        admin_ctx.request.post(f"{BASE_URL}/api/auth/login", data={"username": "admin", "password": "adminpassword123"})
        admin_ctx.request.post(f"{BASE_URL}/api/governance/tags", data={"tag_key": "pii2", "allowed_values": ["x"]})
        admin_ctx.request.post(f"{BASE_URL}/api/governance/masking-policies", data={"name": "mask all pii2", "tag_key": "pii2", "mask_type": "null"})
        admin_ctx.request.put(f"{BASE_URL}/api/governance/objects/warehouse/tags", data={"set": [{"tag_key": "pii2", "tag_value": "x"}]})
        admin_ctx.close()
        page.reload(wait_until="networkidle")
        time.sleep(1.5)
        open_workspace(page)
        open_notebook(page, "Users/analyst_bob/bob_nb.ipynb")
        page.locator("text=Masking policies apply to you").first.wait_for(state="visible", timeout=10000)
        check("a masked user sees why cells cannot be run", True)
        check("Run All is disabled", page.get_by_role("button", name="Run All Cells").is_disabled())
        run = ctx.request.post(f"{BASE_URL}/api/workspace/notebook/cell/run", data={"path": "Users/analyst_bob/bob_nb.ipynb", "cell_index": 1})
        check("the API refuses execution as well", run.status == 403, run.status)
        check("editing still works (add a cell)", ctx.request.post(f"{BASE_URL}/api/workspace/notebook/cell/add",
                                                                     data={"path": "Users/analyst_bob/bob_nb.ipynb", "after_index": 1, "type": "code"}).ok)
        page.screenshot(path=f"{OUT_DIR}/n2_masked_user.png")
        check("no JS errors (regular user)", not errors, errors[:3])
        ctx.close()
        browser.close()

    print(f"\nScreenshots: {OUT_DIR}")
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All notebook UI checks passed.")


if __name__ == "__main__":
    main()
