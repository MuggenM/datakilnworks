#!/usr/bin/env python3
"""Git review UI (Playwright, /usr/bin/python3): diff view, discard, conflict resolution for the shared notebooks against a THROWAWAY studio `gtui`
and THROWAWAY Gitea (scratch/gitea_up.sh; repo ga/nb-rv must exist and be empty):
  docker run -d --name gtui --network gtest_net -p 8117:8891 -v $PWD/web:/workspace/web -v $PWD/docs:/workspace/docs -w /workspace -e WAREHOUSE_DIR=/workspace/warehouse \
     -e INIT_ADMIN_USERNAME=admin -e INIT_ADMIN_PASSWORD_HASH='<hash of adminpassword123>' -e NOTEBOOKS_GIT_REMOTE_URL=http://gtest_gitea:3000/ga/nb-rv.git \
     -e NOTEBOOKS_GIT_TOKEN=$TOK -e NOTEBOOKS_GIT_MODE=pull_request localspark-lakehouse-notebook python -m uvicorn web.app:app --host 0.0.0.0 --port 8891 --no-proxy-headers
  TOK=$TOK GIT_UI_URL=http://localhost:8117 python scratch/verify_git_review_ui.py"""
import json, os, subprocess, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/"); TOK = os.environ["TOK"]
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def sh(script):
    return subprocess.run(["docker", "exec", "gtui", "sh", "-c", script], capture_output=True, text=True)
SH = "/workspace/notebooks/Shared"
CL = f"git -c http.extraHeader='Authorization: token {TOK}' -c user.name=dev -c user.email=d@d"
def ev(page, js):
    for i in range(3):
        try: return page.evaluate(js)
        except Exception as e:
            if "context was destroyed" not in str(e): raise
            time.sleep(2)
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1700, "height": 1100}); page = ctx.new_page(); errs = []; page.on("pageerror", lambda e: errs.append(str(e) + " | " + str(getattr(e, "stack", ""))[:600]))
    api = ctx.request
    check("admin logs in", api.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    sh("python -c \"import sqlite3;c=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit()\"")
    sh(f"mkdir -p {SH} && cd {SH} && printf 'l1\\nl2\\nl3\\nl4\\nl5\\nl6\\nl7\\nl8\\n' > a.txt && printf 'keep\\n' > b.txt")
    check("connect and the first commit (to main) and push", api.post(f"{BASE}/api/git/repos/notebooks/connect").ok and api.post(f"{BASE}/api/git/repos/notebooks/commit", data=json.dumps({"message": "init"}), headers={"Content-Type": "application/json"}).ok and api.post(f"{BASE}/api/git/repos/notebooks/push").ok)
    page.goto(BASE, wait_until="networkidle"); time.sleep(5)
    ev(page, "() => { Alpine.$data(document.body).openNbGit(); }"); time.sleep(2)
    # ---- diff view
    sh(f"cd {SH} && printf 'l1\\nl2 EDITED\\nl3\\nl4\\nl5\\nl6\\nl7 MINE\\nl8\\n' > a.txt && printf 'brand new\\n' > n.txt && rm b.txt")
    page.locator("[data-testid=nb-git-refresh]").click(); time.sleep(1.5)
    page.locator("[data-testid=nb-git-modal] [data-testid=git-review-open]").click(); page.locator("[data-testid=gr-file]").first.wait_for(timeout=8000)
    rows = page.locator("[data-testid=gr-file]").all_inner_texts(); n_files = len(rows)
    check("the review dialog lists the changed files with their status", any(r.startswith("M\na.txt") for r in rows) and any(r.startswith("D\nb.txt") for r in rows) and any(r.startswith("A\nn.txt") for r in rows), rows)
    page.locator("[data-testid=gr-line]").first.wait_for(timeout=8000)
    check("the first file's diff is shown with added and removed lines", page.locator("[data-testid=gr-line]").count() >= 3 and "l2 EDITED" in page.locator("[data-testid=gr-diff]").inner_text() and "l2" in page.locator("[data-testid=gr-diff]").inner_text(), page.locator("[data-testid=gr-diff]").inner_text()[:200])
    page.locator("[data-testid=gr-file]:has-text('n.txt')").click(); time.sleep(1)
    check("a new file is shown as all added", "brand new" in page.locator("[data-testid=gr-diff]").inner_text())
    page.once("dialog", lambda d: d.accept()); page.locator("[data-testid=gr-discard]").click(); time.sleep(2)
    check("discarding removes the file from the list and from disk", page.locator("[data-testid=gr-file]").count() == n_files - 1 and sh(f"test -e {SH}/n.txt").returncode != 0)
    page.locator("[data-testid=gr-close]").click(); time.sleep(0.5)
    # ---- a change branch, then a conflicting change on the base
    api.post(f"{BASE}/api/git/repos/notebooks/commit", data=json.dumps({"message": "my edits"}), headers={"Content-Type": "application/json"}); api.post(f"{BASE}/api/git/repos/notebooks/push")
    sh(f"cd /tmp && rm -rf dev && {CL} clone -q http://gtest_gitea:3000/ga/nb-rv.git dev && cd dev && printf 'l1\\nl2 THEIRS\\nl3\\nl4\\nl5\\nl6\\nl7 THEIRS\\nl8\\n' > a.txt && printf 'kept and changed\\n' > b.txt && git add -A && {CL} commit -q -m theirs && {CL} push -q origin HEAD:main")
    ev(page, "() => { Alpine.$data(document.body).openNbGit(); }"); time.sleep(1.5)
    page.locator("[data-testid=nb-git-refresh]").click(); time.sleep(2)
    page.locator("[data-testid=nb-git-modal] [data-testid=git-review-open]").click(); time.sleep(1.5)
    page.locator("[data-testid=gr-scope-branch]").click(); page.locator("[data-testid=gr-file]").first.wait_for(timeout=8000)
    check("'this branch vs base' shows what a reviewer would see", "a.txt" in page.locator("[data-testid=gr-file]").first.inner_text() and page.locator("[data-testid=gr-discard]").count() == 1 and not page.locator("[data-testid=gr-discard]").is_visible())
    page.locator("[data-testid=gr-close]").click(); time.sleep(0.5)
    page.locator("[data-testid=nb-git-pr-update]").click()
    page.locator("[data-testid=gr-conflict]").first.wait_for(timeout=15000)
    check("'Update from base' finds conflicts and opens the resolver", page.locator("[data-testid=gr-conflict]").count() == 2 and page.locator("[data-testid=gr-tab-conflicts]").inner_text().strip().endswith("2"), page.locator("[data-testid=gr-conflict]").all_inner_texts())
    check("Finish is disabled while files are unresolved", page.locator("[data-testid=gr-finish]").is_disabled())
    page.locator("[data-testid=gr-conflict]:has-text('a.txt')").click(); page.locator("[data-testid=gr-hunk]").first.wait_for(timeout=5000)
    check("each conflict hunk shows yours and incoming side by side", page.locator("[data-testid=gr-hunk]").count() == 2 and "l2 EDITED" in page.locator("[data-testid=gr-hunk]").first.inner_text() and "l2 THEIRS" in page.locator("[data-testid=gr-hunk]").first.inner_text())
    page.locator("[data-testid=gr-resolve-file]").click(); time.sleep(1)
    check("resolving without deciding every hunk is refused with a hint", "Decide every conflict" in page.locator("[data-testid=gr-error]").inner_text())
    page.locator("[data-testid=gr-pick-both]").first.check(); page.locator("[data-testid=gr-pick-custom]").nth(1).check(); page.locator("[data-testid=gr-custom-text]:visible").fill("l7 combined by hand")
    page.locator("[data-testid=gr-resolve-file]").click(); time.sleep(2)
    check("with every hunk decided the file is resolved and leaves the list", page.locator("[data-testid=gr-conflict]").count() == 1 and page.locator("[data-testid=gr-reopen]").count() == 1)
    a_txt = sh(f"cat {SH}/a.txt").stdout
    check("the file has both l2 lines and the typed l7", "l2 EDITED\nl2 THEIRS" in a_txt and "l7 combined by hand" in a_txt and "<<<<<<<" not in a_txt, a_txt)
    page.locator("[data-testid=gr-keep]").wait_for(timeout=5000)
    check("a modify/delete conflict offers keep or delete", page.locator("[data-testid=gr-delete]").is_visible() and page.locator("[data-testid=gr-keep]").is_visible() and not page.locator("[data-testid=gr-resolve-file]").is_visible())
    page.locator("[data-testid=gr-keep]").click(); time.sleep(2)
    check("everything is resolved and Finish is enabled", page.locator("[data-testid=gr-all-resolved]").is_visible() and page.locator("[data-testid=gr-finish]").is_enabled())
    page.locator("[data-testid=gr-reopen]").first.click(); time.sleep(1.5)
    check("a resolved file can be reopened", page.locator("[data-testid=gr-conflict]").count() == 1)
    page.locator("[data-testid=gr-take-theirs]").click(); time.sleep(2)
    check("whole-file 'take incoming' resolves it", "l7 THEIRS" in sh(f"cat {SH}/a.txt").stdout and page.locator("[data-testid=gr-finish]").is_enabled())
    page.locator("[data-testid=gr-finish]").click(); time.sleep(3)
    log = sh(f"cd {SH} && git log -1 --format=%P").stdout.split()
    check("Finish commits the merge (two parents) and returns to the change view", len(log) == 2 and page.locator("[data-testid=gr-tab-changes]").is_visible() and not ev(page, "() => Alpine.$data(document.body).gitReview.merge.in_progress"), log)
    # ---- abort
    page.locator("[data-testid=gr-close]").click(); time.sleep(0.5)
    api.post(f"{BASE}/api/git/repos/notebooks/push")
    sh(f"cd /tmp/dev && git pull -q origin main && printf 'X\\n' > z.txt && git add -A && {CL} commit -q -m z && {CL} push -q origin HEAD:main && printf 'X changed\\n' > z.txt && git add -A && {CL} commit -q -m z2 && {CL} push -q origin HEAD:main")
    sh(f"cd {SH} && printf 'mine z\\n' > z.txt")
    api.post(f"{BASE}/api/git/repos/notebooks/commit", data=json.dumps({"message": "my z"}), headers={"Content-Type": "application/json"})
    head = sh(f"cd {SH} && git rev-parse HEAD").stdout.strip()
    ev(page, "() => { Alpine.$data(document.body).openNbGit(); }"); time.sleep(2)
    page.locator("[data-testid=nb-git-refresh]").click(); time.sleep(2)
    page.locator("[data-testid=nb-git-pr-update]").click(); page.locator("[data-testid=gr-conflict]").first.wait_for(timeout=15000)
    page.once("dialog", lambda d: d.accept()); page.locator("[data-testid=gr-abort]").click(); time.sleep(3)
    check("Abort returns to exactly the commit before the merge", sh(f"cd {SH} && git rev-parse HEAD").stdout.strip() == head and sh(f"cd {SH} && test -e .git/MERGE_HEAD").returncode != 0)
    page.screenshot(path="/tmp/git_review_ui.png")
    # 'u is not a function ... Promise.all (index 3)' comes from the app's own 4-way loaders (initAutoLoader / initGovernance), not from this dialog
    own = [e for e in errs if "Promise.all (index 3)" not in e]
    check("no page errors from the review dialog", not own, own)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
