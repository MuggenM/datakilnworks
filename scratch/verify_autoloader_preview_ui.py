#!/usr/bin/env python3
"""Preview of local and s3:// sources in the Auto-Loader create dialog (Playwright, /usr/bin/python3) against a THROWAWAY studio `pvui` and MinIO `pvminio`
on network pvnet (see scratch/test_autoloader_preview.py for the MinIO command):
  docker run -d --name pvui --network pvnet -p 8117:8891 -v $PWD/web:/workspace/web -v $PWD/docs:/workspace/docs -w /workspace -e WAREHOUSE_DIR=/workspace/warehouse \
     -e INIT_ADMIN_USERNAME=admin -e INIT_ADMIN_PASSWORD_HASH='<hash of adminpassword123>' localspark-lakehouse-notebook python -m uvicorn web.app:app --host 0.0.0.0 --port 8891 --no-proxy-headers
  GIT_UI_URL=http://localhost:8117 python scratch/verify_autoloader_preview_ui.py"""
import os, subprocess, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
SEED = r'''
import os, time, sqlite3, boto3
os.makedirs("/workspace/warehouse/landing_ui", exist_ok=True)
now = time.time()
for name, start, age in (("old.csv", 0, 500), ("new.csv", 100, 5)):
    p = "/workspace/warehouse/landing_ui/" + name
    open(p, "w").write("id,name\n" + "\n".join(f"{start+i},n{start+i}" for i in range(25)) + "\n"); os.utime(p, (now - age, now - age))
c = sqlite3.connect("/workspace/warehouse/.metadata/auth.db"); c.execute("UPDATE users SET must_change_password=0"); c.commit()
s3 = boto3.client("s3", endpoint_url="http://pvminio:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
for _ in range(20):
    try: s3.list_buckets(); break
    except Exception: time.sleep(1)
try: s3.create_bucket(Bucket="landing")
except Exception: pass
s3.put_object(Body=b"sensor,temp\nA,20.5\nB,21.5\n", Bucket="landing", Key="ui/s.csv")
from web import mounts
mounts.save_mounts([{"id": "m1", "type": "s3", "catalog_name": "lake", "name": "lake", "config": {"bucket": "landing", "endpoint": "pvminio:9000", "key_id": "minioadmin", "secret": "minioadmin", "region": "us-east-1", "url_style": "path", "use_ssl": False}}])
'''
def ev(page, js):
    for i in range(3):
        try: return page.evaluate(js)
        except Exception as e:
            if "context was destroyed" not in str(e): raise
            time.sleep(2)
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1500, "height": 1300}); page = ctx.new_page(); errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("login", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    r = subprocess.run(["docker", "exec", "-w", "/workspace", "pvui", "python", "-c", SEED], capture_output=True, text=True); check("seeded a folder, a bucket and an S3 mount", r.returncode == 0, r.stderr[-300:])
    page.goto(BASE, wait_until="networkidle"); time.sleep(5)
    ev(page, "async () => { const d = Alpine.$data(document.body); d.currentView = 'autoloader'; await d.fetchAutoloaderPipelines(); d.showCreatePipelineModal = true; await d.fetchMounts(); }"); time.sleep(1.5)
    check("the volume source shows a Preview button, disabled while the path is empty", page.locator("[data-testid=path-preview]").is_visible() and page.locator("[data-testid=path-preview]").is_disabled())
    page.locator("[data-testid=pipe-source-path]").fill("/workspace/warehouse/landing_ui"); ev(page, "() => { Alpine.$data(document.body).newPipelineForm.file_pattern = '*.csv'; }")
    page.locator("[data-testid=path-preview]").click(); page.locator("[data-testid=path-preview-table] tbody tr").first.wait_for(timeout=15000)
    txt = page.locator("[data-testid=path-preview-table]").inner_text()
    check("a local folder previews the oldest file's columns and rows", "id" in txt and "name" in txt and page.locator("[data-testid=path-preview-table] tbody tr").count() == 10 and "n0" in txt, txt[:200])
    check("both files are listed as buttons and the oldest is selected", page.locator("[data-testid=path-preview-files] button").count() == 2 and "old.csv" in page.locator("[data-testid=path-preview-files] button.bg-dbx-orange").inner_text())
    page.locator("[data-testid=path-preview-files] button:has-text('new.csv')").click(); time.sleep(2)
    check("clicking another file previews that one", "n100" in page.locator("[data-testid=path-preview-table]").inner_text())
    page.locator("[data-testid=pipe-source-path]").fill("/workspace/warehouse/not_there"); time.sleep(0.3)
    check("editing the path clears the old preview", not page.locator("[data-testid=path-preview-table]").is_visible())
    page.locator("[data-testid=path-preview]").click(); time.sleep(1.5)
    check("a folder that does not exist yet is explained", "does not exist yet" in page.locator("[data-testid=path-preview-error]").inner_text())
    page.locator("[data-testid=pipe-source-path]").fill("/workspace/warehouse/.metadata"); page.locator("[data-testid=path-preview]").click(); time.sleep(1.5)
    check("a hidden folder is refused", "Hidden folders" in page.locator("[data-testid=path-preview-error]").inner_text())
    page.locator("[data-testid=pipe-source-path]").fill("s3://landing/ui/"); page.locator("[data-testid=path-preview]").click(); page.locator("[data-testid=path-preview-table] tbody tr").first.wait_for(timeout=20000)
    txt = page.locator("[data-testid=path-preview-table]").inner_text()
    check("an s3:// prefix is previewed through the storage mount, read in place", "sensor" in txt and "20.5" in txt and "read in place" in page.locator("[data-testid=path-preview-block]").inner_text(), txt[:200])
    page.locator("[data-testid=pipe-source-path]").fill("s3://landing/ui/"); ev(page, "() => { Alpine.$data(document.body).newPipelineForm.source_mount_id = 'ghost'; }")
    page.locator("[data-testid=path-preview]").click(); time.sleep(2)
    check("an unknown mount is reported", "does not exist" in page.locator("[data-testid=path-preview-error]").inner_text())
    ev(page, "() => { Alpine.$data(document.body).pipelineSourceKind = 'connection'; }"); time.sleep(0.5)
    check("the connection source keeps its own preview (the path preview is hidden there)", not page.locator("[data-testid=path-preview]").is_visible() and page.locator("[data-testid=pipe-preview]").count() == 1)
    page.screenshot(path="/tmp/path_preview_ui.png")
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
