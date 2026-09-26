#!/usr/bin/env python3
"""S3 bucket events end to end (run on the HOST with /usr/bin/python3; needs Playwright, docker, the studio image and minio/minio). It builds a throwaway
network `s3evnet`, a studio `s3evui` (port 8117) and a real MinIO `s3evminio` that POSTs its bucket notifications to the studio's receiver with a token
created through the UI/API. A pipeline is created in the dialog with 'S3 events' and a one-hour rescan; an upload must be ingested within seconds
(the rescan cannot explain it). Everything is removed at the end."""
import os, subprocess, sys, time
from playwright.sync_api import sync_playwright
import _ui_slow
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = "http://localhost:8117"; NET, UI, MINIO = "s3evnet", "s3evui", "s3evminio"
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def sh(*a, **k): return subprocess.run(a, capture_output=True, text=True, **k)
def cleanup():
    for c in (UI, MINIO): sh("docker", "rm", "-f", c)
    sh("docker", "network", "rm", NET)
def ex(code, container=UI): return sh("docker", "exec", "-w", "/workspace", container, "python", "-c", code)

SEED = r'''
import os, sqlite3, time, boto3
c = sqlite3.connect("/workspace/warehouse/.metadata/auth.db"); c.execute("UPDATE users SET must_change_password=0"); c.commit()
from web import mounts
mounts.save_mounts([{"id": "m1", "type": "s3", "catalog_name": "lake", "name": "lake", "config": {"bucket": "landing", "endpoint": "s3evminio:9000", "key_id": "minioadmin", "secret": "minioadmin", "region": "us-east-1", "url_style": "path", "use_ssl": False}}])
'''
NOTIFY = r'''
import boto3, time
s3 = boto3.client("s3", endpoint_url="http://s3evminio:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
for _ in range(30):
    try: s3.list_buckets(); break
    except Exception: time.sleep(1)
try: s3.create_bucket(Bucket="landing")
except Exception: pass
s3.put_bucket_notification_configuration(Bucket="landing", NotificationConfiguration={"QueueConfigurations": [{"QueueArn": "arn:minio:sqs::dkw:webhook", "Events": ["s3:ObjectCreated:*"]}]})
print("configured")
'''
UPLOAD = lambda key, body: f'''
import boto3
boto3.client("s3", endpoint_url="http://s3evminio:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1").put_object(Bucket="landing", Key="{key}", Body={body!r}.encode())
'''

def main():
    cleanup(); sh("docker", "network", "create", NET)
    sh("docker", "run", "-d", "--name", UI, "--network", NET, "-p", "8117:8891", "-v", f"{ROOT}/web:/workspace/web", "-v", f"{ROOT}/docs:/workspace/docs", "-w", "/workspace",
       "-e", "WAREHOUSE_DIR=/workspace/warehouse", "-e", "INIT_ADMIN_USERNAME=admin", "-e", f"INIT_ADMIN_PASSWORD_HASH={HASH}", "-e", "S3_EVENTS_DEBOUNCE=1",
       "localspark-lakehouse-notebook", "python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8891", "--no-proxy-headers")
    for _ in range(60):
        if sh("curl", "-s", "-o", "/dev/null", f"{BASE}/api/docs").returncode == 0 and ex("import sqlite3;sqlite3.connect('/workspace/warehouse/.metadata/auth.db').execute('select 1 from users')").returncode == 0: break
        time.sleep(1)
    time.sleep(3); r = ex(SEED); check("seeded the S3 mount", r.returncode == 0, r.stderr[-300:])
    try:
        with sync_playwright() as p:
            b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1440, "height": 1300}); page = _ui_slow.apply(ctx.new_page()); errors = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            check("logged in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
            page.goto(BASE, wait_until="networkidle"); time.sleep(1)
            act = lambda code: page.evaluate(f"async () => {{ const d = Alpine.$data(document.body); {code} }}")
            act("d.currentView = 'autoloader'; await d.initAutoLoader();")
            page.click("[data-testid=open-s3events]"); page.wait_for_selector("[data-testid=s3events-modal]", state="visible")
            page.wait_for_function("() => (document.querySelector('[data-testid=s3events-url]').innerText || '').trim().length > 0", timeout=30000)
            check("the modal shows the receiver URL", page.locator("[data-testid=s3events-url]").inner_text().endswith("/hooks/s3-events"))
            page.fill("[data-testid=s3events-token-name]", "MinIO test"); page.click("[data-testid=s3events-token-create]")
            page.wait_for_selector("[data-testid=s3events-secret]", state="visible")
            token = page.locator("[data-testid=s3events-secret]").inner_text().strip()
            check("a token is shown once and listed", token.startswith("dkw_s3ev_") and page.locator("[data-testid=s3events-token-row]").count() == 1)
            page.screenshot(path="/tmp/s3ev_modal.png"); page.keyboard.press("Escape")
            # a real MinIO that posts to the studio
            sh("docker", "run", "-d", "--name", MINIO, "--network", NET, "-e", "MINIO_NOTIFY_WEBHOOK_ENABLE_dkw=on", "-e", f"MINIO_NOTIFY_WEBHOOK_ENDPOINT_dkw=http://{UI}:8891/hooks/s3-events",
               "-e", f"MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN_dkw={token}", "minio/minio", "server", "/data")
            print(sh("docker", "inspect", "-f", "{{.Config.Image}} {{.State.Status}}", MINIO).stdout.strip())
            for _ in range(4):
                r = ex(NOTIFY)
                if "configured" in r.stdout: break
                time.sleep(2)
            if "configured" not in r.stdout:
                print(sh("docker", "ps", "-a").stdout[-800:]); print(sh("docker", "logs", "--tail", "25", MINIO).stderr[-1500:]); print(r.stderr[-600:])
            check("MinIO is configured to notify the studio", "configured" in r.stdout, r.stderr[-400:])
            if "configured" not in r.stdout: raise SystemExit("MinIO could not be configured; the rest of the test cannot run")
            ex(UPLOAD("in/seed.csv", "id,val\n1,a\n"))
            # the pipeline is created in the dialog
            page.click("button:has-text('New Pipeline'):visible"); page.wait_for_selector("text=Create Auto-Loader Pipeline >> visible=true")
            act("Object.assign(d.newPipelineForm, {name: 'Events demo', source_volume_path: 's3://landing/in', file_pattern: '*.csv', target_table: 'events_demo', poll_interval_seconds: 10});")
            time.sleep(0.5); check("the S3 events option is offered for s3:// paths", page.evaluate("() => { const o = document.querySelector('[data-testid=opt-s3events]'); return !!o && getComputedStyle(o).display !== 'none'; }"))
            act("d.newPipelineForm.source_mount_id = 'm1'; d.newPipelineForm.poll_interval_seconds = 's3events'; d.newPipelineForm.watch_sweep_seconds = 3600;")
            time.sleep(0.5); check("the hint explains the mode", page.locator("[data-testid=s3events-hint]").is_visible())
            page.screenshot(path="/tmp/s3ev_form.png")
            page.click("button:has-text('Create Pipeline'):visible"); page.wait_for_selector("[data-testid=s3events-badge] >> visible=true", timeout=8000)
            check("the pipeline card shows the S3 events badge", True)
            pipe = page.evaluate("() => Alpine.$data(document.body).autoloaderPipelines.find(p => p.name === 'Events demo')")
            check("it is stored with s3_events, no cron and no file watch", pipe["s3_events"] and not pipe["cron_schedule"] and not pipe["watch_enabled"] and pipe["watch_sweep_seconds"] == 3600, pipe)
            def rows():
                act("await d.fetchAutoloaderPipelines();"); return page.evaluate("() => Alpine.$data(document.body).autoloaderPipelines.find(p => p.name === 'Events demo').total_rows_ingested") or 0
            dl = time.time() + 30                    # the first cycle after creation loads the seed; the uploads below must arrive by event
            while time.time() < dl and rows() < 1: time.sleep(1)
            base = rows(); t0 = time.time()
            ex(UPLOAD("in/new1.csv", "id,val\n10,x\n11,y\n12,z\n"))
            deadline = time.time() + 25
            while time.time() < deadline and rows() < 4: time.sleep(1)
            got = rows(); check("an upload is ingested within seconds, by the event (rescan is one hour)", got == 4 and time.time() - t0 < 20, (base, got, time.time() - t0))
            ex(UPLOAD("in/ignored.txt", "not a csv")); ex(UPLOAD("other/x.csv", "id,val\n1,q\n")); time.sleep(4)
            check("objects the pipeline does not care about are not loaded", rows() == got)
            act("await d.openS3Events();"); page.wait_for_selector("[data-testid=s3events-pipeline-row]", state="visible")
            check("the modal shows the pipeline with its wake count and the recent notifications", page.locator("[data-testid=s3events-pipeline-row]").count() == 1 and "wake" in page.locator("[data-testid=s3events-pipeline-row]").inner_text() and page.locator("[data-testid=s3events-recent-row]").count() >= 1)
            check("the token shows it was used", "never used" not in page.locator("[data-testid=s3events-token-row]").inner_text())
            page.screenshot(path="/tmp/s3ev_modal2.png")
            page.once("dialog", lambda d: d.accept()); page.click("[data-testid=s3events-token-row] >> text=Revoke"); time.sleep(1)
            check("revoking shows it as revoked", "revoked" in page.locator("[data-testid=s3events-token-row]").inner_text())
            ex(UPLOAD("in/new2.csv", "id,val\n20,k\n")); time.sleep(5)
            check("after revoking, events are refused and nothing new arrives (the rescan is an hour away)", rows() == got)
            check("no JS errors", not errors, errors)
            b.close()
    finally:
        if FAIL: print(sh("docker", "logs", "--tail", "40", UI).stderr[-2500:])
        cleanup()
    print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
main()
