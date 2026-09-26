#!/usr/bin/env python3
"""The optional `proxy` profile (Traefik with TLS) in front of the studio, end to end (host script, /usr/bin/python3; docker, openssl, the studio image and
the traefik image). Traefik is started with EXACTLY the command line and image that `docker compose --profile proxy config` gives, and the shipped
deploy/traefik/dynamic/routers.yml plus a certs.yml (the documented way to bring your own certificate) in a throwaway network `dshhttps` (subnet 172.16.241.0/24):
a studio `datakilnworks-studio` (DELTA_SHARING_ENDPOINT=https://dsh.test/delta-sharing, TRUSTED_PROXIES = that subnet), Traefik (alias dsh.test) and a client
container that trusts the test CA. Checks: https works, http redirects, the real `delta-sharing` client reads the table (Parquet format) and the change feed
through it, per-recipient IP rules see the REAL client address behind the proxy (X-Forwarded-For from the trusted subnet only). The kernel's Delta format
is expected NOT to work (it only fetches S3/Azure/GCS hosts, see CLAUDE.md). Everything is removed at the end."""
import json, os, shutil, subprocess, sys, tempfile, time
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:600]}" if d and not c else ""))
    if not c: FAIL.append(n)
sh = lambda *a, **k: subprocess.run(a, capture_output=True, text=True, **k)
def cleanup():
    for c in ("datakilnworks-studio", "dshtraefik", "dshclient"): sh("docker", "rm", "-f", c)
    sh("docker", "network", "rm", "dshhttps")
T = tempfile.mkdtemp(prefix="dshhttps_"); os.chmod(T, 0o755)
cleanup()
try:
    cfg = json.loads(sh("docker", "compose", "--profile", "proxy", "config", "--format", "json", cwd=ROOT).stdout)["services"]["traefik"]
    check("the compose service is in the proxy profile only", cfg.get("profiles") == ["proxy"] and "traefik" not in sh("docker", "compose", "config", "--services", cwd=ROOT).stdout.split())
    check("no Docker socket, no dashboard", not any("docker.sock" in str(v) for v in cfg.get("volumes", [])) and "--api=false" in cfg["command"] and not any("providers.docker" in c for c in cfg["command"]))
    os.makedirs(f"{T}/dynamic"); os.makedirs(f"{T}/certs")
    for f in os.listdir(f"{ROOT}/deploy/traefik/dynamic"): shutil.copy(f"{ROOT}/deploy/traefik/dynamic/{f}", f"{T}/dynamic/{f}")
    sh("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-keyout", f"{T}/certs/privkey.pem", "-out", f"{T}/certs/fullchain.pem", "-subj", "/CN=dsh.test", "-addext", "subjectAltName=DNS:dsh.test")
    shutil.copy(f"{ROOT}/deploy/traefik/certs.yml.example", f"{T}/dynamic/certs.yml")
    for f in (f"{T}/certs/privkey.pem", f"{T}/certs/fullchain.pem"): os.chmod(f, 0o644)
    os.chmod(f"{T}/certs", 0o755); os.chmod(f"{T}/dynamic", 0o755)
    sh("docker", "network", "create", "--subnet", "172.16.241.0/24", "dshhttps")
    sh("docker", "run", "-d", "--name", "datakilnworks-studio", "--network", "dshhttps", "-v", f"{ROOT}/web:/workspace/web", "-w", "/workspace", "-e", "WAREHOUSE_DIR=/workspace/warehouse", "-e", "INIT_ADMIN_USERNAME=admin",
       "-e", f"INIT_ADMIN_PASSWORD_HASH={HASH}", "-e", "DELTA_SHARING_ENDPOINT=https://dsh.test/delta-sharing", "-e", "TRUSTED_PROXIES=172.16.241.0/24", "localspark-lakehouse-notebook",
       "python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000", "--no-proxy-headers")
    r = sh("docker", "run", "-d", "--name", "dshtraefik", "--network", "dshhttps", "--network-alias", "dsh.test", "-v", f"{T}/dynamic:/etc/traefik/dynamic:ro", "-v", f"{T}/certs:/certs:ro", cfg["image"], *cfg["command"])
    check("Traefik starts with the shipped command line", r.returncode == 0, r.stderr)
    for _ in range(90):
        if sh("docker", "exec", "datakilnworks-studio", "python", "-c", "import urllib.request;urllib.request.urlopen('http://localhost:8000/healthz')").returncode == 0: break
        time.sleep(1)
    time.sleep(3)
    check("Traefik keeps running (no configuration error)", sh("docker", "inspect", "-f", "{{.State.Running}}", "dshtraefik").stdout.strip() == "true", sh("docker", "logs", "dshtraefik").stdout[-800:] + sh("docker", "logs", "dshtraefik").stderr[-800:])
    seed = r'''
import json, sys, pandas as pd
sys.path.insert(0, "/workspace")
from deltalake import write_deltalake, DeltaTable
W = "/workspace/warehouse"
write_deltalake(W + "/sales/orders", pd.DataFrame({"id": [1, 2, 3], "region": ["EMEA", "APAC", "EMEA"], "amount": [1.5, 2.5, 3.5]}), partition_by=["region"], configuration={"delta.enableChangeDataFeed": "true"})
DeltaTable(W + "/sales/orders").update({"amount": "9.5"}, predicate="id = 1")
from web import delta_sharing as ds
ds.create_share("acme", "", "admin"); ds.add_table("acme", "warehouse.sales.orders", "admin", history=True)
r = ds.create_recipient("ACME", "", ["acme"], None, "admin"); r2 = ds.create_recipient("LOCKED", "", ["acme"], None, "admin")
print(json.dumps({"acme": ds.profile("https://dsh.test/delta-sharing", r["token"], None), "locked": ds.profile("https://dsh.test/delta-sharing", r2["token"], None), "locked_id": r2["recipient"]["id"]}))
'''
    r = sh("docker", "exec", "-w", "/workspace", "datakilnworks-studio", "python", "-c", seed)
    check("seeded a CDF table, a share and two recipients", r.returncode == 0, r.stderr[-500:])
    profs = json.loads(r.stdout.strip().splitlines()[-1])
    for name in ("acme", "locked"): open(f"{T}/{name}.share", "w").write(json.dumps(profs[name]))
    client = r'''
import delta_sharing, json, os, requests
out = {}
def run(name, fn):
    try: out[name] = fn()
    except Exception as e: out[name] = "ERROR " + repr(e)[:300]
pf = "/data/acme.share"
run("parquet", lambda: sorted(delta_sharing.load_as_pandas(pf + "#acme.sales.orders")["id"]))
run("changes", lambda: sorted(set(delta_sharing.load_table_changes_as_pandas(pf + "#acme.sales.orders", starting_version=0)["_change_type"])))
run("delta", lambda: sorted(delta_sharing.load_as_pandas(pf + "#acme.sales.orders", use_delta_format=True)["id"]))
run("http_redirect", lambda: (lambda r: [r.status_code, r.headers.get("location", "")])(requests.get("http://dsh.test/", allow_redirects=False, verify=False)))
run("ui", lambda: requests.get("https://dsh.test/healthz").status_code)
prof = json.load(open("/data/locked.share")); H = {"Authorization": "Bearer " + prof["bearerToken"]}
run("locked_open", lambda: requests.get(prof["endpoint"] + "/shares", headers=H).status_code)
open("/data/ip.txt", "w").write(os.popen("hostname -i").read().strip())
print("RESULT " + json.dumps(out))
'''
    def run_client(code):
        return sh("docker", "run", "--rm", "--name", "dshclient", "--network", "dshhttps", "-v", f"{T}:/data", "localspark-lakehouse-notebook", "sh", "-c",
                  "pip install -q delta-sharing 2>&1 | grep -i '^error'; SSL_CERT_FILE=/data/certs/fullchain.pem REQUESTS_CA_BUNDLE=/data/certs/fullchain.pem CURL_CA_BUNDLE=/data/certs/fullchain.pem python -c \"$0\"", code)
    os.chmod(T, 0o777)
    r = run_client(client); line = next((l for l in r.stdout.splitlines() if l.startswith("RESULT ")), None); res = json.loads(line[7:]) if line else {}
    check("the client got answers through Traefik", bool(res), (r.stdout + r.stderr)[-800:])
    check("https reaches the studio", res.get("ui") == 200, res.get("ui"))
    check("http on port 80 redirects to https", isinstance(res.get("http_redirect"), list) and res["http_redirect"][0] in (301, 302, 307, 308) and res["http_redirect"][1].startswith("https://dsh.test"), res.get("http_redirect"))
    check("Parquet format over https: all rows", res.get("parquet") == [1, 2, 3], res.get("parquet"))
    check("change feed over https", isinstance(res.get("changes"), list) and "insert" in res["changes"] and "update_postimage" in res["changes"], res.get("changes"))
    check("(known client limit) the kernel's Delta format parses our lines and fails only at fetching a non-cloud-storage URL", res.get("delta") == [1, 2, 3] or "Object at location /delta-sharing/files/" in str(res.get("delta")), res.get("delta"))
    check("a recipient with no IP rule is served", res.get("locked_open") == 200, res.get("locked_open"))
    ip = open(f"{T}/ip.txt").read().strip().split()[0]
    lock = lambda cidrs: sh("docker", "exec", "-w", "/workspace", "datakilnworks-studio", "python", "-c", f"import sys;sys.path.insert(0,'/workspace');from web import delta_sharing as ds;ds.set_recipient_ips({profs['locked_id']!r}, {cidrs!r}, 'admin')")
    probe = 'import json,requests;p=json.load(open("/data/locked.share"));print("CODE", requests.get(p["endpoint"]+"/shares",headers={"Authorization":"Bearer "+p["bearerToken"]}).status_code)'
    lock([f"{ip}/32"]); r = run_client(probe); check("with the client's own address allowed, the studio sees the REAL client behind Traefik (X-Forwarded-For from the trusted subnet)", "CODE 200" in r.stdout, r.stdout[-300:] + r.stderr[-300:])
    lock(["203.0.113.0/24"]); r = run_client(probe); check("with another network allowed, the same client is refused (403)", "CODE 403" in r.stdout, r.stdout[-300:])
finally:
    if FAIL: print(sh("docker", "logs", "--tail", "15", "dshtraefik").stdout[-800:]); print(sh("docker", "logs", "--tail", "15", "datakilnworks-studio").stderr[-800:])
    cleanup(); shutil.rmtree(T, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
