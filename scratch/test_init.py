#!/usr/bin/env python3
"""`python -m web.init` and the /healthz, /readyz probes against throwaway directories:
  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_init.py"""
import http.server, json, os, shutil, sqlite3, subprocess, sys, tempfile, threading, time
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
def fresh():
    t = tempfile.mkdtemp(prefix="init_")
    return t, {"WAREHOUSE_DIR": f"{t}/warehouse", "NOTEBOOKS_DIR": f"{t}/notebooks", "DBT_PROJECT_DIR": f"{t}/dbt", "INIT_ADMIN_USERNAME": "admin", "INIT_ADMIN_PASSWORD_HASH": HASH}
def run(env, *args, timeout=180):
    e = {k: v for k, v in os.environ.items() if not k.startswith(("INIT_", "WAREHOUSE", "NOTEBOOKS", "DBT_"))}; e.update(env); e["PYTHONPATH"] = "/workspace"
    r = subprocess.run([sys.executable, "-m", "web.init", *args], cwd="/workspace", env=e, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr
def js(env, *args):
    code, out, err = run(env, "--json", *args)
    try: return code, json.loads(out)
    except Exception: return code, {"steps": [], "raw": out + err}
def steps(d): return {s["step"]: s for s in d["steps"]}

print("a fresh deployment")
t, env = fresh()
code, d = js(env)
check("init succeeds on empty directories", code == 0 and d["ok"] is True, d)
S = steps(d)
check("the bootstrap administrator is created from the environment", S["accounts"]["message"].startswith("users database ready (1 account"), S.get("accounts"))
check("every database is prepared", all(S[k]["status"] == "ok" for k in S if k.startswith("database:")) and len([k for k in S if k.startswith("database:")]) >= 15, [k for k in S if k.startswith("database:")])
db = sqlite3.connect(f"{env['WAREHOUSE_DIR']}/.metadata/auth.db"); tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
check("the tables really exist (users, groups, MFA policy, IP allowlist, SCIM)", {"users", "user_groups", "mfa_policy", "ip_allowlist", "scim_config"} <= tables, sorted(tables)[:30])
check("the dbt project is seeded from the template", os.path.exists(f"{env['DBT_PROJECT_DIR']}/dbt_project.yml") and "seeded" in S["dbt project"]["message"])
check("the warehouse, notebook and project directories exist", all(os.path.isdir(env[k]) for k in ("WAREHOUSE_DIR", "NOTEBOOKS_DIR", "DBT_PROJECT_DIR")))
print("idempotent")
before = open(f"{env['DBT_PROJECT_DIR']}/dbt_project.yml").read(); open(f"{env['DBT_PROJECT_DIR']}/dbt_project.yml", "a").write("\n# my own change\n"); mine = open(f"{env['DBT_PROJECT_DIR']}/dbt_project.yml").read()
code, d2 = js(env); S2 = steps(d2)
check("a second run succeeds, changes nothing and does not touch the project", code == 0 and open(f"{env['DBT_PROJECT_DIR']}/dbt_project.yml").read() == mine and "left untouched" in S2["dbt project"]["message"])
check("...and the administrator is not created twice", S2["accounts"]["message"].startswith("users database ready (1 account"))
db.close()
print("concurrent inits (two pods)")
t2, env2 = fresh()
procs = [subprocess.Popen([sys.executable, "-m", "web.init", "--json"], cwd="/workspace", env={**os.environ, **env2, "PYTHONPATH": "/workspace"}, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(3)]
outs = [p.communicate(timeout=240) for p in procs]
check("three inits at once all succeed (the warehouse lock serialises them)", all(p.returncode == 0 for p in procs), [o[1][-200:] for o in outs])
check("...with exactly one administrator", sqlite3.connect(f"{env2['WAREHOUSE_DIR']}/.metadata/auth.db").execute("SELECT COUNT(*) FROM users").fetchone()[0] == 1)

print("a bad bootstrap administrator")
for label, patch, frag in (("no hash", {"INIT_ADMIN_PASSWORD_HASH": ""}, "INIT_ADMIN"), ("a plain password instead of a hash", {"INIT_ADMIN_PASSWORD_HASH": "hunter2"}, "INIT_ADMIN"), ("an unusable username", {"INIT_ADMIN_USERNAME": "A B!"}, "INIT_ADMIN")):
    t3, env3 = fresh(); env3.update(patch); code, d3 = js(env3)
    check(f"{label}: init fails with the reason (not a crash loop of the studio)", code == 1 and d3["ok"] is False and any(s["status"] == "FAIL" and frag in s["message"] for s in d3["steps"]), d3.get("steps"))
check("...and the plain-text form of the plain output says so too", run({**fresh()[1], "INIT_ADMIN_PASSWORD_HASH": "x"})[1].strip().endswith("the studio must not start."))
t4, env4 = fresh(); js(env4); env4["INIT_ADMIN_PASSWORD_HASH"] = ""; code, d4 = js(env4)
check("an already-initialised warehouse does not need the bootstrap variables any more", code == 0, d4.get("steps"))

print("permissions and waiting")
if os.getuid() == 0:
    t5, env5 = fresh(); os.makedirs(env5["WAREHOUSE_DIR"]); os.chmod(env5["WAREHOUSE_DIR"], 0o555)
    # root ignores permission bits; use a path that cannot be created instead
    env5["WAREHOUSE_DIR"] = "/proc/nope/warehouse"
    code, d5 = js(env5); check("a directory that cannot be written fails with a hint about the owner / fsGroup", code == 1 and any("fsGroup" in s["message"] for s in d5["steps"]), d5.get("steps"))
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self): self.send_response(200 if self.path == "/ok" else 404); self.end_headers()
srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H); threading.Thread(target=srv.serve_forever, daemon=True).start(); port = srv.server_address[1]
t6, env6 = fresh(); code, d6 = js(env6, "--wait-for", f"http://127.0.0.1:{port}/ok,http://127.0.0.1:{port}/missing", "--wait-timeout", "5")
check("--wait-for succeeds for a service that answers (any status below 500)", code == 0 and sum(1 for s in d6["steps"] if s["step"].startswith("wait:") and s["status"] == "ok") == 2, d6["steps"][-3:])
t7, env7 = fresh(); t0 = time.time(); code, d7 = js(env7, "--wait-for", "http://127.0.0.1:1/x", "--wait-timeout", "4")
check("a service that never answers fails the init after the timeout", code == 1 and 3 < time.time() - t0 < 60 and any(s["status"] == "FAIL" and s["step"].startswith("wait:") for s in d7["steps"]), d7["steps"][-2:])
t8, env8 = fresh(); env8["INIT_WAIT_FOR"] = f"http://127.0.0.1:{port}/ok"; code, d8 = js(env8); check("INIT_WAIT_FOR from the environment works too", code == 0 and any(s["step"].startswith("wait:") for s in d8["steps"]))
t9, env9 = fresh(); code, d9 = js(env9, "--check-only"); check("--check-only creates no database and changes no account", code == 0 and not os.path.exists(f"{env9['WAREHOUSE_DIR']}/.metadata/auth.db") and not any(s["step"].startswith("database:") for s in d9["steps"]), d9["steps"])

print("health probes")
t10, env10 = fresh()
for k, v in env10.items(): os.environ[k] = v
sys.path.insert(0, "/workspace")
from fastapi.testclient import TestClient
from web import app as app_module, init as init_mod
c = TestClient(app_module.app)
check("/healthz answers ok without any login", c.get("/healthz").status_code == 200 and c.get("/healthz").json() == {"status": "ok"})
check("/readyz answers ready once the databases exist", c.get("/readyz").status_code == 200 and c.get("/readyz").json() == {"status": "ready"}, c.get("/readyz").text)
real = shutil.rmtree
os.environ["WAREHOUSE_DIR"] = "/nonexistent-dir"; app_module.WAREHOUSE_DIR = "/nonexistent-dir"
check("/readyz says 503 (and nothing more) when the warehouse is missing", c.get("/readyz").status_code == 503 and c.get("/readyz").json() == {"status": "not ready"})
app_module.WAREHOUSE_DIR = env10["WAREHOUSE_DIR"]; os.environ["WAREHOUSE_DIR"] = env10["WAREHOUSE_DIR"]
from web import ip_allowlist as ipa
adm = TestClient(app_module.app, client=("203.0.113.5", 1))
with __import__("web.auth", fromlist=["x"]).get_db_connection() as cc: cc.execute("UPDATE users SET must_change_password = 0")
assert adm.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"}).status_code == 200
assert adm.put("/api/ip-allowlist", json={"mode": "enforce", "rules": [{"cidr": "203.0.113.0/24"}], "trusted_proxies": []}).status_code == 200
kubelet = TestClient(app_module.app, client=("10.9.9.9", 1))
check("with the IP allowlist enforcing, the probes still answer (a kubelet is not on the list) but nothing else does", kubelet.get("/healthz").status_code == 200 and kubelet.get("/readyz").status_code in (200, 503) and kubelet.get("/api/auth/me").status_code == 403 and kubelet.get("/").status_code == 403)
adm.put("/api/ip-allowlist", json={"mode": "off", "rules": [], "trusted_proxies": []})
for t_ in (t, t2, t3, t4, t6, t7, t8, t9, t10): shutil.rmtree(t_, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
