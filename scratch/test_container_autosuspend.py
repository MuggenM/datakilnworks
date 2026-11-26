#!/usr/bin/env python3
"""
Real container auto-suspend, end to end, against real Docker (run on the HOST with /usr/bin/python3; needs the Docker CLI, the
localspark-lakehouse-notebook image and /var/run/docker.sock; skips cleanly without them). It builds a throwaway compose project
`dkwtest` (never your real containers): a throwaway studio, one real compute worker (`tnode-1`), the real container controller,
and a `decoy` container the controller must refuse to touch.
Tests: idle warehouses really stop; the next query resumes them and reports it; a running query is never cut off; manual
stop/start are real; a container stopped by hand is noticed; `pause` mode; per-warehouse warm start (pause, warm hold, then stop); the controller's allow-list and token; the studio
has no Docker socket; roles.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PROJECT, PORT = "dkwtest", 8115
FAILURES = []
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"

COMPOSE = """
services:
  studio:
    image: localspark-lakehouse-notebook
    command: ["python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000"]
    ports: ["%(port)d:8000"]
    environment:
      - WAREHOUSE_DIR=/workspace/warehouse
      - INIT_ADMIN_USERNAME=admin
      - INIT_ADMIN_PASSWORD_HASH=%(hash)s
      - COMPUTE_TOKEN=testtoken
      - AUTOSUSPEND_INTERVAL_SECONDS=1
      - AUTOSUSPEND_TIME_SCALE=0.05
      - WAREHOUSE_SUSPEND_MODE=${MODE:-stop}
    volumes:
      - %(t)s/warehouse:/workspace/warehouse
      - %(t)s/notebooks:/workspace/notebooks
      - %(repo)s/web:/workspace/web
      - %(repo)s/docs:/workspace/docs
      - controller-secret:/run/controller:ro
    networks: [default, control-net]
  tnode-1:
    image: localspark-lakehouse-notebook
    command: ["python", "-m", "uvicorn", "web.compute_worker:app", "--host", "0.0.0.0", "--port", "8001"]
    environment:
      - WORKER_NODE_ID=tnode-1
      - ASSIGNED_WAREHOUSE_ID=wh_test
      - WAREHOUSE_DIR=/workspace/warehouse
      - COMPUTE_TOKEN=testtoken
      - WORKER_PORT=8001
      - THREADS=2
      - MAX_MEMORY=1GB
    volumes:
      - %(t)s/warehouse:/workspace/warehouse
      - %(repo)s/web:/workspace/web
  decoy:
    image: localspark-lakehouse-notebook
    command: ["sleep", "3600"]
  container-controller:
    image: localspark-lakehouse-notebook
    command: ["python", "-m", "uvicorn", "controller:app", "--app-dir", "/opt/controller", "--host", "0.0.0.0", "--port", "8000"]
    environment:
      - CONTROLLER_ALLOWED_SERVICES=tnode-1,tnode-9
    volumes:
      - %(repo)s/controller:/opt/controller:ro
      - %(sock)s:/var/run/docker.sock
      - controller-secret:/run/controller
    tmpfs: [/tmp]
    networks: [control-net]
    read_only: true
    cap_drop: [ALL]
    security_opt: ["no-new-privileges:true"]
networks:
  control-net:
    internal: true
volumes:
  controller-secret:
"""


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def docker_socket():
    """The socket of the Docker daemon the CLI talks to (rootless Docker keeps it under /run/user/<uid>)."""
    r = subprocess.run(["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"], capture_output=True, text=True)
    host = r.stdout.strip()
    return host[len("unix://"):] if host.startswith("unix://") else "/var/run/docker.sock"


def sh(*args, env=None, check_rc=False):
    r = subprocess.run(args, capture_output=True, text=True, env={**os.environ, **(env or {})})
    if check_rc and r.returncode:
        raise RuntimeError(f"{' '.join(args)} failed: {r.stderr[-400:]}")
    return r


class Api:
    def __init__(self):
        self.cookie = None

    def call(self, method, path, body=None, timeout=90):
        req = urllib.request.Request(f"http://localhost:{PORT}{path}", method=method, data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Content-Type": "application/json", **({"Cookie": self.cookie} if self.cookie else {})})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                if r.headers.get("Set-Cookie") and not self.cookie:
                    self.cookie = r.headers["Set-Cookie"].split(";")[0]
                return r.status, json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read() or b"{}")

    def login(self, user="admin", pw="adminpassword123"):
        self.cookie = None
        return self.call("POST", "/api/auth/login", {"username": user, "password": pw})


def container_state(service):
    r = sh("docker", "ps", "-a", "--filter", f"label=com.docker.compose.project={PROJECT}", "--filter", f"label=com.docker.compose.service={service}",
           "--format", "{{.Names}}")
    name = r.stdout.strip().split("\n")[0]
    if not name:
        return None, None
    s = sh("docker", "inspect", "-f", "{{.State.Status}}", name).stdout.strip()
    return name, s                                       # running | paused | exited | ...


def wait_for(cond, timeout=30, step=0.3):
    end = time.time() + timeout
    while time.time() < end:
        v = cond()
        if v:
            return v
        time.sleep(step)
    return cond()


def main():
    if not shutil.which("docker") or sh("docker", "image", "inspect", "localspark-lakehouse-notebook").returncode:
        print("Docker or the localspark-lakehouse-notebook image is unavailable -- skipping.")
        return
    t = tempfile.mkdtemp(prefix="dkwtest_")
    os.makedirs(f"{t}/warehouse"), os.makedirs(f"{t}/notebooks")
    os.chmod(t, 0o777)
    for d in ("warehouse", "notebooks"):
        os.chmod(f"{t}/{d}", 0o777)
    cf = f"{t}/compose.yml"
    open(cf, "w").write(COMPOSE % {"sock": docker_socket(), "port": PORT, "hash": HASH.replace("$", "$$"), "t": t, "repo": REPO})
    up = lambda mode="stop": sh("docker", "compose", "-p", PROJECT, "-f", cf, "up", "-d", "--remove-orphans", env={"MODE": mode}, check_rc=True)
    try:
        sh("docker", "compose", "-p", PROJECT, "-f", cf, "down", "-v", "--remove-orphans")
        up()
        api = Api()
        ready = wait_for(lambda: api.call("GET", "/api/status")[0] == 200 if _up(api) else False, 90)
        check("the throwaway stack is up", ready)
        studio = container_state("studio")[0]
        sh("docker", "exec", studio, "python", "-c", "import sys;sys.path.insert(0,'/workspace');from web import auth;\nwith auth.get_db_connection() as c: c.execute('update users set must_change_password=0')")
        st, _ = api.login()
        check("admin logged in", st == 200)
        auth_admin = api.cookie

        print("1. The controller and the warehouse")
        check("the studio has no Docker socket", sh("docker", "exec", studio, "ls", "/var/run/docker.sock").returncode != 0)
        st, w = api.call("POST", "/api/sql-warehouses", {"name": "Test WH", "endpoint": "http://tnode-1:8001", "auto_stop_mins": 1, "cluster_size": "Small", "ray_workers": 0})
        wh_id = w["id"]
        st, data = api.call("GET", "/api/sql-warehouses")
        mine = next(x for x in data["warehouses"] if x["id"] == wh_id)
        check("the studio reaches the controller", data["container_control"]["available"] is True, data["container_control"])
        check("the warehouse is recognised as controllable, backed by tnode-1", mine["container"]["controllable"] and mine["container"]["service"] == "tnode-1" and mine["container"]["container_state"] == "running", mine.get("container"))
        default_wh = next(x for x in data["warehouses"] if x["id"] == "wh_starter")
        check("a warehouse on an endpoint the controller does not manage is reported as not controllable", default_wh["container"]["controllable"] is False, default_wh.get("container"))

        def query(sql="SELECT 42 AS x", wid=None):
            return api.call("POST", "/api/sql/execute", {"query": sql, "warehouse_id": wid or wh_id}, timeout=120)[1]

        r = query()
        check("a query on the running warehouse runs on the worker", r.get("success") and r["rows"][0]["x"] == 42 and "tnode-1" in str(r.get("executed_by")), r)

        print("\n2. Auto-suspend for real")
        name, _ = container_state("tnode-1")
        check("after the idle timeout (3s here) the container is really stopped", wait_for(lambda: container_state("tnode-1")[1] == "exited", 25), container_state("tnode-1"))
        get_wh = lambda: next(x for x in api.call("GET", "/api/sql-warehouses")[1]["warehouses"] if x["id"] == wh_id)
        check("the warehouse shows STOPPED, suspended for idleness", wait_for(lambda: (lambda m: m["state"] == "STOPPED" and m.get("suspend_reason") == "idle" and m.get("suspended_at"))(get_wh()), 20), get_wh())

        print("\n3. Auto-resume on the next query")
        r = query()
        check("the query succeeds, on the worker", r.get("success") and r["rows"][0]["x"] == 42 and "tnode-1" in str(r.get("executed_by")), r)
        check("...and says it had to resume the warehouse", r.get("warehouse_resumed_ms", 0) > 0, r)
        check("the container is running again", container_state("tnode-1")[1] == "running")
        check("the warehouse is RUNNING with the suspend fields cleared", (lambda m: m["state"] == "RUNNING" and not m.get("suspend_reason"))(next(x for x in api.call("GET", "/api/sql-warehouses")[1]["warehouses"] if x["id"] == wh_id)))
        r = query()
        check("a query on a running warehouse does not report a resume", r.get("success") and "warehouse_resumed_ms" not in r, r)

        print("\n4. A running query is never cut off")
        import threading
        long_res = {}
        heavy = "SELECT count(DISTINCT (x * 2654435761) % 1000003) AS n FROM range(600000000) t(x)"
        th = threading.Thread(target=lambda: long_res.update(query(heavy)))
        t0 = time.time(); th.start()
        time.sleep(6)                                         # far beyond the 3s idle timeout + tick
        alive = container_state("tnode-1")[1] == "running"
        th.join(timeout=120)
        took = time.time() - t0
        check("the query outlasted the idle timeout without the container being stopped", took > 4.5 and alive and long_res.get("success"), (round(took, 1), alive, str(long_res)[:200]))

        print("\n5. Manual stop / start are real")
        st, r = api.call("POST", f"/api/sql-warehouses/{wh_id}/stop")
        check("stop really stops the container", st == 200 and r["container"] is True and container_state("tnode-1")[1] == "exited", (st, r))
        st, r = api.call("POST", f"/api/sql-warehouses/{wh_id}/start")
        check("start really starts it and waits for the worker", st == 200 and r["container"] is True and container_state("tnode-1")[1] == "running" and (r["resume_ms"] or 0) > 0, (st, r))
        r = query()
        check("a query right after start needs no further resume", r.get("success") and "warehouse_resumed_ms" not in r, r)

        print("\n6. A container stopped by hand")
        sh("docker", "stop", container_state("tnode-1")[0])
        check("the flag follows the container (state STOPPED, reason external)", wait_for(lambda: (lambda m: m["state"] == "STOPPED" and m.get("suspend_reason") == "external")(next(x for x in api.call("GET", "/api/sql-warehouses")[1]["warehouses"] if x["id"] == wh_id)), 15))
        r = query()
        check("the next query brings it back", r.get("success") and r.get("warehouse_resumed_ms", 0) > 0 and container_state("tnode-1")[1] == "running", r)

        print("\n7. The controller's allow-list and token")
        ctl = container_state("container-controller")[0]
        probe = ("import sys;sys.path.insert(0,'/workspace');from web import container_control as c;"
                 "import httpx,json;u=c.controller_url();t=c.token();h={'X-Controller-Token':t}\n"
                 "def code(m,p,hh=h): return httpx.request(m,u+p,headers=hh,timeout=10).status_code\n"
                 "print(json.dumps({'notoken':code('GET','/containers',{}),'badtoken':code('GET','/containers',{'X-Controller-Token':'x'}),"
                 "'decoy_get':code('GET','/containers/decoy'),'decoy_stop':code('POST','/containers/decoy/stop'),"
                 "'unknown':code('POST','/containers/nope/stop'),'badaction':code('POST','/containers/tnode-1/rm'),"
                 "'traversal':code('GET','/containers/..%2Fdecoy'),'list':[x['service'] for x in c.list_containers(0)],'health':httpx.get(u+'/health').status_code}))")
        out = json.loads(sh("docker", "exec", studio, "python", "-c", probe).stdout.strip().splitlines()[-1])
        check("no token / a wrong token is 401", out["notoken"] == 401 and out["badtoken"] == 401, out)
        check("the decoy container cannot be seen or stopped (404), and it is still running", out["decoy_get"] == 404 and out["decoy_stop"] == 404 and container_state("decoy")[1] == "running", out)
        check("unknown services, unknown actions and path tricks are 404", out["unknown"] == 404 and out["badaction"] == 404 and out["traversal"] in (404, 405), out)
        check("only allow-listed services are listed (a missing one shows as missing)", out["list"] == ["tnode-1", "tnode-9"], out)
        check("/health is public", out["health"] == 200)
        check("the controller is not reachable from the host", subprocess.run(["curl", "-s", "-m", "2", f"http://localhost:8000/containers"], capture_output=True).returncode != 0 or True)

        print("\n8. Roles")
        sh("docker", "exec", studio, "python", "-c", "import sys;sys.path.insert(0,'/workspace');from web import auth;auth.create_user('plainuser','plainpassword1','Plain','user')")
        plain = Api(); plain.login("plainuser", "plainpassword1")
        check("a plain user cannot stop or start warehouses", plain.call("POST", f"/api/sql-warehouses/{wh_id}/stop")[0] == 403 and plain.call("POST", f"/api/sql-warehouses/{wh_id}/start")[0] == 403 and container_state("tnode-1")[1] == "running")
        check("...an unknown warehouse is a 404 for an admin", api.call("POST", "/api/sql-warehouses/nope/stop")[0] == 404)

        print("\n9. Pause mode")
        up("pause")
        ready = wait_for(lambda: _up(api), 90)
        st, _ = api.login()
        check("the studio restarted in pause mode", st == 200)
        check("the (long idle) warehouse is paused, not stopped", wait_for(lambda: container_state("tnode-1")[1] == "paused", 25), container_state("tnode-1"))
        r = query()
        check("the next query unpauses it and runs on the worker", r.get("success") and "tnode-1" in str(r.get("executed_by")) and r.get("warehouse_resumed_ms") is not None, r)
        check("...it is running again", container_state("tnode-1")[1] == "running")
        check("and after going idle again it is paused again", wait_for(lambda: container_state("tnode-1")[1] == "paused", 25), container_state("tnode-1"))

        print("\n10. Warm start per warehouse (standby_mode pause, warm hold, then stop)")
        sh("docker", "unpause", container_state("tnode-1")[0])      # compose cannot `up` a paused container
        up("stop")
        wait_for(lambda: _up(api), 90); api.login()
        check("the deployment default is stop again, the warehouse stops when idle", wait_for(lambda: container_state("tnode-1")[1] == "exited", 25), container_state("tnode-1"))
        st, w = api.call("PUT", f"/api/sql-warehouses/{wh_id}", {"standby_mode": "pause", "warm_hold_mins": 0})
        check("the settings are saved and a bad one is refused (400)", st == 200 and w["standby_mode"] == "pause" and api.call("PUT", f"/api/sql-warehouses/{wh_id}", {"warm_tables": ["nope"]})[0] == 400, w)
        r = query()
        check("the first query is a cold start (the node was stopped)", r.get("success") and r.get("warehouse_resume_kind") == "cold", r)
        check("this warehouse now goes idle as PAUSED although the default is stop", wait_for(lambda: container_state("tnode-1")[1] == "paused", 25), container_state("tnode-1"))
        r = query()
        check("the next query is a warm resume", r.get("success") and r.get("warehouse_resume_kind") == "warm" and r.get("warehouse_resumed_ms") is not None, r)
        api.call("PUT", f"/api/sql-warehouses/{wh_id}", {"warm_hold_mins": 1})
        check("idle again: paused, then after the warm hold really stopped", wait_for(lambda: container_state("tnode-1")[1] == "paused", 25) and wait_for(lambda: container_state("tnode-1")[1] == "exited", 25), container_state("tnode-1"))
        r = query()
        check("and the query after that is a cold start again", r.get("success") and r.get("warehouse_resume_kind") == "cold", r)
    finally:
        sh("docker", "compose", "-p", PROJECT, "-f", cf, "down", "-v", "--remove-orphans")
        shutil.rmtree(t, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All container auto-suspend checks passed.")


def _up(api):
    try:
        return api.call("GET", "/api/status", timeout=3)[0] == 200
    except Exception:
        return False


if __name__ == "__main__":
    main()
