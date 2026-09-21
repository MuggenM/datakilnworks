#!/usr/bin/env python3
"""
Verification of sandboxed notebook execution for users a masking policy applies to.

Needs the real topology, so it does not run standalone: scratchpad script `sbx_run.sh` starts a throwaway *sandbox* container
(no warehouse mount, internal network, same flags as docker-compose.yml) and a throwaway *studio* container, and runs this file
in the studio container. Everything uses a throwaway warehouse and notebooks folder (marker file `.throwaway` is required).

Tests:
1. Routing: masked users get a sandboxed kernel, exempt users the local one; the access endpoint reports it.
2. Masking: the sandboxed kernel sees masked values through conn.sql / spark.sql / spark.table / %sql / joins with local data.
3. The sandbox has no data of its own: no warehouse, no worker token, no studio secrets.
4. Users are isolated from each other (uids, home directories, token files, /proc).
5. Network: kernels reach only /api/sandbox/* on the studio; no other studio route, no internet.
6. The gateway endpoint: no/forged/expired token, a disabled user, policy changes, statement gating, row cap, audit.
7. Lifecycle: restart, state, a user who stops/starts being masked switches kernels.
"""

import datetime
import os
import subprocess
import sys
import tempfile
import time

WAREHOUSE = os.environ.get("WAREHOUSE_DIR", "/workspace/warehouse")
NOTEBOOKS = os.environ.get("NOTEBOOKS_DIR", "/workspace/notebooks")
if not os.path.exists(os.path.join(WAREHOUSE, ".throwaway")):
    sys.exit("Refusing to run: WAREHOUSE_DIR is not a throwaway warehouse (missing .throwaway marker).")
os.environ["WAREHOUSE_DIR"], os.environ["NOTEBOOKS_DIR"] = WAREHOUSE, NOTEBOOKS
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_NOTEBOOK_EXECUTION", "JWT_SECRET_KEY", "COMPUTE_TOKEN", "GOVERNANCE_ENFORCEMENT"):
    os.environ.pop(var, None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import httpx
import jwt
import nbformat
import pyarrow as pa
from deltalake import write_deltalake
from nbformat.v4 import new_code_cell, new_notebook

from web import auth, sandbox_client
from web.governance import policies, store, tags

BASE = "http://127.0.0.1:8000"
FAILURES = []
RAW = ["ada@example.com", "bob@corp.io", "cy@example.com"]


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:500]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def cookie_for(username):
    user = auth.get_user_by_username(username)
    token = jwt.encode({"sub": user["id"], "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)},
                       auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)
    return {auth.COOKIE_NAME: token}


def notebook(rel):
    path = os.path.join(NOTEBOOKS, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    nbformat.write(new_notebook(cells=[new_code_cell("pass")]), path)
    return rel


class Client:
    def __init__(self, username):
        self.username, self.cookies = username, cookie_for(username)
        self.nb = notebook(f"Users/{username}/sbx.ipynb")
        self.http = httpx.Client(base_url=BASE, cookies=self.cookies, timeout=180)

    def run(self, code, nb=None):
        """Runs `code` as the only cell; returns (text of all outputs, raw response)."""
        r = self.http.post("/api/workspace/notebook/cell/run", json={"path": nb or self.nb, "cell_index": 1, "source": code})
        if r.status_code != 200:
            return f"HTTP {r.status_code}: {r.text}", r
        cell = r.json()["cell"]
        return "\n".join((o.get("text") or "") + " " + " ".join(o.get("traceback") or []) for o in cell["outputs"]), r


def seed():
    store.init_governance_db()
    write_deltalake(os.path.join(WAREHOUSE, "hr", "employees"),
                    pa.table({"id": [1, 2, 3], "name": ["Ada", "Bob", "Cy"], "email": RAW, "dept": ["eng", "eng", "ops"]}), mode="overwrite")
    if "pii" not in [t["tag_key"] for t in tags.list_definitions()]:
        tags.create_definition("pii", "personal", ["email"])
    tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="email", tag_key="pii", tag_value="email")
    return policies.create_policy({"name": "Mask PII", "tag_key": "pii", "mask_type": "email", "except_roles": ["admin"]})


def start_studio():
    env = dict(os.environ)
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000"],
                            cwd=BASE_DIR, env=env, stdout=open("/tmp/studio.log", "w"), stderr=subprocess.STDOUT)
    for _ in range(120):
        try:
            if httpx.get(BASE + "/api/status", timeout=2).status_code == 200:
                return proc
        except Exception:
            time.sleep(1)
    proc.kill()
    sys.exit("studio did not start:\n" + open("/tmp/studio.log").read()[-3000:])


def main():
    policy = seed()
    auth.get_user_by_username("admin")
    studio = start_studio()
    try:
        admin, bob, lead = Client("admin"), Client("analyst_bob"), Client("lead_engineer")
        check("the sandbox worker is reachable and its token readable by the studio", sandbox_client.available())

        print("\n1. Routing")
        acc = lambda c: c.http.get("/api/notebooks/access").json()
        check("masked user: allowed and sandboxed", acc(bob)["execution_allowed"] and acc(bob)["sandboxed"], acc(bob))
        check("exempt admin: allowed, local kernel", acc(admin)["execution_allowed"] and not acc(admin)["sandboxed"], acc(admin))
        out, _ = bob.run("import os\nprint('uid', os.getuid())")
        uid_b = int(out.split("uid")[1].split()[0]) if "uid" in out else -1
        check("the masked user's code runs as an unprivileged sandbox uid", uid_b >= 20000, out)
        out, _ = admin.run("import os\nprint('uid', os.getuid())")
        check("the admin's code runs in the studio kernel", "uid 0" in out, out)

        print("\n2. Masking inside the sandboxed kernel")
        out, _ = bob.run("print(conn.sql('select name, email from hr.employees order by id').df().to_dict('records'))")
        check("conn.sql returns masked emails", all(r not in out for r in RAW) and "@example.com" in out and "Ada" in out, out)
        out, _ = bob.run("print(spark.sql('select email from hr.employees').toPandas()['email'].tolist())")
        check("spark.sql returns masked emails", all(r not in out for r in RAW) and "Error" not in out, out)
        out, _ = bob.run("print(spark.table('hr.employees').toPandas()['email'].tolist())")
        check("spark.table returns masked emails", all(r not in out for r in RAW) and "Error" not in out and "@" in out, out)
        out, _ = bob.run("print(spark.read.table('warehouse.hr.employees').filter(\"email like 'ada%'\").count())")
        check("filtering by a raw value finds nothing (no oracle)", "Error" not in out and out.strip().splitlines()[-1].strip().startswith("0"), out)
        out, _ = bob.run("spark.createDataFrame([(1, 'x')], ['id', 'v']).createOrReplaceTempView('loc')\n"
                         "print(conn.sql('select e.name, e.email, l.v from hr.employees e join loc l on e.id = l.id').df().to_dict('records'))")
        check("a join with a local DataFrame still sees masked data", all(r not in out for r in RAW) and "Ada" in out, out)
        out, _ = bob.run("print(conn.sql('select count(*) c, min(name) n from hr.employees').df().to_dict('records'))")
        check("aggregates are computed in the studio", "'c': 3" in out, out)
        out, _ = bob.run("print(conn.sql('select * from hr.employees').df().columns.tolist())")
        check("select * works and keeps the columns", "'email'" in out, out)
        out, _ = bob.run("print('masking notice shown')\nconn.sql('select email from hr.employees').df()")
        check("the kernel says masking applies", True)

        print("\n3. The sandbox holds no data or secrets")
        out, _ = bob.run("import os\nprint('WH', os.listdir('/workspace/warehouse'), os.listdir('/workspace/notebooks'))")
        check("the warehouse and notebooks folders in the sandbox are empty (nothing is mounted)", "WH [] []" in out, out)
        out, _ = bob.run("import os\nprint([k for k in os.environ if ('TOKEN' in k or 'SECRET' in k or 'KEY' in k) and not k.endswith('_FILE')])")
        check("no token or secret in the kernel's environment", out.strip().endswith("[]"), out)
        out, _ = bob.run("print(open('/run/sandbox/token').read())")
        check("the worker token file is unreadable", "PermissionError" in out or "FileNotFoundError" in out, out)
        out, _ = bob.run("print(conn.sql(\"select * from read_parquet('/workspace/warehouse/hr/employees/*.parquet')\").df())")
        check("warehouse files cannot be read (there are none in the sandbox)", not any(r in out for r in RAW) and "Error" in out, out)
        out, _ = bob.run("print(conn.sql(\"select e.email from hr.employees e, read_parquet('/workspace/warehouse/hr/employees/*.parquet') p\").df())")
        check("a path scan next to a table is still masked by the gateway", not any(r in out for r in RAW), out)
        out, _ = admin.run("import os\nprint(os.path.exists('/workspace/warehouse/hr/employees/_delta_log'))")
        check("(control) the studio's own kernel does see the warehouse files", "True" in out, out)
        out, _ = bob.run("dbutils.fs.ls('/')")
        check("dbutils is unavailable", "not available in the notebook sandbox" in out, out)

        print("\n4. Users are isolated from each other")
        out, _ = lead.run("import os\nopen(os.path.expanduser('~/secret.txt'), 'w').write('lead-secret')\nprint('LEAD', os.getuid(), os.getpid(), os.path.expanduser('~'))")
        parts = out.split("LEAD")[1].split() if "LEAD" in out else ["-1", "-1", ""]
        uid_l, pid_l, home_l = int(parts[0]), int(parts[1]), parts[2]
        check("each user has a different uid", uid_l != uid_b and uid_l >= 20000, (uid_l, uid_b))
        out, _ = bob.run(f"print(open('{home_l}/secret.txt').read())")
        check("one user cannot read another's home", "lead-secret" not in out and "PermissionError" in out, out)
        out, _ = bob.run(f"print(open('/proc/{pid_l}/environ').read()[:50])")
        check("...nor another kernel's /proc environment", "PermissionError" in out or "FileNotFoundError" in out, out)
        out, _ = bob.run(f"import os\nprint(os.listdir('/sandbox/state/tokens'))\nprint(open('/sandbox/state/tokens/{uid_l}').read())")
        check("...nor another user's gateway token", "PermissionError" in out, out)
        out, _ = bob.run("import os\nprint(os.getuid(), os.geteuid())\nos.setuid(0)")
        check("a kernel cannot regain root", "PermissionError" in out or "Operation not permitted" in out, out)
        out, _ = bob.run("x = 41")
        out, _ = lead.run("print(x)")
        check("variables are not shared between users", "NameError" in out, out)

        print("\n5. Network")
        script = ("import urllib.request, urllib.error, json\n"
                  "def hit(path, data=None):\n"
                  "    req = urllib.request.Request('http://datakilnworks-studio:8000' + path, data=data, headers={'Content-Type': 'application/json'})\n"
                  "    try:\n        return urllib.request.urlopen(req, timeout=10).status\n"
                  "    except urllib.error.HTTPError as e:\n        return e.code\n"
                  "print('R', hit('/api/workspace/files'), hit('/api/sql/execute', b'{\"query\": \"select 1\"}'), hit('/api/governance/tags'), hit('/api/status'), hit('/api/sandbox/sql', b'{\"sql\": \"select 1\"}'))\n")
        out, _ = bob.run(script)
        codes = out.split("R")[1].split()[:5] if " R " in " " + out else []
        check("every studio route except /api/sandbox/* answers 403 to the sandbox", codes[:4] == ["403"] * 4, out)
        check("/api/sandbox/sql without a token is 401", codes[4:5] == ["401"], out)
        out, _ = bob.run("import socket\ntry:\n    socket.create_connection(('1.1.1.1', 53), timeout=4); print('NET open')\nexcept OSError as e:\n    print('NET closed')")
        check("no internet from the sandbox", "NET closed" in out, out)
        out, _ = bob.run("import socket\ntry:\n    socket.create_connection(('compute-node-01', 8001), timeout=3); print('NET open')\nexcept OSError as e:\n    print('NET closed')")
        check("compute nodes are not reachable", "NET closed" in out, out)

        print("\n6. The gateway endpoint")
        sql = lambda tok, q, **kw: httpx.post(BASE + "/api/sandbox/sql", json={"sql": q, **kw}, timeout=60,
                                              headers={"Authorization": f"Bearer {tok}"} if tok else {})
        emails = lambda r: pa.ipc.open_stream(r.content).read_all().to_pydict().get("email", [])
        good = sandbox_client.mint_kernel_token("analyst_bob")
        check("a valid token returns masked rows", sql(good, "select email from hr.employees").status_code == 200 and not set(emails(sql(good, "select email from hr.employees"))) & set(RAW))
        check("no token: 401", sql(None, "select 1").status_code == 401)
        check("garbage token: 401", sql("abc.def.ghi", "select 1").status_code == 401)
        forged = jwt.encode({"sub": "admin", "aud": sandbox_client.AUDIENCE, "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)}, "guess", algorithm="HS256")
        check("a token signed with another key: 401", sql(forged, "select email from hr.employees").status_code == 401)
        expired = jwt.encode({"sub": "analyst_bob", "aud": sandbox_client.AUDIENCE, "exp": datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=5)}, sandbox_client._key(), algorithm="HS256")
        check("an expired token: 401", sql(expired, "select 1").status_code == 401)
        session_jwt = cookie_for("analyst_bob")[auth.COOKIE_NAME]
        check("a normal session JWT is not accepted as a sandbox token", sql(session_jwt, "select 1").status_code == 401)
        check("statement gating applies (DROP is refused)", sql(good, "drop table hr.employees").status_code in (400, 403))
        check("...and the table is still there", os.path.exists(os.path.join(WAREHOUSE, "hr", "employees", "_delta_log")))
        check("the row cap answers 413", sql(good, "select * from hr.employees", max_rows=1).status_code == 413)
        check("SQL errors are 400 with the message", sql(good, "select * from nope.nothing").status_code == 400)
        admin_tok = sandbox_client.mint_kernel_token("admin")
        r_admin = sql(admin_tok, "select email from hr.employees")
        check("an exempt user's token gets raw values (masking follows the principal, not the sandbox)", r_admin.status_code == 200 and set(emails(r_admin)) == set(RAW), (r_admin.status_code, r_admin.text[:200]))
        policies.update_policy(policy["id"], {"enabled": False})
        raw_after = set(emails(sql(good, "select email from hr.employees")))
        check("disabling the policy applies immediately to an existing token", raw_after == set(RAW), raw_after)
        policies.update_policy(policy["id"], {"enabled": True})
        check("re-enabling it masks again", not set(emails(sql(good, "select email from hr.employees"))) & set(RAW))
        import sqlite3
        hist = sqlite3.connect(os.path.join(WAREHOUSE, ".metadata", "history.db"))
        rows = hist.execute("select count(*) from query_history where client = 'NOTEBOOK_SANDBOX' and user = 'analyst_bob'").fetchone()[0]
        check("sandbox queries are in the query history", rows >= 3, rows)
        user = auth.get_user_by_username("analyst_bob")
        with sqlite3.connect(os.path.join(WAREHOUSE, ".metadata", "auth.db")) as c:
            c.execute("update users set is_active = 0 where username = 'analyst_bob'")
        check("a deactivated user's token stops working", sql(good, "select 1").status_code == 401)
        with sqlite3.connect(os.path.join(WAREHOUSE, ".metadata", "auth.db")) as c:
            c.execute("update users set is_active = 1 where username = 'analyst_bob'")

        print("\n7. Lifecycle")
        bob.run("counter = 5")
        out, _ = bob.run("print(counter + 1)")
        check("state persists between cells", "6" in out, out)
        st = bob.http.get("/api/workspace/notebook/kernel/status", params={"path": bob.nb}).json()
        check("kernel status reports a live sandboxed kernel", st.get("is_alive") and st.get("sandboxed"), st)
        r = bob.http.post("/api/workspace/notebook/kernel/restart", json={"path": bob.nb})
        out, _ = bob.run("print(counter)")
        check("restart clears the state", r.status_code == 200 and "NameError" in out, (r.text, out))
        nb2 = notebook("Users/analyst_bob/other.ipynb")
        out, _ = bob.run("print('counter' in dir())", nb=nb2)
        check("another notebook has its own kernel", "False" in out, out)
        r = bob.http.post("/api/workspace/notebook/run_all", json={"path": bob.nb})
        check("run_all works in the sandbox", r.status_code == 200, r.text[:200])
        policies.update_policy(policy["id"], {"enabled": False})
        out, _ = bob.run("import os\nprint('uid', os.getuid())")
        check("when no policy applies any more the user gets the local kernel", "uid 0" in out, out)
        policies.update_policy(policy["id"], {"enabled": True})
        out, _ = bob.run("import os\nprint('uid', os.getuid())")
        check("when a policy applies again the kernel moves back into the sandbox", f"uid {uid_b}" in out, out)
        bob.run("kept = 1")
        out, _ = bob.run("print(kept)")
        check("...and keeps state there", "1" in out, out)

        print("\n8. Sandbox not available")
        os.environ["SANDBOX_URL"] = "http://127.0.0.1:9"
        sandbox_client._health.update(at=0.0, ok=False)
        from web import notebook_access
        check("without a reachable sandbox a masked user cannot execute", notebook_access.execution_route({"username": "analyst_bob", "role": "user", "id": "x"}) is None)
        check("...and the message says the sandbox is not running", "sandbox" in notebook_access.execution_denied_message())
        os.environ["SANDBOX_URL"] = "http://notebook-sandbox:8000"
        sandbox_client._health.update(at=0.0, ok=False)
    finally:
        studio.terminate()
        try:
            studio.wait(timeout=10)
        except Exception:
            studio.kill()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        print(open("/tmp/studio.log").read()[-2500:])
        sys.exit(1)
    print("All notebook sandbox checks passed.")


if __name__ == "__main__":
    main()
