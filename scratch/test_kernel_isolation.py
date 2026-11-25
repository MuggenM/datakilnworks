"""
Isolation of the studio's local notebook kernels (web/kernel_isolation.py, web/kernel_launch.py).

Runs as root inside the studio image against a throwaway warehouse (never the real one); needs network access to download
DuckDB's delta extension once (here the compose network; a plain bridge network on this machine has no outbound access):

    docker run --rm --network datakilnworks_default -v "$PWD/web:/workspace/web:ro" -v "$PWD/scratch:/workspace/scratch:ro" \
        localspark-lakehouse-notebook python /workspace/scratch/test_kernel_isolation.py

Checks: kernels run under a per-user uid, cannot read .metadata, the studio's environment, /proc/1/environ or another user's kernel
files; still read and write warehouse tables; notebook query history is attributed to the kernel's owner and a planted symlink
spool is never read; a Papermill run uses the owner's uid; non-root refuses (fail closed).
"""
import json
import os
import shutil
import sys
import tempfile

ROOT = tempfile.mkdtemp(prefix="kiso_")
os.chmod(ROOT, 0o755)                                   # mkdtemp is 0700; the real parents (/workspace, /var/lib) are traversable
WH = os.path.join(ROOT, "warehouse")
os.makedirs(os.path.join(WH, ".metadata"))
os.environ.update({
    "WAREHOUSE_DIR": WH, "NOTEBOOKS_DIR": os.path.join(ROOT, "notebooks"),
    "NOTEBOOK_KERNEL_HOME_ROOT": os.path.join(ROOT, "homes"),
    "GIT_TOKEN": "leaky-git-token", "INIT_ADMIN_PASSWORD_HASH": "leaky-hash",
})
os.makedirs(os.environ["NOTEBOOKS_DIR"])
sys.path.insert(0, "/workspace")

FAILS = []


def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail and not ok else ""))
    if not ok:
        FAILS.append(name)


assert os.geteuid() == 0, "run as root (inside the studio image)"

with open(os.path.join(WH, ".metadata", "jwt_secret"), "w") as f:
    f.write("TOP-SECRET-JWT")
os.chmod(os.path.join(WH, ".metadata", "jwt_secret"), 0o600)
with open(os.path.join(WH, ".metadata", "volumes.json"), "w") as f:     # world-readable file: the directory must protect it
    f.write("{}")
os.chmod(os.path.join(WH, ".metadata", "volumes.json"), 0o666)

import pyarrow as pa
from deltalake import write_deltalake, DeltaTable

os.umask(0o022)                                          # data written before the fix, the old way
write_deltalake(os.path.join(WH, "dbo", "people"), pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}))

from web import kernel_isolation as ki
from web.audit import init_history_db
init_history_db()
res = ki.harden_permissions()
check("permission pass touched the old data", res["changed"] > 0, str(res))
ki.prepare_studio()
ki.sync_duckdb_extensions()
import glob
check("DuckDB's delta extension is in the shared, root-owned directory", bool(glob.glob(os.path.join(ki.EXT_SHARED, "*", "*", "delta.duckdb_extension")))
      and os.stat(ki.EXT_SHARED).st_uid == 0 and (os.stat(ki.EXT_SHARED).st_mode & 0o022) == 0)
check(".metadata is root-only", (os.stat(os.path.join(WH, ".metadata")).st_mode & 0o777) == 0o700)
check("second permission pass changes nothing", ki.harden_permissions()["changed"] == 0)

from web.notebook_runner import LocalKernelSession


def run(sess, code):
    r = sess.execute_code(code, timeout=90)
    text = "".join(o.get("text", "") for o in r.get("outputs", []) if o.get("output_type") == "stream")
    errs = [o for o in r.get("outputs", []) if o.get("output_type") == "error"]
    return text.strip(), errs, r


alice = LocalKernelSession("Users/alice/a.ipynb", "alice")
out, errs, _ = run(alice, "import os; print(os.getuid(), os.getgid(), sorted(os.getgroups()))")
uid_a = ki.uid_for("alice")
check("alice's kernel runs as her own uid", out.split()[0] == str(uid_a) and uid_a >= 20001, out + str(errs))
check("supplementary group is the data group only", out.endswith(f"[{ki.DATA_GID}]"), out)

probe = """
import os, json
r = {}
for name, p in [("jwt", os.environ["WAREHOUSE_DIR"] + "/.metadata/jwt_secret"), ("vol", os.environ["WAREHOUSE_DIR"] + "/.metadata/volumes.json"),
                ("environ1", "/proc/1/environ"), ("root", "/root/.ipython/profile_default/startup/00_databricks_shim.py")]:
    try:
        open(p).read(); r[name] = "READ"
    except Exception as e:
        r[name] = type(e).__name__
try:
    os.listdir(os.environ["WAREHOUSE_DIR"] + "/.metadata"); r["ls"] = "READ"
except Exception as e:
    r["ls"] = type(e).__name__
r["env"] = sorted(os.environ)
print(json.dumps(r))
"""
out, errs, _ = run(alice, probe)
try:
    r = json.loads(out.splitlines()[-1])
except Exception:
    r = {}
check("kernel cannot read the JWT secret", r.get("jwt") == "PermissionError", str(r) + str(errs))
check("kernel cannot read a world-readable file in .metadata", r.get("vol") == "PermissionError", str(r))
check("kernel cannot list .metadata", r.get("ls") == "PermissionError", str(r))
check("kernel cannot read the studio's /proc/1/environ", r.get("environ1") == "PermissionError", str(r))
check("kernel cannot read /root", r.get("root") == "PermissionError", str(r))
check("no studio secret in the kernel's environment", not {"GIT_TOKEN", "INIT_ADMIN_PASSWORD_HASH"} & set(r.get("env", [])), str(r.get("env")))
out, errs, _ = run(alice, "print(open('/proc/self/environ','rb').read().count(b'leaky'))")
check("the kernel's own initial environment is clean (execve)", out == "0", out + str(errs))

out, errs, _ = run(alice, "print(spark.table('dbo.people').count())")
check("kernel reads a table written before the fix (shim + duckrun)", out.endswith("3"), out + str(errs)[:400])
out, errs, _ = run(alice, """
import pyarrow as pa, os
from deltalake import write_deltalake
write_deltalake(os.environ['WAREHOUSE_DIR'] + '/dbo/people', pa.table({'id': [4], 'name': ['d']}), mode='append')
write_deltalake(os.environ['WAREHOUSE_DIR'] + '/dbo/from_alice', pa.table({'x': [1, 2]}))
print('ok')""")
check("kernel appends to an existing table and creates a new one", out == "ok", str(errs)[:400])
check("the studio reads what the kernel wrote", DeltaTable(os.path.join(WH, "dbo", "people")).to_pyarrow_table().num_rows == 4
      and DeltaTable(os.path.join(WH, "dbo", "from_alice")).to_pyarrow_table().num_rows == 2)
os.umask(0o002)                                          # the studio's umask after prepare_studio
write_deltalake(os.path.join(WH, "dbo", "by_studio"), pa.table({"y": [1]}))
out, errs, _ = run(alice, """
import pyarrow as pa, os
from deltalake import write_deltalake
write_deltalake(os.environ['WAREHOUSE_DIR'] + '/dbo/by_studio', pa.table({'y': [2]}), mode='append'); print('ok')""")
check("kernel writes to a table the studio created after the fix", out == "ok", str(errs)[:400])

# history: attributed to the owner, written through the spool
import sqlite3
out, errs, _ = run(alice, "duckrun_conn.sql('SELECT 42 AS answer'); print('q')")     # the shim logs duckrun_conn.sql (also behind %sql)
rows = sqlite3.connect(os.path.join(WH, ".metadata", "history.db")).execute(
    "SELECT user, client, query_text FROM query_history WHERE query_text LIKE '%42%'").fetchall()
check("notebook query lands in the history as alice (not admin)", any(u == "alice" and c == "NOTEBOOK" for u, c, _ in rows), str(rows) + str(errs)[:300])
run(alice, "import os; os.symlink(os.environ['WAREHOUSE_DIR'] + '/.metadata/jwt_secret', os.path.expanduser('~/.dkw_history.jsonl'))")
n = ki.ingest_history("alice")
leak = sqlite3.connect(os.path.join(WH, ".metadata", "history.db")).execute(
    "SELECT count(*) FROM query_history WHERE query_text LIKE '%TOP-SECRET%'").fetchone()[0]
check("a symlinked spool is never read by the studio", n == 0 and leak == 0, f"n={n} leak={leak}")
with open(os.path.join(WH, ".metadata", "jwt_secret")) as f:
    check("the secret file is unchanged", f.read() == "TOP-SECRET-JWT")

# a second user
bob = LocalKernelSession("Users/bob/b.ipynb", "bob")
uid_b = ki.uid_for("bob")
out, errs, _ = run(bob, f"""
import os
try:
    open('{ki.home_for(uid_a)}/.kernel.json').read(); print('READ')
except Exception as e:
    print(type(e).__name__)
print(os.getuid())""")
check("bob runs under another uid", out.splitlines()[-1] == str(uid_b) and uid_b != uid_a, out)
check("bob cannot read alice's kernel connection file", out.splitlines()[0] == "PermissionError", out)
check("uids are stable", ki.uid_for("alice") == uid_a)

# a planted symlink in alice's home does not redirect root's writes
target = os.path.join(ROOT, "victim")
os.makedirs(target)
run(alice, f"import os, shutil; p = os.path.expanduser('~/.ipython/profile_default/startup'); shutil.rmtree(p); os.symlink('{target}', p); print('x')")
alice.restart(timeout=30)
check("restart with a planted symlink: root wrote nothing through it", os.listdir(target) == [], str(os.listdir(target)))
out, errs, _ = run(alice, "print(spark is not None)")
check("the kernel still starts with the shim after the repair", out == "True", out + str(errs)[:300])
alice.shutdown(); bob.shutdown()

# Papermill (workflow notebook task) runs as the job owner
import nbformat
import papermill as pm
nb = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("import os; print('UID', os.getuid())")])
src = os.path.join(ROOT, "pm_in.ipynb"); dst = os.path.join(ROOT, "pm_out.ipynb")
nbformat.write(nb, src)
pm.execute_notebook(src, dst, kernel_name=ki.kernelspec_for("carol"), language="python")
txt = json.dumps(nbformat.read(dst, as_version=4).cells[0].outputs)
check("a Papermill run uses the owner's uid", f"UID {ki.uid_for('carol')}" in txt, txt[:300])

# not root: refused, never started unisolated
real = os.geteuid
os.geteuid = lambda: 1000
try:
    ki.kernelspec_for("dave"); refused = False
except ki.IsolationUnavailable:
    refused = True
finally:
    os.geteuid = real
check("without root a kernel is refused (fail closed)", refused)

shutil.rmtree(ROOT, ignore_errors=True)
print(f"\n{'ALL PASSED' if not FAILS else str(len(FAILS)) + ' FAILED: ' + ', '.join(FAILS)}")
sys.exit(1 if FAILS else 0)
