"""git_sync against a throwaway Gitea. Run in a container on the same network:
  docker run --rm --network gtest_net -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch \
     -e TOK=<token> localspark-lakehouse-notebook python /workspace/scratch/test_git_sync.py"""
import os, shutil, subprocess, sys, tempfile
sys.path.insert(0, "/workspace")
tmp = tempfile.mkdtemp(prefix="gs_")
os.environ["WAREHOUSE_DIR"] = os.path.join(tmp, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
proj = os.path.join(tmp, "proj"); shutil.copytree("/workspace/web/dbt_template", proj)
os.environ["DBT_PROJECT_DIR"] = proj
TOK = os.environ["TOK"]
from web import git_sync as g
fails = []
def check(name, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  {extra}" if not cond else ""))
    if not cond: fails.append(name)
def refuses(fn, *a, contains=""):
    try: fn(*a)
    except g.GitError as e: return contains.lower() in str(e).lower(), str(e)
    return False, "no error"

check("unconfigured", g.status()["configured"] is False)
os.environ["GIT_REMOTE_URL"] = "http://gtest_gitea:3000/ga/dbt-project.git"; os.environ["GIT_TOKEN"] = TOK
ok, m = refuses(g.commit, "u", "x", contains="connect"); check("commit before connect refused", ok, m)
os.environ["GIT_REMOTE_URL"] = "http://user:pw@gtest_gitea:3000/ga/dbt-project.git"
ok, m = refuses(g.connect, "admin", contains="credentials"); check("credentials in URL refused", ok, m)
os.environ["GIT_REMOTE_URL"] = "file:///etc"
ok, m = refuses(g.connect, "admin", contains="http"); check("file:// refused", ok, m)
os.environ["GIT_REMOTE_URL"] = "http://gtest_gitea:3000/ga/dbt-project.git"

# a project nested inside another repo must not be operated on
outer = os.path.join(tmp, "outer"); os.makedirs(os.path.join(outer, "inner")); subprocess.run(["git", "init", "-q"], cwd=outer)
os.environ["DBT_PROJECT_DIR"] = os.path.join(outer, "inner")
ok, m = refuses(g.pull, "u", contains="not connected"); check("nested in another repo: refused", ok, m)
os.environ["DBT_PROJECT_DIR"] = proj

st = g.connect("admin"); check("connect (empty remote)", st["connected"] and st["head"] is None, st)
check("build artefacts excluded", "target/" in open(os.path.join(proj, ".git/info/exclude")).read())
open(os.path.join(proj, "target_x.duckdb"), "w").write("x")
check("*.duckdb ignored", not any(c["path"].endswith(".duckdb") for c in g.status()["changes"]))
ok, m = refuses(g.commit, "admin", "  ", contains="message"); check("empty message refused", ok, m)
ok, m = refuses(g.commit, "admin", "x", contains="") if False else (True, "")
# plaintext secret blocks a commit
prof = open(os.path.join(proj, "profiles.yml")).read()
open(os.path.join(proj, "profiles.yml"), "a").write("\n# t\nextra:\n  outputs:\n    dev:\n      type: duckrun\n      password: hunter2\n")
ok, m = refuses(g.commit, "admin", "with secret", contains="plain text"); check("plaintext credential blocks commit", ok, m)
open(os.path.join(proj, "profiles.yml"), "w").write(prof)

r = g.commit("alice", "initial project"); check("commit", r["committed"] and not r["dirty"], r)
who = subprocess.run(["git", "log", "-1", "--format=%an|%ae|%cn"], cwd=proj, capture_output=True, text=True).stdout.strip()
check("authored as the acting user", who == "alice|alice@datakilnworks.local|Data Kiln Works", who)
r = g.push("alice"); check("push", r["pushed"] and r["ahead"] == 0, r)
check("token not stored in .git/config", TOK not in open(os.path.join(proj, ".git/config")).read())
check("head_sha", g.head_sha() == g.status()["head"])

# a developer clones, pushes a change
dev = os.path.join(tmp, "dev")
denv = {**os.environ, "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "http.http://gtest_gitea:3000/.extraHeader", "GIT_CONFIG_VALUE_0": f"Authorization: token {TOK}",
        "GIT_AUTHOR_NAME": "dev", "GIT_AUTHOR_EMAIL": "d@d", "GIT_COMMITTER_NAME": "dev", "GIT_COMMITTER_EMAIL": "d@d"}
def dgit(*a): return subprocess.run(["git", *a], cwd=dev, env=denv, capture_output=True, text=True, check=True)
subprocess.run(["git", "clone", "-q", os.environ["GIT_REMOTE_URL"], dev], env=denv, check=True)
open(os.path.join(dev, "models/marts/extra.sql"), "w").write("select 1 as x\n"); dgit("add", "-A"); dgit("commit", "-qm", "dev model"); dgit("push", "-q", "origin", "HEAD:main")
st = g.status(fetch=True); check("behind=1 after fetch", st["behind"] == 1 and st["ahead"] == 0, st)
open(os.path.join(proj, "README_local.md"), "w").write("local\n")
ok, m = refuses(g.pull, "admin", contains="uncommitted"); check("pull refused on dirty tree", ok, m)
os.remove(os.path.join(proj, "README_local.md"))
r = g.pull("admin"); check("fast-forward pull", r["pulled"] and os.path.exists(os.path.join(proj, "models/marts/extra.sql")), r)
r = g.pull("admin"); check("pull again = up to date", r["pulled"] is False, r)

# a broken project must be rolled back
good = g.head_sha()
open(os.path.join(dev, "models/marts/broken.sql"), "w").write("select {{ ref('does_not_exist') }}\n"); dgit("add", "-A"); dgit("commit", "-qm", "broken"); dgit("push", "-q", "origin", "HEAD:main")
ok, m = refuses(g.pull, "admin", contains="rolled back"); check("dbt-rejected pull rolled back", ok, m)
check("HEAD restored after rollback", g.head_sha() == good and not os.path.exists(os.path.join(proj, "models/marts/broken.sql")))
dgit("revert", "--no-edit", "HEAD"); dgit("push", "-q", "origin", "HEAD:main")

# divergence
g.pull("admin")
open(os.path.join(proj, "models/marts/local.sql"), "w").write("select 2 as y\n"); g.commit("bob", "local model")
open(os.path.join(dev, "models/marts/other.sql"), "w").write("select 3 as z\n"); dgit("add", "-A"); dgit("commit", "-qm", "other"); dgit("push", "-q", "origin", "HEAD:main")
g.status(fetch=True)
ok, m = refuses(g.push, "bob", contains="pull first"); check("non-fast-forward push refused (no force)", ok, m)
ok, m = refuses(g.pull, "bob", contains="diverged"); check("diverged pull refused", ok, m)

# wrong token: error is scrubbed
os.environ["GIT_TOKEN"] = "wrongtoken123"
try: g.status(fetch=True); st = g.status(fetch=True)
except Exception as e: st = {}
check("bad token surfaces as fetch_error, scrubbed", "wrongtoken123" not in str(st) and st.get("fetch_error"), st)
os.environ["GIT_TOKEN"] = TOK

# second checkout on a seeded dir adopts remote history
proj2 = os.path.join(tmp, "proj2"); shutil.copytree("/workspace/web/dbt_template", proj2); os.environ["DBT_PROJECT_DIR"] = proj2
st = g.connect("admin"); check("connect adopts remote history", st["head"] == st["remote_head"], st)
check("connect keeps local files", os.path.exists(os.path.join(proj2, "models/marts/example_hello.sql")))

from web.governance import store
c = store.get_db(); acts = [r[0] for r in c.execute("select action from governance_audit where action like 'GIT_%'")]; c.close()
check("audited", {"GIT_CONNECT", "GIT_COMMIT", "GIT_PUSH", "GIT_PULL", "GIT_PULL_REJECTED"} <= set(acts), acts)

# ---------------------------------------------------------------- notebooks (notebooks/Shared only)
import json as _json
nbroot = os.path.join(tmp, "notebooks"); shared = os.path.join(nbroot, "Shared"); os.makedirs(shared); os.makedirs(os.path.join(nbroot, "Users/alice"))
os.environ["NOTEBOOKS_DIR"] = nbroot
os.environ["NOTEBOOKS_GIT_REMOTE_URL"] = "http://gtest_gitea:3000/ga/notebooks.git"
nb = {"cells": [{"cell_type": "code", "source": ["print(1)"], "execution_count": 7, "metadata": {}, "outputs": [{"output_type": "stream", "name": "stdout", "text": ["SECRET-ROW-DATA\n"]}]}],
      "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
open(os.path.join(shared, "a.ipynb"), "w").write(_json.dumps(nb))
open(os.path.join(shared, "helpers.py"), "w").write("X = 1\n")
open(os.path.join(nbroot, "Users/alice/private.ipynb"), "w").write(_json.dumps(nb))
N = g.NOTEBOOKS
check("notebooks: not connected initially", N.status()["configured"] and not N.status()["connected"])
st = N.connect("admin"); check("notebooks: connect (token falls back to GIT_TOKEN)", st["connected"], st)
r = N.commit("carol", "shared notebooks"); check("notebooks: commit", bool(r["committed"]), r)
check("notebooks: only Shared is in the repo", set(subprocess.run(["git", "ls-files"], cwd=shared, capture_output=True, text=True).stdout.split()) == {"a.ipynb", "helpers.py"})
check("notebooks: nothing above Shared was touched", not os.path.exists(os.path.join(nbroot, ".git")))
blob = subprocess.run(["git", "show", "HEAD:a.ipynb"], cwd=shared, capture_output=True, text=True).stdout
check("notebooks: committed copy has no outputs", "SECRET-ROW-DATA" not in blob and _json.loads(blob)["cells"][0]["outputs"] == [] and _json.loads(blob)["cells"][0]["execution_count"] is None, blob[:200])
check("notebooks: working copy keeps its outputs", "SECRET-ROW-DATA" in open(os.path.join(shared, "a.ipynb")).read())
check("notebooks: clean after commit (filter is stable)", not N.status()["dirty"], N.status()["changes"])
r = N.push("carol"); check("notebooks: push", r["pushed"] and r["ahead"] == 0, r)
check("dbt repo unaffected by notebooks config", g.status()["remote"].endswith("dbt-project.git"))

big = os.path.join(shared, "data.bin"); open(big, "wb").write(b"0" * (g.MAX_FILE_BYTES + 1))
ok, m = refuses(N.commit, "carol", "big", contains="larger than"); check("notebooks: oversized file refused", ok, m)
os.remove(big)

nbdev = os.path.join(tmp, "nbdev")
subprocess.run(["git", "clone", "-q", os.environ["NOTEBOOKS_GIT_REMOTE_URL"], nbdev], env=denv, check=True)
def ngit(*a): return subprocess.run(["git", *a], cwd=nbdev, env=denv, capture_output=True, text=True, check=True)
open(os.path.join(nbdev, "b.ipynb"), "w").write("{ this is not json"); ngit("add", "-A"); ngit("commit", "-qm", "broken notebook"); ngit("push", "-q", "origin", "HEAD:main")
N.status(fetch=True)
ok, m = refuses(N.pull, "admin", contains="not a valid notebook"); check("notebooks: invalid notebook pull rolled back", ok, m)
check("notebooks: rollback removed the file", not os.path.exists(os.path.join(shared, "b.ipynb")))
ngit("rm", "-q", "b.ipynb"); open(os.path.join(nbdev, "c.ipynb"), "w").write(_json.dumps(nb)); ngit("add", "-A"); ngit("commit", "-qm", "fix"); ngit("push", "-q", "origin", "HEAD:main")
# the bad commit is in history: a fast-forward to the fix passes validation of the resulting tree
N.status(fetch=True); r = N.pull("admin"); check("notebooks: pull of a valid tree", r["pulled"] and os.path.exists(os.path.join(shared, "c.ipynb")), r)
c = store.get_db(); acts = [r[0] for r in c.execute("select action from governance_audit where action like 'NB_GIT_%'")]; c.close()
check("notebooks: audited under NB_GIT_*", {"NB_GIT_CONNECT", "NB_GIT_COMMIT", "NB_GIT_PUSH", "NB_GIT_PULL", "NB_GIT_PULL_REJECTED"} <= set(acts), acts)

print("FAILED: " + ", ".join(fails) if fails else "ALL PASS"); sys.exit(1 if fails else 0)
