#!/usr/bin/env python3
"""Git branch / pull-request mode (web/git_sync.py) against a THROWAWAY Gitea (never yours). Run in a container on the same network with
TOK (a token of user `ga`) and repos ga/dbt-pr and ga/nb-pr created empty (see the docstring of test_git_sync.py for the Gitea setup):
  docker run --rm --network gtest_net -e TOK=<token> -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook sh -c 'pip install -q dbt-duckdb && python /workspace/scratch/test_git_pr_mode.py'"""
import json, os, shutil, subprocess, sys, tempfile, requests
TMP = tempfile.mkdtemp(prefix="gpr_"); os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
proj = os.path.join(TMP, "proj"); shutil.copytree("/workspace/web/dbt_template", proj); os.environ["DBT_PROJECT_DIR"] = proj
nbroot = os.path.join(TMP, "notebooks"); os.makedirs(os.path.join(nbroot, "Shared")); os.environ["NOTEBOOKS_DIR"] = nbroot
TOK = os.environ["TOK"]; H = "http://gtest_gitea:3000"
os.environ.update({"GIT_REMOTE_URL": f"{H}/ga/dbt-pr.git", "GIT_TOKEN": TOK, "GIT_MODE": "pull_request",
                   "NOTEBOOKS_GIT_REMOTE_URL": f"{H}/ga/nb-pr.git", "NOTEBOOKS_GIT_MODE": "pull_request"})
sys.path.insert(0, "/workspace")
from web import git_sync as g
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def refuses(fn, *a, contains=""):
    try: fn(*a)
    except g.GitError as e: return contains.lower() in str(e).lower(), str(e)
    return False, "no error"
api = lambda m, path, **kw: requests.request(m, f"{H}/api/v1{path}", headers={"Authorization": f"token {TOK}"}, timeout=20, **kw)
D = g.DBT
def write(rel, text, base=proj):
    p = os.path.join(base, rel); os.makedirs(os.path.dirname(p), exist_ok=True); open(p, "w").write(text)
def dev(repo):
    """A second checkout ('a colleague') that can push straight to main."""
    d = os.path.join(TMP, "dev_" + repo)
    denv = {**os.environ, "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": f"http.{H}/.extraHeader", "GIT_CONFIG_VALUE_0": f"Authorization: token {TOK}",
            "GIT_AUTHOR_NAME": "dev", "GIT_AUTHOR_EMAIL": "d@d", "GIT_COMMITTER_NAME": "dev", "GIT_COMMITTER_EMAIL": "d@d"}
    if not os.path.isdir(d): subprocess.run(["git", "clone", "-q", f"{H}/ga/{repo}.git", d], env=denv, check=True)
    def run(*a): return subprocess.run(["git", *a], cwd=d, env=denv, capture_output=True, text=True, check=True)
    return d, run

print("configuration")
check("mode is read from the environment", D.mode() == "pull_request" and D.pr_mode())
check("the forge API is derived from the remote URL", D.api_base() == f"{H}/api/v1", D.api_base())
os.environ["GIT_REMOTE_URL"] = "https://code.example.org/git/team/proj.git"
check("...including a sub-path install and its owner/repo", D.api_base() == "https://code.example.org/git/api/v1" and D._repo_ref() == ("git", "team", "proj"), (D.api_base(), D._repo_ref()))
os.environ["GIT_REMOTE_URL"] = f"{H}/ga/dbt-pr.git"
os.environ["GIT_MODE"] = "sideways"; ok, m = refuses(D.start_change, "u"); check("an invalid mode is refused", ok or "mode" in m.lower(), m); os.environ["GIT_MODE"] = "pull_request"

print("bootstrap: the first commit of an empty remote goes to the base branch")
st = D.connect("admin"); check("connect (empty remote)", st["connected"] and st["mode"] == "pull_request" and st["on_base"] and not st["remote_has_base"], st)
r = D.commit("alice", "initial project"); check("first commit stays on main (nothing to branch from)", r["current_branch"] == "main" and "started_branch" not in r, r)
r = D.push("alice"); check("...and may be pushed to the empty remote's main", r["pushed"] and r["pushed_branch"] == "main", r)
import time
for _ in range(10):
    if api("GET", "/repos/ga/dbt-pr/branches/main").status_code == 200: break
    time.sleep(0.5)
check("the remote now has main", api("GET", "/repos/ga/dbt-pr/branches/main").status_code == 200)

print("a change goes through a branch")
write("models/marts/first.sql", "select 1 as a\n")
ok, m = refuses(D.push, "alice", contains="pull request"); check("push on main is refused in pull-request mode", ok, m)
r = D.commit("alice", "add first model"); br = r.get("started_branch")
check("committing on main opens a change branch automatically", br and br.startswith("dkw/alice-") and r["current_branch"] == br and not r["on_base"], r)
check("main itself did not move", D._git(["rev-parse", "main"]) != D._git(["rev-parse", br]))
check("the change has one unpushed commit", r["unpushed"] == 1, r["unpushed"])
ok, m = refuses(D.open_pull_request, "alice", "t", "b", contains="push"); check("a pull request needs the branch pushed first", ok, m)
r = D.push("alice"); check("push sends the change branch, not main", r["pushed_branch"] == br and r["unpushed"] == 0, r)
check("main is untouched on the server", api("GET", "/repos/ga/dbt-pr/branches/main").json()["commit"]["id"] == D._git(["rev-parse", "main"]))
ok, m = refuses(D.pull, "alice", contains="change branch"); check("pull is refused while on a change branch", ok, m)
ok, m = refuses(D.finish_change, "alice", contains="not merged"); check("finish is refused before the merge", ok, m)
ok, m = refuses(D.start_change, "alice", contains="already"); check("a second change cannot be started on top", ok, m)

print("pull request")
r = D.open_pull_request("alice", "Add first model", "please review")
pr = r["pull_request"]; check("a pull request is opened on the forge", pr["number"] == 1 and pr["state"] == "open", r)
g_pr = api("GET", "/repos/ga/dbt-pr/pulls/1").json()
check("...from the change branch into main with the title and body", g_pr["head"]["ref"] == br and g_pr["base"]["ref"] == "main" and g_pr["title"] == "Add first model" and g_pr["body"] == "please review", g_pr.get("head"))
r2 = D.open_pull_request("alice", "again"); check("opening again returns the same pull request", r2["pull_request"]["number"] == 1 and len(api("GET", "/repos/ga/dbt-pr/pulls?state=all").json()) == 1, r2)
st = D.status(fetch=True); check("status shows the pull request", st["pull_request"] and st["pull_request"]["number"] == 1 and st["pull_request"]["url"], st.get("pull_request"))

print("merge on the forge, then finish")
rm = api("POST", "/repos/ga/dbt-pr/pulls/1/merge", json={"Do": "merge"}); check("(merged on the forge by a reviewer)", rm.status_code in (200, 201, 204), rm.text)
st = D.status(fetch=True); check("status now says merged", st["pull_request"]["merged"] is True, st.get("pull_request"))
r = D.finish_change("alice")
check("finish returns to main with the merged content", r["current_branch"] == "main" and os.path.exists(os.path.join(proj, "models/marts/first.sql")) and r["finished_branch"] == br, r)
check("...and removes the local change branch", not D._has_ref(f"refs/heads/{br}"))
check("local main equals the server's main", D._git(["rev-parse", "main"]) == api("GET", "/repos/ga/dbt-pr/branches/main").json()["commit"]["id"])

print("a squash-merged change is recognised through the forge (not through ancestry)")
write("models/marts/second.sql", "select 2 as b\n"); r = D.commit("bob", "add second"); br2 = r["started_branch"]; D.push("bob"); n2 = D.open_pull_request("bob", "Second")["pull_request"]["number"]
api("POST", f"/repos/ga/dbt-pr/pulls/{n2}/merge", json={"Do": "squash"})
r = D.finish_change("bob"); check("finish works after a squash merge", r["current_branch"] == "main" and os.path.exists(os.path.join(proj, "models/marts/second.sql")), r)

print("update from base")
write("models/marts/third.sql", "select 3 as c\n"); r = D.commit("carol", "add third"); br3 = r["started_branch"]; D.push("carol")
d, run = dev("dbt-pr"); run("pull", "-q"); write("models/marts/from_dev.sql", "select 9 as z\n", d); run("add", "-A"); run("commit", "-qm", "dev change on main"); run("push", "-q", "origin", "HEAD:main")
D.status(fetch=True); before = D._head()
r = D.update_from_base("carol"); check("the base branch is merged into the change", os.path.exists(os.path.join(proj, "models/marts/from_dev.sql")) and D._head() != before, r)
check("...and stays pushable (a normal fast-forward for the server)", D.push("carol")["pushed"])
ok = D.update_from_base("carol")["message"].startswith("Already"); check("nothing to merge the second time", ok)

print("a conflict is left for the resolver, and can be aborted cleanly")
write("models/marts/clash.sql", "select 'mine' as v\n"); D.commit("carol", "mine"); D.push("carol")
write("models/marts/clash.sql", "select 'theirs' as v\n", d); run("pull", "-q"); run("add", "-A"); run("commit", "-qm", "theirs"); run("push", "-q", "origin", "HEAD:main")
D.status(fetch=True); head_before = D._head()
r = D.update_from_base("carol"); check("update reports the conflict and leaves the merge waiting for the resolver", r.get("conflicts") and r["conflicts"][0]["path"] == "models/marts/clash.sql" and D.status()["merge"]["in_progress"], r.get("message"))
D.abort_merge("carol")
check("aborting leaves HEAD and the tree exactly as they were", D._head() == head_before and not D.status()["dirty"] and not os.path.exists(os.path.join(proj, ".git", "MERGE_HEAD")), D.status()["changes"])

print("abandon")
ok, m = refuses(D.abandon_change, "carol", contains="never pushed") if False else (True, "")
write("models/marts/scratch.sql", "select 0 as s\n")
ok, m = refuses(D.abandon_change, "carol", contains="uncommitted"); check("abandon refuses uncommitted work", ok, m)
D.commit("carol", "scratch"); ok, m = refuses(D.abandon_change, "carol", contains="never pushed"); check("...and unpushed commits", ok, m)
D.push("carol"); r = D.abandon_change("carol"); check("once pushed and clean, it returns to main", r["current_branch"] == "main" and not D._has_ref(f"refs/heads/{br3}"), r)
check("the branch stays on the server", api("GET", f"/repos/ga/dbt-pr/branches/{br3.replace('/', '%2F')}").status_code == 200)

print("validation before a pull request")
D._git(["fetch", "origin"]); D.pull("admin")
write("models/marts/broken.sql", "select {{ ref('does_not_exist') }}\n"); r = D.commit("dan", "broken model"); D.push("dan")
ok, m = refuses(D.open_pull_request, "dan", "bad", contains="does not validate"); check("a project dbt rejects gets no pull request", ok, m)
check("...and none exists on the server", not [p for p in api("GET", "/repos/ga/dbt-pr/pulls?state=all").json() if p["head"]["ref"] == r["started_branch"]])
D.abandon_change("dan") if False else None

print("errors are safe")
os.environ["GIT_TOKEN"] = "wrong-token-value"
ok, m = refuses(D._api, "GET", "/pulls"); check("a refused token gives a clear message without the token", ok and "wrong-token-value" not in m and "write:repository" in m, m)
os.environ["GIT_TOKEN"] = TOK

print("direct mode is unchanged")
os.environ["GIT_MODE"] = "direct"
ok, m = refuses(D.start_change, "u", contains="off"); check("branch commands are refused in direct mode", ok, m)
check("status reports the mode", D.status()["mode"] == "direct")
os.environ["GIT_MODE"] = "pull_request"

print("notebooks use the same machinery")
N = g.NOTEBOOKS; sh = os.path.join(nbroot, "Shared")
nb = {"cells": [{"cell_type": "code", "source": ["1"], "execution_count": 1, "metadata": {}, "outputs": [{"output_type": "stream", "name": "stdout", "text": ["SECRET\n"]}]}], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
write("a.ipynb", json.dumps(nb), sh); N.connect("admin"); N.commit("erin", "first notebook"); N.push("erin")
write("b.ipynb", json.dumps(nb), sh); r = N.commit("erin", "second notebook"); nbr = r["started_branch"]; N.push("erin")
pr = N.open_pull_request("erin", "Second notebook")["pull_request"]; check("notebooks: change branch and pull request", pr["number"] == 1 and api("GET", "/repos/ga/nb-pr/pulls/1").json()["head"]["ref"] == nbr)
blob = subprocess.run(["git", "show", f"origin/{nbr}:b.ipynb"], cwd=sh, capture_output=True, text=True).stdout
check("notebooks: outputs are still stripped from what is proposed", "SECRET" not in blob)
api("POST", "/repos/ga/nb-pr/pulls/1/merge", json={"Do": "merge"}); r = N.finish_change("erin"); check("notebooks: finish", r["current_branch"] == "main" and os.path.exists(os.path.join(sh, "b.ipynb")), r)
write("c.ipynb", "{ not json", sh); r = N.commit("erin", "broken notebook"); N.push("erin")
ok, m = refuses(N.open_pull_request, "erin", "bad", contains="not a valid notebook"); check("notebooks: an invalid notebook gets no pull request", ok, m)

ac = [r[0] for r in __import__("sqlite3").connect(os.path.join(os.environ["WAREHOUSE_DIR"], ".metadata", "governance.db")).execute("select action from governance_audit")]
check("branch and pull-request actions are audited", {"GIT_BRANCH_CREATE", "GIT_PR_OPEN", "GIT_CHANGE_FINISH", "GIT_CHANGE_ABANDON", "GIT_BRANCH_UPDATE", "NB_GIT_PR_OPEN"} <= set(ac), set(ac))
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
