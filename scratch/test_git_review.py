#!/usr/bin/env python3
"""Git review (web/git_review.py, web/git_sync.py): diff view, discard, merge conflicts and their resolution, against a THROWAWAY Gitea.
  eval $(scratch/gitea_up.sh)        # prints TOK=...
  docker run --rm --network gtest_net -e TOK=$TOK -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook \
     sh -c 'pip install -q dbt-duckdb 2>&1 | tail -0; python /workspace/scratch/test_git_review.py'"""
import json, os, shutil, subprocess, sys, tempfile, time, requests
TMP = tempfile.mkdtemp(prefix="grv_"); os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
proj = os.path.join(TMP, "proj"); shutil.copytree("/workspace/web/dbt_template", proj); os.environ["DBT_PROJECT_DIR"] = proj
nbroot = os.path.join(TMP, "notebooks"); os.makedirs(os.path.join(nbroot, "Shared")); os.environ["NOTEBOOKS_DIR"] = nbroot
NBD = os.path.join(nbroot, "Shared")
TOK = os.environ["TOK"]; H = "http://gtest_gitea:3000"
os.environ.update({"GIT_REMOTE_URL": f"{H}/ga/dbt-rv.git", "GIT_TOKEN": TOK, "NOTEBOOKS_GIT_REMOTE_URL": f"{H}/ga/nb-rv.git", "NOTEBOOKS_GIT_MODE": "pull_request"})
sys.path.insert(0, "/workspace")
from web import git_sync as g, git_review as gr
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
def refuses(fn, *a, contains="", **kw):
    try: fn(*a, **kw)
    except g.GitError as e: return contains.lower() in str(e).lower(), str(e)
    return False, "no error"
api = lambda m, path, **kw: requests.request(m, f"{H}/api/v1{path}", headers={"Authorization": f"token {TOK}"}, timeout=20, **kw)
NB, D = g.NOTEBOOKS, g.DBT
def w(rel, text, base=NBD, mode="w"):
    p = os.path.join(base, rel); os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, mode) as f: f.write(text)
def rd(rel, base=NBD): return open(os.path.join(base, rel)).read()
def dev(repo):
    d = os.path.join(TMP, "dev_" + repo)
    denv = {**os.environ, "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": f"http.{H}/.extraHeader", "GIT_CONFIG_VALUE_0": f"Authorization: token {TOK}",
            "GIT_AUTHOR_NAME": "dev", "GIT_AUTHOR_EMAIL": "d@d", "GIT_COMMITTER_NAME": "dev", "GIT_COMMITTER_EMAIL": "d@d"}
    if not os.path.isdir(d): subprocess.run(["git", "clone", "-q", f"{H}/ga/{repo}.git", d], env=denv, check=True)
    def run(*a): return subprocess.run(["git", *a], cwd=d, env=denv, capture_output=True, text=True, check=True).stdout
    run("pull", "-q", "origin", "main")
    return d, run
def nbjson(*cells): return json.dumps({"cells": [{"cell_type": t, "metadata": {}, "source": s.splitlines(True), "outputs": [] if t == "code" else None} for t, s in cells], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}, indent=1) + "\n"
A0 = "".join(f"line {i}\n" for i in range(1, 11))

print("setup: a repository with a few files")
st = NB.connect("admin")
w("a.txt", A0); w("b.txt", "b1\nb2\nb3\n"); w("c.txt", "c1\n"); w("g.txt", "g1\ng2\n"); w("nb.ipynb", nbjson(("code", "x = 1\n"), ("markdown", "# title\n")))
NB.commit("alice", "initial"); NB.push("alice")
time.sleep(1); check("main is on the server", api("GET", "/repos/ga/nb-rv/branches/main").status_code == 200)

print("diff view: pending changes")
w("a.txt", A0.replace("line 3\n", "line 3 changed\n")); os.remove(os.path.join(NBD, "c.txt")); w("d.txt", "d1\nd2\nd3\n"); w("blob.bin", b"\x00\x01\x02binary", mode="wb")
w("nb.ipynb", nbjson(("code", "x = 2\n"), ("markdown", "# title\n"), ("code", "print(x)\n")))
s = NB.diff("pending"); by = {f["path"]: f for f in s["files"]}
check("every changed file is listed with its status", {p: f["status"] for p, f in by.items()} == {"a.txt": "M", "c.txt": "D", "d.txt": "A", "blob.bin": "A", "nb.ipynb": "M"}, {p: f["status"] for p, f in by.items()})
check("counts: modified 1+/1-, new file 3 added, deleted file 1 removed", (by["a.txt"]["additions"], by["a.txt"]["deletions"]) == (1, 1) and by["d.txt"]["additions"] == 3 and by["c.txt"]["deletions"] == 1, by)
check("binary files are flagged", by["blob.bin"]["binary"] is True and s["additions"] > 0)
d = NB.diff_file("a.txt", "pending"); lines = [l for h in d["hunks"] for l in h["lines"]]
check("a diff has hunks with old/new line numbers", any(l["t"] == "del" and l["s"] == "line 3" and l["o"] == 3 for l in lines) and any(l["t"] == "add" and l["s"] == "line 3 changed" and l["n"] == 3 for l in lines) and d["hunks"][0]["header"].startswith("@@"), d["hunks"][:1])
d = NB.diff_file("d.txt", "pending"); check("an untracked file shows as all added", [l["t"] for h in d["hunks"] for l in h["lines"]] == ["add"] * 3 and d["status"] == "A")
d = NB.diff_file("c.txt", "pending"); check("a deleted file shows as all removed", [l["t"] for h in d["hunks"] for l in h["lines"]] == ["del"])
d = NB.diff_file("blob.bin", "pending"); check("a binary file shows a notice, not bytes", d["hunks"] == [] and "Binary" in d["notice"])
d = NB.diff_file("nb.ipynb", "pending"); txt = " ".join(l["s"] for h in d["hunks"] for l in h["lines"])
check("a notebook is compared cell by cell (source only)", "cell" in d["notice"] and "x = 2" in txt and "# %% [code] cell 3" in txt and '"cell_type"' not in txt, txt[:200])
for bad, frag in (("../secret", "inside"), ("/etc/passwd", "inside"), (".git/config", "inside"), ("-rf", "inside"), ("b.txt", "no changes"), ("nope.txt", "no changes")):
    ok, m = refuses(NB.diff_file, bad, "pending", contains=frag); check(f"a path is refused: {bad}", ok, m)
w("big.txt", "".join(f"row {i} " + "x" * 60 + "\n" for i in range(6000)))
d = NB.diff_file("big.txt", "pending"); check("a huge diff is cut and says so", d["truncated"] is True and sum(len(h["lines"]) for h in d["hunks"]) <= gr.MAX_DIFF_LINES)
ok, m = refuses(NB.diff, "sideways", contains="scope"); check("an unknown scope is refused", ok, m)
r = NB.discard("alice", "d.txt"); check("discarding a new file deletes it", not os.path.exists(os.path.join(NBD, "d.txt")) and "d.txt" not in {f["path"] for f in NB.diff("pending")["files"]})
r = NB.discard("alice", "a.txt"); check("discarding a modified file restores it", rd("a.txt") == A0)
r = NB.discard("alice", "c.txt"); check("discarding a deletion brings the file back", rd("c.txt") == "c1\n")
ok, m = refuses(NB.discard, "alice", "b.txt", contains="no uncommitted"); check("an unchanged file cannot be 'discarded'", ok, m)
ok, m = refuses(NB.discard, "alice", "../x", contains="inside"); check("discard checks the path too", ok, m)
os.remove(os.path.join(NBD, "blob.bin")); os.remove(os.path.join(NBD, "big.txt"))

print("diff view: a change branch against the base")
w("a.txt", "".join(("line 3 mine\n" if i == 3 else "line 8 mine\n" if i == 8 else f"line {i}\n") for i in range(1, 11)))
w("b.txt", "b1 mine\nb2\nb3\n"); w("f.txt", "f mine\n"); w("nb.ipynb", nbjson(("code", "x = 5  # mine\n"), ("markdown", "# title\n")))
r = NB.commit("alice", "my change"); br = r["started_branch"]; NB.push("alice")
s = NB.diff("branch"); check("the branch view lists what a reviewer would see", {f["path"]: f["status"] for f in s["files"]} == {"a.txt": "M", "b.txt": "M", "f.txt": "A", "nb.ipynb": "M"}, s["files"])
d = NB.diff_file("f.txt", "branch"); check("a file added on the branch is all additions", [l["t"] for h in d["hunks"] for l in h["lines"]] == ["add"])
check("...and there is nothing pending", NB.diff("pending")["files"] == [])

print("conflicts: the base moved in overlapping ways")
cd, run = dev("nb-rv")
def dw(rel, text): open(os.path.join(cd, rel), "w").write(text)
dw("a.txt", "".join(("line 3 THEIRS\n" if i == 3 else "line 8 THEIRS\n" if i == 8 else f"line {i}\n") for i in range(1, 11)))
run("rm", "-q", "b.txt"); dw("f.txt", "f theirs\n"); dw("g.txt", "g1\ng2\ng3 theirs\n"); dw("nb.ipynb", nbjson(("code", "x = 9  # theirs\n"), ("markdown", "# title\n")))
run("add", "-A"); run("commit", "-q", "-m", "colleague"); run("push", "-q", "origin", "main")
before = NB._head()
r = NB.update_from_base("alice")
kinds = {c["path"]: c["kind"] for c in r["conflicts"]}
check("the merge is left in progress with the conflicted files listed by kind", kinds == {"a.txt": "both_modified", "b.txt": "deleted_by_incoming", "f.txt": "both_added", "nb.ipynb": "both_modified"}, kinds)
check("the non-conflicting change (g.txt) merged by itself", rd("g.txt") == "g1\ng2\ng3 theirs\n")
check("status reports the merge in progress", NB.status()["merge"]["in_progress"] is True and len(NB.status()["merge"]["conflicts"]) == 4 and "waiting for you" in r["message"], NB.status()["merge"])
for label, fn, args in (("commit", NB.commit, ("alice", "x")), ("push", NB.push, ("alice",)), ("pull", NB.pull, ("alice",)), ("open a pull request", NB.open_pull_request, ("alice", "t", "b")),
                        ("update again", NB.update_from_base, ("alice",)), ("discard", NB.discard, ("alice", "g.txt")), ("finish the change", NB.finish_change, ("alice",)), ("start a change", NB.start_change, ("alice",))):
    ok, m = refuses(fn, *args, contains="merge"); check(f"'{label}' is refused while the merge is unresolved", ok, m)
ok, m = refuses(NB.finish_merge, "alice", contains="still in conflict"); check("finishing is refused with unresolved files", ok, m)
c = NB.conflict_file("a.txt"); segs = [s_ for s_ in c["segments"] if s_["type"] == "conflict"]
check("the resolver gets the conflict hunks (yours / incoming) with their text", len(segs) == 2 and segs[0]["ours"] == ["line 3 mine"] and segs[0]["theirs"] == ["line 3 THEIRS"] and segs[1]["ours"] == ["line 8 mine"] and "theirs" in c["theirs_text"].lower(), segs)
ok, m = refuses(NB.resolve_conflict, "alice", "a.txt", "merged", [{"pick": "ours"}], contains="every conflict"); check("every hunk must be decided", ok, m)
ok, m = refuses(NB.resolve_conflict, "alice", "a.txt", "merged", [{"pick": "ours"}, {"pick": "banana"}], contains="choose"); check("an unknown pick is refused", ok, m)
ok, m = refuses(NB.resolve_conflict, "alice", "a.txt", "merged", None, "<<<<<<< x\nfoo\n=======\nbar\n>>>>>>> y\n", contains="markers"); check("typed text with conflict markers is refused", ok, m)
ok, m = refuses(NB.resolve_conflict, "alice", "../x", "ours", contains="inside"); check("the path is checked", ok, m)
ok, m = refuses(NB.resolve_conflict, "alice", "g.txt", "ours", contains="not in conflict"); check("a file that is not in conflict cannot be 'resolved'", ok, m)
r = NB.resolve_conflict("alice", "a.txt", "merged", [{"pick": "both"}, {"pick": "custom", "text": "line 8 combined"}])
check("hunk by hunk: 'both' keeps both sides, 'custom' takes the typed text; other lines untouched", rd("a.txt") == "".join(("line 3 mine\nline 3 THEIRS\n" if i == 3 else "line 8 combined\n" if i == 8 else f"line {i}\n") for i in range(1, 11)), rd("a.txt"))
check("the file left the conflict list", "a.txt" not in {c_["path"] for c_ in NB.conflicts()["conflicts"]} and "a.txt" in NB.conflicts()["resolved"])
ok, m = refuses(NB.resolve_conflict, "alice", "b.txt", "merged", None, "x", contains="keep"); check("a modify/delete conflict offers keep or delete, not a text", ok, m)
r = NB.resolve_conflict("alice", "b.txt", "delete"); check("modify/delete: 'delete' removes the file", not os.path.exists(os.path.join(NBD, "b.txt")))
r = NB.resolve_conflict("alice", "f.txt", "merged", None, "f merged by hand\n"); check("both-added: the complete text can be typed", rd("f.txt") == "f merged by hand\n")
ok, m = refuses(NB.finish_merge, "alice", contains="still in conflict"); check("one file (the notebook) is still open", ok, m)
r = NB.resolve_conflict("alice", "nb.ipynb", "merged", None, '{"cells": "oops"'); ok, m = refuses(NB.finish_merge, "alice", contains="notebook")
check("a resolution that is not a valid notebook is refused at finish (the merge stays open)", ok and NB.status()["merge"]["in_progress"], m)
r = NB.resolve_conflict("alice", "nb.ipynb", "reopen"); check("a resolved file can be taken back into conflict", "nb.ipynb" in {c_["path"] for c_ in NB.conflicts()["conflicts"]})
r = NB.resolve_conflict("alice", "nb.ipynb", "theirs"); check("whole-file 'theirs' takes the incoming version", "theirs" in rd("nb.ipynb"))
subprocess.run(["git", "checkout", "-m", "--", "f.txt"], cwd=NBD, check=True, capture_output=True); w("f.txt", "<<<<<<< a\nx\n=======\ny\n>>>>>>> b\n"); subprocess.run(["git", "add", "f.txt"], cwd=NBD, check=True)
ok, m = refuses(NB.finish_merge, "alice", contains="markers"); check("a file staged with conflict markers left in it blocks the finish", ok, m)
NB.resolve_conflict("alice", "f.txt", "reopen"); NB.resolve_conflict("alice", "f.txt", "ours"); check("'ours' takes my version", rd("f.txt") == "f mine\n")
r = NB.finish_merge("alice"); sha = r["committed"]
parents = subprocess.run(["git", "log", "-1", "--format=%P|%an|%s", sha], cwd=NBD, capture_output=True, text=True).stdout.strip()
check("the merge is committed: two parents, authored as the user, default merge message", len(parents.split("|")[0].split()) == 2 and parents.split("|")[1] == "alice" and "Merge" in parents.split("|")[2] and not NB.status()["merge"]["in_progress"], parents)
check("the merged content is what was decided", rd("a.txt").count("line 3 mine") == 1 and not os.path.exists(os.path.join(NBD, "b.txt")) and rd("g.txt").endswith("g3 theirs\n"))
r = NB.push("alice"); check("the merged branch can be pushed", r["pushed"] and not r["unpushed"], r)
check("the branch view now shows only my changes on top of the new base", "g.txt" not in {f["path"] for f in NB.diff("branch")["files"]})

print("abort returns to exactly where it was")
cd, run = dev("nb-rv"); dw("a.txt", "theirs2\n" + rd("a.txt").split("\n", 1)[1]); run("add", "-A"); run("commit", "-q", "-m", "again"); run("push", "-q", "origin", "main")
w("a.txt", "mine2\n" + rd("a.txt").split("\n", 1)[1]); NB.commit("alice", "second local change"); before = NB._head(); snapshot = rd("a.txt")
r = NB.update_from_base("alice"); check("a second conflict opens", r.get("conflicts") and NB.status()["merge"]["in_progress"], r)
r = NB.abort_merge("alice"); check("abort restores the commit and the files, and clears the merge", NB._head() == before and rd("a.txt") == snapshot and not NB.status()["merge"]["in_progress"] and not NB.status()["dirty"], NB.status())
ok, m = refuses(NB.abort_merge, "alice", contains="no merge"); check("aborting twice is refused", ok, m)
ok, m = refuses(NB.finish_merge, "alice", contains="no merge"); check("finishing with no merge is refused", ok, m)

print("direct mode: local and remote diverged")
rm = api("POST", f"/repos/ga/nb-rv/pulls", json={"head": br, "base": "main", "title": "x"}); num = rm.json().get("number")
api("POST", f"/repos/ga/nb-rv/pulls/{num}/merge", json={"Do": "merge"}) if num else None
os.environ["NOTEBOOKS_GIT_MODE"] = "direct"
try: NB.abandon_change  # noqa
except Exception: pass
NB._git(["fetch", "origin", "--prune"]); NB._git(["switch", "main"]); NB._git(["reset", "-q", "--hard", "origin/main"]); NB._git(["branch", "-D", br], check=False)
w("g.txt", "g1\ng2\ng3 theirs\ng-local\n"); NB.commit("alice", "local commit on main")
cd, run = dev("nb-rv"); dw("h.txt", "remote only\n"); run("add", "-A"); run("commit", "-q", "-m", "remote commit"); run("push", "-q", "origin", "main")
ok, m = refuses(NB.pull, "alice", contains="merge"); check("pull refuses and points to 'Merge remote changes'", ok and "Merge remote" in m, m)
r = NB.merge_remote("alice"); check("a divergence without overlap merges cleanly", r.get("message", "").startswith("Merged") and rd("h.txt") == "remote only\n" and rd("g.txt").endswith("g-local\n") and not NB.status()["merge"]["in_progress"], r.get("message"))
NB.push("alice")
w("nb.ipynb", nbjson(("code", "x = 100  # local\n"), ("markdown", "# title\n"))); NB.commit("alice", "local notebook edit")
cd, run = dev("nb-rv"); open(os.path.join(cd, "nb.ipynb"), "w").write(nbjson(("code", "x = 200  # remote\n"), ("markdown", "# title\n"))); run("add", "-A"); run("commit", "-q", "-m", "remote notebook"); run("push", "-q", "origin", "main")
r = NB.merge_remote("alice"); check("a notebook conflict in direct mode is left for the resolver", r.get("conflicts") and r["conflicts"][0]["path"] == "nb.ipynb", r.get("message"))
c = NB.conflict_file("nb.ipynb"); check("notebook conflicts carry a flag and hunks", c["ipynb"] is True and any(s_["type"] == "conflict" for s_ in c["segments"]))
NB.resolve_conflict("alice", "nb.ipynb", "theirs"); r = NB.finish_merge("alice")
check("finish validates the notebook and commits; the result can be pushed", json.loads(rd("nb.ipynb"))["cells"][0]["source"] == ["x = 200  # remote\n"] and NB.push("alice")["pushed"])

print("the dbt project: a merge only finishes when dbt accepts the result")
os.environ["GIT_MODE"] = "direct"
D.connect("admin"); D.commit("alice", "init dbt"); D.push("alice")
dcd, drun = dev("dbt-rv")
open(os.path.join(dcd, "dbt_project.yml"), "a").write("# remote comment\nvars:\n  remote_var: 1\n"); drun("add", "-A"); drun("commit", "-q", "-m", "remote"); drun("push", "-q", "origin", "main")
open(os.path.join(proj, "dbt_project.yml"), "a").write("# local comment\nvars:\n  local_var: 2\n"); D.commit("alice", "local")
r = D.merge_remote("alice"); check("the dbt project conflicts in dbt_project.yml", r.get("conflicts") and r["conflicts"][0]["path"] == "dbt_project.yml", r.get("message"))
D.resolve_conflict("alice", "dbt_project.yml", "merged", None, open(os.path.join(proj, "dbt_project.yml")).read().split("<<<<<<<")[0] + "vars: [broken\n")
ok, m = refuses(D.finish_merge, "alice", contains="does not validate"); check("a merge that breaks the project is not finished (dbt parse)", ok, m[:200])
D.resolve_conflict("alice", "dbt_project.yml", "reopen"); D.resolve_conflict("alice", "dbt_project.yml", "theirs")
r = D.finish_merge("alice"); check("with a valid result it finishes", "committed" in r and not D.status()["merge"]["in_progress"])
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
