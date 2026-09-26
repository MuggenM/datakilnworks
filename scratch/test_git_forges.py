#!/usr/bin/env python3
"""GitHub and GitLab pull/merge-request clients (web/git_sync.py) against an in-process MOCK forge that checks the documented headers and shapes,
with a THROWAWAY Gitea (scratch/gitea_up.sh) carrying the git traffic:
  docker run --rm --network gtest_net -e TOK=$TOK -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_git_forges.py"""
import base64, json, os, shutil, sys, tempfile, threading, http.server, urllib.parse
TMP = tempfile.mkdtemp(prefix="gfg_"); os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
proj = os.path.join(TMP, "proj"); shutil.copytree("/workspace/web/dbt_template", proj); os.environ["DBT_PROJECT_DIR"] = proj
nbroot = os.path.join(TMP, "notebooks"); os.makedirs(os.path.join(nbroot, "Shared")); os.environ["NOTEBOOKS_DIR"] = nbroot
TOK = os.environ["TOK"]; H = "http://gtest_gitea:3000"
sys.path.insert(0, "/workspace")
from web import git_sync as g
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
def refuses(fn, *a, contains=""):
    try: fn(*a)
    except g.GitError as e: return contains.lower() in str(e).lower(), str(e)
    return False, "no error"
D = g.DBT

# ---- the mock forge
STATE = {"prs": [], "seen": [], "fail": None}
class Mock(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, body):
        b = json.dumps(body).encode(); self.send_response(code); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def _handle(self):
        u = urllib.parse.urlparse(self.path); q = dict(urllib.parse.parse_qsl(u.query)); n = int(self.headers.get("Content-Length") or 0); body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        STATE["seen"].append({"method": self.command, "path": u.path, "query": q, "headers": {k.lower(): v for k, v in self.headers.items()}, "body": body})
        if STATE["fail"]: return self._send(*STATE["fail"])
        path = urllib.parse.unquote(u.path)
        if path.startswith("/repos/ga/dbt-fg/pulls"):                                   # GitHub
            if self.headers.get("Authorization") != f"Bearer {TOK}": return self._send(401, {"message": "Bad credentials"})
            if self.command == "POST":
                pr = {"number": len(STATE["prs"]) + 1, "html_url": f"https://github.example/ga/dbt-fg/pull/{len(STATE['prs']) + 1}", "title": body["title"], "state": "open", "body": body.get("body"),
                      "head": {"ref": body["head"]}, "base": {"ref": body["base"]}, "merged_at": None, "mergeable": None}
                STATE["prs"].append(pr); return self._send(201, pr)
            return self._send(200, [p for p in reversed(STATE["prs"])])
        if path.startswith("/api/v4/projects/team/sub/proj/merge_requests") or path.startswith("/projects/team/sub/proj/merge_requests"):   # GitLab
            if self.headers.get("PRIVATE-TOKEN") != TOK: return self._send(401, {"message": "401 Unauthorized"})
            if self.command == "POST":
                mr = {"iid": len(STATE["prs"]) + 1, "web_url": f"https://gitlab.example/team/sub/proj/-/merge_requests/{len(STATE['prs']) + 1}", "title": body["title"], "state": "opened", "description": body.get("description"),
                      "source_branch": body["source_branch"], "target_branch": body["target_branch"], "detailed_merge_status": "checking"}
                STATE["prs"].append(mr); return self._send(201, mr)
            return self._send(200, [p for p in reversed(STATE["prs"]) if p["source_branch"] == q.get("source_branch") and p["target_branch"] == q.get("target_branch")])
        return self._send(404, {"message": "Not Found"})
    do_GET = do_POST = _handle
srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Mock); threading.Thread(target=srv.serve_forever, daemon=True).start(); MOCK = f"http://127.0.0.1:{srv.server_address[1]}"

print("forge detection and addresses")
def cfg(url, forge=None, api=None, token=TOK):
    for k in ("GIT_FORGE", "GIT_API_URL"): os.environ.pop(k, None)
    os.environ["GIT_REMOTE_URL"] = url; os.environ["GIT_TOKEN"] = token
    if forge: os.environ["GIT_FORGE"] = forge
    if api: os.environ["GIT_API_URL"] = api
cfg("https://github.com/acme/data.git"); check("github.com is GitHub, API on api.github.com", D.forge() == "github" and D.api_base() == "https://api.github.com" and D._repo_ref()[1:] == ("acme", "data"))
cfg("https://ghe.corp.example/acme/data.git", forge="github"); check("GitHub Enterprise: /api/v3 on the same host", D.api_base() == "https://ghe.corp.example/api/v3")
cfg("https://gitlab.com/team/sub/proj.git"); check("gitlab.com is GitLab, /api/v4, the whole project path (subgroups)", D.forge() == "gitlab" and D.api_base() == "https://gitlab.com/api/v4" and D.project_path() == "team/sub/proj")
cfg("https://git.corp.example/team/proj.git", forge="gitlab"); check("a self-hosted GitLab is chosen with GIT_FORGE", D.forge() == "gitlab" and D.api_base() == "https://git.corp.example/api/v4")
cfg("https://code.example.org/team/proj.git"); check("anything else is Gitea (as before)", D.forge() == "gitea" and D.api_base() == "https://code.example.org/api/v1")
cfg("https://x.example/a/b.git", forge="bitbucket"); ok, m = refuses(D.forge, contains="gitea, github or gitlab"); check("an unknown forge is refused", ok, m)
cfg("https://x.example/a/b.git", api="https://api.example/custom/"); check("GIT_API_URL always wins", D.api_base() == "https://api.example/custom")

print("how git itself authenticates")
cfg(f"{H}/ga/dbt-fg.git", forge="gitea"); check("Gitea: token header", D._git_auth_header() == f"Authorization: token {TOK}")
cfg(f"{H}/ga/dbt-fg.git", forge="github"); h = D._git_auth_header(); check("GitHub: Basic x-access-token:<token>", base64.b64decode(h.split()[-1]).decode() == f"x-access-token:{TOK}")
cfg(f"{H}/ga/dbt-fg.git", forge="gitlab"); h = D._git_auth_header(); check("GitLab: Basic oauth2:<token>", base64.b64decode(h.split()[-1]).decode() == f"oauth2:{TOK}")
b64 = h.split()[-1]; check("the token and its base64 form are scrubbed from messages", TOK not in D._scrub(f"fatal {TOK} and {b64}") and b64 not in D._scrub(f"x {b64}"))
env = D._env(); check("the credential reaches git only through the environment", env["GIT_CONFIG_VALUE_0"] == h and TOK not in " ".join(k for k in env if k != "GIT_CONFIG_VALUE_0"))

print("GitHub: open a pull request, read it, finish after a squash-merge")
cfg(f"{H}/ga/dbt-fg.git", forge="github", api=MOCK); os.environ["GIT_MODE"] = "pull_request"
D.connect("admin"); D.commit("alice", "initial"); D.push("alice")
open(os.path.join(proj, "models/marts/gh.sql"), "w").write("select 1 as a\n"); r = D.commit("alice", "add gh model"); br = r["started_branch"]; D.push("alice")
STATE["seen"].clear(); r = D.open_pull_request("alice", "Add gh model", "please review")
post = next(x for x in STATE["seen"] if x["method"] == "POST")
check("the request goes to /repos/<owner>/<repo>/pulls with GitHub's headers and body", post["path"] == "/repos/ga/dbt-fg/pulls" and post["headers"]["authorization"] == f"Bearer {TOK}" and post["headers"]["x-github-api-version"] == "2022-11-28" and "github+json" in post["headers"]["accept"]
      and post["body"] == {"head": br, "base": "main", "title": "Add gh model", "body": "please review"}, post)
check("the pull request is normalised (number, url, state)", r["pull_request"] == {"number": 1, "url": "https://github.example/ga/dbt-fg/pull/1", "title": "Add gh model", "state": "open", "merged": False, "mergeable": None}, r["pull_request"])
lst = next(x for x in STATE["seen"] if x["method"] == "GET"); check("the existing-PR lookup filters by head (owner:branch) and sorts by update", lst["query"].get("head") == f"ga:{br}" and lst["query"]["state"] == "all" and lst["query"]["sort"] == "updated", lst["query"])
n = len(STATE["prs"]); r = D.open_pull_request("alice", "again"); check("opening again returns the same pull request (nothing new is created)", r["pull_request"]["number"] == 1 and len(STATE["prs"]) == n)
st = D.status(fetch=True); check("status shows the pull request and the forge", st["pull_request"]["number"] == 1 and st["forge"] == "github")
ok, m = refuses(D.finish_change, "alice", contains="not merged"); check("finish is refused before the merge", ok, m)
STATE["prs"][0].update(state="closed", merged_at="2026-01-01T00:00:00Z")           # squash-merged on GitHub: the branch is NOT an ancestor of main
st = D.status(fetch=True); check("a squash-merged PR (merged_at set) is recognised as merged", st["pull_request"]["merged"] is True)
r = D.finish_change("alice"); check("finish returns to the base branch", r["current_branch"] == "main" and r["finished_branch"] == br, r.get("current_branch"))

print("GitHub: errors are short and never contain the token")
for code, body, frag in ((401, {"message": "Bad credentials"}, "refused the token"), (403, {"message": "API rate limit exceeded"}, "rate limit"), (404, {"message": "Not Found"}, "not found"), (422, {"message": f"Validation Failed {TOK}"}, "422"), (500, {}, "500")):
    STATE["fail"] = (code, body); ok, m = refuses(D._pr_for_branch, "x", contains=frag); check(f"HTTP {code}: {m[:70]}", ok and TOK not in m, m)
STATE["fail"] = None

print("GitLab: merge requests")
STATE["prs"].clear(); STATE["seen"].clear()
cfg(f"{H}/ga/dbt-fg.git", forge="gitlab", api=MOCK + "/api/v4"); D._git(["remote", "set-url", "origin", f"{H}/ga/dbt-fg.git"])
class P:                                                      # GitLab's project path comes from the URL: point the API at a nested project while git stays on Gitea
    pass
g.Repo.project_path = lambda self: "team/sub/proj"
open(os.path.join(proj, "models/marts/gl.sql"), "w").write("select 2 as b\n"); r = D.commit("alice", "add gl model"); br = r["started_branch"]
os.environ["GIT_FORGE"] = "gitlab"; D.push("alice")
r = D.open_pull_request("alice", "Add gl model", "review please")
post = next(x for x in STATE["seen"] if x["method"] == "POST")
check("a merge request is created under /projects/<url-encoded path>/merge_requests", post["path"] == "/api/v4/projects/team%2Fsub%2Fproj/merge_requests", post["path"])
check("...with GitLab's field names", post["headers"]["private-token"] == TOK and post["body"] == {"source_branch": br, "target_branch": "main", "title": "Add gl model", "description": "review please", "remove_source_branch": False}, post["body"])
check("the merge request is normalised (iid as number, opened as open, mergeable unknown while checking)", r["pull_request"]["number"] == 1 and r["pull_request"]["state"] == "open" and r["pull_request"]["mergeable"] is False and "merge_requests/1" in r["pull_request"]["url"], r["pull_request"])
lst = [x for x in STATE["seen"] if x["method"] == "GET"][0]; check("the lookup filters by source and target branch", lst["query"]["source_branch"] == br and lst["query"]["target_branch"] == "main")
STATE["prs"][0].update(state="merged", detailed_merge_status="not_open"); st = D.status(fetch=True)
check("state 'merged' is a merged request", st["pull_request"]["merged"] is True and st["pull_request"]["state"] == "closed", st["pull_request"])
STATE["prs"][0].update(state="closed"); STATE["prs"][0]["detailed_merge_status"] = "mergeable"; st = D.status(fetch=True); check("'closed' is closed, not merged", st["pull_request"]["merged"] is False and st["pull_request"]["state"] == "closed")
STATE["prs"][0].update(state="opened"); check("'mergeable' status is passed through", D._pr_for_branch(br)["mergeable"] is True)
STATE["fail"] = (401, {"message": "401 Unauthorized"}); ok, m = refuses(D._pr_for_branch, br, contains="'api' scope"); check("a refused GitLab token names the scope it needs", ok, m)
STATE["fail"] = None
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
