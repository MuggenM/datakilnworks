"""Git sync (phase 1: status, connect, pull, commit, push) for two directories, each through its own `Repo`:
the dbt project (`DBT`) and the shared notebooks (`NOTEBOOKS`, only `notebooks/Shared`, never the private `Users/` folders).

A directory (`DBT_PROJECT_DIR`, `NOTEBOOKS_DIR/Shared`) is synchronised with a remote repository (a self-hosted Gitea in the reference
deployment, but this is plain git over HTTP(S), so any server works). Rules that keep this safe:

* The project directory must be **its own repository**. In development it sits inside the platform repository, and running
  git there would commit the platform's files; every operation refuses unless the directory is the top level of its own repo.
* Configuration is environment only (a Kubernetes Secret in production): `GIT_REMOTE_URL` (http/https, no credentials in it),
  `GIT_TOKEN`, `GIT_BRANCH` (default `main`); for the notebooks the same names prefixed `NOTEBOOKS_` (the token falls back to `GIT_TOKEN`). The token is handed to git through `GIT_CONFIG_*` environment variables (never
  argv, never the URL, never written to `.git/config`) and is scrubbed from every message returned or logged.
* Pulls are fast-forward only and refused on a dirty tree, so nothing is merged or overwritten. The pulled project is then checked
  with `dbt parse`; if dbt rejects it the pull is rolled back to the previous commit.
* Commits are refused while a project file holds a plaintext credential, are authored as the acting user, and are audited.
* Two modes per repository (`GIT_MODE` / `NOTEBOOKS_GIT_MODE`): `direct` (default: commit and push straight to the branch) and `pull_request`:
  the base branch is never pushed to; a commit made on it first opens a change branch (`dkw/<user>-<timestamp>`), Push publishes that branch,
  *Open pull request* asks the forge (Gitea API) to review it, and *Finish* returns to the base branch once the pull request is merged there
  (merging is done on the forge by a reviewer, never from here). Nothing is ever force-pushed and a merge conflict is never left half-done.
* One lock serialises every operation (the project is a single working tree, so run a single replica).
"""
import datetime
import json
import shutil
import logging
import os
import re
import subprocess
import sys
import threading
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger("localspark.git_sync")

_LOCK = threading.RLock()
_TIMEOUT = 60
# Kept out of the repository without touching the project's own .gitignore (written to .git/info/exclude on connect).
_EXCLUDES = ["target/", "logs/", "dbt_packages/", ".duckrun_spill/", "*.duckdb", "*.duckdb.wal", ".user.yml"]
_BRANCH = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,100}$")




class GitError(Exception):
    """A refusal or failure with a message that is safe to show (the token is scrubbed)."""


MAX_FILE_BYTES = 5 * 1024 * 1024


class Repo:
    def __init__(self, kind: str):
        self.kind = kind
        if kind == "dbt":
            self.prefix, self.audit_object, self.excludes = "GIT_", "dbt_project", _EXCLUDES
        else:
            self.prefix, self.audit_object, self.excludes = "NB_GIT_", "notebooks_shared", [".ipynb_checkpoints/", "__pycache__/", "*.pyc"]

    # ---- configuration (read on every call: the environment can change under a running process)
    def dir(self) -> str:
        if self.kind == "dbt":
            return os.getenv("DBT_PROJECT_DIR", "/workspace/dbt_project")
        return os.path.join(os.getenv("NOTEBOOKS_DIR", "/workspace/notebooks"), "Shared")

    def _var(self, name: str, default: str = "") -> str:
        if self.kind == "dbt":
            return (os.getenv(f"GIT_{name}") or default).strip()
        return (os.getenv(f"NOTEBOOKS_GIT_{name}") or (os.getenv(f"GIT_{name}") if name == "TOKEN" else "") or default).strip()

    def remote_url(self) -> str:
        return self._var("REMOTE_URL")

    def token(self) -> str:
        return self._var("TOKEN")

    def branch(self) -> str:
        b = self._var("BRANCH", "main")
        if not _BRANCH.match(b) or ".." in b:
            raise GitError("The branch name is not valid.")
        return b

    def configured(self) -> bool:
        return bool(self.remote_url())

    def mode(self) -> str:
        m = self._var("MODE", "direct").lower()
        if m not in ("direct", "pull_request"):
            raise GitError("The mode must be 'direct' or 'pull_request'.")
        return m

    def pr_mode(self) -> bool:
        try:
            return self.mode() == "pull_request"
        except GitError:
            return False

    def forge(self) -> str:
        """gitea | github | gitlab: `GIT_FORGE` (or `NOTEBOOKS_GIT_FORGE`), else guessed from the host name (github.com / *github* -> github, *gitlab* -> gitlab)."""
        f = self._var("FORGE").lower()
        if f:
            if f not in ("gitea", "github", "gitlab"):
                raise GitError("The forge must be gitea, github or gitlab.")
            return f
        host = (urlparse(self.remote_url()).hostname or "").lower()
        if host == "github.com" or host.startswith("github.") or ".github." in host:
            return "github"
        if host == "gitlab.com" or "gitlab" in host:
            return "gitlab"
        return "gitea"

    def _repo_ref(self) -> Tuple[str, str, str]:
        """(prefix, owner, repo) from the remote URL: http(s)://host[/prefix]/owner/repo(.git)."""
        parts = [p for p in urlparse(self.remote_url()).path.split("/") if p]
        if len(parts) < 2:
            raise GitError("The remote URL must look like http(s)://host/owner/repo.git.")
        repo = parts[-1][:-4] if parts[-1].endswith(".git") else parts[-1]
        return "/".join(parts[:-2]), parts[-2], repo

    def api_base(self) -> str:
        explicit = (os.getenv("GIT_API_URL" if self.kind == "dbt" else "NOTEBOOKS_GIT_API_URL") or "").strip()
        if explicit:
            return explicit.rstrip("/")
        u = urlparse(self.remote_url())
        f = self.forge()
        if f == "github":
            return "https://api.github.com" if (u.hostname or "").lower() == "github.com" else f"{u.scheme}://{u.netloc}/api/v3"
        if f == "gitlab":
            return f"{u.scheme}://{u.netloc}/api/v4"
        prefix = self._repo_ref()[0]
        return f"{u.scheme}://{u.netloc}" + (f"/{prefix}" if prefix else "") + "/api/v1"          # Gitea

    def project_path(self) -> str:
        """GitLab: the whole project path (group/subgroup/project) from the remote URL."""
        parts = [p for p in urlparse(self.remote_url()).path.split("/") if p]
        if len(parts) < 2:
            raise GitError("The remote URL must look like http(s)://host/group/project.git.")
        if parts[-1].endswith(".git"):
            parts[-1] = parts[-1][:-4]
        return "/".join(parts)

    def _git_auth_header(self) -> str:
        """How git itself authenticates over HTTP: Gitea takes `token X`; GitHub and GitLab want Basic with a fixed user name and the token as password."""
        import base64
        tok, f = self.token(), self.forge()
        if f == "gitea":
            return f"Authorization: token {tok}"
        user = "x-access-token" if f == "github" else "oauth2"
        return "Authorization: Basic " + base64.b64encode(f"{user}:{tok}".encode()).decode()

    def _check_url(self) -> None:
        u = urlparse(self.remote_url())
        if u.scheme not in ("http", "https") or not u.hostname:
            raise GitError("The remote URL must be an http(s) URL.")
        if u.username or u.password:
            raise GitError("The remote URL must not contain credentials; put the token in its own variable.")

    def _scrub(self, text: str) -> str:
        import base64
        tok = self.token()
        if not tok:
            return text
        text = text.replace(tok, "***")
        for user in ("x-access-token", "oauth2"):
            text = text.replace(base64.b64encode(f"{user}:{tok}".encode()).decode(), "***")
        return text

    def _env(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        env = {k: v for k, v in os.environ.items()
               if (not k.startswith("GIT_") or k in ("GIT_SSL_CAINFO", "GIT_SSL_NO_VERIFY")) and "GIT_TOKEN" not in k}
        env.update({"GIT_TERMINAL_PROMPT": "0", "GIT_ALLOW_PROTOCOL": "http:https", "GIT_CONFIG_NOSYSTEM": "1", "GIT_ASKPASS": "true",
                    "HOME": os.getenv("GIT_HOME", "/tmp"), "LC_ALL": "C"})
        if self.token() and self.remote_url():
            host = urlparse(self.remote_url())
            env.update({"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": f"http.{host.scheme}://{host.netloc}/.extraHeader",
                        "GIT_CONFIG_VALUE_0": self._git_auth_header()})
        if extra:
            env.update(extra)
        return env

    def _git(self, args: List[str], check: bool = True, extra_env: Optional[Dict[str, str]] = None) -> str:
        try:
            r = subprocess.run(["git", *args], cwd=self.dir(), capture_output=True, text=True, timeout=_TIMEOUT, env=self._env(extra_env))
        except subprocess.TimeoutExpired:
            raise GitError(f"git {args[0]} timed out after {_TIMEOUT}s.")
        except FileNotFoundError:
            raise GitError("git is not installed in this image.")
        out = self._scrub((r.stdout or "") + (r.stderr or "")).strip()
        if check and r.returncode != 0:
            raise GitError(out[-800:] or f"git {args[0]} failed.")
        return out if check else f"{r.returncode}\n{out}"

    def own_repo(self) -> bool:
        d = self.dir()
        if not os.path.isdir(os.path.join(d, ".git")):
            return False
        try:
            top = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=d, capture_output=True, text=True, timeout=10, env=self._env()).stdout.strip()
        except Exception:
            return False
        return bool(top) and os.path.realpath(top) == os.path.realpath(d)

    def _refuse_during_merge(self) -> None:
        from web import git_review
        if git_review.merge_state(self)["in_progress"]:
            raise GitError("A merge is in progress. Resolve its conflicts and finish the merge, or abort it, first.")

    def _require_repo(self) -> None:
        if not self.configured():
            raise GitError("Git sync is not configured (set the remote URL).")
        self._check_url()
        if not self.own_repo():
            raise GitError("This folder is not connected to its repository yet. Connect it first.")

    def _head(self) -> Optional[str]:
        code, _, sha = self._git(["rev-parse", "--verify", "-q", "HEAD"], check=False).partition("\n")
        return sha.strip() if code == "0" else None

    def head_sha(self) -> Optional[str]:
        try:
            return self._head() if self.own_repo() else None
        except Exception:
            return None

    def _audit(self, actor: str, action: str, detail: Dict[str, Any]) -> None:
        try:
            from web.governance import store
            store.init_governance_db()
            conn = store.get_db()
            try:
                store.write_audit(conn, actor, self.prefix + action, self.audit_object, detail)
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(f"could not audit {action}: {exc}")

    def _changes(self) -> List[Dict[str, str]]:
        out = self._git(["status", "--porcelain=v1", "-uall"])
        return [{"status": l[:2].strip() or "?", "path": l[3:]} for l in out.splitlines() if l.strip()]

    # ---- validation of what is about to be shared / was just received
    def _problem_with_tree(self) -> Optional[str]:
        """dbt: `dbt parse`. Notebooks: every .ipynb must be a notebook JSON (a pulled half-file would break the runner)."""
        d = self.dir()
        if self.kind == "dbt":
            from web import dbt_config
            p = os.path.join(d, "dbt_project.yml")
            if not os.path.isfile(p):
                return "The pulled project has no dbt_project.yml."
            return dbt_config._dbt_parse("dbt_project.yml", open(p, encoding="utf-8").read())
        for root, dirs, files in os.walk(d):
            dirs[:] = [x for x in dirs if x != ".git"]
            for fn in files:
                if fn.endswith(".ipynb"):
                    try:
                        with open(os.path.join(root, fn), encoding="utf-8") as f:
                            if not isinstance(json.load(f).get("cells"), list):
                                raise ValueError("no cells")
                    except Exception:
                        return f"{os.path.relpath(os.path.join(root, fn), d)} is not a valid notebook."
        return None

    def _problems_before_commit(self) -> List[str]:
        found: List[str] = []
        d = self.dir()
        if self.kind == "dbt":
            from web import dbt_config
            for name in dbt_config.FILES:
                p = os.path.join(d, name)
                if os.path.isfile(p):
                    with open(p, encoding="utf-8") as f:
                        found += [f"{name}: {w}" for w in dbt_config.secret_warnings(f.read())]
        for c in self._changes():
            p = os.path.join(d, c["path"].split(" -> ")[-1])
            if os.path.isfile(p) and os.path.getsize(p) > MAX_FILE_BYTES:
                found.append(f"{c['path']} is larger than {MAX_FILE_BYTES // (1024 * 1024)} MB (data does not belong in git)")
        return found

    # ---- branches and the forge
    def current_branch(self) -> Optional[str]:
        code, _, name = self._git(["symbolic-ref", "--short", "-q", "HEAD"], check=False).partition("\n")
        return name.strip() if code == "0" and name.strip() else None

    def _has_ref(self, ref: str) -> bool:
        return self._git(["rev-parse", "--verify", "-q", ref], check=False).startswith("0")

    def _count(self, rng: str) -> int:
        out = self._git(["rev-list", "--count", rng], check=False)
        code, _, n = out.partition("\n")
        return int(n) if code == "0" and n.strip().isdigit() else 0

    def _api(self, method: str, path: str, body: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None) -> Any:
        """One call to the forge's REST API (Gitea, GitHub or GitLab) with the repository token. `path` is relative to the repository / project.
        Errors are turned into short, token-free messages."""
        import requests
        from urllib.parse import quote
        if not self.token():
            raise GitError("A token is needed to talk to the repository server (set the token variable).")
        f = self.forge()
        if f == "gitlab":
            url = f"{self.api_base()}/projects/{quote(self.project_path(), safe='')}{path}"
            headers = {"PRIVATE-TOKEN": self.token()}
        else:
            owner_repo = self._repo_ref()
            url = f"{self.api_base()}/repos/{owner_repo[1]}/{owner_repo[2]}{path}"
            headers = {"Authorization": (f"Bearer {self.token()}" if f == "github" else f"token {self.token()}")}
            if f == "github":
                headers.update({"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"})
        headers.update({"Accept": headers.get("Accept", "application/json"), "User-Agent": "DataKilnWorks-Git/1"})
        try:
            r = requests.request(method, url, json=body, params=params, timeout=20, allow_redirects=False, headers=headers)
        except requests.RequestException as exc:
            raise GitError(f"The repository server could not be reached ({type(exc).__name__}).")

        def server_message() -> str:
            try:
                j = r.json()
                return str(j.get("message") or j.get("error") or "")[:300] if isinstance(j, dict) else ""
            except ValueError:
                return ""
        scope = {"gitea": "write:repository", "github": "'repo' (or Pull requests: write)", "gitlab": "'api'"}[f]
        if r.status_code in (401, 403):
            raise GitError(self._scrub(f"The repository server refused the token (HTTP {r.status_code}); pull requests need the {scope} scope. {server_message()}".strip()))
        if r.status_code == 404:
            raise GitError("The repository or pull request was not found (check the URL and that the token can see the repository).")
        if r.status_code >= 400:
            raise GitError(self._scrub(f"The repository server answered HTTP {r.status_code}: {server_message()}".strip()))
        try:
            return r.json()
        except ValueError:
            return {}

    def _pr_view(self, p: Dict[str, Any]) -> Dict[str, Any]:
        """One normalised pull / merge request from any forge."""
        if self.forge() == "gitlab":
            state = p.get("state")
            return {"number": p.get("iid"), "url": p.get("web_url"), "title": p.get("title"), "state": "open" if state == "opened" else "closed",
                    "merged": state == "merged", "mergeable": (p.get("detailed_merge_status") == "mergeable") if p.get("detailed_merge_status") else (p.get("merge_status") == "can_be_merged")}
        return {"number": p.get("number"), "url": p.get("html_url"), "title": p.get("title"), "state": p.get("state"),
                "merged": bool(p.get("merged") or p.get("merged_at")), "mergeable": p.get("mergeable")}

    def _pr_for_branch(self, branch: str) -> Optional[Dict[str, Any]]:
        """The open pull request from `branch`, else the most recent one, else None."""
        f = self.forge()
        if f == "gitlab":
            raw = self._api("GET", "/merge_requests", params={"state": "all", "source_branch": branch, "target_branch": self.branch(), "order_by": "updated_at", "sort": "desc", "per_page": 50}) or []
            prs = [p for p in raw if p.get("source_branch") == branch and p.get("target_branch") == self.branch()]
        elif f == "github":
            raw = self._api("GET", "/pulls", params={"state": "all", "sort": "updated", "direction": "desc", "per_page": 50, "head": f"{self._repo_ref()[1]}:{branch}"}) or []
            prs = [p for p in raw if (p.get("head") or {}).get("ref") == branch and (p.get("base") or {}).get("ref") == self.branch()]
        else:
            raw = self._api("GET", "/pulls", params={"state": "all", "sort": "recentupdate", "limit": 50}) or []
            prs = [p for p in raw if (p.get("head") or {}).get("ref") == branch and (p.get("base") or {}).get("ref") == self.branch()]
        if not prs:
            return None
        open_ones = [p for p in prs if (p.get("state") in ("open", "opened"))]
        return self._pr_view((open_ones or prs)[0])

    def _pr_create(self, head: str, base: str, title: str, body: str) -> Dict[str, Any]:
        if self.forge() == "gitlab":
            return self._pr_view(self._api("POST", "/merge_requests", body={"source_branch": head, "target_branch": base, "title": title, "description": body, "remove_source_branch": False}))
        return self._pr_view(self._api("POST", "/pulls", body={"head": head, "base": base, "title": title, "body": body}))

    def _change_name(self, actor: str, name: Optional[str] = None) -> str:
        if name:
            name = name.strip()
            if not _BRANCH.match(name) or ".." in name or name.endswith((".lock", "/")) or name == self.branch():
                raise GitError("That is not a usable branch name.")
            return name
        who = re.sub(r"[^a-z0-9]+", "-", (actor or "user").lower()).strip("-")[:30] or "user"
        return f"dkw/{who}-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    # ---- operations
    def status(self, fetch: bool = False) -> Dict[str, Any]:
        with _LOCK:
            info: Dict[str, Any] = {"configured": self.configured(), "remote": self.remote_url() or None, "branch": None, "connected": False,
                                    "token_set": bool(self.token()), "head": None, "changes": [], "dirty": False, "ahead": 0, "behind": 0,
                                    "unpushed": 0, "log": [], "mode": "direct", "forge": None, "merge": {"in_progress": False, "conflicts": []}, "current_branch": None, "on_base": True,
                                    "remote_has_base": False, "pull_request": None}
            if not self.configured():
                return info
            try:
                info["branch"] = self.branch()
                info["mode"] = self.mode()
                info["forge"] = self.forge()
                self._check_url()
            except GitError as exc:
                info["error"] = str(exc)
                return info
            if not self.own_repo():
                info["reason"] = "This folder is not its own git repository yet."
                return info
            info["connected"] = True
            b = self.branch()
            if fetch:
                try:
                    self._git(["fetch", "origin", "--prune"])
                except GitError as exc:
                    info["fetch_error"] = str(exc)
            info["head"] = self._head()
            from web import git_review
            info["merge"] = git_review.merge_state(self)
            info["changes"] = self._changes()
            info["dirty"] = bool(info["changes"])
            cur = self.current_branch()
            info["current_branch"], info["on_base"] = cur, (cur == b or cur is None)
            ref = f"refs/remotes/origin/{b}"
            info["remote_has_base"] = self._has_ref(ref)
            if info["remote_has_base"]:
                counts = self._git(["rev-list", "--left-right", "--count", f"HEAD...{ref}"], check=False).split("\n", 1)[-1].split()
                if len(counts) == 2 and info["head"]:
                    info["ahead"], info["behind"] = int(counts[0]), int(counts[1])
                elif not info["head"]:
                    info["behind"] = int(self._git(["rev-list", "--count", ref]))
                info["remote_head"] = self._git(["rev-parse", ref])
            elif info["head"]:
                info["ahead"] = int(self._git(["rev-list", "--count", "HEAD"]))     # the remote has no such branch yet
            # Commits of the current branch the remote does not have yet (what Push would send).
            if info["on_base"] or not cur:
                info["unpushed"] = info["ahead"]
            elif self._has_ref(f"refs/remotes/origin/{cur}"):
                info["unpushed"] = self._count(f"origin/{cur}..HEAD")
            else:
                info["unpushed"] = info["ahead"]
            log = self._git(["log", "-n", "10", "--format=%H%x1f%an%x1f%aI%x1f%s"], check=False).split("\n", 1)[-1]
            info["log"] = [dict(zip(("sha", "author", "date", "subject"), l.split("\x1f"))) for l in log.splitlines() if "\x1f" in l]
            if info["mode"] == "pull_request" and not info["on_base"] and cur and self.token() and info["remote_has_base"]:
                try:
                    info["pull_request"] = self._pr_for_branch(cur)
                except GitError as exc:
                    info["pr_error"] = str(exc)
            return info

    def connect(self, actor: str) -> Dict[str, Any]:
        """Makes the folder a checkout of the remote branch. Existing files stay (they show as local changes); nothing is overwritten."""
        with _LOCK:
            if not self.configured():
                raise GitError("Git sync is not configured (set the remote URL).")
            self._check_url()
            b, d = self.branch(), self.dir()
            if not os.path.isdir(d):
                if self.kind == "dbt":
                    raise GitError("The project directory does not exist.")
                os.makedirs(d, exist_ok=True)
            if self.own_repo():
                self._git(["remote", "set-url", "origin", self.remote_url()])
                return self.status(fetch=True)
            if os.path.exists(os.path.join(d, ".git")):
                raise GitError("A .git entry exists but it is not a usable repository of its own.")
            self._git(["init", "-q", "-b", b])
            self._git(["remote", "add", "origin", self.remote_url()])
            with open(os.path.join(d, ".git", "info", "exclude"), "a", encoding="utf-8") as f:
                f.write("\n# build artefacts (added by the studio)\n" + "\n".join(self.excludes) + "\n")
            if self.kind == "notebooks":
                # Outputs never reach the repository: the clean filter runs at `git add`/`git status`, the files on disk are untouched.
                strip = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nb_strip.py")
                with open(os.path.join(d, ".git", "info", "attributes"), "a", encoding="utf-8") as f:
                    f.write("*.ipynb filter=dkwstrip\n")
                self._git(["config", "filter.dkwstrip.clean", f"{sys.executable} {strip}"])
                self._git(["config", "filter.dkwstrip.smudge", "cat"])
            self._git(["config", "user.name", "Data Kiln Works"])
            self._git(["config", "user.email", "dkw@localhost"])
            self._git(["config", "pull.ff", "only"])
            try:
                self._git(["fetch", "origin", b])
                remote_has_branch = True
            except GitError as exc:
                if "couldn't find remote ref" in str(exc).lower():
                    remote_has_branch = False                     # empty remote: the first commit will create the branch
                else:
                    shutil.rmtree(os.path.join(d, ".git"), ignore_errors=True)   # leave no half-connected state
                    raise
            if remote_has_branch:
                self._git(["reset", "-q", "--mixed", f"origin/{b}"])   # adopt the remote history, keep every local file as a change
                self._git(["branch", "--set-upstream-to", f"origin/{b}"], check=False)
            self._audit(actor, "CONNECT", {"remote": self.remote_url(), "branch": b})
            return self.status()

    def pull(self, actor: str) -> Dict[str, Any]:
        with _LOCK:
            self._require_repo()
            self._refuse_during_merge()
            b = self.branch()
            if self.pr_mode() and self.current_branch() not in (None, b):
                raise GitError(f"Pull updates '{b}'. You are on the change branch '{self.current_branch()}': use 'Update from {b}' "
                               f"to bring it in, or finish the change once its pull request is merged.")
            if self._changes():
                raise GitError("There are uncommitted local changes. Commit them (or discard them) before pulling.")
            before = self._head()
            self._git(["fetch", "origin", b])
            ref = f"origin/{b}"
            if before is None:
                self._git(["reset", "-q", "--hard", ref])
            else:
                if self._git(["merge-base", "--is-ancestor", ref, "HEAD"], check=False).startswith("0"):
                    return {**self.status(), "pulled": False, "message": "Already up to date."}
                if not self._git(["merge-base", "--is-ancestor", "HEAD", ref], check=False).startswith("0"):
                    raise GitError("Local and remote history have diverged, so pulling would need a merge. Use 'Merge remote changes' "
                                   "(conflicts, if any, are resolved in the studio).")
                self._git(["merge", "--ff-only", ref])
            after = self._head()
            problem = self._problem_with_tree()
            if problem:
                if before:
                    self._git(["reset", "-q", "--hard", before])
                else:
                    self._git(["update-ref", "-d", "HEAD"], check=False)
                self._audit(actor, "PULL_REJECTED", {"to": after, "reason": problem[:300]})
                raise GitError("The pulled content was rejected, so the pull was rolled back:\n" + problem)
            self._audit(actor, "PULL", {"from": before, "to": after})
            return {**self.status(), "pulled": True, "message": f"Updated to {after[:8]}."}

    def commit(self, actor: str, message: str, branch: Optional[str] = None) -> Dict[str, Any]:
        with _LOCK:
            self._require_repo()
            self._refuse_during_merge()
            message = (message or "").strip()
            if not message:
                raise GitError("A commit message is required.")
            if len(message) > 2000:
                raise GitError("The commit message is too long.")
            if not self._changes():
                raise GitError("Nothing to commit.")
            problems = self._problems_before_commit()
            if problems:
                raise GitError("Refusing to commit:\n" + "\n".join(problems))
            author = re.sub(r"[^A-Za-z0-9._-]", "_", actor or "unknown")[:60] or "unknown"
            ident = {"GIT_AUTHOR_NAME": author, "GIT_AUTHOR_EMAIL": f"{author}@datakilnworks.local",
                     "GIT_COMMITTER_NAME": "Data Kiln Works", "GIT_COMMITTER_EMAIL": "dkw@localhost"}
            started = None
            if self.pr_mode() and self.current_branch() in (None, self.branch()) and self._has_ref(f"refs/remotes/origin/{self.branch()}") and self._head():
                started = self._start_change(actor, branch)        # the base branch is never committed to in pull-request mode
            self._git(["add", "-A"])
            self._git(["commit", "-q", "-m", message], extra_env=ident)
            sha = self._head()
            self._audit(actor, "COMMIT", {"sha": sha, "message": message[:200], "branch": self.current_branch()})
            return {**self.status(), "committed": sha, **({"started_branch": started} if started else {})}

    def push(self, actor: str) -> Dict[str, Any]:
        with _LOCK:
            self._require_repo()
            self._refuse_during_merge()
            b = self.branch()
            if not self._head():
                raise GitError("Nothing to push: there are no commits yet.")
            target = b
            if self.pr_mode():
                cur = self.current_branch()
                if cur in (None, b):
                    if self._has_ref(f"refs/remotes/origin/{b}"):
                        raise GitError(f"In pull-request mode '{b}' is only changed through a reviewed pull request. Commit your changes "
                                       "(that opens a change branch) and push that.")
                else:
                    target = cur                                    # a change branch; the very first commit of an empty remote may go to the base
            try:
                self._git(["push", "-u", "origin", f"HEAD:refs/heads/{target}"])
            except GitError as exc:
                if any(w in str(exc) for w in ("rejected", "non-fast-forward", "fetch first")):
                    raise GitError("The remote has commits you do not have. Pull first (history is never force-pushed).")
                raise
            self._git(["fetch", "origin", target], check=False)
            self._audit(actor, "PUSH", {"sha": self._head(), "branch": target})
            return {**self.status(), "pushed": True, "pushed_branch": target}

    # ---- pull-request mode
    def _require_pr_mode(self) -> None:
        self._require_repo()
        self._refuse_during_merge()
        if not self.pr_mode():
            raise GitError("Branch and pull-request mode is off. Set the mode variable to pull_request to use it.")

    def _start_change(self, actor: str, name: Optional[str] = None) -> str:
        """Creates and switches to a change branch from the current HEAD (working changes come along). Caller holds the lock."""
        b = self.branch()
        cur = self.current_branch()
        if cur not in (None, b):
            raise GitError(f"You are already on the change branch '{cur}'. Open its pull request, or finish it, before starting another.")
        if not self._head():
            raise GitError("There is nothing to branch from yet: make the first commit (it goes to the base branch).")
        branch = self._change_name(actor, name)
        if self._has_ref(f"refs/heads/{branch}") or self._has_ref(f"refs/remotes/origin/{branch}"):
            raise GitError(f"The branch '{branch}' already exists.")
        self._git(["switch", "-c", branch])
        self._audit(actor, "BRANCH_CREATE", {"branch": branch, "from": self._head()})
        return branch

    def start_change(self, actor: str, name: Optional[str] = None) -> Dict[str, Any]:
        with _LOCK:
            self._require_pr_mode()
            b = self.branch()
            if not self._changes() and self._has_ref(f"refs/remotes/origin/{b}") and self.current_branch() in (None, b):
                try:                                              # a clean base that is behind: start from the latest
                    self._git(["fetch", "origin", b])
                    if self._count(f"HEAD..origin/{b}"):
                        self.pull(actor)
                except GitError:
                    pass
            return {**self.status(), "started_branch": self._start_change(actor, name)}

    def open_pull_request(self, actor: str, title: str = "", body: str = "") -> Dict[str, Any]:
        with _LOCK:
            self._require_pr_mode()
            b = self.branch()
            cur = self.current_branch()
            if cur in (None, b):
                raise GitError(f"You are on '{b}': there is no change to propose. Commit your changes first (that opens a change branch).")
            if self._changes():
                raise GitError("There are uncommitted changes. Commit them before opening the pull request.")
            self._git(["fetch", "origin", "--prune"], check=False)
            if not self._has_ref(f"refs/remotes/origin/{cur}") or self._count(f"origin/{cur}..HEAD"):
                raise GitError("The branch has commits the server does not have. Push first.")
            existing = self._pr_for_branch(cur)
            if existing and existing["state"] == "open":
                return {**self.status(), "pull_request": existing, "message": f"Pull request #{existing['number']} is already open."}
            problem = self._problem_with_tree()
            if problem:
                raise GitError("The change does not validate, so no pull request was opened:\n" + problem)
            subject = (title or "").strip() or self._git(["log", "-1", "--format=%s"], check=False).split("\n", 1)[-1].strip() or cur
            pr = self._pr_create(cur, b, subject[:250], (body or "")[:20000])
            self._audit(actor, "PR_OPEN", {"branch": cur, "number": pr["number"], "url": pr["url"]})
            return {**self.status(), "pull_request": pr, "message": f"Opened pull request #{pr['number']}."}

    def finish_change(self, actor: str) -> Dict[str, Any]:
        """Back to the base branch after the change was merged there (on the forge); drops the local change branch."""
        with _LOCK:
            self._require_pr_mode()
            b = self.branch()
            cur = self.current_branch()
            if cur in (None, b):
                raise GitError(f"You are already on '{b}'.")
            if self._changes():
                raise GitError("There are uncommitted changes on this branch. Commit or discard them first.")
            self._git(["fetch", "origin", "--prune"])
            merged = self._git(["merge-base", "--is-ancestor", "HEAD", f"origin/{b}"], check=False).startswith("0")
            if not merged:
                try:
                    pr = self._pr_for_branch(cur)
                except GitError:
                    pr = None
                merged = bool(pr and pr["merged"])
            if not merged:
                raise GitError("The change is not merged into '" + b + "' yet. Merge its pull request on the server first (or use Abandon to leave it).")
            self._git(["switch", b])
            result = self.pull(actor)
            self._git(["branch", "-D", cur], check=False)
            self._audit(actor, "CHANGE_FINISH", {"branch": cur})
            return {**result, "finished_branch": cur}

    def abandon_change(self, actor: str) -> Dict[str, Any]:
        """Leaves the change branch without merging. Refused while anything would be lost: uncommitted changes or unpushed commits."""
        with _LOCK:
            self._require_pr_mode()
            b = self.branch()
            cur = self.current_branch()
            if cur in (None, b):
                raise GitError(f"You are already on '{b}'.")
            if self._changes():
                raise GitError("There are uncommitted changes on this branch; leaving would carry them to the base branch. Commit or discard them first.")
            self._git(["fetch", "origin", "--prune"], check=False)
            if not self._has_ref(f"refs/remotes/origin/{cur}") or self._count(f"origin/{cur}..HEAD"):
                raise GitError("The branch has commits that were never pushed; leaving would lose them. Push it first.")
            self._git(["switch", b])
            self._git(["branch", "-D", cur], check=False)
            self._audit(actor, "CHANGE_ABANDON", {"branch": cur})
            return {**self.status(fetch=True), "abandoned_branch": cur, "message": f"Left '{cur}' (it stays on the server)."}

    def update_from_base(self, actor: str) -> Dict[str, Any]:
        """Merges the base branch into the change branch. A conflict is aborted cleanly, never left half-merged."""
        with _LOCK:
            self._require_pr_mode()
            b = self.branch()
            cur = self.current_branch()
            if cur in (None, b):
                raise GitError(f"You are on '{b}'; use Pull there.")
            if self._changes():
                raise GitError("There are uncommitted changes. Commit them first.")
            self._git(["fetch", "origin", b])
            if not self._count(f"HEAD..origin/{b}"):
                return {**self.status(), "message": f"Already contains everything on '{b}'."}
            before = self._head()
            ident = {"GIT_AUTHOR_NAME": "Data Kiln Works", "GIT_AUTHOR_EMAIL": "dkw@localhost", "GIT_COMMITTER_NAME": "Data Kiln Works", "GIT_COMMITTER_EMAIL": "dkw@localhost"}
            try:
                self._git(["merge", "--no-edit", "-m", f"Merge {b} into {cur}", f"origin/{b}"], extra_env=ident)
            except GitError as exc:
                from web import git_review
                state = git_review.merge_state(self)
                if state["in_progress"] and state["conflicts"]:
                    self._audit(actor, "MERGE_CONFLICT", {"branch": cur, "from": b, "files": [c["path"] for c in state["conflicts"]][:20]})
                    return {**self.status(), "conflicts": state["conflicts"],
                            "message": f"Merging '{b}' into '{cur}' conflicts in {len(state['conflicts'])} file(s). The merge is waiting for you: resolve each file, then finish it (or abort)."}
                self._git(["merge", "--abort"], check=False)
                raise GitError(f"Merging '{b}' into '{cur}' failed, so nothing was changed. ({str(exc)[:200]})")
            problem = self._problem_with_tree()
            if problem:
                self._git(["reset", "-q", "--hard", before])
                raise GitError(f"The merged result does not validate, so the merge was undone:\n{problem}")
            self._audit(actor, "BRANCH_UPDATE", {"branch": cur, "from": b})
            return {**self.status(), "message": f"Merged '{b}' into '{cur}'."}

    def merge_remote(self, actor: str) -> Dict[str, Any]:
        """Direct mode: the local branch and the remote both moved (a pull would have to merge). Merges the remote into the local branch; conflicts are
        left in progress for the resolver, exactly as with 'Update from base'."""
        with _LOCK:
            self._require_repo()
            self._refuse_during_merge()
            b = self.branch()
            cur = self.current_branch()
            if self.pr_mode() and cur not in (None, b):
                raise GitError(f"You are on the change branch '{cur}': use 'Update from {b}'.")
            if self._changes():
                raise GitError("There are uncommitted local changes. Commit them (or discard them) first.")
            self._git(["fetch", "origin", b])
            ref = f"origin/{b}"
            if not self._head() or not self._has_ref(f"refs/remotes/{ref}"):
                raise GitError("There is nothing to merge yet.")
            if not self._count(f"HEAD..{ref}"):
                return {**self.status(), "message": "Already up to date."}
            if not self._count(f"{ref}..HEAD"):
                raise GitError("Your branch has no commits of its own: use Pull.")
            before = self._head()
            ident = {"GIT_AUTHOR_NAME": "Data Kiln Works", "GIT_AUTHOR_EMAIL": "dkw@localhost", "GIT_COMMITTER_NAME": "Data Kiln Works", "GIT_COMMITTER_EMAIL": "dkw@localhost"}
            try:
                self._git(["merge", "--no-edit", "-m", f"Merge {ref} into {cur or b}", ref], extra_env=ident)
            except GitError as exc:
                from web import git_review
                state = git_review.merge_state(self)
                if state["in_progress"] and state["conflicts"]:
                    self._audit(actor, "MERGE_CONFLICT", {"branch": cur or b, "from": ref, "files": [c["path"] for c in state["conflicts"]][:20]})
                    return {**self.status(), "conflicts": state["conflicts"],
                            "message": f"Merging {ref} conflicts in {len(state['conflicts'])} file(s). The merge is waiting for you: resolve each file, then finish it (or abort)."}
                self._git(["merge", "--abort"], check=False)
                raise GitError(f"Merging {ref} failed, so nothing was changed. ({str(exc)[:200]})")
            problem = self._problem_with_tree()
            if problem:
                self._git(["reset", "-q", "--hard", before])
                raise GitError(f"The merged result does not validate, so the merge was undone:\n{problem}")
            self._audit(actor, "MERGE_REMOTE", {"branch": cur or b})
            return {**self.status(), "message": f"Merged {ref} into your branch."}

    # ---- review (web/git_review.py): diffs, discarding one file, resolving a merge
    def diff(self, scope: str = "pending") -> Dict[str, Any]:
        from web import git_review
        with _LOCK:
            return git_review.summary(self, scope)

    def diff_file(self, path: str, scope: str = "pending") -> Dict[str, Any]:
        from web import git_review
        with _LOCK:
            return git_review.file_diff(self, path, scope)

    def discard(self, actor: str, path: str) -> Dict[str, Any]:
        from web import git_review
        return git_review.discard(self, path, actor)

    def conflicts(self) -> Dict[str, Any]:
        from web import git_review
        with _LOCK:
            self._require_repo()
            return git_review.merge_state(self)

    def conflict_file(self, path: str) -> Dict[str, Any]:
        from web import git_review
        with _LOCK:
            return git_review.conflict_detail(self, path)

    def resolve_conflict(self, actor: str, path: str, resolution: str, choices: Optional[List[Dict[str, Any]]] = None, content: Optional[str] = None) -> Dict[str, Any]:
        from web import git_review
        return git_review.resolve(self, path, resolution, choices, content, actor)

    def finish_merge(self, actor: str, message: Optional[str] = None) -> Dict[str, Any]:
        from web import git_review
        return git_review.finish_merge(self, actor, message)

    def abort_merge(self, actor: str) -> Dict[str, Any]:
        from web import git_review
        return git_review.abort_merge(self, actor)


DBT = Repo("dbt")
NOTEBOOKS = Repo("notebooks")


def get(kind: str) -> Repo:
    if kind == "dbt":
        return DBT
    if kind == "notebooks":
        return NOTEBOOKS
    raise GitError("Unknown repository.")


# dbt is the original user; these keep its module-level API.
def head_sha() -> Optional[str]:
    """The commit the dbt project is at (None if it is not its own repository); recorded on every dbt run."""
    return DBT.head_sha()


def status(fetch: bool = False): return DBT.status(fetch)
def connect(actor: str): return DBT.connect(actor)
def pull(actor: str): return DBT.pull(actor)
def commit(actor: str, message: str): return DBT.commit(actor, message)
def push(actor: str): return DBT.push(actor)
