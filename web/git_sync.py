"""Git sync for the dbt project (phase 1: status, connect, pull, commit, push).

The project directory (`DBT_PROJECT_DIR`) is synchronised with a remote repository (a self-hosted Gitea in the reference
deployment, but this is plain git over HTTP(S), so any server works). Rules that keep this safe:

* The project directory must be **its own repository**. In development it sits inside the platform repository, and running
  git there would commit the platform's files; every operation refuses unless the directory is the top level of its own repo.
* Configuration is environment only (a Kubernetes Secret in production): `GIT_REMOTE_URL` (http/https, no credentials in it),
  `GIT_TOKEN`, `GIT_BRANCH` (default `main`). The token is handed to git through `GIT_CONFIG_*` environment variables (never
  argv, never the URL, never written to `.git/config`) and is scrubbed from every message returned or logged.
* Pulls are fast-forward only and refused on a dirty tree, so nothing is merged or overwritten. The pulled project is then checked
  with `dbt parse`; if dbt rejects it the pull is rolled back to the previous commit.
* Commits are refused while a project file holds a plaintext credential, are authored as the acting user, and are audited.
* One lock serialises every operation (the project is a single working tree, so run a single replica).
"""
import datetime
import logging
import os
import re
import subprocess
import threading
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger("localspark.git_sync")

_LOCK = threading.RLock()
_TIMEOUT = 60
# Kept out of the repository without touching the project's own .gitignore (written to .git/info/exclude on connect).
_EXCLUDES = ["target/", "logs/", "dbt_packages/", ".duckrun_spill/", "*.duckdb", "*.duckdb.wal", ".user.yml"]
_BRANCH = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,100}$")


class GitError(Exception):
    """A refusal or failure with a message that is safe to show (the token is scrubbed)."""


def _project_dir() -> str:
    return os.getenv("DBT_PROJECT_DIR", "/workspace/dbt_project")


def remote_url() -> str:
    return (os.getenv("GIT_REMOTE_URL") or "").strip()


def branch() -> str:
    b = (os.getenv("GIT_BRANCH") or "main").strip()
    if not _BRANCH.match(b) or ".." in b:
        raise GitError("GIT_BRANCH is not a valid branch name.")
    return b


def configured() -> bool:
    return bool(remote_url())


def _check_url(url: str) -> None:
    u = urlparse(url)
    if u.scheme not in ("http", "https") or not u.hostname:
        raise GitError("GIT_REMOTE_URL must be an http(s) URL.")
    if u.username or u.password:
        raise GitError("GIT_REMOTE_URL must not contain credentials; put the token in GIT_TOKEN.")


def _scrub(text: str) -> str:
    tok = os.getenv("GIT_TOKEN") or ""
    if tok:
        text = text.replace(tok, "***")
    return text


def _env(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_") or k in ("GIT_SSL_CAINFO", "GIT_SSL_NO_VERIFY")}
    env.update({
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ALLOW_PROTOCOL": "http:https",          # no file:, ext:, ssh: transports
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_ASKPASS": "true",
        "HOME": os.getenv("GIT_HOME", "/tmp"),
        "LC_ALL": "C",
    })
    tok = os.getenv("GIT_TOKEN") or ""
    if tok and remote_url():
        host = urlparse(remote_url())
        env.update({
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": f"http.{host.scheme}://{host.netloc}/.extraHeader",
            "GIT_CONFIG_VALUE_0": f"Authorization: token {tok}",
        })
    if extra:
        env.update(extra)
    return env


def _git(args: List[str], check: bool = True, extra_env: Optional[Dict[str, str]] = None, cwd: Optional[str] = None) -> str:
    try:
        r = subprocess.run(["git", *args], cwd=cwd or _project_dir(), capture_output=True, text=True, timeout=_TIMEOUT, env=_env(extra_env))
    except subprocess.TimeoutExpired:
        raise GitError(f"git {args[0]} timed out after {_TIMEOUT}s.")
    except FileNotFoundError:
        raise GitError("git is not installed in this image.")
    out = _scrub((r.stdout or "") + (r.stderr or "")).strip()
    if check and r.returncode != 0:
        raise GitError(out[-800:] or f"git {args[0]} failed.")
    return out if check else f"{r.returncode}\n{out}"


def _own_repo() -> bool:
    d = _project_dir()
    if not os.path.isdir(os.path.join(d, ".git")):
        return False
    try:
        top = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=d, capture_output=True, text=True, timeout=10, env=_env()).stdout.strip()
    except Exception:
        return False
    return bool(top) and os.path.realpath(top) == os.path.realpath(d)


def _require_repo() -> None:
    if not configured():
        raise GitError("Git sync is not configured (set GIT_REMOTE_URL).")
    _check_url(remote_url())
    if not _own_repo():
        raise GitError("The dbt project is not connected to its repository yet. Connect it first.")


def _head() -> Optional[str]:
    out = _git(["rev-parse", "--verify", "-q", "HEAD"], check=False)
    code, _, sha = out.partition("\n")
    return sha.strip() if code == "0" else None


def head_sha() -> Optional[str]:
    """The commit the project is at (None if it is not its own repository); recorded on every dbt run."""
    try:
        return _head() if _own_repo() else None
    except Exception:
        return None


def _audit(actor: str, action: str, detail: Dict[str, Any]) -> None:
    try:
        from web.governance import store
        store.init_governance_db()
        conn = store.get_db()
        try:
            store.write_audit(conn, actor, action, "dbt_project", detail)
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(f"could not audit {action}: {exc}")


def _changes() -> List[Dict[str, str]]:
    out = _git(["status", "--porcelain=v1", "-uall"])
    return [{"status": l[:2].strip() or "?", "path": l[3:]} for l in out.splitlines() if l.strip()]


# ---------------------------------------------------------------- operations

def status(fetch: bool = False) -> Dict[str, Any]:
    with _LOCK:
        info: Dict[str, Any] = {"configured": configured(), "remote": remote_url() or None, "branch": None, "connected": False,
                                "token_set": bool(os.getenv("GIT_TOKEN")),
                                "head": None, "changes": [], "dirty": False, "ahead": 0, "behind": 0, "log": []}
        if not configured():
            return info
        try:
            info["branch"] = branch()
            _check_url(remote_url())
        except GitError as exc:
            info["error"] = str(exc)
            return info
        if not _own_repo():
            info["reason"] = "The dbt project directory is not its own git repository yet."
            return info
        info["connected"] = True
        if fetch:
            try:
                _git(["fetch", "origin", branch()])
            except GitError as exc:
                info["fetch_error"] = str(exc)
        info["head"] = _head()
        info["changes"] = _changes()
        info["dirty"] = bool(info["changes"])
        info["ahead"] = info["behind"] = 0
        ref = f"refs/remotes/origin/{branch()}"
        if _git(["rev-parse", "--verify", "-q", ref], check=False).startswith("0"):
            counts = _git(["rev-list", "--left-right", "--count", f"HEAD...{ref}"], check=False).split("\n", 1)[-1].split()
            if len(counts) == 2 and _head():
                info["ahead"], info["behind"] = int(counts[0]), int(counts[1])
            elif not _head():
                info["behind"] = int(_git(["rev-list", "--count", ref]))
            info["remote_head"] = _git(["rev-parse", ref])
        elif _head():
            info["ahead"] = int(_git(["rev-list", "--count", "HEAD"]))     # the remote has no such branch yet: everything is unpushed
        log = _git(["log", "-n", "10", "--format=%H%x1f%an%x1f%aI%x1f%s"], check=False).split("\n", 1)[-1]
        info["log"] = [dict(zip(("sha", "author", "date", "subject"), l.split("\x1f"))) for l in log.splitlines() if "\x1f" in l]
        return info


def connect(actor: str) -> Dict[str, Any]:
    """Makes the project directory a checkout of the remote branch. Existing files stay as they are (they show up as local
    changes against the remote); nothing is overwritten, so this is safe to run on a seeded project."""
    with _LOCK:
        if not configured():
            raise GitError("Git sync is not configured (set GIT_REMOTE_URL).")
        _check_url(remote_url())
        b = branch()
        d = _project_dir()
        if not os.path.isdir(d):
            raise GitError("The dbt project directory does not exist.")
        if _own_repo():
            _git(["remote", "set-url", "origin", remote_url()])
            return status(fetch=True)
        if os.path.exists(os.path.join(d, ".git")):
            raise GitError("A .git entry exists but it is not a usable repository of its own.")
        # A directory that lives inside another repository (development) would make git use that one; `git init` here creates our own.
        _git(["init", "-q", "-b", b])
        _git(["remote", "add", "origin", remote_url()])
        with open(os.path.join(d, ".git", "info", "exclude"), "a", encoding="utf-8") as f:
            f.write("\n# dbt build artefacts (added by the studio)\n" + "\n".join(_EXCLUDES) + "\n")
        _git(["config", "user.name", "Data Kiln Works"])
        _git(["config", "user.email", "dkw@localhost"])
        _git(["config", "pull.ff", "only"])
        try:
            _git(["fetch", "origin", b])
            remote_has_branch = True
        except GitError as exc:
            if "couldn't find remote ref" in str(exc).lower():
                remote_has_branch = False                     # empty remote: the first commit will create the branch
            else:
                import shutil
                shutil.rmtree(os.path.join(d, ".git"), ignore_errors=True)   # nothing useful was created: leave no half-connected state
                raise
        if remote_has_branch:
            _git(["reset", "-q", "--mixed", f"origin/{b}"])   # adopt the remote history, keep every local file as a change
            _git(["branch", "--set-upstream-to", f"origin/{b}"], check=False)
        _audit(actor, "GIT_CONNECT", {"remote": remote_url(), "branch": b})
        return status()


def pull(actor: str) -> Dict[str, Any]:
    with _LOCK:
        _require_repo()
        b = branch()
        if _changes():
            raise GitError("There are uncommitted local changes. Commit them (or discard them) before pulling.")
        before = _head()
        _git(["fetch", "origin", b])
        ref = f"origin/{b}"
        if before is None:
            _git(["reset", "-q", "--hard", ref])
        else:
            if _git(["merge-base", "--is-ancestor", ref, "HEAD"], check=False).startswith("0"):
                return {**status(), "pulled": False, "message": "Already up to date."}
            if not _git(["merge-base", "--is-ancestor", "HEAD", ref], check=False).startswith("0"):
                raise GitError("Local and remote history have diverged; pulling would need a merge, which is not done automatically. "
                               "Resolve it in the repository.")
            _git(["merge", "--ff-only", ref])
        after = _head()
        from web import dbt_config
        problem = dbt_config._dbt_parse("dbt_project.yml", open(os.path.join(_project_dir(), "dbt_project.yml"), encoding="utf-8").read()) \
            if os.path.isfile(os.path.join(_project_dir(), "dbt_project.yml")) else "The pulled project has no dbt_project.yml."
        if problem:
            if before:
                _git(["reset", "-q", "--hard", before])
            else:
                _git(["update-ref", "-d", "HEAD"], check=False)
            _audit(actor, "GIT_PULL_REJECTED", {"to": after, "reason": problem[:300]})
            raise GitError("dbt rejected the pulled project, so the pull was rolled back:\n" + problem)
        _audit(actor, "GIT_PULL", {"from": before, "to": after})
        return {**status(), "pulled": True, "message": f"Updated to {after[:8]}."}


def _plaintext_secrets() -> List[str]:
    from web import dbt_config
    found: List[str] = []
    for name in dbt_config.FILES:
        p = os.path.join(_project_dir(), name)
        if os.path.isfile(p):
            with open(p, encoding="utf-8") as f:
                found += [f"{name}: {w}" for w in dbt_config.secret_warnings(f.read())]
    return found


def commit(actor: str, message: str) -> Dict[str, Any]:
    with _LOCK:
        _require_repo()
        message = (message or "").strip()
        if not message:
            raise GitError("A commit message is required.")
        if len(message) > 2000:
            raise GitError("The commit message is too long.")
        if not _changes():
            raise GitError("Nothing to commit.")
        secrets = _plaintext_secrets()
        if secrets:
            raise GitError("Refusing to commit: a credential is in plain text (use env_var() instead):\n" + "\n".join(secrets))
        author = re.sub(r"[^A-Za-z0-9._-]", "_", actor or "unknown")[:60] or "unknown"
        ident = {"GIT_AUTHOR_NAME": author, "GIT_AUTHOR_EMAIL": f"{author}@datakilnworks.local",
                 "GIT_COMMITTER_NAME": "Data Kiln Works", "GIT_COMMITTER_EMAIL": "dkw@localhost"}
        _git(["add", "-A"])
        _git(["commit", "-q", "-m", message], extra_env=ident)
        sha = _head()
        _audit(actor, "GIT_COMMIT", {"sha": sha, "message": message[:200]})
        return {**status(), "committed": sha}


def push(actor: str) -> Dict[str, Any]:
    with _LOCK:
        _require_repo()
        b = branch()
        if not _head():
            raise GitError("Nothing to push: there are no commits yet.")
        try:
            _git(["push", "origin", f"HEAD:refs/heads/{b}"])
        except GitError as exc:
            msg = str(exc)
            if "rejected" in msg or "non-fast-forward" in msg or "fetch first" in msg:
                raise GitError("The remote has commits you do not have. Pull first (history is never force-pushed).")
            raise
        _git(["fetch", "origin", b], check=False)
        _git(["branch", "--set-upstream-to", f"origin/{b}"], check=False)
        _audit(actor, "GIT_PUSH", {"sha": _head(), "branch": b})
        return {**status(), "pushed": True}
