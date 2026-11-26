"""Reviewing and reconciling a repository from inside the studio (`web/git_sync.py` owns the repository and its rules).

Diff view    what is about to be committed (`pending`: the working tree against HEAD, untracked files included) or what the change branch adds to the
             base branch (`branch`: `origin/<base>...HEAD`, the same view a reviewer gets). One entry per file with counts, and a unified diff per file
             split into hunks with old/new line numbers. Notebooks are compared cell by cell (source only; outputs never reach the repository anyway).
             Only files that really changed can be asked for, paths are checked against the changed-file list, and sizes are capped.
Discard      throws away the pending change of ONE file (a tracked file is restored to HEAD, an untracked one is deleted); explicit, audited.
Conflicts    merging the base branch into a change branch (or the remote into a diverged local branch) can conflict. The merge is then LEFT IN PROGRESS
             (nothing is lost, nothing is committed): each conflicted file is listed with its conflict hunks (yours / incoming), and can be resolved
             whole-file (mine, theirs, delete) or hunk by hunk (mine, theirs, both, or a text typed in), or with the complete merged text. Finishing the
             merge is refused while a file is unresolved, still holds conflict markers, or the merged tree does not validate (dbt parse, notebook JSON) or
             contains a plaintext credential. Aborting returns to exactly the state before the merge.
"""
import difflib
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

MAX_FILES = 300
MAX_DIFF_BYTES = 200 * 1024
MAX_DIFF_LINES = 4000
MAX_CONFLICT_BYTES = 1024 * 1024
_MARK_OURS = re.compile(r"^<{7}(?: .*)?\r?\n?$")
_MARK_BASE = re.compile(r"^\|{7}(?: .*)?\r?\n?$")
_MARK_SEP = re.compile(r"^={7}\r?\n?$")
_MARK_THEIRS = re.compile(r"^>{7}(?: .*)?\r?\n?$")


def _git_error():
    from web.git_sync import GitError
    return GitError


def _z(out: str) -> List[str]:
    return [x for x in out.split("\0") if x != ""]


def _raw(repo, args: List[str], check: bool = True) -> Tuple[int, bytes]:
    """git output as raw bytes (diffs of files that are not UTF-8 must survive); the token is never in it (it is only in the environment)."""
    import subprocess
    try:
        r = subprocess.run(["git", *args], cwd=repo.dir(), capture_output=True, timeout=60, env=repo._env())
    except subprocess.TimeoutExpired:
        raise _git_error()(f"git {args[0]} timed out.")
    if check and r.returncode != 0:
        raise _git_error()(repo._scrub((r.stderr or r.stdout or b"").decode("utf-8", "replace")).strip()[-600:] or f"git {args[0]} failed.")
    return r.returncode, r.stdout


def _text(b: bytes) -> Optional[str]:
    if b"\0" in b[:8192]:
        return None
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _safe_path(repo, rel: str) -> str:
    """The absolute path of a repository-relative path, never outside the repository and never through a symlink out of it."""
    GitError = _git_error()
    if not rel or rel.startswith(("/", "-")) or "\0" in rel or ".." in rel.replace("\\", "/").split("/"):
        raise GitError("That is not a path inside the repository.")
    root = os.path.realpath(repo.dir())
    full = os.path.realpath(os.path.join(root, rel))
    if full != root and not full.startswith(root + os.sep):
        raise GitError("That is not a path inside the repository.")
    if ".git" in rel.replace("\\", "/").split("/"):
        raise GitError("That is not a path inside the repository.")
    return full


# ---------------------------------------------------------------- the file list

def _base_ref(repo) -> str:
    return f"origin/{repo.branch()}"


def summary(repo, scope: str = "pending") -> Dict[str, Any]:
    GitError = _git_error()
    if scope not in ("pending", "branch"):
        raise GitError("The scope must be 'pending' or 'branch'.")
    repo._require_repo()
    files: Dict[str, Dict[str, Any]] = {}
    label = ""
    if scope == "pending":
        label = "Uncommitted changes (working tree against the last commit)"
        has_head = repo._head() is not None
        _c, st = _raw(repo, ["status", "--porcelain=v1", "-uall", "-z"])
        parts = _z(st.decode("utf-8", "replace"))
        i = 0
        while i < len(parts):
            code, path = parts[i][:2], parts[i][3:]
            i += 1
            if code[0] in "RC":                                  # a rename lists its origin next; treat it as a new file (the old one shows as deleted)
                i += 1
            letter = "A" if code == "??" else ("D" if "D" in code else ("A" if "A" in code or "R" in code or "C" in code else "M"))
            files[path] = {"path": path, "status": letter, "untracked": code == "??", "additions": 0, "deletions": 0, "binary": False}
        if has_head:
            _c, ns = _raw(repo, ["diff", "HEAD", "--numstat", "--no-renames", "-z"])
            for rec in _z(ns.decode("utf-8", "replace")):
                m = re.match(r"^(-|\d+)\t(-|\d+)\t(.*)$", rec, re.S)
                if m and m.group(3) in files:
                    f = files[m.group(3)]
                    if m.group(1) == "-":
                        f["binary"] = True
                    else:
                        f["additions"], f["deletions"] = int(m.group(1)), int(m.group(2))
        for path, f in files.items():
            if f["untracked"] or not has_head:
                full = os.path.join(repo.dir(), path)
                try:
                    with open(full, "rb") as fh:
                        data = fh.read(MAX_DIFF_BYTES + 1)
                    t = _text(data)
                    if t is None:
                        f["binary"] = True
                    else:
                        f["additions"] = t.count("\n") + (0 if t.endswith("\n") or not t else 1)
                except OSError:
                    pass
    else:
        base = _base_ref(repo)
        if not repo._has_ref(f"refs/remotes/{base}"):
            raise GitError("The base branch is not on the server yet, so there is nothing to compare with.")
        label = f"This branch compared with {repo.branch()} (what a reviewer sees)"
        _c, ns = _raw(repo, ["diff", f"{base}...HEAD", "--numstat", "--no-renames", "-z"])
        for rec in _z(ns.decode("utf-8", "replace")):
            m = re.match(r"^(-|\d+)\t(-|\d+)\t(.*)$", rec, re.S)
            if m:
                files[m.group(3)] = {"path": m.group(3), "status": "M", "untracked": False, "binary": m.group(1) == "-",
                                     "additions": 0 if m.group(1) == "-" else int(m.group(1)), "deletions": 0 if m.group(2) == "-" else int(m.group(2))}
        _c, nm = _raw(repo, ["diff", f"{base}...HEAD", "--name-status", "--no-renames", "-z"])
        parts = _z(nm.decode("utf-8", "replace"))
        for j in range(0, len(parts) - 1, 2):
            if parts[j + 1] in files:
                files[parts[j + 1]]["status"] = parts[j][:1]
    items = sorted(files.values(), key=lambda f: f["path"])
    return {"scope": scope, "label": label, "files": items[:MAX_FILES], "total_files": len(items), "truncated": len(items) > MAX_FILES,
            "additions": sum(f["additions"] for f in items), "deletions": sum(f["deletions"] for f in items)}


# ---------------------------------------------------------------- one file's diff

def _notebook_text(raw: Optional[str]) -> Optional[str]:
    """A notebook as reviewable text: one block per cell (type and source); outputs and metadata are left out."""
    if raw is None:
        return None
    try:
        nb = json.loads(raw)
        out = []
        for i, c in enumerate(nb.get("cells") or []):
            src = c.get("source")
            src = "".join(src) if isinstance(src, list) else str(src or "")
            out.append(f"# %% [{c.get('cell_type', 'code')}] cell {i + 1}\n{src}\n")
        return "".join(out)
    except Exception:
        return raw


def _parse_unified(text: str) -> List[Dict[str, Any]]:
    hunks: List[Dict[str, Any]] = []
    cur = None
    o = n = 0
    for line in text.split("\n"):
        m = re.match(r"^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@(.*)$", line)
        if m:
            cur = {"header": line, "lines": []}
            hunks.append(cur)
            o, n = int(m.group(1)), int(m.group(2))
            continue
        if cur is None or line.startswith("\\"):
            continue
        if line.startswith("+"):
            cur["lines"].append({"t": "add", "o": None, "n": n, "s": line[1:]})
            n += 1
        elif line.startswith("-"):
            cur["lines"].append({"t": "del", "o": o, "n": None, "s": line[1:]})
            o += 1
        elif line.startswith(" "):
            cur["lines"].append({"t": "ctx", "o": o, "n": n, "s": line[1:]})
            o += 1
            n += 1
    return hunks


def _difflib(old: Optional[str], new: Optional[str], path: str) -> str:
    a = (old or "").splitlines()
    b = (new or "").splitlines()
    return "\n".join(list(difflib.unified_diff(a, b, f"a/{path}", f"b/{path}", lineterm="", n=3)))


def file_diff(repo, path: str, scope: str = "pending") -> Dict[str, Any]:
    GitError = _git_error()
    _safe_path(repo, path)
    info = next((f for f in summary(repo, scope)["files"] if f["path"] == path), None)
    if info is None:
        raise GitError("That file has no changes in this view.")
    base = _base_ref(repo)
    old_rev = "HEAD" if scope == "pending" else f"{base}"
    new_rev = None if scope == "pending" else "HEAD"
    out = {**info, "hunks": [], "truncated": False, "notice": ""}
    if info["binary"]:
        out["notice"] = "Binary file (or not UTF-8 text): not shown."
        return out

    def show(rev: Optional[str]) -> Optional[str]:
        if rev is None:                                          # the working file
            try:
                with open(os.path.join(repo.dir(), path), "rb") as fh:
                    data = fh.read(MAX_DIFF_BYTES * 4)
            except OSError:
                return None
            return _text(data)
        code, data = _raw(repo, ["show", f"{rev}:{path}"], check=False)
        return _text(data) if code == 0 else None

    old_exists = repo._head() is not None and not info.get("untracked") and info["status"] != "A"
    if scope == "branch":
        old_exists = info["status"] != "A"
    ipynb = path.endswith(".ipynb")
    if ipynb or info.get("untracked") or repo._head() is None:
        old = show(old_rev) if old_exists else None
        new = show(new_rev) if info["status"] != "D" else None
        if ipynb:
            old, new = _notebook_text(old), _notebook_text(new)
            out["notice"] = "Notebook shown cell by cell (source only)."
        diff_text = _difflib(old, new, path)
    else:
        rng = ["HEAD"] if scope == "pending" else [f"{base}...HEAD"]
        _c, data = _raw(repo, ["diff", *rng, "--no-color", "--no-ext-diff", "--no-renames", "-U3", "--", path])
        diff_text = data.decode("utf-8", "replace")
    if len(diff_text.encode("utf-8", "replace")) > MAX_DIFF_BYTES or diff_text.count("\n") > MAX_DIFF_LINES:
        diff_text = "\n".join(diff_text.split("\n")[:MAX_DIFF_LINES])[:MAX_DIFF_BYTES]
        out["truncated"] = True
    out["hunks"] = _parse_unified(diff_text)
    return out


def discard(repo, path: str, actor: str) -> Dict[str, Any]:
    GitError = _git_error()
    with __import__("web.git_sync", fromlist=["_LOCK"])._LOCK:
        repo._require_repo()
        _safe_path(repo, path)
        if merge_state(repo)["in_progress"]:
            raise GitError("A merge is in progress: resolve it or abort it first.")
        info = next((f for f in summary(repo, "pending")["files"] if f["path"] == path), None)
        if info is None:
            raise GitError("That file has no uncommitted change.")
        full = _safe_path(repo, path)
        if info.get("untracked") or repo._head() is None:
            try:
                os.remove(full)
            except FileNotFoundError:
                pass
        else:
            repo._git(["restore", "--source=HEAD", "--staged", "--worktree", "--", path])
        repo._audit(actor, "DISCARD", {"path": path, "was": info["status"]})
        return {**repo.status(), "discarded": path}


# ---------------------------------------------------------------- merge state and conflicts

def merge_state(repo) -> Dict[str, Any]:
    d = repo.dir()
    if not os.path.isfile(os.path.join(d, ".git", "MERGE_HEAD")):
        return {"in_progress": False, "conflicts": []}
    conflicts = _unmerged(repo)
    open_paths = {c["path"] for c in conflicts}
    _c, out = _raw(repo, ["diff", "--cached", "--name-only", "-z"], check=False)
    resolved = [p for p in _z(out.decode("utf-8", "replace")) if p not in open_paths][:100]
    return {"in_progress": True, "conflicts": conflicts, "resolved": resolved}


def _unmerged(repo) -> List[Dict[str, Any]]:
    _c, out = _raw(repo, ["ls-files", "-u", "-z"], check=False)
    stages: Dict[str, set] = {}
    for rec in _z(out.decode("utf-8", "replace")):
        meta, _, path = rec.partition("\t")
        bits = meta.split()
        if len(bits) >= 3:
            stages.setdefault(path, set()).add(int(bits[2]))
    res = []
    for path, st in sorted(stages.items()):
        if 2 in st and 3 in st:
            kind = "both_modified" if 1 in st else "both_added"
        elif 2 in st:
            kind = "deleted_by_incoming"
        else:
            kind = "deleted_by_you"
        res.append({"path": path, "kind": kind})
    return res


def parse_markers(text: str) -> List[Dict[str, Any]]:
    """Splits a file with conflict markers into ordered segments: {'type': 'common', 'lines': [...]} and {'type': 'conflict', 'index': k, 'ours': [...],
    'base': [...] | None, 'theirs': [...], 'ours_label', 'theirs_label'} (lines keep their line endings)."""
    segs: List[Dict[str, Any]] = []
    common: List[str] = []
    mode = None
    cur: Dict[str, Any] = {}
    k = 0
    for line in text.splitlines(keepends=True):
        if mode is None:
            if _MARK_OURS.match(line):
                if common:
                    segs.append({"type": "common", "lines": common})
                    common = []
                cur = {"type": "conflict", "index": k, "ours": [], "base": None, "theirs": [], "ours_label": line[7:].strip(), "theirs_label": ""}
                mode = "ours"
            else:
                common.append(line)
        elif mode == "ours":
            if _MARK_BASE.match(line):
                mode, cur["base"] = "base", []
            elif _MARK_SEP.match(line):
                mode = "theirs"
            else:
                cur["ours"].append(line)
        elif mode == "base":
            if _MARK_SEP.match(line):
                mode = "theirs"
            else:
                cur["base"].append(line)
        else:
            if _MARK_THEIRS.match(line):
                cur["theirs_label"] = line[7:].strip()
                segs.append(cur)
                k += 1
                mode = None
            else:
                cur["theirs"].append(line)
    if mode is not None:                                         # an unterminated block: keep everything so nothing is lost
        segs.append({"type": "common", "lines": [x for part in ("ours", "theirs") for x in cur.get(part, [])]})
    if common:
        segs.append({"type": "common", "lines": common})
    return segs


def has_markers(text: str) -> bool:
    return any(_MARK_OURS.match(l) or _MARK_THEIRS.match(l) for l in text.splitlines(keepends=True))


def _stage_text(repo, stage: int, path: str) -> Optional[str]:
    code, data = _raw(repo, ["show", f":{stage}:{path}"], check=False)
    return _text(data[:MAX_CONFLICT_BYTES]) if code == 0 else None


def conflict_detail(repo, path: str) -> Dict[str, Any]:
    GitError = _git_error()
    repo._require_repo()
    full = _safe_path(repo, path)
    conflict = next((c for c in merge_state(repo)["conflicts"] if c["path"] == path), None)
    if conflict is None:
        raise GitError("That file is not in conflict (any more).")
    out: Dict[str, Any] = {**conflict, "binary": False, "segments": [], "ours_text": None, "theirs_text": None, "ipynb": path.endswith(".ipynb")}
    kind = conflict["kind"]
    if kind in ("deleted_by_incoming", "deleted_by_you"):
        surviving = 2 if kind == "deleted_by_incoming" else 3
        out["surviving_text"] = _stage_text(repo, surviving, path)
        out["binary"] = out["surviving_text"] is None
        return out
    try:
        with open(full, "rb") as fh:
            raw = fh.read(MAX_CONFLICT_BYTES + 1)
    except OSError:
        raw = b""
    text = _text(raw) if len(raw) <= MAX_CONFLICT_BYTES else None
    if text is None:
        out["binary"] = True                                       # binary or too large to show: whole-file choices only
        return out
    out["segments"] = [{**s, "lines": [l.rstrip("\r\n") for l in s["lines"]]} if s["type"] == "common" else
                       {**s, "ours": [l.rstrip("\r\n") for l in s["ours"]], "theirs": [l.rstrip("\r\n") for l in s["theirs"]],
                        "base": None if s["base"] is None else [l.rstrip("\r\n") for l in s["base"]]} for s in parse_markers(text)]
    out["ours_text"], out["theirs_text"] = _stage_text(repo, 2, path), _stage_text(repo, 3, path)
    out["text"] = text
    return out


def resolve(repo, path: str, resolution: str, choices: Optional[List[Dict[str, Any]]], content: Optional[str], actor: str) -> Dict[str, Any]:
    GitError = _git_error()
    with __import__("web.git_sync", fromlist=["_LOCK"])._LOCK:
        repo._require_repo()
        _safe_path(repo, path)
        state = merge_state(repo)
        if resolution == "reopen":                                 # take a resolved file back into conflict (e.g. the merged result did not validate)
            if not state["in_progress"] or path not in state.get("resolved", []):
                raise GitError("That file was not resolved in this merge.")
            _safe_path(repo, path)
            repo._git(["checkout", "-m", "--", path])
            repo._audit(actor, "CONFLICT_REOPEN", {"path": path})
            return {**repo.status(), "reopened": path}
        conflict = next((c for c in state["conflicts"] if c["path"] == path), None)
        if conflict is None:
            raise GitError("That file is not in conflict (any more).")
        full = _safe_path(repo, path)
        kind = conflict["kind"]
        if resolution in ("ours", "theirs"):
            stage = 2 if resolution == "ours" else 3
            has_side = _raw(repo, ["cat-file", "-e", f":{stage}:{path}"], check=False)[0] == 0
            if has_side:
                repo._git(["checkout", f"--{resolution}", "--", path])
                repo._git(["add", "--", path])
            else:                                                  # that side deleted the file: keeping its version means deleting
                repo._git(["rm", "-q", "-f", "--", path])
        elif resolution == "delete":
            repo._git(["rm", "-q", "-f", "--", path])
        elif resolution == "keep" and kind in ("deleted_by_incoming", "deleted_by_you"):
            surviving = 2 if kind == "deleted_by_incoming" else 3
            repo._git(["checkout", f"--{'ours' if surviving == 2 else 'theirs'}", "--", path])
            repo._git(["add", "--", path])
        elif resolution == "merged":
            if kind not in ("both_modified", "both_added"):
                raise GitError("Choose keep, delete, mine or theirs for this kind of conflict.")
            if content is not None:
                text = content
                if has_markers(text):
                    raise GitError("The text still contains conflict markers (<<<<<<<, =======, >>>>>>>). Remove them or choose a side.")
                if len(text.encode("utf-8")) > MAX_CONFLICT_BYTES:
                    raise GitError("The text is too large.")
            else:
                try:
                    with open(full, "rb") as fh:
                        current = _text(fh.read())
                except OSError:
                    current = None
                if current is None:
                    raise GitError("This file cannot be merged by hand here; choose mine or theirs.")
                segs = parse_markers(current)
                n = sum(1 for s in segs if s["type"] == "conflict")
                choices = choices or []
                if n == 0:
                    raise GitError("The file has no conflict markers left; use its current text.")
                if len(choices) != n:
                    raise GitError(f"Decide every conflict ({n}) of this file.")
                parts: List[str] = []
                for s in segs:
                    if s["type"] == "common":
                        parts.extend(s["lines"])
                        continue
                    pick = (choices[s["index"]] or {}).get("pick")
                    if pick == "ours":
                        parts.extend(s["ours"])
                    elif pick == "theirs":
                        parts.extend(s["theirs"])
                    elif pick == "both":
                        parts.extend(s["ours"])
                        if s["ours"] and not s["ours"][-1].endswith("\n"):
                            parts.append("\n")
                        parts.extend(s["theirs"])
                    elif pick == "custom":
                        custom = str((choices[s["index"]] or {}).get("text", ""))
                        if has_markers(custom):
                            raise GitError("A typed-in text still contains conflict markers.")
                        parts.append(custom if not custom or custom.endswith("\n") else custom + "\n")
                    else:
                        raise GitError(f"Conflict {s['index'] + 1}: choose mine, theirs, both or type a text.")
                text = "".join(parts)
            with open(full, "w", encoding="utf-8", newline="") as fh:
                fh.write(text)
            repo._git(["add", "--", path])
        else:
            raise GitError("Unknown resolution.")
        repo._audit(actor, "CONFLICT_RESOLVE", {"path": path, "resolution": resolution})
        return {**repo.status(), "resolved": path}


def _leftover_markers(repo) -> List[str]:
    _c, out = _raw(repo, ["diff", "--cached", "--name-only", "-z"], check=False)
    bad = []
    for rel in _z(out.decode("utf-8", "replace")):
        try:
            with open(_safe_path(repo, rel), "rb") as fh:
                t = _text(fh.read(MAX_CONFLICT_BYTES))
        except Exception:
            continue
        if t and has_markers(t) and _MARK_SEP_IN(t):
            bad.append(rel)
    return bad


def _MARK_SEP_IN(text: str) -> bool:
    return any(_MARK_SEP.match(l) for l in text.splitlines(keepends=True))


def finish_merge(repo, actor: str, message: Optional[str] = None) -> Dict[str, Any]:
    import re as _re
    GitError = _git_error()
    with __import__("web.git_sync", fromlist=["_LOCK"])._LOCK:
        repo._require_repo()
        st = merge_state(repo)
        if not st["in_progress"]:
            raise GitError("There is no merge in progress.")
        if st["conflicts"]:
            raise GitError(f"{len(st['conflicts'])} file(s) are still in conflict: " + ", ".join(c["path"] for c in st["conflicts"][:8]))
        left = _leftover_markers(repo)
        if left:
            raise GitError("These files still contain conflict markers: " + ", ".join(left[:8]))
        problem = repo._problem_with_tree()
        if problem:
            raise GitError("The merged result does not validate, so the merge is not finished (fix the files, or abort the merge):\n" + problem)
        secrets_found = repo._problems_before_commit()
        if secrets_found:
            raise GitError("Refusing to finish the merge:\n" + "\n".join(secrets_found))
        author = _re.sub(r"[^A-Za-z0-9._-]", "_", actor or "unknown")[:60] or "unknown"
        ident = {"GIT_AUTHOR_NAME": author, "GIT_AUTHOR_EMAIL": f"{author}@datakilnworks.local", "GIT_COMMITTER_NAME": "Data Kiln Works", "GIT_COMMITTER_EMAIL": "dkw@localhost"}
        msg = (message or "").strip()
        repo._git(["commit", "-q", *(["-m", msg[:2000]] if msg else ["--no-edit"])], extra_env=ident)
        sha = repo._head()
        repo._audit(actor, "MERGE_FINISH", {"sha": sha})
        return {**repo.status(), "committed": sha, "message": f"Merge finished ({sha[:8]})."}


def abort_merge(repo, actor: str) -> Dict[str, Any]:
    GitError = _git_error()
    with __import__("web.git_sync", fromlist=["_LOCK"])._LOCK:
        repo._require_repo()
        if not merge_state(repo)["in_progress"]:
            raise GitError("There is no merge in progress.")
        repo._git(["merge", "--abort"])
        repo._audit(actor, "MERGE_ABORT", {})
        return {**repo.status(), "message": "The merge was aborted; everything is as it was before."}
