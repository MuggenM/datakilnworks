"""
Editing the dbt project's two configuration files from the UI: `profiles.yml` (where and how dbt runs, including where the
models land) and `dbt_project.yml` (models, schemas, hooks). Administrators only, because both files decide what dbt does as
the system principal: a hook in dbt_project.yml is arbitrary SQL, and profiles.yml can point dbt at any storage.

  * Every save is validated first, and never half-applied: the candidate is parsed as YAML, cross-checked (the project's
    `profile:` must exist in profiles.yml), and then run through `dbt parse` on a scratch copy of the project, so what dbt itself
    would refuse (a bad adapter setting, an unknown key, a broken Jinja expression) is refused here, with dbt's message.
  * Every save keeps the previous version (last 20 per file, in the studio's metadata, not in the project) and is written to the governance
    audit log (who, which file, a line-level summary; never the content, which may hold credentials).
  * Credentials do not belong in these files. Every configured S3 mount is exported to dbt as DKW_MOUNT_<ID>_* environment
    variables (`mount_env`), so a profile can say `{{ env_var('DKW_MOUNT_<ID>_SECRET') }}` and keep working when the mount's key
    is rotated. `s3_profile` writes such a profile: it lands the models in that mount's bucket. Plain-text secrets are flagged.
"""

import datetime
import difflib
import json
import os
import re
import shutil
import subprocess
import tempfile
from typing import Any, Dict, List, Optional

import yaml

FILES = ("profiles.yml", "dbt_project.yml")
MAX_BYTES = 200_000
KEEP_VERSIONS = 20
_SECRET_KEY = re.compile(r"(secret|password|passwd|token|private_key|access_key|key_id)", re.I)
_ENV_REF = re.compile(r"\{\{\s*env_var\(")


class ConfigError(ValueError):
    """The candidate configuration cannot be saved; `errors` says why (dbt's own message when dbt refused it)."""

    def __init__(self, errors: List[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


def _project_dir() -> str:
    return os.getenv("DBT_PROJECT_DIR", "/workspace/dbt_project")


def _history_dir() -> str:
    """Config history is application data, kept next to the studio's other metadata, so it never lands in the dbt project's own
    git repository (a production project lives in its own repo)."""
    return os.getenv("DBT_CONFIG_HISTORY_DIR") or os.path.join(os.getenv("WAREHOUSE_DIR", "/workspace/warehouse"), ".metadata", "dbt_config_history")


def _path(name: str) -> str:
    if name not in FILES:
        raise LookupError(f"Only {', '.join(FILES)} can be edited here.")
    return os.path.join(_project_dir(), name)


# ---------------------------------------------------------------- mounts as environment variables

def _env_id(mount: Dict[str, Any]) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", str(mount.get("id") or "").upper()).strip("_")


def s3_mounts() -> List[Dict[str, Any]]:
    from web.mounts import load_mounts
    return [m for m in load_mounts() if (m.get("type") or "").lower() == "s3"]


def mount_env() -> Dict[str, str]:
    """DKW_MOUNT_<ID>_{BUCKET,ENDPOINT,ENDPOINT_URL,KEY_ID,SECRET,REGION,URL_STYLE,USE_SSL} for every S3 mount (USE_SSL is `True`/`False`: dbt's `as_bool` filter needs that spelling)."""
    from web.autoloader_s3 import resolve_connection
    env: Dict[str, str] = {}
    for m in s3_mounts():
        try:
            c = resolve_connection({"source_volume_path": f"s3://{(m.get('config') or {}).get('bucket') or 'x'}/", "source_mount_id": m["id"]})
        except Exception:
            continue
        p = f"DKW_MOUNT_{_env_id(m)}_"
        env.update({p + "BUCKET": str((m.get("config") or {}).get("bucket") or ""), p + "ENDPOINT": c["endpoint"],
                    p + "ENDPOINT_URL": f"{'https' if c['use_ssl'] else 'http'}://{c['endpoint']}", p + "KEY_ID": c["key_id"],
                    p + "SECRET": c["secret"], p + "REGION": c["region"], p + "URL_STYLE": c["url_style"],
                    p + "USE_SSL": "True" if c["use_ssl"] else "False"})   # capitalised: dbt's as_bool filter rejects lowercase
    return env


def dbt_env(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """The environment every dbt subprocess gets: the studio's, plus the S3 mounts."""
    return {**os.environ, **mount_env(), **(extra or {})}


# ---------------------------------------------------------------- read / validate / save

def _versions(name: str) -> List[Dict[str, Any]]:
    d = _history_dir()
    if not os.path.isdir(d):
        return []
    out = []
    for f in sorted(os.listdir(d), reverse=True):
        m = re.match(rf"^{re.escape(name)}\.(\d{{8}}T\d{{6}}Z)\.(.*)\.bak$", f)
        if m:
            out.append({"id": m.group(1), "saved_at": datetime.datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "by": m.group(2)})
    return out


def read(name: str) -> Dict[str, Any]:
    p = _path(name)
    with open(p, encoding="utf-8") as f:
        content = f.read()
    return {"name": name, "content": content, "modified": datetime.datetime.fromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S"),
            "versions": _versions(name), "warnings": secret_warnings(content)}


def read_version(name: str, version_id: str) -> Dict[str, Any]:
    _path(name)
    if not re.match(r"^\d{8}T\d{6}Z$", version_id):
        raise LookupError("Unknown version.")
    for f in os.listdir(_history_dir()) if os.path.isdir(_history_dir()) else []:
        if f.startswith(f"{name}.{version_id}."):
            with open(os.path.join(_history_dir(), f), encoding="utf-8") as fh:
                return {"name": name, "id": version_id, "content": fh.read()}
    raise LookupError("Unknown version.")


def secret_warnings(content: str) -> List[str]:
    """Lines that hold a credential in plain text instead of an env_var()."""
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError:
        return []
    found: List[str] = []

    def walk(node, path):
        if isinstance(node, dict):
            for k, v in node.items():
                if isinstance(v, (dict, list)):
                    walk(v, path + [str(k)])
                elif isinstance(v, str) and v and _SECRET_KEY.search(str(k)) and not _ENV_REF.search(v):
                    found.append(".".join(path + [str(k)]))
        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(v, path + [str(i)])
    walk(data, [])
    return [f"{p} holds a credential in plain text; use {{{{ env_var('...') }}}} (a storage mount exports its keys as DKW_MOUNT_<ID>_* variables)." for p in found]


def _static_checks(name: str, content: str) -> List[str]:
    errors: List[str] = []
    if len(content.encode()) > MAX_BYTES:
        return [f"The file is larger than {MAX_BYTES // 1000} KB."]
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        return [f"Not valid YAML: {str(exc).splitlines()[0] if str(exc) else exc}" + (f" (line {exc.problem_mark.line + 1})" if getattr(exc, "problem_mark", None) else "")]
    if not isinstance(data, dict):
        return ["The file must be a YAML mapping."]
    if name == "dbt_project.yml":
        for key in ("name", "profile"):
            if not isinstance(data.get(key), str) or not data[key].strip():
                errors.append(f"dbt_project.yml needs a `{key}`.")
        if not errors:
            try:
                profiles = yaml.safe_load(open(_path("profiles.yml"), encoding="utf-8")) or {}
                if data["profile"] not in profiles:
                    errors.append(f"profile `{data['profile']}` does not exist in profiles.yml.")
            except (OSError, yaml.YAMLError):
                pass
    else:
        for pname, prof in data.items():
            if pname == "config":
                continue
            outputs = prof.get("outputs") if isinstance(prof, dict) else None
            if not isinstance(outputs, dict) or not outputs:
                errors.append(f"profile `{pname}` needs an `outputs:` mapping.")
                continue
            for oname, out in outputs.items():
                if not isinstance(out, dict) or not out.get("type"):
                    errors.append(f"output `{pname}.{oname}` needs a `type`.")
            if prof.get("target") and prof["target"] not in outputs:
                errors.append(f"profile `{pname}`: target `{prof['target']}` is not one of its outputs.")
        try:
            wanted = (yaml.safe_load(open(_path("dbt_project.yml"), encoding="utf-8")) or {}).get("profile")
            if wanted and wanted not in data:
                errors.append(f"dbt_project.yml uses profile `{wanted}`, which this file no longer defines.")
        except (OSError, yaml.YAMLError):
            pass
    return errors


def _dbt_parse(name: str, content: str) -> Optional[str]:
    """Runs `dbt parse` on a scratch copy of the project with the candidate in place; returns dbt's error text or None."""
    src = _project_dir()
    with tempfile.TemporaryDirectory(prefix="dbt_cfg_") as tmp:
        for item in ("models", "macros", "seeds", "tests", "snapshots", "analyses", "dbt_project.yml", "profiles.yml"):
            s = os.path.join(src, item)
            if os.path.isdir(s):
                shutil.copytree(s, os.path.join(tmp, item))
            elif os.path.isfile(s):
                shutil.copy(s, tmp)
        with open(os.path.join(tmp, name), "w", encoding="utf-8") as f:
            f.write(content)
        try:
            r = subprocess.run(["dbt", "parse", "--profiles-dir", ".", "--project-dir", ".", "--no-partial-parse"], cwd=tmp,
                               capture_output=True, text=True, timeout=120, env=dbt_env({"DBT_TARGET_PATH": os.path.join(tmp, "target")}))
        except subprocess.TimeoutExpired:
            return "dbt parse timed out."
        if r.returncode == 0:                      # (inside the scratch dir: the manifest lives there)
            return _check_hook_macros(os.path.join(tmp, "target", "manifest.json"), content if name == "dbt_project.yml" else None, src)
    text = re.sub(r"\x1b\[[0-9;]*m", "", (r.stdout + r.stderr))
    lines = [l for l in text.splitlines() if l.strip() and not re.match(r"^\d\d:\d\d:\d\d\s+(Running with|Registered adapter|Unable to do partial)", l.strip())]
    return "\n".join(lines[-8:])[:900]


_JINJA_BUILTINS = {"ref", "source", "var", "env_var", "config", "log", "print", "return", "run_query", "statement", "adapter", "exceptions", "tojson",
                   "fromjson", "toyaml", "fromyaml", "set", "range", "dict", "list", "zip", "cycle", "lipsum", "namespace", "modules", "api", "load_result",
                   "store_result", "store_raw_result", "is_incremental", "ref_", "as_bool", "as_native", "as_number", "as_text", "doc", "invocation_args_dict"}


def _check_hook_macros(manifest_path: str, project_yml: Optional[str], project_dir: str) -> Optional[str]:
    """`dbt parse` does not render hooks, so a hook that calls a macro that does not exist would pass it and then fail every run.
    Check the macros called from on-run-start / on-run-end (and model pre/post hooks in dbt_project.yml) against the parsed manifest."""
    try:
        text = project_yml if project_yml is not None else open(os.path.join(project_dir, "dbt_project.yml"), encoding="utf-8").read()
        project = yaml.safe_load(text) or {}
        with open(manifest_path, encoding="utf-8") as f:
            macros = {m.get("name") for m in (json.load(f).get("macros") or {}).values()}
    except Exception:
        return None
    hooks: List[str] = []

    def collect(node, key=""):
        if isinstance(node, dict):
            for k, v in node.items():
                collect(v, str(k))
        elif isinstance(node, list):
            for v in node:
                collect(v, key)
        elif isinstance(node, str) and key.lstrip("+").replace("_", "-") in ("on-run-start", "on-run-end", "pre-hook", "post-hook"):
            hooks.append(node)
    collect(project)
    missing = sorted({m.group(1).split(".")[-1] for h in hooks for m in re.finditer(r"\{\{[^}]*?\b([A-Za-z_][\w.]*)\s*\(", h)}
                     - macros - _JINJA_BUILTINS)
    if missing:
        return f"a hook calls macro(s) that do not exist: {', '.join(missing)} (dbt would fail every run with a compilation error)"
    return None


def validate(name: str, content: str) -> Dict[str, Any]:
    _path(name)
    errors = _static_checks(name, content)
    if not errors:
        err = _dbt_parse(name, content)
        if err:
            errors.append("dbt rejected this configuration:\n" + err)
    return {"ok": not errors, "errors": errors, "warnings": secret_warnings(content)}


def _summary(old: str, new: str) -> Dict[str, int]:
    diff = list(difflib.unified_diff(old.splitlines(), new.splitlines(), lineterm="", n=0))
    return {"added": sum(1 for l in diff if l.startswith("+") and not l.startswith("+++")),
            "removed": sum(1 for l in diff if l.startswith("-") and not l.startswith("---"))}


def save(name: str, content: str, actor: str) -> Dict[str, Any]:
    """Validates, backs up the current file, writes atomically, audits. Raises ConfigError (file untouched) when invalid."""
    p = _path(name)
    content = content.replace("\r\n", "\n")
    verdict = validate(name, content)
    if not verdict["ok"]:
        raise ConfigError(verdict["errors"])
    with open(p, encoding="utf-8") as f:
        old = f.read()
    if old == content:
        return {**read(name), "changed": False}
    os.makedirs(_history_dir(), exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_actor = re.sub(r"[^A-Za-z0-9_-]", "_", actor or "unknown")[:40]
    with open(os.path.join(_history_dir(), f"{name}.{stamp}.{safe_actor}.bak"), "w", encoding="utf-8") as f:
        f.write(old)
    for stale in _versions(name)[KEEP_VERSIONS:]:
        for fn in os.listdir(_history_dir()):
            if fn.startswith(f"{name}.{stale['id']}."):
                os.remove(os.path.join(_history_dir(), fn))
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    mode = os.stat(p).st_mode
    os.chmod(tmp, mode)
    os.replace(tmp, p)
    try:
        from web.governance import store
        store.init_governance_db()
        conn = store.get_db()
        try:
            store.write_audit(conn, actor, "DBT_CONFIG_SAVE", name, _summary(old, content))
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass
    return {**read(name), "changed": True}


# ---------------------------------------------------------------- guided settings (line-level edits: comments survive)
#
# The few settings people actually change are offered as a form, but the YAML stays the source of truth: every change is a
# surgical edit of the text (a value replaced, a block inserted or removed), so comments, ordering and everything the form
# does not know about are untouched. The result is only *proposed* (returned as text); saving goes through the same validation.

MATERIALIZATIONS = ("view", "table", "incremental", "ephemeral")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip())


def _is_content(line: str) -> bool:
    st = line.strip()
    return bool(st) and not st.startswith("#")


def _locate(lines: List[str], path: List[str]):
    """(start, end, indent) of the mapping node at `path` (its key line, one past its last line, the key line's indent), or None."""
    idx, indent, end = -1, -1, len(lines)
    for key in path:
        child, found, i = None, None, idx + 1
        while i < end:
            if _is_content(lines[i]):
                ind = _indent(lines[i])
                if ind <= indent:
                    break
                child = ind if child is None else child
                if ind == child and re.match(rf"^\s*['\"]?{re.escape(key)}['\"]?\s*:", lines[i]):
                    found = i
                    break
            i += 1
        if found is None:
            return None
        j = found + 1
        while j < end and (not _is_content(lines[j]) or _indent(lines[j]) > child
                           or (_indent(lines[j]) == child and lines[j].lstrip().startswith("- "))):     # indentless sequence items belong to their key
            j += 1
        while j > found + 1 and not _is_content(lines[j - 1]):
            j -= 1                                            # trailing blank / comment lines belong to what follows
        idx, indent, end = found, child, j
    return idx, end, indent


def _child_indent(lines: List[str], start: int, end: int, parent_indent: int) -> int:
    for i in range(start + 1, end):
        if _is_content(lines[i]):
            return _indent(lines[i])
    return parent_indent + 2


def _set_key(lines: List[str], path: List[str], key: str, value: Any) -> bool:
    """Sets `key: value` inside the mapping at `path` (replacing the line, or appending it). False if `path` does not exist."""
    node = _locate(lines, path)
    if node is None:
        return False
    start, end, indent = node
    ci = _child_indent(lines, start, end, indent)
    rendered = yaml.safe_dump({key: value}, default_flow_style=True, width=10_000).strip()
    rendered = rendered[1:-1].strip() if rendered.startswith("{") else rendered      # `key: value` without flow braces
    for i in range(start + 1, end):
        if _is_content(lines[i]) and _indent(lines[i]) == ci and re.match(rf"^\s*['\"]?{re.escape(key)}['\"]?\s*:", lines[i]):
            lines[i] = " " * ci + rendered
            return True
    lines.insert(end, " " * ci + rendered)
    return True


def _remove_key(lines: List[str], path: List[str]) -> bool:
    node = _locate(lines, path)
    if node is None:
        return False
    del lines[node[0]:node[1]]
    return True


def _put_block(lines: List[str], path: List[str], key: str, value: Any) -> bool:
    """Replaces (or appends) a nested value (mapping / list) under `path` as block YAML."""
    node = _locate(lines, path)
    if node is None:
        return False
    _remove_key(lines, path + [key])
    node = _locate(lines, path)
    start, end, indent = node
    ci = _child_indent(lines, start, end, indent)
    text = yaml.safe_dump({key: value}, default_flow_style=False, sort_keys=False, width=10_000)
    lines[end:end] = [" " * ci + l if l.strip() else l for l in text.rstrip("\n").split("\n")]
    return True


def _active(profiles_text: str):
    data = yaml.safe_load(profiles_text) or {}
    name = next((k for k, v in data.items() if isinstance(v, dict) and isinstance(v.get("outputs"), dict)), None)
    if not name:
        raise ValueError("profiles.yml has no profile with outputs.")
    target = os.getenv("DBT_TARGET") or data[name].get("target") or next(iter(data[name]["outputs"]))
    if target not in data[name]["outputs"]:
        raise ValueError(f"target '{target}' is not one of the profile's outputs.")
    return data, name, target


def read_settings(profiles_text: str, project_text: str) -> Dict[str, Any]:
    data, profile, target = _active(profiles_text)
    out = data[profile]["outputs"][target]
    root = str(out.get("root_path") or "")
    kind, mount_id = "local", None
    if root.lower().startswith("s3://"):
        kind = "s3"
        bucket = root[5:].split("/", 1)[0]
        mount_id = next((m["id"] for m in s3_mounts() if (m.get("config") or {}).get("bucket") == bucket), None)
    project = yaml.safe_load(project_text) or {}
    pname = project.get("name")
    configured = ((project.get("models") or {}).get(pname) or {}) if pname else {}
    folders = []
    models_dir = os.path.join(_project_dir(), "models")
    on_disk = sorted(d for d in os.listdir(models_dir) if os.path.isdir(os.path.join(models_dir, d))) if os.path.isdir(models_dir) else []
    for name in list(dict.fromkeys([k for k, v in configured.items() if isinstance(v, dict)] + on_disk)):
        cfg = configured.get(name) if isinstance(configured.get(name), dict) else {}
        folders.append({"path": name, "materialized": cfg.get("+materialized") or cfg.get("materialized")})
    return {"profile": profile, "target_name": target, "adapter": out.get("type"), "storage": {"kind": kind, "mount_id": mount_id, "root_path": root},
            "schema": out.get("schema"), "threads": out.get("threads"), "project": pname, "folders": folders}


def apply_settings(profiles_text: str, project_text: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Returns the two files with `settings` applied as minimal text edits, plus what changed. Nothing is saved."""
    data, profile, target = _active(profiles_text)
    plines, jlines = profiles_text.replace("\r\n", "\n").split("\n"), project_text.replace("\r\n", "\n").split("\n")
    base = [profile, "outputs", target]
    changed: List[str] = []
    cur = data[profile]["outputs"][target]

    if "schema" in settings and settings["schema"] not in (None, "") and settings["schema"] != cur.get("schema"):
        if not _IDENT.match(str(settings["schema"])):
            raise ValueError("The schema must be letters, digits and underscores (starting with a letter or underscore).")
        _set_key(plines, base, "schema", str(settings["schema"]))
        changed.append(f"schema: {settings['schema']}")
    if "threads" in settings and settings["threads"] not in (None, "") and int(settings["threads"]) != cur.get("threads"):
        threads = int(settings["threads"])
        if not 1 <= threads <= 64:
            raise ValueError("Threads must be between 1 and 64.")
        _set_key(plines, base, "threads", threads)
        changed.append(f"threads: {threads}")

    storage = settings.get("storage")
    if storage:
        want_s3 = storage.get("kind") == "s3"
        cur_root = str(cur.get("root_path") or "")
        if want_s3:
            mount = next((m for m in s3_mounts() if m["id"] == storage.get("mount_id")), None)
            if not mount:
                raise LookupError(f"'{storage.get('mount_id')}' is not a configured S3 mount.")
            bucket = ((mount.get("config") or {}).get("bucket") or "").strip()
            if not bucket:
                raise ValueError("This mount has no bucket configured.")
            snippet = _s3_output_snippet(mount, bucket)
            if cur_root != f"s3://{bucket}" or cur.get("storage_options") != snippet["storage_options"] or cur.get("secrets") != snippet["secrets"]:
                _set_key(plines, base, "root_path", f"s3://{bucket}")
                _put_block(plines, base, "storage_options", snippet["storage_options"])
                _put_block(plines, base, "secrets", snippet["secrets"])
                if not {"httpfs", "delta"} <= set(cur.get("extensions") or []):
                    _put_block(plines, base, "extensions", sorted(set((cur.get("extensions") or []) + ["httpfs", "delta"])))
                changed.append(f"storage: S3 mount {mount.get('name') or mount['id']} (s3://{bucket})")
        elif cur_root.lower().startswith("s3://") or cur.get("storage_options") or cur.get("secrets"):
            _set_key(plines, base, "root_path", os.getenv("WAREHOUSE_DIR", "/workspace/warehouse"))
            _remove_key(plines, base + ["storage_options"])
            _remove_key(plines, base + ["secrets"])
            changed.append("storage: local warehouse")

    project = yaml.safe_load(project_text) or {}
    pname = project.get("name")
    for f in settings.get("folders") or []:
        mat = f.get("materialized")
        if not mat:
            continue
        if mat not in MATERIALIZATIONS:
            raise ValueError(f"Materialization must be one of: {', '.join(MATERIALIZATIONS)}.")
        if not _IDENT.match(str(f.get("path"))):
            raise ValueError("Folder names are letters, digits and underscores.")
        cfg = (((project.get("models") or {}).get(pname) or {}).get(f["path"]) or {}) if pname else {}
        if isinstance(cfg, dict) and (cfg.get("+materialized") == mat):
            continue
        if not _set_key(jlines, ["models", pname, f["path"]], "+materialized", mat):     # the folder is not configured yet: create what is missing
            if _locate(jlines, ["models"]) is None:
                jlines.extend(["", "models:"])
            if _locate(jlines, ["models", pname]) is None:
                _put_block(jlines, ["models"], pname, {f["path"]: {"+materialized": mat}})
            else:
                _put_block(jlines, ["models", pname], f["path"], {"+materialized": mat})
        changed.append(f"models/{f['path']}: +materialized: {mat}")
    return {"profiles.yml": "\n".join(plines), "dbt_project.yml": "\n".join(jlines), "changed": changed}


def _s3_output_snippet(mount: Dict[str, Any], bucket: str) -> Dict[str, Any]:
    cfg = mount.get("config") or {}
    p = f"DKW_MOUNT_{_env_id(mount)}_"
    ev = lambda suffix: '{{ env_var("' + p + suffix + '") }}'         # double quotes inside: the YAML stays single-quoted and readable
    use_ssl = str(cfg.get("use_ssl", False)).lower() in ("true", "1") or str(cfg.get("endpoint", "")).lower().startswith("https://")
    return {"storage_options": {"AWS_ENDPOINT_URL": ev("ENDPOINT_URL"), "AWS_ACCESS_KEY_ID": ev("KEY_ID"), "AWS_SECRET_ACCESS_KEY": ev("SECRET"),
                                "AWS_REGION": ev("REGION"), "AWS_ALLOW_HTTP": "false" if use_ssl else "true", "AWS_S3_ALLOW_UNSAFE_RENAME": "true"},
            "secrets": [{"type": "s3", "key_id": ev("KEY_ID"), "secret": ev("SECRET"), "endpoint": ev("ENDPOINT"), "url_style": ev("URL_STYLE"),
                         "use_ssl": '{{ env_var("' + p + 'USE_SSL") | as_bool }}', "region": ev("REGION")}]}


def s3_profile(mount_id: str) -> str:
    """profiles.yml with its active target pointed at an S3 mount (comments and everything else kept; see apply_settings)."""
    with open(_path("profiles.yml"), encoding="utf-8") as f:
        text = f.read()
    with open(_path("dbt_project.yml"), encoding="utf-8") as f:
        project = f.read()
    return apply_settings(text, project, {"storage": {"kind": "s3", "mount_id": mount_id}})["profiles.yml"]
