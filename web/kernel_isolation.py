"""
OS-level isolation of the studio's own (non-sandboxed) notebook kernels.

Why: users that no governance policy applies to run notebooks in kernels inside the studio container (the sandbox is only for
policy subjects, see web/notebook_access.py). Those kernels used to run as root with the studio's whole environment, so user code
could read `.metadata/` (the JWT signing secret, auth.db, the Fernet keys, the compute token), `/proc/1/environ` (Git tokens, the
bootstrap admin hash) and `/run/controller/token`: a power user could mint an admin session. Data access for these users is
intended (they are not subject to any policy); platform secrets are not.

How (same pattern as sandbox/launch_kernel.py):
  * every studio user gets a stable uid of their own (>= KERNEL_UID_BASE, persisted in .metadata/kernel_uids.json), so kernels of
    different users cannot read each other's connection files or signal each other;
  * a per-user kernelspec `dkw-local-<uid>` starts web/kernel_launch.py as root, which prepares the user's home, then drops to the
    uid (supplementary group = DATA_GID only) and execve()s ipykernel with a clean, allow-listed environment;
  * `.metadata` is mode 0700 (root only); the rest of the warehouse belongs to group DATA_GID, group-writable, with setgid
    directories, and the studio / compute nodes run with umask 002, so kernels keep reading and writing tables;
  * notebook query history is appended by the kernel to `$HOME/.dkw_history.jsonl` and ingested by the studio after each cell /
    Papermill run, attributed to the kernel's owner (the kernel can no longer write history.db; the old shim wrote every notebook
    query as "admin").

`NOTEBOOK_KERNEL_ISOLATION=off` restores the old behaviour (kernels as the studio's own user). When isolation is on and the studio is
not root (it cannot switch uid), kernels are refused rather than started unisolated: fail closed.
"""

import json
import logging
import os
import stat
import sys
import threading
import uuid
from typing import Dict, Optional

logger = logging.getLogger("localspark.kernel_isolation")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
META_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
UID_MAP = os.path.join(META_DIR, "kernel_uids.json")
HOME_ROOT = os.getenv("NOTEBOOK_KERNEL_HOME_ROOT", "/var/lib/dkw-kernels")
KERNELSPEC_ROOT = os.getenv("NOTEBOOK_KERNELSPEC_ROOT", os.path.join(sys.prefix, "share", "jupyter", "kernels"))
LAUNCHER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kernel_launch.py")
HISTORY_HOOK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kernel_history_hook.py")
SHIM = os.getenv("NOTEBOOK_KERNEL_SHIM", "/root/.ipython/profile_default/startup/00_databricks_shim.py")
DATA_GID = int(os.getenv("NOTEBOOK_KERNEL_DATA_GID", "20000"))
KERNEL_UID_BASE = int(os.getenv("NOTEBOOK_KERNEL_UID_BASE", "20001"))
EXT_SHARED = os.path.join(HOME_ROOT, "duckdb-extensions")
EXTENSIONS = [e.strip() for e in os.getenv("NOTEBOOK_KERNEL_DUCKDB_EXTENSIONS", "delta,httpfs,postgres_scanner,sqlite_scanner,azure,iceberg").split(",") if e.strip()]
SPOOL_NAME = ".dkw_history.jsonl"
MAX_SPOOL_BYTES = 4 * 1024 * 1024
MAX_SPOOL_LINES = 2000

_uid_lock = threading.Lock()


class IsolationUnavailable(RuntimeError):
    """Isolation is on but cannot be provided (the studio is not root): no kernel is started."""


def enabled() -> bool:
    return os.getenv("NOTEBOOK_KERNEL_ISOLATION", "on").strip().lower() not in ("off", "0", "false", "no")


def _require_root() -> None:
    if os.geteuid() != 0:
        raise IsolationUnavailable(
            "Notebook kernels are isolated under their own OS user, which needs the studio to run as root (the default image does). "
            "Run the studio as root, or set NOTEBOOK_KERNEL_ISOLATION=off to accept kernels that can read the studio's secrets.")


def uid_for(owner: str) -> int:
    """A stable, unique uid per studio user (persisted so home directories keep their owner across restarts)."""
    owner = owner or "anonymous"
    with _uid_lock:
        try:
            with open(UID_MAP) as f:
                data = json.load(f)
        except (FileNotFoundError, ValueError):
            data = {}
        if owner in data:
            return int(data[owner])
        uid = max([int(v) for v in data.values()] + [KERNEL_UID_BASE - 1]) + 1
        data[owner] = uid
        os.makedirs(META_DIR, exist_ok=True)
        tmp = f"{UID_MAP}.{uuid.uuid4().hex}"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=1, sort_keys=True)
        os.replace(tmp, UID_MAP)
        return uid


def home_for(uid: int) -> str:
    return os.path.join(HOME_ROOT, str(uid))


def kernelspec_for(owner: str) -> str:
    """Name of the (created on demand) kernelspec that runs `owner`'s kernels under their own uid."""
    _require_root()
    uid = uid_for(owner)
    name = f"dkw-local-{uid}"
    spec_dir = os.path.join(KERNELSPEC_ROOT, name)
    spec = {
        "argv": [sys.executable, "-I", LAUNCHER, "{connection_file}"],
        "display_name": f"Python 3 (Data Kiln Works, uid {uid})",
        "language": "python",
        "env": {"DKW_KERNEL_UID": str(uid), "DKW_KERNEL_GID": str(DATA_GID), "DKW_KERNEL_HOME": home_for(uid),
                "DKW_KERNEL_SHIM": SHIM, "DKW_KERNEL_HISTORY_HOOK": HISTORY_HOOK, "DKW_KERNEL_DUCKDB_EXT": EXT_SHARED},
    }
    path = os.path.join(spec_dir, "kernel.json")
    try:
        with open(path) as f:
            if json.load(f) == spec:
                return name
    except (FileNotFoundError, ValueError):
        pass
    os.makedirs(spec_dir, mode=0o755, exist_ok=True)
    tmp = f"{path}.{uuid.uuid4().hex}"
    with open(tmp, "w") as f:
        json.dump(spec, f, indent=1)
    os.replace(tmp, path)
    return name


def sync_duckdb_extensions() -> int:
    """DuckDB keeps extensions under ~/.duckdb; the studio's are in /root (unreachable for a kernel uid), so a kernel would try to
    download `delta` on its first table and hang without internet. Root installs the usual set (as the studio did on first use
    anyway) and copies them into a shared, root-owned, read-only directory; the launcher links each file into the user's own
    ~/.duckdb/extensions. DuckDB only loads signed extensions, and nobody but root can write the shared directory."""
    try:
        import duckdb
        con = duckdb.connect()
        for ext in EXTENSIONS:
            try:
                con.execute(f"INSTALL {ext}")
            except Exception as exc:
                logger.debug(f"duckdb INSTALL {ext} failed: {exc}")
        src = con.execute("SELECT current_setting('extension_directory')").fetchone()[0] or os.path.expanduser("~/.duckdb/extensions")
        con.close()
    except Exception as exc:
        logger.warning(f"could not prepare DuckDB extensions for notebook kernels: {exc}")
        return 0
    if not src or not os.path.isdir(src):
        src = os.path.expanduser("~/.duckdb/extensions")
    if not os.path.isdir(src):
        return 0
    import shutil
    os.makedirs(EXT_SHARED, exist_ok=True)
    n = 0
    for root, dirs, files in os.walk(src):
        rel = os.path.relpath(root, src)
        dst_dir = os.path.normpath(os.path.join(EXT_SHARED, rel))
        os.makedirs(dst_dir, exist_ok=True)
        os.chmod(dst_dir, 0o755)
        for name in files:
            s, d = os.path.join(root, name), os.path.join(dst_dir, name)
            if os.path.islink(s):
                continue
            if not os.path.exists(d) or os.path.getsize(d) != os.path.getsize(s):
                tmp = f"{d}.{uuid.uuid4().hex}"
                shutil.copyfile(s, tmp)
                os.chmod(tmp, 0o644)
                os.replace(tmp, d)
                n += 1
    os.chmod(EXT_SHARED, 0o755)
    return n


# ---------------------------------------------------------------- file permissions

def harden_permissions(warehouse_dir: Optional[str] = None) -> Dict[str, int]:
    """`.metadata` root-only; everything else in the warehouse owned by group DATA_GID, group-writable, setgid directories.
    Idempotent and cheap on a second run (only entries that differ are touched). Symlinks are never followed or changed."""
    wd = warehouse_dir or WAREHOUSE_DIR
    meta = os.path.join(wd, ".metadata")
    os.makedirs(meta, exist_ok=True)
    os.chmod(meta, 0o700)
    changed = 0
    seen = 0
    for root, dirs, files in os.walk(wd):
        if os.path.abspath(root) == os.path.abspath(wd):
            dirs[:] = [d for d in dirs if d != ".metadata"]
        for name, is_dir in [(None, True)] + [(f, False) for f in files]:
            p = root if name is None else os.path.join(root, name)
            try:
                st = os.lstat(p)
            except OSError:
                continue
            if stat.S_ISLNK(st.st_mode):
                continue
            seen += 1
            want = (st.st_mode | 0o2070) if is_dir else (st.st_mode | 0o060)
            try:
                if st.st_gid != DATA_GID:
                    os.lchown(p, -1, DATA_GID)
                    changed += 1
                if stat.S_IMODE(st.st_mode) != stat.S_IMODE(want):
                    os.chmod(p, stat.S_IMODE(want))
                    changed += 1
            except OSError as exc:
                logger.debug(f"could not adjust {p}: {exc}")
    return {"entries": seen, "changed": changed}


def prepare_studio() -> None:
    """Called at studio start: umask for group-writable data, `.metadata` locked down, home root, data group permissions."""
    os.umask(0o002)
    if not enabled():
        logger.warning("NOTEBOOK_KERNEL_ISOLATION=off: notebook kernels run as the studio's own user and can read its secrets.")
        return
    if os.geteuid() != 0:
        logger.error("Notebook kernel isolation is on but the studio is not root: local notebook kernels will be refused.")
        return
    os.makedirs(HOME_ROOT, exist_ok=True)
    os.chmod(HOME_ROOT, 0o711)
    try:
        os.makedirs(META_DIR, exist_ok=True)
        os.chmod(META_DIR, 0o700)
    except OSError as exc:
        logger.error(f"could not restrict {META_DIR}: {exc}")

    def _walk():
        try:
            res = harden_permissions()
            logger.info(f"warehouse permissions for isolated kernels: {res['entries']} entries checked, {res['changed']} changes")
        except Exception as exc:
            logger.error(f"warehouse permission pass failed: {exc}")
        try:
            n = sync_duckdb_extensions()
            logger.info(f"DuckDB extensions for notebook kernels ready ({n} file(s) copied)")
        except Exception as exc:
            logger.error(f"DuckDB extension sync failed: {exc}")
    threading.Thread(target=_walk, name="kernel-perm-pass", daemon=True).start()


# ---------------------------------------------------------------- notebook query history

def ingest_history(owner: str) -> int:
    """Moves the kernel's spooled query history into history.db, attributed to `owner` (never to what the file claims).
    The spool lives in a directory the kernel owns, so it is opened without following links and must be a regular file owned by
    the kernel's uid (a link to a root-owned secret is never read)."""
    if not enabled() or os.geteuid() != 0:
        return 0
    uid = uid_for(owner)
    path = os.path.join(home_for(uid), SPOOL_NAME)
    taken = f"{path}.ingest.{uuid.uuid4().hex}"
    try:
        os.rename(path, taken)
    except OSError:
        return 0
    data = b""
    try:
        fd = os.open(taken, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            st = os.fstat(fd)
            if stat.S_ISREG(st.st_mode) and st.st_uid == uid:
                data = os.read(fd, MAX_SPOOL_BYTES)
        finally:
            os.close(fd)
    except OSError:
        pass
    finally:
        try:
            os.unlink(taken)
        except OSError:
            pass
    if not data:
        return 0
    from web.audit import log_query
    n = 0
    for line in data.decode("utf-8", "replace").splitlines()[:MAX_SPOOL_LINES]:
        try:
            e = json.loads(line)
            text = str(e.get("q", ""))[:100_000]
            if not text.strip():
                continue
            status = "SUCCESS" if e.get("status") == "SUCCESS" else "FAILED"
            err = e.get("err")
            log_query(text.strip(), float(e.get("ms", 0) or 0), rows_produced=max(0, int(e.get("rows", 0) or 0)), status=status,
                      error_message=str(err)[:4000] if err else None, client="NOTEBOOK", user=owner)
            n += 1
        except Exception:
            continue
    return n
