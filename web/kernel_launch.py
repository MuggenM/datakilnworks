"""
Launcher for the studio's local notebook kernels (see web/kernel_isolation.py). Started as root by a `dkw-local-<uid>` kernelspec
with `python -I`, so only the standard library is imported here and nothing from the kernel's home can shadow it.

It prepares the user's home, copies the connection file and the IPython startup files into it, drops to the user's uid (supplementary
group: the warehouse data group only) and execve()s ipykernel with a clean, allow-listed environment. execve matters: after it,
/proc/self/environ of the kernel holds only that environment, not the studio's (which carries tokens).

argv: kernel_launch.py <connection_file>
env:  DKW_KERNEL_UID, DKW_KERNEL_GID, DKW_KERNEL_HOME, DKW_KERNEL_SHIM, DKW_KERNEL_HISTORY_HOOK (from the kernelspec)
"""

import os
import resource
import sys

if os.geteuid() != 0:
    raise SystemExit("kernel_launch.py must start as root (it drops to the user's uid itself)")

uid = int(os.environ["DKW_KERNEL_UID"])
gid = int(os.environ["DKW_KERNEL_GID"])
home = os.environ["DKW_KERNEL_HOME"]
if uid < 1000 or gid < 1000:
    raise SystemExit("refusing to start a kernel for a system uid or gid")

with open(sys.argv[1], "rb") as f:                 # root-owned, mode 0600; the kernel gets its own copy
    connection = f.read()

ipython_dir = os.path.join(home, ".ipython")
tmp = os.path.join(home, "tmp")
os.makedirs(os.path.dirname(home), mode=0o711, exist_ok=True)     # root-owned: the user cannot replace their home itself
try:
    os.mkdir(home, 0o700)
except FileExistsError:
    pass
_D = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _subdir(parent_fd: int, name: str) -> int:
    """Open (creating) a directory below parent_fd without ever following a link: the user's earlier kernel runs as the same uid and
    could swap a directory for a symlink at any moment, so every step is relative to an already opened directory."""
    try:
        os.mkdir(name, 0o700, dir_fd=parent_fd)
    except FileExistsError:
        pass
    try:
        fd = os.open(name, _D, dir_fd=parent_fd)
    except OSError:                                 # a link or a file where the directory belongs: remove it (never follow it)
        os.unlink(name, dir_fd=parent_fd)
        os.mkdir(name, 0o700, dir_fd=parent_fd)
        fd = os.open(name, _D, dir_fd=parent_fd)
    os.fchown(fd, uid, uid)
    return fd


def _write_file(dir_fd: int, name: str, data: bytes, mode: int) -> None:
    """Root replaces the file with a fresh one (O_EXCL|O_NOFOLLOW: never writes through a link the user planted)."""
    try:
        os.unlink(name, dir_fd=dir_fd)
    except FileNotFoundError:
        pass
    except IsADirectoryError:
        raise SystemExit(f"refusing: {name} is a directory")
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode, dir_fd=dir_fd)
    with os.fdopen(fd, "wb") as f:
        f.write(data)
        os.fchown(f.fileno(), uid, uid)


home_fd = os.open(home, _D)
os.fchown(home_fd, uid, uid)
os.fchmod(home_fd, 0o700)
os.close(_subdir(home_fd, "tmp"))
ipy_fd = _subdir(home_fd, ".ipython")
prof_fd = _subdir(ipy_fd, "profile_default")
startup_fd = _subdir(prof_fd, "startup")
for src, name in ((os.environ.get("DKW_KERNEL_SHIM", ""), "00_databricks_shim.py"),
                  (os.environ.get("DKW_KERNEL_HISTORY_HOOK", ""), "01_dkw_history.py")):
    if src and os.path.isfile(src):
        with open(src, "rb") as f:
            _write_file(startup_fd, name, f.read(), 0o600)
_write_file(home_fd, ".kernel.json", connection, 0o600)


def _link_extensions(shared: str) -> None:
    """Mirror <shared>/<version>/<platform>/<file> as symlinks in ~/.duckdb/extensions (real, user-owned directories, so the user can
    still install further extensions of their own). An existing entry of the user's is left alone."""
    if not shared or not os.path.isdir(shared):
        return
    duck_fd = _subdir(home_fd, ".duckdb")
    ext_fd = _subdir(duck_fd, "extensions")
    for version in sorted(os.listdir(shared)):
        vdir = os.path.join(shared, version)
        if not os.path.isdir(vdir) or os.path.islink(vdir):
            continue
        v_fd = _subdir(ext_fd, version)
        for platform in sorted(os.listdir(vdir)):
            pdir = os.path.join(vdir, platform)
            if not os.path.isdir(pdir) or os.path.islink(pdir):
                continue
            p_fd = _subdir(v_fd, platform)
            for name in os.listdir(pdir):
                try:
                    os.symlink(os.path.join(pdir, name), name, dir_fd=p_fd)
                    os.chown(name, uid, uid, dir_fd=p_fd, follow_symlinks=False)
                except FileExistsError:
                    pass
            os.close(p_fd)
        os.close(v_fd)
    os.close(ext_fd)
    os.close(duck_fd)


_link_extensions(os.environ.get("DKW_KERNEL_DUCKDB_EXT", ""))
for fd in (startup_fd, prof_fd, ipy_fd, home_fd):
    os.close(fd)
own_connection = os.path.join(home, ".kernel.json")

PASS = ("LANG", "LC_ALL", "TZ", "WAREHOUSE_DIR", "NOTEBOOKS_DIR")
env = {k: os.environ[k] for k in PASS if k in os.environ}
for k in (os.environ.get("NOTEBOOK_KERNEL_ENV_PASSTHROUGH", "") or "").split(","):
    k = k.strip()
    if k and k in os.environ:
        env[k] = os.environ[k]
env.update({
    "PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": home, "TMPDIR": tmp, "IPYTHONDIR": ipython_dir,
    "MPLCONFIGDIR": os.path.join(home, ".mpl"), "PYTHONUNBUFFERED": "1", "PYTHONUSERBASE": os.path.join(home, ".local"),
})
env.setdefault("LANG", "C.UTF-8")

resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

os.setgroups([gid])
os.setgid(uid)
os.setuid(uid)
try:
    os.setuid(0)
except PermissionError:
    pass
else:
    raise SystemExit("could not drop privileges")
os.umask(0o002)
os.chdir(home)
os.execve(sys.executable, [sys.executable, "-m", "ipykernel_launcher", "-f", own_connection], env)
