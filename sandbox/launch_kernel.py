"""
Kernel launcher for the notebook sandbox. Runs as root (started by the worker through the `sandbox` kernelspec), prepares the
user's home, then drops to the user's uid before ipykernel starts, so user code never runs with any privilege.

argv: launch_kernel.py <connection_file>          env: SANDBOX_UID, HOME, IPYTHONDIR, SANDBOX_SHIM (set by the worker)
"""

import os
import resource
import runpy
import shutil
import sys

uid = int(os.environ["SANDBOX_UID"])
if uid < 1000:
    raise SystemExit("refusing to start a kernel for a system uid")
home = os.environ["HOME"]
connection_file = sys.argv[1]

with open(connection_file, "rb") as f:            # root-owned, mode 0600; the kernel gets its own copy
    connection = f.read()

profile = os.path.join(os.environ["IPYTHONDIR"], "profile_default")
for path in (home, os.path.join(home, "tmp"), os.environ["IPYTHONDIR"], profile, os.path.join(profile, "startup")):
    os.makedirs(path, mode=0o700, exist_ok=True)
    os.chown(path, uid, uid)
own_connection = os.path.join(home, ".kernel.json")
fd = os.open(own_connection, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "wb") as f:
    f.write(connection)
os.chown(own_connection, uid, uid)
startup = os.path.join(os.environ["IPYTHONDIR"], "profile_default", "startup", "00_sandbox_shim.py")
shutil.copyfile(os.environ["SANDBOX_SHIM"], startup)
os.chown(startup, uid, uid)
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
resource.setrlimit(resource.RLIMIT_NOFILE, (4096, 4096))
resource.setrlimit(resource.RLIMIT_NPROC, (512, 512))

os.setgroups([])
os.setgid(uid)
os.setuid(uid)
os.umask(0o077)
os.chdir(home)
for name in ("SANDBOX_UID", "SANDBOX_SHIM"):
    os.environ.pop(name, None)

sys.argv = ["ipykernel_launcher", "-f", own_connection]
runpy.run_module("ipykernel_launcher", run_name="__main__")
