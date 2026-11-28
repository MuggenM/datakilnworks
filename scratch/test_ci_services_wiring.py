#!/usr/bin/env python3
"""Wiring of the tests that use the CI services stack (ci/services, ci/plan.json "services"); no service is started here.

no_silent_skips: a test that needs a service must never pass by skipping it in CI. Every skip path in such a test goes
through scratch/_services.require_or_skip, which fails instead of skipping when REQUIRE_SERVICES=1 (set by ci/run_test.sh
whenever CI_SERVICES is). Also checks the helper itself, and that every profile named in the plan exists in the stack."""
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
FAIL = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:600]}" if detail and not cond else ""))
    if not cond:
        FAIL.append(name)


plan = json.load(open(os.path.join(REPO, "ci", "plan.json")))
entries = [t for t in plan["tests"] if t.get("services")] + [u for u in plan.get("ui", []) if isinstance(u, dict) and u.get("services")]
check("at least one plan entry uses the services stack", len(entries) > 0)

print("no_silent_skips")
SKIP = re.compile(r"\bskip(ping|ped)?\b", re.I)
for e in entries:
    prefix = "test_" if e in plan["tests"] else "verify_"
    path = os.path.join(HERE, f"{prefix}{e['name']}.py")
    lines = open(path).read().splitlines()
    offenders = [f"{i + 1}: {l.strip()[:120]}" for i, l in enumerate(lines)
                 if SKIP.search(l) and ("print(" in l or "sys.exit(0)" in l) and "require_or_skip" not in l]
    check(f"{os.path.basename(path)}: every skip goes through _services.require_or_skip", not offenders, offenders)
    check(f"{os.path.basename(path)}: calls _services.require_or_skip", "require_or_skip(" in "\n".join(lines))

print("profiles named in the plan exist in ci/services/docker-compose.yml")
compose = os.path.join(REPO, "ci", "services", "docker-compose.yml")
try:
    known = set(subprocess.run(["docker", "compose", "-f", compose, "config", "--profiles"], capture_output=True, text=True, timeout=60).stdout.split())
except (OSError, subprocess.TimeoutExpired):
    known = set(re.findall(r"profiles:\s*\[([a-z0-9_-]+)\]", open(compose).read()))      # no docker in this container: read the file
for e in entries:
    missing = [p for p in e["services"].split() if p not in known]
    check(f"{e['name']}: profiles {e['services']!r} exist", not missing, missing)

print("the helper")
sys.path.insert(0, HERE)
import _services   # noqa: E402

import socket   # noqa: E402
listener = socket.socket(); listener.bind(("127.0.0.1", 0)); listener.listen(1)
check("a reachable port is True", _services.require_or_skip("self", "127.0.0.1", listener.getsockname()[1], timeout=1.0) is True)
listener.close()
probe = (f"import sys; sys.path.insert(0, {HERE!r}); import _services; "
         "r = _services.require_or_skip('nothing', '127.0.0.1', 9, timeout=0.5); print('returned', r)")
soft = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, env={**os.environ, "REQUIRE_SERVICES": ""})
check("without REQUIRE_SERVICES an unreachable service is a skip (returns False, exit 0)",
      soft.returncode == 0 and "returned False" in soft.stdout and "skipping" in soft.stdout, soft.stdout + soft.stderr)
hard = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, env={**os.environ, "REQUIRE_SERVICES": "1"})
check("with REQUIRE_SERVICES=1 it is a failure (a FAIL line, exit 1)",
      hard.returncode == 1 and "[FAIL] nothing not reachable at 127.0.0.1:9" in hard.stdout and "returned" not in hard.stdout, hard.stdout + hard.stderr)

empty = (f"import sys; sys.path.insert(0, {HERE!r}); import _services; "
         "r = _services.require_or_skip('lldap', '', 3890, timeout=0.5); print('returned', r)")
e = subprocess.run([sys.executable, "-c", empty], capture_output=True, text=True, env={**os.environ, "REQUIRE_SERVICES": ""})
check("an unset host is a skip without any connection attempt (never a default name that may reach someone's own server)",
      e.returncode == 0 and "returned False" in e.stdout and "not set" in e.stdout, e.stdout + e.stderr)
check("host_port: host:port", _services.host_port("redpanda:9092", 1) == ("redpanda", 9092))
check("host_port: a bare host gets the default port", _services.host_port("azurite", 10000) == ("azurite", 10000))
check("host_port: a URL", _services.host_port("http://redpanda:8081", 1) == ("redpanda", 8081) and _services.host_port("https://x", 1) == ("x", 443))
check("host_port: empty stays empty (never a default host)", _services.host_port("", 9092) == ("", 9092))
for t in ("ldap_auth", "group_sync_ldap"):
    src = open(os.path.join(HERE, f"test_{t}.py")).read()
    check(f"test_{t}.py has no default LDAP host", re.search(r'getenv\("LLDAP_TEST_HOST",\s*"[^"]+"', src) is None)

print()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS")
sys.stdout.flush()
os._exit(1 if FAIL else 0)
