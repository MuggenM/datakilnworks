"""Reachability of an outside service a test needs (lldap, Gitea, Redpanda, ...), shared by the tests that use the CI services stack.

Run by hand without the service, such a test skips (exit 0) so it never blocks unrelated work. In CI, where ci/run_test.sh sets
REQUIRE_SERVICES=1 whenever CI_SERVICES is set, a missing service is a FAILURE: a green job must mean the test really ran."""
import os
import socket
import sys


def require_or_skip(name: str, host: str, port: int, timeout: float = 2.0) -> bool:
    """True when host:port accepts a TCP connection. Otherwise: with REQUIRE_SERVICES=1 print a [FAIL] line and exit 1,
    else print why the test is skipping and return False (the caller then exits 0).
    An empty `host` (the test's host variable is not set) is never connected to: tests have no default host, because a
    default name such as `lldap` can resolve to someone's own server on a shared network, which these tests would modify."""
    if host:
        try:
            socket.create_connection((host, port), timeout=timeout).close()
            return True
        except OSError:
            why = f"not reachable at {host}:{port}"
    else:
        why = "host not set (ci/run_test.sh with CI_SERVICES sets it from the stack)"
    if os.environ.get("REQUIRE_SERVICES") == "1":
        print(f"  [FAIL] {name} {why} (REQUIRE_SERVICES=1: a missing service fails the test)")
        sys.stdout.flush()
        os._exit(1)
    print(f"{name} {why} -- skipping (start it with ci/services/up.sh, or set the test's host variable).")
    return False
