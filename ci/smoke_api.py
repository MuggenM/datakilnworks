#!/usr/bin/env python3
"""HTTP checks of a running studio, shared by the cluster smoke test (through a port-forward) and by anything that can give it a URL.
  ci/smoke_api.py first  <base-url>   sign in with the bootstrap admin, the forced password change, SQL on the studio and on the compute node, write a table
  ci/smoke_api.py again  <base-url>   after a restart: the CHANGED password still works and the table is still there
Standard library only; exit code 0 = all checks passed."""
import http.cookiejar, json, sys, time, urllib.error, urllib.request

OLD_PW, NEW_PW = "adminpassword123", "SmokeTest-Passw0rd"
FAIL = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAIL.append(name)


class Client:
    def __init__(self, base):
        self.base = base.rstrip("/")
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.jar))

    def call(self, method, path, body=None, timeout=120):
        req = urllib.request.Request(self.base + path, method=method, data=json.dumps(body).encode() if body is not None else None, headers={"Content-Type": "application/json"})
        try:
            with self.opener.open(req, timeout=timeout) as r:
                raw = r.read().decode() or "{}"
                return r.status, (json.loads(raw) if raw.strip().startswith(("{", "[")) else {"raw": raw})
        except urllib.error.HTTPError as e:
            raw = e.read().decode() or "{}"
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {"raw": raw}


def wait_ready(c, seconds=180):
    end = time.time() + seconds
    while time.time() < end:
        try:
            if c.call("GET", "/readyz", timeout=5)[0] == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def first(c):
    check("/healthz and /readyz answer", c.call("GET", "/healthz")[0] == 200 and c.call("GET", "/readyz")[0] == 200)
    st, d = c.call("POST", "/api/auth/login", {"username": "admin", "password": OLD_PW})
    check("the bootstrap admin can sign in", st == 200 and d.get("success") is True, d)
    check("the first password must be changed (forced)", (d.get("user") or {}).get("must_change_password") is True, d)
    check("other API calls are refused until then (403)", c.call("GET", "/api/sql-warehouses")[0] == 403)
    st, d = c.call("POST", "/api/auth/change-password", {"current_password": OLD_PW, "new_password": NEW_PW})
    check("the password is changed", st == 200, d)
    st, d = c.call("POST", "/api/auth/login", {"username": "admin", "password": NEW_PW})
    check("...and the new password signs in", st == 200 and d.get("success") is True, d)
    st, d = c.call("POST", "/api/sql/execute", {"query": "SELECT 42 AS x"})
    check("SQL runs on the studio's default warehouse", st == 200 and d.get("success") and d["rows"][0]["x"] == 42, d)
    st, d = c.call("POST", "/api/sql/execute", {"query": "SELECT 43 AS x", "warehouse_id": "wh_starter"})
    check("SQL runs on the starter warehouse", st == 200 and d.get("success") and d["rows"][0]["x"] == 43, d)
    check("...executed by the compute node (not in the studio)", "compute-node" in json.dumps(d.get("executed_by", "")).lower(), d.get("executed_by"))
    st, d = c.call("POST", "/api/sql/execute", {"query": "CREATE TABLE smoke_persist AS SELECT 7 AS n"})
    check("a table is written to the warehouse volume", st == 200 and d.get("success"), d)


def again(c):
    check("/readyz answers after the restart", wait_ready(c))
    st, d = c.call("POST", "/api/auth/login", {"username": "admin", "password": NEW_PW})
    check("the CHANGED password still works (accounts live on the volume)", st == 200 and d.get("success") is True, d)
    check("(and the bootstrap password no longer does)", c.call("POST", "/api/auth/login", {"username": "admin", "password": OLD_PW})[0] == 401)
    c.call("POST", "/api/auth/login", {"username": "admin", "password": NEW_PW})
    st, d = c.call("POST", "/api/sql/execute", {"query": "SELECT n FROM smoke_persist"})
    check("the table is still there", st == 200 and d.get("success") and d["rows"][0]["n"] == 7, d)


if __name__ == "__main__":
    mode, base = sys.argv[1], sys.argv[2]
    client = Client(base)
    if not wait_ready(client):
        print("  [FAIL] the studio did not become ready")
        sys.exit(1)
    {"first": first, "again": again}[mode](client)
    print("ALL PASS" if not FAIL else "FAILED: " + ", ".join(FAIL))
    sys.exit(1 if FAIL else 0)
