"""The short memory of compute-node endpoints that could not be connected to (web/compute_reachability.py).

A studio without a compute node used to pay the 3 s connect timeout on EVERY query before falling back to Ray /
in-process. This checks the module's rules, then real /api/sql/execute calls through the studio app (TestClient, a
throwaway warehouse) against a local stand-in for a missing node: a listening socket whose accept queue is full, so a
connect hangs until the timeout exactly like a node that does not answer. Every address is 127.0.0.1 (never a made-up
hostname: a DNS search domain can resolve one to a real host). Then a fake compute node takes over the same port and a
resume of the warehouse (warehouse_lifecycle._resume_locked, container control stubbed) makes the next query use it.

Run in the image:  ci/run_test.sh compute_reachability
"""
import json
import os
import socket
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

FAILS = []


def check(label, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f"  ({detail})" if detail and not ok else ""), flush=True)
    if not ok:
        FAILS.append(label)


def blackhole():
    """127.0.0.1:<port> that accepts no connection: the accept queue is filled, so the kernel drops further SYNs."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(0)
    fillers = []
    for _ in range(3):
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.setblocking(False)
        try:
            c.connect(("127.0.0.1", srv.getsockname()[1]))
        except BlockingIOError:
            pass
        fillers.append(c)
    time.sleep(0.3)
    return srv, fillers


class FakeNode(BaseHTTPRequestHandler):
    calls = 0

    def do_POST(self):
        FakeNode.calls += 1
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        body = json.dumps({"success": True, "columns": [{"name": "answer", "type": "INTEGER"}], "rows": [{"answer": 7}],
                           "row_count": 1, "elapsed_ms": 1, "executed_by": "fake-node", "is_mutation": False}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def module_rules():
    print("module rules")
    import httpx
    from web import compute_reachability as cr
    os.environ["COMPUTE_UNREACHABLE_TTL"] = "1"
    ep = "http://127.0.0.1:9"
    check("an endpoint never seen is not skipped", not cr.is_skipped(ep))
    cr.mark_unreachable(ep)
    check("after a failed connect it is skipped", cr.is_skipped(ep))
    check("a trailing slash is the same endpoint", cr.is_skipped(ep + "/"))
    check("another endpoint is not affected", not cr.is_skipped("http://127.0.0.1:10"))
    time.sleep(1.1)
    check("the TTL is read when marking and the entry expires", not cr.is_skipped(ep))
    cr.mark_unreachable(ep)
    cr.mark_reachable(ep)
    check("an answer from the node clears it", not cr.is_skipped(ep))
    cr.mark_unreachable(ep)
    cr.forget([None, ep])
    check("forget clears it (and ignores None)", not cr.is_skipped(ep))
    os.environ["COMPUTE_UNREACHABLE_TTL"] = "0"
    cr.mark_unreachable(ep)
    check("COMPUTE_UNREACHABLE_TTL=0 turns the cache off", not cr.is_skipped(ep))
    os.environ["COMPUTE_UNREACHABLE_TTL"] = "nonsense"
    check("an unparsable TTL falls back to 30 s", cr.ttl() == 30.0)
    req = httpx.Request("POST", ep)
    check("refused / unresolvable counts as a connect failure", cr.is_connect_failure(httpx.ConnectError("x", request=req)))
    check("a connect timeout counts", cr.is_connect_failure(httpx.ConnectTimeout("x", request=req)))
    check("a READ timeout does not (the node answered, the query was slow)", not cr.is_connect_failure(httpx.ReadTimeout("x", request=req)))
    check("any other error does not", not cr.is_connect_failure(ValueError("x")))


def through_the_route():
    print("real /api/sql/execute, no compute node")
    srv, fillers = blackhole()
    port = srv.getsockname()[1]
    os.environ["WH_STARTER_ENDPOINT"] = f"http://127.0.0.1:{port}"
    os.environ["COMPUTE_UNREACHABLE_TTL"] = "8"
    from fastapi.testclient import TestClient
    from web import app as app_module, auth, compute_reachability as cr, warehouse_lifecycle, warehouses, container_control
    with auth.get_db_connection() as c:
        c.execute("UPDATE users SET must_change_password = 0")
    tok = auth.create_access_token(auth.get_user_by_username("admin"))
    wh = warehouses.resolve_sql_warehouse("wh_starter")
    check("the warehouse points at the local stand-in", wh.get("endpoint") == f"http://127.0.0.1:{port}", wh.get("endpoint"))

    with TestClient(app_module.app) as cl:
        def run(q):
            t0 = time.perf_counter()
            j = cl.post("/api/sql/execute", json={"query": q, "warehouse_id": "wh_starter"},
                        headers={"Authorization": f"Bearer {tok}"}).json()
            return time.perf_counter() - t0, j

        def answer(j):
            rows = j.get("rows") or []
            return rows[0].get("answer") if rows and isinstance(rows[0], dict) else None

        cl.post("/api/sql/execute", json={"query": "SELECT 1 AS answer", "warehouse_id": "wh_starter"},
                headers={"Authorization": f"Bearer {tok}"})              # starts Ray; this one pays the connect timeout
        cr.forget([wh["endpoint"]])
        t1, j1 = run("SELECT 42 AS answer")
        check("a query that has to try the missing node waits for the connect timeout", t1 >= 2.9, f"{t1:.2f}s")
        check("... and is answered by the fallback", answer(j1) == 42, str(j1)[:200])
        t2, j2 = run("SELECT 43 AS answer")
        check("the next query skips the node (no 3 s wait)", t2 < 1.5, f"{t2:.2f}s")
        check("... and gets the same fallback answer", answer(j2) == 43 and j2.get("executed_by") == j1.get("executed_by"), str(j2)[:200])
        while cr.is_skipped(wh["endpoint"]):
            time.sleep(0.2)
        t3, j3 = run("SELECT 44 AS answer")
        check("after the TTL the node is tried again", t3 >= 2.9 and answer(j3) == 44, f"{t3:.2f}s")

        # the node comes back on the same port while the endpoint is still marked unreachable
        for c in fillers:
            c.close()
        srv.close()
        node = ThreadingHTTPServer(("127.0.0.1", port), FakeNode)
        threading.Thread(target=node.serve_forever, daemon=True).start()
        check("(the endpoint is still marked unreachable)", cr.is_skipped(wh["endpoint"]))
        t4, j4 = run("SELECT 45 AS answer")
        check("within the TTL the returned node is not used yet (the accepted trade-off)",
              FakeNode.calls == 0 and answer(j4) == 45, f"calls={FakeNode.calls} {str(j4)[:120]}")

        # a resume of the warehouse's container (controller stubbed: the real one is never involved here)
        saved = (warehouse_lifecycle.service_of, container_control.status, container_control.act, container_control.wait_healthy)
        warehouse_lifecycle.service_of = lambda w: "stub-node"
        container_control.status = lambda s: {"state": "exited"}
        container_control.act = lambda s, a: {"ok": True}
        container_control.wait_healthy = lambda ep, timeout=60.0, interval=0.5: False
        try:
            warehouse_lifecycle._resume_locked(dict(wh))
            check("a resume whose worker never answered /health keeps the endpoint skipped", cr.is_skipped(wh["endpoint"]))
            container_control.wait_healthy = lambda ep, timeout=60.0, interval=0.5: True
            warehouse_lifecycle._resume_locked(dict(wh))
            check("a resume whose worker answered /health forgets it", not cr.is_skipped(wh["endpoint"]))
        finally:
            (warehouse_lifecycle.service_of, container_control.status, container_control.act, container_control.wait_healthy) = saved
        t5, j5 = run("SELECT 46 AS answer")
        check("the next query after the resume goes to the node", FakeNode.calls == 1 and j5.get("executed_by") == "fake-node",
              f"calls={FakeNode.calls} {t5:.2f}s {str(j5)[:160]}")
        node.shutdown()


if __name__ == "__main__":
    os.environ["WAREHOUSE_DIR"] = tempfile.mkdtemp(prefix="dkw_reach_")
    os.environ["INIT_ADMIN_USERNAME"] = "admin"
    os.environ["INIT_ADMIN_PASSWORD_HASH"] = ("pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$"
                                              "e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d")
    os.environ["CONTROLLER_URL"] = ""          # no container control: nothing outside this process is contacted
    for k in ("WH_ANALYTICS_ENDPOINT", "WH_ETL_ENDPOINT"):
        os.environ[k] = "http://127.0.0.1:9"
    module_rules()
    through_the_route()
    print("ALL PASS" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}", flush=True)
    # The fallback started a local Ray from a worker thread; Ray's shutdown (also the one at interpreter exit) then ends the
    # process with status 1 even when every check passed. os._exit skips it, like test_streaming.py / test_workflow_orchestration.py.
    sys.stdout.flush()
    os._exit(1 if FAILS else 0)
