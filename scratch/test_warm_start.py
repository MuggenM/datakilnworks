#!/usr/bin/env python3
"""Warm start of suspended compute nodes (web/warehouse_lifecycle.py, container_control.suspend_mode, warehouses.clean_standby, the worker's
/api/compute/warmup) with a fake container controller and a throwaway warehouse:
docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_warm_start.py"""
import os, sys, tempfile, time
TMP = tempfile.mkdtemp(prefix="warm_"); os.environ["WAREHOUSE_DIR"] = TMP + "/warehouse"; os.makedirs(TMP + "/warehouse")
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
os.environ["WAREHOUSE_SUSPEND_MODE"] = "stop"; os.environ["AUTOSUSPEND_TIME_SCALE"] = "1"
sys.path.insert(0, "/workspace")
import datetime
from web import container_control as cc, warehouses, warehouse_lifecycle as wl
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def refuses(fn, *a, frag="", **k):
    try: fn(*a, **k); return False
    except ValueError as e: return frag.lower() in str(e).lower()

print("settings")
cs = warehouses.clean_standby
check("defaults and values", cs({"standby_mode": ""}) == {"standby_mode": ""} and cs({"standby_mode": "PAUSE", "warm_hold_mins": "30", "warm_tables": "a.b.c, d.e.f a.b.c"}) == {"standby_mode": "pause", "warm_hold_mins": 30, "warm_tables": ["a.b.c", "d.e.f"]})
check("bad mode, hold and table names are refused", refuses(cs, {"standby_mode": "hibernate"}, frag="standby_mode") and refuses(cs, {"warm_hold_mins": -1}, frag="between") and refuses(cs, {"warm_hold_mins": "x"}, frag="whole number")
      and refuses(cs, {"warm_tables": ["a.b"]}, frag="not a catalog") and refuses(cs, {"warm_tables": ["a.b.c; drop table x"]}, frag="not a catalog") and refuses(cs, {"warm_tables": [f"a.b.t{i}" for i in range(11)]}, frag="at most 10"))
check("a value that is not sent is not returned (update keeps the old one)", cs({"name": "x"}) == {})
check("the per-warehouse mode wins over the deployment default", cc.suspend_mode("pause") == "pause" and cc.suspend_mode("stop") == "stop" and cc.suspend_mode("") == "stop" and cc.suspend_mode(None) == "stop")
os.environ["WAREHOUSE_SUSPEND_MODE"] = "pause"; check("...and the default applies without one", cc.suspend_mode("") == "pause" and cc.suspend_mode("stop") == "stop"); os.environ["WAREHOUSE_SUSPEND_MODE"] = "stop"

print("lifecycle with a fake controller")
state = {"n1": "running", "n2": "running"}; acts = []; posts = []
cc.configured = lambda: True
cc.list_containers = lambda max_age=5.0: [{"service": k, "state": v} for k, v in state.items()]
cc.status = lambda svc: {"service": svc, "state": state[svc]}
def act(svc, a):
    acts.append((svc, a)); state[svc] = {"pause": "paused", "unpause": "running", "stop": "exited", "start": "running"}[a]
cc.act = act; cc.wait_healthy = lambda ep, t=60, i=0.5: True
class R:
    def __init__(s, code=200, js=None): s.status_code, s._j = code, js or {}
    def json(s): return s._j
import httpx
mode = {"fail": False}
def post(url, json=None, headers=None, timeout=None):
    posts.append((url, json))
    if mode["fail"]: raise httpx.ConnectError("boom")
    return R(200, {"tables": {t: ("ok" if t != "bad.bad.bad" else "does not exist") for t in json["tables"]}})
httpx.post = post
warm = warehouses.create_sql_warehouse("Warm", endpoint="http://n1:8001", auto_stop_mins=1, standby={"standby_mode": "pause", "warm_hold_mins": 30, "warm_tables": "warehouse.sales.orders"})
cold = warehouses.create_sql_warehouse("Cold", endpoint="http://n2:8002", auto_stop_mins=1, standby={"warm_tables": ["warehouse.sales.orders"]})
check("the warehouse stores its warm-start settings", warm["standby_mode"] == "pause" and warm["warm_hold_mins"] == 30 and warm["warm_tables"] == ["warehouse.sales.orders"] and cold.get("standby_mode", "") == "")
u = warehouses.update_sql_warehouse(warm["id"], {"warm_hold_mins": 60}); check("an update changes only what it sends", u["warm_hold_mins"] == 60 and u["standby_mode"] == "pause" and u["warm_tables"] == ["warehouse.sales.orders"])
check("an invalid update is refused and changes nothing", refuses(warehouses.update_sql_warehouse, warm["id"], {"warm_hold_mins": 5, "standby_mode": "x"}, frag="standby_mode") and warehouses.get_sql_warehouse(warm["id"])["warm_hold_mins"] == 60)
warehouses.update_sql_warehouse(warm["id"], {"warm_hold_mins": 30})
check("describe reports the effective mode per warehouse", wl.describe(warm)["suspend_mode"] == "pause" and wl.describe(cold)["suspend_mode"] == "stop")

r = wl.suspend(warm["id"]); r2 = wl.suspend(cold["id"])
check("a warm warehouse is paused, a cold one stopped, whatever the deployment default", ("n1", "pause") in acts and ("n2", "stop") in acts and warehouses.get_sql_warehouse(warm["id"])["suspend_mode"] == "pause")
r = wl.ensure_running(warm["id"])
check("resuming a paused node is a warm resume: unpause, no warm-up", r["resume_kind"] == "warm" and ("n1", "unpause") in acts and not posts and warehouses.get_sql_warehouse(warm["id"])["last_resume_kind"] == "warm", r)
r = wl.ensure_running(cold["id"])
check("resuming a stopped node is a cold start followed by the warm-up of its tables", r["resume_kind"] == "cold" and ("n2", "start") in acts and posts and posts[0][0] == "http://n2:8002/api/compute/warmup" and posts[0][1] == {"tables": ["warehouse.sales.orders"]} and r.get("warning") is None, (r, posts))
check("the kind is stored for the UI", warehouses.get_sql_warehouse(cold["id"])["last_resume_kind"] == "cold")
wl.suspend(cold["id"]); posts.clear(); mode["fail"] = True
r = wl.ensure_running(cold["id"])
check("a failing warm-up is a warning, the warehouse still resumes", r.get("resume_kind") == "cold" and "warm-up failed" in (r.get("warning") or "") and warehouses.get_sql_warehouse(cold["id"])["state"] == "RUNNING", r)
mode["fail"] = False; warehouses.update_sql_warehouse(cold["id"], {"warm_tables": ["bad.bad.bad", "ok.ok.ok"]}); wl.suspend(cold["id"])
r = wl.ensure_running(cold["id"]); check("a table that cannot be read is named in the warning", "bad.bad.bad" in (r.get("warning") or "") and "ok.ok.ok" not in (r.get("warning") or ""), r)
warehouses.update_sql_warehouse(cold["id"], {"warm_tables": []}); wl.suspend(cold["id"]); posts.clear(); wl.ensure_running(cold["id"])
check("no warm tables, no warm-up call", not posts)

print("the warm hold ends")
wl.suspend(warm["id"]); acts.clear()
check("within the hold nothing is stopped", wl.escalate_tick() == [] and not acts)
past = time.time() + 31 * 60
check("after the hold the paused node is stopped and the memory freed", wl.escalate_tick(now=past) == [warm["id"]] and acts == [("n1", "stop")] and state["n1"] == "exited")
w = warehouses.get_sql_warehouse(warm["id"]); check("it is recorded as stopped, still suspended", w["suspend_mode"] == "stop" and w["state"] == "STOPPED" and w["suspend_reason"] == "warm hold ended", w)
check("it is not stopped twice", wl.escalate_tick(now=past + 3600) == [])
r = wl.ensure_running(warm["id"]); check("the next query is then a cold start, with warm-up", r["resume_kind"] == "cold" and posts, r)
warehouses.update_sql_warehouse(warm["id"], {"warm_hold_mins": 0}); wl.suspend(warm["id"]); acts.clear()
check("a hold of 0 keeps it paused for good", wl.escalate_tick(now=time.time() + 10**7) == [] and not acts)
warehouses.update_sql_warehouse(warm["id"], {"warm_hold_mins": 30}); warehouses.update_sql_warehouse(cold["id"], {"warm_hold_mins": 30, "auto_stop_mins": 1}); wl.suspend(cold["id"])
check("a cold (stopped) warehouse is never touched by the hold", wl.escalate_tick(now=time.time() + 10**7) == [warm["id"]])
warehouses.mutate_sql_warehouse(warm["id"], {"state": "RUNNING"})

print("the worker's warm-up endpoint")
os.environ["COMPUTE_TOKEN"] = "t"
import pandas as pd
from deltalake import write_deltalake
os.makedirs(TMP + "/warehouse/sales", exist_ok=True)
write_deltalake(TMP + "/warehouse/sales/orders", pd.DataFrame({"id": [1, 2, 3]}))
from fastapi.testclient import TestClient
from web import compute_worker as cw
c = TestClient(cw.app); H = {"X-Compute-Token": "t"}
check("it needs the compute token", c.post("/api/compute/warmup", json={"tables": []}).status_code in (401, 403))
r = c.post("/api/compute/warmup", json={"tables": ["warehouse.sales.orders", "warehouse.sales.nope", "x; drop table y", "a.b"]}, headers=H).json()
t = r["tables"]
check("a real table is read, a missing one and bad names are reported per table, nothing else is returned", t["warehouse.sales.orders"] == "ok" and t["warehouse.sales.nope"] != "ok" and t["x; drop table y"] == "not a catalog.schema.table name" and t["a.b"].startswith("not a") and set(r) == {"success", "node_id", "tables"}, r)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
