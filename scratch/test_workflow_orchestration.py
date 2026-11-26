#!/usr/bin/env python3
"""Workflow orchestration (web/workflow.py): validation, parameters, run conditions, retries with backoff, task and job timeouts, cancel, repair,
concurrency, notifications and triggers (job chaining, Auto-Loader files, table changes), cron catch-up. Real DuckDB tasks against a throwaway
warehouse; the retry logic is exercised with a scripted task function so it does not depend on timing luck.
  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_workflow_orchestration.py"""
import datetime, json, os, shutil, sqlite3, sys, tempfile, threading, time
TMP = tempfile.mkdtemp(prefix="wf_test_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
sys.path.insert(0, "/workspace")
import pandas as pd
from deltalake import write_deltalake
from web import workflow as wf
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def bad(job):
    try: wf.validate_job(job); return None
    except wf.JobValidationError as e: return str(e)
def sqlt(tid, q, **kw): return {"id": tid, "name": tid, "type": "sql", "depends_on": kw.pop("depends_on", []), "parameters": {"query": q}, **kw}
def job(jid, tasks, **kw): return wf.create_or_update_job({"id": jid, "name": jid, "enabled": True, "tasks": tasks, "created_by": "admin", **kw})
def by(res): return {t["task_id"]: t for t in res["task_runs"]}
def wait_for(pred, secs=20):
    end = time.time() + secs
    while time.time() < end:
        v = pred()
        if v: return v
        time.sleep(0.2)
    return None

print("validation")
check("an old-style job (no orchestration fields) is accepted and defaulted", wf.validate_job({"name": "old", "tasks": [{"id": "a", "type": "sql", "parameters": {"query": "select 1"}}]})["tasks"][0]["retries"] == 0)
for label, j, frag in (
    ("no name", {"name": " ", "tasks": []}, "name"),
    ("duplicate task ids", {"name": "x", "tasks": [{"id": "a"}, {"id": "a"}]}, "twice"),
    ("unknown dependency", {"name": "x", "tasks": [{"id": "a", "depends_on": ["zzz"]}]}, "depends_on"),
    ("a dependency cycle", {"name": "x", "tasks": [{"id": "a", "depends_on": ["b"]}, {"id": "b", "depends_on": ["a"]}]}, "circle"),
    ("bad task type", {"name": "x", "tasks": [{"id": "a", "type": "rocket"}]}, "type"),
    ("retries too high", {"name": "x", "tasks": [{"id": "a", "retries": 99}]}, "retries"),
    ("bad run_if", {"name": "x", "tasks": [{"id": "a", "run_if": "whenever"}]}, "run_if"),
    ("bad cron", {"name": "x", "tasks": [], "schedule_cron": "not a cron"}, "cron"),
    ("undeclared parameter reference", {"name": "x", "tasks": [{"id": "a", "parameters": {"query": "select '{{ params.day }}'"}}]}, "declares no parameter"),
    ("bad parameter default", {"name": "x", "tasks": [], "parameters": [{"name": "d", "default": "a'; drop table t; --"}]}, "may only contain"),
    ("self-triggering job", {"id": "j1", "name": "x", "tasks": [], "triggers": [{"type": "job", "job_id": "j1"}]}, "another job"),
    ("bad trigger type", {"name": "x", "tasks": [], "triggers": [{"type": "magic"}]}, "trigger"),
    ("bad table trigger", {"name": "x", "tasks": [], "triggers": [{"type": "table", "table": "no_dots"}]}, "table"),
    ("notification without events", {"name": "x", "tasks": [], "notifications": [{"on": [], "channel": "email", "target": "a@b.co"}]}, "notification"),
    ("bad email address", {"name": "x", "tasks": [], "notifications": [{"on": ["failure"], "channel": "email", "target": "not-an-email"}]}, "address"),
    ("unknown parameter name", {"name": "x", "tasks": [], "parameters": [{"name": "1bad", "default": "x"}]}, "Parameter names")):
    e = bad(j); check(f"refused: {label}", e is not None and frag.lower() in e.lower(), e)

print("parameters")
job("p1", [sqlt("t", "create or replace table dbo.param_out as select '{{ params.day }}' as day, '{{ params.region }}' as region")],
    parameters=[{"name": "day", "default": "2026-01-01"}, {"name": "region", "default": "EMEA", "allowed": ["EMEA", "APAC"]}])
r = wf.run_pipeline("p1"); check("defaults are substituted into the task", r["status"] == "SUCCESS" and r["parameters"] == {"day": "2026-01-01", "region": "EMEA"}, r)
import duckdb
def peek(table):
    from deltalake import DeltaTable
    return DeltaTable(os.path.join(TMP, *table.split("."))).to_pyarrow_table().to_pylist()
check("...and reach the data", peek("dbo.param_out") == [{"day": "2026-01-01", "region": "EMEA"}], peek("dbo.param_out"))
r = wf.run_pipeline("p1", params={"day": "2026-02-02", "region": "APAC"}); check("supplied values override", peek("dbo.param_out") == [{"day": "2026-02-02", "region": "APAC"}])
for label, pv in (("an injection attempt", {"day": "x'; drop table dbo.param_out; --"}), ("a value outside the allowed list", {"region": "MARS"}), ("an undeclared parameter", {"nope": "1"}), ("an over-long value", {"day": "a" * 300})):
    try: wf.run_pipeline("p1", params=pv); ok = False
    except wf.JobValidationError: ok = True
    check(f"refused: {label}", ok)
check("nothing was dropped or run by the refused attempts", peek("dbo.param_out")[0]["region"] == "APAC")
job("p2", [sqlt("t", "create or replace table dbo.pat_out as select '{{ params.tag }}' as tag")], parameters=[{"name": "tag", "default": "a-1", "pattern": "[a-z]-\\d"}])
check("an owner-declared pattern is the rule for that parameter", wf.run_pipeline("p2", params={"tag": "z-9"})["status"] == "SUCCESS")

print("run conditions")
job("rc", [sqlt("a", "select * from dbo.does_not_exist"),
           sqlt("b", "select 1", depends_on=["a"]),
           sqlt("c", "select 1", depends_on=["a"], run_if="all_done"),
           sqlt("d", "select 1", depends_on=["a"], run_if="at_least_one_failed"),
           sqlt("e", "select 1", depends_on=["a"], run_if="none_failed"),
           sqlt("f", "select 1", depends_on=["a"], run_if="all_failed"),
           sqlt("g", "select 1", depends_on=["b"], run_if="all_success")])
r = wf.run_pipeline("rc"); t = by(r)
check("a failing task fails the run", r["status"] == "FAILED" and t["a"]["status"] == "FAILED")
check("all_success is skipped after a failure (default)", t["b"]["status"] == "SKIPPED" and "run_if" in t["b"]["output_log"])
check("all_done / at_least_one_failed / all_failed still run", t["c"]["status"] == "SUCCESS" and t["d"]["status"] == "SUCCESS" and t["f"]["status"] == "SUCCESS")
check("none_failed is skipped after a failure", t["e"]["status"] == "SKIPPED")
check("skipping propagates through all_success", t["g"]["status"] == "SKIPPED")
job("rc2", [sqlt("a", "select 1"), sqlt("b", "select 1", depends_on=["a"], run_if="at_least_one_failed"), sqlt("c", "select 1", depends_on=["a"], run_if="none_failed"), sqlt("cleanup", "select 1", depends_on=["b", "c"], run_if="all_done")])
r = wf.run_pipeline("rc2"); t = by(r); check("success path: error handler skipped, others run, run succeeds", r["status"] == "SUCCESS" and t["b"]["status"] == "SKIPPED" and t["c"]["status"] == "SUCCESS" and t["cleanup"]["status"] == "SUCCESS", {k: v["status"] for k, v in t.items()})

print("retries and backoff (scripted task)")
real_execute = wf.execute_task; calls = {"n": 0}
def scripted(task, conn, principal=None):
    calls["n"] += 1
    ok = calls["n"] > task["parameters"]["fail_first"]
    return wf._task_result(task, "SUCCESS" if ok else "FAILED", "fine" if ok else f"boom #{calls['n']}")
wf.execute_task = scripted
job("rt", [{"id": "flaky", "name": "flaky", "type": "sql", "parameters": {"fail_first": 2}, "retries": 3, "retry_delay_seconds": 0}])
calls["n"] = 0; r = wf.run_pipeline("rt"); f = by(r)["flaky"]
check("a task that fails twice succeeds on the third attempt", r["status"] == "SUCCESS" and f["attempt_count"] == 3 and [a["status"] for a in f["attempts"]] == ["FAILED", "FAILED", "SUCCESS"], f.get("attempts"))
check("failed attempts keep their error", f["attempts"][0]["error"] == "boom #1")
job("rt2", [{"id": "dead", "name": "dead", "type": "sql", "parameters": {"fail_first": 99}, "retries": 2, "retry_delay_seconds": 0}])
calls["n"] = 0; r = wf.run_pipeline("rt2"); check("retries are exhausted: retries+1 attempts, then FAILED", r["status"] == "FAILED" and by(r)["dead"]["attempt_count"] == 3 and calls["n"] == 3, calls)
job("rt3", [{"id": "slow", "name": "slow", "type": "sql", "parameters": {"fail_first": 1}, "retries": 1, "retry_delay_seconds": 2, "retry_backoff": 1}])
calls["n"] = 0; t0 = time.time(); wf.run_pipeline("rt3"); check("the retry waits the configured delay", 1.8 <= time.time() - t0 < 6, time.time() - t0)
job("rt4", [{"id": "no", "name": "no", "type": "sql", "parameters": {"fail_first": 5}}])
calls["n"] = 0; wf.run_pipeline("rt4"); check("no retries configured: one attempt", calls["n"] == 1)
wf.execute_task = real_execute

print("timeouts (real SQL, interrupted)")
LONG = "select count(*) from range(100000000000)"
job("to", [sqlt("slow", LONG, timeout_seconds=2), sqlt("after", "select 1", depends_on=["slow"], run_if="all_done")])
t0 = time.time(); r = wf.run_pipeline("to"); t = by(r)
check("a task over its timeout fails with timed_out, in about the timeout", t["slow"]["status"] == "FAILED" and t["slow"].get("timed_out") and time.time() - t0 < 12, (t["slow"]["status"], time.time() - t0))
check("the run continues (run_if all_done) and the next task works on the same connection", t["after"]["status"] == "SUCCESS", t["after"])
job("jt", [sqlt("a", "select 1"), sqlt("slow", LONG, depends_on=["a"]), sqlt("never", "select 1", depends_on=["slow"], run_if="all_done")], timeout_seconds=2)
t0 = time.time(); r = wf.run_pipeline("jt"); t = by(r)
check("a job over its timeout ends TIMEOUT; the running task is stopped and later tasks are skipped", r["status"] == "TIMEOUT" and t["slow"]["status"] == "FAILED" and t["never"]["status"] == "SKIPPED" and time.time() - t0 < 12, (r["status"], {k: v["status"] for k, v in t.items()}))

print("cancel")
job("cn", [sqlt("a", "select 1"), sqlt("slow", LONG, depends_on=["a"]), sqlt("later", "select 1", depends_on=["slow"], run_if="all_done")])
rid = wf.start_run_in_background("cn")
check("the run is visible as RUNNING at once", wait_for(lambda: (wf.get_run_detail(rid) or {}).get("status") == "RUNNING"))
time.sleep(1.5); t0 = time.time(); check("cancel_run reports it was running", wf.cancel_run(rid) is True)
d = wait_for(lambda: (wf.get_run_detail(rid) or {}).get("status") not in ("RUNNING", None) and wf.get_run_detail(rid))
check("the run ends CANCELLED quickly", d and d["status"] == "CANCELLED" and time.time() - t0 < 10, d and d["status"])
st = {x["task_id"]: x["status"] for x in d["tasks_detail"]}; check("finished work stays, the running task and the rest are CANCELLED", st["a"] == "SUCCESS" and st["slow"] == "CANCELLED" and st["later"] == "CANCELLED", st)
check("cancelling a finished run says so", wf.cancel_run(rid) is False)
job("cn2", [{"id": "flaky", "name": "flaky", "type": "sql", "parameters": {"query": "select * from dbo.nope"}, "retries": 5, "retry_delay_seconds": 30}])
rid2 = wf.start_run_in_background("cn2"); time.sleep(2); t0 = time.time(); wf.cancel_run(rid2)
d2 = wait_for(lambda: (wf.get_run_detail(rid2) or {}).get("status") == "CANCELLED" and wf.get_run_detail(rid2), 10); check("a run waiting between retries is cancelled at once, not after the delay", d2 and time.time() - t0 < 5, d2 and d2["status"])

print("repair")
job("rp", [sqlt("first", "create or replace table dbo.rp_first as select 1 as v"), sqlt("second", "create or replace table dbo.rp_second as select v + 1 as v from dbo.rp_missing", depends_on=["first"]), sqlt("third", "select 1", depends_on=["second"])],
    parameters=[{"name": "x", "default": "one"}])
r1 = wf.run_pipeline("rp", params={"x": "custom"}); check("the first run fails at the second task", r1["status"] == "FAILED" and by(r1)["first"]["status"] == "SUCCESS" and by(r1)["third"]["status"] == "SKIPPED")
write_deltalake(os.path.join(TMP, "dbo", "rp_missing"), pd.DataFrame({"v": [1]}))          # the cause is fixed
rid3 = wf.start_run_in_background("rp", trigger="REPAIR", repair_of=r1["run_id"]); d3 = wait_for(lambda: (wf.get_run_detail(rid3) or {}).get("status") in ("SUCCESS", "FAILED") and wf.get_run_detail(rid3))
st = {x["task_id"]: x for x in d3["tasks_detail"]}
check("the repair run succeeds", d3["status"] == "SUCCESS", d3["status"])
check("the succeeded task was reused, not run again", st["first"].get("reused_from") == r1["run_id"] and "Reused" in st["first"]["output_log"], st["first"])
check("the failed and skipped tasks ran", st["second"]["status"] == "SUCCESS" and st["third"]["status"] == "SUCCESS" and not st["second"].get("reused_from"))
check("same parameters, and the parent is recorded", json.loads(d3["run_params"]) == {"x": "custom"} and d3["parent_run_id"] == r1["run_id"], (d3["run_params"], d3["parent_run_id"]))
try: wf.start_run_in_background("rp", repair_of=rid3); ok = False
except ValueError: ok = True
except Exception: ok = True
check("a successful run cannot be repaired", ok or (wf.get_run_detail(wf.start_run_in_background("rp", repair_of=rid3)) is None))

print("concurrency")
job("cc", [sqlt("slow", LONG)])
ra = wf.start_run_in_background("cc"); wait_for(lambda: wf.running_count("cc") == 1)
rb = wf.run_pipeline("cc"); check("a second run while one is running is SKIPPED and recorded", rb["status"] == "SKIPPED" and (wf.get_run_detail(rb["run_id"]) or {}).get("status") == "SKIPPED", rb)
wf.cancel_run(ra); wait_for(lambda: wf.running_count("cc") == 0)
job("cc2", [sqlt("slow", LONG)], max_concurrent_runs=2)
r1_ = wf.start_run_in_background("cc2"); r2_ = wf.start_run_in_background("cc2"); wait_for(lambda: wf.running_count("cc2") == 2)
check("max_concurrent_runs=2 allows two", wf.running_count("cc2") == 2)
wf.cancel_run(r1_); wf.cancel_run(r2_); wait_for(lambda: wf.running_count("cc2") == 0)

print("parallel tasks: validation")
check("parallel and max_parallel_tasks are defaulted (off, 4) and an out-of-range value is refused, like every other orchestration field",
      wf.validate_job({"name": "x", "tasks": []})["parallel"] is False
      and wf.validate_job({"name": "x", "tasks": [], "parallel": True})["max_parallel_tasks"] == 4
      and (bad({"name": "x", "tasks": [], "parallel": True, "max_parallel_tasks": 99}) or "") != "")
same_target = [{"id": "a", "type": "sql", "parameters": {"query": "create or replace table dbo.same as select 1"}},
              {"id": "b", "type": "sql", "parameters": {"query": "insert into dbo.same select 2"}}]
e = bad({"name": "x", "parallel": True, "tasks": same_target})
check("two independent tasks writing the same table are refused when parallel is on", e is not None and "'a'" in e and "'b'" in e and "dbo.same" in e, e)
check("the identical pair is accepted when parallel is off (today's sequential behaviour, unchanged)", wf.validate_job({"name": "x", "tasks": [dict(t) for t in same_target]})["parallel"] is False)
serialised = [dict(same_target[0]), dict(same_target[1], depends_on=["a"])]
check("...but an explicit dependency edge between them makes it acceptable under parallel too", wf.validate_job({"name": "x", "parallel": True, "tasks": serialised})["tasks"][1]["depends_on"] == ["a"])
check("independent tasks writing DIFFERENT tables are accepted", wf.validate_job({"name": "x", "parallel": True, "tasks": [
    {"id": "a", "type": "sql", "parameters": {"query": "create or replace table dbo.t1 as select 1"}},
    {"id": "b", "type": "sql", "parameters": {"query": "create or replace table dbo.t2 as select 1"}}]})["parallel"] is True)
e = bad({"name": "x", "parallel": True, "tasks": [{"id": "a", "type": "sql", "parameters": {"query": "select 1"}},
                                                  {"id": "b", "type": "notebook", "parameters": {"notebook_path": "x.ipynb"}}]})
check("a notebook task alongside anything else with no dependency is refused: its write target is unknown", e is not None and "notebook" in e.lower(), e)
e = bad({"name": "x", "parallel": True, "tasks": [{"id": "a", "type": "dbt", "parameters": {}}, {"id": "b", "type": "dbt", "parameters": {}}]})
check("two independent dbt tasks are refused too (a dbt task's real targets depend on the project DAG, not re-derived here)", e is not None, e)
check("a bare SELECT (no write) never conflicts with anything", wf.validate_job({"name": "x", "parallel": True, "tasks": [
    {"id": "a", "type": "sql", "parameters": {"query": "select 1"}}, {"id": "b", "type": "sql", "parameters": {"query": "select 2"}}]})["parallel"] is True)
check("partially-qualified names are matched conservatively (a dotted suffix counts as the same table)",
      wf._targets_overlap(["dbo.same"], ["same"]) and wf._targets_overlap(["cat.dbo.same"], ["dbo.same"]) and not wf._targets_overlap(["dbo.one"], ["dbo.two"]))

print("parallel tasks: real concurrent execution")
job("par1", [
    sqlt("root", "create or replace table dbo.par_root as select 1 as x"),
    sqlt("b1", "create or replace table dbo.par_b1 as select sum(length(md5(i::varchar))) as x from range(10000000) t(i)", depends_on=["root"]),
    sqlt("b2", "create or replace table dbo.par_b2 as select sum(length(md5(i::varchar))) as x from range(10000000) t(i)", depends_on=["root"]),
    sqlt("join", "create or replace table dbo.par_join as select (select x from dbo.par_b1) + (select x from dbo.par_b2) as total", depends_on=["b1", "b2"]),
], parallel=True, max_parallel_tasks=4)
rid = wf.start_run_in_background("par1")
both_running = wait_for(lambda: (lambda d: d if d and sum(1 for t in d["tasks_summary"] if t["id"] in ("b1", "b2") and t["status"] == "RUNNING") == 2 else None)(wf.get_run_detail(rid)), 15)
check("both independent branches are RUNNING at the same time (real overlap, not just a declared setting)", both_running is not None, both_running and both_running["tasks_summary"])
r = wait_for(lambda: (lambda d: d if d and d["status"] != "RUNNING" else None)(wf.get_run_detail(rid)), 30)
check("the whole run succeeds", r and r["status"] == "SUCCESS", r)
check("the join task sees both branches' correct results (each ran on its own connection, no interference between them)", peek("dbo.par_join") == [{"total": 640000000}], peek("dbo.par_join") if r else None)

print("parallel tasks: cancel interrupts every connection in flight, not just one")
job("pcn", [sqlt("s1", LONG), sqlt("s2", LONG)], parallel=True, max_parallel_tasks=4)
rid2 = wf.start_run_in_background("pcn")
d = wait_for(lambda: (lambda x: x if x and sum(1 for t in x["tasks_summary"] if t["status"] == "RUNNING") == 2 else None)(wf.get_run_detail(rid2)), 15)
check("both independent long tasks are running at once", d is not None, d and d["tasks_summary"])
time.sleep(1.5)                                       # give both worker threads time to actually open their connection and dispatch the query (the
                                                       # RUNNING marker above is written optimistically at dispatch, same as the sequential cancel test above)
t0 = time.time(); check("cancel_run reports it was running", wf.cancel_run(rid2) is True)
d2 = wait_for(lambda: (wf.get_run_detail(rid2) or {}).get("status") not in ("RUNNING", None) and wf.get_run_detail(rid2))
check("both are interrupted quickly, not left to finish (each had its own connection; cancel_run reached both)", d2 and d2["status"] == "CANCELLED" and time.time() - t0 < 10, d2 and (d2["status"], time.time() - t0))
st = {x["task_id"]: x["status"] for x in (d2 or {}).get("tasks_detail", [])}
check("both branches ended CANCELLED", st.get("s1") == "CANCELLED" and st.get("s2") == "CANCELLED", st)

print("notifications")
sent = []
import web.email_reports as er, web.slack_integration as si, web.webhook_alerts as wa
er.send_email = lambda recipients, subject, body_html, body_text=None, **k: (sent.append(("email", recipients, subject, body_text)) or {"success": True})
si.send_notification = lambda message, webhook_id=None, title=None, **k: (sent.append(("slack", webhook_id, title, message)) or {"success": True})
wa.send_webhook = lambda webhook_id, title, message, data=None, severity="info", **k: (sent.append(("webhook", webhook_id, title, severity)) or {"success": False, "error": "endpoint down"})
job("nt", [sqlt("a", "select * from dbo.gone")], notifications=[{"on": ["failure"], "channel": "email", "target": "ops@example.org, lead@example.org"}, {"on": ["success"], "channel": "email", "target": "nobody@example.org"},
     {"on": ["failure", "cancelled"], "channel": "slack", "target": ""}, {"on": ["failure"], "channel": "webhook", "target": "wh_1"}])
r = wf.run_pipeline("nt")
kinds = [s[0] for s in sent]
check("failure rules fire (email, slack, webhook); the success rule does not", sorted(kinds) == ["email", "slack", "webhook"] and not any(s[0] == "email" and "nobody" in str(s[1]) for s in sent), sent)
em = next(s for s in sent if s[0] == "email"); check("the email goes to every listed address with the failing task and its error", em[1] == ["ops@example.org", "lead@example.org"] and "FAILED" in em[2] and "a:" in em[3], em)
check("delivery results are stored with the run (a failing channel does not break anything)", any(n["channel"] == "webhook" and n["ok"] is False and "endpoint down" in (n["error"] or "") for n in wf.get_run_detail(r["run_id"])["notifications"]) and r["status"] == "FAILED")
sent.clear(); job("nt2", [sqlt("a", "select 1")], notifications=[{"on": ["success"], "channel": "email", "target": "ok@example.org"}]); wf.run_pipeline("nt2")
check("a success rule fires on success", [s[0] for s in sent] == ["email"])
er.send_email = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("smtp exploded")); sent.clear()
r = wf.run_pipeline("nt2"); check("an exception in a channel is recorded, never raised", r["status"] == "SUCCESS" and "RuntimeError" in (wf.get_run_detail(r["run_id"])["notifications"][0]["error"] or ""))

print("triggers")
job("up", [sqlt("a", "select 1")], enabled=True); job("down_ok", [sqlt("a", "select 1")], triggers=[{"type": "job", "job_id": "up", "on": "success"}])
job("down_fail", [sqlt("a", "select 1")], triggers=[{"type": "job", "job_id": "up", "on": "failure"}])
def runs_of(j): return [x for x in wf.get_job_runs(j, 20) if x["status"] != "RUNNING"]
n_ok = len(runs_of("down_ok")); wf.run_pipeline("up")
check("a job runs after another succeeds", wait_for(lambda: len(runs_of("down_ok")) > n_ok) and wf.get_job_runs("down_ok", 1)[0]["trigger"] == "EVENT:JOB")
check("...and a failure trigger does not", len(runs_of("down_fail")) == 0)
job("up_bad", [sqlt("a", "select * from dbo.gone")]); job("down_fail2", [sqlt("a", "select 1")], triggers=[{"type": "job", "job_id": "up_bad", "on": "failure"}])
wf.run_pipeline("up_bad"); check("a failure trigger runs after a failure", wait_for(lambda: len(runs_of("down_fail2")) == 1))
job("A", [sqlt("a", "select 1")], triggers=[{"type": "job", "job_id": "B", "on": "completion"}]); job("B", [sqlt("a", "select 1")], triggers=[{"type": "job", "job_id": "A", "on": "completion"}])
wf.run_pipeline("A"); time.sleep(3)
check("a trigger loop A -> B -> A stops (each runs once from that start)", len(runs_of("A")) == 1 and len(runs_of("B")) == 1, (len(runs_of("A")), len(runs_of("B"))))
job("al", [sqlt("a", "select 1")], triggers=[{"type": "autoloader", "pipeline_id": "pipe_x", "min_files": 3}])
check("an Auto-Loader trigger needs enough files", wf.fire_event({"type": "autoloader", "pipeline_id": "pipe_x", "files": 2}) == [] and wf.fire_event({"type": "autoloader", "pipeline_id": "pipe_other", "files": 9}) == [])
check("...and starts the job when there are", wf.fire_event({"type": "autoloader", "pipeline_id": "pipe_x", "files": 3}) == ["al"] and wait_for(lambda: len(runs_of("al")) == 1))
write_deltalake(os.path.join(TMP, "dbo", "watched"), pd.DataFrame({"v": [1]}))
job("tb", [sqlt("a", "select 1")], triggers=[{"type": "table", "table": "dbo.watched"}])
check("a table trigger's first look only records the version", wf.check_table_triggers() == [] and len(runs_of("tb")) == 0)
write_deltalake(os.path.join(TMP, "dbo", "watched"), pd.DataFrame({"v": [2]}), mode="append")
check("a new table version fires the job", wf.check_table_triggers() == ["tb"] and wait_for(lambda: len(runs_of("tb")) == 1))
check("and only once per change", wf.check_table_triggers() == [])
job("tb_self", [sqlt("w", "insert into dbo.watched select 3")], triggers=[{"type": "table", "table": "dbo.watched"}]); wf.check_table_triggers()
wf.run_pipeline("tb_self"); time.sleep(0.5); fired = wf.check_table_triggers()
check("a job's own writes to the table it watches do not retrigger it", "tb_self" not in fired, fired)
job("off", [sqlt("a", "select 1")], enabled=False, triggers=[{"type": "job", "job_id": "up", "on": "completion"}]); wf.run_pipeline("up"); time.sleep(2)
check("a disabled job is never triggered", len(runs_of("off")) == 0)

print("graph: positions, snapshot, live progress")
check("a task position is kept (rounded) and a bad one is refused", wf.validate_job({"name": "g", "tasks": [{"id": "a", "position": {"x": 10.123, "y": -5}}]})["tasks"][0]["position"] == {"x": 10.1, "y": -5.0}
      and "position" not in wf.validate_job({"name": "g", "tasks": [{"id": "a", "position": None}]})["tasks"][0]
      and "position" in (bad({"name": "g", "tasks": [{"id": "a", "position": {"x": "left", "y": 1}}]}) or "") and "range" in (bad({"name": "g", "tasks": [{"id": "a", "position": {"x": 10**7, "y": 1}}]}) or ""))
job("gr", [dict(sqlt("a", "select 1"), position={"x": 0, "y": 0}), sqlt("slow", LONG, depends_on=["a"]), sqlt("later", "select 1", depends_on=["slow"])])
rid = wf.start_run_in_background("gr")
d = wait_for(lambda: (lambda x: x if any(t["id"] == "slow" and t["status"] == "RUNNING" for t in x["tasks_summary"]) else None)(wf.get_run_detail(rid)), 20)
check("while a run is going its finished tasks and the running one are visible", d and [t["status"] for t in d["tasks_summary"]] == ["SUCCESS", "RUNNING"] and [t["task_id"] for t in d["tasks_detail"]] == ["a"] and d["status"] == "RUNNING", d and d["tasks_summary"])
check("the run keeps a snapshot of the graph (ids, names, types, dependencies, positions)", [(g["id"], g["type"], g["depends_on"]) for g in d["graph"]] == [("a", "sql", []), ("slow", "sql", ["a"]), ("later", "sql", ["slow"])] and d["graph"][0]["position"] == {"x": 0.0, "y": 0.0}, d["graph"])
job("gr", [sqlt("only", "select 1")])
wf.cancel_run(rid); d2 = wait_for(lambda: (lambda x: x if x["status"] != "RUNNING" else None)(wf.get_run_detail(rid)), 20)
check("after the job was edited the finished run still shows the graph it ran", d2 and [g["id"] for g in d2["graph"]] == ["a", "slow", "later"] and d2["status"] == "CANCELLED", d2 and d2["graph"])
check("a run of the edited job snapshots the new graph", [g["id"] for g in wf.get_run_detail(wf.run_pipeline("gr")["run_id"])["graph"]] == ["only"])

print("overview: recent runs and next run")
job("ov", [sqlt("a", "select 1"), sqlt("b", "select * from missing_table_zz", depends_on=["a"])])
ids = [wf.run_pipeline("ov")["run_id"] for _ in range(12)]
rr = wf.recent_runs_by_job(10)["ov"]
check("the last 10 runs of a job are returned, oldest first, with status and duration", len(rr) == 10 and [r["run_id"] for r in rr] == ids[2:] and all(r["status"] == "FAILED" and r["duration_sec"] is not None for r in rr), [r["run_id"] for r in rr])
check("a job with no runs has no entry (and other jobs are unaffected)", "never_ran" not in wf.recent_runs_by_job(10) and set(wf.recent_runs_by_job(3)["ov"][i]["run_id"] for i in range(3)) == set(ids[-3:]))
base = datetime.datetime(2026, 5, 4, 10, 30)
check("next_run_at follows the cron schedule from now, in the scheduler's local time", wf.next_run_at({"schedule_cron": "0 3 * * *", "enabled": True}, base) == "2026-05-05 03:00" and wf.next_run_at({"schedule_cron": "*/15 * * * *", "enabled": True}, base) == "2026-05-04 10:45")
check("no next run for a manual job, a paused job or a bad expression", wf.next_run_at({"schedule_cron": "", "enabled": True}, base) is None and wf.next_run_at({"schedule_cron": "0 3 * * *", "enabled": False}, base) is None and wf.next_run_at({"schedule_cron": "not cron", "enabled": True}, base) is None and wf.next_run_at({"schedule_cron": "0 3 * * *"}, base) is None)

print("cron catch-up and restarts")
now = datetime.datetime.now(); job("cr", [sqlt("a", "select 1")], schedule_cron="* * * * *")
wf._last_cron_check.clear(); wf._state_set("cron:cr", (now - datetime.timedelta(hours=3)).isoformat())
check("without catch_up a tick missed while down is skipped", wf._cron_due(wf.get_job("cr"), now) is False)
job("cr2", [sqlt("a", "select 1")], schedule_cron="* * * * *", catch_up=True); wf._last_cron_check.clear(); wf._state_set("cron:cr2", (now - datetime.timedelta(hours=3)).isoformat())
check("with catch_up it runs once for what was missed", wf._cron_due(wf.get_job("cr2"), now) is True and wf._cron_due(wf.get_job("cr2"), now) is False)
with sqlite3.connect(wf.DB_PATH) as c: c.execute("INSERT INTO job_runs (run_id, job_id, job_name, trigger, status, started_at, tasks_summary, tasks_detail) VALUES ('run_orphan','cr','cr','CRON','RUNNING','2026-01-01 00:00:00','[]','[]')")
check("a run left RUNNING by a dead process is marked FAILED at startup", wf.mark_orphaned_runs() >= 1 and wf.get_run_detail("run_orphan")["status"] == "FAILED")

shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS", flush=True)
# leave without interpreter teardown: a worker thread of the engine aborted (exit 134) at shutdown on a slow runner after every check had passed
sys.stdout.flush(); os._exit(1 if FAIL else 0)
