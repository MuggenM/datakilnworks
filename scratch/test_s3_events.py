#!/usr/bin/env python3
"""S3 bucket event notifications (web/s3_events.py + the pipeline setting `s3_events`) in a throwaway warehouse: tokens, the receiver route, event shapes
(AWS, MinIO wrapper), matching, validation, and the debounced waker (no real S3 needed; scratch/test_s3_events_minio.py does the real thing)."""
import json, os, shutil, sys, tempfile, threading, time
TMP = tempfile.mkdtemp(prefix="s3ev_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
os.environ["S3_EVENTS_DEBOUNCE"] = "0.3"
sys.path.insert(0, "/workspace")
from fastapi.testclient import TestClient
from web import app as app_module, auth, autoloader, s3_events as ev
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def refuses(fn, *a, frag="", **k):
    try: fn(*a, **k); return False
    except (ValueError, ev.EventError) as e: return frag.lower() in str(e).lower()
def rec(bucket, key, name="s3:ObjectCreated:Put"): return {"eventName": name, "s3": {"bucket": {"name": bucket}, "object": {"key": key, "size": 5}}}
def pipe(name, path, **kw): return autoloader.create_pipeline({"name": name, "source_volume_path": path, "file_pattern": kw.pop("pattern", "*.csv"), "target_table": name.replace(" ", "_").lower(), **kw}, created_by="admin")

with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0")
admin = TestClient(app_module.app); admin.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"})
recv = TestClient(app_module.app)

print("tokens")
check("only administrators manage tokens", TestClient(app_module.app).get("/api/autoloader/s3-events").status_code in (401, 403))
r = admin.post("/api/autoloader/s3-events/tokens", json={"name": "MinIO prod"}); tok = r.json()["token"]
check("a token is created and shown once; only a hash is stored", r.status_code == 200 and tok.startswith("dkw_s3ev_") and tok not in json.dumps(admin.get("/api/autoloader/s3-events").json()) and tok not in json.dumps([dict(x) for x in ev._db().execute("SELECT * FROM s3_event_tokens")]))
check("an empty name is refused", admin.post("/api/autoloader/s3-events/tokens", json={"name": " "}).status_code == 400)
info = admin.get("/api/autoloader/s3-events").json(); check("the admin view gives the receiver URL and the token list", info["receiver_url"].endswith("/hooks/s3-events") and info["tokens"][0]["name"] == "MinIO prod")
check("no token: 401; wrong token: 401", recv.post("/hooks/s3-events", json={"Records": []}).status_code == 401 and recv.post("/hooks/s3-events", json={"Records": []}, headers={"Authorization": "Bearer dkw_s3ev_nope"}).status_code == 401 and recv.post("/hooks/s3-events", json={}, headers={"Authorization": "Bearer other"}).status_code == 401)
check("a session cookie is not a credential for the receiver", admin.post("/hooks/s3-events", json={"Records": []}).status_code == 401)
H = {"Authorization": f"Bearer {tok}"}
check("Bearer <token> and the bare token (as MinIO may send it) both work", recv.post("/hooks/s3-events", json={"Records": []}, headers=H).status_code == 200 and recv.post("/hooks/s3-events", json={"Records": []}, headers={"Authorization": tok}).status_code == 200)
check("the token records when it was used", admin.get("/api/autoloader/s3-events").json()["tokens"][0]["last_used_at"] is not None)

print("event shapes")
check("standard S3 records", ev.parse_records({"Records": [rec("b", "in/a.csv")]}) == [("s3:ObjectCreated:Put", "b", "in/a.csv")])
check("MinIO's wrapper (EventName, Key, Records)", ev.parse_records({"EventName": "s3:ObjectCreated:Put", "Key": "b/in/a.csv", "Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "in/a.csv"}}}]}) == [("s3:ObjectCreated:Put", "b", "in/a.csv")])
check("a list of notifications", len(ev.parse_records([{"Records": [rec("b", "x.csv")]}, {"Records": [rec("b", "y.csv")]}])) == 2)
check("keys are URL-decoded (spaces as +, %2B, unicode)", ev.parse_records({"Records": [rec("b", "in/my+file%2Bv2%20%C3%A9.csv")]})[0][2] == "in/my file+v2 é.csv")
check("AWS's test message is ignored, not an error", ev.parse_records({"Service": "Amazon S3", "Event": "s3:TestEvent"}) == [])
check("garbage is refused with a reason", refuses(ev.parse_records, {"foo": 1}, frag="Records") and refuses(ev.parse_records, "text", frag="not an S3") and refuses(ev.parse_records, {"Records": "x"}, frag="list"))
check("too many records in one request are refused", refuses(ev.parse_records, {"Records": [rec("b", f"k{i}.csv") for i in range(1001)]}, frag="At most"))
check("the route answers 400 for JSON that is not an event and for invalid JSON, 413 for a huge body",
      recv.post("/hooks/s3-events", json={"x": 1}, headers=H).status_code == 400 and recv.post("/hooks/s3-events", content=b"{bad", headers=H).status_code == 400 and recv.post("/hooks/s3-events", content=b"[" + b"1," * 600000 + b"1]", headers=H).status_code == 413)

print("pipeline setting")
a = pipe("events a", "s3://landing/inbox/", s3_events=True)
check("a pipeline can be woken by S3 events (and keeps a safety-net rescan)", a["s3_events"] is True and a["watch_sweep_seconds"] == 300)
check("S3 events need an s3:// source", refuses(pipe, "local ev", "/Volumes/warehouse/raw/x", s3_events=True, frag="only apply to s3://"))
check("...are an alternative to a cron schedule and to file watching", refuses(pipe, "cron ev", "s3://landing/x/", s3_events=True, cron_schedule="*/5 * * * *", frag="alternatives") and refuses(pipe, "watch ev", "s3://landing/x/", s3_events=True, watch_enabled=True, frag="File events watch local"))
u = autoloader.update_pipeline(a["id"], {"cron_schedule": "*/10 * * * *"}); check("choosing a cron schedule later switches S3 events off", u["s3_events"] is False and u["cron_schedule"])
u = autoloader.update_pipeline(a["id"], {"cron_schedule": "", "s3_events": True}); check("...and back on", u["s3_events"] is True)
autoloader.update_pipeline(a["id"], {"cron_schedule": "", "s3_events": True})
polled = pipe("polled", "s3://landing/inbox/"); other = pipe("other bucket", "s3://elsewhere/inbox/", s3_events=True); deeper = pipe("deeper", "s3://landing/inbox/2026/", s3_events=True)
off = pipe("off", "s3://landing/off/", s3_events=True); autoloader.update_pipeline(off["id"], {"enabled": False})
csvp = pipe("json only", "s3://landing/inbox/", s3_events=True, pattern="*.json")

print("matching")
M = lambda *e: sorted(ev.matching_pipelines(list(e)))
C = lambda b, k: ("s3:ObjectCreated:Put", b, k)
check("an object under the prefix wakes exactly the pipelines that flagged S3 events, match bucket, prefix and pattern", M(C("landing", "inbox/a.csv")) == sorted([a["id"]]), M(C("landing", "inbox/a.csv")))
check("a nested prefix and a json pattern match their own objects", M(C("landing", "inbox/2026/x.csv")) == sorted([a["id"], deeper["id"]]) and M(C("landing", "inbox/x.json")) == [csvp["id"]])
check("other buckets, other prefixes and disabled pipelines are not woken", M(C("landing", "elsewhere/a.csv")) == [] and M(C("landing", "off/a.csv")) == [] and M(C("nope", "inbox/a.csv")) == [])
check("hidden names, .tmp / .part and _quarantine never wake a pipeline (the scan would skip them)", M(C("landing", "inbox/.hidden.csv")) == [] and M(C("landing", "inbox/a.csv.tmp")) == [] and M(C("landing", "inbox/_quarantine/a.csv")) == [] and M(C("landing", "inbox/sub/.x/a.csv")) == [])
check("removals are ignored; created / copied / multipart uploads are not", M(("s3:ObjectRemoved:Delete", "landing", "inbox/a.csv")) == [] and M(("s3:ObjectCreated:CompleteMultipartUpload", "landing", "inbox/a.csv")) == [a["id"]] and M(("s3:ObjectCreated:Copy", "landing", "inbox/a.csv")) == [a["id"]])
check("a pipeline that is polled (no s3_events) is never woken", polled["id"] not in M(C("landing", "inbox/a.csv")))

print("the waker: debounce, one cycle at a time, no lost event")
runs = []; running = {"n": 0, "max": 0}; gate = threading.Event()
def fake(pid):
    running["n"] += 1; running["max"] = max(running["max"], running["n"]); runs.append(pid); gate.wait(2) if pid == "slow" else time.sleep(0.05); running["n"] -= 1
w = ev.Waker()
for _ in range(20): w.wake("p1", fake)
time.sleep(1.0); check("a burst of 20 events is one run", runs.count("p1") == 1, runs)
runs.clear(); w.wake("slow", fake); time.sleep(0.6)
for _ in range(5): w.wake("slow", fake)                      # events while a run is in progress
gate.set(); time.sleep(1.5)
check("events during a run cause exactly one more run afterwards, never two at once", runs.count("slow") == 2 and running["max"] == 1, (runs, running))
runs.clear(); w.wake("p2", fake); w.wake("p3", fake); time.sleep(1.0); check("different pipelines run independently", sorted(runs) == ["p2", "p3"])
runs.clear(); w.wake("p1", fake); time.sleep(0.9); w.wake("p1", fake); time.sleep(0.9); check("later events start a new run", runs.count("p1") == 2)
boom = ev.Waker(); n = {"c": 0}
def fail(pid):
    n["c"] += 1
    if n["c"] == 1: raise RuntimeError("cycle failed")
boom.wake("x", fail); time.sleep(0.8); boom.wake("x", fail); time.sleep(0.8); check("a failing run does not stop later events from running", n["c"] == 2)

print("the receiver end to end (cycle replaced by a recorder)")
calls = []; ev._run_cycle = lambda pid: calls.append(pid)
ev.WAKER = ev.Waker()
r = recv.post("/hooks/s3-events", json={"Records": [rec("landing", "inbox/a.csv"), rec("landing", "inbox/b.csv"), rec("landing", "nowhere/c.csv")]}, headers=H).json()
time.sleep(1.0)
check("the answer says what was received and which pipelines were woken; one run results", r == {"received": 3, "woke": [a["id"]]} and calls == [a["id"]], (r, calls))
st = admin.get("/api/autoloader/s3-events").json()
check("the admin view shows the pipelines using events, per-pipeline counts and the recent requests", any(p["id"] == a["id"] for p in st["pipelines"]) and st["recent"][0]["events"] == 3 and st["recent"][0]["token"] == "MinIO prod" and [p for p in st["pipelines"] if p["id"] == a["id"]][0]["events"] == 1)
tid = st["tokens"][0]["id"]; admin.delete(f"/api/autoloader/s3-events/tokens/{tid}")
check("a revoked token is refused at once; unknown tokens are 404", recv.post("/hooks/s3-events", json={"Records": []}, headers=H).status_code == 401 and admin.delete("/api/autoloader/s3-events/tokens/nope").status_code == 404)
acts = {x[0] for x in __import__("sqlite3").connect(os.path.join(TMP, ".metadata", "governance.db")).execute("SELECT action FROM governance_audit")}
check("token changes are audited", {"S3EVENTS_TOKEN_CREATE", "S3EVENTS_TOKEN_REVOKE"} <= acts)
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
