#!/usr/bin/env python3
"""Rewinding and moving streams (web/stream_ops.py) against THROWAWAY Redpanda brokers (two clusters):
  docker network create dkwstream
  for n in rpt rpt2; do docker run -d --name $n --network dkwstream docker.redpanda.com/redpandadata/redpanda:latest redpanda start --smp 1 --memory 512M \
     --overprovisioned --node-id 0 --kafka-addr internal://0.0.0.0:9092 --advertise-kafka-addr internal://$n:9092 --mode dev-container; done
  docker run --rm --network dkwstream -e KAFKA=rpt:9092 -e KAFKA2=rpt2:9092 -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook \
     sh -c "pip install -q confluent-kafka && python /workspace/scratch/test_stream_ops.py" """
import json, os, shutil, sys, tempfile, time
TMP = tempfile.mkdtemp(prefix="ops_test_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
sys.path.insert(0, "/workspace")
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from deltalake import DeltaTable
from web import connections, streaming as st, stream_ops as ops
B1, B2 = os.environ.get("KAFKA", "rpt:9092"), os.environ.get("KAFKA2", "rpt2:9092")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:500]}" if d and not c else ""))
    if not c: FAIL.append(n)
def wait_for(pred, secs=45):
    end = time.time() + secs
    while time.time() < end:
        try: v = pred()
        except Exception: v = None
        if v: return v
        time.sleep(0.3)
    return None
admins = {B: AdminClient({"bootstrap.servers": B}) for B in (B1, B2)}; prods = {B: Producer({"bootstrap.servers": B}) for B in (B1, B2)}
def mk_topic(B, name, parts=2):
    for f in admins[B].create_topics([NewTopic(name, num_partitions=parts, replication_factor=1)]).values(): f.result()
BASE = 1_760_000_000_000
def send(B, topic, ids, garbage=0):
    for i in ids: prods[B].produce(topic, value=json.dumps({"id": i, "v": f"x{i}"}), partition=i % 2, timestamp=BASE + i * 1000)
    for _ in range(garbage): prods[B].produce(topic, value="{bad", partition=0, timestamp=BASE + 999000)
    prods[B].flush(20)
def tbl(t):
    p = os.path.join(TMP, "dbo", t)
    return DeltaTable(p).to_pyarrow_table() if os.path.isdir(os.path.join(p, "_delta_log")) else None
def n(t): x = tbl(t); return x.num_rows if x is not None else 0
def dups(t):
    x = tbl(t)
    if x is None: return 0
    k = list(zip(x["_topic"].to_pylist(), x["_partition"].to_pylist(), x["_offset"].to_pylist())); return len(k) - len(set(k))
def ids_of(t): x = tbl(t); return sorted(x["id"].to_pylist()) if x is not None else []
def start(sid): st.set_enabled(sid, True, "admin"); st.sync_runners()
def stop(sid): st.set_enabled(sid, False, "admin"); st.stop_runner(sid)
def raises(fn, exc=st.StreamError, frag=""):
    try: fn(); return False
    except exc as e: return frag.lower() in str(e).lower()

c1 = connections.create_connection({"name": "c1", "type": "kafka", "config": {"bootstrap_servers": B1}}, "admin")
c1b = connections.create_connection({"name": "c1b", "type": "kafka", "config": {"bootstrap_servers": B1}}, "admin")
c2 = connections.create_connection({"name": "c2", "type": "kafka", "config": {"bootstrap_servers": B2}}, "admin")

print("rewind")
mk_topic(B1, "ops"); send(B1, "ops", range(60), garbage=3)
s = st.create_stream({"name": "Ops", "connection": "c1", "topic": "ops", "target_table": "ops_t", "max_wait_seconds": 1}, "admin"); sid = s["id"]; st.sync_runners()
check("the stream loads 60 rows and 3 dead letters", wait_for(lambda: n("ops_t") == 60 and n("ops_t_dlq") == 3), (n("ops_t"), n("ops_t_dlq")))
check("a running stream cannot be rewound (409)", raises(lambda: ops.rewind(sid, {"mode": "earliest"}, "replace", True, "admin"), st.StreamBusy, "stop"))
stop(sid)
check("a bad mode, a bad timestamp and an unknown partition are refused",
      raises(lambda: ops.rewind(sid, {"mode": "earliest"}, "delete-all", True, "admin"), frag="mode") and raises(lambda: ops.rewind(sid, {"mode": "timestamp", "timestamp": "yesterday-ish"}, "replace", True, "admin"), frag="timestamp")
      and raises(lambda: ops.rewind(sid, {"mode": "offsets", "offsets": {"9": 1}}, "replace", True, "admin"), frag="partition"))
plan = ops.rewind(sid, {"mode": "earliest"}, "replace", True, "admin")
check("a dry run reports what would be deleted and changes nothing", plan["delete_rows"] == 60 and plan["delete_dead_letters"] == 3 and n("ops_t") == 60 and n("ops_t_dlq") == 3 and all(r["target"] == r["low"] for r in plan["partitions"]), plan)
r = ops.rewind(sid, {"mode": "earliest"}, "replace", False, "admin")
check("rewind to earliest (replace) empties the table and dead letters in one go, and the recorded offsets go back", n("ops_t") == 0 and n("ops_t_dlq") == 0 and all(v == 0 for v in ops.recorded_offsets(st.get_stream(sid), [0, 1]).values()), (n("ops_t"), ops.recorded_offsets(st.get_stream(sid), [0, 1])))
start(sid)
check("restarted, it reads everything again exactly once (rows and dead letters)", wait_for(lambda: n("ops_t") == 60 and n("ops_t_dlq") == 3) and (time.sleep(2) or (n("ops_t") == 60 and dups("ops_t") == 0)), (n("ops_t"), dups("ops_t")))
stop(sid)
r = ops.rewind(sid, {"mode": "earliest"}, "keep", False, "admin")
check("mode keep leaves the rows and warns about duplicates", n("ops_t") == 60 and r["rewind"]["duplicates_warning"] is True)
start(sid); wait_for(lambda: n("ops_t") == 120); time.sleep(1.5)
check("...and the messages are then appended again (60 duplicates, on purpose)", n("ops_t") == 120 and dups("ops_t") == 60, (n("ops_t"), dups("ops_t")))
stop(sid); ops.rewind(sid, {"mode": "earliest"}, "replace", False, "admin"); start(sid); wait_for(lambda: n("ops_t") == 60); time.sleep(1.5)
check("a replace rewind repairs the duplicates", n("ops_t") == 60 and dups("ops_t") == 0)
stop(sid)
r = ops.rewind(sid, {"mode": "offsets", "offsets": {"0": 10}}, "replace", False, "admin")
gone = r["rewind"]["delete_rows"]
check("explicit offsets: only that partition is rewound, only its later rows are deleted", gone == 20 and n("ops_t") == 40 and all(p["current"] == p["target"] for p in r["rewind"]["partitions"] if p["partition"] == 1), (gone, n("ops_t"), r["rewind"]["partitions"]))
start(sid); wait_for(lambda: n("ops_t") == 60); time.sleep(1.5)
check("...and they are read again once", n("ops_t") == 60 and dups("ops_t") == 0, (n("ops_t"), dups("ops_t")))
stop(sid)
T = "2025-10-09T08:53:50Z"            # BASE + 30 s
import datetime
tms = int(datetime.datetime.fromisoformat(T.replace("Z", "+00:00")).timestamp() * 1000)
plan = ops.rewind(sid, {"mode": "timestamp", "timestamp": BASE + 30000}, "replace", True, "admin")
check("rewind to a timestamp finds the first message at or after it, per partition", plan["delete_rows"] == 30, plan)
ops.rewind(sid, {"mode": "timestamp", "timestamp": BASE + 30000}, "replace", False, "admin")
check("rows from that time on are removed", ids_of("ops_t") == list(range(30)), ids_of("ops_t")[-3:])
start(sid); wait_for(lambda: n("ops_t") == 60); time.sleep(1.5)
check("...and read again once", ids_of("ops_t") == list(range(60)) and dups("ops_t") == 0)
stop(sid); send(B1, "ops", range(60, 70))
plan = ops.rewind(sid, {"mode": "latest"}, "replace", False, "admin")
start(sid); time.sleep(4)
check("rewinding to latest skips what is waiting, deleting nothing", n("ops_t") == 60 and plan["rewind"]["delete_rows"] == 0, n("ops_t"))
send(B1, "ops", range(70, 75)); wait_for(lambda: n("ops_t") == 65)
check("...and continues with new messages", ids_of("ops_t") == list(range(60)) + list(range(70, 75)) and dups("ops_t") == 0)
stop(sid)

print("an interrupted rewind is completed")
real_record = ops._record; calls = {"n": 0}
def flaky(table, sid_, topic, positions, delete_from, dlq):
    if not dlq and calls["n"] == 0:
        calls["n"] += 1; raise RuntimeError("simulated crash between the dead-letter table and the main table")
    return real_record(table, sid_, topic, positions, delete_from, dlq)
ops._record = flaky
check("the failure is reported", raises(lambda: ops.rewind(sid, {"mode": "earliest"}, "replace", False, "admin"), RuntimeError, "simulated") or calls["n"] == 1)
check("the plan stays pending and the table is still intact", st.get_stream(sid)["pending"] is True and n("ops_t") == 65 and n("ops_t_dlq") == 0)
st.sync_runners()
check("the daemon finishes it (a stopped stream is completed too)", st.get_stream(sid)["pending"] is False and n("ops_t") == 0 and all(v == 0 for v in ops.recorded_offsets(st.get_stream(sid), [0, 1]).values()))
ops._record = real_record
start(sid); wait_for(lambda: n("ops_t") == 75); time.sleep(1.5)
check("and the stream then reads everything once", ids_of("ops_t") == list(range(75)) and dups("ops_t") == 0 and n("ops_t_dlq") == 3, (n("ops_t"), dups("ops_t"), n("ops_t_dlq")))
stop(sid)

print("move: connection, target, topic, cluster")
plan = ops.move(sid, {"connection": "c1b"}, None, False, True, "admin")
check("a dry run of a move describes it and changes nothing", plan["keeps_offsets"] is True and st.get_stream(sid)["connection"] == "c1", plan)
r = ops.move(sid, {"connection": "c1b"}, None, False, False, "admin")
check("another connection to the same cluster keeps the offsets", r["connection"] == "c1b" and r["move"]["keeps_offsets"] is True)
start(sid); send(B1, "ops", range(75, 80)); wait_for(lambda: n("ops_t") == 80); time.sleep(1.5)
check("...so nothing is read again or skipped", ids_of("ops_t") == list(range(80)) and dups("ops_t") == 0, (n("ops_t"), dups("ops_t")))
check("a running stream cannot be moved", raises(lambda: ops.move(sid, {"target_table": "ops_t2"}, None, False, False, "admin"), st.StreamBusy, "stop"))
stop(sid)
r = ops.move(sid, {"target_table": "ops_t2"}, None, False, False, "admin")
start(sid); send(B1, "ops", range(80, 85)); wait_for(lambda: n("ops_t2") == 5); time.sleep(1.5)
check("moving to another table continues where it stopped (no re-read, no gap); the old table is untouched", ids_of("ops_t2") == list(range(80, 85)) and n("ops_t") == 80, (ids_of("ops_t2"), n("ops_t")))
stop(sid)
ops.move(sid, {"target_table": "ops_t"}, None, False, False, "admin")
start(sid); send(B1, "ops", range(85, 88)); wait_for(lambda: n("ops_t") == 83); time.sleep(1.5)
check("moving back to a table with old recorded offsets corrects them (no re-read of the gap)", ids_of("ops_t") == list(range(80)) + [85, 86, 87] and dups("ops_t") == 0, (ids_of("ops_t")[-5:], n("ops_t")))
stop(sid)
mk_topic(B1, "ops_b"); send(B1, "ops_b", range(1000, 1010))
check("another topic needs a start position", raises(lambda: ops.move(sid, {"topic": "ops_b"}, None, False, False, "admin"), frag="where to start"))
check("a topic that does not exist is refused", raises(lambda: ops.move(sid, {"topic": "nope_nope"}, {"mode": "earliest"}, False, False, "admin"), frag="does not exist"))
check("offsets for only some partitions are refused for a new topic", raises(lambda: ops.move(sid, {"topic": "ops_b"}, {"mode": "offsets", "offsets": {"0": 1}}, False, False, "admin"), frag="every partition"))
check("nothing to change is refused", raises(lambda: ops.move(sid, {"topic": "ops"}, None, False, False, "admin"), frag="nothing"))
other = st.create_stream({"name": "Other", "connection": "c1", "topic": "ops_b", "target_table": "other_t", "enabled": False}, "admin")
check("a table that another stream loads is refused", raises(lambda: ops.move(sid, {"target_table": "other_t"}, None, False, False, "admin"), frag="already loads"))
st.delete_stream(other["id"], "admin")
r = ops.move(sid, {"topic": "ops_b"}, {"mode": "earliest"}, False, False, "admin")
start(sid); wait_for(lambda: n("ops_t") == 93); time.sleep(1.5)
check("moving to another topic starts fresh there and keeps the table", r["topic"] == "ops_b" and n("ops_t") == 93 and set(tbl("ops_t")["_topic"].to_pylist()) == {"ops", "ops_b"} and dups("ops_t") == 0, (n("ops_t"), r["topic"]))
stop(sid)

print("another cluster, same topic name (offset ids would collide)")
mk_topic(B1, "coll"); send(B1, "coll", range(40))
sc = st.create_stream({"name": "Coll", "connection": "c1", "topic": "coll", "target_table": "coll_t", "max_wait_seconds": 1}, "admin"); st.sync_runners()
wait_for(lambda: n("coll_t") == 40); stop(sc["id"])
mk_topic(B2, "coll"); send(B2, "coll", range(500, 510))
plan = ops.move(sc["id"], {"connection": "c2"}, {"mode": "earliest"}, False, True, "admin")
check("a different cluster is detected", plan["source_changed"] is True and any("different Kafka cluster" in x for x in plan["notes"]), plan)
check("without a start position it is refused", raises(lambda: ops.move(sc["id"], {"connection": "c2"}, None, False, False, "admin"), frag="where to start"))
ops.move(sc["id"], {"connection": "c2"}, {"mode": "earliest"}, False, False, "admin")
start(sc["id"]); wait_for(lambda: n("coll_t") == 50); time.sleep(1.5)
check("the new cluster's topic is read from its start although the old topic had the same name and higher offsets", ids_of("coll_t") == list(range(40)) + list(range(500, 510)), (n("coll_t"), ids_of("coll_t")[-12:]))
stop(sc["id"])

print("an interrupted move is completed")
mk_topic(B1, "coll2"); send(B1, "coll2", range(2000, 2006))
calls["n"] = 0; ops._record = flaky
ok = raises(lambda: ops.move(sc["id"], {"connection": "c1", "topic": "coll2", "target_table": "ops_t2"}, {"mode": "earliest"}, False, False, "admin"), RuntimeError, "simulated")
ops._record = real_record
check("the move is pending, the definition unchanged", (ok or calls["n"] == 1) and st.get_stream(sc["id"])["pending"] and st.get_stream(sc["id"])["topic"] == "coll")
st.sync_runners(); g = st.get_stream(sc["id"])
check("the daemon completes it", g["pending"] is False and g["topic"] == "coll2" and g["target_table"] == "ops_t2" and g["connection"] == "c1", g)
start(sc["id"]); wait_for(lambda: n("ops_t2") == 11); time.sleep(1.5)
check("and the stream reads the new topic into the existing table exactly once", ids_of("ops_t2") == list(range(80, 85)) + list(range(2000, 2006)) and dups("ops_t2") == 0, ids_of("ops_t2"))
st.shutdown(); time.sleep(2); shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.stdout.flush(); os._exit(1 if FAIL else 0)
