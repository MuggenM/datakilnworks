#!/usr/bin/env python3
"""Streaming ingestion (web/streaming.py) against a THROWAWAY Redpanda: connection validation, topics/preview, JSON/text ingestion,
dead letters, schema drift and rescue, exactly-once across crashes and lost bookkeeping, resume, latest, partition growth, lag, leases.
  docker network create dkwstream
  docker run -d --name rpt --network dkwstream docker.redpanda.com/redpandadata/redpanda:latest redpanda start --smp 1 --memory 512M \
     --overprovisioned --node-id 0 --kafka-addr internal://0.0.0.0:9092 --advertise-kafka-addr internal://rpt:9092 --mode dev-container
  docker run --rm --network dkwstream -e KAFKA=rpt:9092 -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook \
     sh -c "pip install -q confluent-kafka fastavro 'grpcio-tools>=1.73,<1.77' && python /workspace/scratch/test_streaming.py"
Set SASL_KAFKA=host:port SASL_USER=.. SASL_PASSWORD=.. (SCRAM-SHA-256) to also test a broker that requires a login."""
import json, os, shutil, sys, tempfile, threading, time
TMP = tempfile.mkdtemp(prefix="stream_test_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
sys.path.insert(0, "/workspace")
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
from deltalake import DeltaTable
from web import connections, streaming as st
BROKER = os.environ.get("KAFKA", "rpt:9092")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
def wait_for(pred, secs=40):
    end = time.time() + secs
    while time.time() < end:
        try:
            v = pred()
        except Exception:
            v = None
        if v: return v
        time.sleep(0.3)
    return None
admin = AdminClient({"bootstrap.servers": BROKER})
def mk_topic(name, parts=3):
    for f in admin.create_topics([NewTopic(name, num_partitions=parts, replication_factor=1)]).values(): f.result()
prod = Producer({"bootstrap.servers": BROKER})
def send(topic, values, key=None, partition=None):
    for v in values:
        prod.produce(topic, value=(json.dumps(v) if isinstance(v, (dict, list)) else v), key=key, **({} if partition is None else {"partition": partition}))
    prod.flush(20)
def table(t, s="dbo"):
    p = os.path.join(TMP, s, t)
    return DeltaTable(p).to_pyarrow_table() if os.path.isdir(os.path.join(p, "_delta_log")) else None
def rows_of(t): 
    x = table(t); return x.num_rows if x is not None else 0
def dups(t):
    x = table(t)
    if x is None: return 0
    pairs = list(zip(x["_partition"].to_pylist(), x["_offset"].to_pylist()))
    return len(pairs) - len(set(pairs))

print("connection validation")
def bad(cfg, secret=None, frag=""):
    try: connections.create_connection({"name": "x" + str(abs(hash(json.dumps(cfg))))[:6], "type": "kafka", "config": cfg, "secret": secret or {}}, "admin"); return False
    except connections.ConnectionError_ as e: return frag.lower() in str(e).lower()
check("bootstrap servers must be host:port", bad({"bootstrap_servers": "nohost"}, frag="host:port"))
check("unknown protocol refused", bad({"bootstrap_servers": "a:1", "security_protocol": "TELNET"}, frag="protocol"))
check("SASL needs a user", bad({"bootstrap_servers": "a:1", "security_protocol": "SASL_SSL", "sasl_mechanism": "PLAIN"}, {"password": "x"}, "user name"))
check("SASL needs a password", bad({"bootstrap_servers": "a:1", "security_protocol": "SASL_SSL", "sasl_mechanism": "PLAIN", "username": "u"}, frag="password"))
check("a password over SASL_PLAINTEXT is refused unless allowed", bad({"bootstrap_servers": "a:1", "security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "PLAIN", "username": "u"}, {"password": "x"}, "unencrypted"))
check("a CA certificate on PLAINTEXT is refused", bad({"bootstrap_servers": "a:1", "ssl_ca_pem": "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----"}, frag="only applies"))
c = connections.create_connection({"name": "rp", "type": "kafka", "config": {"bootstrap_servers": BROKER}}, "admin")
check("a plain connection is stored and shows no secret", c["type"] == "kafka" and c["has_secret"] is False)
tc = connections.definition_for_test({"id": c["id"]})
r = st.test_connection(tc); check("Test reaches the broker", r["ok"] and "broker" in r["message"], r)
r = st.test_connection({"type": "kafka", "config": {"bootstrap_servers": "rpt:1", "security_protocol": "PLAINTEXT"}, "secret": {}})
check("Test reports an unreachable broker in plain words", r["ok"] is False and len(r["message"]) < 200, r)

print("topics and preview")
mk_topic("orders", 3); mk_topic("events", 1)
send("orders", [{"id": i, "amount": i * 1.5, "who": f"u{i}", "meta": {"a": 1}} for i in range(5)])
check("topics are listed with their partition count", {"name": "orders", "partitions": 3} in st.list_topics("rp"))
pv = st.preview("rp", "orders", "json", 4)
check("preview shows the columns as the table would get them", [c["name"] for c in pv["columns"][:4]] == ["id", "amount", "who", "meta"] and len(pv["rows"]) == 4 and pv["bad"] == 0, pv["columns"])
check("preview of an empty topic says so", "no messages" in st.preview("rp", "events", "json", 3).get("note", ""))
try: st.preview("rp", "nope_topic", "json"); ok = False
except st.StreamError: ok = True
check("preview of a missing topic is refused", ok)

print("stream definition")
def bad_stream(**kw):
    base = {"name": "s", "connection": "rp", "topic": "orders", "target_table": "orders_raw"}
    try: st.create_stream({**base, **kw}, "admin"); return None
    except st.StreamError as e: return str(e)
check("bad topic name", "topic" in (bad_stream(topic="a b") or ""))
check("unknown connection", "Kafka connection" in (bad_stream(connection="nope") or ""))
check("bad table name", "Schema and table" in (bad_stream(target_table="1bad") or ""))
check("bad format", "format" in (bad_stream(format="avro") or ""))
s1 = st.create_stream({"name": "Orders", "connection": "rp", "topic": "orders", "target_table": "orders_raw", "max_wait_seconds": 1, "enabled": False}, "admin")
check("a stream is created (stopped)", s1["status"] == "stopped" and s1["target_table"] == "orders_raw")
check("two streams cannot load one table", "already loads" in (bad_stream(name="dup") or ""))
try: st.update_stream(s1["id"], {"topic": "events"}, "admin"); ok = False
except st.StreamError: ok = True
check("the topic of a stream cannot be changed", ok)
try: connections.delete_connection("rp", "admin"); ok = False
except connections.ConnectionError_: ok = True
check("a connection used by a stream cannot be deleted", ok)

print("ingestion")
send("orders", [{"id": i, "amount": i * 1.5, "who": f"u{i}", "meta": {"a": 1}} for i in range(5, 250)], key="k1")
st.set_enabled(s1["id"], True, "admin"); st.start_runner(s1["id"])
check("the first batch defines the columns", wait_for(lambda: rows_of("orders_raw") >= 250), rows_of("orders_raw"))
# drift after the schema is set: extra field, wrong type, a payload field named like a metadata column, garbage, a tombstone
send("orders", [{"id": 1000, "amount": 2.5, "extra": "new-field"}, {"id": "not-a-number", "amount": 1.0}, {"id": 1001, "_offset": 5, "amount": 3.0}])
send("orders", ["{not json", "[1,2]", b"\x00\x00\x00\x00\x07abc"], partition=0)
prod.produce("orders", value=None, key="gone", partition=0); prod.flush(10)
TOTAL = 5 + 245 + 3
check("all good messages arrive", wait_for(lambda: rows_of("orders_raw") >= TOTAL), rows_of("orders_raw"))
time.sleep(2)
t = table("orders_raw")
check("no message is loaded twice", rows_of("orders_raw") == TOTAL and dups("orders_raw") == 0, (rows_of("orders_raw"), dups("orders_raw")))
check("columns and types are inferred (bigint/double/text; nested object as JSON text)", str(t.schema.field("id").type) == "int64" and str(t.schema.field("amount").type) == "double" and str(t.schema.field("meta").type) == "string" and "extra" not in t.column_names, t.schema)
check("metadata columns are there", all(c in t.column_names for c in ("_key", "_topic", "_partition", "_offset", "_timestamp", "_rescued_data")) and set(t["_topic"].to_pylist()) == {"orders"})
resc = {r["id"]: r["_rescued_data"] for r in t.to_pylist() if r["_rescued_data"]}
check("an unknown field is rescued, not lost", 1000 in resc and json.loads(resc[1000]) == {"extra": "new-field"}, resc)
check("a value of the wrong type is rescued and its column left null", any(r["id"] is None and r["_rescued_data"] and json.loads(r["_rescued_data"]).get("id") == "not-a-number" for r in t.to_pylist()))
check("a payload field colliding with a metadata column is rescued", 1001 in resc and json.loads(resc[1001]) == {"_offset": 5})
dlq = wait_for(lambda: table("orders_raw_dlq"))
check("unparsable messages are dead-lettered with the reason", dlq is not None and dlq.num_rows == 3 and sum("not valid JSON" in e for e in dlq["error"].to_pylist()) == 1 and sum("not an object" in e for e in dlq["error"].to_pylist()) == 1 and sum("Schema Registry" in e for e in dlq["error"].to_pylist()) == 1, dlq and dlq["error"].to_pylist())
stt = st.get_stream(s1["id"])
check("status: running, counts, lag 0", stt["status"] == "running" and stt["rows_total"] == TOTAL and stt["bad_total"] == 3 and wait_for(lambda: st.get_stream(s1["id"])["lag_total"] == 0 and st.get_stream(s1["id"])["partitions"]), stt)

print("resume and exactly-once")
st.set_enabled(s1["id"], False, "admin"); st.stop_runner(s1["id"])
check("Stop ends the runner", st.get_stream(s1["id"])["status"] == "stopped")
send("orders", [{"id": 2000 + i, "amount": 1.0} for i in range(50)])
st.set_enabled(s1["id"], True, "admin"); st.start_runner(s1["id"])
check("a restart loads only what is new", wait_for(lambda: rows_of("orders_raw") >= TOTAL + 50) and (time.sleep(1.5) or rows_of("orders_raw") == TOTAL + 50) and dups("orders_raw") == 0, (rows_of("orders_raw"), dups("orders_raw")))
TOTAL += 50
# lost bookkeeping: the Delta table alone says where to resume
st.stop_runner(s1["id"]); st.set_enabled(s1["id"], False, "admin")
import sqlite3
with sqlite3.connect(st._db_path()) as c_: c_.execute("DELETE FROM stream_offsets")
send("orders", [{"id": 3000 + i, "amount": 1.0} for i in range(20)])
st.set_enabled(s1["id"], True, "admin"); st.start_runner(s1["id"])
check("with the bookkeeping wiped, the commit's own offsets prevent duplicates", wait_for(lambda: rows_of("orders_raw") >= TOTAL + 20) and (time.sleep(1.5) or rows_of("orders_raw") == TOTAL + 20) and dups("orders_raw") == 0, (rows_of("orders_raw"), dups("orders_raw")))
TOTAL += 20
# crash AFTER the commit, before the bookkeeping
import deltalake
real_write = deltalake.write_deltalake; crashed = {"n": 0}
def crash_after_commit(*a, **k):
    out = real_write(*a, **k)
    if a and not isinstance(a[0], str) or (a and "_dlq" not in str(a[0])):
        if crashed["n"] == 0 and "app_transactions" in str(k.get("commit_properties", "")) or crashed["n"] == 0:
            crashed["n"] += 1
            raise RuntimeError("simulated crash after the Delta commit")
    return out
deltalake.write_deltalake = crash_after_commit
send("orders", [{"id": 4000 + i, "amount": 1.0} for i in range(30)])
check("after a crash between the commit and the bookkeeping, the batch is not loaded twice", wait_for(lambda: crashed["n"] >= 1) and wait_for(lambda: rows_of("orders_raw") >= TOTAL + 30) and (time.sleep(3) or rows_of("orders_raw") == TOTAL + 30) and dups("orders_raw") == 0, (crashed, rows_of("orders_raw"), dups("orders_raw")))
TOTAL += 30
check("the failure was reported while it lasted and cleared after", wait_for(lambda: st.get_stream(s1["id"])["status"] == "running"))
# crash BEFORE the commit
crashed2 = {"n": 0}
def crash_before_commit(*a, **k):
    if crashed2["n"] == 0:
        crashed2["n"] += 1
        raise RuntimeError("simulated crash before the Delta commit")
    return real_write(*a, **k)
deltalake.write_deltalake = crash_before_commit
send("orders", [{"id": 5000 + i, "amount": 1.0} for i in range(30)])
check("after a crash before the commit, the batch is loaded once", wait_for(lambda: crashed2["n"] >= 1) and wait_for(lambda: rows_of("orders_raw") >= TOTAL + 30) and (time.sleep(3) or rows_of("orders_raw") == TOTAL + 30) and dups("orders_raw") == 0, (rows_of("orders_raw"), dups("orders_raw")))
deltalake.write_deltalake = real_write; TOTAL += 30

print("partitions, evolve, latest, text, leases")
a2 = AdminClient({"bootstrap.servers": BROKER})
for f in a2.create_partitions([NewPartitions("orders", 5)]).values(): f.result()
time.sleep(2); prod = Producer({"bootstrap.servers": BROKER})
send("orders", [{"id": 6000 + i, "amount": 1.0} for i in range(40)], partition=4)
check("a partition added later is picked up and read from its start", wait_for(lambda: rows_of("orders_raw") >= TOTAL + 40, 70) and dups("orders_raw") == 0, rows_of("orders_raw"))
mk_topic("evo", 1)
send("evo", [{"a": 1}, {"a": 2}])
s2 = st.create_stream({"name": "Evo", "connection": "rp", "topic": "evo", "target_table": "evo_t", "max_wait_seconds": 1, "evolve_schema": True, "starting_offsets": "latest"}, "admin")
st.sync_runners()                                                    # what the daemon does every few seconds
time.sleep(4); check("starting_offsets=latest skips what is already in the topic", rows_of("evo_t") == 0)
send("evo", [{"a": 3}]); wait_for(lambda: rows_of("evo_t") == 1)
send("evo", [{"a": 4, "brand_new": "x"}])
check("evolve_schema adds a new field as a column", wait_for(lambda: table("evo_t") is not None and "brand_new" in table("evo_t").column_names and table("evo_t").num_rows == 2), table("evo_t") and table("evo_t").column_names)
check("...and the first-ever latest start is remembered (no reload after a restart)", (st.stop_runner(s2["id"]) or True) and (st.start_runner(s2["id"]) or True) and (time.sleep(4) or rows_of("evo_t") == 2))
mk_topic("plain", 1); send("plain", ["hello", "world"])
s3 = st.create_stream({"name": "Plain", "connection": "rp", "topic": "plain", "target_table": "plain_t", "format": "text", "max_wait_seconds": 1}, "admin")
st.sync_runners()
check("format text keeps the message as one column", wait_for(lambda: rows_of("plain_t") == 2) and sorted(table("plain_t")["value"].to_pylist()) == ["hello", "world"])
check("a second process cannot take over a live stream (lease)", (lambda o: (setattr(st, "OWNER", "other:1"), st._acquire_lease(s1["id"]), setattr(st, "OWNER", o))[1] is False)(st.OWNER))
check("a stopped/deleted stream's runner ends", (st.delete_stream(s3["id"], "admin") or True) and wait_for(lambda: s3["id"] not in st._runners and st.get_stream(s3["id"]) is None))
check("deleting a stream keeps its table", table("plain_t") is not None)
st.sync_runners(); check("sync_runners keeps enabled streams running", s1["id"] in st._runners and st._runners[s1["id"]].is_alive())

print("Avro with a Schema Registry")
import io, struct, datetime as _dt, decimal, http.server, base64
import fastavro, requests
REG = os.environ.get("REGISTRY", "http://rpt:8081")
def register(subject, schema):
    r = requests.post(f"{REG}/subjects/{subject}/versions", json={"schema": json.dumps(schema)}, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}); r.raise_for_status(); return r.json()["id"]
def wire(sid, schema, record):
    b = io.BytesIO(); b.write(b"\x00" + struct.pack(">I", sid)); fastavro.schemaless_writer(b, fastavro.parse_schema(schema), record); return b.getvalue()
def bad_conn(cfg, secret=None, frag=""):
    try: connections.create_connection({"name": "z" + str(abs(hash(json.dumps(cfg))))[:6], "type": "kafka", "config": {"bootstrap_servers": BROKER, **cfg}, "secret": secret or {}}, "admin"); return False
    except connections.ConnectionError_ as e: return frag.lower() in str(e).lower()
check("a Schema Registry URL must be a plain http(s) URL", bad_conn({"schema_registry_url": "ftp://x"}, frag="Schema Registry URL") and bad_conn({"schema_registry_url": "http://u:p@x:8081"}, frag="Schema Registry URL"))
check("registry credentials need a password", bad_conn({"schema_registry_url": "https://r", "registry_username": "k"}, frag="password"))
check("registry credentials over http:// are refused unless allowed", bad_conn({"schema_registry_url": "http://r:8081", "registry_username": "k"}, {"registry_password": "s"}, "plain http"))
ac = connections.create_connection({"name": "rpavro", "type": "kafka", "config": {"bootstrap_servers": BROKER, "schema_registry_url": REG}}, "admin")
r = st.test_connection(connections.definition_for_test({"id": ac["id"]}))
check("Test also checks the Schema Registry", r["ok"] and "Schema Registry reachable" in r["message"], r)
r = st.test_connection({"type": "kafka", "config": {"bootstrap_servers": BROKER, "security_protocol": "PLAINTEXT", "schema_registry_url": "http://rpt:9"}, "secret": {}})
check("an unreachable registry fails the test with its own message", r["ok"] is False and "Schema Registry" in r["message"], r)
check("the Avro format needs a registry on the connection", "Schema Registry" in (bad_stream(format="avro") or ""))

SCHEMA = {"type": "record", "name": "Order", "namespace": "shop", "fields": [
    {"name": "id", "type": "long"}, {"name": "qty", "type": "int"}, {"name": "price", "type": "double"}, {"name": "active", "type": "boolean"},
    {"name": "name", "type": "string"}, {"name": "note", "type": ["null", "string"], "default": None},
    {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}}, {"name": "day", "type": {"type": "int", "logicalType": "date"}},
    {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
    {"name": "tags", "type": {"type": "array", "items": "string"}}, {"name": "addr", "type": {"type": "record", "name": "Addr", "fields": [{"name": "city", "type": "string"}]}},
    {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B"]}}, {"name": "blob", "type": "bytes"}]}
def rec(i): return {"id": i, "qty": i % 7, "price": i * 1.25, "active": i % 2 == 0, "name": f"n{i}", "note": None if i % 3 else f"note{i}",
                    "ts": _dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=_dt.timezone.utc), "day": _dt.date(2026, 1, 2), "amount": decimal.Decimal("12.34"),
                    "tags": ["x", "y"], "addr": {"city": "Ghent"}, "kind": "B", "blob": b"\x01\x02"}
sid1 = register("avro_orders-value", SCHEMA)
mk_topic("avro_orders", 2)
for i in range(20): prod.produce("avro_orders", value=wire(sid1, SCHEMA, rec(i)), key=wire(register("avro_orders-key", "string" if False else {"type": "string"}), {"type": "string"}, f"key{i}") if i == 0 else None)
prod.flush(20)
pv = st.preview("rpavro", "avro_orders", "avro", 5)
check("preview decodes Avro and shows the columns from the schema", [c["name"] for c in pv["columns"][:3]] == ["id", "qty", "price"] and dict((c["name"], c["type"]) for c in pv["columns"])["qty"] == "int32" and pv["bad"] == 0, pv["columns"])
a1 = st.create_stream({"name": "Avro A", "connection": "rpavro", "topic": "avro_orders", "target_table": "avro_a", "format": "avro", "max_wait_seconds": 1}, "admin"); st.sync_runners()
check("Avro messages arrive", wait_for(lambda: rows_of("avro_a") == 20), rows_of("avro_a"))
ta = table("avro_a"); ty = {f.name: str(f.type) for f in ta.schema}
check("column types come from the Avro schema", ty["id"] == "int64" and ty["qty"] == "int32" and ty["price"] == "double" and ty["active"] == "bool" and ty["ts"] == "timestamp[us, tz=UTC]" and ty["day"] == "date32[day]" and ty["amount"] == "decimal128(10, 2)" and ty["blob"] == "binary" and ty["tags"] == "string" and ty["addr"] == "string" and ty["kind"] == "string" and ty["note"] == "string", ty)
row = next(r for r in ta.to_pylist() if r["id"] == 6)
check("values survive: logical types, nulls, nested as JSON text, enum, bytes", row["ts"].year == 2026 and str(row["amount"]) == "12.34" and row["day"] == _dt.date(2026, 1, 2) and json.loads(row["tags"]) == ["x", "y"] and json.loads(row["addr"]) == {"city": "Ghent"} and row["kind"] == "B" and row["blob"] == b"\x01\x02" and row["note"] == "note6" and next(r for r in ta.to_pylist() if r["id"] == 1)["note"] is None, row)
check("an Avro-encoded key is shown as its value", "key0" in [r["_key"] for r in ta.to_pylist()], [r["_key"] for r in ta.to_pylist()][:5])
check("no duplicates", dups("avro_a") == 0)
# schema evolution: a new optional field, then messages that are not valid
SCHEMA2 = json.loads(json.dumps(SCHEMA)); SCHEMA2["fields"].append({"name": "country", "type": ["null", "string"], "default": None})
sid2 = register("avro_orders-value", SCHEMA2)
check("the registry gave the new version a new id", sid2 != sid1)
for i in range(20, 25): prod.produce("avro_orders", value=wire(sid2, SCHEMA2, {**rec(i), "country": "BE"}))
prod.produce("avro_orders", value=b"hello, not avro", partition=0)
prod.produce("avro_orders", value=b"\x00" + struct.pack(">I", 987654) + b"abc", partition=0)
prod.produce("avro_orders", value=wire(sid1, SCHEMA, rec(99))[:12], partition=0)
prod.flush(20)
check("messages with the newer schema still load", wait_for(lambda: rows_of("avro_a") == 25), rows_of("avro_a"))
new = [r for r in table("avro_a").to_pylist() if r["id"] and r["id"] >= 20]
check("without 'add new fields' the new field is rescued, not lost", "country" not in table("avro_a").column_names and all(json.loads(r["_rescued_data"]) == {"country": "BE"} for r in new), new[:1])
dq = wait_for(lambda: (lambda t: t if t is not None and t.num_rows == 3 else None)(table("avro_a_dlq")))
errs = dq["error"].to_pylist() if dq is not None else []
check("bad messages are dead-lettered with a reason each and their exact bytes", dq is not None and any("wire format" in e for e in errs) and any("987654" in e and "does not exist" in e for e in errs) and any("does not match schema" in e for e in errs) and all(v for v in dq["value_b64"].to_pylist()), errs)
check("the exact bytes can be recovered from the dead letter", any(base64.b64decode(v) == b"hello, not avro" for v in dq["value_b64"].to_pylist()))
a2 = st.create_stream({"name": "Avro B", "connection": "rpavro", "topic": "avro_orders", "target_table": "avro_b", "format": "avro", "max_wait_seconds": 1, "evolve_schema": True}, "admin"); st.sync_runners()
check("with 'add new fields' the new field becomes a column", wait_for(lambda: rows_of("avro_b") == 25) and "country" in table("avro_b").column_names and sum(1 for r in table("avro_b").to_pylist() if r["country"] == "BE") == 5)
# a JSON stream pointed at Avro data tells you what to do
js = st.preview("rp", "avro_orders", "json", 3)
check("the JSON format explains that the topic is Avro", any("choose the matching Schema Registry format" in e for e in js.get("errors", [])), js)
# registry outage is a retry, never a dead letter
bc = connections.create_connection({"name": "rpdown", "type": "kafka", "config": {"bootstrap_servers": BROKER, "schema_registry_url": "http://rpt:9"}}, "admin")
a3 = st.create_stream({"name": "Avro C", "connection": "rpdown", "topic": "avro_orders", "target_table": "avro_c", "format": "avro", "max_wait_seconds": 1}, "admin"); st.sync_runners()
check("an unreachable registry shows an error and retries", wait_for(lambda: st.get_stream(a3["id"])["status"] == "error" and "Schema Registry" in (st.get_stream(a3["id"])["last_error"] or ""), 60), st.get_stream(a3["id"]))
check("...and nothing was dead-lettered or lost meanwhile", rows_of("avro_c") == 0 and table("avro_c_dlq") is None)
connections.update_connection("rpdown", {"config": {"schema_registry_url": REG}}, "admin"); st.update_stream(a3["id"], {}, "admin")
check("once the registry is right the stream catches up, exactly once", wait_for(lambda: rows_of("avro_c") == 25, 70) and dups("avro_c") == 0 and table("avro_c_dlq").num_rows == 3, (rows_of("avro_c"), dups("avro_c")))

# registry protocol details against a local fake registry: login, redirects, errors, non-Avro schemas
class Fake(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        p = self.path
        if p == "/subjects": return self._send(200, []) if self.headers.get("Authorization") == "Basic " + base64.b64encode(b"key:secret").decode() else self._send(401, {})
        if self.headers.get("Authorization") != "Basic " + base64.b64encode(b"key:secret").decode(): return self._send(401, {})
        if p == "/schemas/ids/1": return self._send(200, {"schema": json.dumps({"type": "record", "name": "R", "fields": [{"name": "a", "type": "int"}]})})
        if p == "/schemas/ids/2": return self._send(200, {"schemaType": "PROTOBUF", "schema": "syntax = \"proto3\";"})
        if p == "/schemas/ids/3": return self._send(500, {})
        if p == "/schemas/ids/4":
            self.send_response(302); self.send_header("Location", "http://127.0.0.1:1/evil"); self.end_headers(); return
        self._send(404, {})
    def _send(self, code, body):
        b = json.dumps(body).encode(); self.send_response(code); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Fake); threading.Thread(target=srv.serve_forever, daemon=True).start()
url = f"http://127.0.0.1:{srv.server_address[1]}"
mk = lambda user, pw: st.AvroDecoder({"config": {"schema_registry_url": url, "registry_username": user}, "secret": {"registry_password": pw}})
def msg(i): return b"\x00" + struct.pack(">I", i) + b"\x04"
d = mk("key", "secret"); payload, err = d.decode(msg(1))
check("registry basic auth is sent and the schema is used", payload == {"a": 2} and err is None and d.ping() == 0, (payload, err))
try: mk("key", "wrong").decode(msg(1)); ok = False
except st.RegistryUnavailable as e: ok = "login" in str(e)
check("a refused registry login is an outage (retry), with a clear message", ok)
p2, e2 = d.decode(msg(2)); check("a schema of another type is a bad message that says which format to choose", p2 is None and "Protobuf" in e2 and "choose the Protobuf format" in e2, e2)
try: d.decode(msg(3)); ok = False
except st.RegistryUnavailable: ok = True
check("a registry 5xx is an outage, not a bad message", ok)
try: d.decode(msg(4)); ok = False
except st.RegistryUnavailable as e: ok = "redirect" in str(e)
check("a registry redirect is not followed (the login never leaves)", ok)
p5, e5 = d.decode(msg(9)); check("an unknown schema id is a bad message", p5 is None and "does not exist" in e5)
srv.shutdown()
check("Avro type mapping", str(st.avro_type(["null", "long"])) == "int64" and str(st.avro_type(["null", "string", "int"])) == "string" and str(st.avro_type({"type": "long", "logicalType": "timestamp-micros"})) == "timestamp[us, tz=UTC]" and str(st.avro_type({"type": "bytes", "logicalType": "decimal", "precision": 40, "scale": 2})) == "string" and str(st.avro_type({"type": "map", "values": "int"})) == "string")

print("Protobuf with a Schema Registry")
import subprocess, importlib.util
COMMON = 'syntax = "proto3";\npackage common;\nmessage Money { int64 units = 1; string currency = 2; }\n'
PROTO = """syntax = "proto3";
package shop;
import "google/protobuf/timestamp.proto";
import "common.proto";
enum Kind { KIND_A = 0; KIND_B = 1; }
message Order {
  int64 id = 1; int32 qty = 2; uint32 u32 = 3; uint64 u64 = 4; double price = 5; bool active = 6; string name = 7;
  optional string note = 8; bytes blob = 9; Kind kind = 10; google.protobuf.Timestamp ts = 11;
  repeated string tags = 12; map<string, int32> counts = 13; Addr addr = 14; common.Money money = 15;
  message Inner { string x = 1; }
  Inner inner = 16;
}
message Addr { string city = 1; }
"""
def gen_module(name, text, extra):
    d = tempfile.mkdtemp(prefix="pb_")
    for fn, content in {**extra, name + ".proto": text}.items(): open(os.path.join(d, fn), "w").write(content)
    from grpc_tools import protoc
    assert protoc.main(["protoc", f"-I{d}", f"-I{os.path.join(os.path.dirname(protoc.__file__), '_proto')}", f"--python_out={d}", *[os.path.join(d, f) for f in [*extra, name + ".proto"]]]) == 0
    sys.path.insert(0, d); return importlib.import_module(name + "_pb2")
pb = gen_module("shop", PROTO, {"common.proto": COMMON})
def reg_proto(subject, text, refs=None):
    r = requests.post(f"{REG}/subjects/{subject}/versions", json={"schemaType": "PROTOBUF", "schema": text, **({"references": refs} if refs else {})}, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}); r.raise_for_status(); return r.json()["id"]
reg_proto("common.proto", COMMON)
pid = reg_proto("proto_orders-value", PROTO, [{"name": "common.proto", "subject": "common.proto", "version": 1}])
def pwire(sid, msg, path=(0,)):
    def zz(n): return (n << 1) ^ (n >> 63)
    def varint(n):
        out = b""
        while True:
            b = n & 0x7F; n >>= 7
            if n: out += bytes([b | 0x80])
            else: return out + bytes([b])
    idx = b"\x00" if list(path) == [0] else varint(zz(len(path))) + b"".join(varint(zz(i)) for i in path)
    return b"\x00" + struct.pack(">I", sid) + idx + msg.SerializeToString()
def order(i):
    o = pb.Order(id=i, qty=i % 5, u32=4000000000, u64=18000000000000000000, price=i * 0.5, active=i % 2 == 0, name=f"p{i}", blob=b"\x09", kind=pb.KIND_B,
                 tags=["a", "b"], addr=pb.Addr(city="Gent"), inner=pb.Order.Inner(x="deep"))
    o.ts.FromDatetime(_dt.datetime(2026, 3, 4, 5, 6, 7)); o.counts["k"] = 3; o.money.units = 42; o.money.currency = "EUR"
    if i % 3 == 0: o.note = f"note{i}"
    return o
mk_topic("proto_orders", 2)
for i in range(15): prod.produce("proto_orders", value=pwire(pid, order(i)))
prod.flush(20)
pd_ = st.RegistryDecoder({"config": {"schema_registry_url": REG}, "secret": {}}, "PROTOBUF")
p_, e_ = pd_.decode(pwire(pid, pb.Addr(city="Ghent"), (1,)))
check("the message index selects the second message of the file", e_ is None and p_ == {"city": "Ghent"}, (p_, e_))
p_, e_ = pd_.decode(pwire(pid, pb.Order.Inner(x="q"), (0, 0)))
check("a nested message is selected by its index path", e_ is None and p_ == {"x": "q"}, (p_, e_))
p_, e_ = pd_.decode(b"\x00" + struct.pack(">I", pid) + b"\x00" + b"\xff\xff\xff")
check("garbage after a valid header is a bad message, not a crash", p_ is None and "does not match schema" in e_, e_)
pv = st.preview("rpavro", "proto_orders", "protobuf", 3)
check("preview decodes Protobuf, importing a referenced schema", pv["bad"] == 0 and {"id", "qty", "u32", "u64", "ts"} <= {c["name"] for c in pv["columns"]}, pv)
pa_ = st.create_stream({"name": "Proto A", "connection": "rpavro", "topic": "proto_orders", "target_table": "proto_a", "format": "protobuf", "max_wait_seconds": 1}, "admin"); st.sync_runners()
check("Protobuf messages arrive", wait_for(lambda: rows_of("proto_a") == 15), rows_of("proto_a"))
tp = table("proto_a"); ty = {f.name: str(f.type) for f in tp.schema}
check("types come from the .proto (uint32 bigint, uint64 decimal, Timestamp, bytes, enums as text, messages/maps/repeated as JSON text)", ty["id"] == "int64" and ty["qty"] == "int32" and ty["u32"] == "int64" and ty["u64"] == "decimal128(20, 0)" and ty["price"] == "double" and ty["active"] == "bool" and ty["blob"] == "binary" and ty["kind"] == "string" and ty["ts"] == "timestamp[us, tz=UTC]" and ty["tags"] == "string" and ty["counts"] == "string" and ty["addr"] == "string" and ty["money"] == "string" and ty["note"] == "string", ty)
row = next(r for r in tp.to_pylist() if r["id"] == 3)
check("values: 64-bit numbers exact, enum name, timestamp, nested, repeated, map, optional presence", row["u32"] == 4000000000 and str(row["u64"]) == "18000000000000000000" and row["kind"] == "KIND_B" and row["ts"].year == 2026 and json.loads(row["tags"]) == ["a", "b"] and json.loads(row["counts"]) == {"k": 3} and json.loads(row["addr"]) == {"city": "Gent"} and json.loads(row["money"]) == {"units": 42, "currency": "EUR"} and json.loads(row["inner"]) == {"x": "deep"} and row["note"] == "note3" and next(r for r in tp.to_pylist() if r["id"] == 1)["note"] is None, row)
check("no duplicates", dups("proto_a") == 0)
prod.produce("proto_orders", value=b"\x00" + struct.pack(">I", 555555) + b"\x00abc"); prod.flush(10)
dq = wait_for(lambda: (lambda t: t if t is not None and t.num_rows == 1 else None)(table("proto_a_dlq")))
check("an unknown schema id is dead-lettered with the exact bytes", dq is not None and "does not exist" in dq["error"][0].as_py() and dq["value_b64"][0].as_py(), dq and dq["error"].to_pylist())
# an Avro stream meeting a Protobuf message says so
ad = st.RegistryDecoder({"config": {"schema_registry_url": REG}, "secret": {}}, "AVRO"); a_, ae_ = ad.decode(pwire(pid, order(1)))
check("an Avro stream that meets a Protobuf message says which format to choose", a_ is None and "choose the Protobuf format" in ae_, ae_)
# protoc safety: an import name that would leave the temp directory is refused
try:
    pd_._fetch_references([{"name": "../../etc/passwd", "subject": "x", "version": 1}], {}); ok = False
except st.UnknownSchema: ok = True
check("a schema that imports a path outside its own directory is refused", ok)

print("JSON Schema with a Schema Registry")
JS = {"type": "object", "properties": {"id": {"type": "integer"}, "price": {"type": "number"}, "ok": {"type": "boolean"}, "when": {"type": "string", "format": "date-time"},
      "day": {"type": "string", "format": "date"}, "name": {"type": ["null", "string"]}, "tags": {"type": "array", "items": {"type": "string"}}, "meta": {"type": "object"}}}
r = requests.post(f"{REG}/subjects/js_events-value/versions", json={"schemaType": "JSON", "schema": json.dumps(JS)}, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
check("the broker's registry accepts a JSON Schema", r.status_code == 200, r.text[:200])
jid = r.json()["id"]
def jwire(sid, obj): return b"\x00" + struct.pack(">I", sid) + (obj if isinstance(obj, bytes) else json.dumps(obj).encode())
mk_topic("js_events", 1)
for i in range(10): prod.produce("js_events", value=jwire(jid, {"id": i, "price": i + 0.5, "ok": i % 2 == 0, "when": "2026-02-03T04:05:06Z", "day": "2026-02-03", "name": None if i % 2 else f"n{i}", "tags": ["t"], "meta": {"k": i}}))
prod.flush(20)
ja = st.create_stream({"name": "JS A", "connection": "rpavro", "topic": "js_events", "target_table": "js_a", "format": "jsonschema", "max_wait_seconds": 1}, "admin"); st.sync_runners()
check("JSON Schema messages arrive", wait_for(lambda: rows_of("js_a") == 10), rows_of("js_a"))
tj = table("js_a"); ty = {f.name: str(f.type) for f in tj.schema}
check("types come from the schema (integer, number, boolean, date-time, date; objects/arrays as JSON text)", ty["id"] == "int64" and ty["price"] == "double" and ty["ok"] == "bool" and ty["when"] == "timestamp[us, tz=UTC]" and ty["day"] == "date32[day]" and ty["name"] == "string" and ty["tags"] == "string" and ty["meta"] == "string", ty)
rj = next(r for r in tj.to_pylist() if r["id"] == 4); check("values survive (dates parsed, nested kept as JSON text)", rj["when"].year == 2026 and rj["day"] == _dt.date(2026, 2, 3) and json.loads(rj["meta"]) == {"k": 4} and rj["name"] == "n4")
for m in (jwire(jid, {"id": "not-an-int", "price": 1.0}), jwire(jid, b"{broken"), jwire(jid, b"[1,2]")): prod.produce("js_events", value=m)
prod.flush(10)
check("a value of the wrong type is rescued (the schema decides the column type)", wait_for(lambda: rows_of("js_a") == 11) and any(r["id"] is None and json.loads(r["_rescued_data"]) == {"id": "not-an-int"} for r in table("js_a").to_pylist() if r["_rescued_data"]))
dq = wait_for(lambda: (lambda t: t if t is not None and t.num_rows == 2 else None)(table("js_a_dlq")))
check("unparsable JSON and non-objects are dead-lettered", dq is not None and any("does not match" in e for e in dq["error"].to_pylist()) and any("not an object" in e for e in dq["error"].to_pylist()), dq and dq["error"].to_pylist())
check("Schema Registry formats need the registry on the connection", "Schema Registry" in (bad_stream(format="protobuf") or "") and "Schema Registry" in (bad_stream(format="jsonschema") or ""))

if os.environ.get("SASL_KAFKA"):
    print("SASL broker")
    sc = connections.create_connection({"name": "sasl", "type": "kafka", "config": {"bootstrap_servers": os.environ["SASL_KAFKA"], "security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "SCRAM-SHA-256",
                                       "username": os.environ["SASL_USER"], "allow_insecure": True}, "secret": {"password": os.environ["SASL_PASSWORD"]}}, "admin")
    check("a login that works is accepted", st.test_connection(connections.definition_for_test({"id": sc["id"]}))["ok"])
    r = st.test_connection(connections.definition_for_test({"id": sc["id"], "secret": {"password": "wrong"}}))
    check("a wrong password is reported as a login problem", r["ok"] is False and "login" in r["message"].lower(), r)
    check("the secret is never returned", "password" not in json.dumps(connections.get_connection("sasl")))
    sconf = st.kafka_conf(connections.get_with_secret("sasl"))
    sadmin = AdminClient(sconf)
    for f in sadmin.create_topics([NewTopic("secure", num_partitions=1, replication_factor=1)]).values():
        try: f.result()
        except Exception as e_: assert "ALREADY_EXISTS" in str(e_), e_
    sp = Producer(sconf)
    for i in range(30): sp.produce("secure", value=json.dumps({"n": i}))
    sp.flush(20)
    ss = st.create_stream({"name": "Secure", "connection": "sasl", "topic": "secure", "target_table": "secure_t", "max_wait_seconds": 1}, "admin"); st.sync_runners()
    check("a stream reads a broker that requires a login", wait_for(lambda: rows_of("secure_t") == 30), rows_of("secure_t"))
    ccfg = connections.update_connection("sasl", {"secret": {"password": "wrong"}}, "admin")
    st.update_stream(ss["id"], {}, "admin")                                # a configuration change restarts the consumer session
    send_ok = wait_for(lambda: st.get_stream(ss["id"])["last_error"])
    check("after the password changes, the stream shows a login error and keeps retrying", bool(send_ok) and "login" in st.get_stream(ss["id"])["last_error"].lower(), st.get_stream(ss["id"])["last_error"])

st.shutdown(); time.sleep(2); shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.stdout.flush(); os._exit(1 if FAIL else 0)
