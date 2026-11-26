#!/usr/bin/env python3
"""Streaming ingestion UI (Playwright, /usr/bin/python3) against a THROWAWAY studio container `stui` on network dkwstream with a throwaway
Redpanda `rpt` (see scratch/test_streaming.py for the broker command). Messages are produced with `docker exec stui python`.
  docker run -d --name stui --network dkwstream -p 8117:8891 -v $PWD/web:/workspace/web -v $PWD/docs:/workspace/docs -e WAREHOUSE_DIR=/workspace/warehouse \
    -e INIT_ADMIN_USERNAME=admin -e INIT_ADMIN_PASSWORD_HASH='<hash of adminpassword123>' localspark-lakehouse-notebook sh -c "pip install -q confluent-kafka && uvicorn web.app:app --host 0.0.0.0 --port 8891"
  GIT_UI_URL=http://localhost:8117 python scratch/verify_streaming_ui.py"""
import json, os, subprocess, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def produce(topic, values, create=False):
    code = ("import json,sys\nfrom confluent_kafka import Producer\nfrom confluent_kafka.admin import AdminClient, NewTopic\n"
            "a=AdminClient({'bootstrap.servers':'rpt:9092'})\n"
            + ("[f.result() for f in a.create_topics([NewTopic(%r,num_partitions=2,replication_factor=1)]).values()]\n" % topic if create else "")
            + "p=Producer({'bootstrap.servers':'rpt:9092'})\nfor v in json.loads(sys.stdin.read()): p.produce(%r,value=v.encode())\np.flush(20)\n" % topic)
    subprocess.run(["docker", "exec", "-i", "stui", "python", "-c", code], input=json.dumps([v if isinstance(v, str) else json.dumps(v) for v in values]).encode(), check=True, capture_output=True)
with sync_playwright() as p:
    b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1600, "height": 1100}); page = ctx.new_page()
    errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    check("admin logs in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
    produce("ui_events", [{"user": f"u{i}", "n": i, "ok": i % 2 == 0} for i in range(20)], create=True)
    page.goto(BASE, wait_until="networkidle"); time.sleep(3)
    page.evaluate("async () => { const d = Alpine.$data(document.body); d.currentView = 'autoloader'; }"); time.sleep(1)
    # --- a Kafka connection through the Connections dialog
    page.locator("[data-testid=open-connections]").click(); page.locator("[data-testid=conn-new]").click()
    page.locator("[data-testid=conn-name]").fill("redpanda"); page.locator("[data-testid=conn-type]").select_option("kafka")
    time.sleep(0.5)
    check("the Kafka form appears", page.locator("[data-testid=conn-kafka]").is_visible() and not page.locator("[data-testid=conn-base-url]").is_visible())
    page.locator("[data-testid=conn-bootstrap]").fill("rpt:9092")
    page.locator("[data-testid=conn-protocol]").select_option("SASL_PLAINTEXT"); time.sleep(0.5)
    check("SASL shows user and password fields", page.locator("[data-testid=conn-kafka-user]").is_visible() and page.locator("[data-testid=conn-kafka-password]").is_visible())
    page.locator("[data-testid=conn-kafka-user]").fill("u"); page.locator("[data-testid=conn-kafka-password]").fill("pw"); page.locator("[data-testid=conn-save]").click(); time.sleep(1)
    check("a password over SASL_PLAINTEXT is refused with the reason", "unencrypted" in page.locator("[data-testid=conn-error]").inner_text())
    page.locator("[data-testid=conn-protocol]").select_option("PLAINTEXT")
    page.locator("[data-testid=conn-test]").click(); page.locator("text=broker(s)").first.wait_for(timeout=20000)
    page.locator("[data-testid=conn-registry-url]").fill("http://rpt:8081")
    page.locator("[data-testid=conn-test]").click(); page.locator("text=Schema Registry reachable").first.wait_for(timeout=25000)
    check("Test reports the Schema Registry too", True)
    page.locator("[data-testid=conn-save]").click(); page.locator("[data-testid=conn-row]").first.wait_for(timeout=5000)
    row = page.locator("[data-testid=conn-row]").first.inner_text()
    check("the connection is listed as Kafka with its servers", "Kafka" in row and "rpt:9092" in row, row)
    page.keyboard.press("Escape"); time.sleep(0.5)
    check("a Kafka connection is not offered as an Auto-Loader source", "redpanda" not in page.evaluate("() => Alpine.$data(document.body).connectionsList.filter(x => x.type !== 'kafka').map(x => x.name).join(',')"))
    # --- Avro: a topic with Avro messages, previewed and loaded through the UI
    subprocess.run(["docker", "exec", "stui", "python", "-c", """
import io, json, struct, requests, fastavro
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
sch={'type':'record','name':'E','fields':[{'name':'id','type':'long'},{'name':'who','type':'string'},{'name':'at','type':{'type':'long','logicalType':'timestamp-millis'}}]}
sid=requests.post('http://rpt:8081/subjects/ui_avro-value/versions',json={'schema':json.dumps(sch)},headers={'Content-Type':'application/vnd.schemaregistry.v1+json'}).json()['id']
a=AdminClient({'bootstrap.servers':'rpt:9092'}); [f.result() for f in a.create_topics([NewTopic('ui_avro',num_partitions=1,replication_factor=1)]).values()]
p=Producer({'bootstrap.servers':'rpt:9092'})
import datetime
for i in range(8):
    b=io.BytesIO(); b.write(b'\\x00'+struct.pack('>I',sid)); fastavro.schemaless_writer(b,fastavro.parse_schema(sch),{'id':i,'who':'w%d'%i,'at':datetime.datetime(2026,1,1,tzinfo=datetime.timezone.utc)}); p.produce('ui_avro',value=b.getvalue())
p.flush(20)
"""], check=True, capture_output=True)
    page.locator("[data-testid=open-streams]").click(); page.locator("[data-testid=stream-new]").click()
    page.locator("[data-testid=stream-name]").fill("UI avro"); page.locator("[data-testid=stream-topic]").fill("ui_avro"); page.locator("[data-testid=stream-table]").fill("ui_avro_raw")
    check("the format list offers the three registry formats", set(page.locator("[data-testid=stream-format] option").evaluate_all("els => els.map(e => e.value)")) >= {"avro", "protobuf", "jsonschema"})
    page.locator("[data-testid=stream-format]").select_option("avro")
    page.locator("[data-testid=stream-preview]").click(); page.locator("[data-testid=stream-preview-result]").wait_for(timeout=25000)
    txt = page.locator("[data-testid=stream-preview-result]").inner_text()
    check("Avro preview shows the schema's columns and types", "id : int64" in txt and "who : string" in txt and "at : timestamp[us, tz=UTC]" in txt, txt[:300])
    page.locator("[data-testid=stream-save]").click(); page.locator("[data-testid=stream-row]").first.wait_for(timeout=10000)
    end = time.time() + 40
    while time.time() < end and page.evaluate("async () => { await Alpine.$data(document.body).loadStreams(); return (Alpine.$data(document.body).streamsList.find(x => x.name === 'UI avro') || {}).rows_total; }") != 8: time.sleep(1)
    check("the Avro stream loads its 8 messages", page.evaluate("() => Alpine.$data(document.body).streamsList.find(x => x.name === 'UI avro').rows_total") == 8)
    page.once("dialog", lambda d: d.accept()); page.locator("[data-testid=stream-row]:has-text('UI avro') [data-testid=stream-delete]").click(); time.sleep(3)
    page.keyboard.press("Escape"); time.sleep(0.5)
    # --- a stream
    page.locator("[data-testid=open-streams]").click(); page.locator("[data-testid=stream-new]").click()
    page.locator("[data-testid=stream-name]").fill("UI events")
    page.locator("[data-testid=stream-topic]").fill("ui_events"); page.locator("[data-testid=stream-table]").fill("ui_events_raw")
    check("the topic dropdown lists the broker's topics", "ui_events" in page.evaluate("() => Alpine.$data(document.body).streamTopics.map(t => t.name).join(',')"))
    page.locator("[data-testid=stream-preview]").click(); page.locator("[data-testid=stream-preview-result]").wait_for(timeout=20000)
    txt = page.locator("[data-testid=stream-preview-result]").inner_text()
    check("preview shows the columns the table would get", "user : string" in txt and "n : int64" in txt and "ok : bool" in txt and "_offset" in txt, txt[:300])
    page.locator("[data-testid=stream-save]").click()
    page.locator("[data-testid=stream-row]").first.wait_for(timeout=10000)
    def state():
        return page.evaluate("async () => { await Alpine.$data(document.body).loadStreams(); return Alpine.$data(document.body).streamsList[0]; }")
    end = time.time() + 40
    while time.time() < end and state()["rows_total"] < 20: time.sleep(1)
    st = state(); check("the stream runs and loads the 20 messages", st["status"] == "running" and st["rows_total"] == 20, st)
    produce("ui_events", [{"user": "late", "n": 99, "ok": True}, "not json"])
    end = time.time() + 30
    while time.time() < end and (state()["rows_total"] < 21 or state()["bad_total"] < 1): time.sleep(1)
    st = state(); check("new messages arrive and a bad one is counted as dead-lettered", st["rows_total"] == 21 and st["bad_total"] == 1, st)
    check("the row shows status, rows and lag in the table", "running" in page.locator("[data-testid=stream-row]").first.inner_text() and "21" in page.locator("[data-testid=stream-row]").first.inner_text())
    dq = json.loads(subprocess.run(["docker", "exec", "stui", "python", "-c", "import json;from deltalake import DeltaTable;t=DeltaTable('/workspace/warehouse/dbo/ui_events_raw_dlq').to_pyarrow_table();print(json.dumps(t['error'].to_pylist()))"], capture_output=True).stdout.decode().strip().splitlines()[-1])
    check("the dead-letter table holds it", len(dq) == 1 and "JSON" in dq[0], dq)
    page.locator("[data-testid=stream-stop]").click(); time.sleep(6)
    check("Stop shows stopped", state()["status"] == "stopped" and page.locator("[data-testid=stream-start]").is_visible())
    page.locator("[data-testid=stream-edit]").click()
    check("connection, topic and table cannot be edited", page.locator("[data-testid=stream-topic]").is_disabled() and page.locator("[data-testid=stream-table]").is_disabled())
    page.locator("[data-testid=stream-save]").click(); time.sleep(6)
    check("saving re-enables it and it runs again", state()["enabled"] and state()["status"] in ("running", "starting"))
    # --- Rewind and Move from the UI (stream is stopped at this point? stop it first)
    page.locator("[data-testid=stream-stop]").click(); time.sleep(5)
    check("Rewind and Move are enabled for a stopped stream", page.locator("[data-testid=stream-rewind]").is_enabled() and page.locator("[data-testid=stream-move]").is_enabled())
    page.locator("[data-testid=stream-rewind]").click(); page.locator("[data-testid=op-start-mode]").select_option("earliest")
    check("Apply is disabled until the plan was shown", page.locator("[data-testid=op-apply]").is_disabled())
    page.locator("[data-testid=op-preview]").click(); page.locator("[data-testid=op-plan]").wait_for(timeout=20000)
    plan = page.locator("[data-testid=op-plan]").inner_text()
    check("the plan lists partitions and the rows that would be deleted", "Now at" in plan and page.locator("[data-testid=op-apply]").inner_text().startswith("Rewind and delete 21 rows"), (plan[:200], page.locator("[data-testid=op-apply]").inner_text()))
    page.locator("[data-testid=op-apply]").click(); time.sleep(3)
    rows = page.evaluate("async () => { await Alpine.$data(document.body).loadStreams(); return Alpine.$data(document.body).streamsList[0].pending; }")
    check("the rewind is applied (nothing pending)", rows is False)
    page.locator("[data-testid=stream-start]").click()
    end = time.time() + 40
    while time.time() < end and page.evaluate("async () => { await Alpine.$data(document.body).loadStreams(); return Alpine.$data(document.body).streamsList[0].rows_total; }") < 41: time.sleep(1)
    n_rows = json.loads(subprocess.run(["docker", "exec", "stui", "python", "-c", "import json;from deltalake import DeltaTable;t=DeltaTable('/workspace/warehouse/dbo/ui_events_raw').to_pyarrow_table();print(json.dumps([t.num_rows, len(set(zip(t['_partition'].to_pylist(), t['_offset'].to_pylist())))]))"], capture_output=True).stdout.decode().strip().splitlines()[-1])
    check("after the rewind the table holds every message once", n_rows == [21, 21], n_rows)
    page.locator("[data-testid=stream-stop]").click(); time.sleep(5)
    page.locator("[data-testid=stream-move]").click()
    page.locator("[data-testid=op-table]").fill("ui_events_moved"); page.locator("[data-testid=op-preview]").click(); page.locator("[data-testid=op-plan]").wait_for(timeout=20000)
    check("a Move plan shows the change and that the position is kept", "target_table = ui_events_moved" in page.locator("[data-testid=op-plan]").inner_text() and "position kept" in page.locator("[data-testid=op-plan]").inner_text(), page.locator("[data-testid=op-plan]").inner_text()[:200])
    page.locator("[data-testid=op-apply]").click(); time.sleep(3)
    check("the stream now writes to the new table", page.evaluate("() => Alpine.$data(document.body).streamsList[0].target_table") == "ui_events_moved")
    page.locator("[data-testid=stream-move]").click(); page.locator("[data-testid=op-topic]").fill("does_not_exist"); page.locator("[data-testid=op-start-mode]").select_option("earliest"); page.locator("[data-testid=op-preview]").click(); time.sleep(3)
    check("a topic that does not exist is refused with the reason", "does not exist" in page.locator("[data-testid=op-error]").inner_text())
    page.locator("[data-testid=stream-op] button:has-text('Close')").click()
    page.locator("[data-testid=stream-start]").click(); time.sleep(2)
    # duplicate target refused in the UI
    page.locator("[data-testid=stream-new]").click(); page.locator("[data-testid=stream-name]").fill("dup"); page.locator("[data-testid=stream-topic]").fill("ui_events"); page.locator("[data-testid=stream-table]").fill("ui_events_moved")
    page.locator("[data-testid=stream-save]").click(); time.sleep(1)
    check("a second stream into the same table is refused", "already loads" in page.locator("[data-testid=stream-form-error]").inner_text())
    page.locator("[data-testid=stream-form] button:has-text('Cancel')").click()
    page.once("dialog", lambda d: d.accept()); page.locator("[data-testid=stream-delete]").click(); time.sleep(3)
    page.evaluate("async () => { await Alpine.$data(document.body).loadStreams(); }")
    check("Delete removes the stream", page.evaluate("() => Alpine.$data(document.body).streamsList.length") == 0)
    page.screenshot(path="/tmp/streams_ui.png")
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
