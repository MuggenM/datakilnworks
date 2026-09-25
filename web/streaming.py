"""Streaming ingestion from Kafka-compatible brokers (Apache Kafka, Redpanda, Confluent, MSK...) into Delta tables.

A *stream* reads one topic through a `kafka` connection (`web/connections.py`: bootstrap servers, TLS/SASL, the secret stored encrypted)
and appends micro-batches to a Delta table. One runner thread per enabled stream is managed by `sync_runners()`, which the daemon loop
(`streaming_daemon_loop`, started in `startup_event`) calls every few seconds.

Exactly-once, without a second store to keep in step: every Delta commit carries one *application transaction* per partition
(`dkw-stream:<stream id>:<topic>:<partition>` -> next offset to read; Delta's own idempotent-writer mechanism), so the table itself says
how far it has been loaded. A restart resumes from `max(that, our SQLite bookkeeping)`. A crash between the commit and the bookkeeping
resumes from the commit; a crash before the commit re-reads the batch and loads it once. Messages that cannot be parsed go to
`<table>_dlq` (dead letter table), which is made idempotent the same way. Consumer-group offsets are also committed to Kafka after each
batch, best effort, only so external lag tools work; they are never used to resume.

Message handling
  format `json`   the value must be a JSON object. Its top-level fields become columns (types inferred on the first batch: bigint,
                  double, boolean, string; objects and arrays are stored as JSON text). Later batches are cast to the table's schema;
                  a value that does not fit, or a field the table does not have, is kept in `_rescued_data` (JSON text) instead of failing
                  the batch, unless `evolve_schema` is on, which adds the new fields as columns.
  format `text`   one column `value` with the message as text.
  always          `_key`, `_topic`, `_partition`, `_offset`, `_timestamp` and `_rescued_data`. A null value (tombstone) is skipped.
Not implemented: Avro / Protobuf with a Schema Registry (such a message is dead-lettered with that explanation), rewinding a stream.
"""
import datetime
import json
import logging
import os
import re
import socket
import sqlite3
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

logger = logging.getLogger("localspark.streaming")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
NAME_RE = re.compile(r"^.{1,80}$")
TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
FORMATS = ("json", "text", "avro", "protobuf", "jsonschema")
START = ("earliest", "latest")
MAX_COLUMNS = 500
LEASE_SECONDS = 45
KEEP_BATCHES = 200

META_FIELDS = [("_key", pa.string()), ("_topic", pa.string()), ("_partition", pa.int32()), ("_offset", pa.int64()),
               ("_timestamp", pa.timestamp("us", tz="UTC"))]
RESCUED = "_rescued_data"
RESERVED = {n for n, _ in META_FIELDS} | {RESCUED}
DLQ_SCHEMA = pa.schema([("topic", pa.string()), ("partition", pa.int32()), ("offset", pa.int64()), ("timestamp", pa.timestamp("us", tz="UTC")),
                        ("key", pa.string()), ("value", pa.string()), ("error", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
                        ("value_b64", pa.string())])          # the exact bytes, for messages that are not text (Avro): re-decode or replay them later


class StreamError(ValueError):
    """Invalid stream definition or unknown stream (the message is safe to show)."""


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _db_path() -> str:
    return os.path.join(os.getenv("WAREHOUSE_DIR", WAREHOUSE_DIR), ".metadata", "streaming.db")


def _db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(_db_path()), exist_ok=True)
    c = sqlite3.connect(_db_path(), timeout=15)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS streams (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, connection TEXT NOT NULL, topic TEXT NOT NULL, format TEXT NOT NULL,
            starting_offsets TEXT NOT NULL, target_catalog TEXT NOT NULL, target_schema TEXT NOT NULL, target_table TEXT NOT NULL,
            max_records INTEGER NOT NULL, max_wait_seconds INTEGER NOT NULL, evolve_schema INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1, created_by TEXT, created_at TEXT, updated_at TEXT);
        CREATE TABLE IF NOT EXISTS stream_offsets (stream_id TEXT NOT NULL, partition INTEGER NOT NULL, next_offset INTEGER NOT NULL,
            PRIMARY KEY (stream_id, partition));
        CREATE TABLE IF NOT EXISTS stream_state (stream_id TEXT PRIMARY KEY, status TEXT, last_error TEXT, last_batch_at TEXT,
            rows_total INTEGER DEFAULT 0, bad_total INTEGER DEFAULT 0, batches_total INTEGER DEFAULT 0, partitions_json TEXT,
            lease_owner TEXT, heartbeat REAL, started_at TEXT);
        CREATE TABLE IF NOT EXISTS stream_batches (id INTEGER PRIMARY KEY AUTOINCREMENT, stream_id TEXT NOT NULL, at TEXT, rows INTEGER,
            bad INTEGER, skipped INTEGER, offsets_json TEXT, seconds REAL);
    """)
    return c


# ---------------------------------------------------------------- definitions

def _row(r: sqlite3.Row) -> Dict[str, Any]:
    d = dict(r)
    d["enabled"] = bool(d["enabled"])
    d["evolve_schema"] = bool(d["evolve_schema"])
    return d


def _clean(data: Dict[str, Any], current: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from web import autoloader, connections
    cur = current or {}
    g = lambda k, dflt=None: data.get(k, cur.get(k, dflt))
    name = str(g("name") or "").strip()
    if not NAME_RE.match(name):
        raise StreamError("A name of 1-80 characters is required.")
    conn_name = str(g("connection") or "").strip()
    conn = connections.get_connection(conn_name)
    if not conn or conn["type"] != "kafka":
        raise StreamError("Choose a Kafka connection (create it with the Connections button).")
    topic = str(g("topic") or "").strip()
    if not TOPIC_RE.match(topic):
        raise StreamError("The topic name may contain letters, digits, '.', '_' and '-' (max. 249 characters).")
    fmt = g("format", "json")
    if fmt not in FORMATS:
        raise StreamError(f"The format must be one of {', '.join(FORMATS)}.")
    if fmt in REGISTRY_FORMATS and not (conn["config"] or {}).get("schema_registry_url"):
        raise StreamError("This format needs a Schema Registry: add its URL to the Kafka connection.")
    start = g("starting_offsets", "earliest")
    if start not in START:
        raise StreamError("Starting offsets must be 'earliest' or 'latest'.")
    cat, sch, tbl = (str(g("target_catalog", "warehouse") or "warehouse").strip().lower(), str(g("target_schema", "dbo") or "dbo").strip().lower(),
                     str(g("target_table") or "").strip().lower())
    if not (IDENT_RE.match(sch) and IDENT_RE.match(tbl)):
        raise StreamError("Schema and table names may contain letters, digits and '_' (not starting with a digit).")
    try:
        autoloader.resolve_target(cat, sch, tbl)
    except autoloader.TargetError as exc:
        raise StreamError(str(exc))
    try:
        max_records = max(1, min(int(g("max_records", 5000)), 100000))
        max_wait = max(1, min(int(g("max_wait_seconds", 5)), 300))
    except (TypeError, ValueError):
        raise StreamError("Batch size and wait time must be numbers.")
    return {"name": name, "description": str(g("description", "") or "")[:300], "connection": conn["name"], "topic": topic, "format": fmt,
            "starting_offsets": start, "target_catalog": cat, "target_schema": sch, "target_table": tbl, "max_records": max_records,
            "max_wait_seconds": max_wait, "evolve_schema": 1 if g("evolve_schema", False) else 0, "enabled": 1 if g("enabled", True) else 0}


def _audit(actor: str, action: str, name: str, detail: Dict[str, Any]) -> None:
    try:
        from web.governance import store
        store.init_governance_db()
        c = store.get_db()
        try:
            store.write_audit(c, actor, action, f"stream:{name}", detail)
            c.commit()
        finally:
            c.close()
    except Exception as exc:
        logger.warning(f"could not audit {action}: {exc}")


def create_stream(data: Dict[str, Any], actor: str) -> Dict[str, Any]:
    v = _clean(data)
    c = _db()
    try:
        clash = c.execute("SELECT name FROM streams WHERE target_catalog=? AND target_schema=? AND target_table=?",
                          (v["target_catalog"], v["target_schema"], v["target_table"])).fetchone()
        if clash:
            raise StreamError(f"The stream '{clash['name']}' already loads that table.")
        sid = f"stm_{uuid.uuid4().hex[:8]}"
        c.execute("INSERT INTO streams VALUES (:id,:name,:description,:connection,:topic,:format,:starting_offsets,:target_catalog,:target_schema,"
                  ":target_table,:max_records,:max_wait_seconds,:evolve_schema,:enabled,:by,:at,:at)", {**v, "id": sid, "by": actor, "at": _now()})
        c.commit()
    finally:
        c.close()
    _audit(actor, "STREAM_CREATE", v["name"], {"topic": v["topic"], "target": f"{v['target_catalog']}.{v['target_schema']}.{v['target_table']}"})
    _sync_lineage(get_stream(sid))
    return get_stream(sid)


def update_stream(sid: str, data: Dict[str, Any], actor: str) -> Dict[str, Any]:
    cur = get_stream(sid)
    if not cur:
        raise LookupError("Stream not found.")
    # The topic and target decide what the stored offsets mean; changing them would resume from offsets of another topic.
    for locked in ("connection", "topic", "target_catalog", "target_schema", "target_table"):
        if locked in data and str(data[locked]).strip().lower() != str(cur[locked]).strip().lower():
            raise StreamError("The connection, topic and target of a stream cannot be changed (its stored offsets belong to them). Create a new stream instead.")
    v = _clean(data, cur)
    c = _db()
    try:
        c.execute("UPDATE streams SET name=:name, description=:description, format=:format, starting_offsets=:starting_offsets, max_records=:max_records,"
                  " max_wait_seconds=:max_wait_seconds, evolve_schema=:evolve_schema, enabled=:enabled, updated_at=:at WHERE id=:id",
                  {**v, "id": sid, "at": _now()})
        c.commit()
    finally:
        c.close()
    _audit(actor, "STREAM_UPDATE", v["name"], {})
    return get_stream(sid)


def set_enabled(sid: str, enabled: bool, actor: str) -> Dict[str, Any]:
    cur = get_stream(sid)
    if not cur:
        raise LookupError("Stream not found.")
    c = _db()
    try:
        c.execute("UPDATE streams SET enabled=?, updated_at=? WHERE id=?", (1 if enabled else 0, _now(), sid))
        c.commit()
    finally:
        c.close()
    _audit(actor, "STREAM_START" if enabled else "STREAM_STOP", cur["name"], {})
    return get_stream(sid)


def delete_stream(sid: str, actor: str) -> None:
    cur = get_stream(sid)
    if not cur:
        raise LookupError("Stream not found.")
    stop_runner(sid)
    c = _db()
    try:
        for t in ("streams", "stream_state", "stream_offsets", "stream_batches"):
            c.execute(f"DELETE FROM {t} WHERE {'id' if t == 'streams' else 'stream_id'} = ?", (sid,))
        c.commit()
    finally:
        c.close()
    _audit(actor, "STREAM_DELETE", cur["name"], {})                    # the Delta table and its data stay
    try:
        from web.lineage import delete_node
        delete_node(_topic_node_id(cur))
    except Exception:
        pass


def list_streams() -> List[Dict[str, Any]]:
    c = _db()
    try:
        return [_with_state(c, _row(r)) for r in c.execute("SELECT * FROM streams ORDER BY name")]
    finally:
        c.close()


def get_stream(sid: str, detail: bool = False) -> Optional[Dict[str, Any]]:
    c = _db()
    try:
        r = c.execute("SELECT * FROM streams WHERE id = ?", (sid,)).fetchone()
        if not r:
            return None
        out = _with_state(c, _row(r))
        if detail:
            out["batches"] = [dict(b) for b in c.execute("SELECT at, rows, bad, skipped, seconds, offsets_json FROM stream_batches WHERE stream_id=? ORDER BY id DESC LIMIT 30", (sid,))]
        return out
    finally:
        c.close()


def _with_state(c: sqlite3.Connection, d: Dict[str, Any]) -> Dict[str, Any]:
    s = c.execute("SELECT * FROM stream_state WHERE stream_id = ?", (d["id"],)).fetchone()
    s = dict(s) if s else {}
    alive = bool(s.get("heartbeat")) and time.time() - float(s["heartbeat"]) < LEASE_SECONDS
    parts = json.loads(s.get("partitions_json") or "[]")
    status = "stopped" if not d["enabled"] else ((s.get("status") or "starting") if alive else "starting")
    d.update(status=status,
             last_error=s.get("last_error"), last_batch_at=s.get("last_batch_at"), rows_total=s.get("rows_total") or 0,
             bad_total=s.get("bad_total") or 0, batches_total=s.get("batches_total") or 0, partitions=parts,
             lag_total=sum(max(0, int(p.get("lag") or 0)) for p in parts))
    return d


def _topic_node_id(s: Dict[str, Any]) -> str:
    return f"kafka:{s['connection']}/{s['topic']}"


def _sync_lineage(s: Optional[Dict[str, Any]]) -> None:
    if not s:
        return
    try:
        from web.lineage import upsert_node, upsert_edge, make_table_id
        tid = make_table_id(s["target_catalog"], s["target_schema"], s["target_table"])
        upsert_node(_topic_node_id(s), s["topic"], "VOLUME", layer="RAW_FILE", catalog="kafka", schema_name=s["connection"],
                    metadata={"stream_id": s["id"], "connection": s["connection"]})
        upsert_node(tid, s["target_table"], "TABLE", catalog=s["target_catalog"], schema_name=s["target_schema"])
        upsert_edge(_topic_node_id(s), tid, edge_type="STREAMED_TO", job_id=s["id"])
    except Exception as exc:
        logger.debug(f"Lineage update notice: {exc}")


# ---------------------------------------------------------------- kafka client

def _kafka():
    try:
        import confluent_kafka
        import confluent_kafka.admin                     # not imported by the package itself
        return confluent_kafka
    except ImportError:
        raise StreamError("The confluent-kafka package is not installed in this image (rebuild it: docker compose build).")


def kafka_conf(conn: Dict[str, Any]) -> Dict[str, Any]:
    """librdkafka settings for a stored `kafka` connection (public view plus its `secret`)."""
    cfg, secret = conn["config"], conn.get("secret") or {}
    conf: Dict[str, Any] = {"bootstrap.servers": cfg["bootstrap_servers"], "security.protocol": cfg.get("security_protocol", "PLAINTEXT"),
                            "client.id": "datakilnworks", "socket.timeout.ms": 15000}
    if cfg.get("security_protocol", "PLAINTEXT").startswith("SASL"):
        conf.update({"sasl.mechanism": cfg["sasl_mechanism"], "sasl.username": cfg["username"], "sasl.password": secret.get("password", "")})
    if cfg.get("ssl_ca_pem") and cfg.get("security_protocol", "PLAINTEXT").endswith("SSL"):
        conf["ssl.ca.pem"] = cfg["ssl_ca_pem"]
    return conf


def _connection(name: str) -> Dict[str, Any]:
    from web import connections
    c = connections.get_with_secret(name)
    if not c or c["type"] != "kafka":
        raise StreamError(f"The Kafka connection '{name}' no longer exists.")
    return c


def _friendly(exc: Exception) -> str:
    m = str(exc)
    for needle, text in (("SASL authentication", "The broker rejected the login (user name, password or mechanism)."),
                         ("Authentication failed", "The broker rejected the login (user name, password or mechanism)."),
                         ("SSL", "TLS failed: check the security protocol and the CA certificate."),
                         ("Failed to resolve", "The broker's host name could not be resolved."),
                         ("Connection refused", "The broker refused the connection."),
                         ("Broker transport failure", "The broker is not reachable."),
                         ("All broker connections are down", "The broker is not reachable."),
                         ("Timed out", "The broker did not answer in time."),
                         ("UNKNOWN_TOPIC_OR_PART", "The topic does not exist."),
                         ("TOPIC_AUTHORIZATION_FAILED", "The login is not allowed to read this topic.")):
        if needle.lower() in m.lower():
            return text
    return m[:300]


def _metadata(conf: Dict[str, Any]):
    """Cluster metadata. librdkafka reports *why* a login or TLS handshake failed only through its error callback (the metadata call itself
    just times out), so a failure is followed by a short poll to collect those reports for the message."""
    ck = _kafka()
    seen: List[str] = []
    c = ck.Consumer({**conf, "group.id": f"dkw-probe-{uuid.uuid4().hex[:8]}", "error_cb": lambda e: seen.append(str(e))})
    try:
        return _list_topics_explained(c, seen, None)
    finally:
        c.close()


def _list_topics_explained(consumer, seen: List[str], topic: Optional[str]):
    try:
        return consumer.list_topics(topic, timeout=15) if topic else consumer.list_topics(timeout=10)
    except Exception as exc:
        end = time.time() + 3
        while time.time() < end and not any("uthentication" in x or "SSL" in x for x in seen):
            consumer.poll(0.3)
        raise StreamError(_friendly(Exception(" ".join(list(dict.fromkeys(seen))[:8] + [str(exc)]))))


def test_connection(definition: Dict[str, Any]) -> Dict[str, Any]:
    """Tries an unsaved/saved kafka definition: a metadata request (needs TLS and the login to succeed)."""
    try:
        md = _metadata(kafka_conf({"config": definition["config"], "secret": definition["secret"]}))
        topics = [t for t in md.topics if not t.startswith("__")]
        msg = f"Connected to {len(md.brokers)} broker(s); {len(topics)} topic(s) visible."
        if definition["config"].get("schema_registry_url"):
            dec = AvroDecoder({"config": definition["config"], "secret": definition["secret"]})
            try:
                msg += f" Schema Registry reachable ({dec.ping()} subject(s))."
            except StreamError as exc:
                return {"ok": False, "message": msg + " " + str(exc)}
            finally:
                dec.close()
        return {"ok": True, "message": msg}
    except StreamError as exc:
        return {"ok": False, "message": str(exc)}
    except Exception as exc:
        return {"ok": False, "message": _friendly(exc)}


def list_topics(conn_name: str) -> List[Dict[str, Any]]:
    md = _metadata(kafka_conf(_connection(conn_name)))
    return sorted(({"name": n, "partitions": len(t.partitions)} for n, t in md.topics.items() if not n.startswith("__") and not t.error), key=lambda x: x["name"])


# ---------------------------------------------------------------- message -> table (pure, unit-testable)

def _clean_col(name: Any) -> Optional[str]:
    n = re.sub(r"[ ,;{}()\n\t=]", "_", str(name)).strip()
    if not n or len(n) > 128 or n in RESERVED or n.startswith("_"):
        return None
    return n


def _stringify(v: Any) -> Optional[str]:
    if v is None:
        return None
    return v if isinstance(v, str) else json.dumps(v, separators=(",", ":"), ensure_ascii=False, default=_json_default)


def _json_default(o: Any) -> Any:
    import base64
    import decimal
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    if isinstance(o, decimal.Decimal):
        return str(o)
    if isinstance(o, (bytes, bytearray)):
        return base64.b64encode(bytes(o)).decode("ascii")
    return str(o)


def infer_type(values: List[Any]) -> pa.DataType:
    present = [v for v in values if v is not None]
    if not present or any(isinstance(v, (dict, list)) for v in present):
        return pa.string()
    try:
        t = pa.array(present).type
    except Exception:
        return pa.string()
    return t if t in (pa.int64(), pa.float64(), pa.bool_(), pa.string()) else pa.string()


def _parse_ts(v: Any):
    if isinstance(v, str):
        return datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))
    raise ValueError("not a timestamp string")


def coerce(values: List[Any], typ: pa.DataType) -> Tuple[pa.Array, List[int]]:
    """Casts values to `typ`; a value that does not fit becomes null and its index is returned so the caller can rescue it."""
    if pa.types.is_string(typ) or pa.types.is_large_string(typ):
        return pa.array([_stringify(v) for v in values], type=typ), []
    try:
        return pa.array(values, type=typ), []
    except Exception:
        pass
    out, bad = [], []
    for i, v in enumerate(values):
        try:
            if v is not None and (pa.types.is_timestamp(typ) or pa.types.is_date(typ)):
                v = _parse_ts(v)
                if pa.types.is_date(typ):
                    v = v.date()
            out.append(pa.array([v], type=typ)[0].as_py())
        except Exception:
            out.append(None)
            bad.append(i)
    return pa.array(out, type=typ), bad


def build_batch(rows: List[Dict[str, Any]], schema: Optional[pa.Schema], evolve: bool, hints: Optional[Dict[str, pa.DataType]] = None) -> Tuple[pa.Table, int]:
    """rows: [{"payload": {...}, "key", "topic", "partition", "offset", "timestamp_ms"}]. Returns (arrow table in the target's shape, number
    of rows that have something in `_rescued_data`). `schema` is the existing table's schema (None: the first batch defines it)."""
    known: Dict[str, Optional[pa.DataType]] = {}
    order: List[str] = []
    if schema is not None:
        for f in schema:
            known[f.name] = f.type
            order.append(f.name)
    payload_cols: Dict[str, List[Any]] = {}
    n = len(rows)
    rescued: List[Dict[str, Any]] = [dict() for _ in range(n)]
    growable = schema is None or evolve
    for i, r in enumerate(rows):
        seen = set()
        for k, v in r["payload"].items():
            c = _clean_col(k)
            if c is None or c in seen:
                rescued[i][str(k)] = v
                continue
            if c not in known:
                if not growable or len(known) - len(RESERVED) >= MAX_COLUMNS:
                    rescued[i][str(k)] = v
                    continue
                known[c] = None                       # a new column: its type is inferred from this batch below
                order.append(c)
            seen.add(c)
            payload_cols.setdefault(c, [None] * n)[i] = v
    arrays: Dict[str, pa.Array] = {}
    for c in order:
        if c in RESERVED:
            continue
        vals = payload_cols.get(c, [None] * n)
        typ = known[c] if known[c] is not None else ((hints or {}).get(c) or infer_type(vals))
        arr, bad = coerce(vals, typ)
        for i in bad:
            src = next((k for k in rows[i]["payload"] if _clean_col(k) == c), c)
            rescued[i][src] = vals[i]
        arrays[c] = arr
    cols = {name: arrays[name] for name in order if name in arrays}
    cols["_key"] = pa.array([r.get("key") for r in rows], type=pa.string())
    cols["_topic"] = pa.array([r["topic"] for r in rows], type=pa.string())
    cols["_partition"] = pa.array([r["partition"] for r in rows], type=pa.int32())
    cols["_offset"] = pa.array([r["offset"] for r in rows], type=pa.int64())
    ts = [r.get("timestamp_ms") for r in rows]
    cols["_timestamp"] = pa.array(ts, type=pa.timestamp("ms", tz="UTC")).cast(pa.timestamp("us", tz="UTC"))
    cols[RESCUED] = pa.array([json.dumps(d, separators=(",", ":"), ensure_ascii=False, default=_json_default) if d else None for d in rescued], type=pa.string())
    # columns of an existing table that this batch does not have: nulls, so the table's shape is kept
    if schema is not None:
        for f in schema:
            if f.name not in cols:
                cols[f.name] = pa.nulls(n, type=f.type)
        ordered = [f.name for f in schema] + [k for k in cols if k not in {f.name for f in schema}]
        cols = {k: cols[k] for k in ordered}
    return pa.table(cols), sum(1 for d in rescued if d)


# ---------------------------------------------------------------- Avro with a Schema Registry

class RegistryUnavailable(StreamError):
    """The Schema Registry could not be asked (network, TLS, login, 5xx). Not a bad message: the batch is retried, nothing is dead-lettered."""


class UnknownSchema(ValueError):
    """The registry answered that a schema id does not exist, or it is not an Avro schema: a property of the message."""


def avro_type(schema: Any, named: Optional[Dict[str, Any]] = None) -> pa.DataType:
    """The Arrow type a column of this Avro (sub)schema gets. Records, arrays and maps become JSON text (as with JSON messages); a union with
    null is its other branch; any other union is text."""
    named = named if named is not None else {}
    if isinstance(schema, list):
        branches = [b for b in schema if b != "null"]
        return avro_type(branches[0], named) if len(branches) == 1 else pa.string()
    if isinstance(schema, str):
        if schema in named:
            return avro_type(named[schema], named)
        return {"boolean": pa.bool_(), "int": pa.int32(), "long": pa.int64(), "float": pa.float32(), "double": pa.float64(), "string": pa.string(),
                "bytes": pa.binary()}.get(schema, pa.string())
    if isinstance(schema, dict):
        t, logical = schema.get("type"), schema.get("logicalType")
        if isinstance(t, (dict, list)):
            return avro_type(t, named)
        if schema.get("name"):
            named[schema["name"]] = schema
        if logical in ("timestamp-millis", "timestamp-micros"):
            return pa.timestamp("us", tz="UTC")
        if logical in ("local-timestamp-millis", "local-timestamp-micros"):
            return pa.timestamp("us")
        if logical == "date":
            return pa.date32()
        if logical == "decimal" and t in ("bytes", "fixed"):
            prec, scale = int(schema.get("precision") or 0), int(schema.get("scale") or 0)
            return pa.decimal128(prec, scale) if 1 <= prec <= 38 and 0 <= scale <= prec else pa.string()
        if t in ("enum", "array", "map", "record", "error"):
            return pa.string()
        if t == "fixed":
            return pa.binary()
        return avro_type(t, named) if isinstance(t, str) else pa.string()
    return pa.string()


# Schema Registry formats: stream format -> registry schemaType
REGISTRY_FORMATS = {"avro": "AVRO", "protobuf": "PROTOBUF", "jsonschema": "JSON"}
FORMAT_LABEL = {"AVRO": "Avro", "PROTOBUF": "Protobuf", "JSON": "JSON Schema"}
MAX_PROTO_BYTES = 200_000


def jsonschema_type(schema: Any, depth: int = 0) -> pa.DataType:
    """Arrow type for a JSON Schema property: integer/number/boolean/string (date-time and date formats typed); objects and arrays become
    JSON text; ["null", X] / anyOf-with-null is X; anything else text."""
    if not isinstance(schema, dict) or depth > 4:
        return pa.string()
    for key in ("anyOf", "oneOf"):
        if isinstance(schema.get(key), list):
            branches = [b for b in schema[key] if not (isinstance(b, dict) and b.get("type") == "null")]
            return jsonschema_type(branches[0], depth + 1) if len(branches) == 1 else pa.string()
    t = schema.get("type")
    if isinstance(t, list):
        t = [x for x in t if x != "null"]
        t = t[0] if len(t) == 1 else None
    fmt = schema.get("format")
    if t == "integer":
        return pa.int64()
    if t == "number":
        return pa.float64()
    if t == "boolean":
        return pa.bool_()
    if t == "string":
        return pa.timestamp("us", tz="UTC") if fmt == "date-time" else (pa.date32() if fmt == "date" else pa.string())
    return pa.string()


_PROTO_SCALARS = {"TYPE_DOUBLE": pa.float64(), "TYPE_FLOAT": pa.float32(), "TYPE_INT64": pa.int64(), "TYPE_SINT64": pa.int64(), "TYPE_SFIXED64": pa.int64(),
                  "TYPE_INT32": pa.int32(), "TYPE_SINT32": pa.int32(), "TYPE_SFIXED32": pa.int32(), "TYPE_UINT32": pa.int64(), "TYPE_FIXED32": pa.int64(),
                  "TYPE_UINT64": pa.decimal128(20, 0), "TYPE_FIXED64": pa.decimal128(20, 0), "TYPE_BOOL": pa.bool_(), "TYPE_STRING": pa.string(),
                  "TYPE_BYTES": pa.binary(), "TYPE_ENUM": pa.string()}


def _field_type_name(fd) -> str:
    from google.protobuf.descriptor import FieldDescriptor as F
    for name in dir(F):
        if name.startswith("TYPE_") and getattr(F, name) == fd.type:
            return name
    return ""


def proto_type(fd) -> pa.DataType:
    """Arrow type of a protobuf field: scalars typed (uint32 as bigint, uint64 as decimal(20,0), enums as their names), Timestamp as a UTC
    timestamp; repeated fields, maps and messages become JSON text."""
    from google.protobuf.descriptor import FieldDescriptor as F
    if fd.label == F.LABEL_REPEATED:
        return pa.string()
    if fd.type == F.TYPE_MESSAGE:
        return pa.timestamp("us", tz="UTC") if fd.message_type.full_name == "google.protobuf.Timestamp" else pa.string()
    return _PROTO_SCALARS.get(_field_type_name(fd), pa.string())


def proto_value(fd, v: Any) -> Any:
    """A python value for a protobuf field value: messages as dicts (proto field names), enums as names, Timestamp as datetime."""
    from google.protobuf.descriptor import FieldDescriptor as F
    if fd.type == F.TYPE_MESSAGE:
        if fd.message_type.GetOptions().map_entry:
            vf = fd.message_type.fields_by_name["value"]
            return {str(k): proto_value(vf, x) for k, x in v.items()}
        if fd.message_type.full_name == "google.protobuf.Timestamp":
            return datetime.datetime.fromtimestamp(v.seconds + v.nanos / 1e9, tz=datetime.timezone.utc)
        return proto_message(v)
    if fd.type == F.TYPE_ENUM:
        ev = fd.enum_type.values_by_number.get(v)
        return ev.name if ev else int(v)
    return v


def proto_message(msg) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for fd in msg.DESCRIPTOR.fields:
        from google.protobuf.descriptor import FieldDescriptor as F
        val = getattr(msg, fd.name)
        if fd.label == F.LABEL_REPEATED:
            if fd.message_type is not None and fd.message_type.GetOptions().map_entry:
                out[fd.name] = proto_value(fd, val)
            else:
                out[fd.name] = [proto_value(fd, x) for x in val]
        elif fd.type == F.TYPE_MESSAGE:
            out[fd.name] = proto_value(fd, val) if msg.HasField(fd.name) else None
        elif fd.has_presence and not msg.HasField(fd.name):
            out[fd.name] = None
        else:
            out[fd.name] = proto_value(fd, val)
    return out


def _zigzag(buf: bytes, pos: int) -> Tuple[int, int]:
    shift = result = 0
    while True:
        if pos >= len(buf):
            raise ValueError("truncated message index")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            break
        shift += 7
        if shift > 35:
            raise ValueError("bad message index")
    return (result >> 1) ^ -(result & 1), pos


class RegistryDecoder:
    """Decodes Confluent wire-format messages (magic byte 0, 4-byte big-endian schema id, then Avro binary, Protobuf (with message indexes) or
    JSON text) with schemas fetched from a Schema Registry (cached per id; ids are immutable). `kind` is the schema type the stream expects
    (AVRO, PROTOBUF, JSON); another type is a message problem. `hints` collects the column types the schemas ask for, for `build_batch`."""

    def __init__(self, conn: Dict[str, Any], kind: str = "AVRO"):
        cfg, secret = conn["config"], conn.get("secret") or {}
        self.kind = kind
        self.url = (cfg.get("schema_registry_url") or "").rstrip("/")
        if not self.url:
            raise StreamError("Schema Registry formats need a Schema Registry: add its URL to the Kafka connection.")
        self.auth = (cfg["registry_username"], secret.get("registry_password", "")) if cfg.get("registry_username") else None
        self.ca_pem = cfg.get("ssl_ca_pem") if self.url.startswith("https://") else None
        self._ca_file: Optional[str] = None
        self.cache: Dict[int, Any] = {}
        self.hints: Dict[str, pa.DataType] = {}

    def _verify(self):
        if not self.ca_pem:
            return True
        if self._ca_file is None:
            import tempfile
            f = tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False)
            f.write(self.ca_pem)
            f.close()
            self._ca_file = f.name
        return self._ca_file

    def close(self):
        if self._ca_file:
            try:
                os.unlink(self._ca_file)
            except OSError:
                pass

    def _get(self, path: str):
        import requests
        try:
            # No redirects: a stored login must never be sent to another host.
            r = requests.get(self.url + path, auth=self.auth, timeout=10, verify=self._verify(), allow_redirects=False,
                             headers={"Accept": "application/vnd.schemaregistry.v1+json"})
        except requests.exceptions.SSLError:
            raise RegistryUnavailable("TLS to the Schema Registry failed: check the CA certificate.")
        except requests.exceptions.RequestException as exc:
            raise RegistryUnavailable(f"The Schema Registry is not reachable ({type(exc).__name__}).")
        if r.status_code in (401, 403):
            raise RegistryUnavailable("The Schema Registry refused the login.")
        if 300 <= r.status_code < 400:
            raise RegistryUnavailable("The Schema Registry redirected the request; the redirect was not followed.")
        return r

    def ping(self) -> int:
        r = self._get("/subjects")
        if r.status_code != 200:
            raise RegistryUnavailable(f"The Schema Registry answered HTTP {r.status_code}.")
        return len(r.json())

    # -- schemas
    def _schema(self, sid: int):
        if sid in self.cache:
            return self.cache[sid]
        r = self._get(f"/schemas/ids/{sid}")
        if r.status_code == 404:
            raise UnknownSchema(f"Schema id {sid} does not exist in the Schema Registry.")
        if r.status_code != 200:
            raise RegistryUnavailable(f"The Schema Registry answered HTTP {r.status_code}.")
        body = r.json()
        kind = (body.get("schemaType") or "AVRO").upper()
        if kind not in FORMAT_LABEL:
            raise UnknownSchema(f"Schema id {sid} is a {kind} schema; supported are Avro, Protobuf and JSON Schema.")
        if kind != self.kind:
            raise UnknownSchema(f"Schema id {sid} is a {FORMAT_LABEL[kind]} schema, but this stream reads {FORMAT_LABEL[self.kind]}: choose the {FORMAT_LABEL[kind]} format.")
        try:
            if kind == "AVRO":
                import fastavro
                raw = json.loads(body["schema"])
                entry = ("AVRO", fastavro.parse_schema(raw), raw)
            elif kind == "JSON":
                raw = json.loads(body["schema"])
                if not isinstance(raw, dict):
                    raise ValueError("not an object schema")
                entry = ("JSON", raw, raw)
            else:
                entry = ("PROTOBUF", self._compile_proto(sid, body), None)
        except (UnknownSchema, RegistryUnavailable, StreamError):
            raise
        except ImportError as exc:
            raise StreamError(f"A package for {FORMAT_LABEL[kind]} is not installed in this image (rebuild it: docker compose build): {exc.name}.")
        except Exception as exc:
            raise UnknownSchema(f"Schema id {sid} is not a valid {FORMAT_LABEL[kind]} schema ({str(exc)[:100]}).")
        self.cache[sid] = entry
        return entry

    def _fetch_references(self, refs: List[Dict[str, Any]], into: Dict[str, str], depth: int = 0) -> None:
        for ref in refs or []:
            name = str(ref.get("name") or "")
            if not name or name.startswith("/") or ".." in name.split("/") or "\\" in name or len(into) >= 50 or depth > 8:
                raise UnknownSchema(f"The schema imports '{name}', which is not an acceptable file name.")
            if name in into:
                continue
            r = self._get(f"/subjects/{ref['subject']}/versions/{ref['version']}")
            if r.status_code == 404:
                raise UnknownSchema(f"The imported schema '{name}' (subject {ref.get('subject')}) does not exist in the Schema Registry.")
            if r.status_code != 200:
                raise RegistryUnavailable(f"The Schema Registry answered HTTP {r.status_code}.")
            body = r.json()
            into[name] = body["schema"]
            self._fetch_references(body.get("references") or [], into, depth + 1)

    def _compile_proto(self, sid: int, body: Dict[str, Any]):
        """Compiles the registry's .proto (and the files it imports) with protoc into a descriptor pool of its own."""
        import tempfile
        from grpc_tools import protoc
        from google.protobuf import descriptor_pb2, descriptor_pool
        files: Dict[str, str] = {}
        self._fetch_references(body.get("references") or [], files)
        text = body["schema"]
        if len(text) > MAX_PROTO_BYTES or sum(len(v) for v in files.values()) > 5 * MAX_PROTO_BYTES:
            raise UnknownSchema("The schema is too large.")
        with tempfile.TemporaryDirectory(prefix="dkw-proto-") as d:
            main = "schema.proto"
            for name, content in {**files, main: text}.items():
                path = os.path.join(d, name)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content)
            out = os.path.join(d, "out.desc")
            wkt = os.path.join(os.path.dirname(protoc.__file__), "_proto")
            rc = protoc.main(["protoc", f"-I{d}", f"-I{wkt}", f"--descriptor_set_out={out}", "--include_imports", os.path.join(d, main)])
            if rc != 0:
                raise UnknownSchema("protoc could not compile the .proto schema")
            fds = descriptor_pb2.FileDescriptorSet()
            with open(out, "rb") as f:
                fds.ParseFromString(f.read())
        pool = descriptor_pool.DescriptorPool()
        for fdp in fds.file:
            pool.Add(fdp)
        return pool.FindFileByName(main)

    # -- messages
    def decode(self, value: bytes) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """(payload, None) or (None, why this message is bad). Raises RegistryUnavailable when the registry cannot be asked."""
        import struct
        label = FORMAT_LABEL[self.kind]
        if len(value) < 5 or value[0] != 0:
            return None, f"Not in the Schema Registry wire format ({label}): the message does not start with the magic byte 0 and a schema id."
        sid = struct.unpack(">I", value[1:5])[0]
        try:
            kind, schema, raw = self._schema(sid)
        except UnknownSchema as exc:
            return None, str(exc)
        body = value[5:]
        try:
            if kind == "AVRO":
                return self._decode_avro(sid, schema, raw, body)
            if kind == "JSON":
                return self._decode_json(sid, schema, body)
            return self._decode_proto(sid, schema, body)
        except Exception as exc:
            return None, f"The message does not match schema {sid} ({type(exc).__name__}: {str(exc)[:80]})."

    def _decode_avro(self, sid, parsed, raw, body):
        import io
        import fastavro
        obj = fastavro.schemaless_reader(io.BytesIO(body), parsed)
        if isinstance(obj, dict):
            fields = {f["name"]: f["type"] for f in (raw.get("fields") or [])} if isinstance(raw, dict) else {}
            named: Dict[str, Any] = {}
            for name, ftype in fields.items():
                self.hints[name] = avro_type(ftype, named)
            return obj, None
        self.hints["value"] = avro_type(raw)
        return {"value": obj}, None

    def _decode_json(self, sid, schema, body):
        obj = json.loads(body.decode("utf-8"))
        if not isinstance(obj, dict):
            return None, "The JSON value is not an object."
        for name, sub in (schema.get("properties") or {}).items():
            self.hints[name] = jsonschema_type(sub)
        return obj, None

    def _decode_proto(self, sid, file_desc, body):
        from google.protobuf import message_factory
        n, pos = _zigzag(body, 0)
        path = [0]                                       # a single 0 byte is the shorthand for [0]
        if n != 0:
            path = []
            for _ in range(n):
                idx, pos = _zigzag(body, pos)
                path.append(idx)
        desc = file_desc.message_types_by_name[list(file_desc.message_types_by_name)[path[0]]]
        for idx in path[1:]:
            desc = desc.nested_types[idx]
        msg = message_factory.GetMessageClass(desc)()
        msg.ParseFromString(body[pos:])
        payload = proto_message(msg)
        for fd in desc.fields:
            self.hints[fd.name] = proto_type(fd)
        return payload, None

    def decode_key(self, key: bytes) -> Optional[str]:
        """A registry-encoded key as JSON text; None when it is not (the caller falls back to plain text)."""
        if not key or key[0] != 0:
            return None
        try:
            obj, err = self.decode(key)
        except RegistryUnavailable:
            return None
        if err or obj is None:
            return None
        return _stringify(obj["value"] if list(obj) == ["value"] else obj)


AvroDecoder = RegistryDecoder            # the original name; the default kind is Avro


def decoder_for(conn: Dict[str, Any], fmt: str) -> Optional[RegistryDecoder]:
    return RegistryDecoder(conn, REGISTRY_FORMATS[fmt]) if fmt in REGISTRY_FORMATS else None


def decode_message(fmt: str, value: bytes, decoder: Optional[AvroDecoder] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """(payload, None) or (None, error text)."""
    if fmt == "text":
        return {"value": value.decode("utf-8", errors="replace")}, None
    if fmt in REGISTRY_FORMATS:
        return decoder.decode(value)
    if value[:1] == b"\x00" and len(value) > 5:
        return None, "Schema Registry framed message (Avro, Protobuf or JSON Schema): choose the matching Schema Registry format."
    try:
        obj = json.loads(value.decode("utf-8"))
    except UnicodeDecodeError:
        return None, "The value is not valid UTF-8 text."
    except ValueError as exc:
        return None, f"The value is not valid JSON ({str(exc)[:80]})."
    if not isinstance(obj, dict):
        return None, "The JSON value is not an object."
    return obj, None


# ---------------------------------------------------------------- runner

_runners: Dict[str, "_Runner"] = {}
_runners_lock = threading.Lock()
OWNER = f"{socket.gethostname()}:{os.getpid()}"


def _set_state(sid: str, **kw) -> None:
    c = _db()
    try:
        c.execute("INSERT OR IGNORE INTO stream_state (stream_id) VALUES (?)", (sid,))
        sets = ", ".join(f"{k} = ?" for k in kw)
        c.execute(f"UPDATE stream_state SET {sets} WHERE stream_id = ?", (*kw.values(), sid))
        c.commit()
    finally:
        c.close()


def _acquire_lease(sid: str) -> bool:
    """One consumer per stream even if several studio processes run against the same warehouse."""
    c = _db()
    try:
        c.execute("INSERT OR IGNORE INTO stream_state (stream_id) VALUES (?)", (sid,))
        cur = c.execute("UPDATE stream_state SET lease_owner = ?, heartbeat = ? WHERE stream_id = ? AND (lease_owner IS NULL OR lease_owner = ? OR heartbeat IS NULL OR heartbeat < ?)",
                        (OWNER, time.time(), sid, OWNER, time.time() - LEASE_SECONDS))
        c.commit()
        return cur.rowcount == 1
    finally:
        c.close()


def _heartbeat(sid: str) -> bool:
    c = _db()
    try:
        cur = c.execute("UPDATE stream_state SET heartbeat = ? WHERE stream_id = ? AND lease_owner = ?", (time.time(), sid, OWNER))
        c.commit()
        return cur.rowcount == 1
    finally:
        c.close()


def _txn_app(sid: str, topic: str, partition: int, dlq: bool = False) -> str:
    return f"dkw-stream:{sid}:{topic}:{partition}" + (":dlq" if dlq else "")


def _stored_offsets(sid: str) -> Dict[int, int]:
    c = _db()
    try:
        return {r["partition"]: r["next_offset"] for r in c.execute("SELECT partition, next_offset FROM stream_offsets WHERE stream_id = ?", (sid,))}
    finally:
        c.close()


def _store_offsets(sid: str, nxt: Dict[int, int]) -> None:
    c = _db()
    try:
        for p, o in nxt.items():
            c.execute("INSERT INTO stream_offsets VALUES (?,?,?) ON CONFLICT(stream_id, partition) DO UPDATE SET next_offset = MAX(next_offset, excluded.next_offset)", (sid, p, o))
        c.commit()
    finally:
        c.close()


def _key_text(key: Any, decoder: Optional["AvroDecoder"]) -> Optional[str]:
    if key is None:
        return None
    if not isinstance(key, (bytes, bytearray)):
        return str(key)
    if decoder is not None:
        try:
            decoded = decoder.decode_key(bytes(key))
        except Exception:
            decoded = None
        if decoded is not None:
            return decoded
    return key.decode("utf-8", errors="replace")


def _b64_if_binary(fmt: str, value: bytes) -> Optional[str]:
    """The exact bytes of a dead-lettered message when its text form would be lossy (Avro, or bytes that are not UTF-8)."""
    import base64
    if fmt not in REGISTRY_FORMATS:
        try:
            value.decode("utf-8")
            return None
        except UnicodeDecodeError:
            pass
    return base64.b64encode(value[:100000]).decode("ascii")


def _open_table(location: str, so: Optional[Dict[str, str]]):
    from deltalake import DeltaTable
    from web import autoloader
    if not autoloader._is_delta(location, so):
        return None
    return DeltaTable(location, storage_options=so)


def _arrow_schema(table) -> pa.Schema:
    sch = table.schema()
    for attr in ("to_pyarrow", "to_arrow"):
        f = getattr(sch, attr, None)
        if f:
            try:
                out = f()
                return out if isinstance(out, pa.Schema) else pa.schema(out)
            except Exception:
                continue
    raise StreamError("Could not read the target table's schema.")


class _Restart(Exception):
    """The runner has to start a new consumer session (configuration or partitions changed); not an error."""


class _Runner(threading.Thread):
    def __init__(self, sid: str):
        super().__init__(name=f"stream-{sid}", daemon=True)
        self.sid = sid
        self.stop = threading.Event()
        self.decoder: Optional[AvroDecoder] = None
        self.client_errors: List[str] = []             # what librdkafka reported (a rejected login shows up only here)

    # -- thread body
    def run(self):
        backoff = 1
        while not self.stop.is_set():
            s = get_stream(self.sid)
            if not s or not s["enabled"]:
                break
            if not _acquire_lease(self.sid):
                self.stop.wait(LEASE_SECONDS / 3)
                continue
            try:
                _set_state(self.sid, status="running", last_error=None, started_at=_now())
                self._session(s)
                backoff = 1
            except _Restart:
                continue
            except Exception as exc:
                msg = str(exc) if isinstance(exc, StreamError) else _friendly(Exception(" ".join(list(dict.fromkeys(self.client_errors))[:8] + [str(exc)])))
                self.client_errors.clear()
                logger.error(f"Stream {s['name']}: {exc}", exc_info=not isinstance(exc, StreamError))
                _set_state(self.sid, status="error", last_error=msg[:500], heartbeat=time.time())
                self.stop.wait(backoff)
                backoff = min(60, backoff * 2)
        try:
            c = _db()
            try:
                c.execute("UPDATE stream_state SET status = 'stopped', lease_owner = NULL WHERE stream_id = ? AND lease_owner = ?", (self.sid, OWNER))
                c.commit()
            finally:
                c.close()
        except Exception:
            pass
        with _runners_lock:
            if _runners.get(self.sid) is self:
                _runners.pop(self.sid, None)

    # -- one consumer session
    def _session(self, s: Dict[str, Any]):
        from web import autoloader
        ck = _kafka()
        conn = _connection(s["connection"])
        consumer = ck.Consumer({**kafka_conf(conn), "group.id": f"dkw-stream-{s['id']}", "enable.auto.commit": False,
                                "enable.auto.offset.store": False, "auto.offset.reset": "earliest", "session.timeout.ms": 30000,
                                "error_cb": lambda e: self.client_errors.append(str(e))})
        try:
            location, so = autoloader.resolve_target(s["target_catalog"], s["target_schema"], s["target_table"])
            if so is None:
                os.makedirs(os.path.dirname(location), exist_ok=True)
            self.decoder = decoder_for(conn, s["format"])
            topic = s["topic"]
            md = _list_topics_explained(consumer, self.client_errors, topic)
            t = md.topics.get(topic)
            if t is None or t.error is not None:
                raise StreamError(f"The topic '{topic}' does not exist or is not readable with this login.")
            parts = sorted(t.partitions)
            positions = self._start_positions(consumer, s, parts, location, so)
            consumer.assign([ck.TopicPartition(topic, p, positions[p]) for p in parts])
            logger.info(f"Stream {s['name']}: reading {topic} ({len(parts)} partition(s)) into {s['target_schema']}.{s['target_table']}")
            last_idle = 0.0
            last_meta = time.time()
            while not self.stop.is_set():
                fresh = get_stream(self.sid)
                if not fresh or not fresh["enabled"] or fresh.get("updated_at") != s.get("updated_at"):
                    raise _Restart() if fresh and fresh["enabled"] else StopIteration()
                if not _heartbeat(self.sid):
                    raise StreamError("Another studio process took over this stream.")
                batch, deadline = [], time.time() + s["max_wait_seconds"]
                while not self.stop.is_set() and len(batch) < s["max_records"] and time.time() < deadline:
                    got = consumer.consume(num_messages=min(s["max_records"] - len(batch), 5000), timeout=min(1.0, max(0.05, deadline - time.time())))
                    for m in got:
                        if m.error():
                            if m.error().code() == ck.KafkaError._PARTITION_EOF:
                                continue
                            raise ck.KafkaException(m.error())
                        batch.append(m)
                    _heartbeat(self.sid)
                if batch:
                    self._write_batch(consumer, s, batch, location, so)
                if time.time() - last_idle > 10:
                    self._report_lag(consumer, s, parts, positions if not batch else None)
                    last_idle = time.time()
                if time.time() - last_meta > 30:                              # a partition added later: read it from its beginning
                    last_meta = time.time()
                    now_parts = sorted(consumer.list_topics(topic, timeout=15).topics[topic].partitions)
                    if now_parts != parts:
                        raise _Restart()
        except StopIteration:
            return
        finally:
            try:
                consumer.close()
            except Exception:
                pass
            if self.decoder:
                self.decoder.close()

    def _start_positions(self, consumer, s, parts, location, so) -> Dict[int, int]:
        ck = _kafka()
        table = _open_table(location, so)
        stored = _stored_offsets(s["id"])
        pos: Dict[int, int] = {}
        expired = []
        for p in parts:
            low, high = consumer.get_watermark_offsets(ck.TopicPartition(s["topic"], p), timeout=15)
            known = stored.get(p)
            if table is not None:
                try:
                    v = table.transaction_version(_txn_app(s["id"], s["topic"], p))
                    if v is not None:
                        known = max(known if known is not None else 0, int(v))
                except Exception:
                    pass
            if known is None:
                known = low if (s["starting_offsets"] == "earliest" or stored) else high     # a partition added later is read from its start
            if known < low:
                expired.append(p)
                known = low
            pos[p] = known
        _store_offsets(s["id"], pos)
        if expired:
            _set_state(s["id"], last_error=f"Offsets of partition(s) {expired} were already deleted by the broker's retention; reading from the oldest available (some messages were lost).")
        return pos

    def _report_lag(self, consumer, s, parts, _positions):
        ck = _kafka()
        stored = _stored_offsets(s["id"])
        info = []
        for p in parts:
            try:
                low, high = consumer.get_watermark_offsets(ck.TopicPartition(s["topic"], p), timeout=10)
                info.append({"partition": p, "next_offset": stored.get(p, low), "end_offset": high, "lag": max(0, high - stored.get(p, low))})
            except Exception:
                pass
        _set_state(s["id"], partitions_json=json.dumps(info), heartbeat=time.time())

    def _write_batch(self, consumer, s, msgs, location, so):
        from deltalake import write_deltalake, CommitProperties, Transaction
        ck = _kafka()
        t0 = time.time()
        sid, topic = s["id"], s["topic"]
        table = _open_table(location, so)
        good, bad, skipped = [], [], 0
        next_off: Dict[int, int] = {}
        for m in msgs:
            p, o = m.partition(), m.offset()
            next_off[p] = max(next_off.get(p, 0), o + 1)
            v = m.value()
            key = _key_text(m.key(), self.decoder)
            tst = m.timestamp()
            ts = tst[1] if tst and tst[0] != ck.TIMESTAMP_NOT_AVAILABLE and tst[1] and tst[1] > 0 else None
            if v is None:
                skipped += 1
                continue
            payload, err = decode_message(s["format"], v, self.decoder)      # may raise RegistryUnavailable: nothing is written, the batch is retried
            base = {"key": key, "topic": topic, "partition": p, "offset": o, "timestamp_ms": ts}
            if err:
                bad.append({**base, "value": v.decode("utf-8", errors="replace")[:100000], "error": err, "value_b64": _b64_if_binary(s["format"], v)})
            else:
                good.append({**base, "payload": payload})
        # Dead letters first; on a replay after a crash, those already committed are recognised by their offsets.
        if bad:
            self._write_dlq(s, bad, location, so)
        if good:
            schema = _arrow_schema(table) if table is not None else None
            tbl, _rescued = build_batch(good, schema, bool(s["evolve_schema"]), self.decoder.hints if self.decoder else None)
            last_good: Dict[int, int] = {}
            for r in good:
                last_good[r["partition"]] = max(last_good.get(r["partition"], -1), r["offset"])
            txns = [Transaction(_txn_app(sid, topic, p), o + 1) for p, o in last_good.items()]
            write_deltalake(location if table is None else table, tbl, mode="append", schema_mode="merge", storage_options=so,
                            commit_properties=CommitProperties(app_transactions=txns, custom_metadata={"dkw_stream": s["name"], "rows": str(len(good))}))
        _store_offsets(sid, next_off)
        try:
            consumer.commit(offsets=[ck.TopicPartition(topic, p, o) for p, o in next_off.items()], asynchronous=False)
        except Exception as exc:
            logger.debug(f"Stream {s['name']}: consumer-group commit skipped ({exc})")   # only for external lag tools
        secs = round(time.time() - t0, 3)
        c = _db()
        try:
            c.execute("INSERT OR IGNORE INTO stream_state (stream_id) VALUES (?)", (sid,))
            c.execute("UPDATE stream_state SET status='running', last_error=NULL, last_batch_at=?, rows_total=rows_total+?, bad_total=bad_total+?, batches_total=batches_total+1, heartbeat=? WHERE stream_id=?",
                      (_now(), len(good), len(bad), time.time(), sid))
            c.execute("INSERT INTO stream_batches (stream_id, at, rows, bad, skipped, offsets_json, seconds) VALUES (?,?,?,?,?,?,?)",
                      (sid, _now(), len(good), len(bad), skipped, json.dumps(next_off), secs))
            c.execute("DELETE FROM stream_batches WHERE stream_id = ? AND id NOT IN (SELECT id FROM stream_batches WHERE stream_id = ? ORDER BY id DESC LIMIT ?)", (sid, sid, KEEP_BATCHES))
            c.commit()
        finally:
            c.close()
        _sync_lineage(s)

    def _write_dlq(self, s, bad, location, so):
        from deltalake import write_deltalake, CommitProperties, Transaction
        dlq_loc = location.rstrip("/") + "_dlq"
        table = _open_table(dlq_loc, so)
        fresh = []
        for b in bad:
            done = -1
            if table is not None:
                try:
                    v = table.transaction_version(_txn_app(s["id"], s["topic"], b["partition"], dlq=True))
                    done = int(v) - 1 if v is not None else -1
                except Exception:
                    done = -1
            if b["offset"] > done:
                fresh.append(b)
        if not fresh:
            return
        now = datetime.datetime.now(datetime.timezone.utc)
        tbl = pa.table({"topic": [b["topic"] for b in fresh], "partition": pa.array([b["partition"] for b in fresh], pa.int32()),
                        "offset": pa.array([b["offset"] for b in fresh], pa.int64()),
                        "timestamp": pa.array([b["timestamp_ms"] for b in fresh], pa.timestamp("ms", tz="UTC")).cast(pa.timestamp("us", tz="UTC")),
                        "key": [b["key"] for b in fresh], "value": [b["value"] for b in fresh], "error": [b["error"] for b in fresh],
                        "ingested_at": pa.array([now] * len(fresh), pa.timestamp("us", tz="UTC")),
                        "value_b64": [b.get("value_b64") for b in fresh]}, schema=DLQ_SCHEMA)
        top: Dict[int, int] = {}
        for b in fresh:
            top[b["partition"]] = max(top.get(b["partition"], -1), b["offset"])
        write_deltalake(dlq_loc if table is None else table, tbl, mode="append", schema_mode="merge", storage_options=so,
                        commit_properties=CommitProperties(app_transactions=[Transaction(_txn_app(s["id"], s["topic"], p, dlq=True), o + 1) for p, o in top.items()]))


def start_runner(sid: str) -> None:
    with _runners_lock:
        r = _runners.get(sid)
        if r is not None and r.is_alive():
            return
        r = _Runner(sid)
        _runners[sid] = r
    r.start()


def stop_runner(sid: str, wait: float = 10.0) -> None:
    with _runners_lock:
        r = _runners.get(sid)
    if r is not None:
        r.stop.set()
        r.join(timeout=wait)


def sync_runners() -> None:
    """Starts a runner for every enabled stream that has none and stops the runners of disabled or deleted streams."""
    wanted = {s["id"] for s in list_streams() if s["enabled"]}
    with _runners_lock:
        running = list(_runners.items())
    for sid, r in running:
        if sid not in wanted:
            r.stop.set()
    for sid in wanted:
        start_runner(sid)


def shutdown() -> None:
    with _runners_lock:
        rs = list(_runners.values())
    for r in rs:
        r.stop.set()


async def streaming_daemon_loop():
    import asyncio
    logger.info("Streaming ingestion daemon started.")
    while True:
        try:
            await asyncio.to_thread(sync_runners)
        except asyncio.CancelledError:
            shutdown()
            break
        except Exception as exc:
            logger.error(f"Streaming daemon: {exc}", exc_info=True)
        await asyncio.sleep(5)


# ---------------------------------------------------------------- preview

def preview(conn_name: str, topic: str, fmt: str = "json", limit: int = 10) -> Dict[str, Any]:
    """The newest messages of a topic as the table would see them: columns, rows, and how many messages would be dead-lettered. Reads only;
    no consumer group, no offsets, no table."""
    ck = _kafka()
    if not TOPIC_RE.match(topic or ""):
        raise StreamError("Enter the topic name.")
    if fmt not in FORMATS:
        raise StreamError("Unknown format.")
    limit = max(1, min(int(limit or 10), 50))
    conn = _connection(conn_name)
    if fmt in REGISTRY_FORMATS and not conn["config"].get("schema_registry_url"):
        raise StreamError("This format needs a Schema Registry: add its URL to the Kafka connection.")
    decoder = decoder_for(conn, fmt)
    consumer = ck.Consumer({**kafka_conf(conn), "group.id": f"dkw-preview-{uuid.uuid4().hex[:8]}", "enable.auto.commit": False})
    try:
        t = consumer.list_topics(topic, timeout=15).topics.get(topic)
        if t is None or t.error is not None:
            raise StreamError(f"The topic '{topic}' does not exist or is not readable with this login.")
        tps = []
        for p in sorted(t.partitions):
            low, high = consumer.get_watermark_offsets(ck.TopicPartition(topic, p), timeout=10)
            if high > low:
                tps.append(ck.TopicPartition(topic, p, max(low, high - limit)))
        if not tps:
            return {"columns": [], "rows": [], "bad": 0, "messages": 0, "note": "The topic has no messages yet."}
        consumer.assign(tps)
        msgs, end = [], time.time() + 10
        while time.time() < end and len(msgs) < limit * len(tps):
            got = consumer.consume(num_messages=limit * len(tps), timeout=1.0)
            msgs.extend(m for m in got if not m.error())
            if not got and msgs:
                break
    finally:
        consumer.close()
        if decoder:
            decoder.close()
    msgs.sort(key=lambda m: (m.timestamp()[1] if m.timestamp() else 0), reverse=True)
    msgs = msgs[:limit]
    good, bad = [], []
    for m in msgs:
        if m.value() is None:
            continue
        payload, err = decode_message(fmt, m.value(), decoder)
        base = {"key": _key_text(m.key(), decoder), "topic": topic, "partition": m.partition(),
                "offset": m.offset(), "timestamp_ms": (m.timestamp()[1] if m.timestamp() and m.timestamp()[1] > 0 else None)}
        (bad if err else good).append({**base, "payload": payload} if not err else {**base, "error": err})
    if not good:
        return {"columns": [], "rows": [], "bad": len(bad), "messages": len(msgs), "errors": [b["error"] for b in bad[:3]]}
    tbl, rescued = build_batch(good, None, False, decoder.hints if decoder else None)
    cols = [{"name": f.name, "type": str(f.type)} for f in tbl.schema]
    rows = [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()} for r in tbl.to_pylist()]
    return {"columns": cols, "rows": rows, "bad": len(bad), "messages": len(msgs), "errors": [b["error"] for b in bad[:3]]}
