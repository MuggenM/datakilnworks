"""Rewinding a stream and moving it to another connection, topic or table (`web/streaming.py` holds the runner).

Why this is not just an UPDATE: a stream's position lives in the *Delta table* (one application transaction per partition carrying the next
offset) and in `stream_offsets`, and the runner resumes from max(both). So a rewind has to lower the table's recorded offsets with a Delta commit
of its own; a plain `stream_offsets` update would be overridden by the table.

Operations (all need the stream to be stopped; all are audited; both offer a dry run that changes nothing)
  rewind   read again from `earliest`, `latest` (skip ahead), a timestamp or explicit per-partition offsets. Mode `replace` deletes the rows at or
           after the new position from the table (and the dead-letter table) in the SAME Delta commit that lowers the recorded offsets, so nothing is
           duplicated; mode `keep` leaves them, and the re-read messages are appended again (duplicates, by design).
  move     change the connection, the topic and/or the target table.
           - another connection to the SAME cluster (cluster ids equal, or the user confirms when the broker reports none): offsets are kept.
           - another topic, or another cluster: the offsets of the old topic mean nothing there, so a start position is required (earliest, latest,
             timestamp or offsets for every partition). The recorded offsets are overwritten even when the new topic has the same name as the old one
             (its application-transaction ids would otherwise collide).
           - another table: the new table (and its `_dlq`) get the current offsets recorded by a marker commit, so the stream continues there without
             re-reading or skipping; an existing table with stale offsets from an earlier life is corrected the same way.

Crash safety: the intended end state (definition, positions, deletions) is written to `streams.pending` first; `finish_pending` applies it
step by step, each step idempotent (a delete that already happened deletes nothing; recording an offset twice is the same), and the runner / the
daemon call it whenever they find a pending plan. The definition and `stream_offsets` change last, in one SQLite transaction.
"""
import datetime
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

from web import streaming as st

logger = logging.getLogger("localspark.streaming")


# ---------------------------------------------------------------- positions on the broker

def _parse_ts(v: Any) -> int:
    """Milliseconds since the epoch from a number or an ISO-8601 string (no zone = UTC)."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return int(v)
    try:
        d = datetime.datetime.fromisoformat(str(v).strip().replace("Z", "+00:00"))
    except ValueError:
        raise st.StreamError("The timestamp must be an ISO date and time, e.g. 2026-05-01T12:00:00Z.")
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetime.timezone.utc)
    return int(d.timestamp() * 1000)


def broker_view(conn: Dict[str, Any], topic: str, spec: Optional[Dict[str, Any]] = None) -> Tuple[Dict[int, Tuple[int, int]], Dict[int, int], Optional[str]]:
    """(watermarks {partition: (low, high)}, timestamp lookups {partition: offset} for spec mode 'timestamp', cluster id)."""
    ck = st._kafka()
    seen: List[str] = []
    c = ck.Consumer({**st.kafka_conf(conn), "group.id": f"dkw-ops-{time.time_ns()}", "enable.auto.commit": False, "error_cb": lambda e: seen.append(str(e))})
    try:
        md = st._list_topics_explained(c, seen, topic)
        t = md.topics.get(topic)
        if t is None or t.error is not None:
            raise st.StreamError(f"The topic '{topic}' does not exist or is not readable with this login.")
        wm: Dict[int, Tuple[int, int]] = {}
        for p in sorted(t.partitions):
            wm[p] = c.get_watermark_offsets(ck.TopicPartition(topic, p), timeout=15)
        by_time: Dict[int, int] = {}
        if spec and spec.get("mode") == "timestamp":
            ms = _parse_ts(spec.get("timestamp"))
            res = c.offsets_for_times([ck.TopicPartition(topic, p, ms) for p in wm], timeout=20)
            for tp in res:
                by_time[tp.partition] = tp.offset if tp.offset is not None and tp.offset >= 0 else wm[tp.partition][1]     # after the last message: the end
        return wm, by_time, getattr(md, "cluster_id", None)
    finally:
        c.close()


def resolve_start(spec: Dict[str, Any], wm: Dict[int, Tuple[int, int]], by_time: Dict[int, int], current: Optional[Dict[int, int]],
                  require_all: bool) -> Tuple[Dict[int, int], List[str]]:
    """Target offsets per partition (clamped into what the broker still has) and notes about clamping."""
    mode = (spec or {}).get("mode")
    notes: List[str] = []
    if mode == "earliest":
        raw = {p: lo for p, (lo, hi) in wm.items()}
    elif mode == "latest":
        raw = {p: hi for p, (lo, hi) in wm.items()}
    elif mode == "timestamp":
        raw = dict(by_time)
    elif mode == "offsets":
        given = spec.get("offsets") or {}
        try:
            raw = {int(k): int(v) for k, v in given.items()}
        except (TypeError, ValueError, AttributeError):
            raise st.StreamError("Offsets must be a partition number and an offset each.")
        unknown = sorted(set(raw) - set(wm))
        if unknown:
            raise st.StreamError(f"The topic has no partition(s) {unknown}.")
        if any(v < 0 for v in raw.values()):
            raise st.StreamError("An offset cannot be negative.")
        if require_all and set(raw) != set(wm):
            raise st.StreamError(f"Give an offset for every partition ({sorted(wm)}), or choose earliest, latest or a timestamp.")
        for p in wm:                                            # partitions not mentioned stay where they are
            if p not in raw and current and p in current:
                raw[p] = current[p]
    else:
        raise st.StreamError("Choose where to start: earliest, latest, a timestamp or offsets.")
    out: Dict[int, int] = {}
    for p, (lo, hi) in wm.items():
        if p not in raw:
            continue
        v = raw[p]
        if v < lo:
            notes.append(f"Partition {p}: offset {v} is older than the oldest message the broker still has ({lo}); using {lo}.")
            v = lo
        elif v > hi:
            notes.append(f"Partition {p}: offset {v} is beyond the end of the partition ({hi}); using {hi}.")
            v = hi
        out[p] = v
    return out, notes


# ---------------------------------------------------------------- what the stream has recorded

def recorded_offsets(s: Dict[str, Any], partitions: List[int]) -> Dict[int, int]:
    """Where the stream would resume: max(the table's application transactions, the bookkeeping), per partition."""
    from web import autoloader
    stored = st._stored_offsets(s["id"])
    out = {p: stored[p] for p in stored}
    try:
        location, so = autoloader.resolve_target(s["target_catalog"], s["target_schema"], s["target_table"])
        table = st._open_table(location, so)
    except Exception:
        table = None
    for p in set(partitions) | set(out):
        if table is not None:
            try:
                v = table.transaction_version(st._txn_app(s["id"], s["topic"], p))
                if v is not None:
                    out[p] = max(out.get(p, 0), int(v))
            except Exception:
                pass
    return out


def _count_rows(location: str, so, topic: str, delete_from: Dict[int, int], dlq: bool) -> int:
    table = st._open_table(location, so)
    if table is None or not delete_from:
        return 0
    import pyarrow.dataset as ds
    pc, oc = ("partition", "offset") if dlq else ("_partition", "_offset")
    expr = None
    for p, off in delete_from.items():
        e = (ds.field(pc) == p) & (ds.field(oc) >= off)
        expr = e if expr is None else (expr | e)
    if not dlq:
        expr = (ds.field("_topic") == topic) & expr
    try:
        return int(table.to_pyarrow_dataset().count_rows(filter=expr))
    except Exception:
        return 0


def _predicate(topic: str, delete_from: Dict[int, int], dlq: bool) -> str:
    pc, oc = ('"partition"', '"offset"') if dlq else ("_partition", "_offset")
    body = " OR ".join(f"({pc} = {int(p)} AND {oc} >= {int(o)})" for p, o in delete_from.items())
    return body if dlq else f"_topic = '{topic}' AND ({body})"


# ---------------------------------------------------------------- applying (idempotent)

def _record(table, s_id: str, topic: str, positions: Dict[int, int], delete_from: Optional[Dict[int, int]], dlq: bool) -> None:
    """One Delta commit on `table` that sets the recorded offsets (application transactions) and, when asked, deletes the rows at or after
    `delete_from`. Without rows to delete it is an empty append, which carries the transactions all the same."""
    from deltalake import CommitProperties, Transaction, write_deltalake
    txns = [Transaction(st._txn_app(s_id, topic, p, dlq=dlq), int(o)) for p, o in positions.items()]
    props = CommitProperties(app_transactions=txns, custom_metadata={"dkw_stream_op": "reposition"})
    if delete_from:
        try:
            table.delete(_predicate(topic, delete_from, dlq), commit_properties=props)
        except Exception as exc:
            logger.warning(f"Stream reposition: delete failed ({exc}); recording the offsets only")
            raise st.StreamError(f"Could not delete the rows to re-read: {str(exc)[:200]}")
    # a delete that matched nothing may not have committed; make sure the offsets are recorded
    table.update_incremental()
    if any(table.transaction_version(t.app_id) != t.version for t in txns):
        sch = table.schema()
        schema = sch.to_pyarrow() if hasattr(sch, "to_pyarrow") else pa.schema(sch.to_arrow())
        write_deltalake(table, schema.empty_table(), mode="append", commit_properties=props)


def _apply(s_id: str, topic: str, location: str, so, positions: Dict[int, int], delete_from: Optional[Dict[int, int]]) -> None:
    """Dead-letter table first, then the main table (the point of no return; both steps can be repeated safely)."""
    dlq_table = st._open_table(location.rstrip("/") + "_dlq", so)
    if dlq_table is not None:
        _record(dlq_table, s_id, topic, positions, delete_from, dlq=True)
    table = st._open_table(location, so)
    if table is not None:
        _record(table, s_id, topic, positions, delete_from, dlq=False)


def finish_pending(sid: str) -> None:
    """Applies the plan stored in `streams.pending` (a rewind or move) and then makes it the stream's definition. Safe to call repeatedly."""
    from web import autoloader
    c = st._db()
    try:
        r = c.execute("SELECT * FROM streams WHERE id = ?", (sid,)).fetchone()
    finally:
        c.close()
    if not r or not r["pending"]:
        return
    cur = st._row(r)
    plan = json.loads(r["pending"])
    new = {**{k: cur[k] for k in ("connection", "topic", "target_catalog", "target_schema", "target_table")}, **(plan.get("new") or {})}
    positions = {int(k): int(v) for k, v in plan["positions"].items()}
    delete_from = {int(k): int(v) for k, v in (plan.get("delete_from") or {}).items()} or None
    location, so = autoloader.resolve_target(new["target_catalog"], new["target_schema"], new["target_table"])
    if so is None:
        import os
        os.makedirs(os.path.dirname(location), exist_ok=True)
    _apply(sid, new["topic"], location, so, positions, delete_from)
    now = st._now()
    c = st._db()
    try:
        c.execute("UPDATE streams SET connection=?, topic=?, target_catalog=?, target_schema=?, target_table=?, pending=NULL, updated_at=? WHERE id=?",
                  (new["connection"], new["topic"], new["target_catalog"], new["target_schema"], new["target_table"], now, sid))
        c.execute("DELETE FROM stream_offsets WHERE stream_id = ?", (sid,))
        for p, o in positions.items():
            c.execute("INSERT INTO stream_offsets VALUES (?,?,?)", (sid, p, o))
        c.execute("INSERT OR IGNORE INTO stream_state (stream_id) VALUES (?)", (sid,))
        c.execute("UPDATE stream_state SET partitions_json = NULL, last_error = NULL WHERE stream_id = ?", (sid,))
        c.commit()
    finally:
        c.close()
    if plan.get("new"):
        try:
            from web.lineage import delete_node
            delete_node(st._topic_node_id(cur))
        except Exception:
            pass
        st._sync_lineage(st.get_stream(sid))


# ---------------------------------------------------------------- public operations

def _ensure_stopped(sid: str) -> Dict[str, Any]:
    s = st.get_stream(sid)
    if not s:
        raise LookupError("Stream not found.")
    if s["enabled"]:
        raise st.StreamBusy("Stop the stream first.")
    st.stop_runner(sid)                                   # a runner that is still winding down after Stop
    c = st._db()
    try:
        r = c.execute("SELECT lease_owner, heartbeat FROM stream_state WHERE stream_id = ?", (sid,)).fetchone()
    finally:
        c.close()
    if r and r["lease_owner"] and r["lease_owner"] != st.OWNER and r["heartbeat"] and time.time() - float(r["heartbeat"]) < st.LEASE_SECONDS:
        raise st.StreamBusy("Another studio process is still reading this stream; wait a moment after stopping it.")
    if s["pending"]:
        finish_pending(sid)                               # an interrupted earlier operation first
        s = st.get_stream(sid)
    return s


def _store_plan(sid: str, plan: Dict[str, Any]) -> None:
    c = st._db()
    try:
        c.execute("UPDATE streams SET pending = ? WHERE id = ?", (json.dumps(plan), sid))
        c.commit()
    finally:
        c.close()


def rewind(sid: str, spec: Dict[str, Any], mode: str, dry_run: bool, actor: str) -> Dict[str, Any]:
    """Moves the stream's position; see the module docstring. Returns the plan (dry run) or the updated stream."""
    if mode not in ("replace", "keep"):
        raise st.StreamError("Mode must be 'replace' (delete the rows that will be read again) or 'keep' (they are appended again).")
    s = _ensure_stopped(sid)
    conn = st._connection(s["connection"])
    wm, by_time, _cid = broker_view(conn, s["topic"], spec)
    current = recorded_offsets(s, list(wm))
    target, notes = resolve_start(spec, wm, by_time, current, require_all=False)
    from web import autoloader
    location, so = autoloader.resolve_target(s["target_catalog"], s["target_schema"], s["target_table"])
    delete_from = {p: t for p, t in target.items() if current.get(p) is not None and t < current[p]} if mode == "replace" else {}
    plan_rows = []
    for p in sorted(wm):
        cur = current.get(p)
        plan_rows.append({"partition": p, "current": cur, "target": target.get(p, cur), "low": wm[p][0], "high": wm[p][1],
                          "delete_rows": _count_rows(location, so, s["topic"], {p: delete_from[p]}, False) if p in delete_from else 0,
                          "delete_dead_letters": _count_rows(location.rstrip("/") + "_dlq", so, s["topic"], {p: delete_from[p]}, True) if p in delete_from else 0})
    summary = {"mode": mode, "partitions": plan_rows, "notes": notes,
               "delete_rows": sum(r["delete_rows"] for r in plan_rows), "delete_dead_letters": sum(r["delete_dead_letters"] for r in plan_rows),
               "rewinds": any(r["current"] is not None and r["target"] < r["current"] for r in plan_rows),
               "duplicates_warning": mode == "keep" and any(r["current"] is not None and r["target"] < r["current"] for r in plan_rows)}
    if dry_run:
        return summary
    # positions of partitions the spec did not touch are re-recorded as they are
    positions = {p: (target[p] if p in target else current[p]) for p in wm if p in target or p in current}
    _store_plan(sid, {"kind": "rewind", "positions": positions, "delete_from": delete_from, "actor": actor, "spec": spec, "mode": mode})
    finish_pending(sid)
    st._audit(actor, "STREAM_REWIND", s["name"], {"mode": mode, "start": spec.get("mode"), "deleted_rows": summary["delete_rows"]})
    out = st.get_stream(sid)
    out["rewind"] = summary
    return out


def move(sid: str, changes: Dict[str, Any], start: Optional[Dict[str, Any]], confirm_same_cluster: bool, dry_run: bool, actor: str) -> Dict[str, Any]:
    """Changes the connection, topic and/or target table of a stopped stream; see the module docstring."""
    s = _ensure_stopped(sid)
    from web import autoloader, connections
    new = {k: str(changes[k]).strip() for k in ("connection", "topic", "target_catalog", "target_schema", "target_table") if changes.get(k) not in (None, "")}
    new = {k: (v.lower() if k.startswith("target_") else v) for k, v in new.items()}
    merged = {**{k: s[k] for k in ("connection", "topic", "target_catalog", "target_schema", "target_table")}, **new}
    cleaned = st._clean({**merged, "name": s["name"], "format": s["format"], "starting_offsets": s["starting_offsets"], "max_records": s["max_records"],
                         "max_wait_seconds": s["max_wait_seconds"], "evolve_schema": s["evolve_schema"], "enabled": False}, s)
    merged = {k: cleaned[k] for k in merged}
    changed = {k: v for k, v in merged.items() if v != s[k]}
    if not changed:
        raise st.StreamError("Nothing to change.")
    target_changed = any(k.startswith("target_") for k in changed)
    if target_changed:
        clash = st._db().execute("SELECT name FROM streams WHERE target_catalog=? AND target_schema=? AND target_table=? AND id<>?",
                                 (merged["target_catalog"], merged["target_schema"], merged["target_table"], sid)).fetchone()
        if clash:
            raise st.StreamError(f"The stream '{clash['name']}' already loads that table.")
    old_conn = st._connection(s["connection"])
    new_conn = st._connection(merged["connection"])
    wm, by_time, new_cid = broker_view(new_conn, merged["topic"], start)
    same_cluster = merged["connection"] == s["connection"]
    notes: List[str] = []
    if not same_cluster:
        _w, _b, old_cid = broker_view(old_conn, s["topic"])
        if old_cid and new_cid:
            same_cluster = old_cid == new_cid
            if not same_cluster:
                notes.append("The new connection points to a different Kafka cluster.")
        elif confirm_same_cluster:
            same_cluster = True
        elif not start:
            raise st.StreamBusy("The broker does not report a cluster id, so it cannot be checked that both connections lead to the same cluster. "
                                "Confirm it (same_cluster), or choose a start position to begin fresh on the new connection.")
    source_changed = "topic" in changed or not same_cluster
    current = recorded_offsets(s, list(wm))
    if source_changed:
        if not start:
            raise st.StreamError("The topic or cluster changes, so the old offsets mean nothing there: choose where to start (earliest, latest, a timestamp or offsets for every partition).")
        positions, n2 = resolve_start(start, wm, by_time, None, require_all=True)
        notes += n2
    else:
        if sorted(current) and set(current) != set(wm):
            raise st.StreamError(f"The topic has partitions {sorted(wm)} but the stream has read {sorted(current)}: choose a start position instead.")
        bad = [p for p, o in current.items() if p in wm and not (wm[p][0] <= o <= wm[p][1])]
        if bad:
            raise st.StreamError(f"The recorded offsets of partition(s) {bad} are outside what the new connection's topic holds; choose a start position instead.")
        positions = {p: current[p] for p in wm if p in current}
    summary = {"changes": changed, "keeps_offsets": not source_changed, "source_changed": source_changed, "target_changed": target_changed,
               "positions": [{"partition": p, "target": positions.get(p), "low": wm[p][0], "high": wm[p][1], "current": current.get(p)} for p in sorted(wm)], "notes": notes}
    if dry_run:
        return summary
    _store_plan(sid, {"kind": "move", "positions": positions, "delete_from": None, "new": {k: merged[k] for k in ("connection", "topic", "target_catalog", "target_schema", "target_table")},
                      "actor": actor, "start": start})
    finish_pending(sid)
    st._audit(actor, "STREAM_MOVE", s["name"], {"changes": changed, "kept_offsets": not source_changed})
    out = st.get_stream(sid)
    out["move"] = summary
    return out
