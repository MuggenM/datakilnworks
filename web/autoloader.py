import os
import time
import json
import uuid
import sqlite3
import fnmatch
import hashlib
import shutil
import logging
import asyncio
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional

import duckdb
import pyarrow as pa
try:
    from croniter import croniter
except ImportError:  # requirements.txt ships croniter; guard like web/workflow.py
    croniter = None
from deltalake import DeltaTable, write_deltalake

from web.volumes import resolve_volume_posix_path, get_volume_physical_path
from web import autoloader_s3

logger = logging.getLogger("localspark.autoloader")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
DB_PATH = os.path.join(METADATA_DIR, "autoloader.db")

os.makedirs(METADATA_DIR, exist_ok=True)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_autoloader_db():
    """Initializes SQLite schema for pipelines and file checkpoint tracking."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS autoloader_pipelines (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        source_volume_path TEXT NOT NULL,
        file_pattern TEXT DEFAULT '*',
        target_catalog TEXT DEFAULT 'warehouse',
        target_schema TEXT DEFAULT 'dbo',
        target_table TEXT NOT NULL,
        ingest_mode TEXT DEFAULT 'append',
        merge_keys TEXT,
        schema_evolution TEXT DEFAULT 'addNewColumns',
        poll_interval_seconds INTEGER DEFAULT 10,
        enabled INTEGER DEFAULT 1,
        status TEXT DEFAULT 'IDLE',
        created_by TEXT DEFAULT 'admin',
        created_at TEXT,
        last_run_at TEXT,
        last_error TEXT,
        total_files_ingested INTEGER DEFAULT 0,
        total_rows_ingested INTEGER DEFAULT 0
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS autoloader_file_history (
        pipeline_id TEXT,
        file_path TEXT,
        file_hash TEXT,
        file_size_bytes INTEGER,
        status TEXT,
        rows_ingested INTEGER DEFAULT 0,
        execution_ms REAL DEFAULT 0,
        error_message TEXT,
        ingested_at TEXT,
        PRIMARY KEY (pipeline_id, file_hash)
    );
    """)

    # Additive migrations for databases created by earlier versions
    existing_cols = {row[1] for row in cursor.execute("PRAGMA table_info(autoloader_pipelines)").fetchall()}
    if "cron_schedule" not in existing_cols:
        cursor.execute("ALTER TABLE autoloader_pipelines ADD COLUMN cron_schedule TEXT")
    # File-watch triggering (web/autoloader_watch.py): woken by filesystem events instead of a timer; the sweep is the
    # low-frequency safety-net rescan that still runs because events can be missed.
    if "source_mount_id" not in existing_cols:              # S3 sources: which storage mount supplies endpoint + credentials
        cursor.execute("ALTER TABLE autoloader_pipelines ADD COLUMN source_mount_id TEXT")
    if "watch_enabled" not in existing_cols:
        cursor.execute("ALTER TABLE autoloader_pipelines ADD COLUMN watch_enabled INTEGER DEFAULT 0")
    if "watch_sweep_seconds" not in existing_cols:
        cursor.execute("ALTER TABLE autoloader_pipelines ADD COLUMN watch_sweep_seconds INTEGER DEFAULT 300")

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_file_history_pipeline ON autoloader_file_history(pipeline_id);
    """)
    conn.commit()
    conn.close()


def compute_file_fingerprint(file_path: str) -> str:
    """
    Computes a fast, unique file fingerprint combining file size, mtime, and a hash of the first 64KB.
    Ensures exactly-once idempotency without reading entire multi-gigabyte files.
    """
    stat = os.stat(file_path)
    h = hashlib.sha256()
    h.update(f"{stat.st_size}_{stat.st_mtime}_".encode("utf-8"))
    try:
        with open(file_path, "rb") as f:
            chunk = f.read(65536)
            h.update(chunk)
    except Exception:
        pass
    return h.hexdigest()


def list_pipelines() -> List[Dict[str, Any]]:
    """Returns all configured Auto-Loader pipelines."""
    init_autoloader_db()
    conn = get_db()
    rows = conn.execute("SELECT * FROM autoloader_pipelines ORDER BY created_at DESC").fetchall()
    conn.close()
    return [_with_watch_status(dict(r)) for r in rows]


def get_pipeline(pipeline_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves a single Auto-Loader pipeline by ID."""
    init_autoloader_db()
    conn = get_db()
    row = conn.execute("SELECT * FROM autoloader_pipelines WHERE id = ?", (pipeline_id,)).fetchone()
    conn.close()
    return _with_watch_status(dict(row)) if row else None


_watch_manager = None
_watch_manager_lock = threading.Lock()


def get_watch_manager():
    """The process-wide file-watch manager (created on first use)."""
    global _watch_manager
    with _watch_manager_lock:
        if _watch_manager is None:
            from web.autoloader_watch import WatchManager
            _watch_manager = WatchManager(run_pipeline_cycle, resolve_volume_posix_path)
        return _watch_manager


def _with_watch_status(pipe: Dict[str, Any]) -> Dict[str, Any]:
    pipe["watch_enabled"] = bool(pipe.get("watch_enabled"))
    if _watch_manager is not None or pipe["watch_enabled"]:
        try:
            pipe["watch"] = get_watch_manager().status(pipe)
        except Exception as exc:
            pipe["watch"] = {"mode": "fallback", "detail": str(exc)}
    else:
        pipe["watch"] = {"mode": "off"}
    return pipe


def sync_watchers():
    """Reconcile inotify watchers with the pipeline table (called after any pipeline change and by the daemon)."""
    try:
        init_autoloader_db()
        conn = get_db()
        rows = [dict(r) for r in conn.execute("SELECT * FROM autoloader_pipelines").fetchall()]
        conn.close()
        if _watch_manager is None and not any(r.get("watch_enabled") for r in rows):
            return
        get_watch_manager().sync(rows)
    except Exception as exc:
        logger.warning(f"Could not sync Auto-Loader file watchers: {exc}")


def normalize_cron(expr: Optional[str]) -> Optional[str]:
    """Validates a 5-field cron expression (evaluated in UTC); returns None for empty input."""
    expr = " ".join((expr or "").split())
    if not expr:
        return None
    if croniter is None:
        raise ValueError("Cron schedules require the 'croniter' package.")
    if len(expr.split(" ")) != 5 or not croniter.is_valid(expr):
        raise ValueError(f"Invalid cron expression '{expr}'. Use 5 fields, e.g. '*/15 * * * *'.")
    return expr


def cron_is_due(expr: str, last_run_iso: Optional[str], now: datetime) -> bool:
    """True when a cron tick has elapsed since the last run (UTC). A never-run pipeline is due immediately."""
    if not last_run_iso:
        return True
    last_run = datetime.strptime(last_run_iso, "%Y-%m-%d %H:%M:%S")
    return croniter(expr, last_run).get_next(datetime) <= now


INGEST_MODES = ("append", "merge", "overwrite")


def _validate_pipeline_mode(ingest_mode: str, merge_keys: str):
    if ingest_mode not in INGEST_MODES:
        raise ValueError(f"Unknown ingest mode '{ingest_mode}'. Use append, merge or overwrite.")
    if ingest_mode == "merge" and not _parse_merge_keys(merge_keys):
        raise ValueError("Merge mode requires at least one merge key (comma-separated column names).")


def _normalize_sweep(value) -> int:
    """Safety-net rescan interval (seconds) for file-watch pipelines; never below 30 s."""
    from web.autoloader_watch import DEFAULT_SWEEP_SECONDS, MIN_SWEEP_SECONDS
    try:
        return max(MIN_SWEEP_SECONDS, int(value)) if value not in (None, "") else DEFAULT_SWEEP_SECONDS
    except (TypeError, ValueError):
        raise ValueError("watch_sweep_seconds must be a number of seconds.")


def _validate_source(source_vol: str, watch_enabled: int, source_mount_id: Optional[str]) -> str:
    """Validates and normalises a pipeline source. S3 sources are polled: inotify has nothing to watch there."""
    if not autoloader_s3.is_s3_path(source_vol):
        if source_mount_id:
            raise ValueError("A storage mount only applies to s3:// sources.")
        return source_vol
    if watch_enabled:
        raise ValueError("File events watch local volumes only. An S3 source is polled: use a poll interval or a cron schedule.")
    try:
        return autoloader_s3.normalize_path(source_vol)
    except autoloader_s3.S3SourceError as exc:
        raise ValueError(str(exc))


def create_pipeline(data: Dict[str, Any], created_by: str = "admin") -> Dict[str, Any]:
    """Creates a new Auto-Loader pipeline."""
    init_autoloader_db()
    pipeline_id = data.get("id") or f"pipe_{uuid.uuid4().hex[:8]}"
    name = (data.get("name") or "New Ingestion Pipeline").strip()
    desc = (data.get("description") or "").strip()
    source_vol = (data.get("source_volume_path") or "").strip()
    pattern = (data.get("file_pattern") or "*").strip()
    target_cat = (data.get("target_catalog") or "warehouse").strip().lower()
    target_sch = (data.get("target_schema") or "dbo").strip().lower()
    target_tbl = (data.get("target_table") or "").strip().lower()
    ingest_mode = (data.get("ingest_mode") or "append").strip().lower()
    merge_keys = (data.get("merge_keys") or "").strip()
    schema_evol = normalize_schema_evolution(data.get("schema_evolution"))
    poll_sec = max(5, int(data.get("poll_interval_seconds", 10)))
    enabled = 1 if data.get("enabled", True) else 0

    _validate_pipeline_mode(ingest_mode, merge_keys)
    cron_schedule = normalize_cron(data.get("cron_schedule"))
    watch_enabled = 1 if data.get("watch_enabled") else 0
    watch_sweep = _normalize_sweep(data.get("watch_sweep_seconds"))
    if watch_enabled and cron_schedule:
        raise ValueError("File watching and a cron schedule are alternatives; choose one trigger.")
    if not source_vol:
        raise ValueError("Source volume path is required (e.g. /Volumes/warehouse/raw/iot_stream or s3://bucket/prefix/).")
    source_mount_id = (data.get("source_mount_id") or "").strip() or None
    source_vol = _validate_source(source_vol, watch_enabled, source_mount_id)
    if not target_tbl:
        raise ValueError("Target Delta table name is required.")

    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    conn = get_db()
    conn.execute("""
        INSERT INTO autoloader_pipelines (
            id, name, description, source_volume_path, file_pattern,
            target_catalog, target_schema, target_table, ingest_mode,
            merge_keys, schema_evolution, poll_interval_seconds, enabled,
            status, created_by, created_at, last_run_at, total_files_ingested, total_rows_ingested, cron_schedule,
            watch_enabled, watch_sweep_seconds, source_mount_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'IDLE', ?, ?, NULL, 0, 0, ?, ?, ?, ?)
    """, (
        pipeline_id, name, desc, source_vol, pattern,
        target_cat, target_sch, target_tbl, ingest_mode,
        merge_keys, schema_evol, poll_sec, enabled,
        created_by, now_iso, cron_schedule, watch_enabled, watch_sweep, source_mount_id
    ))
    conn.commit()
    conn.close()

    logger.info(f"Created Auto-Loader pipeline '{name}' ({pipeline_id}): {source_vol} -> {target_cat}.{target_sch}.{target_tbl}")
    sync_watchers()
    created = get_pipeline(pipeline_id)
    sync_pipeline_lineage(created)
    return created


def update_pipeline(pipeline_id: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Updates an existing Auto-Loader pipeline."""
    pipe = get_pipeline(pipeline_id)
    if not pipe:
        return None

    name = data.get("name", pipe["name"])
    desc = data.get("description", pipe["description"])
    source_vol = data.get("source_volume_path", pipe["source_volume_path"])
    pattern = data.get("file_pattern", pipe["file_pattern"])
    target_cat = data.get("target_catalog", pipe["target_catalog"])
    target_sch = data.get("target_schema", pipe["target_schema"])
    target_tbl = data.get("target_table", pipe["target_table"])
    ingest_mode = data.get("ingest_mode", pipe["ingest_mode"])
    merge_keys = data.get("merge_keys", pipe["merge_keys"])
    schema_evol = normalize_schema_evolution(data.get("schema_evolution", pipe["schema_evolution"]))
    poll_sec = max(5, int(data.get("poll_interval_seconds", pipe["poll_interval_seconds"])))
    enabled = 1 if data.get("enabled", pipe["enabled"]) else 0
    ingest_mode = (ingest_mode or "append").strip().lower()
    _validate_pipeline_mode(ingest_mode, merge_keys)
    cron_schedule = normalize_cron(data["cron_schedule"]) if "cron_schedule" in data else pipe.get("cron_schedule")
    watch_enabled = (1 if data["watch_enabled"] else 0) if "watch_enabled" in data else (1 if pipe.get("watch_enabled") else 0)
    watch_sweep = _normalize_sweep(data.get("watch_sweep_seconds", pipe.get("watch_sweep_seconds")))
    if "cron_schedule" in data and cron_schedule and "watch_enabled" not in data:
        watch_enabled = 0                    # choosing a schedule switches file watching off (they are alternatives)
    if watch_enabled and cron_schedule:
        raise ValueError("File watching and a cron schedule are alternatives; choose one trigger.")
    source_mount_id = ((data.get("source_mount_id") or "").strip() or None) if "source_mount_id" in data else pipe.get("source_mount_id")
    source_vol = _validate_source(source_vol, watch_enabled, source_mount_id)

    conn = get_db()
    conn.execute("""
        UPDATE autoloader_pipelines SET
            name = ?, description = ?, source_volume_path = ?, file_pattern = ?,
            target_catalog = ?, target_schema = ?, target_table = ?, ingest_mode = ?,
            merge_keys = ?, schema_evolution = ?, poll_interval_seconds = ?, enabled = ?,
            cron_schedule = ?, watch_enabled = ?, watch_sweep_seconds = ?, source_mount_id = ?
        WHERE id = ?
    """, (
        name, desc, source_vol, pattern,
        target_cat, target_sch, target_tbl, ingest_mode,
        merge_keys, schema_evol, poll_sec, enabled,
        cron_schedule, watch_enabled, watch_sweep, source_mount_id, pipeline_id
    ))
    conn.commit()
    conn.close()

    sync_watchers()
    updated = get_pipeline(pipeline_id)
    sync_pipeline_lineage(updated)
    return updated


def delete_pipeline(pipeline_id: str) -> bool:
    """Deletes an Auto-Loader pipeline and its execution history."""
    pipe = get_pipeline(pipeline_id)
    if pipe:
        _remove_pipeline_lineage(pipe)
    conn = get_db()
    conn.execute("DELETE FROM autoloader_file_history WHERE pipeline_id = ?", (pipeline_id,))
    cur = conn.execute("DELETE FROM autoloader_pipelines WHERE id = ?", (pipeline_id,))
    rows_deleted = cur.rowcount
    conn.commit()
    conn.close()
    sync_watchers()
    return rows_deleted > 0


def reset_pipeline_checkpoints(pipeline_id: str) -> Dict[str, Any]:
    """
    Clears file checkpoint history for a pipeline.
    Allows re-ingesting all files from scratch (idempotent replay).
    """
    conn = get_db()
    conn.execute("DELETE FROM autoloader_file_history WHERE pipeline_id = ?", (pipeline_id,))
    conn.execute("""
        UPDATE autoloader_pipelines 
        SET total_files_ingested = 0, total_rows_ingested = 0, last_error = NULL, status = 'IDLE'
        WHERE id = ?
    """, (pipeline_id,))
    conn.commit()
    conn.close()
    logger.info(f"Reset checkpoints for pipeline '{pipeline_id}'")
    return {"success": True, "message": f"Checkpoints for pipeline '{pipeline_id}' cleared. All source files will be re-processed."}


def get_pipeline_history(pipeline_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Retrieves the file ingestion audit log for a pipeline."""
    init_autoloader_db()
    conn = get_db()
    rows = conn.execute("""
        SELECT * FROM autoloader_file_history 
        WHERE pipeline_id = ? 
        ORDER BY ingested_at DESC 
        LIMIT ?
    """, (pipeline_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_autoloader_stats() -> Dict[str, Any]:
    """Calculates global throughput metrics across all Auto-Loader pipelines."""
    init_autoloader_db()
    conn = get_db()
    p_rows = conn.execute("SELECT COUNT(*) as total_pipes, SUM(enabled) as active_pipes FROM autoloader_pipelines").fetchone()
    f_rows = conn.execute("""
        SELECT 
            COUNT(*) as total_files, 
            SUM(rows_ingested) as total_rows,
            AVG(execution_ms) as avg_latency_ms
        FROM autoloader_file_history 
        WHERE status = 'SUCCESS'
    """,).fetchone()
    err_count = conn.execute("SELECT COUNT(*) FROM autoloader_file_history WHERE status IN ('FAILED', 'QUARANTINED')").fetchone()[0]
    conn.close()

    return {
        "total_pipelines": p_rows["total_pipes"] or 0,
        "active_pipelines": p_rows["active_pipes"] or 0,
        "total_files_ingested": f_rows["total_files"] or 0,
        "total_rows_ingested": f_rows["total_rows"] or 0,
        "avg_latency_ms": round(f_rows["avg_latency_ms"] or 0.0, 2),
        "quarantined_files": err_count or 0
    }


def _resolve_target_delta_path(catalog: str, schema: str, table_name: str) -> str:
    """Returns the local on-disk path for the target Delta table."""
    cat_clean = catalog.strip().lower()
    sch_clean = schema.strip().lower()
    tbl_clean = table_name.strip().lower()
    
    if cat_clean == "warehouse":
        return os.path.join(WAREHOUSE_DIR, sch_clean, tbl_clean)
    else:
        # Check if mounted catalog
        cat_dir = os.path.join(WAREHOUSE_DIR, "catalogs", cat_clean)
        return os.path.join(cat_dir, sch_clean, tbl_clean)


SCHEMA_EVOLUTION_MODES = {
    "addnewcolumns": "addNewColumns",
    "failonnewcolumns": "failOnNewColumns",
    "fail": "failOnNewColumns",
    "rescue": "rescue",
}
RESCUED_COLUMN = "_rescued_data"
BATCH_ROWS = int(os.getenv("AUTOLOADER_BATCH_ROWS", "100000"))


def normalize_schema_evolution(value: Optional[str]) -> str:
    """Maps user/UI input to a canonical schema-evolution policy name."""
    key = (value or "addNewColumns").strip().lower()
    if key not in SCHEMA_EVOLUTION_MODES:
        raise ValueError(
            f"Unknown schema evolution policy '{value}'. Use addNewColumns, failOnNewColumns or rescue."
        )
    return SCHEMA_EVOLUTION_MODES[key]


def _plan_schema_policy(columns: List[str], target_path: str, policy: str) -> List[str]:
    """
    Enforces the schema-evolution policy for a file's columns against an existing target table.
    - addNewColumns: nothing to do (Delta schema_mode='merge' adds the columns on write).
    - failOnNewColumns: raises if the file has columns unknown to the target.
    - rescue: returns the unknown columns, which the caller folds into `_rescued_data`.
    """
    if policy == "addNewColumns" or not target_path or not os.path.exists(os.path.join(target_path, "_delta_log")):
        return []
    known = {f.name.lower() for f in DeltaTable(target_path).schema().fields}
    known.discard(RESCUED_COLUMN)
    extra = [c for c in columns if c.lower() not in known]
    if extra and policy == "failOnNewColumns":
        raise ValueError(
            f"Schema mismatch: unknown column(s) {extra} (policy failOnNewColumns). "
            "Change the pipeline's schema evolution policy or update the target table."
        )
    return extra if policy == "rescue" else []


def _rescue_schema(schema: pa.Schema, extra: List[str]) -> pa.Schema:
    """Output schema of a rescue-policy stream: known columns plus a JSON string `_rescued_data`."""
    fields = [f for f in schema if f.name not in extra]
    return pa.schema(fields + [pa.field(RESCUED_COLUMN, pa.string())])


def _rescue_batch(batch: pa.RecordBatch, extra: List[str]) -> List[pa.RecordBatch]:
    """Moves `extra` columns of a batch into a JSON `_rescued_data` column (NULL when nothing was rescued)."""
    table = pa.Table.from_batches([batch])
    if extra:
        rescued = [json.dumps(r, default=str) for r in table.select(extra).to_pylist()]
        table = table.drop_columns(extra)
    else:
        rescued = [None] * table.num_rows
    return table.append_column(RESCUED_COLUMN, pa.array(rescued, type=pa.string())).to_batches()


def _evolve_schema_for_merge(target_path: str, schema: pa.Schema):
    """
    Adds columns that the source has but the target lacks through an empty schema-merging append.
    delta-rs MERGE cannot add columns itself, so this must run before it.
    """
    existing = {f.name for f in DeltaTable(target_path).schema().fields}
    if any(f.name not in existing for f in schema):
        write_deltalake(target_path, schema.empty_table(), mode="append", schema_mode="merge")


def _build_merge_predicate(keys: List[str], source_columns: List[str]) -> str:
    """
    Builds the MERGE join predicate. Each key must be an actual source column (matched case-insensitively)
    and is emitted as a quoted identifier, so pipeline config can never inject SQL.
    """
    by_lower = {c.lower(): c for c in source_columns}
    clauses = []
    for key in keys:
        column = by_lower.get(key.lower())
        if column is None:
            raise ValueError(f"Merge key '{key}' is not a column of the incoming file (columns: {source_columns}).")
        quoted = '"' + column.replace('"', '""') + '"'
        clauses.append(f"target.{quoted} = source.{quoted}")
    return " AND ".join(clauses)


def _parse_merge_keys(raw: Optional[str]) -> List[str]:
    return [k.strip() for k in (raw or "").split(",") if k.strip()]


def _open_source_reader(duck_conn, file_path: str, ext_lower: str) -> pa.RecordBatchReader:
    """Opens a streaming Arrow reader over a source file; nothing is materialized in memory."""
    safe_path = file_path.replace("'", "''")
    if ext_lower in ("csv", "tsv", "txt"):
        query = f"SELECT * FROM read_csv_auto('{safe_path}')"
    elif ext_lower == "parquet":
        query = f"SELECT * FROM read_parquet('{safe_path}')"
    elif ext_lower in ("json", "jsonl", "ndjson"):
        query = f"SELECT * FROM read_json_auto('{safe_path}')"
    else:
        raise ValueError(f"Unsupported file format '.{ext_lower}' for Auto-Loader.")
    return duck_conn.sql(query).fetch_arrow_reader(batch_size=BATCH_ROWS)


def _looks_like_remote_io_error(err: Exception) -> bool:
    """True for errors that say "could not reach or read the storage" rather than "this file is malformed"."""
    text = str(err).lower()
    return any(k in text for k in ("io error", "http", "connection", "timed out", "timeout", "access denied", "forbidden",
                                   "ssl", "secret", "could not resolve", "no such bucket", "extension"))


def _quarantine_file(pipeline_id, file_path, base_source_dir, rel_path, file_hash, file_size,
                     elapsed_ms, now_iso, err, remote=None, pipeline=None) -> Dict[str, Any]:
    """Moves a malformed file (or copies an S3 object) into `_quarantine/`, records it in the checkpoint DB and returns the result."""
    if remote is not None:
        autoloader_s3.quarantine(pipeline, remote)
    else:
        quarantine_dir = os.path.join(base_source_dir, "_quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)
        quarantine_file_dest = os.path.join(quarantine_dir, f"{os.path.basename(file_path)}.{int(time.time())}.bad")
        try:
            shutil.move(file_path, quarantine_file_dest)
            logger.warning(f"Quarantined corrupt file '{file_path}' -> '{quarantine_file_dest}': {err}")
        except Exception as q_err:
            logger.error(f"Failed to move file to quarantine: {q_err}")

    db = get_db()
    db.execute("""
        INSERT OR REPLACE INTO autoloader_file_history (
            pipeline_id, file_path, file_hash, file_size_bytes,
            status, rows_ingested, execution_ms, error_message, ingested_at
        ) VALUES (?, ?, ?, ?, 'QUARANTINED', 0, ?, ?, ?)
    """, (pipeline_id, rel_path, file_hash, file_size, elapsed_ms, str(err), now_iso))
    db.commit()
    db.close()
    return {
        "status": "QUARANTINED",
        "file_path": rel_path,
        "rows": 0,
        "error": str(err),
        "execution_ms": elapsed_ms
    }


def _remove_pipeline_lineage(pipeline: Dict[str, Any]):
    """Drops the pipeline's lineage edge, and the volume node when no other pipeline still reads it."""
    try:
        from web.lineage import delete_node, get_db_connection, make_table_id
        vol_id = _volume_lineage_id(pipeline["source_volume_path"])
        table_id = make_table_id(pipeline["target_catalog"], pipeline["target_schema"], pipeline["target_table"])
        with get_db_connection() as conn:
            conn.execute("DELETE FROM lineage_edges WHERE source_id = ? AND target_id = ? AND edge_type = 'AUTOLOADED_TO'",
                         (vol_id, table_id))
            remaining = conn.execute("SELECT COUNT(*) FROM lineage_edges WHERE source_id = ?", (vol_id,)).fetchone()[0]
        if remaining == 0:
            delete_node(vol_id)
    except Exception as lin_err:
        logger.debug(f"Lineage cleanup notice: {lin_err}")


def _volume_lineage_id(source_volume_path: str) -> str:
    """Lineage node id for the volume behind a pipeline (`volume:/Volumes/cat/schema/vol`)."""
    if autoloader_s3.is_s3_path(source_volume_path):
        return "volume:" + autoloader_s3.normalize_path(source_volume_path).rstrip("/")
    parts = [p for p in (source_volume_path or "").strip().replace("\\", "/").split("/") if p]
    if parts and parts[0].lower() == "volumes":
        parts = parts[1:]
    return "volume:/Volumes/" + "/".join(parts[:3]).lower()


def sync_pipeline_lineage(pipeline: Dict[str, Any], last_file: Optional[str] = None):
    """
    Records `VOLUME --AUTOLOADED_TO--> TABLE` in the lineage graph. The pipeline id rides on the edge
    (job_id), so a volume feeding several tables (or several volumes feeding one table) stays unambiguous.
    Best effort: lineage must never break ingestion.
    """
    try:
        from web.lineage import upsert_node, upsert_edge, make_table_id
        vol_id = _volume_lineage_id(pipeline["source_volume_path"])
        vol_path = vol_id[len("volume:"):]
        if autoloader_s3.is_s3_path(vol_path):
            bucket, _prefix = autoloader_s3.parse_s3_path(vol_path)
            vol_catalog, vol_schema, vol_name = "s3", bucket, vol_path[len("s3://"):]
        else:
            vol_parts = vol_path.split("/")  # ['', 'Volumes', catalog, schema, volume]
            vol_catalog = vol_parts[2] if len(vol_parts) > 2 else "warehouse"
            vol_schema = vol_parts[3] if len(vol_parts) > 3 else "dbo"
            vol_name = vol_parts[-1] or vol_path
        table_id = make_table_id(pipeline["target_catalog"], pipeline["target_schema"], pipeline["target_table"])

        # layer RAW_FILE + type VOLUME puts the node in the "Raw Files / Ingestion" column of the lineage graph
        upsert_node(vol_id, vol_name, "VOLUME", layer="RAW_FILE", catalog=vol_catalog, schema_name=vol_schema,
                    metadata={"posix_path": vol_path, "pipeline_id": pipeline["id"], "last_file": last_file})
        upsert_node(table_id, pipeline["target_table"], "TABLE",
                    catalog=pipeline["target_catalog"], schema_name=pipeline["target_schema"])
        upsert_edge(vol_id, table_id, edge_type="AUTOLOADED_TO", job_id=pipeline["id"])
    except Exception as lin_err:
        logger.debug(f"Lineage graph update notice: {lin_err}")


def purge_legacy_lineage_nodes():
    """Removes the per-file VOLUME_FILE and per-pipeline AUTOLOADER nodes written by earlier versions."""
    try:
        from web.lineage import delete_nodes_by_type
        removed = sum(delete_nodes_by_type(t) for t in ("VOLUME_FILE", "AUTOLOADER"))
        if removed:
            logger.info(f"Auto-Loader: removed {removed} legacy lineage nodes")
    except Exception as err:
        logger.debug(f"Legacy lineage purge skipped: {err}")


def process_single_file(pipeline: Dict[str, Any], file_path: str, base_source_dir: str,
                        remote: Optional["autoloader_s3.RemoteObject"] = None,
                        s3_conn: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Ingests a single file (or, with `remote`, one S3 object read in place) into the pipeline's target Delta Lake table.
    Enforces schema evolution and moves corrupt files into _quarantine.
    """
    start_time = time.perf_counter()
    pipeline_id = pipeline["id"]
    if remote is not None:
        file_size, file_hash, rel_path = remote.size, autoloader_s3.fingerprint(remote), remote.rel_key
        file_path = remote.url
    else:
        stat = os.stat(file_path)
        file_size = stat.st_size
        file_hash = compute_file_fingerprint(file_path)
        rel_path = os.path.relpath(file_path, base_source_dir).replace("\\", "/")

    _, ext = os.path.splitext(file_path)
    ext_lower = ext.lower().lstrip(".")

    target_path = _resolve_target_delta_path(
        pipeline["target_catalog"],
        pipeline["target_schema"],
        pipeline["target_table"]
    )
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Step 1+2: Stream the source file through DuckDB into a single Delta commit.
    # Batches flow lazily reader -> generator -> delta-rs, so memory stays bounded by BATCH_ROWS.
    # The commit happens only after the whole stream is consumed, so a bad row midway aborts atomically.
    duck_conn = duckdb.connect(":memory:")
    read_state = {"opened": False, "error": None, "rows": 0}
    try:
        if remote is not None:
            autoloader_s3.configure_duckdb(duck_conn, s3_conn)
        reader = _open_source_reader(duck_conn, file_path, ext_lower)
        read_state["opened"] = True

        table_exists = os.path.exists(target_path) and os.path.exists(os.path.join(target_path, "_delta_log"))
        ingest_mode = pipeline.get("ingest_mode", "append").lower()
        schema_evol = normalize_schema_evolution(pipeline.get("schema_evolution"))

        out_schema = reader.schema
        extra_cols: List[str] = []
        if schema_evol == "rescue":
            # A fresh (or overwritten) table has nothing to rescue against, but keeps the column for later files.
            replacing = not table_exists or ingest_mode == "overwrite"
            extra_cols = _plan_schema_policy(reader.schema.names, "" if replacing else target_path, schema_evol)
            out_schema = _rescue_schema(reader.schema, extra_cols)
        elif not (not table_exists or ingest_mode == "overwrite"):
            _plan_schema_policy(reader.schema.names, target_path, schema_evol)

        def _batches():
            try:
                for batch in reader:
                    read_state["rows"] += batch.num_rows
                    if schema_evol == "rescue":
                        yield from _rescue_batch(batch, extra_cols)
                    else:
                        yield batch
            except Exception as stream_err:
                read_state["error"] = stream_err
                raise

        source = pa.RecordBatchReader.from_batches(out_schema, _batches())

        if not table_exists or ingest_mode == "overwrite":
            write_deltalake(target_path, source, mode="overwrite", schema_mode="overwrite" if table_exists else None)
        elif ingest_mode == "append":
            write_deltalake(target_path, source, mode="append", schema_mode="merge")
        elif ingest_mode == "merge":
            # Primary-Key Upsert
            predicate = _build_merge_predicate(_parse_merge_keys(pipeline.get("merge_keys")), out_schema.names)
            _evolve_schema_for_merge(target_path, out_schema)
            (DeltaTable(target_path).merge(
                source=source,
                predicate=predicate,
                source_alias="source",
                target_alias="target"
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute())
        else:
            raise ValueError(f"Unknown ingest mode '{ingest_mode}'.")

        row_count = read_state["rows"]
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

        # Step 3: Record Successful Checkpoint
        db = get_db()
        db.execute("""
            INSERT OR REPLACE INTO autoloader_file_history (
                pipeline_id, file_path, file_hash, file_size_bytes,
                status, rows_ingested, execution_ms, error_message, ingested_at
            ) VALUES (?, ?, ?, ?, 'SUCCESS', ?, ?, NULL, ?)
        """, (pipeline_id, rel_path, file_hash, file_size, row_count, elapsed_ms, now_iso))

        db.execute("""
            UPDATE autoloader_pipelines SET
                total_files_ingested = total_files_ingested + 1,
                total_rows_ingested = total_rows_ingested + ?,
                last_run_at = ?,
                last_error = NULL,
                status = 'IDLE'
            WHERE id = ?
        """, (row_count, now_iso, pipeline_id))
        db.commit()
        db.close()

        # Rescued columns may hold values of unknown columns (potentially personal data): flag them for review.
        if schema_evol == "rescue":
            try:
                from web.governance import tags as gov_tags
                gov_tags.set_tag(catalog=pipeline["target_catalog"], schema_name=pipeline["target_schema"],
                                 table_name=pipeline["target_table"], column_name=RESCUED_COLUMN, tag_key="sensitivity",
                                 tag_value="unclassified", actor="autoloader", source="propagated")
            except Exception as gov_err:
                logger.debug(f"Governance tag for {RESCUED_COLUMN} skipped: {gov_err}")

        # Step 4: Lineage DAG Sync (one VOLUME -> TABLE edge per pipeline, not one node per file)
        sync_pipeline_lineage(pipeline, last_file=rel_path)

        logger.info(f"AutoLoader [{pipeline['name']}]: Ingested '{rel_path}' ({row_count} rows, {elapsed_ms}ms) -> {pipeline['target_catalog']}.{pipeline['target_schema']}.{pipeline['target_table']}")

        return {
            "status": "SUCCESS",
            "file_path": rel_path,
            "rows": row_count,
            "execution_ms": elapsed_ms
        }

    except Exception as write_err:
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        source_err = read_state["error"] or write_err
        remote_io = remote is not None and _looks_like_remote_io_error(source_err)
        if (not read_state["opened"] or read_state["error"] is not None) and not remote_io:
            # The source itself is unreadable/corrupt (not a Delta problem): quarantine it and keep going.
            # (An S3 access / network / endpoint problem is not the object's fault: it is FAILED and retried instead.)
            return _quarantine_file(pipeline_id, file_path, base_source_dir, rel_path, file_hash,
                                    file_size, elapsed_ms, now_iso, read_state["error"] or write_err,
                                    remote=remote, pipeline=pipeline)
        db = get_db()
        db.execute("""
            INSERT OR REPLACE INTO autoloader_file_history (
                pipeline_id, file_path, file_hash, file_size_bytes,
                status, rows_ingested, execution_ms, error_message, ingested_at
            ) VALUES (?, ?, ?, ?, 'FAILED', 0, ?, ?, ?)
        """, (pipeline_id, rel_path, file_hash, file_size, elapsed_ms, str(write_err), now_iso))
        db.execute("UPDATE autoloader_pipelines SET last_error = ?, status = 'ERROR' WHERE id = ?", (str(write_err), pipeline_id))
        db.commit()
        db.close()

        logger.error(f"AutoLoader [{pipeline['name']}]: Failed to ingest '{rel_path}': {write_err}")
        return {
            "status": "FAILED",
            "file_path": rel_path,
            "rows": 0,
            "error": str(write_err),
            "execution_ms": elapsed_ms
        }
    finally:
        duck_conn.close()


_cycle_locks: Dict[str, threading.Lock] = {}
_cycle_locks_guard = threading.Lock()


def run_pipeline_cycle(pipeline_id: str) -> Dict[str, Any]:
    """
    Executes one ingestion cycle for a pipeline (a poll tick, a cron tick, a file event or "Run now"). A pipeline never
    runs two cycles at once: a second trigger while one is running returns {"skipped": ...} immediately, and the file
    watcher re-arms itself so a file that landed mid-scan is picked up by the next cycle.
    """
    with _cycle_locks_guard:
        lock = _cycle_locks.setdefault(pipeline_id, threading.Lock())
    if not lock.acquire(blocking=False):
        return {"pipeline_id": pipeline_id, "skipped": "a cycle is already running", "files_found": 0, "files_ingested": 0,
                "files_quarantined": 0, "rows_ingested": 0, "details": []}
    try:
        return _run_pipeline_cycle_impl(pipeline_id)
    finally:
        lock.release()


def _run_s3_cycle(pipe: Dict[str, Any]) -> Dict[str, Any]:
    """One polling cycle over an S3 prefix: list, skip checkpointed objects, stream each new one into Delta."""
    pipeline_id = pipe["id"]
    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    db = get_db()
    db.execute("UPDATE autoloader_pipelines SET status = 'RUNNING', last_run_at = ? WHERE id = ?", (now_iso, pipeline_id))
    # A QUARANTINED object may still be in the bucket (the credentials could not delete it): never retry it.
    known_rows = db.execute("SELECT file_hash FROM autoloader_file_history WHERE pipeline_id = ? AND status IN ('SUCCESS', 'QUARANTINED')",
                            (pipeline_id,)).fetchall()
    known_hashes = {r[0] for r in known_rows}
    db.commit()
    db.close()

    def finish(status: str, error: Optional[str] = None, clear_error: bool = False):
        d = get_db()
        if error is not None or clear_error:
            d.execute("UPDATE autoloader_pipelines SET status = ?, last_error = ? WHERE id = ?", (status, error, pipeline_id))
        else:
            d.execute("UPDATE autoloader_pipelines SET status = ? WHERE id = ?", (status, pipeline_id))
        d.commit()
        d.close()

    try:
        conn = autoloader_s3.resolve_connection(pipe)
        client = autoloader_s3.make_client(conn)
        objects = autoloader_s3.list_objects(pipe, client)
    except autoloader_s3.S3SourceError as exc:
        logger.error(f"Auto-Loader pipeline {pipeline_id}: {exc}")
        finish("ERROR", str(exc))
        return {"error": str(exc), "pipeline_id": pipeline_id, "files_found": 0, "files_ingested": 0, "rows_ingested": 0}

    files_ingested = total_rows = files_quarantined = 0
    results = []
    for obj in objects:
        try:
            if autoloader_s3.fingerprint(obj) in known_hashes:
                continue
            res = process_single_file(pipe, obj.url, "", remote=obj, s3_conn=conn)
            results.append(res)
            if res["status"] == "SUCCESS":
                files_ingested += 1
                total_rows += res.get("rows", 0)
                known_hashes.add(autoloader_s3.fingerprint(obj))
            elif res["status"] == "QUARANTINED":
                files_quarantined += 1
        except Exception as exc:
            logger.error(f"Unexpected error processing '{obj.url}': {exc}")

    failed = any(r["status"] == "FAILED" for r in results)
    finish("ERROR" if failed else ("IDLE" if pipe.get("enabled", 1) else "PAUSED"), clear_error=not failed)
    return {"pipeline_id": pipeline_id, "name": pipe["name"], "files_found": len(objects), "files_ingested": files_ingested,
            "files_quarantined": files_quarantined, "rows_ingested": total_rows, "details": results}


def _run_pipeline_cycle_impl(pipeline_id: str) -> Dict[str, Any]:
    """
    Scans the watched volume directory for new uningested files and loads them.
    """
    pipe = get_pipeline(pipeline_id)
    if not pipe:
        return {"error": "Pipeline not found"}
    if autoloader_s3.is_s3_path(pipe["source_volume_path"]):
        return _run_s3_cycle(pipe)

    try:
        source_dir = resolve_volume_posix_path(pipe["source_volume_path"])
    except Exception as e:
        return {"error": f"Invalid source volume path '{pipe['source_volume_path']}': {e}"}

    if not os.path.exists(source_dir):
        os.makedirs(source_dir, exist_ok=True)
        return {"pipeline_id": pipeline_id, "files_found": 0, "files_ingested": 0, "rows_ingested": 0}

    pattern = pipe.get("file_pattern") or "*"
    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Update status to RUNNING
    db = get_db()
    db.execute("UPDATE autoloader_pipelines SET status = 'RUNNING', last_run_at = ? WHERE id = ?", (now_iso, pipeline_id))
    # Fetch known processed hashes
    known_rows = db.execute("SELECT file_hash FROM autoloader_file_history WHERE pipeline_id = ? AND status = 'SUCCESS'", (pipeline_id,)).fetchall()
    known_hashes = {r[0] for r in known_rows}
    db.commit()
    db.close()

    discovered_files = []
    try:
        for root, dirs, files in os.walk(source_dir):
            # Skip hidden and quarantine directories
            dirs[:] = [d for d in dirs if not d.startswith(".") and d != "_quarantine"]
            for f in files:
                if f.startswith(".") or f.endswith(".tmp") or f.endswith(".part"):
                    continue
                if fnmatch.fnmatch(f.lower(), pattern.lower()) or pattern == "*":
                    discovered_files.append(os.path.join(root, f))
    except Exception as scan_err:
        logger.error(f"Error scanning source directory '{source_dir}': {scan_err}")

    # Sort files by modification time so older batches are ingested first
    discovered_files.sort(key=lambda p: os.path.getmtime(p))

    files_ingested = 0
    total_rows = 0
    files_quarantined = 0
    results = []

    for fpath in discovered_files:
        try:
            fhash = compute_file_fingerprint(fpath)
            if fhash in known_hashes:
                # Already processed exactly once
                continue

            res = process_single_file(pipe, fpath, source_dir)
            results.append(res)

            if res["status"] == "SUCCESS":
                files_ingested += 1
                total_rows += res.get("rows", 0)
                known_hashes.add(fhash)
            elif res["status"] == "QUARANTINED":
                files_quarantined += 1
        except Exception as e:
            logger.error(f"Unexpected error processing '{fpath}': {e}")

    # Set status back to IDLE
    final_status = "IDLE" if pipe.get("enabled", 1) else "PAUSED"
    db = get_db()
    db.execute("UPDATE autoloader_pipelines SET status = ? WHERE id = ?", (final_status, pipeline_id))
    db.commit()
    db.close()

    return {
        "pipeline_id": pipeline_id,
        "name": pipe["name"],
        "files_found": len(discovered_files),
        "files_ingested": files_ingested,
        "files_quarantined": files_quarantined,
        "rows_ingested": total_rows,
        "details": results
    }


async def autoloader_daemon_loop():
    """
    Background daemon continuously monitoring active Auto-Loader pipelines.
    Runs asynchronously alongside the main FastAPI web server.
    """
    logger.info("Volume Auto-Loader Background Daemon started.")
    await asyncio.to_thread(purge_legacy_lineage_nodes)
    last_run_map: Dict[str, float] = {}

    while True:
        try:
            await asyncio.to_thread(sync_watchers)
            pipelines = list_pipelines()
            now = time.time()

            for p in pipelines:
                p_id = p["id"]
                if not p.get("enabled", 1):
                    continue

                cron_expr = p.get("cron_schedule")
                if p.get("watch_enabled") and not cron_expr and get_watch_manager().is_watching(p_id):
                    # Events start cycles; this is only the low-frequency safety-net rescan (missed events, hard links,
                    # filesystems that emit none).
                    from web.autoloader_watch import sweep_seconds
                    due = (now - last_run_map.setdefault(p_id, now)) >= sweep_seconds(p)
                elif cron_expr:
                    # Scheduled pipelines run when a cron tick (UTC) has elapsed since the last run in the DB.
                    try:
                        due = cron_is_due(cron_expr, p.get("last_run_at") or p.get("created_at"), datetime.utcnow())
                    except Exception as cron_err:
                        logger.error(f"Auto-Loader pipeline {p_id}: bad cron '{cron_expr}': {cron_err}")
                        continue
                else:
                    interval = max(5, int(p.get("poll_interval_seconds", 10)))
                    due = (now - last_run_map.get(p_id, 0.0)) >= interval

                if due:
                    last_run_map[p_id] = now
                    # Run cycle in background thread to avoid blocking asyncio event loop
                    await asyncio.to_thread(run_pipeline_cycle, p_id)

        except asyncio.CancelledError:
            logger.info("Volume Auto-Loader daemon stopped gracefully.")
            break
        except Exception as e:
            logger.error(f"Error in Auto-Loader daemon loop: {e}", exc_info=True)

        await asyncio.sleep(5)


def seed_demo_pipeline():
    """Seeds a demonstration volume and Auto-Loader pipeline if none exist."""
    from web.volumes import create_volume, upload_file_to_volume, list_volumes

    # Ensure volume exists
    vols = list_volumes(catalog="warehouse", schema="raw")
    has_iot = any(v.get("name") == "iot_stream" for v in vols)
    if not has_iot:
        create_volume(
            catalog="warehouse",
            schema="raw",
            name="iot_stream",
            description="Incoming raw IoT sensor telemetry streams (CSV, JSON, Parquet).",
            owner="admin"
        )

    # Ensure pipeline exists
    existing_pipes = list_pipelines()
    if not existing_pipes:
        pipe = create_pipeline({
            "name": "IoT Sensor Telemetry Auto-Loader",
            "description": "Continuously streams incoming sensor CSV files into the bronze Delta table with automatic schema evolution.",
            "source_volume_path": "/Volumes/warehouse/raw/iot_stream",
            "file_pattern": "*.csv",
            "target_catalog": "warehouse",
            "target_schema": "dbo",
            "target_table": "bronze_iot_telemetry",
            "ingest_mode": "append",
            "schema_evolution": "addNewColumns",
            "poll_interval_seconds": 10,
            "enabled": True
        })

        # Add initial sample batch CSV file into the volume
        sample_csv = (
            "device_id,sensor_type,temperature,humidity,recorded_at\n"
            "dev_101,thermal,23.5,45.2,2026-09-20 00:01:00\n"
            "dev_102,pressure,24.1,44.8,2026-09-20 00:01:05\n"
            "dev_103,vibration,22.8,46.1,2026-09-20 00:01:10\n"
            "dev_104,thermal,25.2,43.9,2026-09-20 00:01:15\n"
            "dev_105,acoustic,23.9,45.0,2026-09-20 00:01:20\n"
        )
        upload_file_to_volume(
            catalog="warehouse",
            schema="raw",
            volume_name="iot_stream",
            filename="iot_batch_001.csv",
            content_bytes=sample_csv.encode("utf-8")
        )

        logger.info("Successfully seeded demo Auto-Loader pipeline and sample volume files.")
