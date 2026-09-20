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
from datetime import datetime
from typing import Dict, Any, List, Optional

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from web.volumes import resolve_volume_posix_path, get_volume_physical_path

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
    return [dict(r) for r in rows]


def get_pipeline(pipeline_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves a single Auto-Loader pipeline by ID."""
    init_autoloader_db()
    conn = get_db()
    row = conn.execute("SELECT * FROM autoloader_pipelines WHERE id = ?", (pipeline_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


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

    if not source_vol:
        raise ValueError("Source volume path is required (e.g. /Volumes/warehouse/raw/iot_stream).")
    if not target_tbl:
        raise ValueError("Target Delta table name is required.")

    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    conn = get_db()
    conn.execute("""
        INSERT INTO autoloader_pipelines (
            id, name, description, source_volume_path, file_pattern,
            target_catalog, target_schema, target_table, ingest_mode,
            merge_keys, schema_evolution, poll_interval_seconds, enabled,
            status, created_by, created_at, last_run_at, total_files_ingested, total_rows_ingested
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'IDLE', ?, ?, NULL, 0, 0)
    """, (
        pipeline_id, name, desc, source_vol, pattern,
        target_cat, target_sch, target_tbl, ingest_mode,
        merge_keys, schema_evol, poll_sec, enabled,
        created_by, now_iso
    ))
    conn.commit()
    conn.close()

    logger.info(f"Created Auto-Loader pipeline '{name}' ({pipeline_id}): {source_vol} -> {target_cat}.{target_sch}.{target_tbl}")
    return get_pipeline(pipeline_id)


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

    conn = get_db()
    conn.execute("""
        UPDATE autoloader_pipelines SET
            name = ?, description = ?, source_volume_path = ?, file_pattern = ?,
            target_catalog = ?, target_schema = ?, target_table = ?, ingest_mode = ?,
            merge_keys = ?, schema_evolution = ?, poll_interval_seconds = ?, enabled = ?
        WHERE id = ?
    """, (
        name, desc, source_vol, pattern,
        target_cat, target_sch, target_tbl, ingest_mode,
        merge_keys, schema_evol, poll_sec, enabled,
        pipeline_id
    ))
    conn.commit()
    conn.close()

    return get_pipeline(pipeline_id)


def delete_pipeline(pipeline_id: str) -> bool:
    """Deletes an Auto-Loader pipeline and its execution history."""
    conn = get_db()
    conn.execute("DELETE FROM autoloader_file_history WHERE pipeline_id = ?", (pipeline_id,))
    cur = conn.execute("DELETE FROM autoloader_pipelines WHERE id = ?", (pipeline_id,))
    rows_deleted = cur.rowcount
    conn.commit()
    conn.close()
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


def normalize_schema_evolution(value: Optional[str]) -> str:
    """Maps user/UI input to a canonical schema-evolution policy name."""
    key = (value or "addNewColumns").strip().lower()
    if key not in SCHEMA_EVOLUTION_MODES:
        raise ValueError(
            f"Unknown schema evolution policy '{value}'. Use addNewColumns, failOnNewColumns or rescue."
        )
    return SCHEMA_EVOLUTION_MODES[key]


def _apply_schema_policy(arrow_table: pa.Table, target_path: str, policy: str) -> pa.Table:
    """
    Enforces the schema-evolution policy against an existing target table.
    - addNewColumns: unchanged (Delta schema_mode='merge' adds the columns on write).
    - failOnNewColumns: raises if the file has columns unknown to the target.
    - rescue: folds unknown columns into a JSON `_rescued_data` string column.
    """
    if policy == "rescue":
        known = None
        if os.path.exists(os.path.join(target_path, "_delta_log")):
            known = {f.name.lower() for f in DeltaTable(target_path).schema().fields}
            known.discard(RESCUED_COLUMN)
        if known is None:
            rescued = [None] * arrow_table.num_rows
        else:
            extra = [c for c in arrow_table.column_names if c.lower() not in known]
            if extra:
                extra_rows = arrow_table.select(extra).to_pylist()
                rescued = [json.dumps(r, default=str) for r in extra_rows]
                arrow_table = arrow_table.drop_columns(extra)
            else:
                rescued = [None] * arrow_table.num_rows
        return arrow_table.append_column(RESCUED_COLUMN, pa.array(rescued, type=pa.string()))

    if policy == "failOnNewColumns" and os.path.exists(os.path.join(target_path, "_delta_log")):
        known = {f.name.lower() for f in DeltaTable(target_path).schema().fields}
        extra = [c for c in arrow_table.column_names if c.lower() not in known]
        if extra:
            raise ValueError(
                f"Schema mismatch: unknown column(s) {extra} (policy failOnNewColumns). "
                "Change the pipeline's schema evolution policy or update the target table."
            )
    return arrow_table


def _ensure_column_exists(target_path: str, arrow_table: pa.Table, column: str):
    """Adds `column` to the Delta schema through an empty schema-merging append (needed before MERGE)."""
    existing = {f.name for f in DeltaTable(target_path).schema().fields}
    if column not in existing:
        write_deltalake(target_path, arrow_table.slice(0, 0), mode="append", schema_mode="merge")


def process_single_file(pipeline: Dict[str, Any], file_path: str, base_source_dir: str) -> Dict[str, Any]:
    """
    Ingests a single file into the pipeline's target Delta Lake table.
    Enforces schema evolution and moves corrupt files into _quarantine.
    """
    start_time = time.perf_counter()
    pipeline_id = pipeline["id"]
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

    # Step 1: Read source file using DuckDB into PyArrow Table
    duck_conn = duckdb.connect(":memory:")
    try:
        if ext_lower in ("csv", "tsv", "txt"):
            query = f"SELECT * FROM read_csv_auto('{file_path}')"
        elif ext_lower == "parquet":
            query = f"SELECT * FROM read_parquet('{file_path}')"
        elif ext_lower in ("json", "jsonl", "ndjson"):
            query = f"SELECT * FROM read_json_auto('{file_path}')"
        else:
            raise ValueError(f"Unsupported file format '.{ext_lower}' for Auto-Loader.")

        arrow_reader = duck_conn.sql(query).arrow()
        arrow_table = arrow_reader.read_all()
        row_count = arrow_table.num_rows

    except Exception as read_err:
        duck_conn.close()
        # Quarantine malformed file
        quarantine_dir = os.path.join(base_source_dir, "_quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)
        quarantine_file_dest = os.path.join(quarantine_dir, f"{os.path.basename(file_path)}.{int(time.time())}.bad")
        try:
            shutil.move(file_path, quarantine_file_dest)
            logger.warning(f"Quarantined corrupt file '{file_path}' -> '{quarantine_file_dest}': {read_err}")
        except Exception as q_err:
            logger.error(f"Failed to move file to quarantine: {q_err}")

        # Record in history
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        db = get_db()
        db.execute("""
            INSERT OR REPLACE INTO autoloader_file_history (
                pipeline_id, file_path, file_hash, file_size_bytes,
                status, rows_ingested, execution_ms, error_message, ingested_at
            ) VALUES (?, ?, ?, ?, 'QUARANTINED', 0, ?, ?, ?)
        """, (pipeline_id, rel_path, file_hash, file_size, elapsed_ms, str(read_err), now_iso))
        db.commit()
        db.close()

        return {
            "status": "QUARANTINED",
            "file_path": rel_path,
            "rows": 0,
            "error": str(read_err),
            "execution_ms": elapsed_ms
        }
    finally:
        duck_conn.close()

    # Step 2: Write into Delta Lake with Schema Evolution & Ingest Mode
    try:
        table_exists = os.path.exists(target_path) and os.path.exists(os.path.join(target_path, "_delta_log"))
        ingest_mode = pipeline.get("ingest_mode", "append").lower()
        schema_evol = normalize_schema_evolution(pipeline.get("schema_evolution"))

        if not table_exists or ingest_mode == "overwrite":
            if schema_evol == "rescue":
                # Fresh schema: nothing to rescue yet, but keep the column so later files can use it.
                arrow_table = _apply_schema_policy(arrow_table, "", schema_evol)
            write_deltalake(target_path, arrow_table, mode="overwrite", schema_mode="overwrite" if table_exists else None)
        else:
            arrow_table = _apply_schema_policy(arrow_table, target_path, schema_evol)
            if ingest_mode == "append":
                write_deltalake(target_path, arrow_table, mode="append", schema_mode="merge")
            elif ingest_mode == "merge":
                # Primary-Key Upsert
                dt = DeltaTable(target_path)
                raw_keys = pipeline.get("merge_keys") or ""
                keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
                if not keys:
                    # Fallback to append if no merge keys defined
                    write_deltalake(target_path, arrow_table, mode="append", schema_mode="merge")
                else:
                    if schema_evol == "rescue":
                        _ensure_column_exists(target_path, arrow_table, RESCUED_COLUMN)
                        dt = DeltaTable(target_path)
                    predicate = " AND ".join([f"target.{k} = source.{k}" for k in keys])
                    (dt.merge(
                        source=arrow_table,
                        predicate=predicate,
                        source_alias="source",
                        target_alias="target"
                    )
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                    .execute())

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

        # Step 4: Lineage DAG Sync
        try:
            from web.lineage import upsert_node, upsert_edge, make_table_id
            v_name = os.path.basename(pipeline["source_volume_path"])
            file_node_id = f"volume_file:{v_name}/{rel_path}"
            pipe_node_id = f"pipeline:{pipeline['id']}"
            table_node_id = make_table_id(pipeline["target_catalog"], pipeline["target_schema"], pipeline["target_table"])

            upsert_node(file_node_id, os.path.basename(rel_path), "VOLUME_FILE", layer="RAW")
            upsert_node(pipe_node_id, pipeline["name"], "AUTOLOADER", layer="INGESTION")
            upsert_node(table_node_id, pipeline["target_table"], "TABLE", catalog=pipeline["target_catalog"], schema_name=pipeline["target_schema"])

            upsert_edge(file_node_id, pipe_node_id, edge_type="CONSUMED_BY")
            upsert_edge(pipe_node_id, table_node_id, edge_type="AUTOLOADS_TO")
        except Exception as lin_err:
            logger.debug(f"Lineage graph update notice: {lin_err}")

        logger.info(f"AutoLoader [{pipeline['name']}]: Ingested '{rel_path}' ({row_count} rows, {elapsed_ms}ms) -> {pipeline['target_catalog']}.{pipeline['target_schema']}.{pipeline['target_table']}")

        return {
            "status": "SUCCESS",
            "file_path": rel_path,
            "rows": row_count,
            "execution_ms": elapsed_ms
        }

    except Exception as write_err:
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
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


def run_pipeline_cycle(pipeline_id: str) -> Dict[str, Any]:
    """
    Executes one ingestion cycle for a pipeline.
    Scans the watched volume directory for new uningested files and loads them.
    """
    pipe = get_pipeline(pipeline_id)
    if not pipe:
        return {"error": "Pipeline not found"}

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
    last_run_map: Dict[str, float] = {}

    while True:
        try:
            pipelines = list_pipelines()
            now = time.time()

            for p in pipelines:
                p_id = p["id"]
                if not p.get("enabled", 1):
                    continue

                interval = max(5, int(p.get("poll_interval_seconds", 10)))
                last_time = last_run_map.get(p_id, 0.0)

                if (now - last_time) >= interval:
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
