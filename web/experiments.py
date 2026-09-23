import os
import time
import uuid
import json
import shutil
import sqlite3
import datetime
import re
import logging
from typing import Optional, Dict, Any, List, Union

logger = logging.getLogger("localspark.experiments")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
if not os.path.exists(WAREHOUSE_DIR):
    local_alt = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "warehouse"))
    if os.path.exists(local_alt):
        WAREHOUSE_DIR = local_alt

METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
DB_PATH = os.path.join(METADATA_DIR, "experiments.db")
ARTIFACTS_BASE_DIR = os.path.join(WAREHOUSE_DIR, "mlflow", "artifacts")


def get_exp_db() -> sqlite3.Connection:
    """Returns a SQLite connection to experiments.db with WAL mode enabled."""
    os.makedirs(METADATA_DIR, exist_ok=True)
    os.makedirs(ARTIFACTS_BASE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_experiments_db():
    """Initializes tables for experiments, runs, params, metrics, tags, and artifacts."""
    try:
        with get_exp_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS experiments (
                    experiment_id TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    artifact_location TEXT NOT NULL,
                    lifecycle_stage TEXT NOT NULL DEFAULT 'active',
                    user_id TEXT NOT NULL DEFAULT 'admin',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_exp_user ON experiments(user_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_exp_name ON experiments(name);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL,
                    run_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'RUNNING',
                    start_time INTEGER NOT NULL,
                    end_time INTEGER DEFAULT NULL,
                    duration_ms REAL DEFAULT 0.0,
                    user_id TEXT NOT NULL DEFAULT 'admin',
                    source_type TEXT NOT NULL DEFAULT 'NOTEBOOK',
                    source_name TEXT DEFAULT '',
                    lifecycle_stage TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id) ON DELETE CASCADE
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_exp ON runs(experiment_id, start_time DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_user ON runs(user_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS run_params (
                    run_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (run_id, key),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_params_key ON run_params(key);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS run_metrics (
                    run_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value REAL NOT NULL,
                    timestamp INTEGER NOT NULL,
                    step INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (run_id, key, step, timestamp),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_lookup ON run_metrics(run_id, key, step);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS run_tags (
                    run_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (run_id, key),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS run_artifacts (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    is_dir INTEGER NOT NULL DEFAULT 0,
                    file_size INTEGER NOT NULL DEFAULT 0,
                    file_type TEXT NOT NULL DEFAULT 'file',
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_run ON run_artifacts(run_id);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS experiment_tags (
                    experiment_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (experiment_id, key),
                    FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id) ON DELETE CASCADE
                );
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS run_inputs (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    dataset_name TEXT NOT NULL,
                    dataset_digest TEXT DEFAULT '',
                    dataset_source_type TEXT DEFAULT '',
                    dataset_source TEXT DEFAULT '',
                    dataset_schema TEXT DEFAULT '',
                    dataset_profile TEXT DEFAULT '',
                    tags TEXT DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_inputs_run ON run_inputs(run_id);")

            # -------------------------------------------------------------
            # GenAI & LLM Tracing (MLflow 2.14+ OpenTelemetry-compatible)
            # -------------------------------------------------------------
            conn.execute("""
                CREATE TABLE IF NOT EXISTS traces (
                    request_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL DEFAULT '0',
                    name TEXT NOT NULL DEFAULT 'trace',
                    timestamp_ms INTEGER NOT NULL,
                    execution_time_ms REAL DEFAULT 0.0,
                    status TEXT NOT NULL DEFAULT 'OK',
                    request TEXT DEFAULT '{}',
                    response TEXT DEFAULT '{}',
                    tags TEXT DEFAULT '{}',
                    total_tokens INTEGER DEFAULT 0,
                    prompt_tokens INTEGER DEFAULT 0,
                    completion_tokens INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id) ON DELETE CASCADE
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_traces_exp ON traces(experiment_id, timestamp_ms DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_traces_status ON traces(status);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS spans (
                    span_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    parent_id TEXT DEFAULT NULL,
                    name TEXT NOT NULL,
                    span_type TEXT NOT NULL DEFAULT 'UNKNOWN',
                    start_time_ns INTEGER NOT NULL,
                    end_time_ns INTEGER DEFAULT NULL,
                    duration_ms REAL DEFAULT 0.0,
                    status_code TEXT NOT NULL DEFAULT 'OK',
                    status_message TEXT DEFAULT '',
                    inputs TEXT DEFAULT '{}',
                    outputs TEXT DEFAULT '{}',
                    attributes TEXT DEFAULT '{}',
                    events TEXT DEFAULT '[]',
                    FOREIGN KEY (request_id) REFERENCES traces(request_id) ON DELETE CASCADE
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(request_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_spans_parent ON spans(parent_id);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS trace_assessments (
                    assessment_id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'HUMAN',
                    source_id TEXT DEFAULT 'admin',
                    name TEXT NOT NULL,
                    value TEXT NOT NULL,
                    rationale TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (trace_id) REFERENCES traces(request_id) ON DELETE CASCADE
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_assessments_trace ON trace_assessments(trace_id);")

            # Create default experiment if no experiments exist
            cur = conn.execute("SELECT COUNT(*) FROM experiments")
            if cur.fetchone()[0] == 0:
                now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                def_exp_id = "0"
                def_artifact = os.path.join(ARTIFACTS_BASE_DIR, def_exp_id)
                os.makedirs(def_artifact, exist_ok=True)
                conn.execute(
                    "INSERT INTO experiments (experiment_id, name, artifact_location, lifecycle_stage, user_id, created_at, updated_at) "
                    "VALUES (?, ?, ?, 'active', 'admin', ?, ?)",
                    (def_exp_id, "Default", def_artifact, now_str, now_str)
                )
                logger.info("Initialized default experiment '0'")

            # Check if default demo traces need seeding
            try:
                cur_traces = conn.execute("SELECT COUNT(*) FROM traces")
                if cur_traces.fetchone()[0] == 0:
                    seed_default_traces(conn)
            except Exception as e_seed:
                logger.debug(f"Trace seeding notice: {e_seed}")

    except Exception as e:
        logger.error(f"Failed to initialize experiments database: {e}", exc_info=True)


# =========================================================================
# MLflow 2.0 REST API Service Implementation
# =========================================================================

def mlflow_create_experiment(name: str, artifact_location: Optional[str] = None, user_id: str = "admin") -> Dict[str, Any]:
    name = (name or "").strip()
    if not name:
        raise ValueError("Experiment name cannot be empty")

    with get_exp_db() as conn:
        row = conn.execute("SELECT experiment_id FROM experiments WHERE name = ?", (name,)).fetchone()
        if row:
            raise ValueError(f"Experiment with name '{name}' already exists")

        exp_id = str(uuid.uuid4().hex[:8])
        if not artifact_location:
            artifact_location = os.path.join(ARTIFACTS_BASE_DIR, exp_id)
        os.makedirs(artifact_location, exist_ok=True)

        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO experiments (experiment_id, name, artifact_location, lifecycle_stage, user_id, created_at, updated_at) "
            "VALUES (?, ?, ?, 'active', ?, ?, ?)",
            (exp_id, name, artifact_location, user_id, now_str, now_str)
        )
        return {"experiment_id": exp_id}


def mlflow_get_experiment(experiment_id: str) -> Optional[Dict[str, Any]]:
    with get_exp_db() as conn:
        row = conn.execute(
            "SELECT * FROM experiments WHERE experiment_id = ? AND lifecycle_stage != 'deleted'",
            (str(experiment_id),)
        ).fetchone()
        if not row:
            return None
        tag_rows = conn.execute(
            "SELECT key, value FROM experiment_tags WHERE experiment_id = ? ORDER BY key ASC",
            (str(experiment_id),)
        ).fetchall()
        tags_list = [{"key": t["key"], "value": t["value"]} for t in tag_rows]
        tags_dict = {t["key"]: t["value"] for t in tag_rows}
        return {
            "experiment_id": row["experiment_id"],
            "name": row["name"],
            "artifact_location": row["artifact_location"],
            "lifecycle_stage": row["lifecycle_stage"],
            "user_id": row["user_id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "tags": tags_list,
            "tags_dict": tags_dict
        }


def mlflow_get_experiment_by_name(name: str) -> Optional[Dict[str, Any]]:
    with get_exp_db() as conn:
        row = conn.execute(
            "SELECT * FROM experiments WHERE name = ? AND lifecycle_stage != 'deleted'",
            (name.strip(),)
        ).fetchone()
        if not row:
            return None
        tag_rows = conn.execute(
            "SELECT key, value FROM experiment_tags WHERE experiment_id = ? ORDER BY key ASC",
            (row["experiment_id"],)
        ).fetchall()
        tags_list = [{"key": t["key"], "value": t["value"]} for t in tag_rows]
        tags_dict = {t["key"]: t["value"] for t in tag_rows}
        return {
            "experiment_id": row["experiment_id"],
            "name": row["name"],
            "artifact_location": row["artifact_location"],
            "lifecycle_stage": row["lifecycle_stage"],
            "user_id": row["user_id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "tags": tags_list,
            "tags_dict": tags_dict
        }


def mlflow_list_experiments(view_type: str = "ACTIVE_ONLY", user_id: Optional[str] = None, is_admin: bool = True) -> List[Dict[str, Any]]:
    stage_filter = "lifecycle_stage = 'active'" if view_type != "ALL" else "1=1"
    with get_exp_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM experiments WHERE {stage_filter} ORDER BY created_at ASC"
        ).fetchall()
        result = []
        for r in rows:
            exp_user = (r["user_id"] or "").strip().lower()
            u_filter = (user_id or "").strip().lower()
            is_owner = bool(u_filter and exp_user == u_filter)
            is_shared = exp_user in ("0", "default", "shared", "admin")
            if not is_admin and u_filter and not (is_owner or is_shared):
                continue

            c_runs = conn.execute(
                "SELECT COUNT(*) FROM runs WHERE experiment_id = ? AND lifecycle_stage != 'deleted'",
                (r["experiment_id"],)
            ).fetchone()[0]
            tag_rows = conn.execute(
                "SELECT key, value FROM experiment_tags WHERE experiment_id = ? ORDER BY key ASC",
                (r["experiment_id"],)
            ).fetchall()
            tags_list = [{"key": t["key"], "value": t["value"]} for t in tag_rows]
            result.append({
                "experiment_id": r["experiment_id"],
                "name": r["name"],
                "artifact_location": r["artifact_location"],
                "lifecycle_stage": r["lifecycle_stage"],
                "user_id": r["user_id"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "runs_count": c_runs,
                "tags": tags_list,
                "is_owner": is_owner
            })
        return result


def mlflow_update_experiment(experiment_id: str, new_name: str) -> Dict[str, Any]:
    new_name = (new_name or "").strip()
    if not new_name:
        raise ValueError("New experiment name cannot be empty")
    with get_exp_db() as conn:
        existing = conn.execute(
            "SELECT experiment_id FROM experiments WHERE name = ? AND experiment_id != ?",
            (new_name, str(experiment_id))
        ).fetchone()
        if existing:
            raise ValueError(f"Experiment with name '{new_name}' already exists")
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = conn.execute(
            "UPDATE experiments SET name = ?, updated_at = ? WHERE experiment_id = ?",
            (new_name, now_str, str(experiment_id))
        )
        if cur.rowcount == 0:
            raise ValueError(f"Experiment '{experiment_id}' not found")
    return {}


def mlflow_restore_experiment(experiment_id: str) -> Dict[str, Any]:
    with get_exp_db() as conn:
        cur = conn.execute(
            "UPDATE experiments SET lifecycle_stage = 'active' WHERE experiment_id = ?",
            (str(experiment_id),)
        )
        if cur.rowcount == 0:
            raise ValueError(f"Experiment '{experiment_id}' not found")
        conn.execute(
            "UPDATE runs SET lifecycle_stage = 'active' WHERE experiment_id = ?",
            (str(experiment_id),)
        )
    return {}


def mlflow_set_experiment_tag(experiment_id: str, key: str, value: str) -> Dict[str, Any]:
    key = str(key).strip()
    if not key:
        raise ValueError("Experiment tag key cannot be empty")
    with get_exp_db() as conn:
        # Check experiment exists
        exp = conn.execute("SELECT experiment_id FROM experiments WHERE experiment_id = ?", (str(experiment_id),)).fetchone()
        if not exp:
            raise ValueError(f"Experiment '{experiment_id}' not found")
        conn.execute(
            "INSERT OR REPLACE INTO experiment_tags (experiment_id, key, value) VALUES (?, ?, ?)",
            (str(experiment_id), key, str(value))
        )
    return {}


def mlflow_delete_experiment(experiment_id: str):
    with get_exp_db() as conn:
        conn.execute(
            "UPDATE experiments SET lifecycle_stage = 'deleted' WHERE experiment_id = ?",
            (str(experiment_id),)
        )
        conn.execute(
            "UPDATE runs SET lifecycle_stage = 'deleted' WHERE experiment_id = ?",
            (str(experiment_id),)
        )


def mlflow_create_run(
    experiment_id: str,
    run_name: Optional[str] = None,
    start_time: Optional[int] = None,
    user_id: str = "admin",
    tags: Optional[List[Dict[str, str]]] = None,
    source_type: str = "NOTEBOOK",
    source_name: str = ""
) -> Dict[str, Any]:
    exp = mlflow_get_experiment(experiment_id)
    if not exp:
        raise ValueError(f"Experiment '{experiment_id}' does not exist")

    run_id = uuid.uuid4().hex
    if not run_name:
        run_name = f"run_{run_id[:7]}"
    if start_time is None:
        start_time = int(time.time() * 1000)

    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_exp_db() as conn:
        conn.execute("""
            INSERT INTO runs (run_id, experiment_id, run_name, status, start_time, duration_ms, user_id, source_type, source_name, lifecycle_stage, created_at)
            VALUES (?, ?, ?, 'RUNNING', ?, 0.0, ?, ?, ?, 'active', ?)
        """, (run_id, experiment_id, run_name, start_time, user_id, source_type, source_name, now_str))

        if tags:
            for t in tags:
                k = (t.get("key") or "").strip()
                v = str(t.get("value") or "")
                if k:
                    conn.execute(
                        "INSERT OR REPLACE INTO run_tags (run_id, key, value) VALUES (?, ?, ?)",
                        (run_id, k, v)
                    )

    return mlflow_get_run(run_id)


def mlflow_update_run(
    run_id: str,
    status: str = "FINISHED",
    end_time: Optional[int] = None
) -> Dict[str, Any]:
    if end_time is None:
        end_time = int(time.time() * 1000)

    with get_exp_db() as conn:
        r = conn.execute("SELECT start_time FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if not r:
            raise ValueError(f"Run '{run_id}' not found")

        start_time = r["start_time"]
        duration_ms = max(0.0, float(end_time - start_time))

        conn.execute("""
            UPDATE runs SET status = ?, end_time = ?, duration_ms = ? WHERE run_id = ?
        """, (status.upper(), end_time, duration_ms, run_id))

    return mlflow_get_run(run_id)


def mlflow_delete_run(run_id: str):
    with get_exp_db() as conn:
        conn.execute("UPDATE runs SET lifecycle_stage = 'deleted' WHERE run_id = ?", (run_id,))


def mlflow_restore_run(run_id: str) -> Dict[str, Any]:
    with get_exp_db() as conn:
        cur = conn.execute("UPDATE runs SET lifecycle_stage = 'active' WHERE run_id = ?", (run_id,))
        if cur.rowcount == 0:
            raise ValueError(f"Run '{run_id}' not found")
    return {}


def mlflow_delete_tag(run_id: str, key: str) -> Dict[str, Any]:
    key = str(key).strip()
    with get_exp_db() as conn:
        conn.execute("DELETE FROM run_tags WHERE run_id = ? AND key = ?", (run_id, key))
    return {}


def mlflow_log_inputs(run_id: str, datasets: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not datasets:
        return {}
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_exp_db() as conn:
        for ds_entry in datasets:
            ds = ds_entry.get("dataset", {})
            tags = ds_entry.get("tags", [])
            inp_id = uuid.uuid4().hex
            conn.execute("""
                INSERT INTO run_inputs (id, run_id, dataset_name, dataset_digest, dataset_source_type, dataset_source, dataset_schema, dataset_profile, tags, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                inp_id,
                run_id,
                str(ds.get("name", "dataset")),
                str(ds.get("digest", "")),
                str(ds.get("source_type", "")),
                str(ds.get("source", "")),
                json.dumps(ds.get("schema", {})),
                json.dumps(ds.get("profile", {})),
                json.dumps(tags),
                now_str
            ))
            # Also register lineage tags for UI compatibility
            if ds.get("name"):
                conn.execute("INSERT OR REPLACE INTO run_tags (run_id, key, value) VALUES (?, 'mlflow.data.name', ?)", (run_id, str(ds["name"])))
            if ds.get("source"):
                conn.execute("INSERT OR REPLACE INTO run_tags (run_id, key, value) VALUES (?, 'mlflow.data.source', ?)", (run_id, str(ds["source"])))
            if isinstance(ds.get("profile"), dict):
                prof = ds["profile"]
                if "num_rows" in prof:
                    conn.execute("INSERT OR REPLACE INTO run_tags (run_id, key, value) VALUES (?, 'data_num_samples', ?)", (run_id, str(prof["num_rows"])))
                if "num_columns" in prof:
                    conn.execute("INSERT OR REPLACE INTO run_tags (run_id, key, value) VALUES (?, 'data_num_features', ?)", (run_id, str(prof["num_columns"])))
    return {}


def mlflow_log_param(run_id: str, key: str, value: Any):
    key = str(key).strip()
    val_str = str(value)
    if not key:
        raise ValueError("Param key cannot be empty")

    with get_exp_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO run_params (run_id, key, value) VALUES (?, ?, ?)
        """, (run_id, key, val_str))


def mlflow_log_metric(run_id: str, key: str, value: float, timestamp: Optional[int] = None, step: int = 0):
    key = str(key).strip()
    if not key:
        raise ValueError("Metric key cannot be empty")
    if timestamp is None:
        timestamp = int(time.time() * 1000)

    with get_exp_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO run_metrics (run_id, key, value, timestamp, step) VALUES (?, ?, ?, ?, ?)
        """, (run_id, key, float(value), timestamp, int(step)))


def mlflow_set_tag(run_id: str, key: str, value: str):
    key = str(key).strip()
    if not key:
        raise ValueError("Tag key cannot be empty")

    with get_exp_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO run_tags (run_id, key, value) VALUES (?, ?, ?)
        """, (run_id, key, str(value)))


def mlflow_log_batch(
    run_id: str,
    metrics: Optional[List[Dict[str, Any]]] = None,
    params: Optional[List[Dict[str, Any]]] = None,
    tags: Optional[List[Dict[str, Any]]] = None
):
    with get_exp_db() as conn:
        if params:
            for p in params:
                k = (p.get("key") or "").strip()
                if k:
                    conn.execute(
                        "INSERT OR REPLACE INTO run_params (run_id, key, value) VALUES (?, ?, ?)",
                        (run_id, k, str(p.get("value", "")))
                    )
        if metrics:
            now_ms = int(time.time() * 1000)
            for m in metrics:
                k = (m.get("key") or "").strip()
                if k:
                    val = float(m.get("value", 0.0))
                    ts = m.get("timestamp", now_ms)
                    step = int(m.get("step", 0))
                    conn.execute(
                        "INSERT OR REPLACE INTO run_metrics (run_id, key, value, timestamp, step) VALUES (?, ?, ?, ?, ?)",
                        (run_id, k, val, ts, step)
                    )
        if tags:
            for t in tags:
                k = (t.get("key") or "").strip()
                if k:
                    conn.execute(
                        "INSERT OR REPLACE INTO run_tags (run_id, key, value) VALUES (?, ?, ?)",
                        (run_id, k, str(t.get("value", "")))
                    )


def _normalize_fs_path(p: Optional[str]) -> Optional[str]:
    """Translates path between host filesystem and docker container mount if needed."""
    if not p:
        return p
    if os.path.exists(p):
        return p
    HOST_PREFIX = "/home/martin/volumes/datakilnworks"
    CONTAINER_PREFIX = "/workspace"
    if p.startswith(HOST_PREFIX) and os.path.exists(CONTAINER_PREFIX):
        translated = p.replace(HOST_PREFIX, CONTAINER_PREFIX, 1)
        if os.path.exists(translated):
            return translated
        return translated
    elif p.startswith(CONTAINER_PREFIX) and os.path.exists(HOST_PREFIX):
        translated = p.replace(CONTAINER_PREFIX, HOST_PREFIX, 1)
        if os.path.exists(translated):
            return translated
        return translated
    return p


def mlflow_log_artifact(
    run_id: str,
    local_file: str,
    artifact_path: Optional[str] = None
) -> Dict[str, Any]:
    """Logs a local file or directory as an artifact for the specified run."""
    with get_exp_db() as conn:
        r = conn.execute("""
            SELECT r.experiment_id, e.artifact_location
            FROM runs r
            JOIN experiments e ON r.experiment_id = e.experiment_id
            WHERE r.run_id = ?
        """, (run_id,)).fetchone()
        if not r:
            raise ValueError(f"Run '{run_id}' not found")
        exp_id = r["experiment_id"]
        custom_loc = _normalize_fs_path(r["artifact_location"])

        if custom_loc and os.path.isabs(custom_loc):
            run_artifact_root = os.path.join(custom_loc, run_id, "artifacts")
        else:
            base_dir = _normalize_fs_path(ARTIFACTS_BASE_DIR) or ARTIFACTS_BASE_DIR
            run_artifact_root = os.path.join(base_dir, exp_id, run_id)

        if artifact_path:
            target_dir = os.path.join(run_artifact_root, artifact_path.strip("/\\"))
        else:
            target_dir = run_artifact_root

        os.makedirs(target_dir, exist_ok=True)

        if os.path.isdir(local_file):
            dir_name = os.path.basename(local_file.rstrip("/\\"))
            dest_dir = os.path.join(target_dir, dir_name)
            if os.path.exists(dest_dir):
                shutil.rmtree(dest_dir)
            shutil.copytree(local_file, dest_dir)
            for root, _, files in os.walk(dest_dir):
                for f in files:
                    full_f = os.path.join(root, f)
                    rel_p = os.path.relpath(full_f, run_artifact_root)
                    f_size = os.path.getsize(full_f)
                    f_ext = os.path.splitext(f)[1].lstrip(".").lower() or "file"
                    art_id = uuid.uuid4().hex[:12]
                    conn.execute("""
                        INSERT OR REPLACE INTO run_artifacts (id, run_id, path, is_dir, file_size, file_type)
                        VALUES (?, ?, ?, 0, ?, ?)
                    """, (art_id, run_id, rel_p, f_size, f_ext))
            return {"status": "SUCCESS", "path": artifact_path or dir_name}
        else:
            filename = os.path.basename(local_file)
            dest_file = os.path.join(target_dir, filename)
            shutil.copy2(local_file, dest_file)
            rel_p = os.path.join(artifact_path, filename) if artifact_path else filename
            f_size = os.path.getsize(dest_file)
            f_ext = os.path.splitext(filename)[1].lstrip(".").lower() or "file"
            art_id = uuid.uuid4().hex[:12]
            conn.execute("""
                INSERT OR REPLACE INTO run_artifacts (id, run_id, path, is_dir, file_size, file_type)
                VALUES (?, ?, ?, 0, ?, ?)
            """, (art_id, run_id, rel_p, f_size, f_ext))
            return {"status": "SUCCESS", "path": rel_p, "file_size": f_size, "file_type": f_ext}


def mlflow_log_dict(run_id: str, dictionary: Dict[str, Any], artifact_file: str) -> Dict[str, Any]:
    """Writes dictionary as JSON and records as artifact for the run."""
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
        json.dump(dictionary, tf, indent=2)
        tmp_name = tf.name
    try:
        dirname = os.path.dirname(artifact_file)
        basename = os.path.basename(artifact_file)
        renamed_tmp = os.path.join(os.path.dirname(tmp_name), basename)
        shutil.move(tmp_name, renamed_tmp)
        return mlflow_log_artifact(run_id, renamed_tmp, artifact_path=dirname or None)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except Exception:
                pass


def mlflow_get_artifact_path(run_id: str, artifact_path: Optional[str] = "") -> Optional[str]:
    """Returns absolute on-disk path for a run artifact, verifying safety."""
    with get_exp_db() as conn:
        r = conn.execute("""
            SELECT r.experiment_id, e.artifact_location
            FROM runs r
            JOIN experiments e ON r.experiment_id = e.experiment_id
            WHERE r.run_id = ?
        """, (run_id,)).fetchone()
        if not r:
            return None
        exp_id = r["experiment_id"]
        custom_loc = r["artifact_location"]

        candidate_roots = []
        if custom_loc and os.path.isabs(custom_loc):
            norm_custom = _normalize_fs_path(custom_loc)
            if norm_custom:
                candidate_roots.append(os.path.abspath(os.path.join(norm_custom, run_id, "artifacts")))
                candidate_roots.append(os.path.abspath(norm_custom))
            if norm_custom != custom_loc:
                candidate_roots.append(os.path.abspath(os.path.join(custom_loc, run_id, "artifacts")))
                candidate_roots.append(os.path.abspath(custom_loc))

        norm_base = _normalize_fs_path(ARTIFACTS_BASE_DIR)
        if norm_base:
            candidate_roots.append(os.path.abspath(os.path.join(norm_base, exp_id, run_id)))
        if norm_base != ARTIFACTS_BASE_DIR:
            candidate_roots.append(os.path.abspath(os.path.join(ARTIFACTS_BASE_DIR, exp_id, run_id)))

        clean_sub = artifact_path.lstrip("/\\") if artifact_path else ""
        for root in candidate_roots:
            target_path = os.path.abspath(os.path.join(root, clean_sub)) if clean_sub else root
            norm_target = _normalize_fs_path(target_path)
            if norm_target and os.path.exists(norm_target):
                return norm_target

        fallback = os.path.join(candidate_roots[0], clean_sub) if clean_sub else candidate_roots[0]
        return _normalize_fs_path(fallback)


def mlflow_get_artifact_content(run_id: str, artifact_path: str) -> Optional[Dict[str, Any]]:
    """Returns content preview of a run artifact if readable text or JSON."""
    file_path = mlflow_get_artifact_path(run_id, artifact_path)
    if not file_path or not os.path.isfile(file_path):
        return None
    file_size = os.path.getsize(file_path)
    ext = os.path.splitext(file_path)[1].lstrip(".").lower()

    # If larger than 2MB, indicate binary/large
    if file_size > 2 * 1024 * 1024:
        return {"path": artifact_path, "size": file_size, "type": ext, "content": "[File size exceeds preview threshold]"}

    if ext in ["png", "jpg", "jpeg", "webp", "gif", "svg"]:
        import base64
        try:
            with open(file_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("utf-8")
            mime = f"image/{'svg+xml' if ext == 'svg' else ext}"
            return {"path": artifact_path, "size": file_size, "type": ext, "is_image": True, "image_data": f"data:{mime};base64,{b64}"}
        except Exception as e:
            return {"path": artifact_path, "size": file_size, "type": ext, "error": str(e)}

    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            raw_text = f.read()
        if ext == "json":
            try:
                parsed = json.loads(raw_text)
                return {"path": artifact_path, "size": file_size, "type": "json", "json_data": parsed, "content": raw_text}
            except Exception:
                pass
        return {"path": artifact_path, "size": file_size, "type": ext, "content": raw_text}
    except Exception as e:
        return {"path": artifact_path, "size": file_size, "type": ext, "error": str(e)}


def mlflow_list_artifacts(run_id: str, path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Lists artifacts for a run. If path is provided, lists items within that directory.
    Identifies subdirectories with is_dir: true, matching Databricks MLflow specification.
    """
    with get_exp_db() as conn:
        rows = conn.execute(
            "SELECT path, file_size, file_type FROM run_artifacts WHERE run_id = ? ORDER BY path ASC",
            (run_id,)
        ).fetchall()
        all_items = [dict(r) for r in rows]

        if not path or path.strip() in ("", "/", "."):
            results = []
            seen_dirs = set()
            for it in all_items:
                p = it["path"].strip("/\\").replace("\\", "/")
                if "/" in p:
                    root_dir = p.split("/")[0]
                    if root_dir not in seen_dirs:
                        seen_dirs.add(root_dir)
                        results.append({
                            "path": root_dir,
                            "is_dir": True,
                            "file_size": None
                        })
                else:
                    results.append({
                        "path": p,
                        "is_dir": False,
                        "file_size": it.get("file_size", 0),
                        "file_type": it.get("file_type", "")
                    })
            return results
        else:
            clean_prefix = path.strip("/\\").replace("\\", "/") + "/"
            results = []
            seen_dirs = set()
            for it in all_items:
                norm_p = it["path"].strip("/\\").replace("\\", "/")
                if norm_p.startswith(clean_prefix):
                    rel = norm_p[len(clean_prefix):]
                    if "/" in rel:
                        subdir = rel.split("/")[0]
                        sub_path = clean_prefix + subdir
                        if sub_path not in seen_dirs:
                            seen_dirs.add(sub_path)
                            results.append({
                                "path": sub_path,
                                "is_dir": True,
                                "file_size": None
                            })
                    else:
                        results.append({
                            "path": norm_p,
                            "is_dir": False,
                            "file_size": it.get("file_size", 0),
                            "file_type": it.get("file_type", "")
                        })
            return results


def mlflow_get_run(run_id: str) -> Optional[Dict[str, Any]]:
    with get_exp_db() as conn:
        r = conn.execute(
            "SELECT * FROM runs WHERE run_id = ? AND lifecycle_stage != 'deleted'",
            (run_id,)
        ).fetchone()
        if not r:
            return None

        # Fetch params
        params_rows = conn.execute(
            "SELECT key, value FROM run_params WHERE run_id = ? ORDER BY key ASC",
            (run_id,)
        ).fetchall()
        params_dict = {p["key"]: p["value"] for p in params_rows}
        params_list = [{"key": p["key"], "value": p["value"]} for p in params_rows]

        # Fetch latest metrics (group by key, highest step/timestamp)
        metrics_rows = conn.execute("""
            SELECT m.key, m.value, m.step, m.timestamp
            FROM run_metrics m
            INNER JOIN (
                SELECT key, MAX(step) as max_step, MAX(timestamp) as max_ts
                FROM run_metrics
                WHERE run_id = ?
                GROUP BY key
            ) latest ON m.key = latest.key AND m.step = latest.max_step
            WHERE m.run_id = ?
            ORDER BY m.key ASC
        """, (run_id, run_id)).fetchall()

        latest_metrics_dict = {m["key"]: round(float(m["value"]), 4) for m in metrics_rows}
        metrics_list = [{
            "key": m["key"],
            "value": float(m["value"]),
            "step": m["step"],
            "timestamp": m["timestamp"]
        } for m in metrics_rows]

        # Fetch tags
        tags_rows = conn.execute(
            "SELECT key, value FROM run_tags WHERE run_id = ? ORDER BY key ASC",
            (run_id,)
        ).fetchall()
        tags_dict = {t["key"]: t["value"] for t in tags_rows}
        tags_list = [{"key": t["key"], "value": t["value"]} for t in tags_rows]

        # Artifacts
        artifacts_rows = conn.execute(
            "SELECT path, file_size, file_type FROM run_artifacts WHERE run_id = ?",
            (run_id,)
        ).fetchall()
        artifacts_list = [{
            "path": a["path"],
            "file_size": a["file_size"],
            "file_type": a["file_type"]
        } for a in artifacts_rows]

        # Inputs (datasets)
        inputs_rows = conn.execute(
            "SELECT dataset_name, dataset_digest, dataset_source_type, dataset_source, dataset_schema, dataset_profile, tags FROM run_inputs WHERE run_id = ?",
            (run_id,)
        ).fetchall()
        dataset_inputs = []
        for inp in inputs_rows:
            try:
                sch = json.loads(inp["dataset_schema"])
            except Exception:
                sch = inp["dataset_schema"]
            try:
                prof = json.loads(inp["dataset_profile"])
            except Exception:
                prof = inp["dataset_profile"]
            try:
                tgs = json.loads(inp["tags"])
            except Exception:
                tgs = []
            dataset_inputs.append({
                "dataset": {
                    "name": inp["dataset_name"],
                    "digest": inp["dataset_digest"],
                    "source_type": inp["dataset_source_type"],
                    "source": inp["dataset_source"],
                    "schema": sch,
                    "profile": prof
                },
                "tags": tgs
            })

        exp = mlflow_get_experiment(r["experiment_id"])
        exp_name = exp["name"] if exp else r["experiment_id"]

        run_info = {
            "run_id": r["run_id"],
            "run_uuid": r["run_id"],
            "run_name": r["run_name"],
            "experiment_id": r["experiment_id"],
            "experiment_name": exp_name,
            "status": r["status"],
            "start_time": r["start_time"],
            "end_time": r["end_time"],
            "duration_ms": r["duration_ms"],
            "duration_sec": round((r["duration_ms"] or 0) / 1000.0, 2),
            "user_id": r["user_id"],
            "source_type": r["source_type"],
            "source_name": r["source_name"],
            "lifecycle_stage": r["lifecycle_stage"],
            "created_at": r["created_at"],
            "artifact_uri": os.path.join(ARTIFACTS_BASE_DIR, r["experiment_id"], r["run_id"])
        }

        return {
            "info": run_info,
            "data": {
                "params": params_list,
                "metrics": metrics_list,
                "tags": tags_list,
                "inputs": {"dataset_inputs": dataset_inputs}
            },
            # Convenience maps for Studio UI:
            "params_dict": params_dict,
            "metrics_dict": latest_metrics_dict,
            "tags_dict": tags_dict,
            "artifacts": artifacts_list,
            "inputs": dataset_inputs
        }


def _evaluate_single_filter_clause(run: Dict[str, Any], clause: str) -> bool:
    clause = clause.strip()
    if not clause:
        return True

    m = re.match(r"""^(?P<key>[a-zA-Z0-9_."' -]+?)\s*(?P<op>=|!=|<=|>=|<|>|(?i:like)|(?i:ilike))\s*(?P<val>.+)$""", clause)
    if not m:
        return True
    raw_key = m.group("key").strip()
    op = m.group("op").upper()
    raw_val = m.group("val").strip()

    if (raw_val.startswith("'") and raw_val.endswith("'")) or (raw_val.startswith('"') and raw_val.endswith('"')):
        target_val = raw_val[1:-1]
    else:
        target_val = raw_val

    entity = None
    sub_key = raw_key
    for prefix in ["metrics.", "metric.", "params.", "param.", "tags.", "tag.", "attributes.", "attribute."]:
        if raw_key.lower().startswith(prefix):
            entity = prefix.rstrip(".").lower()
            if not entity.endswith("s"):
                entity += "s"
            sub_key = raw_key[len(prefix):].strip()
            break

    if (sub_key.startswith("'") and sub_key.endswith("'")) or (sub_key.startswith('"') and sub_key.endswith('"')):
        sub_key = sub_key[1:-1]

    info = run.get("info", {})
    metrics = run.get("metrics_dict", {})
    params = run.get("params_dict", {})
    tags = run.get("tags_dict", {})

    actual_val = None
    is_numeric = False

    if entity == "metrics":
        if sub_key in metrics:
            actual_val = float(metrics[sub_key])
            is_numeric = True
        else:
            return False
    elif entity == "params":
        if sub_key in params:
            actual_val = str(params[sub_key])
        else:
            return False
    elif entity == "tags":
        if sub_key in tags:
            actual_val = str(tags[sub_key])
        else:
            return False
    elif entity == "attributes":
        if sub_key in info:
            actual_val = info[sub_key]
            if sub_key in ("start_time", "end_time", "duration_ms"):
                try:
                    actual_val = float(actual_val)
                    is_numeric = True
                except Exception:
                    pass
            else:
                actual_val = str(actual_val)
        else:
            return False
    else:
        if sub_key in metrics:
            actual_val = float(metrics[sub_key])
            is_numeric = True
        elif sub_key in params:
            actual_val = str(params[sub_key])
        elif sub_key in tags:
            actual_val = str(tags[sub_key])
        elif sub_key in info:
            actual_val = info[sub_key]
            if sub_key in ("start_time", "end_time", "duration_ms"):
                try:
                    actual_val = float(actual_val)
                    is_numeric = True
                except Exception:
                    pass
            else:
                actual_val = str(actual_val)
        else:
            return False

    if is_numeric:
        try:
            target_num = float(target_val)
            if op == "=": return actual_val == target_num
            if op == "!=": return actual_val != target_num
            if op == "<": return actual_val < target_num
            if op == "<=": return actual_val <= target_num
            if op == ">": return actual_val > target_num
            if op == ">=": return actual_val >= target_num
        except Exception:
            return False
    else:
        actual_str = str(actual_val)
        target_str = str(target_val)
        if op == "=": return actual_str.lower() == target_str.lower()
        if op == "!=": return actual_str.lower() != target_str.lower()
        if op in ("LIKE", "ILIKE"):
            pattern = re.escape(target_str).replace(r"\%", ".*").replace(r"\_", ".")
            return bool(re.match(f"^{pattern}$", actual_str, re.IGNORECASE))
        if op == "<": return actual_str < target_str
        if op == "<=": return actual_str <= target_str
        if op == ">": return actual_str > target_str
        if op == ">=": return actual_str >= target_str

    return True


def _matches_filter(run: Dict[str, Any], filter_string: Optional[str]) -> bool:
    if not filter_string or not filter_string.strip():
        return True
    clauses = re.split(r"(?i)\s+and\s+", filter_string.strip())
    for c in clauses:
        if not _evaluate_single_filter_clause(run, c):
            return False
    return True


def _sort_runs(runs: List[Dict[str, Any]], order_by: Optional[List[str]]) -> List[Dict[str, Any]]:
    if not order_by:
        return runs
    for order_clause in reversed(order_by):
        clause = order_clause.strip()
        desc = False
        if clause.upper().endswith(" DESC"):
            desc = True
            key_expr = clause[:-5].strip()
        elif clause.upper().endswith(" ASC"):
            key_expr = clause[:-4].strip()
        else:
            key_expr = clause

        for prefix in ["metrics.", "params.", "tags.", "attributes."]:
            if key_expr.lower().startswith(prefix):
                key_expr = key_expr[len(prefix):]
                break
        key_expr = key_expr.strip("'\"")

        def sort_key(r):
            info = r.get("info", {})
            metrics = r.get("metrics_dict", {})
            params = r.get("params_dict", {})
            tags = r.get("tags_dict", {})
            if key_expr in metrics:
                return (1, float(metrics[key_expr]))
            if key_expr in info:
                val = info[key_expr]
                try:
                    return (1, float(val))
                except Exception:
                    return (0, str(val))
            if key_expr in params:
                return (0, str(params[key_expr]))
            if key_expr in tags:
                return (0, str(tags[key_expr]))
            return (-1, 0)

        runs.sort(key=sort_key, reverse=desc)
    return runs


def mlflow_search_runs(
    experiment_ids: List[str],
    filter_string: Optional[str] = None,
    order_by: Optional[List[str]] = None,
    max_results: int = 100
) -> List[Dict[str, Any]]:
    if not experiment_ids:
        return []

    placeholders = ",".join(["?"] * len(experiment_ids))
    with get_exp_db() as conn:
        query = f"""
            SELECT run_id FROM runs
            WHERE experiment_id IN ({placeholders}) AND lifecycle_stage != 'deleted'
            ORDER BY start_time DESC
        """
        params = list(map(str, experiment_ids))
        rows = conn.execute(query, params).fetchall()

        matched = []
        for r in rows:
            run_data = mlflow_get_run(r["run_id"])
            if run_data and _matches_filter(run_data, filter_string):
                matched.append(run_data)

        if order_by:
            matched = _sort_runs(matched, order_by)

        return matched[:max_results]


def mlflow_get_metric_history(run_id: str, metric_key: str) -> List[Dict[str, Any]]:
    with get_exp_db() as conn:
        rows = conn.execute("""
            SELECT key, value, timestamp, step
            FROM run_metrics
            WHERE run_id = ? AND key = ?
            ORDER BY step ASC, timestamp ASC
        """, (run_id, metric_key)).fetchall()

        return [{
            "key": r["key"],
            "value": float(r["value"]),
            "timestamp": r["timestamp"],
            "step": r["step"]
        } for r in rows]


# =========================================================================
# Studio UI Convenience & Comparison Methods
# =========================================================================

def get_experiments_summary() -> Dict[str, Any]:
    """Returns high-level statistics for Studio UI."""
    with get_exp_db() as conn:
        total_exp = conn.execute("SELECT COUNT(*) FROM experiments WHERE lifecycle_stage != 'deleted'").fetchone()[0]
        total_runs = conn.execute("SELECT COUNT(*) FROM runs WHERE lifecycle_stage != 'deleted'").fetchone()[0]
        active_runs = conn.execute("SELECT COUNT(*) FROM runs WHERE status = 'RUNNING' AND lifecycle_stage != 'deleted'").fetchone()[0]
        finished_runs = conn.execute("SELECT COUNT(*) FROM runs WHERE status = 'FINISHED' AND lifecycle_stage != 'deleted'").fetchone()[0]

        # Top metric keys across all runs
        top_metrics = conn.execute("""
            SELECT key, COUNT(DISTINCT run_id) as c
            FROM run_metrics
            GROUP BY key
            ORDER BY c DESC LIMIT 6
        """).fetchall()

        recent_runs_rows = conn.execute("""
            SELECT r.run_id, r.run_name, r.experiment_id, r.status, r.duration_ms, r.created_at, e.name as experiment_name
            FROM runs r
            JOIN experiments e ON r.experiment_id = e.experiment_id
            WHERE r.lifecycle_stage != 'deleted'
            ORDER BY r.start_time DESC LIMIT 5
        """).fetchall()

        recent_runs = []
        for r in recent_runs_rows:
            recent_runs.append({
                "run_id": r["run_id"],
                "run_name": r["run_name"],
                "experiment_id": r["experiment_id"],
                "experiment_name": r["experiment_name"],
                "status": r["status"],
                "duration_sec": round((r["duration_ms"] or 0) / 1000.0, 2),
                "created_at": r["created_at"]
            })

        return {
            "total_experiments": total_exp,
            "total_runs": total_runs,
            "active_runs": active_runs,
            "finished_runs": finished_runs,
            "popular_metrics": [m["key"] for m in top_metrics],
            "recent_runs": recent_runs
        }


def compare_runs(run_ids: List[str]) -> Dict[str, Any]:
    """Compares multiple runs, computing parameter diffs and metric variations."""
    if not run_ids:
        return {"runs": [], "common_params": {}, "diff_params": {}, "metrics_comparison": {}, "step_metrics": {}}

    runs_data = []
    all_param_keys = set()
    all_metric_keys = set()

    for rid in run_ids:
        r = mlflow_get_run(rid)
        if r:
            runs_data.append(r)
            all_param_keys.update(r["params_dict"].keys())
            all_metric_keys.update(r["metrics_dict"].keys())

    if not runs_data:
        return {"runs": [], "common_params": {}, "diff_params": {}, "metrics_comparison": {}, "step_metrics": {}}

    common_params = {}
    diff_params = {}

    for k in sorted(all_param_keys):
        vals = [r["params_dict"].get(k, "-") for r in runs_data]
        if all(v == vals[0] for v in vals):
            common_params[k] = vals[0]
        else:
            diff_params[k] = vals

    # Compare metrics: compute values, best, and delta percentage
    metrics_comparison = {}
    for k in sorted(all_metric_keys):
        vals = [r["metrics_dict"].get(k, None) for r in runs_data]
        valid_vals = [v for v in vals if v is not None]
        max_val = max(valid_vals) if valid_vals else None
        min_val = min(valid_vals) if valid_vals else None
        metrics_comparison[k] = {
            "values": vals,
            "max": max_val,
            "min": min_val
        }

    # Detect numeric vs categorical parameters
    numeric_params = []
    categorical_params = []
    for k in sorted(all_param_keys):
        is_num = True
        has_val = False
        for r in runs_data:
            val = r["params_dict"].get(k)
            if val is not None and val != "-" and val != "":
                has_val = True
                try:
                    float(val)
                except (ValueError, TypeError):
                    is_num = False
                    break
        if has_val and is_num:
            numeric_params.append(k)
        elif has_val:
            categorical_params.append(k)

    # Fetch time-series for popular loss/eval curves (up to 6 metrics)
    step_metrics = {}
    candidate_curve_keys = [k for k in all_metric_keys if any(term in k.lower() for term in ["loss", "accuracy", "error", "auc", "f1", "score"])]
    for mk in candidate_curve_keys[:6]:
        curves = {}
        for r in runs_data:
            rid = r["info"]["run_id"]
            history = mlflow_get_metric_history(rid, mk)
            if history:
                curves[rid] = {
                    "run_name": r["info"]["run_name"],
                    "points": [{"step": pt["step"], "value": pt["value"]} for pt in history]
                }
        if curves:
            step_metrics[mk] = curves

    return {
        "runs": [{
            "run_id": r["info"]["run_id"],
            "run_name": r["info"]["run_name"],
            "status": r["info"]["status"],
            "duration_sec": r["info"]["duration_sec"],
            "experiment_name": r["info"]["experiment_name"],
            "created_at": r["info"]["created_at"],
            "params": r["params_dict"],
            "metrics": r["metrics_dict"],
            "tags": r.get("tags_dict", {})
        } for r in runs_data],
        "common_params": common_params,
        "diff_params": diff_params,
        "metrics_comparison": metrics_comparison,
        "step_metrics": step_metrics,
        "numeric_params": numeric_params,
        "categorical_params": categorical_params,
        "numeric_metrics": sorted(list(all_metric_keys))
    }


def seed_demo_experiments() -> Dict[str, Any]:
    """Generates realistic demo MLflow experiments and training runs."""
    init_experiments_db()

    # 1. Experiment: Customer Churn Classification
    exp_churn_name = "customer_churn_prediction"
    exp_churn = mlflow_get_experiment_by_name(exp_churn_name)
    if not exp_churn:
        exp_res = mlflow_create_experiment(exp_churn_name)
        churn_id = exp_res["experiment_id"]
    else:
        churn_id = exp_churn["experiment_id"]

    # Run 1: XGBoost Classifier (Champion model)
    now_ms = int(time.time() * 1000)
    r1 = mlflow_create_run(churn_id, run_name="xgboost_churn_baseline", start_time=now_ms - 3600000, source_type="NOTEBOOK", source_name="notebooks/customer_churn_ml.ipynb")
    r1_id = r1["info"]["run_id"]
    mlflow_log_batch(
        r1_id,
        params=[
            {"key": "model_type", "value": "xgboost"},
            {"key": "max_depth", "value": "6"},
            {"key": "learning_rate", "value": "0.05"},
            {"key": "n_estimators", "value": "150"},
            {"key": "subsample", "value": "0.8"},
            {"key": "delta_source", "value": "dbo.silver_telemetry"}
        ],
        metrics=[
            {"key": "train_loss", "value": 0.65, "step": 1, "timestamp": now_ms - 3590000},
            {"key": "train_loss", "value": 0.48, "step": 2, "timestamp": now_ms - 3580000},
            {"key": "train_loss", "value": 0.35, "step": 3, "timestamp": now_ms - 3570000},
            {"key": "train_loss", "value": 0.24, "step": 4, "timestamp": now_ms - 3560000},
            {"key": "train_loss", "value": 0.16, "step": 5, "timestamp": now_ms - 3550000},
            {"key": "val_loss", "value": 0.68, "step": 1, "timestamp": now_ms - 3590000},
            {"key": "val_loss", "value": 0.51, "step": 2, "timestamp": now_ms - 3580000},
            {"key": "val_loss", "value": 0.38, "step": 3, "timestamp": now_ms - 3570000},
            {"key": "val_loss", "value": 0.28, "step": 4, "timestamp": now_ms - 3560000},
            {"key": "val_loss", "value": 0.19, "step": 5, "timestamp": now_ms - 3550000},
            {"key": "accuracy", "value": 0.942, "step": 5, "timestamp": now_ms - 3550000},
            {"key": "f1_score", "value": 0.915, "step": 5, "timestamp": now_ms - 3550000},
            {"key": "roc_auc", "value": 0.963, "step": 5, "timestamp": now_ms - 3550000},
            {"key": "latency_ms", "value": 4.8, "step": 5, "timestamp": now_ms - 3550000}
        ],
        tags=[
            {"key": "framework", "value": "xgboost"},
            {"key": "mlflow.user", "value": "admin"},
            {"key": "candidate_stage", "value": "Production"}
        ]
    )
    mlflow_update_run(r1_id, status="FINISHED", end_time=now_ms - 3548000)

    # Run 2: Random Forest
    r2 = mlflow_create_run(churn_id, run_name="random_forest_v1", start_time=now_ms - 7200000, source_type="NOTEBOOK", source_name="notebooks/customer_churn_ml.ipynb")
    r2_id = r2["info"]["run_id"]
    mlflow_log_batch(
        r2_id,
        params=[
            {"key": "model_type", "value": "random_forest"},
            {"key": "max_depth", "value": "8"},
            {"key": "n_estimators", "value": "200"},
            {"key": "criterion", "value": "gini"},
            {"key": "delta_source", "value": "dbo.silver_telemetry"}
        ],
        metrics=[
            {"key": "train_loss", "value": 0.72, "step": 1, "timestamp": now_ms - 7190000},
            {"key": "train_loss", "value": 0.55, "step": 2, "timestamp": now_ms - 7180000},
            {"key": "train_loss", "value": 0.42, "step": 3, "timestamp": now_ms - 7170000},
            {"key": "train_loss", "value": 0.31, "step": 4, "timestamp": now_ms - 7160000},
            {"key": "train_loss", "value": 0.22, "step": 5, "timestamp": now_ms - 7150000},
            {"key": "val_loss", "value": 0.75, "step": 1, "timestamp": now_ms - 7190000},
            {"key": "val_loss", "value": 0.58, "step": 2, "timestamp": now_ms - 7180000},
            {"key": "val_loss", "value": 0.46, "step": 3, "timestamp": now_ms - 7170000},
            {"key": "val_loss", "value": 0.36, "step": 4, "timestamp": now_ms - 7160000},
            {"key": "val_loss", "value": 0.27, "step": 5, "timestamp": now_ms - 7150000},
            {"key": "accuracy", "value": 0.921, "step": 5, "timestamp": now_ms - 7150000},
            {"key": "f1_score", "value": 0.887, "step": 5, "timestamp": now_ms - 7150000},
            {"key": "roc_auc", "value": 0.938, "step": 5, "timestamp": now_ms - 7150000},
            {"key": "latency_ms", "value": 8.2, "step": 5, "timestamp": now_ms - 7150000}
        ],
        tags=[
            {"key": "framework", "value": "scikit-learn"},
            {"key": "mlflow.user", "value": "admin"},
            {"key": "candidate_stage", "value": "Staging"}
        ]
    )
    mlflow_update_run(r2_id, status="FINISHED", end_time=now_ms - 7149000)

    # Run 3: Logistic Regression
    r3 = mlflow_create_run(churn_id, run_name="logistic_regression_baseline", start_time=now_ms - 10800000, source_type="NOTEBOOK", source_name="notebooks/customer_churn_ml.ipynb")
    r3_id = r3["info"]["run_id"]
    mlflow_log_batch(
        r3_id,
        params=[
            {"key": "model_type", "value": "logistic_regression"},
            {"key": "C", "value": "1.0"},
            {"key": "penalty", "value": "l2"},
            {"key": "solver", "value": "lbfgs"},
            {"key": "delta_source", "value": "dbo.silver_telemetry"}
        ],
        metrics=[
            {"key": "train_loss", "value": 0.82, "step": 1, "timestamp": now_ms - 10790000},
            {"key": "train_loss", "value": 0.69, "step": 2, "timestamp": now_ms - 10780000},
            {"key": "train_loss", "value": 0.58, "step": 3, "timestamp": now_ms - 10770000},
            {"key": "train_loss", "value": 0.51, "step": 4, "timestamp": now_ms - 10760000},
            {"key": "train_loss", "value": 0.46, "step": 5, "timestamp": now_ms - 10750000},
            {"key": "val_loss", "value": 0.85, "step": 1, "timestamp": now_ms - 10790000},
            {"key": "val_loss", "value": 0.72, "step": 2, "timestamp": now_ms - 10780000},
            {"key": "val_loss", "value": 0.61, "step": 3, "timestamp": now_ms - 10770000},
            {"key": "val_loss", "value": 0.54, "step": 4, "timestamp": now_ms - 10760000},
            {"key": "val_loss", "value": 0.49, "step": 5, "timestamp": now_ms - 10750000},
            {"key": "accuracy", "value": 0.845, "step": 5, "timestamp": now_ms - 10750000},
            {"key": "f1_score", "value": 0.792, "step": 5, "timestamp": now_ms - 10750000},
            {"key": "roc_auc", "value": 0.860, "step": 5, "timestamp": now_ms - 10750000},
            {"key": "latency_ms", "value": 1.2, "step": 5, "timestamp": now_ms - 10750000}
        ],
        tags=[
            {"key": "framework", "value": "scikit-learn"},
            {"key": "mlflow.user", "value": "admin"},
            {"key": "candidate_stage", "value": "Archived"}
        ]
    )
    mlflow_update_run(r3_id, status="FINISHED", end_time=now_ms - 10749000)

    # 2. Experiment: Sensor Telemetry Anomaly Detection
    exp_anomaly_name = "sensor_telemetry_anomaly_detection"
    exp_anomaly = mlflow_get_experiment_by_name(exp_anomaly_name)
    if not exp_anomaly:
        exp_res2 = mlflow_create_experiment(exp_anomaly_name)
        anom_id = exp_res2["experiment_id"]
    else:
        anom_id = exp_anomaly["experiment_id"]

    r_anom = mlflow_create_run(anom_id, run_name="isolation_forest_prod", start_time=now_ms - 1800000, source_type="JOB", source_name="job_medallion_pipeline")
    r_anom_id = r_anom["info"]["run_id"]
    mlflow_log_batch(
        r_anom_id,
        params=[
            {"key": "algorithm", "value": "IsolationForest"},
            {"key": "contamination", "value": "0.03"},
            {"key": "n_estimators", "value": "100"},
            {"key": "target_table", "value": "dbo.bronze_telemetry"}
        ],
        metrics=[
            {"key": "anomalies_detected", "value": 14.0, "step": 1, "timestamp": now_ms - 1790000},
            {"key": "anomaly_pct", "value": 2.8, "step": 1, "timestamp": now_ms - 1790000},
            {"key": "silhouette_score", "value": 0.724, "step": 1, "timestamp": now_ms - 1790000},
            {"key": "latency_ms", "value": 6.5, "step": 1, "timestamp": now_ms - 1790000}
        ],
        tags=[
            {"key": "domain", "value": "IoT Telemetry"},
            {"key": "pipeline", "value": "Medallion Automated"}
        ]
    )
    mlflow_update_run(r_anom_id, status="FINISHED", end_time=now_ms - 1785000)

    return {"status": "SUCCESS", "message": "Seeded demo experiments: customer_churn_prediction (3 runs) and sensor_telemetry_anomaly_detection (1 run)"}


# =========================================================================
# MLflow 2.14+ GenAI & LLM Tracing Service Implementation
# =========================================================================

def _safe_json_loads(val: Any, default: Any = None) -> Any:
    """Safely parses a JSON string, returning a default object or raw value if not JSON."""
    if val is None:
        return default if default is not None else {}
    if not isinstance(val, str):
        return val
    val_stripped = val.strip()
    if not val_stripped:
        return default if default is not None else {}
    try:
        return json.loads(val_stripped)
    except Exception:
        return val


def seed_default_traces(conn: sqlite3.Connection):
    """Seeds rich demonstration GenAI & LLM traces with nested span hierarchies and assessments."""
    now_ms = int(time.time() * 1000)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 1. Trace: Customer Support RAG Agent
    tr1_id = "tr_rag_agent_8f1a"
    conn.execute("""
        INSERT OR REPLACE INTO traces (
            request_id, experiment_id, name, timestamp_ms, execution_time_ms, status,
            request, response, tags, total_tokens, prompt_tokens, completion_tokens, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        tr1_id,
        "0",
        "customer_support_rag_agent",
        now_ms - 3600000,
        1240.5,
        "OK",
        json.dumps({"query": "How do I configure mutual TLS for our Kafka cluster in the EU region?"}),
        json.dumps({"answer": "To configure mutual TLS (mTLS) for your Kafka broker in the EU region, follow these steps:\n1. Generate client and server certificates signed by your enterprise CA.\n2. In server.properties set `ssl.client.auth=required`.\n3. Add the CA certificate to the truststore and deploy with role-based ACLs.\nRefer to KB Doc #841 for complete parameter templates."}),
        json.dumps({"framework": "langchain", "agent_type": "rag", "model": "claude-3-5-sonnet", "environment": "production"}),
        1055,
        840,
        215,
        now_str
    ))

    t1_start_ns = (now_ms - 3600000) * 1_000_000
    spans_tr1 = [
        ("sp_rag_root_01", tr1_id, None, "customer_support_rag_agent", "AGENT", t1_start_ns, t1_start_ns + int(1240.5 * 1e6), 1240.5, "OK", "",
         json.dumps({"user_query": "How do I configure mutual TLS for our Kafka cluster in the EU region?"}),
         json.dumps({"answer": "To configure mutual TLS (mTLS) for your Kafka broker in the EU region..."}),
         json.dumps({"framework": "langchain", "agent_mode": "hybrid_rag"}), "[]"),
        
        ("sp_rewriter_02", tr1_id, "sp_rag_root_01", "query_rewriter", "CHAIN", t1_start_ns + int(15 * 1e6), t1_start_ns + int(130.2 * 1e6), 115.2, "OK", "",
         json.dumps({"original_query": "How do I configure mutual TLS for our Kafka cluster in the EU region?"}),
         json.dumps({"search_queries": ["kafka mutual tls mtls configuration eu region", "kafka ssl.client.auth enterprise CA"]}),
         json.dumps({"model": "gpt-4o-mini", "temperature": 0.0, "usage.prompt_tokens": 140, "usage.completion_tokens": 35}), "[]"),

        ("sp_retriever_03", tr1_id, "sp_rag_root_01", "vector_hybrid_retriever", "RETRIEVER", t1_start_ns + int(140 * 1e6), t1_start_ns + int(452.4 * 1e6), 312.4, "OK", "",
         json.dumps({"query": "kafka mutual tls mtls configuration eu region", "top_k": 3}),
         json.dumps({"documents": [{"doc_id": "kb_841", "title": "Kafka Security Architecture & mTLS Guide", "score": 0.94}, {"doc_id": "kb_219", "title": "EU Infrastructure Network ACLs", "score": 0.88}]}),
         json.dumps({"index_name": "enterprise_kb_vector", "metric": "cosine", "hybrid_alpha": 0.75}), "[]"),

        ("sp_embed_04", tr1_id, "sp_retriever_03", "text_embedding_3_small", "EMBEDDING", t1_start_ns + int(145 * 1e6), t1_start_ns + int(193.1 * 1e6), 48.1, "OK", "",
         json.dumps({"text": "kafka mutual tls mtls configuration eu region"}),
         json.dumps({"embedding_dim": 1536}),
         json.dumps({"model": "text-embedding-3-small", "usage.prompt_tokens": 12}), "[]"),

        ("sp_tool_05", tr1_id, "sp_rag_root_01", "verify_cluster_status_tool", "TOOL", t1_start_ns + int(460 * 1e6), t1_start_ns + int(645 * 1e6), 185.0, "OK", "",
         json.dumps({"cluster_id": "kafka-prod-eu-west-1", "action": "check_tls_readiness"}),
         json.dumps({"status": "ready", "installed_ciphers": ["TLS_AES_256_GCM_SHA384", "TLS_CHACHA20_POLY1305_SHA256"]}),
         json.dumps({"tool_name": "kubernetes_kafka_operator_api"}), "[]"),

        ("sp_llm_06", tr1_id, "sp_rag_root_01", "synthesis_llm", "LLM", t1_start_ns + int(655 * 1e6), t1_start_ns + int(1237 * 1e6), 582.0, "OK", "",
         json.dumps({"messages": [{"role": "system", "content": "You are an enterprise cloud security architect. Ground responses in provided docs."}, {"role": "user", "content": "How do I configure mutual TLS for our Kafka cluster in the EU region?"}]}),
         json.dumps({"content": "To configure mutual TLS (mTLS) for your Kafka broker in the EU region..."}),
         json.dumps({"model": "claude-3-5-sonnet", "temperature": 0.2, "usage.prompt_tokens": 700, "usage.completion_tokens": 180, "usage.total_tokens": 880}), "[]")
    ]

    for sp in spans_tr1:
        conn.execute("""
            INSERT OR REPLACE INTO spans (
                span_id, request_id, parent_id, name, span_type, start_time_ns, end_time_ns,
                duration_ms, status_code, status_message, inputs, outputs, attributes, events
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, sp)

    conn.execute("""
        INSERT OR REPLACE INTO trace_assessments (
            assessment_id, trace_id, source_type, source_id, name, value, rationale, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """, (f"asm_{uuid.uuid4().hex[:8]}", tr1_id, "HUMAN", "admin", "groundedness", "thumbs_up", "Accurate mTLS steps with correct server.properties directive", now_str))

    # 2. Trace: SQL Copilot Agent
    tr2_id = "tr_sql_copilot_9e2b"
    conn.execute("""
        INSERT OR REPLACE INTO traces (
            request_id, experiment_id, name, timestamp_ms, execution_time_ms, status,
            request, response, tags, total_tokens, prompt_tokens, completion_tokens, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        tr2_id,
        "0",
        "sql_copilot_pipeline",
        now_ms - 7200000,
        845.0,
        "OK",
        json.dumps({"prompt": "Calculate 30-day customer retention rate grouped by subscription tier"}),
        json.dumps({"sql": "WITH cohorts AS (SELECT user_id, tier, MIN(event_date) AS signup_date FROM warehouse.dbo.silver_events GROUP BY 1, 2) SELECT tier, COUNT(DISTINCT user_id) FROM cohorts GROUP BY 1;"}),
        json.dumps({"framework": "dspy", "agent_type": "text2sql", "model": "gpt-4o", "environment": "production"}),
        740,
        610,
        130,
        now_str
    ))

    t2_start_ns = (now_ms - 7200000) * 1_000_000
    spans_tr2 = [
        ("sp_sql_root_01", tr2_id, None, "sql_copilot_pipeline", "CHAIN", t2_start_ns, t2_start_ns + int(845.0 * 1e6), 845.0, "OK", "",
         json.dumps({"prompt": "Calculate 30-day customer retention rate grouped by subscription tier"}),
         json.dumps({"sql": "WITH cohorts AS (...)"}),
         json.dumps({"framework": "dspy"}), "[]"),

        ("sp_cat_02", tr2_id, "sp_sql_root_01", "catalog_schema_lookup", "RETRIEVER", t2_start_ns + int(10 * 1e6), t2_start_ns + int(85 * 1e6), 75.0, "OK", "",
         json.dumps({"table_filter": ["silver_events", "customers"]}),
         json.dumps({"schemas": [{"table": "silver_events", "columns": ["user_id", "tier", "event_date"]}]}),
         json.dumps({"store": "unity_catalog"}), "[]"),

        ("sp_gen_03", tr2_id, "sp_sql_root_01", "gpt4o_sql_generator", "LLM", t2_start_ns + int(90 * 1e6), t2_start_ns + int(670 * 1e6), 580.0, "OK", "",
         json.dumps({"schema_context": "silver_events (user_id, tier, event_date)", "goal": "Calculate 30-day retention"}),
         json.dumps({"sql": "WITH cohorts AS (SELECT user_id, tier, MIN(event_date) AS signup_date FROM warehouse.dbo.silver_events GROUP BY 1, 2)..."}),
         json.dumps({"model": "gpt-4o", "usage.prompt_tokens": 580, "usage.completion_tokens": 130, "usage.total_tokens": 710}), "[]"),

        ("sp_ast_04", tr2_id, "sp_sql_root_01", "duckdb_ast_validator", "TOOL", t2_start_ns + int(675 * 1e6), t2_start_ns + int(840 * 1e6), 165.0, "OK", "",
         json.dumps({"sql": "WITH cohorts AS (SELECT user_id, tier, MIN(event_date) AS signup_date FROM warehouse.dbo.silver_events GROUP BY 1, 2)..."}),
         json.dumps({"syntax_valid": True, "dialect": "duckdb", "ast_nodes": 8}),
         json.dumps({"parser": "sqlglot"}), "[]")
    ]

    for sp in spans_tr2:
        conn.execute("""
            INSERT OR REPLACE INTO spans (
                span_id, request_id, parent_id, name, span_type, start_time_ns, end_time_ns,
                duration_ms, status_code, status_message, inputs, outputs, attributes, events
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, sp)

    conn.execute("""
        INSERT OR REPLACE INTO trace_assessments (
            assessment_id, trace_id, source_type, source_id, name, value, rationale, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """, (f"asm_{uuid.uuid4().hex[:8]}", tr2_id, "LLM_JUDGE", "eval_judge_01", "syntax_rating", "5", "Valid DuckDB CTE query generated with proper group by", now_str))

    # 3. Trace: Document Risk Extractor (Error Status)
    tr3_id = "tr_risk_doc_7c3d"
    conn.execute("""
        INSERT OR REPLACE INTO traces (
            request_id, experiment_id, name, timestamp_ms, execution_time_ms, status,
            request, response, tags, total_tokens, prompt_tokens, completion_tokens, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        tr3_id,
        "0",
        "document_risk_analyzer",
        now_ms - 14400000,
        412.0,
        "ERROR",
        json.dumps({"document_url": "s3://compliance/annual_filings_2026.pdf"}),
        json.dumps({"error": "RateLimitError: Rate limit reached for model gpt-4o on organization org-corp"}),
        json.dumps({"framework": "openai", "model": "gpt-4o", "environment": "staging"}),
        320,
        320,
        0,
        now_str
    ))

    t3_start_ns = (now_ms - 14400000) * 1_000_000
    spans_tr3 = [
        ("sp_doc_root_01", tr3_id, None, "document_risk_analyzer", "AGENT", t3_start_ns, t3_start_ns + int(412.0 * 1e6), 412.0, "ERROR", "RateLimitError: 429 Too Many Requests",
         json.dumps({"document_url": "s3://compliance/annual_filings_2026.pdf"}),
         json.dumps({"error": "RateLimitError"}),
         json.dumps({"framework": "openai"}), "[]"),

        ("sp_pdf_02", tr3_id, "sp_doc_root_01", "pdf_text_parser", "TOOL", t3_start_ns + int(5 * 1e6), t3_start_ns + int(90 * 1e6), 85.0, "OK", "",
         json.dumps({"path": "annual_filings_2026.pdf"}),
         json.dumps({"pages": 12, "char_count": 34800}),
         json.dumps({"tool": "pypdf_extractor"}), "[]"),

        ("sp_llm_03", tr3_id, "sp_doc_root_01", "risk_extraction_llm", "LLM", t3_start_ns + int(95 * 1e6), t3_start_ns + int(410 * 1e6), 315.0, "ERROR", "RateLimitError: Rate limit reached for model gpt-4o (429)",
         json.dumps({"prompt": "Extract high risk financial disclosures..."}),
         json.dumps({"error": "RateLimitError"}),
         json.dumps({"model": "gpt-4o", "usage.prompt_tokens": 320, "usage.completion_tokens": 0}), "[]")
    ]

    for sp in spans_tr3:
        conn.execute("""
            INSERT OR REPLACE INTO spans (
                span_id, request_id, parent_id, name, span_type, start_time_ns, end_time_ns,
                duration_ms, status_code, status_message, inputs, outputs, attributes, events
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, sp)


def mlflow_create_trace(
    name: str = "trace",
    experiment_id: str = "0",
    request_id: Optional[str] = None,
    timestamp_ms: Optional[int] = None,
    execution_time_ms: float = 0.0,
    status: str = "OK",
    request: Optional[Any] = None,
    response: Optional[Any] = None,
    tags: Optional[Dict[str, Any]] = None,
    total_tokens: int = 0,
    prompt_tokens: int = 0,
    completion_tokens: int = 0
) -> Dict[str, Any]:
    """Creates a new trace header record in MLflow experiments database."""
    init_experiments_db()
    rid = request_id or f"tr_{uuid.uuid4().hex[:12]}"
    ts_ms = timestamp_ms or int(time.time() * 1000)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    req_str = json.dumps(request) if isinstance(request, (dict, list)) else (str(request) if request else "{}")
    resp_str = json.dumps(response) if isinstance(response, (dict, list)) else (str(response) if response else "{}")
    tags_str = json.dumps(tags or {})

    with get_exp_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO traces (
                request_id, experiment_id, name, timestamp_ms, execution_time_ms, status,
                request, response, tags, total_tokens, prompt_tokens, completion_tokens, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            rid, experiment_id, name, ts_ms, float(execution_time_ms), status,
            req_str, resp_str, tags_str, int(total_tokens), int(prompt_tokens), int(completion_tokens), now_str
        ))

    return mlflow_get_trace(rid)


def mlflow_update_trace(
    request_id: str,
    execution_time_ms: Optional[float] = None,
    status: Optional[str] = None,
    response: Optional[Any] = None,
    tags: Optional[Dict[str, Any]] = None,
    total_tokens: Optional[int] = None,
    prompt_tokens: Optional[int] = None,
    completion_tokens: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """Updates an existing trace status, execution latency, response, or token counts."""
    init_experiments_db()
    with get_exp_db() as conn:
        row = conn.execute("SELECT * FROM traces WHERE request_id = ?", (request_id,)).fetchone()
        if not row:
            return None

        current_tags = _safe_json_loads(row["tags"], {})
        if tags:
            current_tags.update(tags)

        updates = []
        params = []
        if execution_time_ms is not None:
            updates.append("execution_time_ms = ?")
            params.append(float(execution_time_ms))
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if response is not None:
            resp_str = json.dumps(response) if isinstance(response, (dict, list)) else str(response)
            updates.append("response = ?")
            params.append(resp_str)
        if tags is not None:
            updates.append("tags = ?")
            params.append(json.dumps(current_tags))
        if total_tokens is not None:
            updates.append("total_tokens = ?")
            params.append(int(total_tokens))
        if prompt_tokens is not None:
            updates.append("prompt_tokens = ?")
            params.append(int(prompt_tokens))
        if completion_tokens is not None:
            updates.append("completion_tokens = ?")
            params.append(int(completion_tokens))

        if updates:
            params.append(request_id)
            conn.execute(f"UPDATE traces SET {', '.join(updates)} WHERE request_id = ?;", params)

    return mlflow_get_trace(request_id)


def mlflow_log_span(
    request_id: str,
    span_id: Optional[str] = None,
    parent_id: Optional[str] = None,
    name: str = "span",
    span_type: str = "UNKNOWN",
    start_time_ns: Optional[int] = None,
    end_time_ns: Optional[int] = None,
    duration_ms: Optional[float] = None,
    status_code: str = "OK",
    status_message: str = "",
    inputs: Optional[Any] = None,
    outputs: Optional[Any] = None,
    attributes: Optional[Dict[str, Any]] = None,
    events: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """Records an individual span into the trace span store."""
    init_experiments_db()
    sid = span_id or f"sp_{uuid.uuid4().hex[:10]}"
    st_ns = start_time_ns or int(time.time() * 1e9)
    et_ns = end_time_ns or (st_ns + int((duration_ms or 0) * 1e6))
    dur_ms = duration_ms if duration_ms is not None else max(0.0, round((et_ns - st_ns) / 1e6, 2))

    in_str = json.dumps(inputs) if isinstance(inputs, (dict, list)) else (str(inputs) if inputs is not None else "{}")
    out_str = json.dumps(outputs) if isinstance(outputs, (dict, list)) else (str(outputs) if outputs is not None else "{}")
    attr_str = json.dumps(attributes or {})
    events_str = json.dumps(events or [])

    with get_exp_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO spans (
                span_id, request_id, parent_id, name, span_type, start_time_ns, end_time_ns,
                duration_ms, status_code, status_message, inputs, outputs, attributes, events
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            sid, request_id, parent_id, name, span_type, st_ns, et_ns,
            dur_ms, status_code, status_message, in_str, out_str, attr_str, events_str
        ))

    return {
        "span_id": sid,
        "request_id": request_id,
        "parent_id": parent_id,
        "name": name,
        "span_type": span_type,
        "start_time_ns": st_ns,
        "end_time_ns": et_ns,
        "duration_ms": dur_ms,
        "status_code": status_code,
        "status_message": status_message
    }


def mlflow_log_trace(trace_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ingests or updates a complete trace with all its spans in a single atomic transaction.
    Accepts Databricks MLflow OpenTelemetry trace schema.
    """
    init_experiments_db()
    rid = trace_data.get("request_id") or f"tr_{uuid.uuid4().hex[:12]}"
    exp_id = str(trace_data.get("experiment_id", "0"))
    name = trace_data.get("name", "trace")
    ts_ms = trace_data.get("timestamp_ms") or int(time.time() * 1000)
    exec_ms = float(trace_data.get("execution_time_ms", 0.0))
    status = trace_data.get("status", "OK")
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    req_raw = trace_data.get("request", {})
    resp_raw = trace_data.get("response", {})
    tags_raw = trace_data.get("tags", {})
    req_str = json.dumps(req_raw) if isinstance(req_raw, (dict, list)) else str(req_raw)
    resp_str = json.dumps(resp_raw) if isinstance(resp_raw, (dict, list)) else str(resp_raw)
    tags_str = json.dumps(tags_raw) if isinstance(tags_raw, (dict, list)) else str(tags_raw)

    spans = trace_data.get("spans", [])

    # Calculate token totals across spans if not provided directly
    p_tokens = int(trace_data.get("prompt_tokens", 0))
    c_tokens = int(trace_data.get("completion_tokens", 0))
    for s in spans:
        attrs = s.get("attributes", {})
        if isinstance(attrs, dict):
            p_tokens += int(attrs.get("usage.prompt_tokens", attrs.get("prompt_tokens", 0)))
            c_tokens += int(attrs.get("usage.completion_tokens", attrs.get("completion_tokens", 0)))
    tot_tokens = p_tokens + c_tokens if (p_tokens + c_tokens) > 0 else int(trace_data.get("total_tokens", 0))

    with get_exp_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO traces (
                request_id, experiment_id, name, timestamp_ms, execution_time_ms, status,
                request, response, tags, total_tokens, prompt_tokens, completion_tokens, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            rid, exp_id, name, ts_ms, exec_ms, status,
            req_str, resp_str, tags_str, tot_tokens, p_tokens, c_tokens, now_str
        ))

        for sp in spans:
            sid = sp.get("span_id") or f"sp_{uuid.uuid4().hex[:10]}"
            pid = sp.get("parent_id")
            sname = sp.get("name", "span")
            stype = sp.get("span_type", "UNKNOWN")
            st_ns = sp.get("start_time_ns") or (ts_ms * 1_000_000)
            et_ns = sp.get("end_time_ns") or (st_ns + int(exec_ms * 1e6))
            dur_ms = float(sp.get("duration_ms", max(0.0, (et_ns - st_ns) / 1e6)))
            scode = sp.get("status_code", "OK")
            smsg = sp.get("status_message", "")

            sin = sp.get("inputs", {})
            sout = sp.get("outputs", {})
            sattrs = sp.get("attributes", {})
            sevents = sp.get("events", [])

            conn.execute("""
                INSERT OR REPLACE INTO spans (
                    span_id, request_id, parent_id, name, span_type, start_time_ns, end_time_ns,
                    duration_ms, status_code, status_message, inputs, outputs, attributes, events
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                sid, rid, pid, sname, stype, st_ns, et_ns, dur_ms, scode, smsg,
                json.dumps(sin) if isinstance(sin, (dict, list)) else str(sin),
                json.dumps(sout) if isinstance(sout, (dict, list)) else str(sout),
                json.dumps(sattrs) if isinstance(sattrs, (dict, list)) else str(sattrs),
                json.dumps(sevents) if isinstance(sevents, (dict, list)) else str(sevents)
            ))

    return mlflow_get_trace(rid)


def mlflow_get_trace(request_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves full trace details, spans list, reconstructed hierarchical span tree, and assessments."""
    init_experiments_db()
    with get_exp_db() as conn:
        t_row = conn.execute("SELECT * FROM traces WHERE request_id = ?;", (request_id,)).fetchone()
        if not t_row:
            return None

        trace = dict(t_row)
        trace["request"] = _safe_json_loads(trace["request"], {})
        trace["response"] = _safe_json_loads(trace["response"], {})
        trace["tags"] = _safe_json_loads(trace["tags"], {})

        # Fetch spans ordered chronologically
        s_rows = conn.execute("SELECT * FROM spans WHERE request_id = ? ORDER BY start_time_ns ASC;", (request_id,)).fetchall()
        spans = []
        min_start_ns = None
        for r in s_rows:
            sp = dict(r)
            sp["inputs"] = _safe_json_loads(sp["inputs"], {})
            sp["outputs"] = _safe_json_loads(sp["outputs"], {})
            sp["attributes"] = _safe_json_loads(sp["attributes"], {})
            sp["events"] = _safe_json_loads(sp["events"], [])
            if min_start_ns is None or sp["start_time_ns"] < min_start_ns:
                min_start_ns = sp["start_time_ns"]
            spans.append(sp)

        # Compute offset_ms and relative percentage for waterfall timeline
        total_trace_dur = trace["execution_time_ms"] or 1.0
        for sp in spans:
            if min_start_ns is not None:
                offset_ms = max(0.0, round((sp["start_time_ns"] - min_start_ns) / 1_000_000.0, 2))
            else:
                offset_ms = 0.0
            sp["offset_ms"] = offset_ms
            sp["offset_pct"] = min(100.0, round((offset_ms / total_trace_dur) * 100, 1))
            sp["duration_pct"] = min(100.0, max(2.0, round((sp["duration_ms"] / total_trace_dur) * 100, 1)))

        # Build nested hierarchical span tree
        span_map = {sp["span_id"]: dict(sp, children=[]) for sp in spans}
        root_spans = []
        for sp in spans:
            sid = sp["span_id"]
            pid = sp["parent_id"]
            if pid and pid in span_map:
                span_map[pid]["children"].append(span_map[sid])
            else:
                root_spans.append(span_map[sid])

        trace["spans"] = spans
        trace["span_tree"] = root_spans
        trace["span_count"] = len(spans)

        # Fetch assessments
        a_rows = conn.execute("SELECT * FROM trace_assessments WHERE trace_id = ? ORDER BY created_at DESC;", (request_id,)).fetchall()
        trace["assessments"] = [dict(a) for a in a_rows]

    return trace


def mlflow_search_traces(
    experiment_ids: Optional[List[str]] = None,
    status: Optional[str] = None,
    model: Optional[str] = None,
    min_duration: Optional[float] = None,
    max_duration: Optional[float] = None,
    search_term: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> Dict[str, Any]:
    """Searches traces with multi-criteria filtering and pagination."""
    init_experiments_db()
    conditions = []
    params = []

    if experiment_ids:
        placeholders = ",".join("?" for _ in experiment_ids)
        conditions.append(f"experiment_id IN ({placeholders})")
        params.extend(experiment_ids)

    if status and status.upper() != "ALL":
        conditions.append("status = ?")
        params.append(status.upper())

    if min_duration is not None:
        conditions.append("execution_time_ms >= ?")
        params.append(float(min_duration))

    if max_duration is not None:
        conditions.append("execution_time_ms <= ?")
        params.append(float(max_duration))

    if model:
        conditions.append("(tags LIKE ? OR request_id IN (SELECT request_id FROM spans WHERE attributes LIKE ?))")
        params.append(f"%{model}%")
        params.append(f"%{model}%")

    if search_term:
        term_clean = f"%{search_term.strip()}%"
        conditions.append("(name LIKE ? OR request_id LIKE ? OR request LIKE ? OR response LIKE ?)")
        params.extend([term_clean, term_clean, term_clean, term_clean])

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_exp_db() as conn:
        count_query = f"SELECT COUNT(*) FROM traces {where_clause};"
        total_count = conn.execute(count_query, params).fetchone()[0]

        data_query = f"""
            SELECT * FROM traces {where_clause}
            ORDER BY timestamp_ms DESC
            LIMIT ? OFFSET ?;
        """
        data_params = params + [int(limit), int(offset)]
        rows = conn.execute(data_query, data_params).fetchall()

        traces = []
        for r in rows:
            t = dict(r)
            t["request"] = _safe_json_loads(t["request"], {})
            t["response"] = _safe_json_loads(t["response"], {})
            t["tags"] = _safe_json_loads(t["tags"], {})

            # Fast span count and assessments count
            cur_sp = conn.execute("SELECT COUNT(*), COUNT(DISTINCT span_type) FROM spans WHERE request_id = ?;", (t["request_id"],)).fetchone()
            t["span_count"] = cur_sp[0] if cur_sp else 0
            t["span_types_count"] = cur_sp[1] if cur_sp else 0

            # Attach latest assessment if present
            ass = conn.execute("SELECT name, value, rationale FROM trace_assessments WHERE trace_id = ? ORDER BY created_at DESC LIMIT 1;", (t["request_id"],)).fetchone()
            t["latest_assessment"] = dict(ass) if ass else None

            traces.append(t)

    return {
        "traces": traces,
        "total_count": total_count,
        "limit": limit,
        "offset": offset
    }


def mlflow_delete_trace(request_id: str) -> bool:
    """Deletes a trace and its cascading spans and assessments."""
    init_experiments_db()
    with get_exp_db() as conn:
        c = conn.execute("DELETE FROM traces WHERE request_id = ?;", (request_id,))
        return c.rowcount > 0


def mlflow_log_assessment(
    trace_id: str,
    name: str,
    value: str,
    rationale: str = "",
    source_type: str = "HUMAN",
    source_id: str = "admin"
) -> Dict[str, Any]:
    """Logs human thumbs up/down, numeric rating, or LLM-judge feedback on a trace."""
    init_experiments_db()
    aid = f"asm_{uuid.uuid4().hex[:10]}"
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_exp_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO trace_assessments (
                assessment_id, trace_id, source_type, source_id, name, value, rationale, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (aid, trace_id, source_type, source_id, name, str(value), rationale, now_str))

    return {
        "assessment_id": aid,
        "trace_id": trace_id,
        "source_type": source_type,
        "source_id": source_id,
        "name": name,
        "value": str(value),
        "rationale": rationale,
        "created_at": now_str
    }


def mlflow_get_assessments(trace_id: str) -> List[Dict[str, Any]]:
    """Returns all assessment feedback items associated with a trace."""
    init_experiments_db()
    with get_exp_db() as conn:
        rows = conn.execute("SELECT * FROM trace_assessments WHERE trace_id = ? ORDER BY created_at DESC;", (trace_id,)).fetchall()
        return [dict(r) for r in rows]

