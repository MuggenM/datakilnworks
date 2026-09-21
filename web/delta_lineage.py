"""
Delta Lake Lineage & MLflow Integration Engine for Data Kiln Works.
Provides automated Delta table commit versioning, time-travel reproducibility,
and links MLflow runs and trained models directly into the Unity Catalog Lakehouse Lineage DAG.
"""

import os
import re
import json
import logging
import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger("localspark.delta_lineage")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
if not os.path.exists(WAREHOUSE_DIR):
    local_alt = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "warehouse"))
    if os.path.exists(local_alt):
        WAREHOUSE_DIR = local_alt


def resolve_delta_table_path(table_name_or_path: str, warehouse_dir: Optional[str] = None) -> Optional[str]:
    """Resolves a table name or path to an existing Delta table directory with a _delta_log."""
    wh_dir = warehouse_dir or WAREHOUSE_DIR
    clean = (table_name_or_path or "").strip().rstrip("/")
    if not clean:
        return None

    # 1. Direct path check
    if os.path.isdir(clean) and os.path.isdir(os.path.join(clean, "_delta_log")):
        return os.path.abspath(clean)

    # 2. Path relative to warehouse_dir
    rel_candidate = os.path.join(wh_dir, clean.lstrip("/"))
    if os.path.isdir(rel_candidate) and os.path.isdir(os.path.join(rel_candidate, "_delta_log")):
        return os.path.abspath(rel_candidate)

    # 3. Dotted namespace check: catalog.schema.table, schema.table, or table
    parts = clean.split(".")
    if len(parts) >= 3 and parts[0] in ("warehouse", "local", "hive_metastore"):
        parts = parts[1:]

    # Try schema/table under warehouse
    if len(parts) == 2:
        schema_tbl_path = os.path.join(wh_dir, parts[0], parts[1])
        if os.path.isdir(schema_tbl_path) and os.path.isdir(os.path.join(schema_tbl_path, "_delta_log")):
            return os.path.abspath(schema_tbl_path)
    elif len(parts) == 1:
        # Check in dbo or default schema
        for default_schema in ["dbo", "default", "public"]:
            c = os.path.join(wh_dir, default_schema, parts[0])
            if os.path.isdir(c) and os.path.isdir(os.path.join(c, "_delta_log")):
                return os.path.abspath(c)

    # 4. Recursive search for _delta_log in warehouse subdirectories
    tbl_basename = parts[-1]
    for root, dirs, _ in os.walk(wh_dir):
        if os.path.basename(root) == tbl_basename and "_delta_log" in dirs:
            return os.path.abspath(root)

    return None


def get_delta_table_info(table_name_or_path: str, warehouse_dir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Inspects a Delta table's _delta_log to extract latest commit version, schema, and metadata."""
    tbl_path = resolve_delta_table_path(table_name_or_path, warehouse_dir=warehouse_dir)
    if not tbl_path:
        return None

    log_dir = os.path.join(tbl_path, "_delta_log")
    if not os.path.isdir(log_dir):
        return None

    # Find numeric commit files
    try:
        commit_files = [f for f in os.listdir(log_dir) if f.endswith(".json")]
    except Exception as e:
        logger.warning(f"Failed to list _delta_log at {log_dir}: {e}")
        return None

    versions = sorted([int(f.split(".")[0]) for f in commit_files if f.split(".")[0].isdigit()])
    if not versions:
        return None

    latest_ver = versions[-1]
    commit_info: Dict[str, Any] = {}
    meta_data: Dict[str, Any] = {}

    # Read latest commit file
    latest_file = os.path.join(log_dir, f"{latest_ver:020d}.json")
    try:
        with open(latest_file, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if "commitInfo" in obj:
                        commit_info = obj["commitInfo"]
                    if "metaData" in obj:
                        meta_data = obj["metaData"]
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"Failed to read Delta commit {latest_file}: {e}")

    # If metaData not in latest commit, search backwards from latest version
    if not meta_data:
        for v in reversed(versions[:-1]):
            vf = os.path.join(log_dir, f"{v:020d}.json")
            try:
                with open(vf, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        if '"metaData"' in line:
                            obj = json.loads(line.strip())
                            if "metaData" in obj:
                                meta_data = obj["metaData"]
                                break
                if meta_data:
                    break
            except Exception:
                continue

    # Extract schema fields
    schema_fields = []
    raw_schema_str = meta_data.get("schemaString", "{}")
    try:
        parsed_schema = json.loads(raw_schema_str) if isinstance(raw_schema_str, str) else raw_schema_str
        schema_fields = parsed_schema.get("fields", [])
    except Exception:
        pass

    # Extract table and schema names
    rel_path = os.path.relpath(tbl_path, warehouse_dir or WAREHOUSE_DIR)
    path_parts = rel_path.split(os.sep)
    schema_name = path_parts[0] if len(path_parts) > 1 else "dbo"
    table_basename = path_parts[-1]
    full_table_name = f"{schema_name}.{table_basename}"

    ts = commit_info.get("timestamp")
    time_str = datetime.datetime.fromtimestamp(ts / 1000.0).strftime("%Y-%m-%d %H:%M:%S") if ts else ""

    return {
        "table_name": full_table_name,
        "schema_name": schema_name,
        "table_basename": table_basename,
        "table_path": tbl_path,
        "version": latest_ver,
        "timestamp": ts,
        "commit_time_str": time_str,
        "operation": commit_info.get("operation", "WRITE"),
        "operation_metrics": commit_info.get("operationMetrics", {}),
        "engine_info": commit_info.get("engineInfo", "delta-rs"),
        "table_id": meta_data.get("id"),
        "schema_fields": schema_fields,
        "schema_string": raw_schema_str,
        "column_names": [f.get("name") for f in schema_fields if "name" in f]
    }


def record_run_delta_lineage(run_id: str, table_name_or_path: str, context: str = "training") -> Optional[Dict[str, Any]]:
    """Captures Delta Lake commit versioning and registers dataset inputs into SQLite."""
    delta_info = get_delta_table_info(table_name_or_path)
    if not delta_info:
        return None

    try:
        from web import experiments as exp_mod
        # Set Delta tags on run
        exp_mod.mlflow_set_tag(run_id, "delta.table_name", delta_info["table_name"])
        exp_mod.mlflow_set_tag(run_id, "delta.table_version", str(delta_info["version"]))
        exp_mod.mlflow_set_tag(run_id, "delta.commit_timestamp", str(delta_info["timestamp"] or ""))
        exp_mod.mlflow_set_tag(run_id, "delta.commit_time", delta_info["commit_time_str"])
        exp_mod.mlflow_set_tag(run_id, "delta.operation", delta_info["operation"])
        exp_mod.mlflow_set_tag(run_id, "delta.table_path", delta_info["table_path"])
        if delta_info.get("table_id"):
            exp_mod.mlflow_set_tag(run_id, "delta.table_id", str(delta_info["table_id"]))
        if delta_info.get("schema_string"):
            exp_mod.mlflow_set_tag(run_id, "delta.schema", delta_info["schema_string"])

        exp_mod.mlflow_set_tag(run_id, "mlflow.data.source", delta_info["table_name"])
        exp_mod.mlflow_set_tag(run_id, "spark.data.source", delta_info["table_name"])

        # Also register formal run input
        schema_dict = {"columns": [{"name": f.get("name"), "type": str(f.get("type"))} for f in delta_info.get("schema_fields", [])]}
        profile_dict = {
            "delta_version": delta_info["version"],
            "commit_timestamp": delta_info["timestamp"],
            "operation": delta_info["operation"],
            "num_columns": len(delta_info.get("column_names", []))
        }

        dataset_entry = {
            "dataset": {
                "name": delta_info["table_name"],
                "digest": delta_info.get("table_id") or f"v{delta_info['version']}",
                "source_type": "delta",
                "source": f"delta://{delta_info['table_name']}@v{delta_info['version']}",
                "schema": schema_dict,
                "profile": profile_dict
            },
            "tags": [
                {"key": "context", "value": context},
                {"key": "source_format", "value": "delta"},
                {"key": "delta_version", "value": str(delta_info["version"])}
            ]
        }
        exp_mod.mlflow_log_inputs(run_id, [dataset_entry])
    except Exception as e:
        logger.warning(f"Failed to record Delta lineage for run {run_id}: {e}")

    return delta_info


def sync_ml_models_to_lineage_graph():
    """Scans MLflow runs and models and wires them into the Global Lineage DAG (lineage.db)."""
    try:
        from web.experiments import get_exp_db
        from web.lineage import upsert_node, upsert_edge, make_table_id

        with get_exp_db() as conn:
            runs = conn.execute("""
                SELECT r.run_id, r.run_name, r.experiment_id, r.status, r.duration_ms, e.name as experiment_name
                FROM runs r
                JOIN experiments e ON r.experiment_id = e.experiment_id
                WHERE r.lifecycle_stage != 'deleted'
                ORDER BY r.start_time DESC
                LIMIT 50
            """).fetchall()

            for r in runs:
                rid = r["run_id"]
                # Fetch tags for this run
                tag_rows = conn.execute("SELECT key, value FROM run_tags WHERE run_id = ?", (rid,)).fetchall()
                tags = {t["key"]: t["value"] for t in tag_rows}

                delta_table = tags.get("delta.table_name") or tags.get("mlflow.data.source") or tags.get("delta_source") or tags.get("spark.data.source")
                if not delta_table:
                    continue

                estimator_class = tags.get("estimator_class", "")
                delta_ver = tags.get("delta.table_version", "")
                accuracy = tags.get("training_accuracy_score", "")

                # 1. Resolve source table ID
                parts = delta_table.split(".")
                s_schema = parts[0] if len(parts) > 1 else "dbo"
                s_table = parts[-1]
                table_node_id = make_table_id("warehouse", s_schema, s_table)

                # Ensure table node exists
                upsert_node(
                    node_id=table_node_id,
                    name=s_table,
                    node_type="TABLE",
                    catalog="warehouse",
                    schema_name=s_schema,
                    metadata={"format": "delta"}
                )

                # 2. Create Model Lineage Node
                model_node_id = f"model:{rid}"
                display_name = f"{estimator_class or r['run_name']}"
                if delta_ver:
                    display_name += f" (v{delta_ver})"

                model_meta = {
                    "run_id": rid,
                    "run_name": r["run_name"],
                    "experiment_id": r["experiment_id"],
                    "experiment_name": r["experiment_name"],
                    "estimator_class": estimator_class,
                    "delta_table": delta_table,
                    "delta_version": delta_ver,
                    "status": r["status"],
                    "duration_sec": round((r["duration_ms"] or 0) / 1000.0, 2)
                }

                upsert_node(
                    node_id=model_node_id,
                    name=display_name,
                    node_type="MODEL",
                    layer="MODEL",
                    catalog="warehouse",
                    schema_name=s_schema,
                    metadata=model_meta
                )

                # 3. Create TRAINED_ON directed edge from Delta table to Model
                edge_desc = f"Trained on {delta_table}" + (f" (Delta commit v{delta_ver})" if delta_ver else "")
                upsert_edge(
                    source_id=table_node_id,
                    target_id=model_node_id,
                    edge_type="TRAINED_ON",
                    query_text=edge_desc
                )

        logger.info("Successfully synchronized ML model nodes into the Lakehouse lineage graph.")
    except Exception as e:
        logger.warning(f"Error synchronizing ML models to lineage graph: {e}", exc_info=True)
