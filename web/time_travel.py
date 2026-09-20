"""
Delta Lake Time-Travel, Visual Diff, and 1-Click Restore Engine for Localspark.
Provides point-in-time table inspection, row-level additions/deletions diffing via DuckDB & Arrow,
and ACID rollback via deltalake.DeltaTable.restore().
"""

import os
import time
import json
import logging
import datetime
from typing import Dict, Any, List, Optional, Tuple

import duckdb
from deltalake import DeltaTable

logger = logging.getLogger("localspark.time_travel")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")


def resolve_table_path(schema_name: str, table_name: str, catalog: Optional[str] = None) -> Tuple[str, str]:
    """Resolves local filesystem directory or S3 URI for a given Delta table."""
    cat_id = catalog or "warehouse"
    if cat_id != "warehouse":
        from web.warehouses import get_catalog
        c = get_catalog(cat_id)
        if c:
            if c.get("is_mounted") and c.get("type") == "s3":
                bucket = c.get("config", {}).get("bucket", "localspark")
                return f"s3://{bucket}/{schema_name}/{table_name}", cat_id
            p = os.path.join(c["path"], schema_name, table_name)
            if os.path.exists(p):
                return p, cat_id
            p_flat = os.path.join(c["path"], table_name)
            if os.path.exists(p_flat):
                return p_flat, cat_id

    # Standard warehouse checks
    path_std = os.path.join(WAREHOUSE_DIR, schema_name, table_name)
    if os.path.exists(path_std):
        return path_std, "warehouse"

    path_flat = os.path.join(WAREHOUSE_DIR, table_name)
    if os.path.exists(path_flat):
        return path_flat, "warehouse"

    # Fallback to local relative warehouse directory
    local_alt = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "warehouse", schema_name, table_name))
    if os.path.exists(local_alt):
        return local_alt, "warehouse"

    return path_std, cat_id


def get_delta_table(target_path: str, version: Optional[int] = None) -> DeltaTable:
    """Instantiates a DeltaTable with appropriate storage options for S3 or local storage."""
    if target_path.startswith("s3://"):
        from web.mounts import load_mounts, get_s3_storage_options
        s3_mount = next((m for m in load_mounts() if m.get("type") == "s3"), None)
        storage_options = get_s3_storage_options(s3_mount.get("config", {})) if s3_mount else {}
        if version is not None:
            return DeltaTable(target_path, version=version, storage_options=storage_options)
        return DeltaTable(target_path, storage_options=storage_options)
    else:
        if not os.path.exists(target_path):
            raise FileNotFoundError(f"Delta table not found at {target_path}")
        if version is not None:
            return DeltaTable(target_path, version=version)
        return DeltaTable(target_path)


def sanitize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Ensures row dictionary values are JSON serializable."""
    res = {}
    for k, v in row.items():
        if v is None:
            res[k] = None
        elif isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
            res[k] = v.isoformat()
        elif isinstance(v, bytes):
            res[k] = v.hex()
        elif hasattr(v, "item"):  # numpy types
            try:
                res[k] = v.item()
            except Exception:
                res[k] = str(v)
        else:
            try:
                json.dumps(v)
                res[k] = v
            except (TypeError, OverflowError):
                res[k] = str(v)
    return res


def _masked(arrow_tbl, user, catalog: str, schema_name: str, table_name: str):
    """Masks an Arrow table for `user` (None = least privilege, never raw). Row counts stay real; displayed values are masked."""
    from web.governance import gateway
    return gateway.mask_arrow(arrow_tbl, catalog, schema_name, table_name, user)


def _masked_df(df, user, catalog: str, schema_name: str, table_name: str):
    import pyarrow as pa
    if df is None or df.empty:
        return df
    return _masked(pa.Table.from_pandas(df, preserve_index=False), user, catalog, schema_name, table_name).to_pandas()


def get_version_preview(target_path: str, version: int, limit: int = 50, *, user=None, catalog: str = "warehouse",
                        schema_name: str = "", table_name: str = "") -> Dict[str, Any]:
    """
    Loads a specific historical version of a Delta table and returns columns and sample rows,
    masked for `user` (governance): historical versions carry the same tagged columns as the live table.
    """
    dt = get_delta_table(target_path, version=version)
    schema_fields = dt.schema().fields
    columns = [
        {
            "name": f.name,
            "type": str(f.type),
            "nullable": f.nullable
        }
        for f in schema_fields
    ]

    # Convert to pyarrow then pandas for sample rows
    arrow_tbl = dt.to_pyarrow_table()
    total_rows = arrow_tbl.num_rows

    # Read slice for preview
    slice_tbl = _masked(arrow_tbl.slice(0, limit), user, catalog, schema_name, table_name)
    df = slice_tbl.to_pandas()
    sample_rows = [sanitize_row(r) for r in df.to_dict(orient="records")]

    return {
        "version": version,
        "total_rows": total_rows,
        "columns": columns,
        "sample_rows": sample_rows
    }


def compare_table_versions(
    target_path: str,
    v1: int,
    v2: int,
    sample_limit: int = 50,
    *,
    user=None,
    catalog: str = "warehouse",
    schema_name: str = "",
    table_name: str = ""
) -> Dict[str, Any]:
    """
    Computes a deep visual difference between version v1 (earlier/reference) and v2 (later/target).
    Returns schema changes, net row counts, and sample added/deleted rows.
    """
    dt_1 = get_delta_table(target_path, version=v1)
    dt_2 = get_delta_table(target_path, version=v2)

    # 1. Schema Diff
    fields_1 = {f.name: str(f.type) for f in dt_1.schema().fields}
    fields_2 = {f.name: str(f.type) for f in dt_2.schema().fields}

    added_cols = [
        {"name": col, "type": fields_2[col]}
        for col in fields_2 if col not in fields_1
    ]
    removed_cols = [
        {"name": col, "type": fields_1[col]}
        for col in fields_1 if col not in fields_2
    ]
    modified_cols = [
        {"name": col, "from_type": fields_1[col], "to_type": fields_2[col]}
        for col in fields_1 if col in fields_2 and fields_1[col] != fields_2[col]
    ]

    schema_diff = {
        "is_identical": len(added_cols) == 0 and len(removed_cols) == 0 and len(modified_cols) == 0,
        "added_columns": added_cols,
        "removed_columns": removed_cols,
        "modified_columns": modified_cols
    }

    # 2. Row Counts and Data Diff via DuckDB in-memory registration
    conn = duckdb.connect(":memory:")
    tbl_1 = dt_1.to_pyarrow_table()
    tbl_2 = dt_2.to_pyarrow_table()

    conn.register("t1", tbl_1)
    conn.register("t2", tbl_2)

    count_1 = int(tbl_1.num_rows)
    count_2 = int(tbl_2.num_rows)
    net_diff = count_2 - count_1

    # Intersecting columns for data comparison if schemas diverged
    common_cols = [c for c in fields_1 if c in fields_2]
    added_rows: List[Dict[str, Any]] = []
    deleted_rows: List[Dict[str, Any]] = []
    added_count = 0
    deleted_count = 0

    if common_cols:
        col_list_sql = ", ".join([f'"{c}"' for c in common_cols])
        try:
            # Rows in v2 but not v1 (Added)
            added_query = f"SELECT {col_list_sql} FROM t2 EXCEPT SELECT {col_list_sql} FROM t1"
            added_cnt_res = conn.sql(f"SELECT COUNT(*) FROM ({added_query})").fetchone()
            added_count = int(added_cnt_res[0]) if added_cnt_res else 0
            added_df = _masked_df(conn.sql(f"{added_query} LIMIT {sample_limit}").df(), user, catalog, schema_name, table_name)
            added_rows = [sanitize_row(r) for r in added_df.to_dict(orient="records")]
        except Exception as e:
            logger.warning(f"Failed to calculate added rows: {e}")

        try:
            # Rows in v1 but not v2 (Deleted)
            deleted_query = f"SELECT {col_list_sql} FROM t1 EXCEPT SELECT {col_list_sql} FROM t2"
            deleted_cnt_res = conn.sql(f"SELECT COUNT(*) FROM ({deleted_query})").fetchone()
            deleted_count = int(deleted_cnt_res[0]) if deleted_cnt_res else 0
            deleted_df = _masked_df(conn.sql(f"{deleted_query} LIMIT {sample_limit}").df(), user, catalog, schema_name, table_name)
            deleted_rows = [sanitize_row(r) for r in deleted_df.to_dict(orient="records")]
        except Exception as e:
            logger.warning(f"Failed to calculate deleted rows: {e}")

    # Columns list for UI rendering
    display_columns = [
        {"name": f.name, "type": str(f.type)}
        for f in dt_2.schema().fields
    ]

    return {
        "v1": v1,
        "v2": v2,
        "v1_row_count": count_1,
        "v2_row_count": count_2,
        "net_diff": net_diff,
        "added_count": added_count,
        "deleted_count": deleted_count,
        "schema_diff": schema_diff,
        "columns": display_columns,
        "added_rows_sample": added_rows,
        "deleted_rows_sample": deleted_rows
    }


def restore_table_to_version(
    target_path: str,
    target_version: int,
    user: str = "martin"
) -> Dict[str, Any]:
    """
    Restores the Delta table to target_version using native Delta Lake rollback.
    Creates a new commit in _delta_log with operation 'RESTORE'.
    """
    if not (target_path.startswith("s3://") or os.path.exists(target_path)):
        raise FileNotFoundError(f"Delta table not found at {target_path}")

    start_time = time.perf_counter()
    dt = get_delta_table(target_path)
    old_version = dt.version()

    if target_version == old_version:
        raise ValueError(f"Table is already at version {target_version}")

    if target_version < 0 or target_version > old_version:
        raise ValueError(f"Invalid target version {target_version}. Table history range is 0 to {old_version}.")

    # Execute native Delta restore
    dt.restore(target_version)

    # Re-read restored table metadata
    dt_restored = get_delta_table(target_path)
    new_version = dt_restored.version()
    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

    # Log to audit history
    try:
        from web.audit import log_query
        table_name = os.path.basename(target_path)
        log_query(
            query_text=f"RESTORE TABLE {table_name} TO VERSION AS OF {target_version};",
            duration_ms=duration_ms,
            rows_produced=0,
            status="SUCCESS",
            client="DELTA_TIME_TRAVEL",
            is_mutation=True,
            user=user
        )
    except Exception as e:
        logger.warning(f"Could not log restore query: {e}")

    return {
        "status": "SUCCESS",
        "previous_version": old_version,
        "restored_from_version": target_version,
        "new_version": new_version,
        "duration_ms": duration_ms,
        "timestamp_iso": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    }
