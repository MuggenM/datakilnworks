import os
import sys
import time
import math
import json
import logging
import decimal
import datetime
from typing import Optional, Dict, Any, List

import psutil
import pandas as pd
import numpy as np
import duckdb
import duckrun
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from web.warehouses import sync_catalogs_with_duckrun, load_catalogs

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [compute-worker] %(message)s")
logger = logging.getLogger("localspark.compute_worker")

# Environment & Node Identity Configuration
NODE_ID = os.getenv("WORKER_NODE_ID", "compute-node-01")
NODE_NAME = os.getenv("WORKER_NODE_NAME", f"Compute Node ({NODE_ID})")
ASSIGNED_WAREHOUSE_ID = os.getenv("ASSIGNED_WAREHOUSE_ID", "wh_starter")
WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
MAX_MEMORY = os.getenv("MAX_MEMORY", "2GB")
THREADS = int(os.getenv("THREADS", "2"))
WORKER_PORT = int(os.getenv("WORKER_PORT", "8001"))

START_TIME = time.time()

app = FastAPI(
    title=f"LocalSpark Compute Worker - {NODE_ID}",
    description=f"Vectorized C++ DuckDB compute node for {NODE_NAME}",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.middleware("http")
async def require_compute_token(request, call_next):
    """Every route except the health probe needs the studio's shared secret (see web/compute_auth.py)."""
    from fastapi.responses import JSONResponse
    from web.compute_auth import COMPUTE_TOKEN_HEADER, PUBLIC_PATHS, token_is_valid
    if request.method != "OPTIONS" and request.url.path not in PUBLIC_PATHS:
        if not token_is_valid(request.headers.get(COMPUTE_TOKEN_HEADER, "")):
            return JSONResponse(status_code=401, content={"detail": "Compute token required."})
    return await call_next(request)


# Execution telemetry
metrics = {
    "queries_total": 0,
    "queries_active": 0,
    "queries_failed": 0,
    "total_duration_ms": 0.0,
    "last_query_at": None,
    "last_query_preview": None
}

# Worker Duckrun Session
_worker_conn = None

def get_worker_conn():
    """Initializes and returns the Duckrun connection configured with node limits."""
    global _worker_conn
    if _worker_conn is None:
        logger.info(f"Initializing Duckrun engine on {NODE_ID} for {WAREHOUSE_DIR}...")
        _worker_conn = duckrun.connect(WAREHOUSE_DIR, read_only=False)
        try:
            _worker_conn.sql(f"SET threads = {THREADS};")
            _worker_conn.sql(f"SET max_memory = '{MAX_MEMORY}';")
            logger.info(f"Configured DuckDB compute limits: threads={THREADS}, max_memory={MAX_MEMORY}")
        except Exception as e:
            logger.warning(f"Failed setting compute limits: {e}")
        sync_catalogs_with_duckrun(_worker_conn)
        try:
            from web.ai_sql import register_duckdb_ai_functions
            register_duckdb_ai_functions(_worker_conn.con)
        except Exception as e:
            logger.warning(f"Failed registering DuckDB AI UDFs on worker {NODE_ID}: {e}")
    return _worker_conn

def clean_json_val(v: Any) -> Any:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    elif isinstance(v, (np.floating,)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v)
    elif isinstance(v, (np.integer,)):
        return int(v)
    elif isinstance(v, (np.bool_,)):
        return bool(v)
    elif isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return v.isoformat()
    elif isinstance(v, decimal.Decimal):
        if v.is_nan() or v.is_infinite():
            return None
        return float(v)
    elif isinstance(v, bytes):
        return v.hex()
    elif isinstance(v, (list, tuple, set)):
        return [clean_json_val(x) for x in v]
    elif isinstance(v, dict):
        return {str(k): clean_json_val(sub_v) for k, sub_v in v.items()}
    return v

def serialize_row(row_dict: Any) -> Any:
    if not isinstance(row_dict, dict):
        return clean_json_val(row_dict)
    return {str(k): clean_json_val(v) for k, v in row_dict.items()}

ACTIVE_WORKER_QUERIES: Dict[str, Dict[str, Any]] = {}

class ComputeExecuteRequest(BaseModel):
    query: str
    warehouse_id: Optional[str] = None
    catalog: Optional[str] = None
    limit: Optional[int] = None
    execution_id: Optional[str] = None

@app.on_event("startup")
async def startup_event():
    logger.info(f"Compute Worker starting: id={NODE_ID}, name='{NODE_NAME}', warehouse={ASSIGNED_WAREHOUSE_ID}")
    try:
        get_worker_conn()
        logger.info(f"Compute Worker {NODE_ID} ready for queries.")
    except Exception as e:
        logger.error(f"Error during worker initialization: {e}")

@app.get("/")
@app.get("/health")
def healthcheck():
    return {
        "status": "ok",
        "node_id": NODE_ID,
        "node_name": NODE_NAME,
        "warehouse_id": ASSIGNED_WAREHOUSE_ID,
        "state": "healthy"
    }

@app.get("/api/compute/status")
def get_compute_status():
    uptime_sec = round(time.time() - START_TIME, 1)
    mins, secs = divmod(int(uptime_sec), 60)
    hours, mins = divmod(mins, 60)
    uptime_human = f"{hours}h {mins}m {secs}s" if hours > 0 else f"{mins}m {secs}s"

    try:
        proc = psutil.Process()
        cpu_pct = psutil.cpu_percent(interval=None)
        rss_mb = round(proc.memory_info().rss / (1024 * 1024), 1)
        mem_pct = round(proc.memory_percent(), 2)
    except Exception:
        cpu_pct = 0.0
        rss_mb = 0.0
        mem_pct = 0.0

    avg_duration = round(metrics["total_duration_ms"] / max(1, metrics["queries_total"]), 2)

    return {
        "node_id": NODE_ID,
        "node_name": NODE_NAME,
        "warehouse_id": ASSIGNED_WAREHOUSE_ID,
        "status": "ONLINE",
        "uptime_seconds": uptime_sec,
        "uptime_human": uptime_human,
        "threads": THREADS,
        "max_memory": MAX_MEMORY,
        "duckdb_version": duckdb.__version__,
        "duckrun_version": getattr(duckrun, "__version__", "0.4.68"),
        "queries_total": metrics["queries_total"],
        "queries_active": metrics["queries_active"],
        "queries_failed": metrics["queries_failed"],
        "avg_duration_ms": avg_duration,
        "cpu_percent": cpu_pct,
        "memory_rss_mb": rss_mb,
        "memory_percent": mem_pct,
        "last_query_at": metrics["last_query_at"],
        "last_query_preview": metrics["last_query_preview"]
    }

@app.post("/api/compute/execute")
def execute_query(req: ComputeExecuteRequest):
    query = req.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Empty query string")

    exec_id = req.execution_id
    metrics["queries_active"] += 1
    start_time = time.perf_counter()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur = None
    try:
        conn = get_worker_conn()

        is_delta_special = (
            any(k in query.lower() for k in ["describe detail", "describe history", "restore table", "vacuum"])
            or any(query.strip().lower().startswith(p) for p in ["insert ", "update ", "delete ", "merge "])
        )

        if not is_delta_special:
            cur = conn.con.cursor()
            if exec_id:
                ACTIVE_WORKER_QUERIES[exec_id] = {"cursor": cur, "cancelled": False}
        else:
            if exec_id:
                ACTIVE_WORKER_QUERIES[exec_id] = {"cursor": conn.con, "cancelled": False}

        if exec_id and ACTIVE_WORKER_QUERIES.get(exec_id, {}).get("cancelled"):
            return {
                "success": False,
                "cancelled": True,
                "error": "Query execution cancelled by user.",
                "executed_by": NODE_ID,
                "node_name": NODE_NAME,
                "warehouse_id": ASSIGNED_WAREHOUSE_ID,
                "elapsed_ms": 0,
                "is_mutation": False
            }

        # Switch catalog context if specified
        if req.catalog and req.catalog != "warehouse":
            try:
                (cur if cur else conn).sql(f'USE "{req.catalog}";')
            except Exception:
                pass
        else:
            try:
                (cur if cur else conn).sql("USE warehouse;")
            except Exception:
                pass

        if not is_delta_special:
            res = cur.sql(query)
        else:
            res = conn.sql(query)

        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

        # Update metrics
        metrics["queries_total"] += 1
        metrics["total_duration_ms"] += elapsed_ms
        metrics["last_query_at"] = now_str
        metrics["last_query_preview"] = (query[:60] + "...") if len(query) > 60 else query

        if res is not None and hasattr(res, "df"):
            df = res.df()
            if req.limit and len(df) > req.limit:
                df = df.head(req.limit)
            columns = [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
            rows = [serialize_row(row) for row in df.to_dict(orient="records")]
            return {
                "success": True,
                "executed_by": NODE_ID,
                "node_name": NODE_NAME,
                "warehouse_id": ASSIGNED_WAREHOUSE_ID,
                "elapsed_ms": elapsed_ms,
                "is_mutation": False,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows)
            }
        else:
            try:
                conn.refresh()
            except Exception:
                pass
            return {
                "success": True,
                "executed_by": NODE_ID,
                "node_name": NODE_NAME,
                "warehouse_id": ASSIGNED_WAREHOUSE_ID,
                "elapsed_ms": elapsed_ms,
                "is_mutation": True,
                "message": f"Statement executed and committed successfully on {NODE_ID}.",
                "row_count": 0
            }
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        metrics["queries_failed"] += 1
        is_interrupted = (
            (exec_id and ACTIVE_WORKER_QUERIES.get(exec_id, {}).get("cancelled", False))
            or "interrupted" in str(e).lower()
            or "interruptexception" in type(e).__name__.lower()
        )
        if is_interrupted:
            logger.info(f"Query {exec_id} on {NODE_ID} was cancelled.")
            return {
                "success": False,
                "cancelled": True,
                "error": "Query execution was cancelled by user.",
                "executed_by": NODE_ID,
                "node_name": NODE_NAME,
                "warehouse_id": ASSIGNED_WAREHOUSE_ID,
                "elapsed_ms": elapsed_ms,
                "is_mutation": False
            }
        logger.error(f"Query execution failed on {NODE_ID}: {e}")
        return {
            "success": False,
            "error": str(e),
            "executed_by": NODE_ID,
            "node_name": NODE_NAME,
            "warehouse_id": ASSIGNED_WAREHOUSE_ID,
            "elapsed_ms": elapsed_ms,
            "is_mutation": False
        }
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if exec_id:
            ACTIVE_WORKER_QUERIES.pop(exec_id, None)
        metrics["queries_active"] = max(0, metrics["queries_active"] - 1)


@app.post("/api/compute/cancel/{execution_id}")
def cancel_worker_query(execution_id: str):
    info = ACTIVE_WORKER_QUERIES.get(execution_id)
    if not info:
        return {"success": False, "message": "Query not found or already completed."}
    info["cancelled"] = True
    cur = info.get("cursor")
    if cur is not None:
        try:
            cur.interrupt()
            logger.info(f"Interrupted query {execution_id} on worker {NODE_ID}")
            return {"success": True, "message": "Interrupt signal sent to worker cursor."}
        except Exception as e:
            logger.warning(f"Error interrupting query {execution_id}: {e}")
            return {"success": False, "error": str(e)}
    return {"success": True, "message": "Marked cancelled"}

@app.post("/api/compute/refresh")
def refresh_catalogs():
    try:
        conn = get_worker_conn()
        sync_catalogs_with_duckrun(conn)
        if hasattr(conn, "refresh"):
            conn.refresh()
        return {"success": True, "node_id": NODE_ID, "message": "Worker catalogs and tables refreshed"}
    except Exception as e:
        return {"success": False, "node_id": NODE_ID, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web.compute_worker:app", host="0.0.0.0", port=WORKER_PORT, reload=False)
