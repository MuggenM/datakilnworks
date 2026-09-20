import os
import json
import time
import uuid
import datetime
import logging
from typing import Optional, Dict, Any, List, Tuple
from deltalake import DeltaTable

logger = logging.getLogger("localspark.warehouses")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
SQL_WAREHOUSES_FILE = os.path.join(METADATA_DIR, "sql_warehouses.json")
CATALOGS_FILE = os.path.join(METADATA_DIR, "catalogs.json")

CLUSTER_SIZES = {
    "2X-Small": {"threads": 1, "max_memory": "1GB", "description": "1 vCPU, 1 GB RAM (Ultra-light queries)"},
    "X-Small": {"threads": 2, "max_memory": "2GB", "description": "2 vCPU, 2 GB RAM (Ad-hoc query development)"},
    "Small": {"threads": 2, "max_memory": "4GB", "description": "2 vCPU, 4 GB RAM (Balanced starter warehouse)"},
    "Medium": {"threads": 4, "max_memory": "8GB", "description": "4 vCPU, 8 GB RAM (Dashboards & aggregations)"},
    "Large": {"threads": 8, "max_memory": "16GB", "description": "8 vCPU, 16 GB RAM (Heavy analytical workloads)"},
    "2X-Large": {"threads": 16, "max_memory": "32GB", "description": "16 vCPU, 32 GB RAM (High-concurrency batch)"},
    "Custom": {"threads": 4, "max_memory": "4GB", "description": "Customized CPU and RAM limits"}
}

# Clustered Docker Compute Worker Defaults
DEFAULT_WORKER_ENDPOINTS = {
    "wh_starter": os.getenv("WH_STARTER_ENDPOINT", "http://compute-node-01:8001"),
    "wh_analytics_pro": os.getenv("WH_ANALYTICS_ENDPOINT", "http://compute-node-02:8002"),
    "wh_etl_batch": os.getenv("WH_ETL_ENDPOINT", "http://compute-node-03:8003"),
}

KNOWN_WORKER_NODES = [
    {
        "node_id": "compute-node-01",
        "node_name": "Compute Worker 01 (Starter Node)",
        "warehouse_id": "wh_starter",
        "endpoint": os.getenv("WH_STARTER_ENDPOINT", "http://compute-node-01:8001"),
        "host_endpoint": "http://localhost:8001",
        "allocated_cores": 2,
        "allocated_ram": "2GB"
    },
    {
        "node_id": "compute-node-02",
        "node_name": "Compute Worker 02 (Analytics Pro)",
        "warehouse_id": "wh_analytics_pro",
        "endpoint": os.getenv("WH_ANALYTICS_ENDPOINT", "http://compute-node-02:8002"),
        "host_endpoint": "http://localhost:8002",
        "allocated_cores": 4,
        "allocated_ram": "4GB"
    },
    {
        "node_id": "compute-node-03",
        "node_name": "Compute Worker 03 (ETL Batch)",
        "warehouse_id": "wh_etl_batch",
        "endpoint": os.getenv("WH_ETL_ENDPOINT", "http://compute-node-03:8003"),
        "host_endpoint": "http://localhost:8003",
        "allocated_cores": 4,
        "allocated_ram": "4GB"
    }
]

# ==============================================================================
# SQL WAREHOUSES (COMPUTE ENDPOINTS)
# ==============================================================================

def get_default_sql_warehouses() -> List[Dict[str, Any]]:
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return [
        {
            "id": "wh_starter",
            "name": "Serverless Starter Warehouse",
            "cluster_size": "Small",
            "threads": 2,
            "max_memory": "4GB",
            "endpoint": DEFAULT_WORKER_ENDPOINTS["wh_starter"],
            "auto_stop_mins": 10,
            "state": "RUNNING",
            "is_default": True,
            "channel": "DuckDB 1.5.5",
            "query_count": 0,
            "ray_workers": 2,
            "min_workers": 0,
            "max_workers": 16,
            "created_at": now_str,
            "last_active_at": now_str
        },
        {
            "id": "wh_analytics_pro",
            "name": "Analytics Pro Warehouse",
            "cluster_size": "Large",
            "threads": 8,
            "max_memory": "8GB",
            "endpoint": DEFAULT_WORKER_ENDPOINTS["wh_analytics_pro"],
            "auto_stop_mins": 20,
            "state": "RUNNING",
            "is_default": False,
            "channel": "DuckDB 1.5.5 (Vectorized)",
            "query_count": 0,
            "ray_workers": 1,
            "min_workers": 0,
            "max_workers": 16,
            "created_at": now_str,
            "last_active_at": now_str
        },
        {
            "id": "wh_etl_batch",
            "name": "ETL & Maintenance Warehouse",
            "cluster_size": "Medium",
            "threads": 4,
            "max_memory": "4GB",
            "endpoint": DEFAULT_WORKER_ENDPOINTS["wh_etl_batch"],
            "auto_stop_mins": 15,
            "state": "STOPPED",
            "is_default": False,
            "channel": "DuckDB 1.5.5",
            "query_count": 0,
            "ray_workers": 0,
            "min_workers": 0,
            "max_workers": 16,
            "created_at": now_str,
            "last_active_at": now_str
        }
    ]

def load_sql_warehouses() -> List[Dict[str, Any]]:
    os.makedirs(METADATA_DIR, exist_ok=True)
    if not os.path.exists(SQL_WAREHOUSES_FILE):
        defaults = get_default_sql_warehouses()
        save_sql_warehouses(defaults)
        return defaults
    try:
        with open(SQL_WAREHOUSES_FILE, "r") as f:
            data = json.load(f)
            wh_list = data.get("warehouses", [])
            for w in wh_list:
                if "endpoint" not in w or not w["endpoint"]:
                    w["endpoint"] = DEFAULT_WORKER_ENDPOINTS.get(w.get("id"), "")
                w.setdefault("ray_workers", 2 if w.get("id") == "wh_starter" else 1)
                w.setdefault("min_workers", 0)
                w.setdefault("max_workers", 16)
            return wh_list
    except Exception as e:
        logger.error(f"Failed to load sql_warehouses.json: {e}")
        return get_default_sql_warehouses()

def save_sql_warehouses(warehouses: List[Dict[str, Any]]):
    os.makedirs(METADATA_DIR, exist_ok=True)
    try:
        with open(SQL_WAREHOUSES_FILE, "w") as f:
            json.dump({"warehouses": warehouses, "updated_at": datetime.datetime.now().isoformat()}, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save sql_warehouses.json: {e}")

def get_sql_warehouse(wh_id: str) -> Optional[Dict[str, Any]]:
    warehouses = load_sql_warehouses()
    return next((w for w in warehouses if w["id"] == wh_id), None)

def create_sql_warehouse(
    name: str,
    cluster_size: str = "Small",
    threads: Optional[int] = None,
    max_memory: Optional[str] = None,
    auto_stop_mins: int = 10,
    is_default: bool = False,
    endpoint: Optional[str] = None,
    ray_workers: int = 1,
    min_workers: int = 0,
    max_workers: int = 16
) -> Dict[str, Any]:
    warehouses = load_sql_warehouses()
    preset = CLUSTER_SIZES.get(cluster_size, CLUSTER_SIZES["Small"])
    
    t = threads if threads is not None else preset["threads"]
    m = max_memory if max_memory is not None else preset["max_memory"]
    wh_id = f"wh_{uuid.uuid4().hex[:8]}"
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if is_default:
        for w in warehouses:
            w["is_default"] = False

    new_wh = {
        "id": wh_id,
        "name": name.strip(),
        "cluster_size": cluster_size,
        "threads": int(t),
        "max_memory": str(m),
        "auto_stop_mins": int(auto_stop_mins),
        "endpoint": (endpoint or "").strip(),
        "state": "RUNNING",
        "is_default": is_default,
        "channel": "DuckDB 1.5.5",
        "query_count": 0,
        "ray_workers": int(ray_workers),
        "min_workers": int(min_workers),
        "max_workers": int(max_workers),
        "created_at": now_str,
        "last_active_at": now_str
    }
    warehouses.append(new_wh)
    save_sql_warehouses(warehouses)
    return new_wh

def update_sql_warehouse(wh_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    warehouses = load_sql_warehouses()
    target = None
    for w in warehouses:
        if w["id"] == wh_id:
            target = w
            break
    if not target:
        return None

    if "name" in updates and updates["name"]:
        target["name"] = updates["name"].strip()
    if "cluster_size" in updates:
        cs = updates["cluster_size"]
        target["cluster_size"] = cs
        if cs in CLUSTER_SIZES and cs != "Custom":
            target["threads"] = CLUSTER_SIZES[cs]["threads"]
            target["max_memory"] = CLUSTER_SIZES[cs]["max_memory"]
    if "threads" in updates and updates["threads"] is not None:
        target["threads"] = int(updates["threads"])
    if "max_memory" in updates and updates["max_memory"]:
        target["max_memory"] = str(updates["max_memory"])
    if "auto_stop_mins" in updates:
        target["auto_stop_mins"] = int(updates["auto_stop_mins"])
    if "endpoint" in updates:
        target["endpoint"] = str(updates["endpoint"]).strip() if updates["endpoint"] else ""
    if "ray_workers" in updates and updates["ray_workers"] is not None:
        target["ray_workers"] = int(updates["ray_workers"])
    if "min_workers" in updates and updates["min_workers"] is not None:
        target["min_workers"] = int(updates["min_workers"])
    if "max_workers" in updates and updates["max_workers"] is not None:
        target["max_workers"] = int(updates["max_workers"])
    if "is_default" in updates and updates["is_default"]:
        for w in warehouses:
            w["is_default"] = (w["id"] == wh_id)

    save_sql_warehouses(warehouses)
    return target

def get_compute_nodes_status() -> List[Dict[str, Any]]:
    """Polls real-time telemetry from all clustered Docker compute worker nodes."""
    results = []
    import httpx
    from web.compute_auth import compute_headers
    for node in KNOWN_WORKER_NODES:
        endpoints_to_try = [node["endpoint"], node.get("host_endpoint", "")]
        node_res = {
            "node_id": node["node_id"],
            "node_name": node["node_name"],
            "warehouse_id": node["warehouse_id"],
            "endpoint": node["endpoint"],
            "allocated_cores": node["allocated_cores"],
            "allocated_ram": node["allocated_ram"],
            "status": "OFFLINE",
            "latency_ms": None,
            "uptime_human": "-",
            "cpu_percent": 0.0,
            "memory_rss_mb": 0.0,
            "memory_percent": 0.0,
            "queries_total": 0,
            "queries_active": 0,
            "queries_failed": 0,
            "avg_duration_ms": 0.0,
            "duckdb_version": "1.5.5",
            "duckrun_version": "0.4.68",
            "last_query_at": None
        }
        for ep in endpoints_to_try:
            if not ep:
                continue
            try:
                t0 = time.perf_counter()
                with httpx.Client(timeout=1.0, headers=compute_headers()) as client:
                    resp = client.get(f"{ep}/api/compute/status")
                latency = round((time.perf_counter() - t0) * 1000, 1)
                if resp.status_code == 200:
                    data = resp.json()
                    node_res.update({
                        "status": "ONLINE",
                        "latency_ms": latency,
                        "uptime_human": data.get("uptime_human", "-"),
                        "cpu_percent": data.get("cpu_percent", 0.0),
                        "memory_rss_mb": data.get("memory_rss_mb", 0.0),
                        "memory_percent": data.get("memory_percent", 0.0),
                        "queries_total": data.get("queries_total", 0),
                        "queries_active": data.get("queries_active", 0),
                        "queries_failed": data.get("queries_failed", 0),
                        "avg_duration_ms": data.get("avg_duration_ms", 0.0),
                        "duckdb_version": data.get("duckdb_version", "1.5.5"),
                        "duckrun_version": data.get("duckrun_version", "0.4.68"),
                        "last_query_at": data.get("last_query_at")
                    })
                    break
            except Exception:
                continue
        results.append(node_res)
    return results

def start_sql_warehouse(wh_id: str) -> Optional[Dict[str, Any]]:
    warehouses = load_sql_warehouses()
    for w in warehouses:
        if w["id"] == wh_id:
            w["state"] = "RUNNING"
            w["last_active_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_sql_warehouses(warehouses)
            return w
    return None

def stop_sql_warehouse(wh_id: str) -> Optional[Dict[str, Any]]:
    warehouses = load_sql_warehouses()
    for w in warehouses:
        if w["id"] == wh_id:
            w["state"] = "STOPPED"
            save_sql_warehouses(warehouses)
            return w
    return None

def delete_sql_warehouse(wh_id: str) -> bool:
    warehouses = load_sql_warehouses()
    target = next((w for w in warehouses if w["id"] == wh_id), None)
    if not target or target.get("is_default"):
        return False
    warehouses = [w for w in warehouses if w["id"] != wh_id]
    save_sql_warehouses(warehouses)
    return True

def touch_sql_warehouse(wh_id: str):
    try:
        warehouses = load_sql_warehouses()
        for w in warehouses:
            if w["id"] == wh_id:
                w["query_count"] = w.get("query_count", 0) + 1
                w["last_active_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                save_sql_warehouses(warehouses)
                break
    except Exception:
        pass

def apply_warehouse_compute(conn, warehouse_id: Optional[str] = None) -> Dict[str, Any]:
    """Applies threads and max_memory from the active/requested SQL Warehouse onto the DuckDB connection."""
    warehouses = load_sql_warehouses()
    wh = None
    if warehouse_id:
        wh = next((w for w in warehouses if w["id"] == warehouse_id), None)
    if not wh:
        wh = next((w for w in warehouses if w.get("is_default")), warehouses[0] if warehouses else None)

    if not wh:
        return {"id": "wh_starter", "name": "Default Starter", "threads": 4, "max_memory": "4GB"}

    # Auto-resume warehouse if stopped
    if wh.get("state") == "STOPPED":
        start_sql_warehouse(wh["id"])
        wh["state"] = "RUNNING"

    threads = int(wh.get("threads", 4))
    max_memory = str(wh.get("max_memory", "4GB"))

    try:
        conn.sql(f"SET threads = {threads};")
        conn.sql(f"SET max_memory = '{max_memory}';")
    except Exception as e:
        logger.warning(f"Could not apply compute settings for warehouse {wh['id']}: {e}")

    touch_sql_warehouse(wh["id"])
    return wh

# ==============================================================================
# STORAGE WAREHOUSES & CATALOGS (UNITY CATALOG)
# ==============================================================================

def get_default_catalogs() -> List[Dict[str, Any]]:
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return [
        {
            "id": "warehouse",
            "name": "Main Lakehouse (Default)",
            "path": WAREHOUSE_DIR,
            "is_default": True,
            "read_only": False,
            "description": "Primary production Delta Lakehouse storage",
            "created_at": now_str
        },
        {
            "id": "dev_catalog",
            "name": "Development Lakehouse",
            "path": os.path.join(WAREHOUSE_DIR, "catalogs", "dev_catalog"),
            "is_default": False,
            "read_only": False,
            "description": "Isolated sandbox & staging environment for testing",
            "created_at": now_str
        },
        {
            "id": "analytics_catalog",
            "name": "Analytics Data Warehouse",
            "path": os.path.join(WAREHOUSE_DIR, "catalogs", "analytics_catalog"),
            "is_default": False,
            "read_only": False,
            "description": "Aggregated business intelligence & reporting marts",
            "created_at": now_str
        }
    ]

def load_catalogs() -> List[Dict[str, Any]]:
    os.makedirs(METADATA_DIR, exist_ok=True)
    if not os.path.exists(CATALOGS_FILE):
        defaults = get_default_catalogs()
        save_catalogs(defaults)
        for cat in defaults:
            os.makedirs(os.path.join(cat["path"], "dbo"), exist_ok=True)
        return defaults
    try:
        with open(CATALOGS_FILE, "r") as f:
            data = json.load(f)
            return data.get("catalogs", [])
    except Exception as e:
        logger.error(f"Failed to load catalogs.json: {e}")
        return get_default_catalogs()

def save_catalogs(catalogs: List[Dict[str, Any]]):
    os.makedirs(METADATA_DIR, exist_ok=True)
    try:
        with open(CATALOGS_FILE, "w") as f:
            json.dump({"catalogs": catalogs, "updated_at": datetime.datetime.now().isoformat()}, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save catalogs.json: {e}")

def get_catalog(cat_id: str) -> Optional[Dict[str, Any]]:
    catalogs = load_catalogs()
    cat = next((c for c in catalogs if c["id"] == cat_id), None)
    if cat:
        return cat

    # Check external storage mounts
    try:
        from web.mounts import load_mounts
        for m in load_mounts():
            if m.get("catalog_name") == cat_id or m.get("id") == cat_id:
                cfg = m.get("config", {})
                bucket = cfg.get("bucket", "localspark")
                path = f"s3://{bucket}" if m.get("type") == "s3" else cfg.get("path", "")
                return {
                    "id": m.get("catalog_name"),
                    "catalog_name": m.get("catalog_name"),
                    "name": m.get("name"),
                    "type": m.get("type"),
                    "is_mounted": True,
                    "mount_id": m.get("id"),
                    "config": cfg,
                    "read_only": m.get("read_only", False),
                    "path": path,
                    "description": m.get("description", "")
                }
    except Exception as e:
        logger.warning(f"Error checking mounts in get_catalog: {e}")
    return None

def create_catalog(
    name: str,
    cat_id: str,
    description: str = "",
    path: Optional[str] = None,
    read_only: bool = False,
    owner: str = "admin"
) -> Dict[str, Any]:
    catalogs = load_catalogs()
    cid = cat_id.strip().lower().replace("-", "_").replace(" ", "_")
    if any(c["id"] == cid for c in catalogs):
        raise ValueError(f"Catalog '{cid}' already exists.")

    cpath = path or os.path.join(WAREHOUSE_DIR, "catalogs", cid)
    os.makedirs(os.path.join(cpath, "dbo"), exist_ok=True)

    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    clean_owner = owner.strip().lower() if owner else "admin"
    new_cat = {
        "id": cid,
        "name": name.strip(),
        "path": cpath,
        "is_default": False,
        "read_only": read_only,
        "description": description.strip() or f"Lakehouse catalog {name}",
        "created_at": now_str,
        "owner": clean_owner,
        "created_by": clean_owner
    }
    catalogs.append(new_cat)
    save_catalogs(catalogs)
    return new_cat

def delete_catalog(cat_id: str) -> bool:
    catalogs = load_catalogs()
    target = next((c for c in catalogs if c["id"] == cat_id), None)
    if target:
        if target.get("is_default") or cat_id == "warehouse":
            return False
        catalogs = [c for c in catalogs if c["id"] != cat_id]
        save_catalogs(catalogs)
        return True

    # Check if target is an external mount
    try:
        from web.mounts import load_mounts, delete_mount
        for m in load_mounts():
            if m.get("catalog_name") == cat_id or m.get("id") == cat_id:
                return delete_mount(m["id"])
    except Exception as e:
        logger.warning(f"Error checking mount for deletion: {e}")

    return False

def create_catalog_schema(cat_id: str, schema_name: str) -> str:
    cat = get_catalog(cat_id)
    if not cat:
        raise ValueError(f"Catalog '{cat_id}' not found.")
    s_clean = schema_name.strip().lower().replace(" ", "_")
    target_path = os.path.join(cat["path"], s_clean)
    os.makedirs(target_path, exist_ok=True)
    return target_path

def sync_catalogs_with_duckrun(conn):
    """Ensures all registered secondary catalogs and external storage mounts are attached to the duckrun session."""
    catalogs = load_catalogs()
    attached = getattr(conn, "_catalogs", {})
    valid_cids = {c["id"] for c in catalogs}

    # Detach any secondary catalogs that were deleted
    for cid in list(attached.keys()):
        if cid != "warehouse" and cid not in valid_cids:
            try:
                attached.pop(cid, None)
                if hasattr(conn, "execute"):
                    conn.execute(f'DETACH "{cid}"')
                logger.info(f"Detached removed catalog '{cid}' from duckrun session.")
            except Exception as e:
                logger.debug(f"Could not detach catalog '{cid}': {e}")

    for cat in catalogs:
        cid = cat["id"]
        cpath = cat["path"]
        if cid == "warehouse":
            continue
        if cid not in attached:
            try:
                os.makedirs(os.path.join(cpath, "dbo"), exist_ok=True)
                conn.attach(cpath, name=cid, read_only=cat.get("read_only", False))
                logger.info(f"Attached catalog '{cid}' ({cpath}) to duckrun session.")
            except Exception as e:
                logger.warning(f"Could not attach catalog '{cid}': {e}")

    # Synchronize external storage mounts (Postgres, S3, SQLite)
    try:
        from web.mounts import sync_all_mounts
        sync_all_mounts(conn)
    except Exception as e:
        logger.warning(f"Could not sync storage mounts with DuckDB: {e}")


def scan_all_catalogs_and_tables(conn=None) -> Dict[str, Any]:
    """Scans all registered catalogs and external storage mounts, returning hierarchical Unity Catalog metadata."""
    catalogs = load_catalogs()
    result = []

    # Discover registered models to include in Unity Catalog 3-level governance
    try:
        from web.serving import list_registered_models
        all_models = list_registered_models()
    except Exception as e:
        logger.warning(f"Error loading registered models for catalog hierarchy: {e}")
        all_models = []

    models_by_cat_schema: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for m in all_models:
        c_name = m.get("catalog_name") or "warehouse"
        s_name = m.get("schema_name") or "dbo"
        models_by_cat_schema.setdefault((c_name, s_name), []).append(m)

    for cat in catalogs:
        cat_id = cat["id"]
        cat_path = cat["path"]
        cat_schemas: Dict[str, List[Dict[str, Any]]] = {}

        if os.path.exists(cat_path):
            for root, dirs, files in os.walk(cat_path):
                # For default primary warehouse, skip catalogs/ subfolder and .metadata/
                if cat_id == "warehouse":
                    rel_from_main = os.path.relpath(root, cat_path)
                    parts_rel = rel_from_main.split(os.sep)
                    if parts_rel[0] in ["catalogs", ".metadata"]:
                        continue

                if "_delta_log" in dirs:
                    rel = os.path.relpath(root, cat_path)
                    parts = rel.split(os.sep)
                    if len(parts) >= 2:
                        schema_name = parts[0]
                        table_name = parts[1]
                    else:
                        schema_name = "dbo"
                        table_name = parts[0]

                    total_size = sum(os.path.getsize(os.path.join(root, f)) for f in files if os.path.exists(os.path.join(root, f)))
                    version = 0
                    num_files = 0
                    try:
                        dt = DeltaTable(root)
                        fields = [f.name for f in dt.schema().fields]
                        if fields == ["__duckrun_deleted__"] or "__duckrun_deleted__" in fields:
                            continue
                        version = dt.version()
                        num_files = len(dt.file_uris())
                    except Exception as e:
                        logger.warning(f"Error loading DeltaTable at {root}: {e}")
                        continue

                    if schema_name not in cat_schemas:
                        cat_schemas[schema_name] = []

                    cat_schemas[schema_name].append({
                        "catalog": cat_id,
                        "schema": schema_name,
                        "name": table_name,
                        "full_name": f"{cat_id}.{schema_name}.{table_name}" if cat_id != "warehouse" else f"{schema_name}.{table_name}",
                        "canonical_name": f"{cat_id}.{schema_name}.{table_name}",
                        "path": root,
                        "version": version,
                        "num_files": num_files,
                        "size_bytes": total_size
                    })

        # Ensure dbo schema always appears
        if "dbo" not in cat_schemas:
            cat_schemas["dbo"] = []

        # Include schemas that host registered models
        all_schema_names = set(cat_schemas.keys())
        for (c_name, s_name) in models_by_cat_schema.keys():
            if c_name == cat_id:
                all_schema_names.add(s_name)

        schemas_list = []
        for s_name in sorted(all_schema_names):
            s_tables = cat_schemas.get(s_name, [])
            s_models = models_by_cat_schema.get((cat_id, s_name), [])
            schemas_list.append({
                "name": s_name,
                "tables": s_tables,
                "models": s_models
            })

        result.append({
            "id": cat_id,
            "name": cat["name"],
            "description": cat.get("description", ""),
            "path": cat_path,
            "is_default": cat.get("is_default", False),
            "read_only": cat.get("read_only", False),
            "owner": cat.get("owner", "admin"),
            "schemas": schemas_list,
            "table_count": sum(len(s["tables"]) for s in schemas_list),
            "model_count": sum(len(s.get("models", [])) for s in schemas_list)
        })

    # Append federated external storage mounts
    try:
        from web.mounts import get_mount_catalogs_metadata, sync_all_mounts
        target_conn = conn
        ephemeral = False
        if target_conn is None:
            try:
                import duckdb
                target_conn = duckdb.connect()
                sync_all_mounts(target_conn)
                ephemeral = True
            except Exception:
                target_conn = None

        if target_conn is not None:
            mounted_cats = get_mount_catalogs_metadata(target_conn)
            result.extend(mounted_cats)
            if ephemeral:
                try:
                    target_conn.close()
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"Error appending mounted catalogs: {e}")

    return {
        "catalogs": result,
        "active_catalog": "warehouse"
    }
