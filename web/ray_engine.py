"""
Ray Distributed Compute Engine & Dynamic Actor Pool Orchestrator for Databricks Local Studio.
Option B: Ray-native dynamic actor/task scaling with zero Docker socket privileged requirements.
Provides:
- Distributed DuckDB execution across dynamic Ray Actor Pools
- Map-Reduce / Scatter-Gather partitioned execution over Delta Lake tables
- Sub-second horizontal compute scale on the fly (up/down/zero)
- Live cluster telemetry, node metrics, and Plasma object store tracking
- Full portability across Local Docker, Single-Host, and Production KubeRay (Kubernetes)
"""

import os
import sys
import time
import logging
from typing import Dict, Any, List, Optional
import pyarrow as pa
import duckdb
from deltalake import DeltaTable

logger = logging.getLogger("localspark.ray_engine")

# Check Ray availability
try:
    import ray
    RAY_INSTALLED = True
except ImportError:
    ray = None
    RAY_INSTALLED = False

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
RAY_ADDRESS = os.getenv("RAY_ADDRESS", None)  # e.g., "ray://ray-head:10001" or None for local embedded

# ==============================================================================
# RAY WORKER ACTOR DEFINITION
# ==============================================================================

if RAY_INSTALLED:
    @ray.remote
    class DuckDBWorkerActor:
        """Stateful, persistent DuckDB worker actor representing an isolated compute unit."""
        def __init__(self, actor_id: str, warehouse_id: str, max_memory: str = "2GB", threads: int = 2, warehouse_dir: str = WAREHOUSE_DIR):
            self.actor_id = actor_id
            self.warehouse_id = warehouse_id
            self.max_memory = max_memory
            self.threads = threads
            self.warehouse_dir = warehouse_dir
            try:
                import duckrun
                self.con = duckrun.connect(warehouse_dir, read_only=True)
            except Exception:
                self.con = duckdb.connect(":memory:")
            try:
                self.con.sql(f"SET threads = {threads}")
                self.con.sql(f"SET max_memory = '{max_memory}'")
            except Exception:
                pass
            try:
                from web.governance.macros import install_governance_macros
                install_governance_macros(getattr(self.con, "con", self.con))
            except Exception as e:
                logger.error(f"Failed installing governance masks on Ray actor {actor_id} (masked queries will fail closed): {e}")
            self.queries_executed = 0
            self.created_at = time.time()
            self.last_active_at = self.created_at

        def ping(self) -> Dict[str, Any]:
            return {
                "actor_id": self.actor_id,
                "warehouse_id": self.warehouse_id,
                "status": "HEALTHY",
                "queries_executed": self.queries_executed,
                "uptime_seconds": round(time.time() - self.created_at, 1)
            }

        def execute_query(self, sql: str) -> Dict[str, Any]:
            start_t = time.perf_counter()
            self.last_active_at = time.time()
            try:
                rel = self.con.sql(sql)
                duration_ms = round((time.perf_counter() - start_t) * 1000, 2)
                self.queries_executed += 1

                if rel is not None and hasattr(rel, "description") and rel.description:
                    columns = [d[0] for d in rel.description]
                    rows = rel.fetchall()
                    safe_rows = []
                    for row in rows[:1000]:  # Cap preview at 1000 rows
                        safe_row = []
                        for val in row:
                            if hasattr(val, "isoformat"):
                                safe_row.append(val.isoformat())
                            elif isinstance(val, (bytes, bytearray)):
                                safe_row.append(str(val))
                            else:
                                safe_row.append(val)
                        safe_rows.append(safe_row)

                    return {
                        "success": True,
                        "actor_id": self.actor_id,
                        "columns": columns,
                        "rows": safe_rows,
                        "row_count": len(rows),
                        "duration_ms": duration_ms
                    }
                else:
                    return {
                        "success": True,
                        "actor_id": self.actor_id,
                        "columns": [],
                        "rows": [],
                        "row_count": 0,
                        "duration_ms": duration_ms
                    }
            except Exception as e:
                return {
                    "success": False,
                    "actor_id": self.actor_id,
                    "error": str(e),
                    "duration_ms": round((time.perf_counter() - start_t) * 1000, 2)
                }

        def execute_partition_scan(self, file_paths: List[str], select_clause: str, where_clause: str = "") -> pa.Table:
            """Scans a partitioned slice of Parquet files and returns an Apache Arrow Table."""
            self.last_active_at = time.time()
            self.queries_executed += 1
            query = f"SELECT {select_clause} FROM read_parquet(?)"
            if where_clause:
                query += f" WHERE {where_clause}"
            raw_con = getattr(self.con, "con", self.con)
            rel = raw_con.execute(query, [file_paths])
            if hasattr(rel, "to_arrow_table"):
                return rel.to_arrow_table()
            return rel.fetch_arrow_table()
else:
    DuckDBWorkerActor = None


# ==============================================================================
# CLUSTER MANAGER & DYNAMIC ACTOR POOLS
# ==============================================================================

class RayClusterManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RayClusterManager, cls).__new__(cls)
            cls._instance._initialized = False
            cls._instance.actor_pools = {}  # warehouse_id -> List[ActorHandle]
            cls._instance.next_worker_idx = 0
        return cls._instance

    def initialize_ray(self, num_cpus: Optional[int] = None) -> bool:
        if not RAY_INSTALLED:
            logger.warning("Ray is not installed in the current Python environment.")
            return False

        if ray.is_initialized():
            return True

        try:
            if RAY_ADDRESS:
                logger.info(f"Connecting to remote Ray cluster at {RAY_ADDRESS}...")
                ray.init(address=RAY_ADDRESS, ignore_reinit_error=True)
            else:
                logger.info("Initializing local embedded Ray cluster...")
                # Auto-determine sensible cores for local embedded mode
                cpus = num_cpus or max(2, min(os.cpu_count() or 4, 8))
                ray.init(
                    ignore_reinit_error=True,
                    num_cpus=cpus,
                    include_dashboard=True,
                    dashboard_host="0.0.0.0"
                )
            self._initialized = True
            logger.info("Ray cluster initialized successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Ray cluster: {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        if not RAY_INSTALLED:
            return {
                "available": False,
                "initialized": False,
                "error": "Ray library not installed"
            }

        is_init = ray.is_initialized()
        if not is_init:
            return {
                "available": True,
                "initialized": False,
                "nodes_count": 0,
                "total_cpus": 0,
                "total_memory_mb": 0,
                "actor_pools": {}
            }

        try:
            resources = ray.cluster_resources()
            nodes = ray.nodes()
            active_nodes = [n for n in nodes if n.get("Alive", False)]
            
            # Format pools telemetry
            pools_info = {}
            for wh_id, actors in self.actor_pools.items():
                alive_actors = len(actors)
                pools_info[wh_id] = {
                    "warehouse_id": wh_id,
                    "active_workers": alive_actors,
                    "status": "RUNNING" if alive_actors > 0 else "STOPPED"
                }

            return {
                "available": True,
                "initialized": True,
                "ray_version": ray.__version__,
                "nodes_count": len(active_nodes),
                "total_cpus": resources.get("CPU", 0),
                "total_memory_mb": round(resources.get("memory", 0) / (1024 * 1024), 1),
                "object_store_memory_mb": round(resources.get("object_store_memory", 0) / (1024 * 1024), 1),
                "actor_pools": pools_info,
                "address": RAY_ADDRESS or "local://embedded"
            }
        except Exception as e:
            return {
                "available": True,
                "initialized": is_init,
                "error": str(e)
            }

    def scale_warehouse(self, warehouse_id: str, target_workers: int, max_memory: str = "2GB", threads: int = 2) -> Dict[str, Any]:
        """Dynamically scales the Ray Actor Pool for a warehouse on the fly."""
        if not self.initialize_ray():
            return {"success": False, "error": "Ray cluster is not available"}

        current_actors = self.actor_pools.get(warehouse_id, [])
        current_count = len(current_actors)
        target_workers = max(0, min(target_workers, 16))  # Clamp between 0 and 16

        logger.info(f"Scaling warehouse '{warehouse_id}' from {current_count} to {target_workers} workers on the fly...")

        if target_workers > current_count:
            # Scale UP: spawn new DuckDBWorkerActors
            needed = target_workers - current_count
            for _ in range(needed):
                self.next_worker_idx += 1
                actor_id = f"ray-worker-{warehouse_id}-{self.next_worker_idx}"
                actor = DuckDBWorkerActor.options(name=actor_id).remote(
                    actor_id=actor_id,
                    warehouse_id=warehouse_id,
                    max_memory=max_memory,
                    threads=threads,
                    warehouse_dir=WAREHOUSE_DIR
                )
                current_actors.append(actor)
        elif target_workers < current_count:
            # Scale DOWN: terminate excess actors
            excess = current_count - target_workers
            for _ in range(excess):
                actor_to_remove = current_actors.pop()
                try:
                    ray.kill(actor_to_remove)
                except Exception as e:
                    logger.warning(f"Error terminating actor: {e}")

        self.actor_pools[warehouse_id] = current_actors
        return {
            "success": True,
            "warehouse_id": warehouse_id,
            "previous_workers": current_count,
            "current_workers": len(current_actors),
            "target_workers": target_workers,
            "status": "SCALED"
        }

    def execute_query(self, warehouse_id: str, sql: str) -> Dict[str, Any]:
        """Dispatches a query to an available DuckDB actor in the warehouse pool."""
        if not self.initialize_ray():
            return {"success": False, "error": "Ray cluster not initialized"}

        actors = self.actor_pools.get(warehouse_id, [])
        if not actors:
            # Auto-scale 1 worker on-demand if warehouse pool is at 0
            scale_res = self.scale_warehouse(warehouse_id, 1)
            actors = self.actor_pools.get(warehouse_id, [])
            if not actors:
                return {"success": False, "error": f"No active compute workers for warehouse '{warehouse_id}'"}

        # Round-robin selection
        actor = actors[self.next_worker_idx % len(actors)]
        self.next_worker_idx += 1

        future = actor.execute_query.remote(sql)
        result = ray.get(future)
        return result

    def execute_distributed_delta_scan(
        self,
        warehouse_id: str,
        table_path: str,
        select_clause: str = "*",
        where_clause: str = ""
    ) -> Dict[str, Any]:
        """
        Executes a distributed Map-Reduce query across Delta Lake Parquet files
        using Ray tasks and zero-copy Apache Arrow tables.
        """
        if not self.initialize_ray():
            return {"success": False, "error": "Ray cluster not initialized"}

        start_t = time.perf_counter()
        actors = self.actor_pools.get(warehouse_id, [])
        if not actors:
            self.scale_warehouse(warehouse_id, 2)
            actors = self.actor_pools.get(warehouse_id, [])

        try:
            dt = DeltaTable(table_path)
            if hasattr(dt, "file_uris"):
                files = dt.file_uris()
            elif hasattr(dt, "files"):
                files = [os.path.join(table_path, f) for f in dt.files()]
            else:
                files = [os.path.join(table_path, f) for f in os.listdir(table_path) if f.endswith(".parquet")]
        except Exception as e:
            return {"success": False, "error": f"Failed to inspect Delta table at {table_path}: {e}"}

        if not files:
            return {"success": True, "columns": [], "rows": [], "row_count": 0, "duration_ms": 0.0}

        # Divide files into balanced slices per active worker
        num_workers = max(1, len(actors))
        slice_size = max(1, (len(files) + num_workers - 1) // num_workers)
        file_slices = [files[i:i + slice_size] for i in range(0, len(files), slice_size)]

        # Map Phase: Parallel execution on Ray workers
        futures = []
        for idx, file_slice in enumerate(file_slices):
            actor = actors[idx % len(actors)]
            f = actor.execute_partition_scan.remote(file_slice, select_clause, where_clause)
            futures.append(f)

        # Gather Arrow tables from Plasma Object Store
        arrow_tables: List[pa.Table] = ray.get(futures)

        # Filter out empty tables
        valid_tables = [t for t in arrow_tables if t.num_rows > 0]
        if not valid_tables:
            return {
                "success": True,
                "columns": arrow_tables[0].column_names if arrow_tables else [],
                "rows": [],
                "row_count": 0,
                "duration_ms": round((time.perf_counter() - start_t) * 1000, 2),
                "partitions_scanned": len(file_slices)
            }

        # Zero-copy concatenate Arrow Tables
        merged_table = pa.concat_tables(valid_tables)
        duration_ms = round((time.perf_counter() - start_t) * 1000, 2)

        # Extract preview rows
        columns = merged_table.column_names
        pydict = merged_table.slice(0, 1000).to_pydict()
        rows = []
        num_rows = merged_table.num_rows
        for row_idx in range(min(num_rows, 1000)):
            rows.append([pydict[col][row_idx] for col in columns])

        return {
            "success": True,
            "columns": columns,
            "rows": rows,
            "row_count": num_rows,
            "duration_ms": duration_ms,
            "partitions_scanned": len(file_slices),
            "workers_utilized": min(len(actors), len(file_slices))
        }


# Global singleton manager
ray_manager = RayClusterManager()
