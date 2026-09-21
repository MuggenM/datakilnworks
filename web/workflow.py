import os
import time
import uuid
import json
import logging
import sqlite3
import datetime
import asyncio
from typing import Dict, Any, List, Optional
try:
    from croniter import croniter
except ImportError:
    croniter = None

logger = logging.getLogger("localspark.workflow")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
NOTEBOOKS_DIR = os.getenv("NOTEBOOKS_DIR", "/workspace/notebooks")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
JOBS_FILE = os.path.join(METADATA_DIR, "jobs.json")
DB_PATH = os.path.join(METADATA_DIR, "history.db")

os.makedirs(METADATA_DIR, exist_ok=True)

DEFAULT_JOBS = [
    {
        "id": "job_medallion_pipeline",
        "name": "Medallion Lakehouse Pipeline",
        "description": "Auto-ingests raw sensor telemetry, cleanses to silver layer, aggregates gold metrics, and compacts Delta files.",
        "schedule_cron": "0 * * * *",
        "enabled": True,
        "created_by": "system",
        "created_at": "2026-09-12 03:00:00",
        "tasks": [
            {
                "id": "task_1_bronze",
                "name": "Ingest Bronze Telemetry",
                "type": "sql",
                "depends_on": [],
                "parameters": {
                    "query": "CREATE OR REPLACE TABLE bronze_telemetry AS SELECT 'dev_' || range as device_id, 'sensor_' || (range % 4) as sensor_type, round(20 + random() * 15, 2) as temp_c, current_timestamp as recorded_at FROM range(250)"
                }
            },
            {
                "id": "task_2_silver",
                "name": "Cleanse & Enrich Silver Events",
                "type": "sql",
                "depends_on": ["task_1_bronze"],
                "parameters": {
                    "query": "CREATE OR REPLACE TABLE silver_telemetry AS SELECT device_id, sensor_type, temp_c, round(temp_c * 9/5 + 32, 2) as temp_f, recorded_at, CASE WHEN temp_c > 30 THEN 'CRITICAL_HIGH' WHEN temp_c > 26 THEN 'WARNING' ELSE 'NORMAL' END as alert_status FROM bronze_telemetry"
                }
            },
            {
                "id": "task_3_gold",
                "name": "Aggregate Gold KPI Summary",
                "type": "sql",
                "depends_on": ["task_2_silver"],
                "parameters": {
                    "query": "CREATE OR REPLACE TABLE gold_telemetry_kpis AS SELECT sensor_type, count(*) as event_count, round(avg(temp_c), 2) as avg_temp_c, round(max(temp_c), 2) as max_temp_c, sum(case when alert_status != 'NORMAL' then 1 else 0 end) as alert_count FROM silver_telemetry GROUP BY sensor_type"
                }
            },
            {
                "id": "task_4_optimize",
                "name": "Compact & Optimize Gold Table",
                "type": "optimize",
                "depends_on": ["task_3_gold"],
                "parameters": {
                    "target_table": "dbo.gold_telemetry_kpis",
                    "vacuum_retention_hours": 168
                }
            }
        ]
    },
    {
        "id": "job_table_maintenance",
        "name": "Automated Delta Compaction & Vacuum",
        "description": "Weekly file compaction and tombstone vacuuming across production Delta tables.",
        "schedule_cron": "0 2 * * 0",
        "enabled": False,
        "created_at": "2026-09-12 03:00:00",
        "tasks": [
            {
                "id": "task_opt_employees",
                "name": "Optimize Silver Employees",
                "type": "optimize",
                "depends_on": [],
                "parameters": {
                    "target_table": "dbo.silver_employees",
                    "vacuum_retention_hours": 168
                }
            },
            {
                "id": "task_opt_tickers",
                "name": "Optimize NYSE Tickers",
                "type": "optimize",
                "depends_on": [],
                "parameters": {
                    "target_table": "dbo.nyse_tickers",
                    "vacuum_retention_hours": 168
                }
            }
        ]
    }
]

def init_runs_db():
    try:
        with sqlite3.connect(DB_PATH, timeout=10.0) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_runs (
                    run_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    trigger TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration_sec REAL,
                    tasks_summary TEXT,
                    tasks_detail TEXT
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_job_id ON job_runs(job_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_started_at ON job_runs(started_at DESC);")
    except Exception as e:
        logger.error(f"Error initializing job_runs table: {e}")

init_runs_db()

def load_jobs() -> List[Dict[str, Any]]:
    if not os.path.exists(JOBS_FILE):
        save_jobs(DEFAULT_JOBS)
        return DEFAULT_JOBS
    try:
        with open(JOBS_FILE, "r") as f:
            data = json.load(f)
            return data.get("jobs", DEFAULT_JOBS)
    except Exception as e:
        logger.warning(f"Error loading jobs: {e}")
        return DEFAULT_JOBS

def save_jobs(jobs: List[Dict[str, Any]]):
    with open(JOBS_FILE, "w") as f:
        json.dump({"jobs": jobs}, f, indent=2)

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    jobs = load_jobs()
    return next((j for j in jobs if j["id"] == job_id), None)

def create_or_update_job(job: Dict[str, Any]) -> Dict[str, Any]:
    jobs = load_jobs()
    if not job.get("id"):
        job["id"] = f"job_{uuid.uuid4().hex[:8]}"
    if not job.get("created_at"):
        job["created_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    existing_idx = next((i for i, j in enumerate(jobs) if j["id"] == job["id"]), -1)
    if existing_idx >= 0:
        jobs[existing_idx] = job
    else:
        jobs.append(job)
    save_jobs(jobs)
    return job

def delete_job(job_id: str) -> bool:
    jobs = load_jobs()
    orig_len = len(jobs)
    jobs = [j for j in jobs if j["id"] != job_id]
    if len(jobs) != orig_len:
        save_jobs(jobs)
        return True
    return False

def resolve_table_path(table_ref: str) -> str:
    clean = table_ref.replace(".", "/")
    direct = os.path.join(WAREHOUSE_DIR, clean)
    if os.path.exists(direct):
        return direct
    if "." in table_ref:
        schema, table = table_ref.split(".", 1)
        st_path = os.path.join(WAREHOUSE_DIR, schema, table)
        if os.path.exists(st_path):
            return st_path
    table_only = os.path.join(WAREHOUSE_DIR, table_ref.split(".")[-1])
    if os.path.exists(table_only):
        return table_only
    dbo_path = os.path.join(WAREHOUSE_DIR, "dbo", table_ref.split(".")[-1])
    if os.path.exists(dbo_path):
        return dbo_path
    return direct

def job_principal(job: Dict[str, Any]):
    """
    Jobs run as their owner (`created_by`, set server-side when saved), so a masked owner cannot use a job to copy raw
    columns into an untagged table. The seeded default jobs are system jobs; a job with no recorded owner is anonymous.
    """
    from web.governance import gateway
    owner = job.get("created_by")
    if not owner and job.get("id") in {j["id"] for j in DEFAULT_JOBS}:
        owner = "system"
    return gateway.principal_for_username(owner)


def execute_task(task: Dict[str, Any], conn, principal=None) -> Dict[str, Any]:
    task_type = task.get("type", "sql").lower()
    task_name = task.get("name", task.get("id"))
    params = task.get("parameters", {})
    t0 = time.perf_counter()
    started_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        if task_type == "sql":
            query = params.get("query", "").strip()
            if not query:
                raise ValueError("SQL task missing query parameter")
            from web.governance import gateway
            gateway.ensure_masks(conn)
            governed = gateway.govern_sql(query, principal, client="job", con=getattr(conn, "con", conn).cursor())
            if governed.blocked:
                raise ValueError(f"Blocked by governance: {governed.blocked}")
            res = conn.sql(governed.sql)
            if governed.exempt_reads:
                try:
                    gateway.propagate_tags(query, principal, governed, con=getattr(conn, "con", conn).cursor())
                except Exception as e_prop:
                    logger.warning(f"Tag propagation failed for job task: {e_prop}")
            duration_sec = round(time.perf_counter() - t0, 3)
            row_count = 0
            if res is not None and hasattr(res, "df"):
                df = res.df()
                row_count = len(df)
            else:
                try:
                    conn.refresh()
                except Exception:
                    pass
            log_output = f"Successfully executed SQL statement in {duration_sec * 1000:.1f}ms.\nAffected/Returned rows: {row_count}\nQuery: {query[:120]}..."
            return {
                "task_id": task["id"],
                "task_name": task_name,
                "name": task_name,
                "task_type": task_type,
                "type": task_type,
                "status": "SUCCESS",
                "started_at": started_at,
                "finished_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration_sec": duration_sec,
                "output_log": log_output,
                "output": log_output
            }

        elif task_type == "optimize":
            target_table = params.get("target_table", "")
            if not target_table:
                raise ValueError("Optimize task missing target_table parameter")
            table_path = resolve_table_path(target_table)
            if not os.path.exists(table_path):
                raise FileNotFoundError(f"Delta table at {table_path} does not exist.")

            dt = DeltaTable(table_path)
            opt_res = dt.optimize.compact()
            vacuum_hours = int(params.get("vacuum_retention_hours", 168))
            vac_res = dt.vacuum(retention_hours=vacuum_hours, enforce_retention_duration=False, dry_run=False)
            duration_sec = round(time.perf_counter() - t0, 3)
            log_output = (
                f"Delta table {target_table} optimized successfully in {duration_sec * 1000:.1f}ms.\n"
                f"- Compaction: Added {opt_res.get('numFilesAdded', 0)} files, removed {opt_res.get('numFilesRemoved', 0)} small files.\n"
                f"- Vacuum: Cleaned {len(vac_res)} unreferenced parquet tombstones (retention: {vacuum_hours}h)."
            )
            return {
                "task_id": task["id"],
                "task_name": task_name,
                "name": task_name,
                "task_type": task_type,
                "type": task_type,
                "status": "SUCCESS",
                "started_at": started_at,
                "finished_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration_sec": duration_sec,
                "output_log": log_output,
                "output": log_output
            }

        elif task_type == "notebook":
            from web.governance import gateway
            gateway.deny_if_subject(principal, "Notebook tasks")
            nb_rel = params.get("notebook_path", "").strip()
            if not nb_rel:
                raise ValueError("Notebook task missing notebook_path parameter")

            # Resolve notebook file path flexibly
            if os.path.isabs(nb_rel) and os.path.exists(nb_rel):
                input_path = nb_rel
            elif os.path.exists(os.path.join(NOTEBOOKS_DIR, nb_rel)):
                input_path = os.path.join(NOTEBOOKS_DIR, nb_rel)
            elif nb_rel.startswith("/workspace/notebooks/") and os.path.exists(nb_rel):
                input_path = nb_rel
            elif nb_rel.startswith("notebooks/") and os.path.exists(os.path.join(os.path.dirname(NOTEBOOKS_DIR), nb_rel)):
                input_path = os.path.join(os.path.dirname(NOTEBOOKS_DIR), nb_rel)
            else:
                input_path = os.path.join(NOTEBOOKS_DIR, nb_rel)

            if not os.path.exists(input_path):
                raise FileNotFoundError(f"Notebook file '{nb_rel}' not found in {NOTEBOOKS_DIR}")

            out_dir = "/tmp/papermill_runs"
            os.makedirs(out_dir, exist_ok=True)
            output_path = os.path.join(out_dir, f"out_{uuid.uuid4().hex[:6]}_{os.path.basename(nb_rel)}")

            import papermill as pm
            pm_params = params.get("parameters", {})
            if isinstance(pm_params, str):
                try:
                    pm_params = json.loads(pm_params)
                except Exception:
                    pm_params = {}
            if not isinstance(pm_params, dict):
                pm_params = {}

            pm.execute_notebook(input_path, output_path, kernel_name="python3", language="python", parameters=pm_params)
            duration_sec = round(time.perf_counter() - t0, 3)

            params_formatted = json.dumps(pm_params, indent=2) if pm_params else "{}"
            log_output = (
                f"Notebook '{nb_rel}' executed headlessly via Papermill in {duration_sec:.2f}s.\n"
                f"Injected Parameters:\n{params_formatted}\n"
                f"Output notebook saved to: {output_path}"
            )
            return {
                "task_id": task["id"],
                "task_name": task_name,
                "name": task_name,
                "task_type": task_type,
                "type": task_type,
                "status": "SUCCESS",
                "started_at": started_at,
                "finished_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration_sec": duration_sec,
                "output_log": log_output,
                "output": log_output
            }

        elif task_type == "ingest":
            source_file = params.get("source_file", "")
            target_table = params.get("target_table", "dbo.ingest_output")
            mode = params.get("mode", "overwrite").lower()
            if not os.path.exists(source_file):
                raise FileNotFoundError(f"Source file {source_file} not found")

            _, ext = os.path.splitext(source_file)
            if ext == ".csv":
                scan_sql = f"read_csv_auto('{source_file}', header=True)"
            elif ext == ".parquet":
                scan_sql = f"read_parquet('{source_file}')"
            elif ext == ".json":
                scan_sql = f"read_json_auto('{source_file}')"
            else:
                scan_sql = f"read_csv_auto('{source_file}')"

            if mode == "append":
                sql = f"INSERT INTO {target_table} SELECT * FROM {scan_sql}"
            else:
                sql = f"CREATE OR REPLACE TABLE {target_table} AS SELECT * FROM {scan_sql}"

            conn.sql(sql)
            conn.refresh()
            duration_sec = round(time.perf_counter() - t0, 3)
            return {
                "task_id": task["id"],
                "task_name": task_name,
                "name": task_name,
                "task_type": task_type,
                "type": task_type,
                "status": "SUCCESS",
                "started_at": started_at,
                "finished_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration_sec": duration_sec,
                "output_log": f"Ingested {source_file} into {target_table} (mode: {mode}) in {duration_sec * 1000:.1f}ms.",
                "output": f"Ingested {source_file} into {target_table} (mode: {mode}) in {duration_sec * 1000:.1f}ms."
            }

        elif task_type == "dbt":
            from web.governance import gateway
            gateway.deny_if_subject(principal, "dbt tasks")
            from web.dbt_service import run_dbt_cli
            action = params.get("action", "run")
            select = params.get("select", None)
            full_refresh = bool(params.get("full_refresh", False))
            target = params.get("target", "dev")

            run_res = run_dbt_cli(action=action, select=select, full_refresh=full_refresh, target=target)
            duration_sec = run_res.get("duration_seconds", round(time.perf_counter() - t0, 3))
            status = "SUCCESS" if run_res.get("status") == "SUCCESS" else "FAILED"
            log_output = run_res.get("output", "")
            return {
                "task_id": task["id"],
                "task_name": task_name,
                "name": task_name,
                "task_type": task_type,
                "type": task_type,
                "status": status,
                "started_at": started_at,
                "finished_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration_sec": duration_sec,
                "output_log": log_output,
                "output": log_output
            }

        else:
            raise ValueError(f"Unknown task type: {task_type}")

    except Exception as e:
        duration_sec = round(time.perf_counter() - t0, 3)
        return {
            "task_id": task["id"],
            "task_name": task_name,
            "name": task_name,
            "task_type": task_type,
            "type": task_type,
            "status": "FAILED",
            "started_at": started_at,
            "finished_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration_sec": duration_sec,
            "output_log": f"Task execution failed: {str(e)}",
            "output": f"Task execution failed: {str(e)}"
        }

def topological_sort_tasks(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    task_map = {t["id"]: t for t in tasks}
    in_degree = {t["id"]: len(t.get("depends_on", [])) for t in tasks}
    graph = {t["id"]: [] for t in tasks}
    for t in tasks:
        for dep in t.get("depends_on", []):
            if dep in graph:
                graph[dep].append(t["id"])

    queue = [t_id for t_id, deg in in_degree.items() if deg == 0]
    sorted_tasks = []

    while queue:
        curr = queue.pop(0)
        sorted_tasks.append(task_map[curr])
        for child in graph[curr]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(sorted_tasks) != len(tasks):
        # Fallback to original order if cycle detected
        logger.warning("Cycle detected or missing dependencies in task graph; falling back to declaration order.")
        return tasks
    return sorted_tasks

def run_pipeline(job_id: str, trigger: str = "MANUAL", conn=None, principal=None) -> Dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")
    principal = principal or job_principal(job)

    if conn is None:
        import duckrun
        conn = duckrun.connect(WAREHOUSE_DIR, read_only=False)

    run_id = f"run_{uuid.uuid4().hex[:8]}"
    start_time = time.perf_counter()
    started_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Record initial RUNNING state
    with sqlite3.connect(DB_PATH) as sconn:
        sconn.execute("""
            INSERT INTO job_runs (
                run_id, job_id, job_name, trigger, status, started_at,
                finished_at, duration_sec, tasks_summary, tasks_detail
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (run_id, job["id"], job["name"], trigger.upper(), "RUNNING", started_at, None, 0.0, "[]", "[]"))

    ordered_tasks = topological_sort_tasks(job.get("tasks", []))
    task_runs: List[Dict[str, Any]] = []
    failed_tasks = set()
    overall_status = "SUCCESS"

    for task in ordered_tasks:
        deps = task.get("depends_on", [])
        # If any dependency failed, skip this task
        if any(d in failed_tasks for d in deps):
            task_runs.append({
                "task_id": task["id"],
                "task_name": task.get("name", task["id"]),
                "name": task.get("name", task["id"]),
                "task_type": task.get("type", "sql"),
                "type": task.get("type", "sql"),
                "status": "SKIPPED",
                "started_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "finished_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration_sec": 0.0,
                "output_log": f"Skipped because upstream dependency failed: {deps}",
                "output": f"Skipped because upstream dependency failed: {deps}"
            })
            failed_tasks.add(task["id"])
            overall_status = "FAILED"
            continue

        res = execute_task(task, conn, principal=principal)
        task_runs.append(res)
        if res["status"] != "SUCCESS":
            failed_tasks.add(task["id"])
            overall_status = "FAILED"

    finished_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_duration_sec = round(time.perf_counter() - start_time, 2)

    tasks_summary = [
        {"id": tr["task_id"], "name": tr["task_name"], "type": tr["task_type"], "status": tr["status"], "duration_sec": tr["duration_sec"]}
        for tr in task_runs
    ]

    with sqlite3.connect(DB_PATH) as sconn:
        sconn.execute("""
            UPDATE job_runs
            SET status = ?, finished_at = ?, duration_sec = ?, tasks_summary = ?, tasks_detail = ?
            WHERE run_id = ?
        """, (
            overall_status,
            finished_at,
            total_duration_sec,
            json.dumps(tasks_summary),
            json.dumps(task_runs),
            run_id
        ))

    return {
        "run_id": run_id,
        "job_id": job["id"],
        "job_name": job["name"],
        "trigger": trigger.upper(),
        "status": overall_status,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_sec": total_duration_sec,
        "task_runs": task_runs
    }

def get_job_runs(job_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    init_runs_db()
    with sqlite3.connect(DB_PATH) as sconn:
        sconn.row_factory = sqlite3.Row
        if job_id:
            cursor = sconn.execute("""
                SELECT run_id, job_id, job_name, trigger, status, started_at, finished_at, duration_sec, tasks_summary
                FROM job_runs
                WHERE job_id = ?
                ORDER BY started_at DESC, rowid DESC
                LIMIT ?
            """, (job_id, limit))
        else:
            cursor = sconn.execute("""
                SELECT run_id, job_id, job_name, trigger, status, started_at, finished_at, duration_sec, tasks_summary
                FROM job_runs
                ORDER BY started_at DESC, rowid DESC
                LIMIT ?
            """, (limit,))

        rows = cursor.fetchall()
        runs = []
        for r in rows:
            d = dict(r)
            try:
                d["tasks_summary"] = json.loads(d.get("tasks_summary") or "[]")
            except Exception:
                d["tasks_summary"] = []
            runs.append(d)
        return runs

def get_run_detail(run_id: str) -> Optional[Dict[str, Any]]:
    init_runs_db()
    with sqlite3.connect(DB_PATH) as sconn:
        sconn.row_factory = sqlite3.Row
        cursor = sconn.execute("SELECT * FROM job_runs WHERE run_id = ?", (run_id,))
        row = cursor.fetchone()
        if not row:
            return None
        d = dict(row)
        try:
            d["tasks_summary"] = json.loads(d.get("tasks_summary") or "[]")
            d["tasks_detail"] = json.loads(d.get("tasks_detail") or "[]")
        except Exception:
            pass
        return d

_last_cron_check: Dict[str, datetime.datetime] = {}
_running_jobs = set()

async def _run_job_async(job_id: str, trigger: str):
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, run_pipeline, job_id, trigger)
    except Exception as e:
        logger.error(f"Background execution of job {job_id} failed: {e}")
    finally:
        _running_jobs.discard(job_id)

async def cron_scheduler_loop():
    logger.info("Starting localspark workflow cron scheduler loop...")
    while True:
        try:
            now = datetime.datetime.now()
            jobs = load_jobs()
            for job in jobs:
                if not job.get("enabled", False):
                    continue
                cron_expr = job.get("schedule_cron", "").strip()
                if not cron_expr:
                    continue
                job_id = job["id"]
                if job_id in _running_jobs:
                    continue

                last_time = _last_cron_check.get(job_id)
                if last_time is None:
                    _last_cron_check[job_id] = now
                    continue

                try:
                    if not croniter:
                        continue
                    itr = croniter(cron_expr, last_time)
                    next_time = itr.get_next(datetime.datetime)
                    if now >= next_time:
                        _last_cron_check[job_id] = now
                        logger.info(f"Cron triggering scheduled job: {job['name']} ({job_id})")
                        _running_jobs.add(job_id)
                        asyncio.create_task(_run_job_async(job_id, trigger="CRON"))
                except Exception as e:
                    logger.warning(f"Error evaluating cron '{cron_expr}' for job {job_id}: {e}")

        except Exception as e:
            logger.error(f"Error in cron scheduler loop: {e}")

        await asyncio.sleep(15)
