import os
import time
import uuid
import json
import logging
import sqlite3
import datetime
import asyncio
import math
import re
import threading
import concurrent.futures
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
            for col in ("run_params", "parent_run_id", "trigger_detail", "notifications"):      # added with orchestration v2
                if col not in {r[1] for r in conn.execute("PRAGMA table_info(job_runs)")}:
                    conn.execute(f"ALTER TABLE job_runs ADD COLUMN {col} TEXT")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workflow_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
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
    validate_job(job)
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

# =====================================================================================================================
# Orchestration: validation, parameters, retries, timeouts, run conditions, cancel, repair, notifications, triggers.
#
# A job is a DAG of tasks run one after another in dependency order on one connection, as the job's owner. Everything below is optional
# and additive: a job saved before these fields existed behaves exactly as it did.
#
#   task  retries / retry_delay_seconds / retry_backoff   a failed attempt is retried; the delay grows by the backoff factor
#         timeout_seconds                                 an attempt still running after this long is abandoned (a SQL task is interrupted)
#         run_if                                          when the task runs given how its dependencies ended (see RUN_IF)
#   job   parameters      [{name, default, allowed?, pattern?}] substituted for {{ params.name }} in every task parameter; a supplied value
#                         must match the declared allowed list / pattern (default: letters, digits and _.:@- and space only), because the
#                         job runs with its OWNER's rights and whoever starts it must not be able to inject SQL
#         timeout_seconds, max_concurrent_runs, catch_up
#         triggers        [{type: job|autoloader|table, ...}]  besides the cron schedule
#         notifications   [{on: [failure|success|cancelled], channel: email|slack|webhook, target}]
# =====================================================================================================================

RUN_IF = ("all_success", "all_done", "at_least_one_failed", "all_failed", "at_least_one_success", "none_failed")
TASK_TYPES = ("sql", "optimize", "notebook", "ingest", "dbt")
MAX_CHAIN_DEPTH = 5
DEFAULT_PARAM_PATTERN = re.compile(r"^[A-Za-z0-9_.:@\- ]{0,100}$")
_PARAM_REF = re.compile(r"\{\{\s*params\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")
_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_TABLE_RE = re.compile(r"^[a-z0-9_]+(\.[a-z0-9_]+){1,2}$")


class JobValidationError(ValueError):
    """The job definition is not acceptable; the message says what to change."""


def _int(v, lo, hi, default, what):
    if v in (None, ""):
        return default
    try:
        n = int(v)
    except (TypeError, ValueError):
        raise JobValidationError(f"{what} must be a whole number.")
    if not lo <= n <= hi:
        raise JobValidationError(f"{what} must be between {lo} and {hi}.")
    return n


def _walk_strings(obj):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _walk_strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_strings(v)


def _has_cycle(tasks: List[Dict[str, Any]]) -> bool:
    deps = {t["id"]: list(t.get("depends_on") or []) for t in tasks}
    state: Dict[str, int] = {}

    def visit(n: str) -> bool:
        if state.get(n) == 1:
            return True
        if state.get(n) == 2:
            return False
        state[n] = 1
        if any(visit(d) for d in deps.get(n, [])):
            return True
        state[n] = 2
        return False
    return any(visit(n) for n in deps)


def validate_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """Normalises a job definition (clamping and defaulting the orchestration fields) or raises JobValidationError."""
    if not isinstance(job, dict):
        raise JobValidationError("A job is an object.")
    name = str(job.get("name") or "").strip()
    if not name or len(name) > 120:
        raise JobValidationError("The job needs a name (at most 120 characters).")
    job["name"] = name
    tasks = job.get("tasks") or []
    if not isinstance(tasks, list):
        raise JobValidationError("tasks must be a list.")
    seen = set()
    for t in tasks:
        if not isinstance(t, dict) or not _ID_RE.match(str(t.get("id") or "")):
            raise JobValidationError("Every task needs an id (letters, digits, '_' or '-', at most 64 characters).")
        if t["id"] in seen:
            raise JobValidationError(f"Task id '{t['id']}' is used twice.")
        seen.add(t["id"])
    for t in tasks:
        who = f"Task '{t['id']}'"
        if str(t.get("type", "sql")).lower() not in TASK_TYPES:
            raise JobValidationError(f"{who}: the type must be one of {', '.join(TASK_TYPES)}.")
        deps = t.get("depends_on") or []
        if not isinstance(deps, list) or any(d not in seen or d == t["id"] for d in deps):
            raise JobValidationError(f"{who}: depends_on must list other tasks of this job.")
        t["depends_on"] = list(dict.fromkeys(deps))
        t["retries"] = _int(t.get("retries"), 0, 10, 0, f"{who}: retries")
        t["retry_delay_seconds"] = _int(t.get("retry_delay_seconds"), 0, 3600, 30, f"{who}: the retry delay")
        try:
            t["retry_backoff"] = max(1.0, min(float(t.get("retry_backoff") or 2), 10.0))
        except (TypeError, ValueError):
            raise JobValidationError(f"{who}: the retry backoff must be a number.")
        t["timeout_seconds"] = _int(t.get("timeout_seconds"), 0, 86400, 0, f"{who}: the timeout")
        t["run_if"] = str(t.get("run_if") or "all_success").lower()
        if t["run_if"] not in RUN_IF:
            raise JobValidationError(f"{who}: run_if must be one of {', '.join(RUN_IF)}.")
    if _has_cycle(tasks):
        raise JobValidationError("The tasks depend on each other in a circle.")
    cron = str(job.get("schedule_cron") or "").strip()
    if cron and croniter and not croniter.is_valid(cron):
        raise JobValidationError(f"'{cron}' is not a valid cron expression.")
    job["schedule_cron"] = cron
    job["timeout_seconds"] = _int(job.get("timeout_seconds"), 0, 7 * 86400, 0, "The job timeout")
    job["max_concurrent_runs"] = _int(job.get("max_concurrent_runs"), 1, 10, 1, "Max concurrent runs")
    job["catch_up"] = bool(job.get("catch_up"))
    # parameters
    params, names = [], set()
    for p in job.get("parameters") or []:
        pn = str((p or {}).get("name") or "")
        if not _NAME_RE.match(pn) or pn in names:
            raise JobValidationError(f"Parameter names are letters, digits and '_' (starting with a letter) and must be unique: '{pn}'.")
        names.add(pn)
        allowed = [str(a) for a in (p.get("allowed") or []) if str(a) != ""]
        pat = str(p.get("pattern") or "").strip()
        if pat:
            try:
                re.compile(pat)
            except re.error:
                raise JobValidationError(f"Parameter '{pn}': the pattern is not a valid regular expression.")
        entry = {"name": pn, "default": str(p.get("default") if p.get("default") is not None else ""), "description": str(p.get("description") or "")[:200],
                 "allowed": allowed, "pattern": pat}
        _check_param_value(entry, entry["default"])
        params.append(entry)
    job["parameters"] = params
    for t in tasks:
        for text in _walk_strings(t.get("parameters") or {}):
            for ref in _PARAM_REF.findall(text):
                if ref not in names:
                    raise JobValidationError(f"Task '{t['id']}' uses {{{{ params.{ref} }}}} but the job declares no parameter '{ref}'.")
    # triggers
    triggers = []
    for tr in job.get("triggers") or []:
        kind = str((tr or {}).get("type") or "")
        if kind == "job":
            if not tr.get("job_id") or tr.get("job_id") == job.get("id"):
                raise JobValidationError("A 'job' trigger needs the id of another job.")
            on = tr.get("on") or "success"
            if on not in ("success", "failure", "completion"):
                raise JobValidationError("A 'job' trigger runs on success, failure or completion.")
            triggers.append({"type": "job", "job_id": str(tr["job_id"]), "on": on})
        elif kind == "autoloader":
            if not tr.get("pipeline_id"):
                raise JobValidationError("An 'autoloader' trigger needs a pipeline id.")
            triggers.append({"type": "autoloader", "pipeline_id": str(tr["pipeline_id"]), "min_files": _int(tr.get("min_files"), 1, 100000, 1, "Minimum files")})
        elif kind == "table":
            tb = str(tr.get("table") or "").strip().lower()
            if not _TABLE_RE.match(tb):
                raise JobValidationError("A 'table' trigger names a table as schema.table or catalog.schema.table.")
            triggers.append({"type": "table", "table": tb})
        else:
            raise JobValidationError("A trigger is of type job, autoloader or table.")
    job["triggers"] = triggers
    # notifications
    notes = []
    for n in job.get("notifications") or []:
        on = [e for e in (n or {}).get("on") or [] if e in ("failure", "success", "cancelled")]
        channel = str((n or {}).get("channel") or "")
        target = str((n or {}).get("target") or "").strip()
        if not on or channel not in ("email", "slack", "webhook") or len(target) > 500:
            raise JobValidationError("A notification needs at least one event (failure, success, cancelled) and a channel (email, slack, webhook).")
        if channel == "email":
            addrs = [a.strip() for a in re.split(r"[,;\s]+", target) if a.strip()]
            if not addrs or len(addrs) > 20 or any(not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", a) for a in addrs):
                raise JobValidationError("An email notification needs one to twenty valid addresses.")
            target = ", ".join(addrs)
        notes.append({"on": on, "channel": channel, "target": target})
    job["notifications"] = notes
    return job


def _check_param_value(p: Dict[str, Any], value: str) -> None:
    if len(value) > 200:
        raise JobValidationError(f"Parameter '{p['name']}': the value is too long.")
    if p.get("allowed"):
        if value not in p["allowed"]:
            raise JobValidationError(f"Parameter '{p['name']}' must be one of: {', '.join(p['allowed'])}.")
    elif p.get("pattern"):
        if not re.fullmatch(p["pattern"], value):
            raise JobValidationError(f"Parameter '{p['name']}' does not match its pattern.")
    elif not DEFAULT_PARAM_PATTERN.match(value):
        raise JobValidationError(f"Parameter '{p['name']}' may only contain letters, digits and _ . : @ - and spaces "
                                 "(the job owner can declare an allowed list or a pattern to permit more).")


def resolve_run_params(job: Dict[str, Any], supplied: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """The parameter values of one run: the declared defaults overridden by validated supplied values (unknown names are refused)."""
    declared = {p["name"]: p for p in job.get("parameters") or []}
    supplied = supplied or {}
    for k in supplied:
        if k not in declared:
            raise JobValidationError(f"The job has no parameter '{k}'.")
    out = {}
    for name, p in declared.items():
        v = supplied.get(name)
        out[name] = str(p["default"] if v is None else (str(v).lower() if isinstance(v, bool) else v))
        _check_param_value(p, out[name])
    return out


def substitute_params(obj: Any, params: Dict[str, str]) -> Any:
    if isinstance(obj, str):
        return _PARAM_REF.sub(lambda m: params.get(m.group(1), m.group(0)), obj)
    if isinstance(obj, dict):
        return {k: substitute_params(v, params) for k, v in obj.items()}
    if isinstance(obj, list):
        return [substitute_params(v, params) for v in obj]
    return obj


def should_run(run_if: str, dep_statuses: List[str]) -> bool:
    """Whether a task runs, given how its dependencies ended. A root task (no dependencies) always runs."""
    if not dep_statuses:
        return True
    ok = [s == "SUCCESS" for s in dep_statuses]
    failed = [s in ("FAILED",) for s in dep_statuses]
    return {
        "all_success": all(ok),
        "all_done": True,
        "at_least_one_failed": any(failed),
        "all_failed": all(failed),
        "at_least_one_success": any(ok),
        "none_failed": not any(failed),
    }.get(run_if, all(ok))


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
        logger.warning("Cycle detected or missing dependencies in task graph; falling back to declaration order.")
        return tasks
    return sorted_tasks


def _now() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _task_result(task: Dict[str, Any], status: str, message: str, started: Optional[str] = None, duration: float = 0.0, **extra) -> Dict[str, Any]:
    name = task.get("name", task["id"])
    ttype = task.get("type", "sql")
    r = {"task_id": task["id"], "task_name": name, "name": name, "task_type": ttype, "type": ttype, "status": status,
         "started_at": started or _now(), "finished_at": _now(), "duration_sec": duration, "output_log": message, "output": message}
    r.update(extra)
    return r


# ---------------------------------------------------------------- active runs (cancel), state

_active: Dict[str, Dict[str, Any]] = {}
_active_lock = threading.Lock()


def running_count(job_id: str) -> int:
    with _active_lock:
        return sum(1 for a in _active.values() if a["job_id"] == job_id)


def _interrupt(conn) -> None:
    try:
        getattr(conn, "con", conn).interrupt()
    except Exception:
        pass


def cancel_run(run_id: str) -> bool:
    """Asks a running run to stop: no further task or retry starts, and a running SQL statement is interrupted. False if it is not running."""
    with _active_lock:
        a = _active.get(run_id)
    if not a:
        return False
    a["cancel"].set()
    if a.get("conn") is not None:
        _interrupt(a["conn"])
    return True


def _state_get(key: str) -> Optional[str]:
    try:
        with sqlite3.connect(DB_PATH, timeout=10.0) as c:
            r = c.execute("SELECT value FROM workflow_state WHERE key = ?", (key,)).fetchone()
            return r[0] if r else None
    except Exception:
        return None


def _state_set(key: str, value: str) -> None:
    try:
        with sqlite3.connect(DB_PATH, timeout=10.0) as c:
            c.execute("INSERT INTO workflow_state (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))
    except Exception as exc:
        logger.warning(f"could not save workflow state {key}: {exc}")


def mark_orphaned_runs() -> int:
    """Runs left RUNNING by a process that died: they are not running any more. Called once when the scheduler starts."""
    with _active_lock:
        live = set(_active)
    try:
        with sqlite3.connect(DB_PATH, timeout=10.0) as c:
            rows = [r[0] for r in c.execute("SELECT run_id FROM job_runs WHERE status = 'RUNNING'") if r[0] not in live]
            for rid in rows:
                c.execute("UPDATE job_runs SET status = 'FAILED', finished_at = ? WHERE run_id = ?", (_now(), rid))
            return len(rows)
    except Exception:
        return 0


# ---------------------------------------------------------------- one attempt, with retries

def _run_attempt(task: Dict[str, Any], conn, principal, timeout: int) -> Dict[str, Any]:
    if not timeout:
        return execute_task(task, conn, principal=principal)
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(execute_task, task, conn, principal)
    try:
        return fut.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        _interrupt(conn)                                    # a SQL statement stops; anything else is abandoned and its result ignored
        ex.shutdown(wait=False)
        return _task_result(task, "FAILED", f"Timed out after {timeout}s.", duration=float(timeout), timed_out=True)
    finally:
        ex.shutdown(wait=False)


def _execute_with_retries(task: Dict[str, Any], conn, principal, cancel: threading.Event, deadline: Optional[float]) -> Dict[str, Any]:
    retries = int(task.get("retries") or 0)
    delay = int(task.get("retry_delay_seconds") if task.get("retry_delay_seconds") is not None else 30)
    backoff = float(task.get("retry_backoff") or 2)
    attempts: List[Dict[str, Any]] = []
    res: Dict[str, Any] = {}
    for n in range(1, retries + 2):
        timeout = int(task.get("timeout_seconds") or 0)
        if deadline is not None:                            # the job's own timeout also bounds every attempt
            left = math.ceil(deadline - time.time())
            if left <= 0:
                res = _task_result(task, "FAILED", "The job timed out before this task could run.", timed_out=True)
                break
            timeout = min(timeout, left) if timeout else left
        res = _run_attempt(task, conn, principal, timeout)
        attempts.append({"attempt": n, "status": res["status"], "started_at": res.get("started_at"), "duration_sec": res.get("duration_sec"),
                         "error": (res.get("output_log") or "")[:300] if res["status"] != "SUCCESS" else None})
        if res["status"] == "SUCCESS" or cancel.is_set() or n > retries:
            break
        wait = min(3600.0, delay * (backoff ** (n - 1)))
        logger.info(f"Task {task['id']} attempt {n} failed; retrying in {wait:.0f}s")
        end = time.time() + wait
        while time.time() < end and not cancel.is_set():
            time.sleep(min(0.5, max(0.0, end - time.time())))
        if cancel.is_set():
            break
    res["attempts"] = attempts
    res["attempt_count"] = len(attempts)
    return res


# ---------------------------------------------------------------- notifications

def _notify(job: Dict[str, Any], run: Dict[str, Any]) -> List[Dict[str, Any]]:
    event = {"SUCCESS": "success", "CANCELLED": "cancelled"}.get(run["status"], "failure")
    sent: List[Dict[str, Any]] = []
    rules = [n for n in job.get("notifications") or [] if event in n.get("on", [])]
    if not rules:
        return sent
    bad = [t for t in run["task_runs"] if t["status"] == "FAILED"]
    lines = [f"Job '{job['name']}' finished {run['status']} in {run['duration_sec']}s (run {run['run_id']}, trigger {run['trigger']})."]
    for t in bad[:5]:
        lines.append(f"- {t['task_name']}: {(t.get('output_log') or '')[:300].strip().splitlines()[-1] if (t.get('output_log') or '').strip() else 'failed'}")
    text = "\n".join(lines)
    title = f"Job {job['name']}: {run['status']}"
    for rule in rules:
        entry = {"channel": rule["channel"], "target": rule.get("target", ""), "ok": False, "error": None}
        try:
            if rule["channel"] == "email":
                from web import email_reports
                res = email_reports.send_email([a.strip() for a in rule["target"].split(",") if a.strip()], title,
                                               "<pre style='font-family:monospace'>" + text.replace("&", "&amp;").replace("<", "&lt;") + "</pre>", text)
            elif rule["channel"] == "slack":
                from web import slack_integration
                res = slack_integration.send_notification(text, webhook_id=rule["target"] or None, title=title,
                                                          color="#36a64f" if event == "success" else "#d9534f")
            else:
                from web import webhook_alerts
                res = webhook_alerts.send_webhook(rule["target"], title, text, data={"job_id": job["id"], "run_id": run["run_id"], "status": run["status"]},
                                                  severity="info" if event == "success" else "error")
            entry["ok"] = bool((res or {}).get("success"))
            entry["error"] = None if entry["ok"] else str((res or {}).get("error") or "not delivered")[:200]
        except Exception as exc:
            entry["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
        sent.append(entry)
    return sent


# ---------------------------------------------------------------- the run

def run_pipeline(job_id: str, trigger: str = "MANUAL", conn=None, principal=None, params: Optional[Dict[str, Any]] = None,
                 run_id: Optional[str] = None, repair_of: Optional[str] = None, ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")
    principal = principal or job_principal(job)
    ctx = ctx or {"depth": 0, "chain": [job_id]}
    run_id = run_id or f"run_{uuid.uuid4().hex[:8]}"
    started_at = _now()
    base = None
    if repair_of:
        base = get_run_detail(repair_of)
        if not base or base["job_id"] != job_id or base["status"] not in ("FAILED", "CANCELLED", "TIMEOUT"):
            raise ValueError("Only a failed or cancelled run of this job can be repaired.")
        params = json.loads(base.get("run_params") or "{}")
    run_params = resolve_run_params(job, params)

    if running_count(job_id) >= int(job.get("max_concurrent_runs") or 1):
        with sqlite3.connect(DB_PATH) as sconn:
            sconn.execute("INSERT INTO job_runs (run_id, job_id, job_name, trigger, status, started_at, finished_at, duration_sec, tasks_summary, tasks_detail, run_params, trigger_detail) "
                          "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (run_id, job["id"], job["name"], trigger.upper(), "SKIPPED", started_at, started_at, 0.0, "[]", "[]",
                                                             json.dumps(run_params), "skipped: the maximum number of concurrent runs is already running"))
        return {"run_id": run_id, "job_id": job["id"], "job_name": job["name"], "trigger": trigger.upper(), "status": "SKIPPED", "started_at": started_at,
                "finished_at": started_at, "duration_sec": 0.0, "task_runs": [], "message": "Skipped: the maximum number of concurrent runs is already running."}

    if conn is None:
        import duckrun
        conn = duckrun.connect(WAREHOUSE_DIR, read_only=False)
    cancel = threading.Event()
    with _active_lock:
        _active[run_id] = {"job_id": job_id, "cancel": cancel, "conn": conn, "started_at": started_at}
    start_time = time.perf_counter()
    with sqlite3.connect(DB_PATH) as sconn:
        sconn.execute("""
            INSERT INTO job_runs (run_id, job_id, job_name, trigger, status, started_at, finished_at, duration_sec, tasks_summary, tasks_detail,
                                  run_params, parent_run_id, trigger_detail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (run_id, job["id"], job["name"], trigger.upper(), "RUNNING", started_at, None, 0.0, "[]", "[]", json.dumps(run_params), repair_of,
              json.dumps({"depth": ctx["depth"], "chain": ctx["chain"]}) if ctx["depth"] else None))
    deadline = time.time() + int(job["timeout_seconds"]) if job.get("timeout_seconds") else None
    results: Dict[str, Dict[str, Any]] = {}
    task_runs: List[Dict[str, Any]] = []
    job_timed_out = False
    base_ok = {t["task_id"]: t for t in (base or {}).get("tasks_detail", []) if t.get("status") == "SUCCESS"}
    try:
        for raw in topological_sort_tasks(job.get("tasks", [])):
            task = substitute_params(raw, run_params)
            deps = raw.get("depends_on") or []
            if cancel.is_set():
                res = _task_result(task, "CANCELLED", "The run was cancelled before this task started.")
            elif deadline is not None and time.time() >= deadline:
                job_timed_out = True
                res = _task_result(task, "SKIPPED", "The job timed out before this task started.")
            elif raw["id"] in base_ok:
                res = dict(base_ok[raw["id"]])
                res.update(reused_from=repair_of, output_log=f"Reused from run {repair_of} (it had succeeded): {res.get('output_log', '')[:200]}")
                res["output"] = res["output_log"]
            elif not should_run(raw.get("run_if", "all_success"), [results[d]["status"] for d in deps if d in results]):
                dep_desc = ", ".join(f"{d}={results[d]['status']}" for d in deps if d in results)
                res = _task_result(task, "SKIPPED", f"Skipped: run_if '{raw.get('run_if', 'all_success')}' is not met ({dep_desc}).")
            else:
                res = _execute_with_retries(task, conn, principal, cancel, deadline)
                if cancel.is_set() and res["status"] != "SUCCESS":
                    res["status"] = "CANCELLED"
                if res.get("timed_out") and deadline is not None and time.time() >= deadline - 1:
                    job_timed_out = True
            results[raw["id"]] = res
            task_runs.append(res)
    except Exception as exc:                                    # an engine error must not leave the run RUNNING forever
        logger.error(f"Run {run_id} of {job_id} failed in the engine: {exc}", exc_info=True)
        task_runs.append(_task_result({"id": "_engine", "name": "Engine"}, "FAILED", f"Internal error: {exc}"))
    finally:
        with _active_lock:
            _active.pop(run_id, None)

    if cancel.is_set():
        overall = "CANCELLED"
    elif job_timed_out:
        overall = "TIMEOUT"
    elif any(t["status"] == "FAILED" for t in task_runs):
        overall = "FAILED"
    else:
        overall = "SUCCESS"
    finished_at = _now()
    duration = round(time.perf_counter() - start_time, 2)
    summary = [{"id": tr["task_id"], "name": tr["task_name"], "type": tr["task_type"], "status": tr["status"], "duration_sec": tr["duration_sec"],
                "attempts": tr.get("attempt_count", 1)} for tr in task_runs]
    result = {"run_id": run_id, "job_id": job["id"], "job_name": job["name"], "trigger": trigger.upper(), "status": overall, "started_at": started_at,
              "finished_at": finished_at, "duration_sec": duration, "task_runs": task_runs, "parameters": run_params}
    notes = []
    try:
        notes = _notify(job, result)
    except Exception as exc:
        logger.warning(f"notifications of {run_id} failed: {exc}")
    with sqlite3.connect(DB_PATH) as sconn:
        sconn.execute("UPDATE job_runs SET status = ?, finished_at = ?, duration_sec = ?, tasks_summary = ?, tasks_detail = ?, notifications = ? WHERE run_id = ?",
                      (overall, finished_at, duration, json.dumps(summary), json.dumps(task_runs), json.dumps(notes), run_id))
    result["notifications"] = notes
    try:
        _baseline_table_triggers(job)                 # this run's own writes must not fire its own table triggers
        fire_event({"type": "job", "job_id": job_id, "status": overall}, ctx)
    except Exception as exc:
        logger.warning(f"post-run triggers of {run_id} failed: {exc}")
    return result


def start_run_in_background(job_id: str, trigger: str = "MANUAL", params: Optional[Dict[str, Any]] = None, repair_of: Optional[str] = None,
                            ctx: Optional[Dict[str, Any]] = None) -> str:
    """Starts a run on its own thread and returns its run id at once (the run row exists as soon as it starts; poll get_run_detail)."""
    job = get_job(job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")
    if not repair_of:
        resolve_run_params(job, params)                     # refuse bad parameters before starting anything
    run_id = f"run_{uuid.uuid4().hex[:8]}"

    def target():
        try:
            run_pipeline(job_id, trigger=trigger, params=params, run_id=run_id, repair_of=repair_of, ctx=ctx)
        except Exception as exc:
            logger.error(f"Background run {run_id} of {job_id} failed: {exc}")
    threading.Thread(target=target, name=f"job-{job_id}", daemon=True).start()
    end = time.time() + 5                                   # the caller may look the run up at once: wait until its row exists
    while time.time() < end and not get_run_detail(run_id):
        time.sleep(0.05)
    return run_id


# ---------------------------------------------------------------- triggers

def _table_dir(table: str) -> Optional[str]:
    parts = table.split(".")
    if len(parts) == 3:
        cand = os.path.join(WAREHOUSE_DIR, "catalogs", parts[0], parts[1], parts[2])
        return cand if os.path.isdir(cand) else (os.path.join(WAREHOUSE_DIR, parts[1], parts[2]) if parts[0] == "warehouse" else None)
    path = resolve_table_path(table)
    return path if os.path.isdir(path) else None


def _table_version(table: str) -> Optional[int]:
    d = _table_dir(table)
    if not d:
        return None
    log = os.path.join(d, "_delta_log")
    try:
        nums = [int(f.split(".")[0]) for f in os.listdir(log) if f.endswith(".json") and f.split(".")[0].isdigit()]
    except OSError:
        return None
    return max(nums) if nums else None


def _baseline_table_triggers(job: Dict[str, Any]) -> None:
    for tr in job.get("triggers") or []:
        if tr.get("type") == "table":
            v = _table_version(tr["table"])
            if v is not None:
                _state_set(f"table:{job['id']}:{tr['table']}", str(v))


def check_table_triggers() -> List[str]:
    """Fires the jobs whose watched Delta table has a new version since the last look. The first look only records the version."""
    fired = []
    for job in load_jobs():
        if not job.get("enabled", False):
            continue
        for tr in job.get("triggers") or []:
            if tr.get("type") != "table":
                continue
            v = _table_version(tr["table"])
            if v is None:
                continue
            key = f"table:{job['id']}:{tr['table']}"
            last = _state_get(key)
            _state_set(key, str(v))
            if last is not None and int(last) != v:
                fire_event({"type": "table", "table": tr["table"], "version": v}, only_job=job["id"])
                fired.append(job["id"])
    return fired


def fire_event(event: Dict[str, Any], ctx: Optional[Dict[str, Any]] = None, only_job: Optional[str] = None) -> List[str]:
    """Starts every enabled job with a matching trigger, each on its own thread. Chains are bounded (depth and no job twice in a chain)."""
    ctx = ctx or {"depth": 0, "chain": []}
    started = []
    for job in load_jobs():
        if not job.get("enabled", False) or (only_job and job["id"] != only_job):
            continue
        for tr in job.get("triggers") or []:
            hit = False
            if event["type"] == "job" and tr["type"] == "job" and tr["job_id"] == event["job_id"]:
                hit = tr["on"] == "completion" or (tr["on"] == "success" and event["status"] == "SUCCESS") or (
                    tr["on"] == "failure" and event["status"] in ("FAILED", "TIMEOUT"))
            elif event["type"] == "autoloader" and tr["type"] == "autoloader" and tr["pipeline_id"] == event["pipeline_id"]:
                hit = event.get("files", 0) >= tr.get("min_files", 1)
            elif event["type"] == "table" and tr["type"] == "table" and tr["table"] == event["table"]:
                hit = True
            if not hit:
                continue
            if ctx["depth"] + 1 > MAX_CHAIN_DEPTH or job["id"] in ctx["chain"]:
                logger.warning(f"Not starting job {job['id']} from {event}: the trigger chain would loop or is too deep ({ctx['chain']}).")
                continue
            try:
                start_run_in_background(job["id"], trigger=f"EVENT:{event['type']}", ctx={"depth": ctx["depth"] + 1, "chain": ctx["chain"] + [job["id"]]})
                started.append(job["id"])
            except Exception as exc:
                logger.warning(f"Could not start job {job['id']} from {event}: {exc}")
            break
    return started


# ---------------------------------------------------------------- history

def get_job_runs(job_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    init_runs_db()
    with sqlite3.connect(DB_PATH) as sconn:
        sconn.row_factory = sqlite3.Row
        cols = "run_id, job_id, job_name, trigger, status, started_at, finished_at, duration_sec, tasks_summary, parent_run_id"
        if job_id:
            cursor = sconn.execute(f"SELECT {cols} FROM job_runs WHERE job_id = ? ORDER BY started_at DESC, rowid DESC LIMIT ?", (job_id, limit))
        else:
            cursor = sconn.execute(f"SELECT {cols} FROM job_runs ORDER BY started_at DESC, rowid DESC LIMIT ?", (limit,))
        runs = []
        for r in cursor.fetchall():
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
        row = sconn.execute("SELECT * FROM job_runs WHERE run_id = ?", (run_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        for key, default in (("tasks_summary", []), ("tasks_detail", []), ("notifications", [])):
            try:
                d[key] = json.loads(d.get(key) or json.dumps(default))
            except Exception:
                d[key] = default
        d["running"] = d["run_id"] in _active
        return d


# ---------------------------------------------------------------- scheduler

async def _run_job_async(job_id: str, trigger: str):
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, run_pipeline, job_id, trigger)
    except Exception as e:
        logger.error(f"Background execution of job {job_id} failed: {e}")


_last_cron_check: Dict[str, datetime.datetime] = {}


def _cron_due(job: Dict[str, Any], now: datetime.datetime) -> bool:
    """Whether a cron tick has passed since the last look. Without catch_up a tick missed while the studio was down is skipped;
    with it the last look is remembered across restarts and a missed tick runs once."""
    cron_expr = (job.get("schedule_cron") or "").strip()
    if not cron_expr or not croniter:
        return False
    job_id = job["id"]
    last = _last_cron_check.get(job_id)
    if last is None:
        saved = _state_get(f"cron:{job_id}") if job.get("catch_up") else None
        try:
            last = datetime.datetime.fromisoformat(saved) if saved else now
        except ValueError:
            last = now
        _last_cron_check[job_id] = last
    next_time = croniter(cron_expr, last).get_next(datetime.datetime)
    if now >= next_time:
        _last_cron_check[job_id] = now
        _state_set(f"cron:{job_id}", now.isoformat())
        return True
    if job.get("catch_up") and _state_get(f"cron:{job_id}") is None:
        _state_set(f"cron:{job_id}", last.isoformat())
    return False


async def cron_scheduler_loop():
    logger.info("Starting localspark workflow scheduler loop...")
    orphaned = await asyncio.to_thread(mark_orphaned_runs)
    if orphaned:
        logger.warning(f"Marked {orphaned} run(s) left RUNNING by a previous process as FAILED.")
    while True:
        try:
            now = datetime.datetime.now()
            for job in load_jobs():
                if not job.get("enabled", False):
                    continue
                try:
                    if _cron_due(job, now):
                        logger.info(f"Cron triggering scheduled job: {job['name']} ({job['id']})")
                        asyncio.create_task(_run_job_async(job["id"], trigger="CRON"))
                except Exception as e:
                    logger.warning(f"Error evaluating the schedule of job {job['id']}: {e}")
            await asyncio.to_thread(check_table_triggers)
        except Exception as e:
            logger.error(f"Error in scheduler loop: {e}")

        await asyncio.sleep(15)
