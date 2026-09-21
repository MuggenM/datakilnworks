import os
import time
import uuid
import sqlite3
import datetime
import asyncio
import logging
from typing import Optional, Dict, Any, List, Union

from web.audit import get_db_connection, WAREHOUSE_DIR
from web.saved_queries import get_saved_query
from web.warehouses import sync_catalogs_with_duckrun

logger = logging.getLogger("localspark.alerts")

_running_alert_checks = set()


def init_alerts_db():
    """Initializes sql_alerts and alert_evaluations tables in history.db and seeds initial defaults if empty."""
    try:
        with get_db_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sql_alerts (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL DEFAULT 'martin',
                    name TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    query_id TEXT DEFAULT NULL,
                    custom_query TEXT DEFAULT NULL,
                    warehouse_id TEXT DEFAULT 'wh_starter',
                    catalog TEXT DEFAULT 'warehouse',
                    schema_name TEXT DEFAULT 'dbo',
                    target_column TEXT NOT NULL,
                    operator TEXT NOT NULL,
                    threshold_value TEXT NOT NULL,
                    schedule_interval TEXT NOT NULL DEFAULT '5m',
                    notify_on_state_change_only INTEGER NOT NULL DEFAULT 1,
                    is_enabled INTEGER NOT NULL DEFAULT 1,
                    is_muted INTEGER NOT NULL DEFAULT 0,
                    is_shared INTEGER NOT NULL DEFAULT 1,
                    state TEXT NOT NULL DEFAULT 'UNKNOWN',
                    last_evaluated_at TEXT DEFAULT NULL,
                    last_value TEXT DEFAULT NULL,
                    last_error TEXT DEFAULT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_user ON sql_alerts(user_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_state ON sql_alerts(state);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_enabled ON sql_alerts(is_enabled);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS alert_evaluations (
                    id TEXT PRIMARY KEY,
                    alert_id TEXT NOT NULL,
                    user_id TEXT NOT NULL DEFAULT 'martin',
                    evaluated_at TEXT NOT NULL,
                    observed_value TEXT,
                    threshold_value TEXT,
                    state TEXT NOT NULL,
                    duration_ms REAL NOT NULL DEFAULT 0.0,
                    error_message TEXT DEFAULT NULL,
                    FOREIGN KEY (alert_id) REFERENCES sql_alerts(id) ON DELETE CASCADE
                );
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_evaluations_alert_id 
                ON alert_evaluations(alert_id, evaluated_at DESC);
            """)

            # Check if seeding defaults is needed
            cur = conn.execute("SELECT COUNT(*) FROM sql_alerts;")
            count = cur.fetchone()[0]
            if count == 0:
                _seed_default_alerts(conn)
    except Exception as e:
        logger.error(f"Failed to initialize alerts database: {e}")


def _seed_default_alerts(conn: sqlite3.Connection):
    """Seeds starter alerts showcasing inventory health, payroll audits, and IoT sensor limits."""
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    defaults = [
        (
            "alt_low_stock_watch",
            "martin",
            "Critical Product Low Stock Alert",
            "Monitors warehouse inventory to flag any items falling below safety reorder levels (< 25 units).",
            None,
            "SELECT COUNT(*) AS low_stock_items FROM dim_products WHERE stock_qty < 25;",
            "wh_starter",
            "warehouse",
            "dbo",
            "low_stock_items",
            ">",
            "0",
            "5m",
            1,
            1,
            0,
            1,
            "UNKNOWN",
            None,
            None,
            None,
            now_str,
            now_str,
        ),
        (
            "alt_payroll_cap_monitor",
            "martin",
            "Executive Payroll Outlier Breach",
            "Validates that no individual base compensation in silver_employees exceeds the $150,000 threshold.",
            None,
            "SELECT MAX(salary) AS max_salary FROM silver_employees;",
            "wh_starter",
            "warehouse",
            "dbo",
            "max_salary",
            ">",
            "150000",
            "15m",
            1,
            1,
            0,
            1,
            "UNKNOWN",
            None,
            None,
            None,
            now_str,
            now_str,
        ),
        (
            "alt_sensor_temp_critical",
            "martin",
            "Lakehouse Sensor Temp Critical Spikes",
            "Monitors telemetry feed to detect equipment operating at critical temperatures above 85°C.",
            None,
            "SELECT MAX(temp_c) AS max_temp FROM silver_telemetry;",
            "wh_starter",
            "warehouse",
            "dbo",
            "max_temp",
            ">",
            "85.0",
            "1m",
            1,
            1,
            0,
            1,
            "UNKNOWN",
            None,
            None,
            None,
            now_str,
            now_str,
        ),
    ]
    try:
        conn.executemany("""
            INSERT INTO sql_alerts (
                id, user_id, name, description, query_id, custom_query,
                warehouse_id, catalog, schema_name, target_column, operator,
                threshold_value, schedule_interval, notify_on_state_change_only,
                is_enabled, is_muted, is_shared, state, last_evaluated_at,
                last_value, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, defaults)
        logger.info("Successfully seeded default SQL alerts.")
    except Exception as e:
        logger.error(f"Failed to seed default alerts: {e}")


# Initialize tables on import
init_alerts_db()


def evaluate_condition(observed_val: Any, operator: str, threshold: str) -> bool:
    """
    Evaluates observed value against threshold using the specified operator.
    Returns True if the alert condition is BREACHED (trigger state), False otherwise.
    """
    op = (operator or "").strip().lower()

    # Null checks
    if op in ("is_null", "null"):
        return observed_val is None or str(observed_val).strip().lower() in ("none", "null", "nan", "")
    if op in ("is_not_null", "not_null"):
        return observed_val is not None and str(observed_val).strip().lower() not in ("none", "null", "nan", "")

    if observed_val is None:
        return False

    # Attempt numeric comparison
    try:
        obs_num = float(observed_val)
        thresh_num = float(threshold)
        if op in (">", "gt"):
            return obs_num > thresh_num
        elif op in (">=", "gte"):
            return obs_num >= thresh_num
        elif op in ("<", "lt"):
            return obs_num < thresh_num
        elif op in ("<=", "lte"):
            return obs_num <= thresh_num
        elif op in ("==", "=", "eq"):
            return obs_num == thresh_num
        elif op in ("!=", "<>", "neq"):
            return obs_num != thresh_num
    except (ValueError, TypeError):
        pass

    # Fallback to string comparison
    obs_str = str(observed_val).strip()
    thresh_str = str(threshold).strip()
    if op in ("==", "=", "eq"):
        return obs_str.lower() == thresh_str.lower()
    elif op in ("!=", "<>", "neq"):
        return obs_str.lower() != thresh_str.lower()
    elif op in (">", "gt"):
        return obs_str > thresh_str
    elif op in (">=", "gte"):
        return obs_str >= thresh_str
    elif op in ("<", "lt"):
        return obs_str < thresh_str
    elif op in ("<=", "lte"):
        return obs_str <= thresh_str

    return False


def execute_alert_check(alert_id: str, triggered_by: str = "scheduler", user_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Executes an alert query against DuckDB/duckrun, extracts the target column,
    evaluates condition, updates alert state, and records to alert_evaluations.
    """
    with get_db_connection() as conn:
        row = conn.execute("SELECT * FROM sql_alerts WHERE id = ?;", (alert_id,)).fetchone()
        if not row:
            raise ValueError(f"Alert with ID {alert_id} not found.")
        alert = dict(row)

    query_text = None
    if alert.get("query_id"):
        sq = get_saved_query(alert["query_id"])
        if sq and sq.get("query_text"):
            query_text = sq["query_text"]

    if not query_text:
        query_text = alert.get("custom_query")

    if not query_text or not query_text.strip():
        raise ValueError(f"Alert '{alert.get('name')}' has no query defined.")

    start_time = time.perf_counter()
    observed_val = None
    error_msg = None
    new_state = "UNKNOWN"

    try:
        import duckrun
        duck_conn = duckrun.connect(WAREHOUSE_DIR, read_only=True)
        sync_catalogs_with_duckrun(duck_conn)

        # The alert runs as its owner: a masked owner evaluates the condition on masked values.
        from web.governance import gateway
        gateway.ensure_masks(duck_conn)
        owner = gateway.principal_for_username(alert.get("user_id"))
        governed = gateway.govern_sql(query_text, owner, client="alert", con=duck_conn.con.cursor())
        if governed.blocked:
            raise ValueError(f"Blocked by governance: {governed.blocked}")

        rel = duck_conn.sql(governed.sql)
        columns = rel.columns or []
        rows = rel.fetchall()

        if rows and len(rows) > 0:
            first_row = rows[0]
            target_col = (alert.get("target_column") or "").strip().lower()

            col_idx = 0
            if target_col and target_col != "*":
                col_map = {col.lower(): idx for idx, col in enumerate(columns)}
                if target_col in col_map:
                    col_idx = col_map[target_col]

            if col_idx < len(first_row):
                observed_val = first_row[col_idx]
        else:
            observed_val = None

        is_triggered = evaluate_condition(
            observed_val=observed_val,
            operator=alert.get("operator", ">"),
            threshold=alert.get("threshold_value", "0")
        )
        new_state = "TRIGGERED" if is_triggered else "OK"

    except Exception as e:
        logger.error(f"Error executing alert check {alert_id}: {e}")
        error_msg = str(e)
        new_state = "ERROR"

    duration_ms = round((time.perf_counter() - start_time) * 1000.0, 2)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    observed_str = str(observed_val) if observed_val is not None else None

    # Update sql_alerts state
    eval_id = f"aev_{uuid.uuid4().hex[:12]}"
    with get_db_connection() as conn:
        conn.execute("""
            UPDATE sql_alerts SET
                state = ?,
                last_evaluated_at = ?,
                last_value = ?,
                last_error = ?,
                updated_at = ?
            WHERE id = ?;
        """, (new_state, now_str, observed_str, error_msg, now_str, alert_id))

        # Insert into alert_evaluations
        conn.execute("""
            INSERT INTO alert_evaluations (
                id, alert_id, user_id, evaluated_at, observed_value, threshold_value, state, duration_ms, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            eval_id,
            alert_id,
            user_id or alert.get("user_id", "martin"),
            now_str,
            observed_str,
            str(alert.get("threshold_value", "")),
            new_state,
            duration_ms,
            error_msg
        ))

        # Prune older evaluations (keep last 100 per alert)
        conn.execute("""
            DELETE FROM alert_evaluations
            WHERE alert_id = ? AND id NOT IN (
                SELECT id FROM alert_evaluations WHERE alert_id = ? ORDER BY evaluated_at DESC LIMIT 100
            );
        """, (alert_id, alert_id))

    return {
        "evaluation_id": eval_id,
        "alert_id": alert_id,
        "name": alert.get("name"),
        "state": new_state,
        "observed_value": observed_str,
        "threshold_value": str(alert.get("threshold_value", "")),
        "operator": alert.get("operator"),
        "duration_ms": duration_ms,
        "evaluated_at": now_str,
        "error": error_msg
    }


def is_alert_due(alert: Dict[str, Any], now: datetime.datetime) -> bool:
    """Determines whether an alert is due for execution based on schedule_interval."""
    last_eval_str = alert.get("last_evaluated_at")
    if not last_eval_str:
        return True

    try:
        last_eval = datetime.datetime.fromisoformat(last_eval_str)
    except Exception:
        try:
            last_eval = datetime.datetime.strptime(last_eval_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return True

    interval_str = (alert.get("schedule_interval") or "5m").strip().lower()

    seconds = None
    if interval_str.endswith("s"):
        try:
            seconds = int(interval_str[:-1])
        except ValueError:
            pass
    elif interval_str.endswith("m"):
        try:
            seconds = int(interval_str[:-1]) * 60
        except ValueError:
            pass
    elif interval_str.endswith("h"):
        try:
            seconds = int(interval_str[:-1]) * 3600
        except ValueError:
            pass
    elif interval_str.endswith("d"):
        try:
            seconds = int(interval_str[:-1]) * 86400
        except ValueError:
            pass
    elif interval_str.isdigit():
        seconds = int(interval_str)

    if seconds is not None:
        return (now - last_eval).total_seconds() >= seconds

    # Cron evaluation if 5 space-separated parts
    parts = interval_str.split()
    if len(parts) == 5:
        try:
            from croniter import croniter
            itr = croniter(interval_str, last_eval)
            next_time = itr.get_next(datetime.datetime)
            return now >= next_time
        except Exception as e:
            logger.warning(f"Failed to evaluate cron for alert {alert.get('id')}: {e}")

    # Fallback to 5 minutes
    return (now - last_eval).total_seconds() >= 300


async def alerts_scheduler_loop():
    """Background asyncio worker that periodically evaluates due enabled alerts."""
    logger.info("Starting LocalSpark SQL alerts background scheduler loop...")
    while True:
        try:
            now = datetime.datetime.now()
            alerts = []
            with get_db_connection() as conn:
                cur = conn.execute("SELECT * FROM sql_alerts WHERE is_enabled = 1;")
                alerts = [dict(r) for r in cur.fetchall()]

            for alert in alerts:
                alert_id = alert["id"]
                if alert_id in _running_alert_checks:
                    continue

                if is_alert_due(alert, now):
                    _running_alert_checks.add(alert_id)
                    asyncio.create_task(_run_alert_check_async(alert_id))

        except Exception as e:
            logger.error(f"Error in alerts scheduler loop: {e}")

        await asyncio.sleep(15)


async def _run_alert_check_async(alert_id: str):
    """Executes an alert check in a thread executor to avoid blocking asyncio event loop."""
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, execute_alert_check, alert_id, "scheduler")
    except Exception as e:
        logger.error(f"Async evaluation of alert {alert_id} failed: {e}")
    finally:
        _running_alert_checks.discard(alert_id)


# --- CRUD Functions ---

def get_alerts(
    user_id: str = "martin",
    state_filter: Optional[str] = None,
    search: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Retrieves all accessible alerts for the user, supporting state filter and search keyword."""
    with get_db_connection() as conn:
        query = "SELECT * FROM sql_alerts WHERE (user_id = ? OR is_shared = 1)"
        params: List[Any] = [user_id]

        if state_filter and state_filter.lower() != "all":
            sf = state_filter.strip().upper()
            if sf == "MUTED":
                query += " AND is_muted = 1"
            elif sf == "TRIGGERED":
                query += " AND state = 'TRIGGERED' AND is_muted = 0"
            elif sf == "OK":
                query += " AND state = 'OK'"
            elif sf == "ERROR":
                query += " AND state = 'ERROR'"

        if search and search.strip():
            kw = f"%{search.strip().lower()}%"
            query += " AND (LOWER(name) LIKE ? OR LOWER(description) LIKE ? OR LOWER(target_column) LIKE ?)"
            params.extend([kw, kw, kw])

        query += " ORDER BY CASE WHEN state = 'TRIGGERED' AND is_muted = 0 THEN 0 ELSE 1 END, updated_at DESC;"
        cur = conn.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


def get_alert(alert_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves a single alert and its last 20 evaluations."""
    with get_db_connection() as conn:
        row = conn.execute("SELECT * FROM sql_alerts WHERE id = ?;", (alert_id,)).fetchone()
        if not row:
            return None
        alert = dict(row)
        evals_cur = conn.execute(
            "SELECT * FROM alert_evaluations WHERE alert_id = ? ORDER BY evaluated_at DESC LIMIT 20;",
            (alert_id,)
        )
        alert["evaluations"] = [dict(r) for r in evals_cur.fetchall()]
        return alert


def create_alert(data: Dict[str, Any], user_id: str = "martin") -> Dict[str, Any]:
    """Creates a new SQL alert definition."""
    alert_id = f"alt_{uuid.uuid4().hex[:8]}"
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    name = (data.get("name") or "Untitled Alert").strip()
    description = (data.get("description") or "").strip()
    query_id = data.get("query_id") or None
    custom_query = (data.get("custom_query") or "").strip() if not query_id else None
    warehouse_id = data.get("warehouse_id") or "wh_starter"
    catalog = data.get("catalog") or "warehouse"
    schema_name = data.get("schema_name") or "dbo"
    target_column = (data.get("target_column") or "count").strip()
    operator = (data.get("operator") or ">").strip()
    threshold_value = str(data.get("threshold_value", "0")).strip()
    schedule_interval = (data.get("schedule_interval") or "5m").strip()
    notify_change_only = 1 if data.get("notify_on_state_change_only", True) else 0
    is_enabled = 1 if data.get("is_enabled", True) else 0
    is_muted = 1 if data.get("is_muted", False) else 0
    is_shared = 1 if data.get("is_shared", True) else 0

    with get_db_connection() as conn:
        conn.execute("""
            INSERT INTO sql_alerts (
                id, user_id, name, description, query_id, custom_query,
                warehouse_id, catalog, schema_name, target_column, operator,
                threshold_value, schedule_interval, notify_on_state_change_only,
                is_enabled, is_muted, is_shared, state, last_evaluated_at,
                last_value, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'UNKNOWN', NULL, NULL, NULL, ?, ?);
        """, (
            alert_id, user_id, name, description, query_id, custom_query,
            warehouse_id, catalog, schema_name, target_column, operator,
            threshold_value, schedule_interval, notify_change_only,
            is_enabled, is_muted, is_shared, now_str, now_str
        ))

    # Trigger first evaluation immediately in background
    asyncio.create_task(_run_alert_check_async(alert_id))

    return get_alert(alert_id) or {"id": alert_id}


def update_alert(alert_id: str, data: Dict[str, Any], user_id: str = "martin") -> Optional[Dict[str, Any]]:
    """Updates an existing SQL alert definition."""
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_db_connection() as conn:
        row = conn.execute("SELECT * FROM sql_alerts WHERE id = ?;", (alert_id,)).fetchone()
        if not row:
            return None

        name = (data.get("name") if "name" in data else row["name"]).strip()
        description = (data.get("description") if "description" in data else row["description"]).strip()
        query_id = data.get("query_id") if "query_id" in data else row["query_id"]
        custom_query = (data.get("custom_query") if "custom_query" in data else row["custom_query"])
        if custom_query is not None:
            custom_query = custom_query.strip()
        target_column = (data.get("target_column") if "target_column" in data else row["target_column"]).strip()
        operator = (data.get("operator") if "operator" in data else row["operator"]).strip()
        threshold_value = str(data.get("threshold_value") if "threshold_value" in data else row["threshold_value"]).strip()
        schedule_interval = (data.get("schedule_interval") if "schedule_interval" in data else row["schedule_interval"]).strip()
        
        notify_change_only = row["notify_on_state_change_only"]
        if "notify_on_state_change_only" in data:
            notify_change_only = 1 if data["notify_on_state_change_only"] else 0
            
        is_enabled = row["is_enabled"]
        if "is_enabled" in data:
            is_enabled = 1 if data["is_enabled"] else 0

        is_muted = row["is_muted"]
        if "is_muted" in data:
            is_muted = 1 if data["is_muted"] else 0

        is_shared = row["is_shared"]
        if "is_shared" in data:
            is_shared = 1 if data["is_shared"] else 0

        conn.execute("""
            UPDATE sql_alerts SET
                name = ?, description = ?, query_id = ?, custom_query = ?,
                target_column = ?, operator = ?, threshold_value = ?,
                schedule_interval = ?, notify_on_state_change_only = ?,
                is_enabled = ?, is_muted = ?, is_shared = ?, updated_at = ?
            WHERE id = ?;
        """, (
            name, description, query_id, custom_query,
            target_column, operator, threshold_value,
            schedule_interval, notify_change_only,
            is_enabled, is_muted, is_shared, now_str, alert_id
        ))

    return get_alert(alert_id)


def delete_alert(alert_id: str, user_id: str = "martin") -> bool:
    """Deletes an alert and its evaluation history."""
    with get_db_connection() as conn:
        conn.execute("DELETE FROM alert_evaluations WHERE alert_id = ?;", (alert_id,))
        cur = conn.execute("DELETE FROM sql_alerts WHERE id = ?;", (alert_id,))
        return cur.rowcount > 0


def toggle_mute_alert(alert_id: str, user_id: str = "martin") -> Optional[Dict[str, Any]]:
    """Toggles muted status for an alert."""
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db_connection() as conn:
        row = conn.execute("SELECT is_muted FROM sql_alerts WHERE id = ?;", (alert_id,)).fetchone()
        if not row:
            return None
        new_muted = 0 if row["is_muted"] == 1 else 1
        conn.execute(
            "UPDATE sql_alerts SET is_muted = ?, updated_at = ? WHERE id = ?;",
            (new_muted, now_str, alert_id)
        )
    return get_alert(alert_id)


def toggle_enable_alert(alert_id: str, user_id: str = "martin") -> Optional[Dict[str, Any]]:
    """Toggles enabled/disabled status for an alert."""
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db_connection() as conn:
        row = conn.execute("SELECT is_enabled FROM sql_alerts WHERE id = ?;", (alert_id,)).fetchone()
        if not row:
            return None
        new_enabled = 0 if row["is_enabled"] == 1 else 1
        conn.execute(
            "UPDATE sql_alerts SET is_enabled = ?, updated_at = ? WHERE id = ?;",
            (new_enabled, now_str, alert_id)
        )
    return get_alert(alert_id)


def get_alert_evaluations(alert_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Fetches chronological evaluation history for an alert."""
    with get_db_connection() as conn:
        cur = conn.execute(
            "SELECT * FROM alert_evaluations WHERE alert_id = ? ORDER BY evaluated_at DESC LIMIT ?;",
            (alert_id, limit)
        )
        return [dict(r) for r in cur.fetchall()]


def get_alerts_summary(user_id: str = "martin") -> Dict[str, Any]:
    """Returns high-level summary KPIs and list of active triggered alerts for badges and modals."""
    with get_db_connection() as conn:
        cur = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN state = 'TRIGGERED' AND is_muted = 0 THEN 1 ELSE 0 END) as triggered,
                SUM(CASE WHEN state = 'OK' THEN 1 ELSE 0 END) as ok,
                SUM(CASE WHEN is_muted = 1 THEN 1 ELSE 0 END) as muted,
                SUM(CASE WHEN state = 'ERROR' THEN 1 ELSE 0 END) as error
            FROM sql_alerts
            WHERE (user_id = ? OR is_shared = 1) AND is_enabled = 1;
        """, (user_id,))
        row = cur.fetchone()

        triggered_cur = conn.execute("""
            SELECT id, name, target_column, operator, threshold_value, last_value, last_evaluated_at, state
            FROM sql_alerts
            WHERE (user_id = ? OR is_shared = 1) AND state = 'TRIGGERED' AND is_muted = 0 AND is_enabled = 1
            ORDER BY updated_at DESC
            LIMIT 10;
        """, (user_id,))
        recent_triggered = [dict(r) for r in triggered_cur.fetchall()]

        return {
            "total": (row["total"] or 0) if row else 0,
            "triggered": (row["triggered"] or 0) if row else 0,
            "ok": (row["ok"] or 0) if row else 0,
            "muted": (row["muted"] or 0) if row else 0,
            "error": (row["error"] or 0) if row else 0,
            "recent_triggered": recent_triggered
        }
