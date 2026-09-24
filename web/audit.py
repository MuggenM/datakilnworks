import os
import time
import uuid
import sqlite3
import datetime
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger("localspark.audit")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
if not os.path.exists(WAREHOUSE_DIR):
    local_alt = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "warehouse"))
    if os.path.exists(local_alt):
        WAREHOUSE_DIR = local_alt
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
DB_PATH = os.path.join(METADATA_DIR, "history.db")

def get_db_connection() -> sqlite3.Connection:
    os.makedirs(METADATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_history_db():
    try:
        with get_db_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS query_history (
                    query_id TEXT PRIMARY KEY,
                    query_text TEXT NOT NULL,
                    executed_at TEXT NOT NULL,
                    duration_ms REAL NOT NULL,
                    rows_produced INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    client TEXT NOT NULL DEFAULT 'SQL_EDITOR',
                    is_mutation INTEGER NOT NULL DEFAULT 0,
                    user TEXT NOT NULL DEFAULT 'admin',
                    warehouse_id TEXT DEFAULT 'wh_starter',
                    catalog TEXT DEFAULT 'warehouse'
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_history_executed_at ON query_history(executed_at DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_history_status ON query_history(status);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_history_client ON query_history(client);")
            try:
                conn.execute("ALTER TABLE query_history ADD COLUMN warehouse_id TEXT DEFAULT 'wh_starter';")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE query_history ADD COLUMN catalog TEXT DEFAULT 'warehouse';")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE query_history ADD COLUMN profile_json TEXT;")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE query_history ADD COLUMN executed_by TEXT DEFAULT 'local-studio';")
            except sqlite3.OperationalError:
                pass
            try:
                # number of columns column-masking replaced in this query's result (governance)
                conn.execute("ALTER TABLE query_history ADD COLUMN masked_columns INTEGER NOT NULL DEFAULT 0;")
            except sqlite3.OperationalError:
                pass
    except Exception as e:
        logger.error(f"Failed to initialize history database: {e}")

# Initialize on module load
init_history_db()

def log_query(
    query_text: str,
    duration_ms: float,
    rows_produced: int = 0,
    status: str = "SUCCESS",
    error_message: Optional[str] = None,
    client: str = "SQL_EDITOR",
    is_mutation: bool = False,
    user: str = "admin",
    warehouse_id: str = "wh_starter",
    catalog: str = "warehouse",
    profile_json: Optional[str] = None,
    executed_by: Optional[str] = None,
    masked_columns: int = 0
) -> str:
    query_id = f"q_{uuid.uuid4().hex[:8]}"
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    node_source = executed_by or "local-studio"
    try:
        with get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO query_history (
                    query_id, query_text, executed_at, duration_ms,
                    rows_produced, status, error_message, client,
                    is_mutation, user, warehouse_id, catalog, profile_json, executed_by, masked_columns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    query_id,
                    query_text.strip(),
                    now_str,
                    round(duration_ms, 2),
                    int(rows_produced or 0),
                    status.upper(),
                    error_message,
                    client.upper(),
                    1 if is_mutation else 0,
                    user,
                    warehouse_id,
                    catalog,
                    profile_json,
                    node_source,
                    int(masked_columns or 0)
                )
            )
        return query_id
    except Exception as e:
        logger.warning(f"Error logging query to history.db: {e}")
        return query_id

def save_query_profile(query_id: str, profile_json: str) -> bool:
    try:
        with get_db_connection() as conn:
            conn.execute("UPDATE query_history SET profile_json = ? WHERE query_id = ?", (profile_json, query_id))
        return True
    except Exception as e:
        logger.warning(f"Error saving query profile for {query_id}: {e}")
        return False

def get_query_history(
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = None,
    client: Optional[str] = None,
    search: Optional[str] = None,
    min_duration_ms: Optional[float] = None,
    user: Optional[str] = None
) -> Dict[str, Any]:
    init_history_db()
    where_clauses = ["1=1"]
    params: List[Any] = []

    if user and user.upper() != "ALL":
        where_clauses.append("user = ?")
        params.append(user)

    if status and status.upper() != "ALL":
        where_clauses.append("status = ?")
        params.append(status.upper())

    if client and client.upper() != "ALL":
        where_clauses.append("client = ?")
        params.append(client.upper())

    if search and search.strip():
        where_clauses.append("(query_text LIKE ? OR error_message LIKE ?)")
        term = f"%{search.strip()}%"
        params.extend([term, term])

    if min_duration_ms is not None and min_duration_ms > 0:
        where_clauses.append("duration_ms >= ?")
        params.append(min_duration_ms)

    where_sql = " AND ".join(where_clauses)

    with get_db_connection() as conn:
        count_cursor = conn.execute(f"SELECT COUNT(*) FROM query_history WHERE {where_sql}", params)
        total_count = count_cursor.fetchone()[0]

        metrics_cursor = conn.execute(f"""
            SELECT
                COUNT(*) as total_queries,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
                AVG(duration_ms) as avg_duration_ms,
                SUM(rows_produced) as total_rows_produced
            FROM query_history
            WHERE {where_sql}
        """, params)
        m_row = metrics_cursor.fetchone()
        tot = m_row["total_queries"] or 0
        succ = m_row["success_count"] or 0
        fail = m_row["failed_count"] or 0
        avg_d = round(m_row["avg_duration_ms"] or 0.0, 2)
        tot_rows = m_row["total_rows_produced"] or 0
        succ_rate = round((succ / tot * 100.0), 1) if tot > 0 else 100.0

        metrics = {
            "total_queries": tot,
            "success_count": succ,
            "failed_count": fail,
            "success_rate_pct": succ_rate,
            "avg_duration_ms": avg_d,
            "total_rows_produced": tot_rows
        }

        query_sql = f"""
            SELECT * FROM query_history
            WHERE {where_sql}
            ORDER BY executed_at DESC, rowid DESC
            LIMIT ? OFFSET ?
        """
        page_params = params + [limit, offset]
        rows_cursor = conn.execute(query_sql, page_params)
        history = [dict(r) for r in rows_cursor.fetchall()]

    return {
        "history": history,
        "total_count": total_count,
        "metrics": metrics
    }

def get_query_by_id(query_id: str) -> Optional[Dict[str, Any]]:
    init_history_db()
    with get_db_connection() as conn:
        cursor = conn.execute("SELECT * FROM query_history WHERE query_id = ?", (query_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

def clear_query_history() -> bool:
    init_history_db()
    with get_db_connection() as conn:
        conn.execute("DELETE FROM query_history;")
    return True


def delete_queries(query_ids) -> int:
    """Deletes the given history rows; returns how many existed. (Callers restrict this to admins and audit it.)"""
    ids = [q for q in dict.fromkeys(query_ids) if isinstance(q, str) and q]
    if not ids:
        return 0
    init_history_db()
    deleted = 0
    with get_db_connection() as conn:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            deleted += conn.execute(f"DELETE FROM query_history WHERE query_id IN ({','.join('?' * len(chunk))})", chunk).rowcount
    return deleted
