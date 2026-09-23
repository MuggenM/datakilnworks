import os
import re
import time
import uuid
import sqlite3
import datetime
import shutil
import inspect
import duckdb
import duckrun
from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb.dataframe import DuckDBDataFrame
from IPython import get_ipython
from IPython.core.magic import register_line_cell_magic
from itables import show as itables_show
from IPython.display import display as ipy_display

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
os.makedirs(WAREHOUSE_DIR, exist_ok=True)

# 1. Initialize duckrun connection mapped to the local Delta Lake warehouse
duckrun_conn = duckrun.connect(WAREHOUSE_DIR, read_only=False)

# 2. Attach SQLFrame session to use the shared underlying DuckDB instance (duckrun_conn.con)
spark = DuckDBSession(conn=duckrun_conn.con)

# 3. Helper to auto-register SQLFrame DataFrames referenced in SQL queries
def _auto_register_df_from_query(query: str):
    """Inspects the query and notebook namespace to automatically register any referenced SQLFrame DataFrames."""
    ipy = get_ipython()
    ns = ipy.user_ns if ipy else {}
    # Also check caller frames if ipython namespace is empty
    if not ns:
        frame = inspect.currentframe()
        while frame:
            if frame.f_locals:
                ns.update(frame.f_locals)
            frame = frame.f_back

    for name, val in ns.items():
        if isinstance(val, DuckDBDataFrame):
            if re.search(r'\b' + re.escape(name) + r'\b', query):
                try:
                    duckrun_conn.register(name, val.toArrow())
                except Exception:
                    duckrun_conn.register(name, val.toPandas())

# Patch DuckDBDataFrame.createOrReplaceTempView to automatically register in duckrun
_orig_create_temp_view = DuckDBDataFrame.createOrReplaceTempView
def _patched_create_temp_view(self, name: str) -> None:
    _orig_create_temp_view(self, name)
    try:
        duckrun_conn.register(name, self.toArrow())
    except Exception:
        duckrun_conn.register(name, self.toPandas())
DuckDBDataFrame.createOrReplaceTempView = _patched_create_temp_view

# Audit Logging Helper for Notebook Queries
HISTORY_DB = os.path.join(WAREHOUSE_DIR, ".metadata", "history.db")

def _log_notebook_query(query_text: str, duration_ms: float, rows: int = 0, status: str = "SUCCESS", err: str = None):
    try:
        os.makedirs(os.path.dirname(HISTORY_DB), exist_ok=True)
        with sqlite3.connect(HISTORY_DB, timeout=5.0) as sconn:
            sconn.execute("PRAGMA journal_mode=WAL;")
            sconn.execute("PRAGMA synchronous=NORMAL;")
            qid = f"q_{uuid.uuid4().hex[:8]}"
            now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sconn.execute("""
                INSERT INTO query_history (
                    query_id, query_text, executed_at, duration_ms,
                    rows_produced, status, error_message, client,
                    is_mutation, user
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (qid, query_text.strip(), now_str, round(duration_ms, 2), rows, status, err, "NOTEBOOK", 0, "admin"))
    except Exception:
        pass

# Wrap duckrun_conn.sql to auto-register any SQLFrame DataFrames referenced in queries
_orig_duckrun_sql = duckrun_conn.sql
def _smart_duckrun_sql(query: str, *args, **kwargs):
    _auto_register_df_from_query(query)
    start_t = time.perf_counter()
    try:
        res = _orig_duckrun_sql(query, *args, **kwargs)
        elapsed_ms = (time.perf_counter() - start_t) * 1000
        rows = 0
        if res is not None and hasattr(res, "__len__"):
            try:
                rows = len(res)
            except Exception:
                rows = 0
        _log_notebook_query(query, elapsed_ms, rows=rows, status="SUCCESS")
        return res
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start_t) * 1000
        _log_notebook_query(query, elapsed_ms, rows=0, status="FAILED", err=str(e))
        raise e
duckrun_conn.sql = _smart_duckrun_sql

# 4. Databricks display() implementation
def display(obj, limit=50):
    """Emulates Databricks display() using client-side interactive tables."""
    if hasattr(obj, "toPandas"):
        pdf = obj.limit(limit).toPandas()
        itables_show(pdf, paging=True, maxBytes=0)
    elif isinstance(obj, (duckdb.DuckDBPyRelation, duckdb.DuckDBPyConnection)):
        itables_show(obj.limit(limit).df(), paging=True, maxBytes=0)
    elif hasattr(obj, "head") and hasattr(obj, "columns"):  # pandas / polars
        itables_show(obj.head(limit), paging=True, maxBytes=0)
    else:
        ipy_display(obj)

# 5. Mock Databricks dbutils (Filesystem and Notebook context)
class DBUtilsFS:
    @staticmethod
    def _resolve_path(path: str) -> str:
        clean = path.replace("dbfs:/", "").lstrip("/")
        direct = os.path.join(WAREHOUSE_DIR, clean)
        # Check direct path first
        if os.path.exists(direct):
            return direct
        # Duckrun often nests tables under the default schema (e.g. dbo/)
        dbo_path = os.path.join(WAREHOUSE_DIR, "dbo", clean)
        if os.path.exists(dbo_path):
            return dbo_path
        return direct

    @classmethod
    def ls(cls, path: str):
        resolved = cls._resolve_path(path)
        if not os.path.exists(resolved):
            return []
        items = []
        for name in os.listdir(resolved):
            full_path = os.path.join(resolved, name)
            items.append({
                "path": f"dbfs:/{os.path.relpath(full_path, WAREHOUSE_DIR)}",
                "name": name,
                "size": os.path.getsize(full_path),
                "isDir": os.path.isdir(full_path)
            })
        return items

    @classmethod
    def rm(cls, path: str, recurse: bool = False):
        resolved = cls._resolve_path(path)
        if os.path.isdir(resolved) and recurse:
            shutil.rmtree(resolved)
        elif os.path.exists(resolved):
            os.remove(resolved)

    @classmethod
    def mkdirs(cls, path: str):
        clean = path.replace("dbfs:/", "").lstrip("/")
        target = os.path.join(WAREHOUSE_DIR, clean)
        os.makedirs(target, exist_ok=True)

    @classmethod
    def head(cls, path: str, max_bytes: int = 65536):
        resolved = cls._resolve_path(path)
        with open(resolved, "rb") as f:
            return f.read(max_bytes).decode("utf-8", errors="replace")

class DBUtilsNotebook:
    @staticmethod
    def exit(value: str = ""):
        print(f"Notebook exited with: {value}")

class DBUtils:
    fs = DBUtilsFS()
    notebook = DBUtilsNotebook()

dbutils = DBUtils()

# 6. Cell magic %sql and %%sql for interactive SQL execution
@register_line_cell_magic
def sql(line, cell=None):
    query = (cell if cell else line).strip()
    if not query:
        return
    _auto_register_df_from_query(query)
    res = duckrun_conn.sql(query)
    if res is not None and hasattr(res, "df"):
        try:
            df = res.df()
            itables_show(df, paging=True, maxBytes=0)
        except Exception:
            pass
    elif res is not None and hasattr(res, "show"):
        res.show()

# 7. Preload MLflow tracking shim
try:
    import sys
    sys.path.insert(0, "/workspace")
    from web import mlflow_shim as mlflow
    sys.modules["mlflow"] = mlflow
    _has_mlflow = True
except Exception:
    _has_mlflow = False

# Export objects to the notebook's global namespace if running inside IPython
ipy = get_ipython()
if ipy is not None:
    globals_dict = {
        "spark": spark,
        "dbutils": dbutils,
        "display": display,
        "conn": duckrun_conn,
        "duckrun_conn": duckrun_conn,
    }
    if _has_mlflow:
        globals_dict["mlflow"] = mlflow
        globals_dict["MlflowClient"] = mlflow.MlflowClient
        try:
            mlflow.autolog(silent=True)
        except Exception:
            pass
    ipy.user_ns.update(globals_dict)

print("⚡ Databricks-local environment ready (SQLFrame + duckrun + DuckDB).")
print(f"📦 SparkSession initialized. Warehouse path: {WAREHOUSE_DIR}")
print("🔧 Preloaded globals: 'spark', 'dbutils', 'display()', 'conn', 'mlflow', '%sql' / '%%sql'")
print("🤖 MLflow Autologging enabled for Scikit-Learn & ML frameworks.")
