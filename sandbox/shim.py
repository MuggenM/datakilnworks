"""
IPython startup for sandboxed kernels (the counterpart of config/00_databricks_shim.py).

The kernel has no warehouse. `spark`, `conn` and `%sql` work on a private in-memory DuckDB, and data arrives from the studio's
governed endpoint /api/sandbox/sql, already masked for the user this kernel belongs to:

  * a query that only reads warehouse tables is sent to the studio as one statement (filters, joins and aggregates run there
    and only the result comes back);
  * a query that also touches local objects (DataFrames, temp views) pulls each warehouse table it names into local memory
    (masked, capped at DKW_MAX_ROWS rows) and runs here.

Nothing local can undo a mask, because the values that reach the kernel are already masked.
"""

import itertools
import json
import os
import time
import urllib.error
import urllib.request

import duckdb
import pyarrow as pa
import sqlglot
from IPython import get_ipython
from IPython.core.magic import register_line_cell_magic
from IPython.display import display as ipy_display
from sqlglot import exp
from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb.dataframe import DuckDBDataFrame
from sqlframe.duckdb.readwriter import DuckDBDataFrameReader

GATEWAY = os.environ["DKW_GATEWAY_URL"].rstrip("/")
TOKEN_FILE = os.environ["DKW_TOKEN_FILE"]
MAX_ROWS = int(os.environ.get("DKW_MAX_ROWS", "1000000"))
TABLE_TTL = 30.0                       # seconds a pulled table is reused before it is fetched (and re-masked) again

_local = duckdb.connect(":memory:")
_loaded = {}                           # local table key -> time it was pulled
_result_ids = itertools.count(1)
_notified = set()


_SYSTEM_SCHEMAS = {"information_schema", "pg_catalog"}


def _quote(part):
    return '"' + part.replace('"', '""') + '"'


def _remote(sql):
    """Runs `sql` in the studio as this kernel's user; returns an Arrow table (masked)."""
    try:
        with open(TOKEN_FILE) as f:
            token = f.read().strip()
    except OSError:
        raise RuntimeError("No sandbox token: run the cell again from the Studio.")
    request = urllib.request.Request(
        GATEWAY + "/api/sandbox/sql", data=json.dumps({"sql": sql, "max_rows": MAX_ROWS}).encode(),
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=600) as response:
            body, masked = response.read(), response.headers.get("X-Masked-Columns")
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read()).get("detail", "")
        except Exception:
            detail = ""
        detail = detail if isinstance(detail, str) and detail else f"HTTP {exc.code}"
        raise (PermissionError if exc.code in (401, 403) else RuntimeError)(detail) from None
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Cannot reach the Studio: {exc.reason}") from None
    if masked and masked not in _notified:
        _notified.add(masked)
        try:
            columns = json.loads(masked)
            if columns:
                print("🔒 Masking policies apply to: " + ", ".join(columns))
        except Exception:
            pass
    return pa.ipc.open_stream(body).read_all()


def _refs(sql):
    """(catalog, schema, table) of every base table a statement reads (CTE names and table functions excluded)."""
    found = []
    try:
        trees = sqlglot.parse(sql, dialect="duckdb")
    except Exception:
        return found
    for tree in trees:
        if tree is None:
            continue
        ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
        for table in tree.find_all(exp.Table):
            if not isinstance(table.this, exp.Identifier) or (not table.db and table.name.lower() in ctes):
                continue
            ref = (table.catalog, table.db, table.name)
            if table.db.lower() in _SYSTEM_SCHEMAS or table.catalog.lower() in ("system", "temp") or table.name.lower().startswith("duckdb_"):
                continue                # DuckDB's own catalog views (SQLFrame reads them for column lookups)
            if ref not in found:
                found.append(ref)
    return found


def _key(ref):
    return ".".join(p.lower() for p in ref if p)


def _exists_locally(ref):
    cat, db, name = ref
    row = _local.execute(
        "SELECT 1 FROM information_schema.tables WHERE lower(table_name) = ? AND (? = '' OR lower(table_schema) = ?) "
        "AND (? = '' OR lower(table_catalog) = ?) LIMIT 1", [name.lower(), db.lower(), db.lower(), cat.lower(), cat.lower()]).fetchone()
    return row is not None


def _is_remote(ref):
    return _key(ref) in _loaded or not _exists_locally(ref)


def _pull(ref):
    """Copies one warehouse table (as masked for this user) into local memory under the same qualified name."""
    cat, db, name = ref
    table = _remote("SELECT * FROM " + ".".join(_quote(p) for p in ref if p))
    if cat:
        _local.execute(f"ATTACH IF NOT EXISTS ':memory:' AS {_quote(cat)}")
    if db:
        _local.execute(f"CREATE SCHEMA IF NOT EXISTS {(_quote(cat) + '.') if cat else ''}{_quote(db)}")
    _local.register("__dkw_pull", table)
    try:
        _local.execute("CREATE OR REPLACE TABLE " + ".".join(_quote(p) for p in ref if p) + " AS SELECT * FROM __dkw_pull")
    finally:
        _local.unregister("__dkw_pull")
    _loaded[_key(ref)] = time.time()


def _ensure_tables(sql):
    for ref in _refs(sql):
        stamp = _loaded.get(_key(ref))
        if stamp is not None and time.time() - stamp < TABLE_TTL:
            continue
        if stamp is not None or not _exists_locally(ref):
            _pull(ref)


def _pushable(sql):
    """A plain read whose tables are all warehouse tables: the studio can answer it in one round trip."""
    try:
        trees = [t for t in sqlglot.parse(sql, dialect="duckdb") if t is not None]
    except Exception:
        return False
    if len(trees) != 1 or not isinstance(trees[0], exp.Query):
        return False
    refs = _refs(sql)
    return bool(refs) and all(_is_remote(r) for r in refs)


def _to_local(table):
    name = f"__dkw_result_{next(_result_ids)}"
    _local.register("__dkw_res_in", table)
    try:
        _local.execute(f'CREATE TABLE "{name}" AS SELECT * FROM __dkw_res_in')
    finally:
        _local.unregister("__dkw_res_in")
    return name


class SandboxConnection:
    """The `conn` / `duckrun_conn` of a sandboxed kernel: DuckDB-style .sql() over governed data."""

    def sql(self, query, *args, **kwargs):
        query = query.strip().rstrip(";")
        if _pushable(query):
            return _local.sql(f'SELECT * FROM "{_to_local(_remote(query))}"')
        _ensure_tables(query)
        return _local.sql(query, *args, **kwargs)

    query = sql

    def execute(self, query, *args, **kwargs):
        _ensure_tables(query)
        return _local.execute(query, *args, **kwargs)

    def register(self, name, obj):
        return _local.register(name, obj)

    def __getattr__(self, name):
        return getattr(_local, name)


class _GovernedReader(DuckDBDataFrameReader):
    def table(self, tableName):
        _ensure_tables(f"SELECT * FROM {tableName}")
        return super().table(tableName)


class _GovernedSession(DuckDBSession):
    _reader = _GovernedReader

    def _execute(self, sql):
        _ensure_tables(sql)
        super()._execute(sql)

    def table(self, tableName):
        _ensure_tables(f"SELECT * FROM {tableName}")
        return super().table(tableName)


conn = SandboxConnection()
spark = _GovernedSession(conn=_local)
_spark_sql = spark.sql


def _governed_spark_sql(query, *args, **kwargs):
    if not args and not kwargs and _pushable(query.strip().rstrip(";")):
        return _spark_sql(f'SELECT * FROM "{_to_local(_remote(query.strip().rstrip(";")))}"')
    return _spark_sql(query, *args, **kwargs)


spark.sql = _governed_spark_sql


def _register_dataframes(query):
    ipy = get_ipython()
    import re
    for name, value in (ipy.user_ns.items() if ipy else []):
        if isinstance(value, DuckDBDataFrame) and re.search(r"\b" + re.escape(name) + r"\b", query):
            _local.register(name, value.toArrow())


_orig_create_temp_view = DuckDBDataFrame.createOrReplaceTempView


def _create_temp_view(self, name):
    try:
        _orig_create_temp_view(self, name)
    except Exception:                   # SQLFrame's catalog insists on one table-name depth; the DuckDB registration below is what SQL uses
        pass
    _local.register(name, self.toArrow())


DuckDBDataFrame.createOrReplaceTempView = _create_temp_view

try:
    from itables import show as _itables_show
except Exception:                       # pragma: no cover
    _itables_show = None


def display(obj, limit=50):
    if _itables_show is None:
        return ipy_display(obj)
    if hasattr(obj, "toPandas"):
        _itables_show(obj.limit(limit).toPandas(), paging=True, maxBytes=0)
    elif isinstance(obj, duckdb.DuckDBPyRelation):
        _itables_show(obj.limit(limit).df(), paging=True, maxBytes=0)
    elif hasattr(obj, "head") and hasattr(obj, "columns"):
        _itables_show(obj.head(limit), paging=True, maxBytes=0)
    else:
        ipy_display(obj)


class _NoDbutils:
    """dbutils.fs reads the warehouse directory, which does not exist in the sandbox."""

    def __getattr__(self, name):
        raise PermissionError("dbutils is not available in the notebook sandbox: it works on warehouse files, which are "
                              "not accessible while masking policies apply. Use spark.sql(...) / %sql instead.")


@register_line_cell_magic
def sql(line, cell=None):
    query = (cell if cell else line).strip()
    if not query:
        return
    _register_dataframes(query)
    result = conn.sql(query)
    if result is not None and _itables_show is not None:
        _itables_show(result.df(), paging=True, maxBytes=0)


ipy = get_ipython()
if ipy is not None:
    ipy.user_ns.update({"spark": spark, "conn": conn, "duckrun_conn": conn, "display": display, "dbutils": _NoDbutils()})

print("🔒 Sandboxed environment: data is read through the governance gateway (masking policies apply).")
print("🔧 Preloaded globals: 'spark', 'conn', 'display()', '%sql' / '%%sql'. Files in the warehouse are not accessible.")
