"""
Existence checks and column listings against the live DuckDB catalogs.

A catalog id is a DuckDB database name (`warehouse`, mounted catalogs such as `postgres_prod`). Delta tables of the
default warehouse are registered as views, so `duckdb_columns()` (which covers tables and views) is the single source
of truth for "does this table/column exist and what is its type".
"""

from typing import Any, Dict, List, Optional

_SYSTEM_DBS = ("system", "temp")


def _rows(con, sql: str, params: Optional[list] = None) -> List[tuple]:
    return con.execute(sql, params or []).fetchall()


def list_catalogs(con) -> List[str]:
    return [r[0] for r in _rows(con, "SELECT database_name FROM duckdb_databases() WHERE database_name NOT IN ('system','temp','memory')")]


def catalog_exists(con, catalog: str) -> bool:
    return bool(_rows(con, "SELECT 1 FROM duckdb_databases() WHERE lower(database_name) = ?", [catalog.lower()]))


def schema_exists(con, catalog: str, schema: str) -> bool:
    return bool(_rows(con, "SELECT 1 FROM duckdb_schemas() WHERE lower(database_name) = ? AND lower(schema_name) = ?",
                      [catalog.lower(), schema.lower()]))


def list_columns(con, catalog: str, schema: Optional[str] = None, table: Optional[str] = None) -> List[Dict[str, Any]]:
    """Columns (name, type, position) of tables and views, optionally narrowed to a schema/table."""
    sql = ("SELECT database_name, schema_name, table_name, column_name, data_type, column_index "
           "FROM duckdb_columns() WHERE lower(database_name) = ?")
    params: list = [catalog.lower()]
    if schema:
        sql += " AND lower(schema_name) = ?"
        params.append(schema.lower())
    if table:
        sql += " AND lower(table_name) = ?"
        params.append(table.lower())
    sql += " ORDER BY schema_name, table_name, column_index"
    return [{"catalog": r[0], "schema": r[1], "table": r[2], "column": r[3], "type": r[4], "position": r[5]}
            for r in _rows(con, sql, params)]


def table_exists(con, catalog: str, schema: str, table: str) -> bool:
    return bool(list_columns(con, catalog, schema, table))


def column_exists(con, catalog: str, schema: str, table: str, column: str) -> bool:
    return any(c["column"].lower() == column.lower() for c in list_columns(con, catalog, schema, table))


def validate_object(con, catalog: str, schema: str = "", table: str = "", column: str = "") -> None:
    """Raises ValueError with a precise message when the addressed object does not exist."""
    if not catalog_exists(con, catalog):
        raise ValueError(f"Catalog '{catalog}' does not exist.")
    if schema and not schema_exists(con, catalog, schema):
        raise ValueError(f"Schema '{catalog}.{schema}' does not exist.")
    if table and not table_exists(con, catalog, schema, table):
        raise ValueError(f"Table '{catalog}.{schema}.{table}' does not exist.")
    if column and not column_exists(con, catalog, schema, table, column):
        raise ValueError(f"Column '{column}' does not exist in '{catalog}.{schema}.{table}'.")
