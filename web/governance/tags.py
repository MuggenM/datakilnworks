"""
Tag definitions, assignments and effective-tag resolution.

Tags attach to four levels: catalog, schema, table, column. A column's *effective* tags are the union of every level
above it, resolved per tag key with the most specific level winning (column > table > schema > catalog). Inheritance is
computed at read time, so columns added later (schema evolution, Auto-Loader) pick up table/schema tags automatically.
"""

import json
import re
import threading
from typing import Any, Dict, List, Optional, Tuple

from web.governance import store

TAG_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
MAX_VALUE_LEN = 128
LEVELS = ("catalog", "schema", "table", "column")

Key = Tuple[str, str, str]


class NotFound(ValueError):
    """Raised when a definition or assignment does not exist (mapped to HTTP 404)."""


def norm(ident: Optional[str]) -> str:
    """DuckDB identifiers are case-insensitive; store them lower-cased and unquoted."""
    return (ident or "").strip().strip('"').lower()


def level_of(schema_name: str, table_name: str, column_name: str) -> str:
    if column_name:
        return "column"
    if table_name:
        return "table"
    if schema_name:
        return "schema"
    return "catalog"


def _check_hierarchy(catalog: str, schema_name: str, table_name: str, column_name: str) -> None:
    if not catalog:
        raise ValueError("A catalog is required.")
    if table_name and not schema_name:
        raise ValueError("A table tag needs a schema.")
    if column_name and not table_name:
        raise ValueError("A column tag needs a table.")


def object_label(catalog: str, schema_name: str = "", table_name: str = "", column_name: str = "") -> str:
    return ".".join(p for p in (catalog, schema_name, table_name, column_name) if p)


# ----------------------------------------------------------------------------
# Definitions
# ----------------------------------------------------------------------------

def _definition_row(row) -> Dict[str, Any]:
    d = dict(row)
    d["allowed_values"] = json.loads(d["allowed_values"]) if d.get("allowed_values") else None
    return d


def create_definition(tag_key: str, description: str = "", allowed_values: Optional[List[str]] = None,
                      actor: str = "admin") -> Dict[str, Any]:
    key = norm(tag_key)
    if not TAG_KEY_RE.match(key):
        raise ValueError("Tag keys are 1-64 characters of a-z, 0-9, '_', '.', '-' and must start with a letter or digit.")
    values = None
    if allowed_values:
        values = sorted({v.strip() for v in allowed_values if v and v.strip()})
        if any(len(v) > MAX_VALUE_LEN for v in values):
            raise ValueError(f"Allowed values are limited to {MAX_VALUE_LEN} characters.")
    store.init_governance_db()
    conn = store.get_db()
    try:
        if conn.execute("SELECT 1 FROM tag_definitions WHERE tag_key = ?", (key,)).fetchone():
            raise ValueError(f"Tag '{key}' already exists.")
        conn.execute(
            "INSERT INTO tag_definitions (tag_key, description, allowed_values, created_by, created_at) VALUES (?, ?, ?, ?, ?)",
            (key, (description or "").strip(), json.dumps(values) if values else None, actor, store.utcnow()))
        store.bump_version(conn)
        store.write_audit(conn, actor, "TAG_DEFINE", key, {"allowed_values": values, "description": description})
        conn.commit()
    finally:
        conn.close()
    return get_definition(key)


def get_definition(tag_key: str) -> Dict[str, Any]:
    conn = store.get_db()
    try:
        row = conn.execute("SELECT * FROM tag_definitions WHERE tag_key = ?", (norm(tag_key),)).fetchone()
        if not row:
            raise NotFound(f"Tag '{tag_key}' is not defined.")
        return _definition_row(row)
    finally:
        conn.close()


def list_definitions() -> List[Dict[str, Any]]:
    """Definitions with assignment and policy usage counts."""
    store.init_governance_db()
    conn = store.get_db()
    try:
        rows = conn.execute("""
            SELECT d.*,
                   (SELECT COUNT(*) FROM object_tags t WHERE t.tag_key = d.tag_key AND t.orphaned = 0) AS assignments,
                   (SELECT COUNT(*) FROM object_tags t WHERE t.tag_key = d.tag_key AND t.orphaned = 1) AS orphaned_assignments,
                   (SELECT COUNT(*) FROM masking_policies p WHERE p.tag_key = d.tag_key) AS policies
            FROM tag_definitions d ORDER BY d.tag_key
        """).fetchall()
        return [_definition_row(r) for r in rows]
    finally:
        conn.close()


def delete_definition(tag_key: str, force: bool = False, actor: str = "admin") -> Dict[str, int]:
    key = norm(tag_key)
    conn = store.get_db()
    try:
        if not conn.execute("SELECT 1 FROM tag_definitions WHERE tag_key = ?", (key,)).fetchone():
            raise NotFound(f"Tag '{tag_key}' is not defined.")
        assignments = conn.execute("SELECT COUNT(*) FROM object_tags WHERE tag_key = ?", (key,)).fetchone()[0]
        policies = conn.execute("SELECT COUNT(*) FROM masking_policies WHERE tag_key = ?", (key,)).fetchone()[0]
        if (assignments or policies) and not force:
            raise ValueError(
                f"Tag '{key}' is in use ({assignments} assignment(s), {policies} masking policy(ies)). "
                "Remove them first or delete with force=true.")
        conn.execute("DELETE FROM masking_policies WHERE tag_key = ?", (key,))
        conn.execute("DELETE FROM object_tags WHERE tag_key = ?", (key,))
        conn.execute("DELETE FROM tag_definitions WHERE tag_key = ?", (key,))
        store.bump_version(conn)
        store.write_audit(conn, actor, "TAG_UNDEFINE", key, {"assignments_removed": assignments, "policies_removed": policies})
        conn.commit()
        return {"assignments_removed": assignments, "policies_removed": policies}
    finally:
        conn.close()


# ----------------------------------------------------------------------------
# Assignments
# ----------------------------------------------------------------------------

def set_tag(*, catalog: str, schema_name: str = "", table_name: str = "", column_name: str = "",
            tag_key: str, tag_value: str = "", actor: str = "admin", source: str = "manual") -> Dict[str, Any]:
    """Creates or updates one assignment. Existence of the object is validated by the caller (routes)."""
    catalog, schema_name, table_name, column_name = norm(catalog), norm(schema_name), norm(table_name), norm(column_name)
    _check_hierarchy(catalog, schema_name, table_name, column_name)
    key = norm(tag_key)
    value = (tag_value or "").strip()
    if len(value) > MAX_VALUE_LEN:
        raise ValueError(f"Tag values are limited to {MAX_VALUE_LEN} characters.")
    definition = get_definition(key)
    allowed = definition["allowed_values"]
    if allowed and value not in allowed:
        raise ValueError(f"'{value}' is not an allowed value for tag '{key}'. Allowed: {', '.join(allowed)}.")
    label = object_label(catalog, schema_name, table_name, column_name)

    conn = store.get_db()
    try:
        previous = conn.execute(
            "SELECT tag_value FROM object_tags WHERE catalog=? AND schema_name=? AND table_name=? AND column_name=? AND tag_key=?",
            (catalog, schema_name, table_name, column_name, key)).fetchone()
        conn.execute("""
            INSERT INTO object_tags (catalog, schema_name, table_name, column_name, tag_key, tag_value, source,
                                     orphaned, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT (catalog, schema_name, table_name, column_name, tag_key) DO UPDATE SET
                tag_value = excluded.tag_value, source = excluded.source, orphaned = 0
        """, (catalog, schema_name, table_name, column_name, key, value, source, actor, store.utcnow()))
        store.bump_version(conn)
        store.write_audit(conn, actor, "TAG_SET", label,
                          {"tag": key, "value": value, "previous": previous["tag_value"] if previous else None,
                           "source": source, "level": level_of(schema_name, table_name, column_name)})
        conn.commit()
    finally:
        conn.close()
    return {"object": label, "level": level_of(schema_name, table_name, column_name), "tag_key": key, "tag_value": value,
            "source": source}


def unset_tag(*, catalog: str, schema_name: str = "", table_name: str = "", column_name: str = "",
              tag_key: str, actor: str = "admin") -> bool:
    catalog, schema_name, table_name, column_name = norm(catalog), norm(schema_name), norm(table_name), norm(column_name)
    _check_hierarchy(catalog, schema_name, table_name, column_name)
    key = norm(tag_key)
    label = object_label(catalog, schema_name, table_name, column_name)
    conn = store.get_db()
    try:
        cur = conn.execute(
            "DELETE FROM object_tags WHERE catalog=? AND schema_name=? AND table_name=? AND column_name=? AND tag_key=?",
            (catalog, schema_name, table_name, column_name, key))
        if cur.rowcount:
            store.bump_version(conn)
            store.write_audit(conn, actor, "TAG_UNSET", label, {"tag": key})
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def list_assignments(*, tag_key: Optional[str] = None, tag_value: Optional[str] = None, catalog: Optional[str] = None,
                     include_orphaned: bool = True, limit: int = 500) -> List[Dict[str, Any]]:
    clauses, params = [], []
    if tag_key:
        clauses.append("tag_key = ?")
        params.append(norm(tag_key))
    if tag_value is not None:
        clauses.append("tag_value = ?")
        params.append(tag_value)
    if catalog:
        clauses.append("catalog = ?")
        params.append(norm(catalog))
    if not include_orphaned:
        clauses.append("orphaned = 0")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    conn = store.get_db()
    try:
        rows = conn.execute(
            f"SELECT * FROM object_tags {where} ORDER BY catalog, schema_name, table_name, column_name, tag_key LIMIT ?",
            (*params, min(max(limit, 1), 5000))).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["level"] = level_of(d["schema_name"], d["table_name"], d["column_name"])
            d["object"] = object_label(d["catalog"], d["schema_name"], d["table_name"], d["column_name"])
            out.append(d)
        return out
    finally:
        conn.close()


# ----------------------------------------------------------------------------
# Effective-tag resolution (cached against the governance version)
# ----------------------------------------------------------------------------

_index_lock = threading.Lock()
_index: Dict[str, Any] = {"version": -1, "data": None}


def _load_index() -> Dict[str, Any]:
    """Loads every non-orphaned assignment into lookup tables keyed by object path."""
    conn = store.get_db()
    try:
        rows = conn.execute("SELECT * FROM object_tags WHERE orphaned = 0").fetchall()
    finally:
        conn.close()
    catalogs: Dict[str, Dict[str, str]] = {}
    schemas: Dict[Tuple[str, str], Dict[str, str]] = {}
    tables: Dict[Key, Dict[str, str]] = {}
    columns: Dict[Key, Dict[str, Dict[str, str]]] = {}
    for r in rows:
        cat, sch, tbl, col, key, val = r["catalog"], r["schema_name"], r["table_name"], r["column_name"], r["tag_key"], r["tag_value"]
        if col:
            columns.setdefault((cat, sch, tbl), {}).setdefault(col, {})[key] = val
        elif tbl:
            tables.setdefault((cat, sch, tbl), {})[key] = val
        elif sch:
            schemas.setdefault((cat, sch), {})[key] = val
        else:
            catalogs.setdefault(cat, {})[key] = val
    return {"catalogs": catalogs, "schemas": schemas, "tables": tables, "columns": columns}


def get_index() -> Dict[str, Any]:
    """The tag lookup index, rebuilt only when governance.db changed (one point read per call)."""
    version = store.get_version()
    with _index_lock:
        if _index["version"] != version or _index["data"] is None:
            _index["data"] = _load_index()
            _index["version"] = version
        return _index["data"]


def effective_table_tags(catalog: str, schema_name: str, table_name: str) -> Dict[str, Dict[str, str]]:
    """
    {tag_key: {"value": v, "level": "catalog|schema|table"}} inherited at the table itself, with no column overlay.
    What a table-level policy (a row filter) resolves against: masking's `effective_tags` layers column tags on top
    of this same inheritance for a per-column result.
    """
    catalog, schema_name, table_name = norm(catalog), norm(schema_name), norm(table_name)
    idx = get_index()
    inherited: Dict[str, Dict[str, str]] = {}
    for level, level_tags in (("catalog", idx["catalogs"].get(catalog, {})),
                              ("schema", idx["schemas"].get((catalog, schema_name), {})),
                              ("table", idx["tables"].get((catalog, schema_name, table_name), {}))):
        for key, value in level_tags.items():
            inherited[key] = {"value": value, "level": level}
    return inherited


def effective_tags(catalog: str, schema_name: str, table_name: str, columns: List[str]) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    {column: {tag_key: {"value": v, "level": "column|table|schema|catalog"}}} for the given columns.
    Most specific level wins per tag key.
    """
    catalog, schema_name, table_name = norm(catalog), norm(schema_name), norm(table_name)
    idx = get_index()
    inherited = effective_table_tags(catalog, schema_name, table_name)
    col_tags = idx["columns"].get((catalog, schema_name, table_name), {})
    out: Dict[str, Dict[str, Dict[str, str]]] = {}
    for col in columns:
        merged = dict(inherited)
        for key, value in col_tags.get(norm(col), {}).items():
            merged[key] = {"value": value, "level": "column"}
        out[col] = merged
    return out


def has_any_tags() -> bool:
    """False on installs that do not use tags: masking is impossible and metadata lookups can be skipped entirely."""
    idx = get_index()
    return bool(idx["catalogs"] or idx["schemas"] or idx["tables"] or idx["columns"])


def table_has_any_tags(catalog: str, schema_name: str, table_name: str) -> bool:
    """True when any level at or above the table (or any of its columns) carries a tag."""
    catalog, schema_name, table_name = norm(catalog), norm(schema_name), norm(table_name)
    idx = get_index()
    return bool(idx["catalogs"].get(catalog) or idx["schemas"].get((catalog, schema_name))
                or idx["tables"].get((catalog, schema_name, table_name))
                or idx["columns"].get((catalog, schema_name, table_name)))


# ----------------------------------------------------------------------------
# Lifecycle
# ----------------------------------------------------------------------------

def reconcile(con, actor: str = "system") -> Dict[str, int]:
    """
    Flags assignments whose object no longer exists as orphaned (never deleted: an admin decides) and restores flags
    for objects that came back. Orphaned tags are ignored by effective-tag resolution.
    """
    from web.governance import catalog_meta
    conn = store.get_db()
    flagged = restored = 0
    try:
        rows = conn.execute("SELECT id, catalog, schema_name, table_name, column_name, orphaned FROM object_tags").fetchall()
        col_cache: Dict[Key, Optional[set]] = {}
        catalogs_present = {c.lower() for c in catalog_meta.list_catalogs(con)}
        for r in rows:
            cat, sch, tbl, col = r["catalog"], r["schema_name"], r["table_name"], r["column_name"]
            if cat not in catalogs_present:
                exists = False
            elif not sch:
                exists = True
            elif not tbl:
                exists = catalog_meta.schema_exists(con, cat, sch)
            else:
                cols = col_cache.get((cat, sch, tbl))
                if cols is None:
                    cols = {c["column"].lower() for c in catalog_meta.list_columns(con, cat, sch, tbl)}
                    col_cache[(cat, sch, tbl)] = cols
                exists = bool(cols) and (not col or col in cols)
            if exists and r["orphaned"]:
                conn.execute("UPDATE object_tags SET orphaned = 0 WHERE id = ?", (r["id"],))
                restored += 1
            elif not exists and not r["orphaned"]:
                conn.execute("UPDATE object_tags SET orphaned = 1 WHERE id = ?", (r["id"],))
                flagged += 1
        if flagged or restored:
            store.bump_version(conn)
            store.write_audit(conn, actor, "TAG_RECONCILE", None, {"orphaned": flagged, "restored": restored})
        conn.commit()
    finally:
        conn.close()
    return {"orphaned": flagged, "restored": restored, "checked": len(rows)}


def rename_table(catalog: str, schema_name: str, old_table: str, new_table: str, actor: str = "system") -> int:
    """Re-keys table and column tags after a table rename; returns rows moved."""
    conn = store.get_db()
    try:
        cur = conn.execute(
            "UPDATE OR REPLACE object_tags SET table_name = ? WHERE catalog = ? AND schema_name = ? AND table_name = ?",
            (norm(new_table), norm(catalog), norm(schema_name), norm(old_table)))
        if cur.rowcount:
            store.bump_version(conn)
            store.write_audit(conn, actor, "TAG_RENAME_TABLE", object_label(norm(catalog), norm(schema_name), norm(old_table)),
                              {"to": norm(new_table), "rows": cur.rowcount})
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def drop_object(catalog: str, schema_name: str = "", table_name: str = "", column_name: str = "", actor: str = "system") -> int:
    """Removes tags of a dropped table/column (and everything beneath it); returns rows deleted."""
    catalog, schema_name, table_name, column_name = norm(catalog), norm(schema_name), norm(table_name), norm(column_name)
    clauses, params = ["catalog = ?"], [catalog]
    for col_name, val in (("schema_name", schema_name), ("table_name", table_name), ("column_name", column_name)):
        if val:
            clauses.append(f"{col_name} = ?")
            params.append(val)
    conn = store.get_db()
    try:
        cur = conn.execute(f"DELETE FROM object_tags WHERE {' AND '.join(clauses)}", params)
        if cur.rowcount:
            store.bump_version(conn)
            store.write_audit(conn, actor, "TAG_DROP_OBJECT", object_label(catalog, schema_name, table_name, column_name),
                              {"rows": cur.rowcount})
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()
