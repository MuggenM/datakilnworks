"""
Row-level security: row filter policies, CRUD, validation and per-table resolution.

Like a masking policy, a row filter policy points at a *tag*, not at objects: "restrict everything tagged
region_scoped to the caller's own region". Unlike masking, a row filter is a whole-table restriction, so it binds to a
tag on a catalog/schema/table (never a column), and it needs a column on the target table to filter on
(`filter_column`) plus a way to decide which values that column may hold for the caller:

  owner      filter_column = the caller's username (self-service data: "see only rows you created")
  attribute  filter_column must be one of the values assigned to the caller (their username, or their role as a
             default) for `attribute_key` in the `principal_attributes` table -- the governance-managed equivalent of
             the membership/mapping tables Databricks and Snowflake customers build by hand. No assignment means no
             visible rows (fail closed), never "unrestricted".
  custom     an admin-authored boolean expression, validated the same way a custom mask is: no subqueries, no table
             references, a curated function allowlist. `{col}` is the filter column, `{user}`/`{role}` are the
             caller's identity as string literals.

Multiple applicable policies for one table combine with AND (each is an independent restriction, not alternatives).
A policy whose `filter_column` does not exist on a matched table fails closed (denies all rows) rather than silently
not filtering, so a naming mistake cannot turn into a leak.

Resolution never depends on session state DuckDB would carry (there is none to depend on): predicates are literal
text built once per rewrite, from data the gateway already resolved (the caller's identity and their assigned
attribute values), exactly like how a masking policy is picked before the SQL is built.
"""

import json
import re
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from web.governance import store, tags
from web.governance.masks import quote_ident
from web.governance.policies import NAME_RE, ROLES, Principal, is_exempt

FILTER_MODES = ("owner", "attribute", "custom")
ATTR_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
PRINCIPAL_TYPES = ("user", "role")


class NotFound(tags.NotFound):
    """Unknown row policy id (HTTP 404)."""


@dataclass
class RowFilterSpec:
    """One applicable row filter: the literal predicate to AND into the WHERE clause, and why."""
    predicate: str
    policy_id: str
    policy_name: str
    filter_column: str
    tag_key: str
    tag_value: Optional[str]


def _literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _row(r) -> Dict[str, Any]:
    d = dict(r)
    d["enabled"] = bool(d["enabled"])
    for key in ("except_roles", "except_users"):
        d[key] = json.loads(d[key]) if d.get(key) else []
    return d


# ----------------------------------------------------------------------------
# Custom expression validation (mirrors masks.py's approach, adapted for a boolean row predicate)
# ----------------------------------------------------------------------------

import sqlglot
from sqlglot import exp

_PLACEHOLDER_DUMMIES = {"{col}": "__gov_row_col__", "{user}": "__gov_row_user__", "{role}": "__gov_row_role__"}
_FORBIDDEN_NODES = (exp.Select, exp.Subquery, exp.Window, exp.AggFunc, exp.Star, exp.Placeholder, exp.Parameter,
                    exp.Table, exp.Command, exp.Lambda, exp.Union, exp.With, exp.Query, exp.Insert, exp.Update, exp.Delete)
_ALLOWED_FUNC_CLASSES = {"Left", "Right", "Concat", "ConcatWs", "RegexpLike", "Substring", "Length", "Lower", "Upper",
                         "Coalesce", "Nullif", "Trim", "Case", "If", "Cast", "TryCast", "DateTrunc", "Extract", "Anonymous",
                         "Or", "And"}       # sqlglot models these two logical connectives as Func subclasses
_ALLOWED_ANONYMOUS = {"lower", "upper", "trim", "regexp_matches", "starts_with", "contains", "list_contains", "strpos", "split_part"}


def _ast_errors(template: str) -> List[str]:
    if "{col}" not in template:
        return ["The expression must reference the filtered column with the {col} placeholder."]
    text = template
    for placeholder, dummy in _PLACEHOLDER_DUMMIES.items():
        text = text.replace(placeholder, dummy)
    if "{" in text or "}" in text:
        return ["Only the {col}, {user} and {role} placeholders are allowed between braces."]
    if ";" in text:
        return ["Multiple statements are not allowed."]
    try:
        tree = sqlglot.parse_one(text, dialect="duckdb")
    except Exception as exc:
        return [f"Not a valid SQL boolean expression: {str(exc).splitlines()[0][:160]}"]
    errors: List[str] = []
    known = set(_PLACEHOLDER_DUMMIES.values())
    for node in tree.walk():
        node = node[0] if isinstance(node, tuple) else node
        if isinstance(node, _FORBIDDEN_NODES):
            errors.append(f"{type(node).__name__} is not allowed in a row filter expression.")
        elif isinstance(node, exp.Column) and node.name not in known:
            errors.append(f"Unknown column '{node.name}': use {{col}} for the filtered column, {{user}}/{{role}} for the caller's identity.")
        elif isinstance(node, exp.Func):
            kind = type(node).__name__
            if kind == "Anonymous":
                if node.name.lower() not in _ALLOWED_ANONYMOUS:
                    errors.append(f"Function '{node.name}' is not allowed.")
            elif kind not in _ALLOWED_FUNC_CLASSES:
                errors.append(f"Function '{node.sql_name()}' is not allowed.")
    if not tree.find(exp.Column):
        errors.append("The expression never uses {col}, {user} or {role}, so it would not depend on the row or the caller.")
    return sorted(set(errors))


def validate_filter_expression(template: str) -> Dict[str, Any]:
    """Static AST checks for a custom row filter expression (no live dry run: the filter column's table isn't fixed)."""
    errors = _ast_errors(template or "")
    return {"ok": not errors, "errors": errors}


# ----------------------------------------------------------------------------
# Row policy CRUD
# ----------------------------------------------------------------------------

def _validate(data: Dict[str, Any]) -> Dict[str, Any]:
    name = (data.get("name") or "").strip()
    if not NAME_RE.match(name):
        raise ValueError("Policy names are 1-100 characters (letters, digits, space, '_', '.', '-').")
    tag_key = tags.norm(data.get("tag_key"))
    definition = tags.get_definition(tag_key)
    tag_value = data.get("tag_value")
    tag_value = None if tag_value in (None, "") else str(tag_value).strip()
    if tag_value is not None and definition["allowed_values"] and tag_value not in definition["allowed_values"]:
        raise ValueError(f"'{tag_value}' is not an allowed value for tag '{tag_key}'.")
    filter_column = (data.get("filter_column") or "").strip()
    if not filter_column or len(filter_column) > 128 or '"' in filter_column or "\n" in filter_column:
        raise ValueError("filter_column must be a real column name (1-128 characters, no quotes or newlines).")
    filter_mode = (data.get("filter_mode") or "").strip().lower()
    if filter_mode not in FILTER_MODES:
        raise ValueError(f"filter_mode must be one of: {', '.join(FILTER_MODES)}.")
    attribute_key = None
    filter_expr = None
    if filter_mode == "attribute":
        attribute_key = tags.norm(data.get("attribute_key"))
        if not ATTR_KEY_RE.match(attribute_key or ""):
            raise ValueError("attribute_key is required for attribute-mode policies (lowercase letters, digits, '_', '.', '-').")
    elif filter_mode == "custom":
        filter_expr = (data.get("filter_expr") or "").strip()
        verdict = validate_filter_expression(filter_expr)
        if not verdict["ok"]:
            raise ValueError("Invalid row filter expression: " + " ".join(verdict["errors"]))
    except_roles = data.get("except_roles", ["admin"])
    if not isinstance(except_roles, list) or any(r not in ROLES for r in except_roles):
        raise ValueError(f"except_roles must be a list of: {', '.join(ROLES)}.")
    except_users = data.get("except_users", [])
    if not isinstance(except_users, list) or any(not isinstance(u, str) or not u.strip() for u in except_users):
        raise ValueError("except_users must be a list of usernames.")
    try:
        priority = int(data.get("priority", 100))
    except (TypeError, ValueError):
        raise ValueError("priority must be an integer.")
    if not 0 <= priority <= 10000:
        raise ValueError("priority must be between 0 and 10000.")
    return {"name": name, "description": (data.get("description") or "").strip(), "tag_key": tag_key, "tag_value": tag_value,
            "filter_column": filter_column, "filter_mode": filter_mode, "attribute_key": attribute_key,
            "filter_expr": filter_expr, "except_roles": sorted(set(except_roles)),
            "except_users": sorted({u.strip() for u in except_users}), "priority": priority,
            "enabled": bool(data.get("enabled", True))}


def create_row_policy(data: Dict[str, Any], actor: str = "admin") -> Dict[str, Any]:
    store.init_governance_db()
    rec = _validate(data)
    pid = f"rlf_{uuid.uuid4().hex[:8]}"
    now = store.utcnow()
    conn = store.get_db()
    try:
        if conn.execute("SELECT 1 FROM row_policies WHERE name = ?", (rec["name"],)).fetchone():
            raise ValueError(f"A row policy named '{rec['name']}' already exists.")
        conn.execute("""
            INSERT INTO row_policies (id, name, description, tag_key, tag_value, filter_column, filter_mode,
                                      attribute_key, filter_expr, except_roles, except_users, priority, enabled,
                                      created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (pid, rec["name"], rec["description"], rec["tag_key"], rec["tag_value"], rec["filter_column"], rec["filter_mode"],
              rec["attribute_key"], rec["filter_expr"], json.dumps(rec["except_roles"]), json.dumps(rec["except_users"]),
              rec["priority"], int(rec["enabled"]), actor, now, now))
        store.bump_version(conn)
        store.write_audit(conn, actor, "ROW_POLICY_CREATE", pid, rec)
        conn.commit()
    finally:
        conn.close()
    return get_row_policy(pid)


def get_row_policy(policy_id: str) -> Dict[str, Any]:
    conn = store.get_db()
    try:
        row = conn.execute("SELECT * FROM row_policies WHERE id = ?", (policy_id,)).fetchone()
        if not row:
            raise NotFound(f"Row policy '{policy_id}' does not exist.")
        return _row(row)
    finally:
        conn.close()


def list_row_policies(enabled_only: bool = False) -> List[Dict[str, Any]]:
    store.init_governance_db()
    conn = store.get_db()
    try:
        where = "WHERE enabled = 1" if enabled_only else ""
        return [_row(r) for r in conn.execute(f"SELECT * FROM row_policies {where} ORDER BY priority, name").fetchall()]
    finally:
        conn.close()


def update_row_policy(policy_id: str, changes: Dict[str, Any], actor: str = "admin") -> Dict[str, Any]:
    current = get_row_policy(policy_id)
    merged = {**current, **{k: v for k, v in changes.items() if k in current and k not in ("id", "created_by", "created_at", "updated_at")}}
    rec = _validate(merged)
    conn = store.get_db()
    try:
        clash = conn.execute("SELECT id FROM row_policies WHERE name = ? AND id != ?", (rec["name"], policy_id)).fetchone()
        if clash:
            raise ValueError(f"A row policy named '{rec['name']}' already exists.")
        conn.execute("""
            UPDATE row_policies SET name=?, description=?, tag_key=?, tag_value=?, filter_column=?, filter_mode=?,
                   attribute_key=?, filter_expr=?, except_roles=?, except_users=?, priority=?, enabled=?, updated_at=? WHERE id=?
        """, (rec["name"], rec["description"], rec["tag_key"], rec["tag_value"], rec["filter_column"], rec["filter_mode"],
              rec["attribute_key"], rec["filter_expr"], json.dumps(rec["except_roles"]), json.dumps(rec["except_users"]),
              rec["priority"], int(rec["enabled"]), store.utcnow(), policy_id))
        store.bump_version(conn)
        store.write_audit(conn, actor, "ROW_POLICY_UPDATE", policy_id, {"before": {k: current[k] for k in rec if k in current}, "after": rec})
        conn.commit()
    finally:
        conn.close()
    return get_row_policy(policy_id)


def delete_row_policy(policy_id: str, actor: str = "admin") -> None:
    current = get_row_policy(policy_id)
    conn = store.get_db()
    try:
        conn.execute("DELETE FROM row_policies WHERE id = ?", (policy_id,))
        store.bump_version(conn)
        store.write_audit(conn, actor, "ROW_POLICY_DELETE", policy_id, {"name": current["name"]})
        conn.commit()
    finally:
        conn.close()


_lock = threading.Lock()
_cache: Dict[str, Any] = {"version": -1, "policies": []}


def enabled_row_policies() -> List[Dict[str, Any]]:
    version = store.get_version()
    with _lock:
        if _cache["version"] != version:
            _cache["policies"] = list_row_policies(enabled_only=True)
            _cache["version"] = version
        return _cache["policies"]


# ----------------------------------------------------------------------------
# Principal attributes (the built-in membership/mapping table for 'attribute' mode)
# ----------------------------------------------------------------------------

def set_attribute_values(principal_type: str, principal_value: str, attribute_key: str, values: List[str],
                         actor: str = "admin") -> Dict[str, Any]:
    """Replaces the full set of values for (principal_type, principal_value, attribute_key)."""
    if principal_type not in PRINCIPAL_TYPES:
        raise ValueError(f"principal_type must be one of: {', '.join(PRINCIPAL_TYPES)}.")
    principal_value = (principal_value or "").strip()
    if principal_type == "role":
        principal_value = principal_value.lower()
        if principal_value not in ROLES:
            raise ValueError(f"principal_value must be one of: {', '.join(ROLES)} when principal_type is 'role'.")
    elif not principal_value:
        raise ValueError("principal_value (a username) is required.")
    key = tags.norm(attribute_key)
    if not ATTR_KEY_RE.match(key):
        raise ValueError("attribute_key must be lowercase letters, digits, '_', '.', '-' (1-64 characters).")
    clean_values = sorted({v.strip() for v in (values or []) if v and v.strip()})
    if any(len(v) > 128 for v in clean_values):
        raise ValueError("Attribute values are limited to 128 characters.")
    store.init_governance_db()
    conn = store.get_db()
    try:
        conn.execute("DELETE FROM principal_attributes WHERE principal_type = ? AND principal_value = ? AND attribute_key = ?",
                     (principal_type, principal_value, key))
        now = store.utcnow()
        for v in clean_values:
            conn.execute(
                "INSERT INTO principal_attributes (principal_type, principal_value, attribute_key, attribute_value, "
                "created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)", (principal_type, principal_value, key, v, actor, now))
        store.bump_version(conn)
        store.write_audit(conn, actor, "ATTRIBUTE_SET", f"{principal_type}:{principal_value}",
                          {"attribute_key": key, "values": clean_values})
        conn.commit()
    finally:
        conn.close()
    return {"principal_type": principal_type, "principal_value": principal_value, "attribute_key": key, "values": clean_values}


def delete_attribute(principal_type: str, principal_value: str, attribute_key: str, actor: str = "admin") -> bool:
    conn = store.get_db()
    try:
        cur = conn.execute(
            "DELETE FROM principal_attributes WHERE principal_type = ? AND principal_value = ? AND attribute_key = ?",
            (principal_type, principal_value, tags.norm(attribute_key)))
        if cur.rowcount:
            store.bump_version(conn)
            store.write_audit(conn, actor, "ATTRIBUTE_DELETE", f"{principal_type}:{principal_value}", {"attribute_key": attribute_key})
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def list_attributes(principal_type: Optional[str] = None, principal_value: Optional[str] = None,
                    attribute_key: Optional[str] = None) -> List[Dict[str, Any]]:
    store.init_governance_db()
    clauses, params = [], []
    if principal_type:
        clauses.append("principal_type = ?")
        params.append(principal_type)
    if principal_value:
        clauses.append("principal_value = ?")
        params.append(principal_value)
    if attribute_key:
        clauses.append("attribute_key = ?")
        params.append(tags.norm(attribute_key))
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    conn = store.get_db()
    try:
        rows = conn.execute(
            f"SELECT * FROM principal_attributes {where} "
            "ORDER BY principal_type, principal_value, attribute_key, attribute_value").fetchall()
    finally:
        conn.close()
    grouped: Dict[tuple, List[str]] = {}
    for r in rows:
        grouped.setdefault((r["principal_type"], r["principal_value"], r["attribute_key"]), []).append(r["attribute_value"])
    return [{"principal_type": k[0], "principal_value": k[1], "attribute_key": k[2], "values": v}
            for k, v in sorted(grouped.items())]


_attr_lock = threading.Lock()
_attr_cache: Dict[str, Any] = {"version": -1, "by_key": {}}


def _attr_index() -> Dict[tuple, List[str]]:
    version = store.get_version()
    with _attr_lock:
        if _attr_cache["version"] != version:
            conn = store.get_db()
            try:
                rows = conn.execute("SELECT principal_type, principal_value, attribute_key, attribute_value FROM principal_attributes").fetchall()
            finally:
                conn.close()
            idx: Dict[tuple, List[str]] = {}
            for r in rows:
                idx.setdefault((r["principal_type"], r["principal_value"], r["attribute_key"]), []).append(r["attribute_value"])
            _attr_cache["by_key"] = idx
            _attr_cache["version"] = version
        return _attr_cache["by_key"]


def resolve_attribute_values(principal: Principal, attribute_key: str) -> List[str]:
    """Union of the values assigned directly to this user and to their role, for one attribute key."""
    idx = _attr_index()
    key = tags.norm(attribute_key)
    out = set(idx.get(("user", principal.username, key), []))
    out |= set(idx.get(("role", principal.role, key), []))
    return sorted(out)


# ----------------------------------------------------------------------------
# Resolution
# ----------------------------------------------------------------------------

def _predicate(pol: Dict[str, Any], principal: Principal) -> str:
    col = quote_ident(pol["filter_column"])
    if pol["filter_mode"] == "owner":
        return f"{col} = {_literal(principal.username)}"
    if pol["filter_mode"] == "attribute":
        values = resolve_attribute_values(principal, pol["attribute_key"])
        if not values:
            return "1 = 0"                                   # fail closed: nothing assigned, nothing visible
        return f"{col} IN ({', '.join(_literal(v) for v in values)})"
    text = pol["filter_expr"].replace("{col}", col)
    return text.replace("{user}", _literal(principal.username)).replace("{role}", _literal(principal.role))


def filters_for_table(catalog: str, schema_name: str, table_name: str, columns: List[Dict[str, Any]],
                      principal: Principal) -> List[RowFilterSpec]:
    """Row filters that apply to `principal` for this table ([{column, type}, ...]); empty when nothing is filtered."""
    if not enabled_row_policies():
        return []
    eff = tags.effective_table_tags(catalog, schema_name, table_name)
    col_names = {c["column"].lower() for c in columns}
    out: List[RowFilterSpec] = []
    for pol in enabled_row_policies():
        tagged = eff.get(pol["tag_key"])
        if tagged is None:
            continue
        if pol["tag_value"] is not None and tagged["value"] != pol["tag_value"]:
            continue
        if is_exempt(pol, principal):
            continue
        if pol["filter_column"].lower() not in col_names:
            predicate = "1 = 0"                              # fail closed: the policy's column is missing here
        else:
            predicate = _predicate(pol, principal)
        out.append(RowFilterSpec(predicate=predicate, policy_id=pol["id"], policy_name=pol["name"],
                                 filter_column=pol["filter_column"], tag_key=pol["tag_key"], tag_value=pol["tag_value"]))
    return out
