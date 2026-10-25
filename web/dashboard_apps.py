"""
Dashboard apps: a dashboard with pages, typed parameters and text blocks (the Apps catalogue is "dashboards with pages").

An app contains NO author code. It is data: parameter declarations, pages, widgets (each a SQL query plus a chart type) and
text blocks. The DKW frontend renders it with the same fixed widget code every dashboard uses, so there is nothing for an
author to inject. proposals/declarative-apps.md has the reasoning.

What lives here:
  - spec validation (`validate_spec`): parameters, pages, widget references, SQL parsed with sqlglot (one read-only query).
  - typed parameter binding (`resolve_values` + `bind_sql`). A dashboard that DECLARES parameters never goes through the
    legacy `:name` regex in dashboards.resolve_query_parameters: every value is validated for its declared type (number
    parsed, date parsed, select checked against its options, text length-capped) and rendered as a literal; placeholders
    are found by a scanner that skips string literals, quoted identifiers and comments, so a `:word` inside a value or a
    string is never rewritten. A dashboard without declared parameters keeps the legacy behaviour unchanged.
  - the permission rules for dashboards (`level_for`, `require_level`): the dashboard list/read/query/export/widget endpoints
    in app.py had no permission check before apps made dashboards shareable.
  - sample apps and cloning (`SAMPLES`, `/api/dashboard-apps/*`).
"""

import copy
import datetime
import logging
import math
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from web.auth import get_current_user, resolve_principal

logger = logging.getLogger("localspark.dashboard_apps")
router = APIRouter(tags=["dashboard-apps"])

PARAM_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
PAGE_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
IDENT_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
WIDTH_RE = re.compile(r"^col-span-[1-4]$")
PARAM_TYPES = ("text", "number", "date", "select")
WIDGET_TYPES = (
    "kpi", "big_number_trendline", "bar", "line", "area", "pie", "scatter", "bubble", "mixed", "waterfall", "funnel", "gauge",
    "heatmap", "treemap", "sunburst", "boxplot", "radar", "gantt", "graph", "tree", "table", "pivot_table", "world_map", "text",
)
MAX_PARAMS = 20
MAX_PAGES = 12
MAX_WIDGETS = 80
MAX_OPTIONS = 200
MAX_TEXT_VALUE = 500
MAX_MARKDOWN = 20000
OPTIONS_TTL = 60


class SpecError(ValueError):
    """An invalid app definition; the message is safe to show to the author."""


class ParamError(ValueError):
    """An invalid parameter value supplied by a viewer; the message is safe to show."""


# --------------------------------------------------------------------------------------------------------------------
# Permissions
# --------------------------------------------------------------------------------------------------------------------

_LEVELS = {"none": 0, "viewer": 2, "editor": 3, "owner": 4}


def level_for(dashboard_id: str, user: Dict[str, Any]) -> str:
    """none | viewer | editor | owner. A dashboard with no permission record predates dashboard permissions (the seeded and
    older ones): any signed-in user may read it, only an admin changes it. Every dashboard created now has a record."""
    from web.dashboard_permissions import get_dashboard_permissions, get_user_permission_level
    username, role = user.get("username", ""), user.get("role", "user")
    if get_dashboard_permissions(dashboard_id) is None:
        return "owner" if role == "admin" else "viewer"
    return get_user_permission_level(dashboard_id, username, role)


def require_level(dashboard_id: str, user: Dict[str, Any], needed: str = "viewer") -> str:
    level = level_for(dashboard_id, user)
    if _LEVELS.get(level, 0) < _LEVELS[needed]:
        # A dashboard the caller cannot see does not exist as far as they are concerned.
        if _LEVELS.get(level, 0) < _LEVELS["viewer"]:
            raise HTTPException(status_code=404, detail="Dashboard not found")
        raise HTTPException(status_code=403, detail="You don't have permission to change this dashboard")
    return level


# --------------------------------------------------------------------------------------------------------------------
# Placeholder scanner and typed binding
# --------------------------------------------------------------------------------------------------------------------

_MUSTACHE_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def scan_placeholders(sql: str) -> List[Tuple[int, int, str]]:
    """(start, end, name) of every `:name` / `{{name}}` outside string literals, quoted identifiers and comments. A `::` cast, a
    slice such as `x[1:n]` and a struct key `{'a': b}` are not placeholders."""
    out: List[Tuple[int, int, str]] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c in ("'", '"'):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:
                        j += 2
                        continue
                    break
                j += 1
            i = j + 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j + 1
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
        elif sql.startswith("{{", i):
            m = _MUSTACHE_RE.match(sql, i)
            if m:
                out.append((m.start(), m.end(), m.group(1)))
                i = m.end()
            else:
                i += 2
        elif c == ":":
            prev = sql[i - 1] if i else " "
            nxt = sql[i + 1] if i + 1 < n else ""
            m = _NAME_RE.match(sql, i + 1) if (nxt.isalpha() or nxt == "_") else None
            if m and prev != ":" and not (prev.isalnum() or prev in "_])\"'"):
                out.append((i, m.end(), m.group(0)))
                i = m.end()
            else:
                i += 1
        else:
            i += 1
    return out


def bind_sql(sql: str, literals: Dict[str, str]) -> str:
    """Replace each scanned placeholder whose name is in `literals` with its already-validated literal."""
    parts, last = [], 0
    for start, end, name in scan_placeholders(sql):
        if name in literals:
            parts.append(sql[last:start])
            parts.append(literals[name])
            last = end
    parts.append(sql[last:])
    return "".join(parts)


def _is_all(v: Any) -> bool:
    return v is None or (isinstance(v, str) and (v.strip() == "" or v.strip().upper() == "ALL"))


def _quote_str(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def _quote_ident(value: str) -> str:
    parts = value.split(".")
    if not 1 <= len(parts) <= 3 or not all(IDENT_PART_RE.match(p) for p in parts):
        raise ParamError(f"'{value}' is not a valid table name.")
    return ".".join('"' + p + '"' for p in parts)


def _render(param: Dict[str, Any], raw: Any, options: Optional[List[str]]) -> Tuple[str, Optional[str]]:
    """(literal, text) for one value of one parameter. `text` is the plain string form, kept for identifier parameters."""
    label = param.get("label") or param["id"]
    ptype = param["type"]
    if ptype == "text":
        s = str(raw)
        if len(s) > MAX_TEXT_VALUE or "\x00" in s:
            raise ParamError(f"'{label}' is too long or contains invalid characters.")
        return _quote_str(s), None
    if ptype == "number":
        try:
            if isinstance(raw, bool):
                raise ValueError
            f = float(str(raw).strip())
        except (TypeError, ValueError):
            raise ParamError(f"'{label}' must be a number.")
        if not math.isfinite(f):
            raise ParamError(f"'{label}' must be a finite number.")
        return (str(int(f)) if f == int(f) and abs(f) < 1e15 else repr(f)), None
    if ptype == "date":
        try:
            d = datetime.date.fromisoformat(str(raw).strip())
        except ValueError:
            raise ParamError(f"'{label}' must be a date (YYYY-MM-DD).")
        return f"DATE '{d.isoformat()}'", None
    if ptype == "select":
        s = str(raw)
        if options is not None and s not in options:
            raise ParamError(f"'{s[:60]}' is not one of the choices for '{label}'.")
        if param.get("as_identifier"):
            return _quote_ident(s), s
        return _quote_str(s), None
    raise ParamError(f"Unsupported parameter type '{ptype}'.")


# --------------------------------------------------------------------------------------------------------------------
# Option lists for select parameters
# --------------------------------------------------------------------------------------------------------------------

def _options_from_query(conn, param: Dict[str, Any], principal) -> List[str]:
    """Run a select parameter's options query as the viewer (governed, ACL-checked) and return its first column as strings."""
    from web.dashboards import cache_result, get_cache_key, get_cached_result
    from web.governance import gateway
    q = (param.get("options_query") or "").strip().rstrip(";")
    if isinstance(principal, dict) and principal.get("role"):
        from web.permissions import enforce_sql_permissions
        enforce_sql_permissions(q, principal, action="READ")
    gov = gateway.govern_sql(q, principal, client="dashboard-param")
    if gov.blocked:
        raise ParamError(f"The choices for '{param.get('label') or param['id']}' are not available to you.")
    key = get_cache_key("options:" + gov.sql, {}, gateway.fingerprint(gov))
    cached = get_cached_result(key)
    if cached is not None:
        return cached["options"]
    inner = gov.sql.strip().rstrip(";")
    df = conn.sql(f"SELECT * FROM ({inner}) AS o LIMIT {MAX_OPTIONS}").df()
    values = [str(v) for v in df[df.columns[0]].tolist() if v is not None]
    cache_result(key, {"options": values}, ttl=OPTIONS_TTL)
    return values


def options_for(conn, param: Dict[str, Any], principal) -> Optional[List[str]]:
    """The allowed values of a select parameter, or None when the parameter has no list to check against."""
    if param["type"] != "select":
        return None
    if param.get("options_query"):
        values = _options_from_query(conn, param, principal)
    else:
        values = list(param.get("options") or [])
    if param.get("as_identifier"):
        values = [v for v in values if _ident_ok(v) and _catalog_visible(v, principal)]
    return values


def _ident_ok(v: str) -> bool:
    parts = v.split(".")
    return 1 <= len(parts) <= 3 and all(IDENT_PART_RE.match(p) for p in parts)


def _catalog_visible(value: str, principal) -> bool:
    """A table name from a metadata listing is only offered when its catalog is one the viewer may read."""
    parts = value.split(".")
    if len(parts) != 3 or not isinstance(principal, dict) or principal.get("role") == "admin":
        return True
    try:
        from web.permissions import can_user_access_catalog
        return bool(can_user_access_catalog(principal, parts[0], action="READ"))
    except Exception:
        return False


def resolve_values(conn, declared: List[Dict[str, Any]], supplied: Optional[Dict[str, Any]], principal) -> Dict[str, str]:
    """name -> SQL literal for every declared parameter (plus `<id>_text` for identifier parameters). Raises ParamError."""
    supplied = supplied or {}
    literals: Dict[str, str] = {}
    for p in declared:
        pid = p["id"]
        raw = supplied.get(pid)
        options: Optional[List[str]] = None
        if p["type"] == "select":
            options = options_for(conn, p, principal)
        if _is_all(raw):
            raw = p.get("default")
        if _is_all(raw) and p.get("required") and p["type"] == "select" and options:
            raw = options[0]
        if _is_all(raw):
            if p.get("required"):
                raise ParamError(f"'{p.get('label') or pid}' is required.")
            literals[pid] = "NULL"
            if p.get("as_identifier"):
                literals[pid + "_text"] = "NULL"
            continue
        lit, text = _render(p, raw, options)
        literals[pid] = lit
        if text is not None:
            literals[pid + "_text"] = _quote_str(text)
    return literals


def parameter_options(conn, declared: List[Dict[str, Any]], principal) -> Dict[str, List[str]]:
    """Dropdown contents for the parameter bar: `ALL` first unless the parameter is required."""
    out: Dict[str, List[str]] = {}
    for p in declared:
        if p["type"] != "select":
            continue
        try:
            vals = options_for(conn, p, principal) or []
        except Exception as e:
            logger.warning("options for %s failed: %s", p["id"], e)
            vals = []
        out[p["id"]] = vals if p.get("required") else ["ALL"] + vals
    return out


# --------------------------------------------------------------------------------------------------------------------
# Running a widget
# --------------------------------------------------------------------------------------------------------------------

def _failed(msg: str, query: str = "") -> Dict[str, Any]:
    return {"success": False, "query_id": None, "query_executed": query, "columns": [], "rows": [], "row_count": 0,
            "elapsed_ms": 0, "error": msg, "from_cache": False, "masked_columns": []}


def hydrate_widget(conn, widget: Dict[str, Any], params: Optional[Dict[str, Any]], principal,
                   declared: Optional[List[Dict[str, Any]]] = None,
                   literals: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Result of one widget for `principal`. A dashboard that declares parameters is bound strictly (typed literals); one that
    does not keeps the legacy substitution. `literals` lets a caller bind the parameters once for a whole page of widgets."""
    from web.dashboards import execute_widget_query
    if widget.get("type") == "text":
        return execute_widget_query(conn, "", None, principal)
    query = widget.get("query", "")
    if declared:
        try:
            lits = literals if literals is not None else resolve_values(conn, declared, params, principal)
        except ParamError as e:
            return _failed(str(e), query)
        except HTTPException as e:
            return _failed(str(e.detail), query)
        except Exception as e:
            logger.warning("parameter resolution failed: %s", e)
            return _failed("The parameters could not be resolved.", query)
        return execute_widget_query(conn, bind_sql(query, lits), None, principal, bound=True)
    return execute_widget_query(conn, query, params or {}, principal)


def hydrate_dashboard(conn, dashboard: Dict[str, Any], params: Optional[Dict[str, Any]], principal) -> List[Dict[str, Any]]:
    declared = dashboard.get("parameters") or []
    literals: Optional[Dict[str, str]] = None
    err: Optional[str] = None
    if declared:
        try:
            literals = resolve_values(conn, declared, params, principal)
        except ParamError as e:
            err = str(e)
        except HTTPException as e:
            err = str(e.detail)
        except Exception as e:
            logger.warning("parameter resolution failed: %s", e)
            err = "The parameters could not be resolved."
    out = []
    for w in dashboard.get("widgets", []):
        wc = dict(w)
        wc["result"] = _failed(err, w.get("query", "")) if (err and w.get("type") != "text") else \
            hydrate_widget(conn, w, params, principal, declared or None, literals)
        out.append(wc)
    return out


# --------------------------------------------------------------------------------------------------------------------
# Spec validation
# --------------------------------------------------------------------------------------------------------------------

def _clean_str(v: Any, field: str, max_len: int, required: bool = False) -> str:
    s = "" if v is None else str(v).strip()
    if required and not s:
        raise SpecError(f"{field} is required.")
    if len(s) > max_len:
        raise SpecError(f"{field} is longer than {max_len} characters.")
    if "\x00" in s:
        raise SpecError(f"{field} contains invalid characters.")
    return s


def _check_query(sql: str, field: str, known: Dict[str, str]) -> None:
    """One read-only query; every placeholder refers to a declared parameter."""
    import sqlglot
    from sqlglot import exp
    if not sql.strip():
        raise SpecError(f"{field}: the query is empty.")
    for _, _, name in scan_placeholders(sql):
        if name not in known:
            raise SpecError(f"{field}: ':{name}' is not a declared parameter.")
    probe = bind_sql(sql, known)
    try:
        stmts = [s for s in sqlglot.parse(probe, read="duckdb") if s is not None]
    except Exception as e:
        raise SpecError(f"{field}: the query does not parse ({str(e).splitlines()[0][:120]}).")
    if len(stmts) != 1:
        raise SpecError(f"{field}: exactly one statement is allowed.")
    if not isinstance(stmts[0], exp.Query):
        raise SpecError(f"{field}: only a SELECT query is allowed.")


def clean_parameters(raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if not isinstance(raw, list) or len(raw) > MAX_PARAMS:
        raise SpecError(f"parameters must be a list of at most {MAX_PARAMS}.")
    out, seen = [], set()
    for i, p in enumerate(raw):
        if not isinstance(p, dict):
            raise SpecError("Each parameter must be an object.")
        pid = _clean_str(p.get("id"), f"Parameter {i + 1} id", 32, required=True)
        if not PARAM_ID_RE.match(pid):
            raise SpecError(f"Parameter id '{pid}' must be lower-case letters, digits and underscores, starting with a letter.")
        if pid in seen or (pid.endswith("_text") and pid[:-5] in seen):
            raise SpecError(f"Parameter id '{pid}' is used twice.")
        seen.add(pid)
        ptype = p.get("type", "text")
        if ptype not in PARAM_TYPES:
            raise SpecError(f"Parameter '{pid}': type must be one of {', '.join(PARAM_TYPES)}.")
        c: Dict[str, Any] = {"id": pid, "type": ptype, "label": _clean_str(p.get("label") or pid, f"Parameter '{pid}' label", 60),
                             "required": bool(p.get("required", False))}
        default = p.get("default")
        if default is not None and not isinstance(default, (str, int, float)):
            raise SpecError(f"Parameter '{pid}': default must be text or a number.")
        c["default"] = None if default is None or default == "" else (default if not isinstance(default, str) else _clean_str(default, f"Parameter '{pid}' default", MAX_TEXT_VALUE))
        if ptype == "select":
            opts = p.get("options")
            if opts is not None:
                if not isinstance(opts, list) or len(opts) > MAX_OPTIONS:
                    raise SpecError(f"Parameter '{pid}': options must be a list of at most {MAX_OPTIONS}.")
                c["options"] = [_clean_str(o, f"Parameter '{pid}' option", 200, required=True) for o in opts]
            oq = _clean_str(p.get("options_query"), f"Parameter '{pid}' options_query", 4000)
            if oq:
                _check_query(oq, f"Parameter '{pid}' options_query", {})
                c["options_query"] = oq
            if not c.get("options") and not oq:
                raise SpecError(f"Parameter '{pid}': a select needs options or an options_query.")
            c["as_identifier"] = bool(p.get("as_identifier", False))
            if c["as_identifier"]:
                bad = [o for o in c.get("options", []) if not _ident_ok(o)]
                if bad:
                    raise SpecError(f"Parameter '{pid}': '{bad[0]}' is not a valid table name.")
        # A default must itself be a valid value for its type (a select default is checked against its options at run time).
        if c["default"] is not None and ptype in ("number", "date"):
            try:
                _render(c, c["default"], None)
            except ParamError as e:
                raise SpecError(f"Parameter '{pid}': default: {e}")
        if c["default"] is not None and ptype == "select" and c.get("options") and str(c["default"]) not in c["options"]:
            raise SpecError(f"Parameter '{pid}': the default is not one of the options.")
        out.append(c)
    return out


def known_names(parameters: List[Dict[str, Any]]) -> Dict[str, str]:
    """name -> a harmless literal to parse the query with (an identifier parameter stands for a table, so it probes as one)."""
    names: Dict[str, str] = {}
    for p in parameters:
        names[p["id"]] = '"t"' if p.get("as_identifier") else "NULL"
        if p.get("as_identifier"):
            names[p["id"] + "_text"] = "'x'"
    return names


def clean_pages(raw: Any) -> List[Dict[str, str]]:
    if raw is None:
        return []
    if not isinstance(raw, list) or len(raw) > MAX_PAGES:
        raise SpecError(f"pages must be a list of at most {MAX_PAGES}.")
    out, seen = [], set()
    for i, pg in enumerate(raw):
        if not isinstance(pg, dict):
            raise SpecError("Each page must be an object.")
        pid = _clean_str(pg.get("id"), f"Page {i + 1} id", 32, required=True)
        if not PAGE_ID_RE.match(pid):
            raise SpecError(f"Page id '{pid}' must be lower-case letters, digits, '-' and '_'.")
        if pid in seen:
            raise SpecError(f"Page id '{pid}' is used twice.")
        seen.add(pid)
        out.append({"id": pid, "title": _clean_str(pg.get("title") or pid, f"Page '{pid}' title", 60)})
    return out


def clean_widget(w: Dict[str, Any], parameters: List[Dict[str, Any]], page_ids: set, check_sql: bool = True) -> Dict[str, Any]:
    """Validated copy of a widget definition (the fields the renderer reads; unknown fields are dropped)."""
    wtype = w.get("type")
    if wtype not in WIDGET_TYPES:
        raise SpecError(f"Widget type '{wtype}' is not supported.")
    title = _clean_str(w.get("title"), "Widget title", 80) or "Untitled"
    c: Dict[str, Any] = {k: v for k, v in w.items() if k != "result"}
    c.update({"id": w.get("id") or f"w_{uuid.uuid4().hex[:8]}", "title": title, "type": wtype})
    if not re.match(r"^[A-Za-z0-9_\-]{1,40}$", str(c["id"])):
        raise SpecError("Invalid widget id.")
    width = w.get("width") or ("col-span-4" if wtype in ("table", "pivot_table", "world_map", "text") else "col-span-2")
    if not WIDTH_RE.match(str(width)):
        raise SpecError("Widget width must be col-span-1 .. col-span-4.")
    c["width"] = width
    page = w.get("page")
    if page:
        if page_ids and page not in page_ids:
            raise SpecError(f"Widget '{title}' is on an unknown page '{page}'.")
        c["page"] = page
    c.pop("page", None) if not page else None
    if wtype == "text":
        c["query"] = ""
        c["markdown"] = _clean_str(w.get("markdown"), "Text", MAX_MARKDOWN)
        return c
    query = _clean_str(w.get("query"), f"Widget '{title}' query", 20000, required=True)
    if check_sql and parameters:
        _check_query(query, f"Widget '{title}'", known_names(parameters))
    c["query"] = query
    for k in ("x_col", "y_col", "z_col"):
        c[k] = _clean_str(w.get(k), f"Widget '{title}' {k}", 128) or None
    c["unit"] = _clean_str(w.get("unit"), f"Widget '{title}' unit", 16)
    fd = _clean_str(w.get("filter_dimension"), f"Widget '{title}' click parameter", 64)
    if fd and parameters and fd not in {p["id"] for p in parameters}:
        raise SpecError(f"Widget '{title}': clicking it sets '{fd}', which is not a declared parameter.")
    if fd:
        c["filter_dimension"] = fd
    else:
        c.pop("filter_dimension", None)
    return c


def validate_spec(parameters: Any, pages: Any, widgets: List[Dict[str, Any]]) -> Dict[str, Any]:
    params = clean_parameters(parameters)
    pgs = clean_pages(pages)
    if len(widgets) > MAX_WIDGETS:
        raise SpecError(f"An app can have at most {MAX_WIDGETS} widgets.")
    page_ids = {p["id"] for p in pgs}
    cleaned = [clean_widget(w, params, page_ids) for w in widgets]
    if len({w["id"] for w in cleaned}) != len(cleaned):
        raise SpecError("Widget ids must be unique.")
    return {"parameters": params, "pages": pgs, "widgets": cleaned}


# --------------------------------------------------------------------------------------------------------------------
# Sample apps. Self-contained (synthetic data from range()), so they run on any warehouse; point the queries at your own
# tables after cloning.
# --------------------------------------------------------------------------------------------------------------------

_ORDERS_CTE = (
    "WITH orders AS (SELECT i AS order_id, (['North','South','East','West'])[1 + (i % 4)] AS region, "
    "(['Hardware','Software','Services'])[1 + (i % 3)] AS category, "
    "DATE '2026-01-01' + CAST((i * 7) % 270 AS INTEGER) AS order_date, "
    "ROUND(20 + ((i * 37) % 480), 2) AS amount FROM range(1, 2001) AS t(i)), "
    "f AS (SELECT * FROM orders WHERE (:region IS NULL OR region = :region) AND (:category IS NULL OR category = :category) "
    "AND (:from_date IS NULL OR order_date >= :from_date) AND (:to_date IS NULL OR order_date <= :to_date)) "
)
_CUSTOMERS_CTE = (
    "WITH customers AS (SELECT i AS customer_id, (['Enterprise','Mid-market','SMB','Consumer'])[1 + (i % 4)] AS segment, "
    "CAST(100 + ((i * 53) % 9900) AS INTEGER) AS annual_spend, CAST(i % 9 AS INTEGER) AS tenure_years "
    "FROM range(1, 1501) AS t(i)), "
    "f AS (SELECT * FROM customers WHERE (:segment IS NULL OR segment = :segment) AND (:min_spend IS NULL OR annual_spend >= :min_spend)) "
)

SAMPLES: List[Dict[str, Any]] = [
    {
        "id": "sample_sales_explorer",
        "title": "Sales explorer",
        "description": "Two pages with region / category / date filters. Click a bar or slice to filter the whole app; export any table.",
        "tags": ["filters", "cross-filtering", "export"],
        "parameters": [
            {"id": "region", "type": "select", "label": "Region", "options": ["North", "South", "East", "West"], "default": None},
            {"id": "category", "type": "select", "label": "Category", "options": ["Hardware", "Software", "Services"], "default": None},
            {"id": "from_date", "type": "date", "label": "From", "default": "2026-01-01"},
            {"id": "to_date", "type": "date", "label": "To", "default": "2026-12-31"},
        ],
        "pages": [{"id": "overview", "title": "Overview"}, {"id": "details", "title": "Order details"}],
        "widgets": [
            {"page": "overview", "type": "text", "title": "About this app", "width": "col-span-4",
             "markdown": "**Sales explorer** is a sample app on synthetic data.\n\n- Change the filters above, or click a bar / slice to filter by it (click it again to clear).\n- Edit a widget's query to point it at your own tables, keeping the `:region`-style parameters in the WHERE clause."},
            {"page": "overview", "type": "kpi", "title": "Revenue", "width": "col-span-1", "unit": "$",
             "query": _ORDERS_CTE + "SELECT ROUND(COALESCE(SUM(amount), 0), 0) AS revenue FROM f"},
            {"page": "overview", "type": "kpi", "title": "Orders", "width": "col-span-1",
             "query": _ORDERS_CTE + "SELECT COUNT(*) AS orders FROM f"},
            {"page": "overview", "type": "kpi", "title": "Average order", "width": "col-span-1", "unit": "$",
             "query": _ORDERS_CTE + "SELECT ROUND(COALESCE(AVG(amount), 0), 2) AS avg_order FROM f"},
            {"page": "overview", "type": "kpi", "title": "Largest order", "width": "col-span-1", "unit": "$",
             "query": _ORDERS_CTE + "SELECT COALESCE(MAX(amount), 0) AS largest FROM f"},
            {"page": "overview", "type": "line", "title": "Revenue by month", "width": "col-span-2",
             "query": _ORDERS_CTE + "SELECT strftime(order_date, '%Y-%m') AS month, ROUND(SUM(amount), 0) AS revenue FROM f GROUP BY 1 ORDER BY 1",
             "x_col": "month", "y_col": "revenue"},
            {"page": "overview", "type": "bar", "title": "Revenue by region (click to filter)", "width": "col-span-1",
             "query": _ORDERS_CTE + "SELECT region, ROUND(SUM(amount), 0) AS revenue FROM f GROUP BY 1 ORDER BY 2 DESC",
             "x_col": "region", "y_col": "revenue", "filter_dimension": "region"},
            {"page": "overview", "type": "pie", "title": "Share by category (click to filter)", "width": "col-span-1",
             "query": _ORDERS_CTE + "SELECT category, ROUND(SUM(amount), 0) AS revenue FROM f GROUP BY 1 ORDER BY 2 DESC",
             "x_col": "category", "y_col": "revenue", "filter_dimension": "category"},
            {"page": "details", "type": "pivot_table", "title": "Revenue: region by category", "width": "col-span-4",
             "query": _ORDERS_CTE + "SELECT region, category, ROUND(SUM(amount), 0) AS revenue FROM f GROUP BY 1, 2 ORDER BY 1, 2",
             "x_col": "region", "y_col": "revenue", "z_col": "category"},
            {"page": "details", "type": "table", "title": "Orders (latest 200)", "width": "col-span-4",
             "query": _ORDERS_CTE + "SELECT order_id, order_date, region, category, amount FROM f ORDER BY order_date DESC, order_id DESC LIMIT 200"},
        ],
    },
    {
        "id": "sample_customer_segments",
        "title": "Customer segments",
        "description": "A number parameter and a segment filter driving KPIs, charts and a top-customers list on a second page.",
        "tags": ["number parameter", "multi-page"],
        "parameters": [
            {"id": "segment", "type": "select", "label": "Segment", "options": ["Enterprise", "Mid-market", "SMB", "Consumer"], "default": None},
            {"id": "min_spend", "type": "number", "label": "Minimum annual spend", "default": 0},
        ],
        "pages": [{"id": "segments", "title": "Segments"}, {"id": "top", "title": "Top customers"}],
        "widgets": [
            {"page": "segments", "type": "kpi", "title": "Customers", "width": "col-span-1",
             "query": _CUSTOMERS_CTE + "SELECT COUNT(*) AS customers FROM f"},
            {"page": "segments", "type": "kpi", "title": "Total annual spend", "width": "col-span-1", "unit": "$",
             "query": _CUSTOMERS_CTE + "SELECT COALESCE(SUM(annual_spend), 0) AS spend FROM f"},
            {"page": "segments", "type": "kpi", "title": "Average spend", "width": "col-span-1", "unit": "$",
             "query": _CUSTOMERS_CTE + "SELECT ROUND(COALESCE(AVG(annual_spend), 0), 0) AS avg_spend FROM f"},
            {"page": "segments", "type": "kpi", "title": "Average tenure (years)", "width": "col-span-1",
             "query": _CUSTOMERS_CTE + "SELECT ROUND(COALESCE(AVG(tenure_years), 0), 1) AS tenure FROM f"},
            {"page": "segments", "type": "bar", "title": "Spend by segment (click to filter)", "width": "col-span-2",
             "query": _CUSTOMERS_CTE + "SELECT segment, SUM(annual_spend) AS spend FROM f GROUP BY 1 ORDER BY 2 DESC",
             "x_col": "segment", "y_col": "spend", "filter_dimension": "segment"},
            {"page": "segments", "type": "bar", "title": "Customers by tenure (years)", "width": "col-span-2",
             "query": _CUSTOMERS_CTE + "SELECT CAST(tenure_years AS VARCHAR) AS tenure, COUNT(*) AS customers FROM f GROUP BY tenure_years ORDER BY tenure_years",
             "x_col": "tenure", "y_col": "customers"},
            {"page": "top", "type": "text", "title": "Note", "width": "col-span-4",
             "markdown": "The 50 highest-spending customers for the current filters. Use the download icon on the table to export."},
            {"page": "top", "type": "table", "title": "Top 50 customers", "width": "col-span-4",
             "query": _CUSTOMERS_CTE + "SELECT customer_id, segment, annual_spend, tenure_years FROM f ORDER BY annual_spend DESC, customer_id LIMIT 50"},
        ],
    },
    {
        "id": "sample_table_explorer",
        "title": "Table explorer",
        "description": "Pick any table you can read and see its size, columns and a preview. Shows a table-name parameter filled from the catalog.",
        "tags": ["real data", "table picker"],
        "parameters": [
            {"id": "tbl", "type": "select", "label": "Table", "required": True, "as_identifier": True, "default": None,
             "options_query": "SELECT table_catalog || '.' || table_schema || '.' || table_name FROM information_schema.tables "
                              "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_catalog NOT IN ('system', 'temp') ORDER BY 1"},
        ],
        "pages": [{"id": "overview", "title": "Overview"}, {"id": "preview", "title": "Preview"}],
        "widgets": [
            {"page": "overview", "type": "text", "title": "About this app", "width": "col-span-4",
             "markdown": "Choose a table above. The list only shows tables in catalogs you can read, and every query below runs with **your** permissions, so masks and row filters apply."},
            {"page": "overview", "type": "kpi", "title": "Rows", "width": "col-span-2",
             "query": "SELECT COUNT(*) AS row_count FROM :tbl"},
            {"page": "overview", "type": "kpi", "title": "Columns", "width": "col-span-2",
             "query": "SELECT COUNT(*) AS column_count FROM information_schema.columns "
                      "WHERE table_catalog || '.' || table_schema || '.' || table_name = :tbl_text"},
            {"page": "overview", "type": "table", "title": "Columns", "width": "col-span-4",
             "query": "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                      "WHERE table_catalog || '.' || table_schema || '.' || table_name = :tbl_text ORDER BY ordinal_position"},
            {"page": "preview", "type": "table", "title": "First 100 rows", "width": "col-span-4",
             "query": "SELECT * FROM :tbl LIMIT 100"},
        ],
    },
]


def sample_by_id(sample_id: str) -> Optional[Dict[str, Any]]:
    return next((s for s in SAMPLES if s["id"] == sample_id), None)


def materialize(spec: Dict[str, Any], name: str, description: str, owner: str) -> Dict[str, Any]:
    """A fresh dashboard dict (new ids) from a sample or an existing app; validated like any saved spec."""
    widgets = []
    for w in spec.get("widgets", []):
        w = dict(w)
        w["id"] = f"w_{uuid.uuid4().hex[:8]}"
        widgets.append(w)
    cleaned = validate_spec(copy.deepcopy(spec.get("parameters")), copy.deepcopy(spec.get("pages")), widgets)
    return {
        "id": f"dash_{uuid.uuid4().hex[:10]}",
        "name": name,
        "description": description,
        "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "created_by": owner,
        "parameters": cleaned["parameters"],
        "pages": cleaned["pages"],
        "widgets": cleaned["widgets"],
        "filters": [],
    }


def _save_new(dash: Dict[str, Any], owner: str) -> None:
    from web.dashboard_permissions import initialize_dashboard_permissions
    from web.dashboards import load_dashboards_store, save_dashboards_store
    dashboards = load_dashboards_store()
    dashboards.append(dash)
    save_dashboards_store(dashboards)
    initialize_dashboard_permissions(dash["id"], owner)


def _name(v: Optional[str], fallback: str) -> str:
    s = (v or "").strip() or fallback
    if len(s) > 80 or "\x00" in s:
        raise HTTPException(status_code=400, detail="The name is too long.")
    return s


# --------------------------------------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------------------------------------

def _is_app(d: Dict[str, Any]) -> bool:
    return bool(d.get("pages"))


@router.get("/api/dashboard-apps")
async def list_apps(user: Dict[str, Any] = Depends(get_current_user)):
    """The Apps catalogue: dashboards that have pages, limited to those the caller may view."""
    from web.dashboards import load_dashboards_store
    out = []
    for d in load_dashboards_store():
        if not _is_app(d):
            continue
        level = level_for(d["id"], user)
        if _LEVELS.get(level, 0) < _LEVELS["viewer"]:
            continue
        out.append({"id": d["id"], "name": d["name"], "description": d.get("description", ""), "created_by": d.get("created_by", ""),
                    "created_at": d.get("created_at", ""), "page_count": len(d.get("pages", [])),
                    "widget_count": len(d.get("widgets", [])), "parameter_count": len(d.get("parameters", [])),
                    "my_access": level})
    return {"apps": out}


class NewAppPayload(BaseModel):
    name: str
    description: Optional[str] = ""


@router.post("/api/dashboard-apps")
async def create_app(payload: NewAppPayload, user: Dict[str, Any] = Depends(get_current_user)):
    dash = materialize({"pages": [{"id": "page1", "title": "Page 1"}], "parameters": [], "widgets": []},
                       _name(payload.name, "Untitled app"), (payload.description or "")[:500], user["username"])
    _save_new(dash, user["username"])
    return dash


@router.get("/api/dashboard-apps/samples")
async def list_samples(user: Dict[str, Any] = Depends(get_current_user)):
    return {"samples": [{"id": s["id"], "title": s["title"], "description": s["description"], "tags": s["tags"],
                         "page_count": len(s["pages"]), "widget_count": len(s["widgets"]),
                         "parameter_count": len(s["parameters"]),
                         "pages": [p["title"] for p in s["pages"]]} for s in SAMPLES]}


class ClonePayload(BaseModel):
    name: Optional[str] = None


@router.post("/api/dashboard-apps/samples/{sample_id}/clone")
async def clone_sample(sample_id: str, payload: ClonePayload, user: Dict[str, Any] = Depends(get_current_user)):
    s = sample_by_id(sample_id)
    if not s:
        raise HTTPException(status_code=404, detail="Sample not found")
    try:
        dash = materialize(s, _name(payload.name, s["title"]), s["description"], user["username"])
    except SpecError as e:
        raise HTTPException(status_code=500, detail=f"The sample is invalid: {e}")
    _save_new(dash, user["username"])
    return dash


@router.post("/api/dashboard-apps/{dashboard_id}/clone")
async def clone_app(dashboard_id: str, payload: ClonePayload, user: Dict[str, Any] = Depends(get_current_user)):
    """Copy an app (or any dashboard) the caller can view into one they own."""
    from web.dashboards import load_dashboards_store
    require_level(dashboard_id, user, "viewer")
    src = next((d for d in load_dashboards_store() if d["id"] == dashboard_id), None)
    if not src:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    spec = dict(src)
    if not spec.get("pages"):
        spec["pages"] = [{"id": "page1", "title": "Page 1"}]
        spec["widgets"] = [dict(w, page="page1") for w in src.get("widgets", [])]
    try:
        dash = materialize(spec, _name(payload.name, f"{src['name']} (copy)"), src.get("description", ""), user["username"])
    except SpecError as e:
        raise HTTPException(status_code=400, detail=str(e))
    _save_new(dash, user["username"])
    return dash


class SpecPayload(BaseModel):
    parameters: Optional[List[Dict[str, Any]]] = None
    pages: Optional[List[Dict[str, Any]]] = None


@router.put("/api/dashboards/{dashboard_id}/spec")
async def save_spec(dashboard_id: str, payload: SpecPayload, user: Dict[str, Any] = Depends(get_current_user)):
    """Replace the parameters and pages of a dashboard. Widgets on a removed page move to the first remaining page."""
    from web.dashboards import load_dashboards_store, save_dashboards_store
    require_level(dashboard_id, user, "editor")
    dashboards = load_dashboards_store()
    d = next((x for x in dashboards if x["id"] == dashboard_id), None)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    params = payload.parameters if payload.parameters is not None else d.get("parameters", [])
    pages = payload.pages if payload.pages is not None else d.get("pages", [])
    if payload.pages is not None:
        pg_ids = {p.get("id") for p in pages if isinstance(p, dict)}
        first = next(iter(p.get("id") for p in pages if isinstance(p, dict)), None)
        for w in d.get("widgets", []):
            if w.get("page") not in pg_ids:
                w["page"] = first
                if first is None:
                    w.pop("page", None)
    try:
        cleaned = validate_spec(params, pages, d.get("widgets", []))
    except SpecError as e:
        raise HTTPException(status_code=400, detail=str(e))
    d["parameters"], d["pages"], d["widgets"] = cleaned["parameters"], cleaned["pages"], cleaned["widgets"]
    save_dashboards_store(dashboards)
    return {"success": True, "parameters": d["parameters"], "pages": d["pages"]}


class WidgetUpdate(BaseModel):
    title: Optional[str] = None
    type: Optional[str] = None
    query: Optional[str] = None
    x_col: Optional[str] = None
    y_col: Optional[str] = None
    z_col: Optional[str] = None
    unit: Optional[str] = None
    width: Optional[str] = None
    page: Optional[str] = None
    markdown: Optional[str] = None
    filter_dimension: Optional[str] = None


@router.put("/api/dashboards/{dashboard_id}/widgets/{widget_id}")
async def update_widget(dashboard_id: str, widget_id: str, payload: WidgetUpdate, request: Request,
                        user: Dict[str, Any] = Depends(get_current_user)):
    from web.app import get_duckrun_conn
    from web.dashboards import load_dashboards_store, save_dashboards_store
    require_level(dashboard_id, user, "editor")
    dashboards = load_dashboards_store()
    d = next((x for x in dashboards if x["id"] == dashboard_id), None)
    w = next((x for x in (d or {}).get("widgets", []) if x["id"] == widget_id), None)
    if not d or not w:
        raise HTTPException(status_code=404, detail="Widget not found")
    merged = dict(w)
    merged.update({k: v for k, v in payload.dict().items() if v is not None})
    try:
        cleaned = clean_widget(merged, d.get("parameters", []), {p["id"] for p in d.get("pages", [])})
    except SpecError as e:
        raise HTTPException(status_code=400, detail=str(e))
    cleaned["id"] = widget_id
    d["widgets"] = [cleaned if x["id"] == widget_id else x for x in d["widgets"]]
    save_dashboards_store(dashboards)
    principal = await resolve_principal(request)
    out = dict(cleaned)
    out["result"] = hydrate_widget(get_duckrun_conn(), cleaned, None, principal, d.get("parameters") or None)
    return out
