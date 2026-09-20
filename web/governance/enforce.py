"""
The governance gateway: rewrites a user's SQL so masked columns can never be read, and refuses statements that could
bypass masking. Every path that runs user-controlled SQL on behalf of a principal must go through
`rewrite_for_principal` (see scratch/test_governance_coverage.py for the lint that enforces this).

How masking works (mask at scan)
  Each physical table scan of a table with masked columns is replaced by
      (SELECT * REPLACE (<mask> AS "col", ...) FROM <original table>) AS <original alias>
  so filters, joins, aggregates, ORDER BY and SELECT * only ever see masked values, and there is no
  "WHERE ssn = ..." oracle. Views are inlined recursively, path-based scans (delta_scan('/warehouse/hr/employees'))
  are mapped back to their table, and CTE/derived-table names are told apart from real tables with sqlglot's scope
  analysis.

Fail closed
  If a principal is subject to any masking policy and a statement cannot be parsed or resolved, or is not on the
  allowlist, it is blocked with an explanation. It is never executed unmasked.

Other guarantees (independent of masking policies, for non-admin principals)
  * File functions only accept literal paths inside the warehouse's tables/volumes/exports; `.metadata` (secrets, auth
    DB) and arbitrary paths are refused, since reading `.metadata/jwt_secret` would allow forging admin sessions.
"""

import hashlib
import logging
import os
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope

from web.governance import catalog_meta, policies, store, tags
from web.governance.policies import MaskSpec, Principal

logging.getLogger("sqlglot").setLevel(logging.ERROR)
logger = logging.getLogger("localspark.governance")

MAX_VIEW_DEPTH = 10
SYSTEM_CATALOGS = ("system", "temp")

# Table functions a masked principal may use. Anything else (query(), query_table(), glob(), read_text(), ...) is refused.
ALLOWED_TABLE_FUNCS = {"range", "generate_series", "unnest", "generate_subscripts", "duckdb_tables", "duckdb_columns",
                       "duckdb_views", "duckdb_schemas", "duckdb_databases", "values"}
FILE_SCAN_FUNCS = {"delta_scan", "read_parquet", "parquet_scan", "read_csv", "read_csv_auto", "read_json", "read_json_auto",
                   "read_ndjson", "read_ndjson_auto", "iceberg_scan", "read_text", "read_blob", "parquet_metadata"}
# Scans that expose a table's *columns* (rewritable by masking). The rest read raw bytes/metadata and are refused on masked tables.
COLUMN_SCAN_FUNCS = {"delta_scan", "read_parquet", "parquet_scan", "read_csv", "read_csv_auto", "read_json", "read_json_auto",
                     "read_ndjson", "read_ndjson_auto"}
_FILE_EXT = re.compile(r"\.(parquet|csv|tsv|json|jsonl|ndjson|txt|gz|zst)(\?.*)?$", re.IGNORECASE)
_EXPLAIN = re.compile(r"^\s*(EXPLAIN(?:\s+ANALYZE)?)\s+(.*)$", re.IGNORECASE | re.DOTALL)


class GovernanceBlocked(Exception):
    """Raised by callers that prefer exceptions; carries the user-facing reason."""


@dataclass
class MaskedColumn:
    table: str
    column: str
    policy_id: str
    policy_name: str
    mask_type: str


@dataclass
class RewriteResult:
    sql: str                                   # SQL to execute (== the input when nothing changed)
    original_sql: str = ""
    changed: bool = False
    masked: List[MaskedColumn] = field(default_factory=list)
    blocked: Optional[str] = None              # when set the statement must NOT be executed
    tables: List[str] = field(default_factory=list)
    exempt_reads: List[MaskedColumn] = field(default_factory=list)  # columns the principal is exempt from (audited)

    @property
    def masked_tables(self) -> List[str]:
        return sorted({m.table for m in self.masked})


class _Block(Exception):
    """Internal: unwinds the rewrite with a reason."""


# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

def enforcement_mode() -> str:
    """`enforce` (default) | `audit` (compute and log, never change/block) | `off`."""
    mode = os.getenv("GOVERNANCE_ENFORCEMENT", "enforce").strip().lower()
    return mode if mode in ("enforce", "audit", "off") else "enforce"


def _warehouse_dir() -> str:
    return os.path.realpath(os.getenv("WAREHOUSE_DIR", "/workspace/warehouse"))


def _extra_allowed_paths() -> List[str]:
    raw = os.getenv("GOVERNANCE_ALLOWED_PATHS", "")
    return [os.path.realpath(p) for p in raw.split(os.pathsep) if p.strip()]


# ----------------------------------------------------------------------------
# Path -> table resolution (for delta_scan('...'), read_parquet('...'), 'file.parquet')
# ----------------------------------------------------------------------------

@dataclass
class PathInfo:
    kind: str                                   # table | volume | exports | metadata | outside | remote-unknown | warehouse-other
    identity: Optional[Tuple[str, str, str]] = None


_GLOB = re.compile(r"[*?\[]")


def resolve_path(path: str, home_catalog: str) -> PathInfo:
    """Classifies a literal file path. Globs resolve by their static prefix."""
    p = path.strip()
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", p):
        scheme, rest = p.split("://", 1)
        if scheme.lower() == "file":
            return resolve_path("/" + rest.lstrip("/"), home_catalog)
        if scheme.lower() in ("s3", "s3a", "s3n"):
            bucket, _, key = rest.partition("/")
            try:
                from web.mounts import load_mounts
                for m in load_mounts():
                    if m.get("enabled", True) and m.get("type") == "s3" and (m.get("config") or {}).get("bucket") == bucket:
                        parts = [x for x in _GLOB.split(key)[0].split("/") if x]
                        cat = m.get("catalog_name", "")
                        if len(parts) >= 2:
                            return PathInfo("table", (tags.norm(cat), tags.norm(parts[0]), tags.norm(parts[1])))
                        if len(parts) == 1:
                            return PathInfo("table", (tags.norm(cat), "dbo", tags.norm(parts[0])))
                        return PathInfo("remote-unknown")
            except Exception:
                pass
        return PathInfo("remote-unknown")

    prefix = _GLOB.split(p)[0]
    if not os.path.isabs(prefix):
        prefix = os.path.join(os.getcwd(), prefix)
    root = _warehouse_dir()
    # Classify by the lexical path (external volumes are symlinks under warehouse/volumes), but never let a symlink
    # smuggle a read of the metadata directory.
    real_target = os.path.realpath(prefix)
    if real_target == os.path.join(root, ".metadata") or real_target.startswith(os.path.join(root, ".metadata") + os.sep):
        return PathInfo("metadata")
    real = os.path.abspath(prefix)
    if real != root and not real.startswith(root + os.sep):
        allowed = ["/tmp/uploads"] + _extra_allowed_paths()
        if any(real == a or real.startswith(a + os.sep) for a in allowed):
            return PathInfo("volume")
        return PathInfo("outside")
    has_glob = bool(_GLOB.search(p))
    if has_glob:
        static_dir = prefix if prefix.endswith(os.sep) else os.path.dirname(prefix)
        # a wildcard in the first component under the warehouse root (`.meta*`, `[.]metadata`, `**`) could expand to .metadata
        if os.path.abspath(static_dir) == root:
            return PathInfo("warehouse-other")
        if ".." in p[len(_GLOB.split(p)[0]):].split("/"):
            return PathInfo("outside")
    parts = [x for x in os.path.relpath(real, root).split(os.sep) if x and x != "."]
    if not parts:
        return PathInfo("warehouse-other")
    head = parts[0]
    if head == ".metadata":
        return PathInfo("metadata")
    if head == "volumes":
        return PathInfo("volume")
    if head == "exports":
        return PathInfo("exports")
    if head == "catalogs" and len(parts) >= 4:
        return PathInfo("table", (tags.norm(parts[1]), tags.norm(parts[2]), tags.norm(parts[3])))
    if head in ("catalogs", "mlflow", "notebooks"):
        return PathInfo("warehouse-other")
    if len(parts) >= 2:
        return PathInfo("table", (tags.norm(home_catalog), tags.norm(parts[0]), tags.norm(parts[1])))
    return PathInfo("table", (tags.norm(home_catalog), "dbo", tags.norm(parts[0])))


# ----------------------------------------------------------------------------
# Rewrite context
# ----------------------------------------------------------------------------

class _Ctx:
    """Per-call state: metadata lookups (cached), current catalog/schema (tracks USE), accumulated results."""

    def __init__(self, con, principal: Principal, subject: bool, sandbox: bool, default_catalog: Optional[str],
                 default_schema: Optional[str], trusted: bool = False):
        self.con = con
        self.trusted = trusted          # server-built SQL (previews, exports): masking applies, file sandbox/allowlists do not
        self.principal = principal
        self.subject = subject          # at least one masking policy applies -> allowlists + masking
        self.sandbox = sandbox          # non-admin: file sandbox applies
        cur = con.execute("SELECT current_database(), current_schema()").fetchone()
        self.session_catalog = tags.norm(cur[0])
        self.home_catalog = self._warehouse_catalog() or self.session_catalog
        self.catalog = tags.norm(default_catalog) if default_catalog else self.session_catalog
        self.schema = tags.norm(default_schema) if default_schema else tags.norm(cur[1])
        self._cols: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        self._views: Optional[Dict[Tuple[str, str, str], str]] = None
        self.masked: List[MaskedColumn] = []
        self.exempt_reads: List[MaskedColumn] = []
        self.tables: Set[str] = set()
        self.changed = False
        self.uses_file_scan = False
        self.use_texts: Dict[int, str] = {}     # USE statements are re-emitted verbatim (sqlglot mangles `USE catalog.schema`)

    def _warehouse_catalog(self) -> Optional[str]:
        """
        The catalog that WAREHOUSE_DIR is attached as. Path scans of `<warehouse>/<schema>/<table>` belong to it no matter
        which catalog the executing session happens to be in (cursors start in memory.main, workers in warehouse).
        """
        name = tags.norm(os.path.basename(os.path.normpath(os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")))).replace("-", "_")
        for candidate in (name, "warehouse"):
            try:
                if catalog_meta.catalog_exists(self.con, candidate):
                    return candidate
            except Exception:
                pass
        return None

    def prefetch(self, candidates: List[Tuple[str, str, str]]) -> None:
        """
        Loads the columns of every candidate table in ONE catalog query. duckdb_columns() enumerates all attached catalogs
        (a 300-table Postgres mount costs ~5 ms per call), so per-table lookups would multiply that.
        """
        missing = list({c for c in candidates if c not in self._cols and c[0] not in SYSTEM_CATALOGS})
        if not missing:
            return
        placeholders = ", ".join("(?, ?, ?)" for _ in missing)
        params = [p for c in missing for p in c]
        rows = self.con.execute(
            "SELECT lower(database_name), lower(schema_name), lower(table_name), column_name, data_type, column_index "
            f"FROM duckdb_columns() WHERE (lower(database_name), lower(schema_name), lower(table_name)) IN ({placeholders}) "
            "ORDER BY 1, 2, 3, column_index", params).fetchall()
        for c in missing:
            self._cols[c] = []
        for db, sch, tbl, col, typ, pos in rows:
            self._cols[(db, sch, tbl)].append({"catalog": db, "schema": sch, "table": tbl, "column": col, "type": typ, "position": pos})

    def columns(self, cat: str, sch: str, tbl: str) -> List[Dict[str, Any]]:
        key = (cat, sch, tbl)
        if key not in self._cols:
            self.prefetch([key])
        return self._cols.get(key, [])

    def view_sql(self, cat: str, sch: str, name: str) -> Optional[str]:
        """View definitions are read once per rewrite (one catalog pass) and never cached across queries."""
        if self._views is None:
            rows = self.con.execute(
                "SELECT lower(database_name), lower(schema_name), lower(view_name), sql FROM duckdb_views() WHERE NOT internal").fetchall()
            self._views = {(r[0], r[1], r[2]): r[3] for r in rows}
        return self._views.get((cat, sch, name))


# ----------------------------------------------------------------------------
# Table reference resolution
# ----------------------------------------------------------------------------

def _candidates(tbl: exp.Table, ctx: _Ctx) -> List[Tuple[str, str, str]]:
    """Where DuckDB might look for this reference, in resolution order."""
    name = tags.norm(tbl.name)
    db = tags.norm(tbl.db)
    cat = tags.norm(tbl.catalog)
    if cat and db:
        return [(cat, db, name)]
    if db:
        return [(ctx.catalog, db, name), (db, "main", name), (db, ctx.schema, name), (db, "dbo", name)]
    return [(ctx.catalog, ctx.schema, name), (ctx.catalog, "main", name), (ctx.catalog, "dbo", name)]


def _resolve_identifier(tbl: exp.Table, ctx: _Ctx) -> Optional[Tuple[str, str, str]]:
    """Maps a table reference to (catalog, schema, table) the way DuckDB would; None if it does not exist."""
    for cand in _candidates(tbl, ctx):
        if cand[0] in SYSTEM_CATALOGS:
            return None
        if ctx.columns(*cand):
            return cand
    return None


def _label(ident: Tuple[str, str, str]) -> str:
    return ".".join(ident)


def _looks_like_path(tbl: exp.Table) -> Optional[str]:
    """`FROM 'x.parquet'` / `FROM 's3://b/t'`: a string used as a table name is a file scan."""
    name = tbl.name
    if name and not tbl.db and (_FILE_EXT.search(name) or "://" in name or "/" in name):
        return name
    return None


def _masks_for(ident: Tuple[str, str, str], ctx: _Ctx, principal: Principal) -> List[MaskSpec]:
    cols = ctx.columns(*ident)
    return policies.masks_for_table(ident[0], ident[1], ident[2], cols, principal)


def _record_exempt_reads(ident: Tuple[str, str, str], ctx: _Ctx, specs: List[MaskSpec]) -> None:
    """
    Columns of `ident` that a fully non-exempt principal would lose but this principal reads raw. Includes the mixed
    case (exempt from some policies, masked by others): only the columns still masked are excluded.
    """
    still_masked = {s.column for s in specs}
    for g in _would_mask_for_exempt(ident, ctx):
        if g.column not in still_masked:
            ctx.exempt_reads.append(MaskedColumn(_label(ident), g.column, g.policy_id, g.policy_name, g.mask_type))


def _would_mask_for_exempt(ident: Tuple[str, str, str], ctx: _Ctx) -> List[MaskSpec]:
    """Columns a *non-exempt* principal would lose here (used to audit exempt reads such as admins)."""
    ghost = Principal(username="\u0000audit", role="user")
    return _masks_for(ident, ctx, ghost)


def _build_masked_subquery(source: exp.Expression, specs: List[MaskSpec], alias: Optional[exp.Expression],
                           default_alias: str) -> exp.Subquery:
    """(SELECT * REPLACE (mask AS col, ...) FROM <source>) AS alias"""
    replace_list = ", ".join(f"{s.expression} AS {_quote(s.column)}" for s in specs)
    template = _cache_get(_TEMPLATE_CACHE, replace_list)
    if template is None:
        template = sqlglot.parse_one(f"SELECT * REPLACE ({replace_list}) FROM __src__", dialect="duckdb")
        _cache_put(_TEMPLATE_CACHE, replace_list, template)
    inner = template.copy()
    inner.find(exp.Table).replace(source)
    tbl_alias = alias if alias is not None else exp.TableAlias(this=exp.to_identifier(default_alias))
    return exp.Subquery(this=inner, alias=tbl_alias)


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _record(ctx: _Ctx, ident: Tuple[str, str, str], specs: List[MaskSpec]) -> None:
    for s in specs:
        ctx.masked.append(MaskedColumn(_label(ident), s.column, s.policy_id, s.policy_name, s.mask_type))
    ctx.changed = True


def _rewrite_table_ref(tbl: exp.Table, ctx: _Ctx, depth: int, stack: Tuple[Tuple[str, str, str], ...]) -> Set[Tuple[str, str, str]]:
    """
    Rewrites one physical table reference in place. Returns the set of table identities masked inside (so a view over
    a Delta path is not masked twice). Raises _Block when the reference cannot be handled safely.
    """
    # -- table functions and string-literal file scans ------------------------------------------------------------
    if not isinstance(tbl.this, exp.Identifier):
        return _rewrite_function_ref(tbl, ctx, depth, stack)

    if tags.norm(tbl.db) in ("information_schema", "pg_catalog") and not tbl.catalog:
        return set()                                        # engine metadata views: names and types, never row values
    ident = _resolve_identifier(tbl, ctx)
    if ident is None and _looks_like_path(tbl):
        # sqlglot cannot tell FROM 'x.parquet' from FROM "x.parquet": a real object of that name wins, else it is a file scan
        return _rewrite_function_ref(tbl, ctx, depth, stack)
    if ident is None:
        # A name we cannot resolve might still resolve for the engine (its default catalog/schema can differ from ours).
        # Refuse when it could be tagged data: a tagged table's name, or any broad (catalog/schema) tag exists.
        if ctx.subject and not ctx.trusted and (_name_is_sensitive(tags.norm(tbl.name), ctx) or _broad_tags_exist()):
            raise _Block(f"Could not resolve table '{tbl.sql()}' while masking policies apply; qualify it as catalog.schema.table.")
        return set()
    ctx.tables.add(_label(ident))
    if ctx.subject:
        # Execute exactly what was analysed: pin the reference to its resolved identity so the engine's own default
        # catalog/schema (which differs between the studio cursor, workers and Ray actors) cannot pick another table.
        tbl.set("catalog", exp.to_identifier(ident[0]))
        tbl.set("db", exp.to_identifier(ident[1]))
        ctx.changed = True

    inner_masked: Set[Tuple[str, str, str]] = set()
    alias_node = tbl.args.get("alias")
    default_alias = tags.norm(tbl.name) if not alias_node else None
    source: exp.Expression = tbl
    view_body_replaced = False

    # -- views: mask through their definition ----------------------------------------------------------------------
    body_sql = ctx.view_sql(*ident)
    if body_sql is not None and ctx.subject:
        if ident in stack or depth >= MAX_VIEW_DEPTH:
            raise _Block(f"View '{_label(ident)}' is too deeply nested or recursive to verify.")
        try:
            created = sqlglot.parse_one(body_sql, dialect="duckdb")
            body = created.args.get("expression") if isinstance(created, exp.Create) else None
            if not isinstance(body, exp.Query) or (isinstance(created, exp.Create) and created.this is not None
                                                   and getattr(created.this, "expressions", None)):
                raise ValueError("unsupported view definition")
        except Exception:
            raise _Block(f"View '{_label(ident)}' has a definition that cannot be verified for masking.")
        saved = (ctx.catalog, ctx.schema)
        ctx.catalog, ctx.schema = ident[0], ident[1]
        before = len(ctx.masked)
        try:
            body = body.copy()
            inner_masked = _rewrite_query(body, ctx, depth + 1, stack + (ident,))
        finally:
            ctx.catalog, ctx.schema = saved
        if len(ctx.masked) > before:
            plain = tbl.copy()
            plain.set("alias", None)
            source = exp.Subquery(this=body, alias=exp.TableAlias(this=exp.to_identifier(ident[2])))
            view_body_replaced = True

    # -- masks on the object itself --------------------------------------------------------------------------------
    specs = _masks_for(ident, ctx, ctx.principal) if ident not in inner_masked else []
    if ident not in inner_masked:            # (masked inside the view body already: nothing was read raw)
        _record_exempt_reads(ident, ctx, specs)
    if not specs:
        if view_body_replaced:
            tbl.replace(_with_alias(source, alias_node))
        return inner_masked | ({ident} if view_body_replaced else set())

    _record(ctx, ident, specs)
    if source is tbl:
        stripped = tbl.copy()
        stripped.set("alias", None)
        source = stripped
    tbl.replace(_build_masked_subquery(source, specs, alias_node, default_alias or tags.norm(tbl.name)))
    return inner_masked | {ident}


def _with_alias(subquery: exp.Subquery, alias_node: Optional[exp.Expression]) -> exp.Subquery:
    if alias_node is not None:
        subquery.set("alias", alias_node)
    return subquery


def _broad_tags_exist() -> bool:
    idx = tags.get_index()
    return bool(idx["catalogs"] or idx["schemas"])


def _name_is_sensitive(name: str, ctx: _Ctx) -> bool:
    """True when some table with masked columns for this principal has this bare name."""
    idx = tags.get_index()
    return any(t[2] == name for t in idx["tables"]) or any(t[2] == name for t in idx["columns"])


def _function_name(tbl: exp.Table) -> str:
    node = tbl.this
    if isinstance(node, exp.Anonymous):
        return node.name.lower()
    if isinstance(node, exp.Identifier):
        return ""
    return {"ReadParquet": "read_parquet", "ReadCSV": "read_csv", "GenerateSeries": "generate_series", "Unnest": "unnest",
            "Explode": "unnest", "Values": "values"}.get(type(node).__name__, node.sql_name().lower())


def _literal_paths(node: exp.Expression) -> Optional[List[str]]:
    """Paths of a scan's first argument if it is a string literal or a list of string literals; None if computed."""
    if isinstance(node, exp.Literal) and node.is_string:
        return [node.this]
    if isinstance(node, exp.Array) and node.expressions and all(isinstance(e, exp.Literal) and e.is_string for e in node.expressions):
        return [e.this for e in node.expressions]
    return None


def _first_arg(fn: exp.Expression) -> Optional[exp.Expression]:
    if isinstance(fn, exp.Anonymous):
        return fn.expressions[0] if fn.expressions else None
    return fn.this if fn.this is not None else (fn.expressions[0] if fn.expressions else None)


def _rewrite_function_ref(tbl: exp.Table, ctx: _Ctx, depth: int, stack) -> Set[Tuple[str, str, str]]:
    """Table functions and file scans: sandbox the path, then map it back to a table so masks still apply."""
    literal_name = _looks_like_path(tbl)
    fn_name = _function_name(tbl) if literal_name is None else "read_parquet" if literal_name.lower().endswith(".parquet") else "read_csv"
    if literal_name is None and not isinstance(tbl.this, exp.Identifier) and fn_name not in FILE_SCAN_FUNCS:
        if ctx.subject and not ctx.trusted and fn_name not in ALLOWED_TABLE_FUNCS:
            raise _Block(f"The table function '{fn_name}' is not available while masking policies apply to you.")
        return set()

    ctx.uses_file_scan = True
    if literal_name is not None:
        paths: Optional[List[str]] = [literal_name]
    else:
        arg = _first_arg(tbl.this)
        paths = _literal_paths(arg) if arg is not None else None
    if paths is None:
        if (ctx.sandbox or ctx.subject) and not ctx.trusted:
            raise _Block(f"'{fn_name}' needs a literal file path; computed paths are not allowed for your role.")
        return set()

    idents: Set[Tuple[str, str, str]] = set()
    for path in paths:
        info = resolve_path(path, ctx.home_catalog)
        if info.kind == "metadata" and (ctx.sandbox or ctx.subject):
            raise _Block("Access to platform metadata files is not allowed.")
        if info.kind in ("outside", "remote-unknown", "warehouse-other") and (ctx.sandbox or ctx.subject) and not ctx.trusted:
            raise _Block(f"Reading '{path}' is not allowed: only warehouse tables, volumes and exports can be scanned.")
        if info.kind == "table" and info.identity:
            idents.add(info.identity)
    if not idents:
        return set()
    if len(idents) > 1 and ctx.subject and not ctx.trusted:
        raise _Block("A single scan cannot mix files of several tables while masking policies apply.")

    ident = next(iter(idents))
    ctx.tables.add(_label(ident))
    cols = ctx.columns(*ident)
    if not cols:
        # Path looks like a table dir but the table is not registered: treat as sensitive only if it carries tags.
        if ctx.subject and tags.table_has_any_tags(*ident):
            raise _Block(f"Cannot verify the columns of '{_label(ident)}' for masking.")
        return set()
    specs = _masks_for(ident, ctx, ctx.principal)
    _record_exempt_reads(ident, ctx, specs)
    if not specs:
        return set()
    if fn_name not in COLUMN_SCAN_FUNCS:
        raise _Block(f"'{fn_name}' cannot read files of '{_label(ident)}': it has masked columns "
                     f"({', '.join(s.column for s in specs)}).")
    _record(ctx, ident, specs)
    alias_node = tbl.args.get("alias")
    stripped = tbl.copy()
    stripped.set("alias", None)
    tbl.replace(_build_masked_subquery(stripped, specs, alias_node, fn_name if literal_name is None else "scan"))
    return {ident}


# ----------------------------------------------------------------------------
# Statement handling
# ----------------------------------------------------------------------------

def _rewrite_query(root: exp.Expression, ctx: _Ctx, depth: int = 0, stack=()) -> Set[Tuple[str, str, str]]:
    """Rewrites every physical table scan inside a query expression (SELECT / set operation / CTE)."""
    masked_ids: Set[Tuple[str, str, str]] = set()
    refs: List[exp.Table] = []
    for scope in traverse_scope(root):
        for t in scope.tables:
            if not t.db and not t.catalog and isinstance(scope.sources.get(t.name), Scope):
                continue                                   # CTE or derived-table reference, not a physical table
            refs.append(t)
    ctx.prefetch([c for t in refs if isinstance(t.this, exp.Identifier) for c in _candidates(t, ctx)])
    unaliased: List[Tuple[str, str]] = []
    for t in refs:
        had_alias = t.args.get("alias") is not None
        ids = _rewrite_table_ref(t, ctx, depth, stack)
        masked_ids |= ids
        if ids and not had_alias and isinstance(t.this, exp.Identifier):
            unaliased.append((tags.norm(t.name), tags.norm(t.db)))
    # `hr.employees.email` -> `employees.email` now that the table is a subquery named `employees`
    for col in root.find_all(exp.Column):
        if (col.args.get("db") or col.args.get("catalog")) and any(tags.norm(col.table) == n for n, _ in unaliased):
            col.set("db", None)
            col.set("catalog", None)
    return masked_ids


def _forbidden_function(stmt: exp.Expression) -> Optional[str]:
    for fn in stmt.find_all(exp.Anonymous):
        name = fn.name.lower()
        if name.startswith("gov_"):
            return f"The function '{fn.name}' is reserved for the governance layer."
        if name in ("query", "query_table"):
            return f"'{fn.name}' executes SQL from a string and would bypass masking."
    return None


_ALLOWED_ROOTS_SUBJECT = (exp.Query, exp.Describe, exp.Show, exp.Use, exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Alter,
                          exp.TruncateTable, exp.Transaction, exp.Commit, exp.Rollback, exp.Copy, exp.Summarize, exp.Pivot,
                          exp.Create, exp.Drop, exp.Set)
_CREATE_KINDS_OK = {"TABLE", "VIEW", "SCHEMA", "SEQUENCE", "INDEX", "TYPE"}
_DROP_KINDS_OK = {"TABLE", "VIEW", "SCHEMA", "SEQUENCE", "INDEX", "TYPE"}
_READ_ONLY_ROOTS = (exp.Update, exp.Delete, exp.Merge, exp.Copy, exp.Summarize, exp.Pivot)


def _gate_sandbox(stmt: exp.Expression, ctx: _Ctx) -> None:
    """Rules for every non-admin principal, with or without masking policies."""
    if not ctx.sandbox or ctx.trusted:
        return
    if isinstance(stmt, (exp.Create, exp.Drop)) and (stmt.args.get("kind") or "").upper() in ("MACRO", "FUNCTION"):
        target = stmt.this.sql(dialect="duckdb").lower() if stmt.this is not None else ""
        if "gov_" in target or target.startswith("memory."):
            raise _Block("The governance mask functions cannot be redefined or dropped.")
    if isinstance(stmt, exp.Command) and _COMMAND_RISKY.search(stmt.sql(dialect="duckdb")):
        raise _Block("This statement can read files or run SQL from strings and is not available for your role.")
    if isinstance(stmt, exp.Copy):
        for lit in stmt.args.get("files") or []:
            if isinstance(lit, exp.Literal) and lit.is_string:
                info = resolve_path(lit.this, ctx.home_catalog)
                allowed = ("table", "volume", "exports") if _copy_writes(stmt) is False else ("table", "volume", "exports")
                if info.kind not in allowed:
                    raise _Block(f"COPY may only use warehouse tables, volumes and exports, not '{lit.this}'.")


_COMMAND_RISKY = re.compile(r"\b(query|query_table|read_text|read_blob|glob|read_csv\w*|read_parquet|read_json\w*|delta_scan)\s*\(|\.metadata",
                            re.IGNORECASE)


def _gate_statement(stmt: exp.Expression, ctx: _Ctx) -> None:
    """Default-deny statement allowlist for principals subject to masking."""
    _gate_sandbox(stmt, ctx)
    if not ctx.subject or ctx.trusted:
        return
    kind = (stmt.args.get("kind") or "").upper() if isinstance(stmt, (exp.Create, exp.Drop, exp.Alter)) else ""
    if isinstance(stmt, exp.Command):
        raise _Block("This statement type is not available while masking policies apply to you.")
    if not isinstance(stmt, _ALLOWED_ROOTS_SUBJECT):
        raise _Block(f"'{type(stmt).__name__.upper()}' statements are not available while masking policies apply to you.")
    if isinstance(stmt, exp.Create) and kind not in _CREATE_KINDS_OK:
        raise _Block(f"CREATE {kind or 'this object'} is not available while masking policies apply to you "
                     "(masks are defined by governance and cannot be redefined).")
    if isinstance(stmt, exp.Drop) and kind not in _DROP_KINDS_OK:
        raise _Block(f"DROP {kind or 'this object'} is not available while masking policies apply to you.")
    if isinstance(stmt, exp.Set):
        text = stmt.sql(dialect="duckdb").lower()
        if not re.match(r"^set\s+(local\s+|session\s+)?(search_path|schema)\b", text):
            raise _Block("Changing engine settings is not available while masking policies apply to you.")
    if isinstance(stmt, exp.Copy) and _copy_writes(stmt):
        raise _Block("COPY ... TO is not available while masking policies apply to you; use the export feature instead.")


def _copy_writes(stmt: exp.Copy) -> bool:
    return not stmt.args.get("kind")          # sqlglot: kind=True means COPY ... FROM (import); falsy means TO


def _apply_use(stmt: exp.Use, ctx: _Ctx) -> None:
    """Tracks `USE db.schema` / `USE schema` so later unqualified names resolve the way DuckDB will resolve them."""
    target = stmt.this
    if target is None:
        return
    parts: List[str] = []
    kind = stmt.args.get("kind")
    kind_name = tags.norm(getattr(kind, "name", "") or "")
    if kind_name and kind_name not in ("database", "schema", "catalog"):
        parts.append(kind_name)             # `USE warehouse.hr` parses as USE WAREHOUSE hr: the "kind" is the catalog
    if isinstance(target, exp.Table):
        for piece in (target.args.get("catalog"), target.args.get("db"), target.this):
            if piece is not None:
                parts.append(tags.norm(getattr(piece, "name", str(piece))))
    else:
        parts.append(tags.norm(getattr(target, "name", str(target))))
    if kind_name == "schema" and len(parts) == 1:
        ctx.schema = parts[0]
        text = f"USE {_quote(parts[0])}"
    elif len(parts) >= 2:
        ctx.catalog, ctx.schema = parts[-2], parts[-1]
        text = f"USE {_quote(parts[-2])}.{_quote(parts[-1])}"
    elif len(parts) == 1:
        text = f"USE {_quote(parts[0])}"
        if catalog_meta.catalog_exists(ctx.con, parts[0]):
            ctx.catalog, ctx.schema = parts[0], "main"
        else:
            ctx.schema = parts[0]
    else:
        return
    ctx.use_texts[id(stmt)] = text


def _query_targets(stmt: exp.Expression) -> List[exp.Expression]:
    """Query expressions inside a statement that we can scope-analyse."""
    if isinstance(stmt, exp.Query):
        return [stmt]
    if isinstance(stmt, (exp.Create, exp.Insert)) and isinstance(stmt.args.get("expression"), exp.Query):
        return [stmt.args["expression"]]
    if isinstance(stmt, exp.Describe) and isinstance(stmt.this, exp.Query):
        return [stmt.this]
    return []


def _referenced_masked(stmt: exp.Expression, ctx: _Ctx) -> List[MaskedColumn]:
    """For statements we do not rewrite: which masked columns would they touch? (over-approximation via find_all)"""
    found: List[MaskedColumn] = []
    for t in stmt.find_all(exp.Table):
        if not isinstance(t.this, exp.Identifier):
            continue
        ident = _resolve_identifier(t, ctx)
        if ident is None:
            continue
        for s in _masks_for(ident, ctx, ctx.principal):
            found.append(MaskedColumn(_label(ident), s.column, s.policy_id, s.policy_name, s.mask_type))
    return found


def _rewrite_statement(stmt: exp.Expression, ctx: _Ctx) -> exp.Expression:
    if isinstance(stmt, exp.Use):
        _apply_use(stmt, ctx)
        return stmt
    bad = _forbidden_function(stmt)
    if bad and (ctx.subject or ctx.sandbox) and not ctx.trusted:
        raise _Block(bad)
    _gate_statement(stmt, ctx)

    targets = _query_targets(stmt)
    if targets:
        for q in targets:
            _rewrite_query(q, ctx)
        # CREATE VIEW over masked data would persist masked SQL that depends on internal functions
        if isinstance(stmt, exp.Create) and (stmt.args.get("kind") or "").upper() == "VIEW" and ctx.masked:
            raise _Block("Views over masked columns cannot be created while masking policies apply to you.")
        return stmt

    # Statements without a rewritable query: refuse when they reach masked columns.
    if isinstance(stmt, exp.Describe):
        return stmt
    touched = _referenced_masked(stmt, ctx) if (ctx.subject and not isinstance(stmt, (exp.Show, exp.Transaction, exp.Commit, exp.Rollback))) else []
    if touched and isinstance(stmt, _READ_ONLY_ROOTS + (exp.Create, exp.Alter, exp.Drop, exp.TruncateTable)):
        cols = ", ".join(sorted({f"{m.table}.{m.column}" for m in touched})[:6])
        raise _Block(f"{type(stmt).__name__.upper()} cannot be used on tables with columns masked for you ({cols}).")
    # file scans inside non-query statements (e.g. COPY (SELECT ... FROM read_parquet(...))) still need sandboxing
    for t in stmt.find_all(exp.Table):
        if not isinstance(t.this, exp.Identifier) or _looks_like_path(t):
            _rewrite_function_ref(t, ctx, 0, ())
    return stmt


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------

_PARSE_CACHE: "OrderedDict[str, List[exp.Expression]]" = OrderedDict()
_PARSE_CACHE_MAX = 256
_TEMPLATE_CACHE: "OrderedDict[str, exp.Expression]" = OrderedDict()


def _cache_get(cache: "OrderedDict", key: str):
    hit = cache.get(key)
    if hit is not None:
        cache.move_to_end(key)
    return hit


def _cache_put(cache: "OrderedDict", key: str, value, limit: int = _PARSE_CACHE_MAX) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > limit:
        cache.popitem(last=False)


def _split_statements(sql: str) -> List[exp.Expression]:
    """Parses (with an LRU cache: dashboards repeat the same SQL); callers get private copies to mutate."""
    hit = _cache_get(_PARSE_CACHE, sql)
    if hit is None:
        hit = [p for p in sqlglot.parse(sql, dialect="duckdb") if p is not None]
        _cache_put(_PARSE_CACHE, sql, hit)
    return [p.copy() for p in hit]


def _subject_to_policies(principal: Principal) -> bool:
    """True when at least one enabled policy applies to this principal (i.e. is not exempt)."""
    return any(not policies.is_exempt(p, principal) for p in policies.enabled_policies())


def rewrite_for_principal(sql: str, principal: Principal, con, *, default_catalog: Optional[str] = None,
                          default_schema: Optional[str] = None, trusted: bool = False) -> RewriteResult:
    """
    Returns the SQL to execute for `principal`, or a `blocked` reason. `con` is a DuckDB connection/cursor used only for
    metadata lookups (current catalog/schema, columns, view definitions).
    """
    result = RewriteResult(sql=sql, original_sql=sql)
    if enforcement_mode() == "off" or principal.is_system:
        return result
    sandbox = principal.role != "admin"
    tagged_data = tags.has_any_tags()
    # With no tags anywhere nothing can be masked: skip every catalog lookup (masks themselves stay protected via sandbox rules).
    subject = tagged_data and _subject_to_policies(principal)
    audit_exempt = tagged_data and bool(policies.enabled_policies())
    if not (sandbox or subject or audit_exempt):
        return result

    if (sandbox or subject) and ".metadata" in sql.lower() and not trusted:
        result.blocked = "Access to platform metadata files is not allowed."
        return result

    explain_prefix = ""
    body = sql
    m = _EXPLAIN.match(sql)
    if m:
        explain_prefix, body = m.group(1) + " ", m.group(2)

    try:
        statements = _split_statements(body)
    except Exception as exc:
        return _on_parse_failure(sql, body, principal, subject, sandbox, result, exc)

    ctx = _Ctx(con, principal, subject, sandbox, default_catalog, default_schema, trusted)
    try:
        rewritten = [_rewrite_statement(s, ctx) for s in statements]
    except _Block as blocked:
        result.blocked = str(blocked)
        result.tables = sorted(ctx.tables)
        return result
    except Exception as exc:                                   # any bug in the rewriter must fail closed for subjects
        logger.exception("Governance rewrite failed")
        if subject:
            result.blocked = f"The query could not be verified for column masking ({type(exc).__name__}); it was not run."
            return result
        return result

    result.tables = sorted(ctx.tables)
    result.masked = ctx.masked
    result.exempt_reads = ctx.exempt_reads
    if ctx.changed:
        result.changed = True
        result.sql = explain_prefix + "; ".join(ctx.use_texts.get(id(s)) or s.sql(dialect="duckdb") for s in rewritten)
    return result


def _on_parse_failure(sql: str, body: str, principal: Principal, subject: bool, sandbox: bool, result: RewriteResult,
                      exc: Exception) -> RewriteResult:
    """Unparseable SQL: refuse when it might touch protected data, otherwise let the engine report the error."""
    lowered = body.lower()
    if (sandbox or subject) and ".metadata" in lowered:
        result.blocked = "Access to platform metadata files is not allowed."
    elif subject and _text_mentions_sensitive(lowered):
        result.blocked = ("The query uses syntax that cannot be verified for column masking and mentions tagged data; "
                          "it was not run. Rephrase it using standard SQL.")
    return result


def _text_mentions_sensitive(lowered_sql: str) -> bool:
    idx = tags.get_index()
    if idx["catalogs"] or idx["schemas"]:
        return True
    names = {t[2] for t in idx["tables"]} | {t[2] for t in idx["columns"]}
    return any(re.search(rf"\b{re.escape(n)}\b", lowered_sql) for n in names if n) or "delta_scan" in lowered_sql \
        or "read_parquet" in lowered_sql


# ----------------------------------------------------------------------------
# Auditing
# ----------------------------------------------------------------------------

def record_outcome(result: RewriteResult, principal: Principal, *, client: str = "sql") -> None:
    """Writes one aggregated audit row per query that was masked, blocked, or read raw by an exempt principal."""
    if not (result.masked or result.blocked or result.exempt_reads):
        return
    digest = hashlib.sha1(result.original_sql.encode("utf-8", "ignore")).hexdigest()[:12]
    try:
        conn = store.get_db()
        try:
            if result.blocked:
                store.write_audit(conn, principal.username, "QUERY_BLOCKED", None,
                                  {"reason": result.blocked, "query": digest, "client": client, "role": principal.role})
            if result.masked:
                store.write_audit(conn, principal.username, "MASK_APPLIED", None, {
                    "tables": result.masked_tables, "columns": sorted({f"{m.table}.{m.column}" for m in result.masked}),
                    "policies": sorted({m.policy_name for m in result.masked}), "query": digest, "client": client,
                    "role": principal.role})
            if result.exempt_reads:
                store.write_audit(conn, principal.username, "EXEMPT_READ", None, {
                    "columns": sorted({f"{m.table}.{m.column}" for m in result.exempt_reads}),
                    "policies": sorted({m.policy_name for m in result.exempt_reads}), "query": digest, "client": client,
                    "role": principal.role})
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(f"Could not write governance audit row: {exc}")


def govern(sql: str, principal: Principal, con, *, default_catalog: Optional[str] = None,
           default_schema: Optional[str] = None, client: str = "sql", trusted: bool = False) -> RewriteResult:
    """
    The call every egress path makes: rewrite + audit + mode handling.
    In `audit` mode nothing is changed or blocked; what would have happened is recorded and the original SQL returned.
    """
    result = rewrite_for_principal(sql, principal, con, default_catalog=default_catalog, default_schema=default_schema,
                                   trusted=trusted)
    record_outcome(result, principal, client=client)
    if enforcement_mode() == "audit":
        result.sql, result.blocked, result.changed = sql, None, False
    return result


def masked_relation(catalog: str, schema_name: str, table_name: str, principal: Principal, con) -> str:
    """
    A FROM-clause fragment for endpoints that address a table by name (previews, exports). Returns
    `catalog.schema.table` when nothing is masked, else the masked subquery text. Raises GovernanceBlocked on refusal.
    """
    ident_sql = ".".join(_quote(p) for p in (catalog, schema_name, table_name))
    res = govern(f"SELECT * FROM {ident_sql}", principal, con, client="table-reference", trusted=True)
    if res.blocked:
        raise GovernanceBlocked(res.blocked)
    if not res.changed:
        return ident_sql
    tree = sqlglot.parse_one(res.sql, dialect="duckdb")
    sub = tree.find(exp.Subquery)
    return sub.sql(dialect="duckdb") if sub is not None else ident_sql
