"""
Tag propagation on derived tables.

An exempt principal (an admin, say) who runs `CREATE TABLE copy AS SELECT * FROM hr.employees` writes the raw values of
tagged columns into a brand-new, untagged table that every masked user could then read. To close that leak, after such a
statement succeeds the tags of the columns the principal read raw are copied onto the matching columns of the new table:

  * a destination column that is a direct projection of one tagged source column receives all of its effective tags
    (source 'propagated');
  * a destination column computed from tagged columns (expressions, joins, aggregates) cannot be classified
    automatically, so it is tagged `sensitivity=unclassified` for an admin to review;
  * nothing is propagated for masked principals: the values they write are already masked.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope

from web.governance import enforce, policies, tags
from web.governance.enforce import RewriteResult
from web.governance.policies import Principal

logger = logging.getLogger("localspark.governance")

_STARTS_LIKE_WRITE = re.compile(r"^\s*(create\b[\s\S]{0,60}?\btable|insert\s+(or\s+\w+\s+)?into)\b", re.IGNORECASE)
UNCLASSIFIED = ("sensitivity", "unclassified")

Ident = Tuple[str, str, str]


def looks_like_derived_write(sql: str) -> bool:
    """Cheap pre-check so ordinary SELECTs never pay for propagation."""
    return bool(_STARTS_LIKE_WRITE.match(sql or ""))


def _trace(column: exp.Column, scope: Scope, ctx: "enforce._Ctx", depth: int = 0) -> List[Tuple[Ident, str]]:
    """The physical (table, column) pairs a column reference ultimately reads (through CTEs and subqueries)."""
    if depth > 8:
        return []
    name = column.name
    source = scope.sources.get(column.table) if column.table else None
    candidates = [source] if source is not None else list(scope.sources.values())
    out: List[Tuple[Ident, str]] = []
    for src in candidates:
        if isinstance(src, exp.Table):
            if not isinstance(src.this, exp.Identifier):
                continue
            ident = enforce._resolve_identifier(src, ctx)
            if ident and any(c["column"].lower() == name.lower() for c in ctx.columns(*ident)):
                out.append((ident, name))
        elif isinstance(src, Scope):
            select = src.expression
            selects = getattr(select, "selects", None)
            if selects is None and isinstance(select, exp.Union):
                selects = select.this.selects if hasattr(select.this, "selects") else []
            for proj in selects or []:
                if proj.alias_or_name.lower() == name.lower():
                    inner = list(proj.find_all(exp.Column))
                    for c in inner:
                        out.extend(_trace(c, src, ctx, depth + 1))
                    if isinstance(proj, exp.Star):
                        out.extend(_trace(exp.column(name), src, ctx, depth + 1))
    return out


def _projection_plan(query: exp.Expression, ctx: "enforce._Ctx") -> Dict[str, Tuple[List[Tuple[Ident, str]], bool]]:
    """{destination column: (source columns, is_direct)} for the outermost SELECT of a derived-table statement."""
    scopes = traverse_scope(query)
    if not scopes:
        return {}
    root = scopes[-1]
    node = root.expression
    if isinstance(node, exp.Union):
        while isinstance(node, exp.Union):
            node = node.this
    plan: Dict[str, Tuple[List[Tuple[Ident, str]], bool]] = {}
    ctx.prefetch([c for s in scopes for t in s.tables if isinstance(t.this, exp.Identifier) for c in enforce._candidates(t, ctx)])
    for proj in getattr(node, "selects", []):
        if isinstance(proj, exp.Star) or (isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star)):
            wanted = proj.table if isinstance(proj, exp.Column) else ""
            sources = [root.sources.get(wanted)] if wanted else list(root.sources.values())
            for src in sources:
                if isinstance(src, exp.Table) and isinstance(src.this, exp.Identifier):
                    ident = enforce._resolve_identifier(src, ctx)
                    for c in (ctx.columns(*ident) if ident else []):
                        plan.setdefault(c["column"], ([(ident, c["column"])], True))
                elif isinstance(src, Scope):
                    for inner in getattr(src.expression, "selects", []):
                        cols = [t for c in inner.find_all(exp.Column) for t in _trace(c, src, ctx)]
                        plan.setdefault(inner.alias_or_name, (cols, isinstance(inner, exp.Column) or isinstance(inner.this if isinstance(inner, exp.Alias) else None, exp.Column)))
            continue
        inner = proj.this if isinstance(proj, exp.Alias) else proj
        columns = list(proj.find_all(exp.Column))
        traced = [t for c in columns for t in _trace(c, root, ctx)]
        plan[proj.alias_or_name] = (traced, isinstance(inner, exp.Column) and len(traced) == 1)
    return plan


def _find_destination(name: exp.Table, ctx: "enforce._Ctx") -> Optional[Ident]:
    """Where the statement actually created/inserted the table (the engine's own default schema decides), by existence."""
    ctx._cols.clear()
    for cand in enforce._candidates(name, ctx):
        if ctx.columns(*cand):
            return cand
    return None


def propagate_after(sql: str, principal: Principal, result: RewriteResult, con, *, default_catalog: str = "warehouse") -> List[Dict[str, str]]:
    """
    Copies tags from the raw-read source columns of a successful CREATE TABLE AS / INSERT ... SELECT onto the
    destination table. Returns what was tagged. Best effort: never raises into the caller.
    """
    try:
        if principal.is_system or not result.exempt_reads or not looks_like_derived_write(sql):
            return []
        raw_read: Dict[str, Set[str]] = {}
        for m in result.exempt_reads:
            raw_read.setdefault(m.table, set()).add(m.column.lower())
        applied: List[Dict[str, str]] = []
        ctx = enforce._Ctx(con, principal, False, False, default_catalog, "main")
        for stmt in sqlglot.parse(sql, dialect="duckdb"):
            if stmt is None or not isinstance(stmt, (exp.Create, exp.Insert)):
                continue
            query = stmt.args.get("expression")
            target = stmt.this.this if isinstance(stmt.this, exp.Schema) else stmt.this
            if not isinstance(query, exp.Query) or not isinstance(target, exp.Table):
                continue
            plan = _projection_plan(query, ctx)
            dest = _find_destination(target, ctx)
            if not dest or not plan:
                continue
            dest_cols = {c["column"].lower(): c["column"] for c in ctx.columns(*dest)}
            for dst_col, (sources, direct) in plan.items():
                real_col = dest_cols.get(dst_col.lower())
                if real_col is None:
                    continue
                tainted = [(i, c) for i, c in sources if c.lower() in raw_read.get(".".join(i), set())]
                if not tainted:
                    continue
                if direct and len(sources) == 1:
                    ident, src_col = sources[0]
                    eff = tags.effective_tags(ident[0], ident[1], ident[2], [src_col])[src_col]
                    to_set = [(k, v["value"]) for k, v in eff.items()]
                else:
                    to_set = [UNCLASSIFIED]
                for key, value in to_set:
                    try:
                        tags.set_tag(catalog=dest[0], schema_name=dest[1], table_name=dest[2], column_name=real_col, tag_key=key,
                                     tag_value=value, actor=principal.username, source="propagated")
                        applied.append({"object": ".".join(dest + (real_col,)), "tag": key, "value": value})
                    except ValueError as exc:            # e.g. the 'sensitivity' definition was deleted
                        logger.debug(f"Tag propagation skipped for {dest}.{real_col}: {exc}")
        return applied
    except Exception as exc:
        logger.warning(f"Tag propagation failed: {exc}")
        return []
