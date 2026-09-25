"""Table- and schema-level data access, granted to users and groups (web/groups.py `resource_grants`, types `table` and `schema`).

Catalog-level ACLs (web/permissions.py) stay the coarse switch: READ / WRITE / ADMIN on a whole catalog. A table or schema grant is an
*allow-list entry* for someone with no access to the catalog as a whole: SELECT lets them read the table (or every table in the schema,
present and future), MODIFY also write. Grants only ever ADD access; they never take away what a catalog grant gives. The default
`warehouse` catalog is public to every user (existing behaviour), so table grants only matter in the other catalogs.

The SQL path is the risky one, so it is deliberately strict (`sql_covered`): the statement must be ONE plain query, and every reference to a
catalog the user cannot read must be a fully qualified `catalog.schema.table` (as a table or as a column qualifier) that a grant covers.
The number of such references found by the parser is compared with the number of `catalog.` occurrences the tokenizer sees (string literals
and comments excluded); any difference (a function call, an ATTACH, a two-part name, something the parser did not understand) means "cannot
verify" and the statement is refused. When in doubt: refuse.
"""
import logging
from typing import Any, Dict, List, Optional, Set

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType

from web import groups

logger = logging.getLogger("localspark.table_access")

_NEED = {"READ": "SELECT", "WRITE": "MODIFY"}


def _norm(x: Any) -> str:
    return str(x or "").strip().lower()


def _level(user: Dict[str, Any], catalog: str, schema: str, table: Optional[str], tmap: Dict[str, str], smap: Dict[str, str]) -> Optional[str]:
    """Highest of the user's table grant and schema grant, given their grant maps."""
    ladder = groups.RESOURCE_TYPES["table"]
    best = -1
    for level in (tmap.get(f"{catalog}.{schema}.{table}") if table else None, smap.get(f"{catalog}.{schema}")):
        if level in ladder:
            best = max(best, ladder.index(level))
    return ladder[best] if best >= 0 else None


def can_access_table(user: Dict[str, Any], catalog: str, schema: str, table: str, action: str = "READ") -> bool:
    """Catalog-level access, or a table/schema grant of at least the needed level."""
    from web.permissions import can_user_access_catalog
    if can_user_access_catalog(user, catalog, action=action):
        return True
    need = _NEED.get(action.upper())
    if not need:
        return False
    catalog, schema, table = _norm(catalog), _norm(schema), _norm(table)
    level = _level(user, catalog, schema, table, groups.granted_map(user, "table"), groups.granted_map(user, "schema"))
    ladder = groups.RESOURCE_TYPES["table"]
    return level is not None and ladder.index(level) >= ladder.index(need)


def granted_scope(user: Dict[str, Any]) -> Dict[str, Dict[str, Set]]:
    """{catalog: {'schemas': {schema, ...}, 'tables': {(schema, table), ...}}} the user holds at least SELECT on through table/schema grants."""
    out: Dict[str, Dict[str, Set]] = {}
    for rid in groups.granted_map(user, "schema"):
        cat, sch = rid.split(".")
        out.setdefault(cat, {"schemas": set(), "tables": set()})["schemas"].add(sch)
    for rid in groups.granted_map(user, "table"):
        cat, sch, tbl = rid.split(".")
        out.setdefault(cat, {"schemas": set(), "tables": set()})["tables"].add((sch, tbl))
    return out


def prune_catalog(cat: Dict[str, Any], scope: Dict[str, Set]) -> Dict[str, Any]:
    """A copy of a catalog tree holding only the schemas/tables the scope allows (for a user with no catalog-level access)."""
    pruned = dict(cat)
    schemas = []
    for s in cat.get("schemas", []):
        name = _norm(s.get("name"))
        tables = [t for t in s.get("tables", []) if name in scope["schemas"] or (name, _norm(t.get("name"))) in scope["tables"]]
        if tables or name in scope["schemas"]:
            schemas.append({**s, "tables": tables, "models": []})
    pruned["schemas"] = schemas
    pruned["table_count"] = sum(len(s["tables"]) for s in schemas)
    pruned["model_count"] = 0
    return pruned


# ---------------------------------------------------------------- SQL

def _catalog_token_count(sql: str, catalog: str) -> int:
    """How often `catalog` appears as the first part of a dotted name, seen by the tokenizer (strings and comments are not identifiers)."""
    toks = sqlglot.tokenize(sql, read="duckdb")
    n = 0
    for i in range(len(toks) - 1):
        t = toks[i]
        if t.token_type == TokenType.STRING or t.text.lower() != catalog or toks[i + 1].token_type != TokenType.DOT:
            continue
        if i > 0 and toks[i - 1].token_type == TokenType.DOT:
            continue                                   # `x.catalog.y`: a schema or column part of another name, not a catalog
        n += 1
    return n


def sql_covered(sql: str, user: Dict[str, Any], denied_catalogs: List[str]) -> bool:
    """True only if the statement is one plain query and every reference to a catalog in `denied_catalogs` is a fully qualified table the
    user holds SELECT on (see the module docstring). Anything the parser does not fully account for is False."""
    try:
        trees = [t for t in sqlglot.parse(sql, read="duckdb") if t is not None]
        if len(trees) != 1 or not isinstance(trees[0], exp.Query):
            return False
        tree = trees[0]
        tmap, smap = groups.granted_map(user, "table"), groups.granted_map(user, "schema")
        if not tmap and not smap:
            return False
        ladder = groups.RESOURCE_TYPES["table"]

        def allowed(cat: str, sch: str, tbl: str) -> bool:
            level = _level(user, cat, sch, tbl, tmap, smap)
            return level is not None and ladder.index(level) >= ladder.index("SELECT")

        for cat in {_norm(c) for c in denied_catalogs}:
            found = 0
            for node in tree.find_all(exp.Table):
                c, d = node.args.get("catalog"), node.args.get("db")
                if c is not None and _norm(c.name) == cat:
                    if not isinstance(node.this, exp.Identifier) or d is None or not allowed(cat, _norm(d.name), _norm(node.name)):
                        return False
                    found += 1
                elif c is None and d is not None and _norm(d.name) == cat:
                    return False                       # `catalog.table` (two parts): ambiguous, must be catalog.schema.table
            for node in tree.find_all(exp.Column):
                c = node.args.get("catalog")
                if c is not None and _norm(c.name) == cat:
                    d, t = node.args.get("db"), node.args.get("table")
                    if d is None or t is None or not allowed(cat, _norm(d.name), _norm(t.name)):
                        return False
                    found += 1
            if found == 0 or found != _catalog_token_count(sql, cat):
                return False
        return True
    except Exception as exc:
        logger.info(f"table-level SQL check could not verify the statement: {exc}")
        return False
