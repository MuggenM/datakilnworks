"""Expands table names in a SQL text to the full `catalog.schema.table` form.

Works on the *text*: every name is completed by inserting the missing prefix at the identifier's own position, so the author's
formatting, comments and casing are left alone (a reformat through sqlglot would rewrite the whole statement). A name is only
completed when it resolves to exactly one table; anything unknown or ambiguous is reported and left as it is. CTE names, table
functions (`read_parquet(...)`) and names that are already three-part are never touched.

It only looks at table *names* in the catalogs' directory layout (no table is opened), and it never executes anything.
"""
import os
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp


def _tables_by_catalog() -> Dict[str, List[Tuple[str, str]]]:
    """{catalog_id: [(schema, table), ...]} from the Delta directories of every registered catalog."""
    from web.warehouses import load_catalogs
    out: Dict[str, List[Tuple[str, str]]] = {}
    for cat in load_catalogs():
        found: List[Tuple[str, str]] = []
        base = cat.get("path")
        if base and os.path.isdir(base):
            for root, dirs, _files in os.walk(base):
                dirs[:] = [d for d in dirs if not d.startswith(".")]
                rel = os.path.relpath(root, base).split(os.sep)
                if cat["id"] == "warehouse" and rel[0] == "catalogs":
                    dirs[:] = []
                    continue
                if "_delta_log" in dirs:
                    found.append((rel[0], rel[1]) if len(rel) >= 2 else ("dbo", rel[0]))
                    dirs[:] = []                       # a table's own subfolders are not tables
        out[cat["id"]] = found
    return out


def _span(node: exp.Expression) -> Optional[Tuple[int, int]]:
    m = getattr(node, "meta", None) or {}
    return (m["start"], m["end"]) if "start" in m and "end" in m else None


def qualify(sql: str, catalog: Optional[str] = None, known: Optional[Dict[str, List[Tuple[str, str]]]] = None) -> Dict[str, Any]:
    """Returns {sql, changes: [{from, to}], unresolved: [{name, reason}]}. Raises ValueError if the SQL cannot be parsed."""
    try:
        trees = [t for t in sqlglot.parse(sql, dialect="duckdb") if t is not None]
    except sqlglot.errors.SqlglotError as exc:
        raise ValueError(f"The SQL could not be parsed: {str(exc).splitlines()[0]}")
    known = known if known is not None else _tables_by_catalog()
    lower = {c: [(s.lower(), t.lower(), s, t) for s, t in tabs] for c, tabs in known.items()}
    hint = catalog if catalog in known else ("warehouse" if "warehouse" in known else None)

    def locate(schema: Optional[str], table: str) -> List[Tuple[str, str, str]]:
        """Candidates (catalog, schema, table), the hinted catalog first: the first catalog that has any wins."""
        order = ([hint] if hint else []) + [c for c in known if c != hint]
        for cat in order:
            hits = [(cat, s, t) for sl, tl, s, t in lower.get(cat, []) if tl == table.lower() and (schema is None or sl == schema.lower())]
            if hits:
                return hits
        return []

    edits: List[Tuple[int, str]] = []
    changes: List[Dict[str, str]] = []
    unresolved: List[Dict[str, str]] = []

    for tree in trees:
        ctes = {c.alias.lower() for c in tree.find_all(exp.CTE) if c.alias}
        for t in tree.find_all(exp.Table):
            ident = t.this
            if not isinstance(ident, exp.Identifier) or t.args.get("catalog"):
                continue                               # a table function, or already fully qualified
            name, db = t.name, t.args.get("db")
            if db is None and name.lower() in ctes:
                continue
            span = _span(db if db is not None else ident)
            if span is None:
                continue
            hits = locate(db.name if db is not None else None, name)
            label = f"{db.name}.{name}" if db is not None else name
            if len(set(hits)) == 1:
                cat, s, tbl = hits[0]
                prefix = f"{cat}." if db is not None else f"{cat}.{s}."
                edits.append((span[0], prefix))
                changes.append({"from": label, "to": f"{cat}.{s}.{tbl}"})
            elif not hits:
                unresolved.append({"name": label, "reason": "no table with this name in any catalog"})
            else:
                unresolved.append({"name": label, "reason": "ambiguous: " + ", ".join(sorted({f'{c}.{s}.{x}' for c, s, x in hits}))})

    out = sql
    for pos, prefix in sorted(set(edits), reverse=True):
        out = out[:pos] + prefix + out[pos:]
    # the same name can appear several times in one statement: report each once
    return {"sql": out,
            "changes": list({(c["from"], c["to"]): c for c in changes}.values()),
            "unresolved": list({u["name"]: u for u in unresolved}.values())}
