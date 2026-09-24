"""
Governance for dbt output: closed by default, opened deliberately, and never less governed than its sources.

dbt writes its `table` / `incremental` models as Delta tables into the lakehouse (`<warehouse>/<schema>/<model>`, the duckrun
adapter's `root_path`). That output is derived from governed tables and is produced by a run that reads raw data, so it must
not simply appear to every user. Three rules, all built on the existing governance engine (tags + a row filter policy):

  1. Closed by default. Before a run, every schema dbt writes to gets the tag `access=closed` (unless it already has an
     `access` tag), and one row filter policy, "dbt output closed by default", denies all rows of anything tagged
     `access=closed` to everyone but the exempt roles (admin and power_user; edit the policy in Governance to change that).
     A schema tag is inherited by every table below it, so a model that dbt creates is closed the instant it exists: there is
     no window and nothing to remember per model. Users see the table's structure but no rows.
  2. Deliberate opening. `open_model` sets `access=open` on one table (an admin's explicit act, audited); `close_model` puts it
     back. Opening a table does not lift masking: rule 3 keeps applying.
  3. Derived data inherits its sources' tags. After a run, each output column gets the tags of every source column it derives
     from (sqlglot column lineage through the whole model DAG, so renames, aggregates and CTEs are followed), and the table gets
     its sources' table-level tags (row filters follow). A tagged column can therefore never come out untagged once a table is
     opened. Lineage that cannot be resolved for a column falls back to tagging it with every tag of every source it might use.

Caveat that is inherent to how governance works here: a policy that applies to a role makes those users "governed" (their
notebooks run in the sandbox, file functions are blocked). Closing dbt output for the `user` role therefore also governs them.
Set DBT_CLOSED_BY_DEFAULT=false to switch this whole mechanism off.
"""

import glob
import json
import logging
import os
import re
import subprocess
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("localspark.dbt_governance")

TAG_KEY = "access"
CLOSED, OPEN = "closed", "open"
POLICY_NAME = "dbt output closed by default"
SOURCE_CATALOG = "warehouse"                   # dbt sources read the local lakehouse (external_location: delta_scan(...))
PROPAGATED_SOURCE = "dbt"                      # `source` of tag assignments this module derives (and may refresh / remove)
_MISSING_COLUMN = "__dbt_output_closed__"      # never exists, so the row filter fails closed (1 = 0) on every table it matches
OUTPUT_MATERIALIZATIONS = ("table", "incremental", "delta")


def enabled() -> bool:
    return os.getenv("DBT_CLOSED_BY_DEFAULT", "true").strip().lower() not in ("0", "false", "no", "off")


def _env() -> Dict[str, str]:
    from web.dbt_config import dbt_env
    return dbt_env()


def _project_dir() -> str:
    return os.getenv("DBT_PROJECT_DIR", "/workspace/dbt_project")


def _warehouse_dir() -> str:
    return os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")


# ---------------------------------------------------------------- where dbt lands its tables

def target() -> Dict[str, Any]:
    """Where the profile's `root_path` points and which catalog that is: the local warehouse ('warehouse'), an S3 mount's bucket (the
    mount's catalog) or somewhere no catalog reads (catalog None: nobody can read those tables through a governed path)."""
    from web.dbt_service import _profile_output
    root = str(_profile_output().get("root_path") or "").strip()
    if root.lower().startswith("s3://"):
        from web.autoloader_s3 import parse_s3_path
        from web.dbt_config import s3_mounts
        from web.mounts import get_s3_storage_options
        bucket, prefix = parse_s3_path(root)
        mount = next((m for m in s3_mounts() if (m.get("config") or {}).get("bucket") == bucket), None)
        if not mount:
            return {"kind": "s3", "catalog": None, "root": root, "note": f"no S3 storage mount is configured for bucket '{bucket}', so the "
                                                                          "tables are not visible in any catalog"}
        return {"kind": "s3", "catalog": mount.get("catalog_name") or mount["id"], "root": f"s3://{bucket}/{prefix}".rstrip("/"),
                "storage_options": get_s3_storage_options(mount.get("config") or {})}
    if not root or os.path.abspath(root) == os.path.abspath(_warehouse_dir()):
        return {"kind": "local", "catalog": "warehouse", "root": _warehouse_dir()}
    return {"kind": "local", "catalog": None, "root": root, "note": "the profile's root_path is outside the warehouse, so the tables are not in any catalog"}


def _dest_catalog() -> Optional[str]:
    return target()["catalog"]


def _delta_table(schema: str, alias: str):
    """The output table as a DeltaTable (local or S3), or None if it has not been built."""
    from deltalake import DeltaTable
    t = target()
    try:
        if t["kind"] == "s3":
            return DeltaTable(f'{t["root"]}/{schema}/{alias}', storage_options=t.get("storage_options"))
        path = os.path.join(t["root"], schema, alias)
        return DeltaTable(path) if os.path.isdir(os.path.join(path, "_delta_log")) else None
    except Exception:
        return None


# ---------------------------------------------------------------- what dbt will build

def list_models() -> List[Dict[str, Any]]:
    """Every model dbt knows (`dbt ls`): name, alias, schema, materialization, upstream node ids, compiled-file location."""
    r = subprocess.run(["dbt", "ls", "--resource-type", "model", "--output", "json", "--output-keys",
                        "name alias schema database config depends_on original_file_path package_name", "--profiles-dir", "."],
                       cwd=_project_dir(), capture_output=True, text=True, timeout=90, env=_env())
    models = []
    for line in (r.stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            n = json.loads(line)
        except ValueError:
            continue
        models.append({"name": n["name"], "alias": n.get("alias") or n["name"], "schema": n["schema"], "database": n.get("database"),
                       "materialized": (n.get("config") or {}).get("materialized"), "package": n.get("package_name"),
                       "file": n.get("original_file_path"), "depends_on": (n.get("depends_on") or {}).get("nodes", [])})
    if r.returncode != 0 and not models:
        raise RuntimeError(f"dbt ls failed: {(r.stdout + r.stderr)[-300:]}")
    return models


def output_schemas(models: List[Dict[str, Any]]) -> Set[str]:
    return {m["schema"] for m in models if m["materialized"] in OUTPUT_MATERIALIZATIONS and m["schema"]}


# ---------------------------------------------------------------- rule 1: closed by default

def ensure_closed_by_default(schemas: Set[str], actor: str = "dbt") -> Dict[str, Any]:
    """Idempotently sets up the `access` tag, the deny-all row policy and `access=closed` on each schema that has no `access`
    tag yet (an explicit `open` on a schema is respected, never overwritten)."""
    from web.governance import row_filters, store, tags
    store.init_governance_db()
    done: Dict[str, Any] = {"schemas_closed": [], "policy_created": False, "tag_created": False}
    catalog = _dest_catalog()
    if catalog is None:
        return {**done, "skipped": target().get("note", "the output is not in a catalog")}
    done["catalog"] = catalog
    try:
        tags.get_definition(TAG_KEY)
    except tags.NotFound:
        tags.create_definition(TAG_KEY, "Who may read a table's rows: `closed` denies everyone except the exempt roles; `open` lifts it. "
                               "dbt output starts closed.", [CLOSED, OPEN], actor=actor)
        done["tag_created"] = True
    if not any(p["name"] == POLICY_NAME for p in row_filters.list_row_policies()):
        row_filters.create_row_policy({
            "name": POLICY_NAME, "tag_key": TAG_KEY, "tag_value": CLOSED, "filter_mode": "custom",
            "description": "dbt tables are closed until an administrator opens them (tag access=open). Deliberately fails closed: "
                           "its filter column never exists, so no row is visible to the governed roles.",
            "filter_column": _MISSING_COLUMN, "filter_expr": "{col} IS NULL", "except_roles": ["admin", "power_user"], "priority": 10}, actor=actor)
        done["policy_created"] = True
    for schema in sorted(schemas):
        if TAG_KEY not in tags.effective_table_tags(catalog, schema, "_"):
            tags.set_tag(catalog=catalog, schema_name=schema, tag_key=TAG_KEY, tag_value=CLOSED, actor=actor, source=PROPAGATED_SOURCE)
            done["schemas_closed"].append(schema)
    return done


def access_state(schema: str, table: str) -> Dict[str, Any]:
    """{'access': 'closed'|'open'|None, 'level': 'schema'|'table'|...} for one output table."""
    from web.governance import tags
    catalog = _dest_catalog()
    if catalog is None:
        return {"access": None, "level": None}
    info = tags.effective_table_tags(catalog, schema, table).get(TAG_KEY)
    return {"access": info["value"] if info else None, "level": info["level"] if info else None}


# ---------------------------------------------------------------- rule 3: derived data inherits its sources' tags

def _compiled_sql(model: Dict[str, Any]) -> Optional[str]:
    hits = glob.glob(os.path.join(_project_dir(), "target", "compiled", model["package"] or "*", model["file"] or "_"))
    return open(hits[0], encoding="utf-8").read() if hits else None


def _normalise(sql: str, models: List[Dict[str, Any]]) -> str:
    """Compiled dbt SQL -> SQL sqlglot can follow: model relations become their bare alias, `delta_scan('<warehouse>/s/t')`
    (how dbt sources read Delta) becomes the catalog table `warehouse.s.t`."""
    for m in models:
        if m["database"]:
            sql = sql.replace(f'"{m["database"]}"."{m["schema"]}"."{m["alias"]}"', m["alias"])
    root = _warehouse_dir().rstrip("/") + "/"

    def scan(match):
        path = match.group(1)
        if path.startswith(root):
            parts = path[len(root):].strip("/").split("/")
            if len(parts) == 2:
                return f"{SOURCE_CATALOG}.{parts[0]}.{parts[1]}"
        return match.group(0)
    return re.sub(r"delta_scan\(\s*'([^']+)'\s*\)", scan, sql)


def _leaf_sources(column: str, sql: str, upstream: Dict[str, str]) -> Optional[List[Tuple[str, str, str, str]]]:
    """[(catalog, schema, table, column)] of the lakehouse columns `column` derives from, or None when lineage cannot say."""
    from sqlglot import exp
    from sqlglot.lineage import lineage
    try:
        node = lineage(column, sql, dialect="duckdb", sources=upstream)
    except Exception as exc:
        logger.debug(f"lineage of {column} failed: {exc}")
        return None
    out = []
    for n in node.walk():
        if n.downstream:
            continue
        src = n.source
        if isinstance(src, exp.Table) and src.db:
            out.append((src.catalog or SOURCE_CATALOG, src.db, src.name, n.name.split(".")[-1]))
    return out


def propagate_tags(models: List[Dict[str, Any]], actor: str = "dbt") -> Dict[str, Any]:
    """Copies source tags onto every output table (see rule 3). Returns per-table counts and any columns handled by the
    conservative fallback."""
    from web.governance import tags
    dest = _dest_catalog()
    if dest is None:
        return {"tables": {}, "fallback_columns": [], "skipped": [m["alias"] for m in models], "note": target().get("note")}
    sql_by_alias = {}
    for m in models:
        raw = _compiled_sql(m)
        if raw:
            sql_by_alias[m["alias"]] = _normalise(raw, models)
    result: Dict[str, Any] = {"tables": {}, "fallback_columns": [], "skipped": []}
    for m in models:
        if m["materialized"] not in OUTPUT_MATERIALIZATIONS:
            continue
        table = _delta_table(m["schema"], m["alias"])
        if table is None or m["alias"] not in sql_by_alias:
            result["skipped"].append(m["alias"])
            continue
        columns = [f.name for f in table.schema().fields]
        upstream = {a: s for a, s in sql_by_alias.items() if a != m["alias"]}
        # refresh what this module derived earlier: an upstream tag that was removed must not linger on the output
        for a in tags.list_assignments(catalog=dest, limit=5000):
            if a["schema_name"] == m["schema"] and a["table_name"] == m["alias"] and a.get("source") == PROPAGATED_SOURCE:
                tags.unset_tag(catalog=dest, schema_name=m["schema"], table_name=m["alias"], column_name=a.get("column_name") or "",
                               tag_key=a["tag_key"], actor=actor)
        col_tags: Dict[str, Dict[str, str]] = {}
        table_tags: Dict[str, str] = {}
        all_leaves: List[Tuple[str, str, str, str]] = []
        for col in columns:
            leaves = _leaf_sources(col, sql_by_alias[m["alias"]], upstream)
            if leaves is None:                                            # cannot say: assume it may use any upstream column
                result["fallback_columns"].append(f'{m["alias"]}.{col}')
                leaves = [(c, s, t, f.name) for c, s, t in _base_tables(sql_by_alias[m["alias"]], upstream)
                          for f in _table_columns(c, s, t)]
            all_leaves.extend(leaves)
            for cat, sch, tbl, src_col in leaves:
                for key, info in tags.effective_tags(cat, sch, tbl, [src_col]).get(src_col, {}).items():
                    if key != TAG_KEY:
                        col_tags.setdefault(col, {})[key] = info["value"]
        for cat, sch, tbl, _c in set(all_leaves):
            for key, info in tags.effective_table_tags(cat, sch, tbl).items():
                if key != TAG_KEY:
                    table_tags[key] = info["value"]
        n = 0
        for key, value in table_tags.items():
            n += _set(m, "", key, value, actor)
        for col, kv in col_tags.items():
            for key, value in kv.items():
                n += _set(m, col, key, value, actor)
        result["tables"][f'{m["schema"]}.{m["alias"]}'] = {"tags_set": n, "tagged_columns": sorted(col_tags)}
    return result


def _set(m: Dict[str, Any], column: str, key: str, value: str, actor: str) -> int:
    from web.governance import tags
    try:
        tags.set_tag(catalog=_dest_catalog(), schema_name=m["schema"], table_name=m["alias"], column_name=column, tag_key=key, tag_value=value,
                     actor=actor, source=PROPAGATED_SOURCE)
        return 1
    except ValueError as exc:                                              # e.g. a value the tag's allowed list rejects
        logger.warning(f"Could not carry tag {key}={value} to {m['schema']}.{m['alias']}.{column or '*'}: {exc}")
        return 0


def _base_tables(sql: str, upstream: Dict[str, str]) -> Set[Tuple[str, str, str]]:
    """Every lakehouse table (catalog, schema, table) a model's SQL reads, following upstream model SQL (fallback path)."""
    import sqlglot
    from sqlglot import exp
    seen, out, todo = set(), set(), [sql]
    while todo:
        try:
            tree = sqlglot.parse_one(todo.pop(), dialect="duckdb")
        except Exception:
            continue
        for t in tree.find_all(exp.Table):
            if t.db:
                out.add((t.catalog or SOURCE_CATALOG, t.db, t.name))
            elif t.name in upstream and t.name not in seen:
                seen.add(t.name)
                todo.append(upstream[t.name])
    return out


def _table_columns(cat: str, schema: str, table: str):
    from deltalake import DeltaTable
    try:
        return DeltaTable(os.path.join(_warehouse_dir(), schema, table)).schema().fields if cat == SOURCE_CATALOG else []
    except Exception:
        return []


# ---------------------------------------------------------------- rule 2: deliberate opening

def _find_output(model_name: str, models: List[Dict[str, Any]]) -> Dict[str, Any]:
    m = next((x for x in models if x["name"] == model_name or x["alias"] == model_name), None)
    if not m:
        raise LookupError(f"Unknown dbt model '{model_name}'.")
    if m["materialized"] not in OUTPUT_MATERIALIZATIONS:
        raise ValueError(f"'{model_name}' is a {m['materialized']} model: only table / incremental models are written to the lakehouse.")
    return m


def open_model(model_name: str, actor: str) -> Dict[str, Any]:
    """An administrator's explicit decision to let users read one dbt table. Refreshes the carried-over tags first, so what is
    opened is exactly as governed as its sources are right now."""
    from web.governance import tags
    models = list_models()
    m = _find_output(model_name, models)
    catalog = _dest_catalog()
    if catalog is None:
        raise ValueError("dbt's output is not in a catalog, so there is nothing to open: " + target().get("note", ""))
    if _delta_table(m["schema"], m["alias"]) is None:
        raise ValueError(f"'{model_name}' has not been built yet (run dbt first).")
    ensure_closed_by_default({m["schema"]}, actor=actor)
    carried = propagate_tags(models, actor=actor)
    info = carried["tables"].get(f'{m["schema"]}.{m["alias"]}', {"tags_set": 0, "tagged_columns": []})
    tags.set_tag(catalog=catalog, schema_name=m["schema"], table_name=m["alias"], tag_key=TAG_KEY, tag_value=OPEN, actor=actor, source="manual")
    return {"table": f'{catalog}.{m["schema"]}.{m["alias"]}', "access": OPEN, "carried_tags": info["tags_set"],
            "columns_with_source_tags": info["tagged_columns"], "fallback_columns": [c for c in carried["fallback_columns"] if c.startswith(m["alias"] + ".")]}


def close_model(model_name: str, actor: str) -> Dict[str, Any]:
    from web.governance import tags
    m = _find_output(model_name, list_models())
    catalog = _dest_catalog()
    if catalog is None:
        raise ValueError("dbt's output is not in a catalog: " + target().get("note", ""))
    ensure_closed_by_default({m["schema"]}, actor=actor)
    tags.set_tag(catalog=catalog, schema_name=m["schema"], table_name=m["alias"], tag_key=TAG_KEY, tag_value=CLOSED, actor=actor, source="manual")
    return {"table": f'{catalog}.{m["schema"]}.{m["alias"]}', "access": CLOSED}


# ---------------------------------------------------------------- around a dbt run

def before_run(actor: str = "dbt") -> Dict[str, Any]:
    """Closes the schemas dbt is about to write to (no window: the tag exists before the first table does)."""
    if not enabled():
        return {"enabled": False}
    try:
        models = list_models()
        return {"enabled": True, **ensure_closed_by_default(output_schemas(models), actor=actor)}
    except Exception as exc:
        logger.error(f"dbt closed-by-default setup failed: {exc}", exc_info=True)
        return {"enabled": True, "error": str(exc)}


def after_run(actor: str = "dbt") -> Dict[str, Any]:
    """Makes the new tables visible to the studio, then carries source tags onto them."""
    if not enabled():
        return {"enabled": False}
    try:
        try:
            from web.app import get_duckrun_conn
            get_duckrun_conn().refresh()
        except Exception as exc:
            logger.debug(f"could not refresh the duckrun session after dbt: {exc}")
        return {"enabled": True, **propagate_tags(list_models(), actor=actor)}
    except Exception as exc:
        logger.error(f"dbt tag propagation failed: {exc}", exc_info=True)
        return {"enabled": True, "error": str(exc)}
