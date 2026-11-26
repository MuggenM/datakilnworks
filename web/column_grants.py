"""Column-level SQL GRANT / REVOKE (`web/sql_grants.py`), built on the existing masking machinery rather than a new
store: `GRANT SELECT (col1, col2) ON TABLE catalog.schema.table TO principal` tags each named column with a private,
per-column tag (`sql_grant_column`, value = the column's own fully-qualified name, so the tag can only ever match that
one column anywhere in the warehouse) and gets-or-creates ONE masking policy scoped to exactly that tag (mask_type
"null": the same expression for every column type, so it can never leak from a type mismatch), then adds the principal
to the policy's exempt list. REVOKE removes them from it. Everyone else already sees NULL for that column the moment
the policy exists, through the normal query-time masking path (`web/governance/enforce.py` -> `policies.masks_for_table`);
no new query-rewriting code is needed here.

This means: the *first* column-level grant on a column switches it from "open to whoever can read the table" to
"masked for everyone except admins and whoever is explicitly granted" -- there is no bare "make this column public
again" operation, matching how every other grant in this app only ever ADDS access (see web/table_access.py). REVOKE
therefore never deletes the policy, even when its exempt list becomes empty: an admin who wants the column fully open
again removes the masking policy directly (Governance > Masking).

Kept deliberately simple: SELECT only (no column-level MODIFY -- writing is all-or-nothing on the table), and
WITH GRANT OPTION does not apply here (see web/sql_grants.py for why).
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("localspark.column_grants")

TAG_KEY = "sql_grant_column"
MAX_TAG_VALUE = 128


class ColumnGrantError(ValueError):
    """A problem with a column-level grant; the message is safe to show."""


def _tag_value(catalog: str, schema_name: str, table_name: str, column: str) -> str:
    value = f"{catalog}.{schema_name}.{table_name}.{column}"
    if len(value) > MAX_TAG_VALUE:
        raise ColumnGrantError(f"'{value}' is too long for a column-level grant (identifiers must fit in {MAX_TAG_VALUE} characters together).")
    return value


def _ensure_tag_definition(actor: str) -> None:
    from web.governance import tags
    try:
        tags.get_definition(TAG_KEY)
    except tags.NotFound:
        try:
            tags.create_definition(TAG_KEY, "Internal: marks a column governed by a SQL-level column grant (web/sql_grants.py). "
                                             "Not meant to be assigned by hand.", actor=actor)
        except ValueError:
            pass                                       # a concurrent request just created it; fine


def column_exists(catalog: str, schema_name: str, table_name: str, column: str) -> bool:
    """Typo protection, mirroring web/sql_grants.py's `_exists` for tables: local catalogs are checked against the
    Delta schema on disk, a mounted catalog is trusted (its own engine enforces existence at read time)."""
    from web.warehouses import get_catalog
    cat = get_catalog(catalog)
    if not cat:
        return False
    if cat.get("is_mounted"):
        return True
    import os
    base = cat.get("path") or ""
    table_path = os.path.join(base, schema_name, table_name)
    if not os.path.isdir(os.path.join(table_path, "_delta_log")):
        return False
    try:
        from deltalake import DeltaTable
        names = {f.name.lower() for f in DeltaTable(table_path).schema().fields}
    except Exception as exc:
        logger.warning(f"Could not read the schema of {catalog}.{schema_name}.{table_name} to check column '{column}': {exc}")
        return True                                    # a schema we cannot read is not evidence the column is missing
    return column.lower() in names


def _find_policy(tag_value: str) -> Optional[Dict[str, Any]]:
    from web.governance import policies
    return next((p for p in policies.list_policies() if p["tag_key"] == TAG_KEY and p["tag_value"] == tag_value), None)


def _get_or_create_policy(catalog: str, schema_name: str, table_name: str, column: str, tag_value: str, actor: str) -> Dict[str, Any]:
    from web.governance import policies
    existing = _find_policy(tag_value)
    if existing:
        return existing
    return policies.create_policy({
        "name": f"Column grant - {catalog}.{schema_name}.{table_name}.{column}",
        "description": "Created by a SQL GRANT/REVOKE statement on this column; edit its exempt list here, or drop it to reopen the column to everyone.",
        "tag_key": TAG_KEY, "tag_value": tag_value, "mask_type": "null",
        "except_roles": ["admin"], "except_users": [], "except_groups": [], "priority": 100, "enabled": True,
    }, actor=actor)


def grant_column(catalog: str, schema_name: str, table_name: str, column: str,
                 principal_kind: str, principal_id: str, principal_label: str, actor: str) -> str:
    """Adds the principal to the column's masking-policy exempt list (creating the tag/policy on first use). Returns a message."""
    from web.governance import policies as gov_policies
    if not column_exists(catalog, schema_name, table_name, column):
        raise ColumnGrantError(f"Column '{column}' does not exist on table {catalog}.{schema_name}.{table_name}.")
    from web.governance import tags
    tag_value = _tag_value(catalog, schema_name, table_name, column)
    _ensure_tag_definition(actor)
    tags.set_tag(catalog=catalog, schema_name=schema_name, table_name=table_name, column_name=column,
                tag_key=TAG_KEY, tag_value=tag_value, actor=actor, source="sql_grant")
    pol = _get_or_create_policy(catalog, schema_name, table_name, column, tag_value, actor)
    key = "except_groups" if principal_kind == "group" else "except_users"
    current = list(pol[key])
    label = principal_id if principal_kind == "group" else principal_label
    if label in current:
        return f"{principal_kind} {principal_label} already holds SELECT on column {catalog}.{schema_name}.{table_name}.{column}."
    gov_policies.update_policy(pol["id"], {key: current + [label]}, actor=actor)
    return f"Granted SELECT on column {catalog}.{schema_name}.{table_name}.{column} to {principal_kind} {principal_label}."


def revoke_column(catalog: str, schema_name: str, table_name: str, column: str,
                  principal_kind: str, principal_id: str, principal_label: str, actor: str) -> str:
    """Removes the principal from the column's masking-policy exempt list, if a column grant exists at all. The policy
    (and its NULL mask for everyone else) is never removed by a REVOKE; see the module docstring."""
    from web.governance import policies as gov_policies
    tag_value = _tag_value(catalog, schema_name, table_name, column)
    pol = _find_policy(tag_value)
    who = f"{principal_kind} {principal_label}"
    if not pol:
        return f"{who} does not hold an explicit SELECT grant on column {catalog}.{schema_name}.{table_name}.{column}."
    key = "except_groups" if principal_kind == "group" else "except_users"
    label = principal_id if principal_kind == "group" else principal_label
    current = list(pol[key])
    if label not in current:
        return f"{who} does not hold an explicit SELECT grant on column {catalog}.{schema_name}.{table_name}.{column}."
    gov_policies.update_policy(pol["id"], {key: [x for x in current if x != label]}, actor=actor)
    return f"Revoked SELECT on column {catalog}.{schema_name}.{table_name}.{column} from {who}."


def list_column_grants_for_principal(principal_kind: str, principal_id: str, principal_label: str) -> List[Dict[str, Any]]:
    """Every column grant the principal holds, across every table, for SHOW GRANTS with no table target."""
    from web.governance import policies as gov_policies
    label = principal_id if principal_kind == "group" else principal_label
    key = "except_groups" if principal_kind == "group" else "except_users"
    out: List[Dict[str, Any]] = []
    for pol in gov_policies.list_policies():
        if pol["tag_key"] != TAG_KEY or label not in pol[key]:
            continue
        catalog, schema_name, table_name, column = pol["tag_value"].split(".", 3)
        out.append({"catalog": catalog, "schema": schema_name, "table": table_name, "column": column,
                    "granted_by": pol["created_by"], "created_at": pol["updated_at"]})
    return out


def list_column_grants(catalog: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
    """One row per (column, principal) currently exempt, for SHOW GRANTS ON TABLE ... For the whole table's prefix,
    since a masking policy's tag_value is the column's own fully-qualified name."""
    from web.governance import policies as gov_policies
    from web import groups
    prefix = f"{catalog}.{schema_name}.{table_name}."
    gnames = {g["id"]: g["name"] for g in groups.list_groups()}
    out: List[Dict[str, Any]] = []
    for pol in gov_policies.list_policies():
        if pol["tag_key"] != TAG_KEY or not pol["tag_value"].startswith(prefix):
            continue
        column = pol["tag_value"][len(prefix):]
        for u in pol["except_users"]:
            out.append({"column": column, "principal_type": "user", "name": u, "granted_by": pol["created_by"], "created_at": pol["updated_at"]})
        for gid in pol["except_groups"]:
            out.append({"column": column, "principal_type": "group", "name": gnames.get(gid, gid), "granted_by": pol["created_by"], "created_at": pol["updated_at"]})
    return out
