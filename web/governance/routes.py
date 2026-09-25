"""
REST API for governance under /api/governance.

Roles: tag definitions are admin-only. Assigning tags needs management rights on the catalog
(web.permissions.can_user_manage_catalog); reading tags needs READ on the catalog.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from web.auth import resolve_principal, get_user_by_username
from web.governance import catalog_meta, classify, gateway, macros, masks, policies, row_filters, store, tags
from web.permissions import can_user_access_catalog, can_user_manage_catalog

logger = logging.getLogger("localspark.governance")

router = APIRouter(prefix="/api/governance", tags=["governance"])

def set_connection_provider(provider) -> None:
    """app.py injects a callable returning a DuckDB cursor (avoids importing app from here)."""
    gateway.set_connection_provider(provider)


def _con():
    try:
        return gateway._cursor()
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Governance is not connected to the query engine.")


async def principal(request: Request) -> Dict[str, Any]:
    return await resolve_principal(request)


def _require_admin(user: Dict[str, Any]) -> None:
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only admins can do this.")


def _require_manage(user: Dict[str, Any], catalog: str) -> None:
    if not can_user_manage_catalog(user, tags.norm(catalog)):
        raise HTTPException(status_code=403, detail=f"You cannot manage tags in catalog '{catalog}'.")


def _require_read(user: Dict[str, Any], catalog: str) -> None:
    if not can_user_access_catalog(user, tags.norm(catalog), action="READ"):
        raise HTTPException(status_code=403, detail=f"Access denied: you cannot view catalog '{catalog}'.")


def _actor(user: Dict[str, Any]) -> str:
    return user.get("username") or "unknown"


def _translate(exc: Exception) -> HTTPException:
    if isinstance(exc, tags.NotFound):
        return HTTPException(status_code=404, detail=str(exc))
    return HTTPException(status_code=400, detail=str(exc))


class TagDefinitionIn(BaseModel):
    tag_key: str
    description: str = ""
    allowed_values: Optional[List[str]] = None


class TagAssignment(BaseModel):
    tag_key: str
    tag_value: str = ""


class TagChanges(BaseModel):
    set: List[TagAssignment] = Field(default_factory=list)
    unset: List[str] = Field(default_factory=list)


class BulkAssignment(BaseModel):
    catalog: str
    schema_name: str = ""
    table_name: str = ""
    column_name: str = ""
    tag_key: str
    tag_value: str = ""
    source: str = "manual"


class BulkApply(BaseModel):
    assignments: List[BulkAssignment]


class PolicyIn(BaseModel):
    name: str
    description: str = ""
    tag_key: str
    tag_value: Optional[str] = None
    mask_type: str
    mask_expr: Optional[str] = None
    applies_to_types: Optional[List[str]] = None
    except_roles: List[str] = Field(default_factory=lambda: ["admin"])
    except_users: List[str] = Field(default_factory=list)
    except_groups: List[str] = Field(default_factory=list)
    priority: int = 100
    enabled: bool = True


class PolicyPatch(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tag_key: Optional[str] = None
    tag_value: Optional[str] = None
    mask_type: Optional[str] = None
    mask_expr: Optional[str] = None
    applies_to_types: Optional[List[str]] = None
    except_roles: Optional[List[str]] = None
    except_users: Optional[List[str]] = None
    except_groups: Optional[List[str]] = None
    priority: Optional[int] = None
    enabled: Optional[bool] = None


class ValidateIn(BaseModel):
    mask_type: str
    mask_expr: Optional[str] = None
    applies_to_types: Optional[List[str]] = None
    data_type: str = "VARCHAR"
    value: Optional[str] = None


class RowPolicyIn(BaseModel):
    name: str
    description: str = ""
    tag_key: str
    tag_value: Optional[str] = None
    filter_column: str
    filter_mode: str
    attribute_key: Optional[str] = None
    filter_expr: Optional[str] = None
    except_roles: List[str] = Field(default_factory=lambda: ["admin"])
    except_users: List[str] = Field(default_factory=list)
    except_groups: List[str] = Field(default_factory=list)
    priority: int = 100
    enabled: bool = True


class RowPolicyPatch(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tag_key: Optional[str] = None
    tag_value: Optional[str] = None
    filter_column: Optional[str] = None
    filter_mode: Optional[str] = None
    attribute_key: Optional[str] = None
    filter_expr: Optional[str] = None
    except_roles: Optional[List[str]] = None
    except_users: Optional[List[str]] = None
    except_groups: Optional[List[str]] = None
    priority: Optional[int] = None
    enabled: Optional[bool] = None


class RowFilterValidateIn(BaseModel):
    filter_expr: str


class AttributeIn(BaseModel):
    principal_type: str
    principal_value: str
    attribute_key: str
    values: List[str] = Field(default_factory=list)


# ----------------------------------------------------------------------------
# Definitions
# ----------------------------------------------------------------------------

@router.get("/tags")
async def list_tag_definitions(user: Dict[str, Any] = Depends(principal)):
    return {"tags": tags.list_definitions()}


@router.post("/tags")
async def create_tag_definition(body: TagDefinitionIn, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    try:
        return tags.create_definition(body.tag_key, body.description, body.allowed_values, actor=_actor(user))
    except ValueError as exc:
        raise _translate(exc)


@router.delete("/tags/{tag_key}")
async def delete_tag_definition(tag_key: str, force: bool = False, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    try:
        return {"deleted": tag_key, **tags.delete_definition(tag_key, force=force, actor=_actor(user))}
    except ValueError as exc:
        raise _translate(exc)


@router.get("/tags/{tag_key}/usage")
async def tag_usage(tag_key: str, tag_value: Optional[str] = None, user: Dict[str, Any] = Depends(principal)):
    try:
        definition = tags.get_definition(tag_key)
    except ValueError as exc:
        raise _translate(exc)
    rows = [r for r in tags.list_assignments(tag_key=tag_key, tag_value=tag_value)
            if can_user_access_catalog(user, r["catalog"], action="READ")]
    return {"tag": definition, "assignments": rows}


# ----------------------------------------------------------------------------
# Assignments
# ----------------------------------------------------------------------------

def _apply_changes(user: Dict[str, Any], changes: TagChanges, *, catalog: str, schema_name: str = "",
                   table_name: str = "", column_name: str = "") -> Dict[str, Any]:
    _require_manage(user, catalog)
    try:
        catalog_meta.validate_object(_con(), catalog, schema_name, table_name, column_name)
        applied = [tags.set_tag(catalog=catalog, schema_name=schema_name, table_name=table_name, column_name=column_name,
                                tag_key=a.tag_key, tag_value=a.tag_value, actor=_actor(user)) for a in changes.set]
        removed = [k for k in changes.unset
                   if tags.unset_tag(catalog=catalog, schema_name=schema_name, table_name=table_name,
                                     column_name=column_name, tag_key=k, actor=_actor(user))]
    except ValueError as exc:
        raise _translate(exc)
    return {"applied": applied, "removed": removed}


@router.put("/objects/{catalog}/tags")
async def put_catalog_tags(catalog: str, changes: TagChanges, user: Dict[str, Any] = Depends(principal)):
    return _apply_changes(user, changes, catalog=catalog)


@router.put("/objects/{catalog}/{schema_name}/tags")
async def put_schema_tags(catalog: str, schema_name: str, changes: TagChanges, user: Dict[str, Any] = Depends(principal)):
    return _apply_changes(user, changes, catalog=catalog, schema_name=schema_name)


@router.put("/objects/{catalog}/{schema_name}/{table_name}/tags")
async def put_table_tags(catalog: str, schema_name: str, table_name: str, changes: TagChanges,
                         user: Dict[str, Any] = Depends(principal)):
    return _apply_changes(user, changes, catalog=catalog, schema_name=schema_name, table_name=table_name)


@router.put("/objects/{catalog}/{schema_name}/{table_name}/columns/{column_name}/tags")
async def put_column_tags(catalog: str, schema_name: str, table_name: str, column_name: str, changes: TagChanges,
                          user: Dict[str, Any] = Depends(principal)):
    return _apply_changes(user, changes, catalog=catalog, schema_name=schema_name, table_name=table_name,
                          column_name=column_name)


@router.get("/objects/{catalog}/{schema_name}/{table_name}/tags")
async def get_table_tags(catalog: str, schema_name: str, table_name: str, user: Dict[str, Any] = Depends(principal)):
    """Direct and inherited tags for a table and each of its columns."""
    _require_read(user, catalog)
    columns = catalog_meta.list_columns(_con(), catalog, schema_name, table_name)
    if not columns:
        raise HTTPException(status_code=404, detail=f"Table '{catalog}.{schema_name}.{table_name}' does not exist.")
    names = [c["column"] for c in columns]
    effective = tags.effective_tags(catalog, schema_name, table_name, names)
    idx = tags.get_index()
    cat_n, sch_n, tbl_n = tags.norm(catalog), tags.norm(schema_name), tags.norm(table_name)
    direct_cols = idx["columns"].get((cat_n, sch_n, tbl_n), {})
    return {
        "object": tags.object_label(cat_n, sch_n, tbl_n),
        "catalog_tags": idx["catalogs"].get(cat_n, {}),
        "schema_tags": idx["schemas"].get((cat_n, sch_n), {}),
        "table_tags": idx["tables"].get((cat_n, sch_n, tbl_n), {}),
        "columns": [{"column": c["column"], "type": c["type"], "direct": direct_cols.get(tags.norm(c["column"]), {}),
                     "effective": effective[c["column"]]} for c in columns],
    }


@router.post("/tags/apply")
async def apply_tags_bulk(body: BulkApply, user: Dict[str, Any] = Depends(principal)):
    """Applies many assignments (e.g. accepted suggestions). Each item succeeds or fails independently."""
    results = []
    con = _con()
    for item in body.assignments:
        label = tags.object_label(item.catalog, item.schema_name, item.table_name, item.column_name)
        try:
            _require_manage(user, item.catalog)
            catalog_meta.validate_object(con, item.catalog, item.schema_name, item.table_name, item.column_name)
            source = item.source if item.source in ("manual", "suggested") else "manual"
            tags.set_tag(catalog=item.catalog, schema_name=item.schema_name, table_name=item.table_name,
                         column_name=item.column_name, tag_key=item.tag_key, tag_value=item.tag_value,
                         actor=_actor(user), source=source)
            results.append({"object": label, "tag_key": item.tag_key, "ok": True})
        except HTTPException as exc:
            results.append({"object": label, "tag_key": item.tag_key, "ok": False, "error": exc.detail})
        except ValueError as exc:
            results.append({"object": label, "tag_key": item.tag_key, "ok": False, "error": str(exc)})
    return {"results": results, "applied": sum(1 for r in results if r["ok"]), "failed": sum(1 for r in results if not r["ok"])}


# ----------------------------------------------------------------------------
# Suggestions, lifecycle, audit
# ----------------------------------------------------------------------------

@router.get("/suggestions")
async def tag_suggestions(catalog: str, schema_name: Optional[str] = None, min_confidence: float = 0.5,
                          user: Dict[str, Any] = Depends(principal)):
    _require_manage(user, catalog)
    columns = catalog_meta.list_columns(_con(), catalog, schema_name)
    tagged: Dict[tuple, set] = {}
    for a in tags.list_assignments(catalog=catalog, include_orphaned=False, limit=5000):
        tagged.setdefault((a["catalog"], a["schema_name"], a["table_name"], a["column_name"]), set()).add(a["tag_key"])
    suggestions = classify.suggest_for_columns(columns, tagged, min_confidence=min_confidence)
    return {"catalog": catalog, "scanned_columns": len(columns), "suggestions": suggestions}


@router.post("/reconcile")
async def reconcile_tags(user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    return tags.reconcile(_con(), actor=_actor(user))


@router.get("/audit")
async def audit_log(since: Optional[str] = None, actor: Optional[str] = None, action: Optional[str] = None,
                    limit: int = 200, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    return {"events": store.list_audit(since=since, actor=actor, action=action, limit=limit)}


# ----------------------------------------------------------------------------
# Masking policies
# ----------------------------------------------------------------------------

@router.get("/masking-policies")
async def list_masking_policies(user: Dict[str, Any] = Depends(principal)):
    return {"policies": policies.list_policies()}


@router.post("/masking-policies")
async def create_masking_policy(body: PolicyIn, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    try:
        return policies.create_policy(body.model_dump(), actor=_actor(user))
    except ValueError as exc:
        raise _translate(exc)


@router.put("/masking-policies/{policy_id}")
async def update_masking_policy(policy_id: str, body: PolicyPatch, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    try:
        return policies.update_policy(policy_id, body.model_dump(exclude_unset=True), actor=_actor(user))
    except ValueError as exc:
        raise _translate(exc)


@router.delete("/masking-policies/{policy_id}")
async def delete_masking_policy(policy_id: str, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    try:
        policies.delete_policy(policy_id, actor=_actor(user))
    except ValueError as exc:
        raise _translate(exc)
    return {"deleted": policy_id}


@router.post("/masking-policies/validate")
async def validate_masking_policy(body: ValidateIn, user: Dict[str, Any] = Depends(principal)):
    """Dry run for the policy editor: per-type behaviour, custom-expression verdict and a sample masked value."""
    _require_admin(user)
    if body.mask_type not in masks.MASK_TYPES:
        raise HTTPException(status_code=400, detail=f"mask_type must be one of: {', '.join(masks.MASK_TYPES)}.")
    out: Dict[str, Any] = {"behaviour": masks.behaviour_table(body.mask_type)}
    if body.mask_type == "custom":
        out["custom"] = masks.validate_custom_expression(body.mask_expr or "", body.applies_to_types)
    if body.value is not None and (body.mask_type != "custom" or out["custom"]["ok"]):
        try:
            out["sample"] = masks.run_mask(body.mask_type, body.data_type, body.value, body.mask_expr)
        except ValueError as exc:
            raise _translate(exc)
    return out


# ----------------------------------------------------------------------------
# Row filter policies (row-level security) and principal attributes
# ----------------------------------------------------------------------------

@router.get("/row-policies")
async def list_row_policies_route(user: Dict[str, Any] = Depends(principal)):
    return {"policies": row_filters.list_row_policies()}


@router.post("/row-policies")
async def create_row_policy_route(body: RowPolicyIn, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    try:
        return row_filters.create_row_policy(body.model_dump(), actor=_actor(user))
    except ValueError as exc:
        raise _translate(exc)


@router.put("/row-policies/{policy_id}")
async def update_row_policy_route(policy_id: str, body: RowPolicyPatch, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    try:
        return row_filters.update_row_policy(policy_id, body.model_dump(exclude_unset=True), actor=_actor(user))
    except ValueError as exc:
        raise _translate(exc)


@router.delete("/row-policies/{policy_id}")
async def delete_row_policy_route(policy_id: str, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    try:
        row_filters.delete_row_policy(policy_id, actor=_actor(user))
    except ValueError as exc:
        raise _translate(exc)
    return {"deleted": policy_id}


@router.post("/row-policies/validate")
async def validate_row_policy(body: RowFilterValidateIn, user: Dict[str, Any] = Depends(principal)):
    """Dry run for the row policy editor: static AST verdict for a custom filter expression."""
    _require_admin(user)
    return row_filters.validate_filter_expression(body.filter_expr)


@router.get("/attributes")
async def list_attributes_route(principal_type: Optional[str] = None, principal_value: Optional[str] = None,
                                attribute_key: Optional[str] = None, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    return {"attributes": row_filters.list_attributes(principal_type, principal_value, attribute_key)}


@router.post("/attributes")
async def set_attributes_route(body: AttributeIn, user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    try:
        return row_filters.set_attribute_values(body.principal_type, body.principal_value, body.attribute_key,
                                                body.values, actor=_actor(user))
    except ValueError as exc:
        raise _translate(exc)


@router.delete("/attributes")
async def delete_attribute_route(principal_type: str, principal_value: str, attribute_key: str,
                                 user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    deleted = row_filters.delete_attribute(principal_type, principal_value, attribute_key, actor=_actor(user))
    if not deleted:
        raise HTTPException(status_code=404, detail="No such attribute assignment.")
    return {"deleted": True}


@router.get("/effective/{catalog}/{schema_name}/{table_name}")
async def effective_governance(catalog: str, schema_name: str, table_name: str, as_user: Optional[str] = None,
                               user: Dict[str, Any] = Depends(principal)):
    """Per column: effective tags, the winning masking policy and whether it masks the caller (or `as_user`, admin only)."""
    _require_read(user, catalog)
    subject = user
    if as_user:
        _require_admin(user)
        subject = get_user_by_username(as_user)
        if not subject:
            raise HTTPException(status_code=404, detail=f"User '{as_user}' does not exist.")
    who = policies.Principal.from_user(subject)
    columns = catalog_meta.list_columns(_con(), catalog, schema_name, table_name)
    if not columns:
        raise HTTPException(status_code=404, detail=f"Table '{catalog}.{schema_name}.{table_name}' does not exist.")
    eff = tags.effective_tags(catalog, schema_name, table_name, [c["column"] for c in columns])
    out = []
    for c in columns:
        spec = policies.resolve_column_policy(c["column"], c["type"], eff[c["column"]], who)
        out.append({"column": c["column"], "type": c["type"], "family": masks.type_family(c["type"]), "tags": eff[c["column"]],
                    "masked": spec is not None,
                    "policy": ({"id": spec.policy_id, "name": spec.policy_name, "mask_type": spec.mask_type,
                                "tied_with": spec.conflicts} if spec else None)})
    row_specs = row_filters.filters_for_table(catalog, schema_name, table_name, columns, who)
    return {"object": tags.object_label(tags.norm(catalog), tags.norm(schema_name), tags.norm(table_name)),
            "as_user": subject.get("username"), "role": subject.get("role"), "columns": out,
            "masked_columns": [c["column"] for c in out if c["masked"]],
            "row_filtered": bool(row_specs),
            "row_policies": [{"id": s.policy_id, "name": s.policy_name, "filter_column": s.filter_column} for s in row_specs]}


@router.get("/status")
async def governance_status(user: Dict[str, Any] = Depends(principal)):
    _require_admin(user)
    con = _con()
    return {
        "version": store.get_version(),
        "tag_definitions": len(tags.list_definitions()),
        "tag_assignments": len(tags.list_assignments(limit=5000)),
        "policies_total": len(policies.list_policies()),
        "policies_enabled": len(policies.enabled_policies()),
        "row_policies_total": len(row_filters.list_row_policies()),
        "row_policies_enabled": len(row_filters.enabled_row_policies()),
        "masks_installed": macros.macros_installed(con),
        "posture": _posture(),
        "workers": _workers(),
    }


def _workers() -> List[Dict[str, Any]]:
    """Whether each compute node can run masked SQL (the studio polls their status; nodes without masks fail closed)."""
    try:
        from web.warehouses import get_compute_nodes_status
        return [{"node_id": n.get("node_id"), "status": n.get("status"), "masks_installed": n.get("governance_masks_installed")}
                for n in get_compute_nodes_status()]
    except Exception:
        return []


def _posture() -> Dict[str, Any]:
    """The governance trust boundary at a glance (what masking does and does not cover in this install)."""
    from web.auth import governance_require_auth
    from web.governance.enforce import enforcement_mode
    from web.notebook_access import execution_mode
    return {
        "enforcement_mode": enforcement_mode(),
        "require_auth": governance_require_auth(),
        "notebook_execution": execution_mode(),
    }


class PreviewAsIn(BaseModel):
    sql: str
    as_user: str
    catalog: Optional[str] = "warehouse"


@router.post("/preview-as")
async def preview_as(body: PreviewAsIn, user: Dict[str, Any] = Depends(principal)):
    """What would `as_user` actually run for this SQL? Returns the rewritten SQL and masked columns; nothing executes."""
    _require_admin(user)
    subject = get_user_by_username(body.as_user)
    if not subject:
        raise HTTPException(status_code=404, detail=f"User '{body.as_user}' does not exist.")
    from web.governance import enforce
    cur = _con()
    try:
        result = enforce.rewrite_for_principal(body.sql, policies.Principal.from_user(subject), cur,
                                               default_catalog=body.catalog or "warehouse", default_schema="main")
    finally:
        cur.close()
    return {"as_user": subject["username"], "role": subject.get("role"), "blocked": result.blocked, "changed": result.changed,
            "rewritten_sql": result.sql if result.changed else None, "tables": result.tables,
            "masked_columns": gateway.masked_columns_payload(result),
            "exempt_reads": sorted({f"{m.table}.{m.column}" for m in result.exempt_reads}),
            "row_filters_applied": gateway.row_filter_payload(result),
            "row_filter_exempt_reads": sorted({m.table for m in result.row_filter_exempt_reads})}


@router.get("/coverage")
async def coverage(user: Dict[str, Any] = Depends(principal)):
    """Where governance stands: assignments by level, orphans, and what each enabled policy currently matches."""
    _require_admin(user)
    assignments = tags.list_assignments(limit=5000)
    by_level: Dict[str, int] = {}
    for a in assignments:
        by_level[a["level"]] = by_level.get(a["level"], 0) + 1
    per_policy = []
    for pol in policies.list_policies():
        matched = [a for a in assignments if not a["orphaned"] and a["tag_key"] == pol["tag_key"]
                   and (pol["tag_value"] is None or a["tag_value"] == pol["tag_value"])]
        per_policy.append({"id": pol["id"], "name": pol["name"], "enabled": pol["enabled"], "mask_type": pol["mask_type"],
                           "matching_assignments": len(matched)})
    # Row policies only match table/schema/catalog-level assignments (a row filter can never bind to a column tag).
    table_level_assignments = [a for a in assignments if a["level"] in ("table", "schema", "catalog")]
    per_row_policy = []
    for pol in row_filters.list_row_policies():
        matched = [a for a in table_level_assignments if not a["orphaned"] and a["tag_key"] == pol["tag_key"]
                   and (pol["tag_value"] is None or a["tag_value"] == pol["tag_value"])]
        missing_column = any(pol["filter_column"].lower() not in {c["column"].lower() for c in catalog_meta.list_columns(
            _con(), a["catalog"], a["schema_name"], a["table_name"])} for a in matched if a["table_name"])
        per_row_policy.append({"id": pol["id"], "name": pol["name"], "enabled": pol["enabled"], "filter_mode": pol["filter_mode"],
                               "filter_column": pol["filter_column"], "matching_assignments": len(matched),
                               "filter_column_missing_somewhere": missing_column})
    return {"assignments": len(assignments), "by_level": by_level, "orphaned": sum(1 for a in assignments if a["orphaned"]),
            "policies": per_policy,
            "policies_without_matches": [p["name"] for p in per_policy if p["enabled"] and p["matching_assignments"] == 0],
            "row_policies": per_row_policy,
            "row_policies_without_matches": [p["name"] for p in per_row_policy if p["enabled"] and p["matching_assignments"] == 0],
            "row_policies_with_missing_column": [p["name"] for p in per_row_policy if p["filter_column_missing_somewhere"]]}
