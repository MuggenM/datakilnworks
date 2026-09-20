"""
REST API for governance under /api/governance.

Roles: tag definitions are admin-only. Assigning tags needs management rights on the catalog
(web.permissions.can_user_manage_catalog); reading tags needs READ on the catalog.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from web.auth import resolve_principal
from web.governance import catalog_meta, classify, store, tags
from web.permissions import can_user_access_catalog, can_user_manage_catalog

logger = logging.getLogger("localspark.governance")

router = APIRouter(prefix="/api/governance", tags=["governance"])

_connection_provider = None


def set_connection_provider(provider) -> None:
    """app.py injects a callable returning a DuckDB connection (avoids importing app from here)."""
    global _connection_provider
    _connection_provider = provider


def _con():
    if _connection_provider is None:
        raise HTTPException(status_code=503, detail="Governance is not connected to the query engine.")
    return _connection_provider()


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
