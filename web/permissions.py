"""
Catalog Access Control Lists (ACLs) & Permission Governance for Localspark.
Enforces catalog ownership, grant/revoke permissions for users,
and query-level zero-trust access filtering.
"""

import re
import datetime
import logging
from typing import Dict, Any, List, Optional, Set

from fastapi import HTTPException, status
from web.auth import get_db_connection, get_user_by_username, get_user_by_id

logger = logging.getLogger("localspark.permissions")


def get_all_catalog_ids() -> Set[str]:
    """Returns the set of all registered catalog IDs from catalogs.json and storage_mounts.json."""
    c_ids = set()
    try:
        from web.warehouses import load_catalogs
        for cat in load_catalogs():
            c_ids.add(cat["id"])
    except Exception:
        pass
    try:
        from web.mounts import load_mounts
        for m in load_mounts():
            if m.get("enabled", True):
                c_ids.add(m.get("catalog_name", ""))
    except Exception:
        pass
    return c_ids


def get_catalog_details(catalog_id: str) -> Optional[Dict[str, Any]]:
    """Fetches catalog metadata record from warehouses or mounts."""
    from web.warehouses import get_catalog
    return get_catalog(catalog_id)


def get_catalog_owner(catalog_id: str) -> str:
    """Returns the username of the catalog's owner. Defaults to 'admin' if unspecified."""
    cat = get_catalog_details(catalog_id)
    if cat:
        return cat.get("owner") or cat.get("created_by") or "admin"
    return "admin"


def is_catalog_owner(user: Dict[str, Any], catalog_id: str) -> bool:
    """Returns True if the user is the owner of the catalog, or is a system admin."""
    if user.get("role") == "admin":
        return True
    owner = get_catalog_owner(catalog_id)
    username = user.get("username", "")
    user_id = user.get("id", "")
    return owner in (username, user_id)


def can_user_manage_catalog(user: Dict[str, Any], catalog_id: str) -> bool:
    """
    Returns True if the user has rights to grant/revoke permissions or configure the catalog.
    - admin: Can manage any catalog.
    - power_user: Can manage ONLY if they own the catalog.
    - user: Cannot manage any catalog.
    """
    role = user.get("role", "user")
    if role == "admin":
        return True
    if role == "power_user":
        return is_catalog_owner(user, catalog_id)
    return False


def can_user_delete_catalog(user: Dict[str, Any], catalog_id: str) -> bool:
    """
    Evaluates whether the user is permitted to delete the specified catalog.
    Rules:
      1. Default catalog ('warehouse' or is_default=True): cannot be deleted.
      2. Admin: Can delete all catalogs.
      3. Power User: Can only delete catalogs where they are the owner.
      4. User: Cannot delete any catalog.
    """
    if not catalog_id or catalog_id == "warehouse":
        return False
    cat = get_catalog_details(catalog_id)
    if not cat or cat.get("is_default"):
        return False

    role = user.get("role", "user")
    if role == "admin":
        return True
    if role == "power_user":
        owner = cat.get("owner") or cat.get("created_by") or ""
        username = user.get("username", "")
        user_id = user.get("id", "")
        return bool(owner and (owner == username or owner == user_id))
    return False


def delete_all_catalog_permissions(catalog_id: str):
    """Purges all explicit permission records for a catalog from auth.db upon catalog deletion."""
    try:
        conn = get_db_connection()
        try:
            conn.execute("DELETE FROM catalog_permissions WHERE catalog_id = ?", (catalog_id,))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Could not purge catalog permissions for '{catalog_id}': {e}")



def can_user_access_catalog(user: Dict[str, Any], catalog_id: str, action: str = "READ") -> bool:
    """
    Evaluates whether the user is permitted to perform 'action' ('READ', 'WRITE') on 'catalog_id'.
    Rules:
      1. Admin: Always True.
      2. Default 'warehouse' lakehouse: Public read/write for all users.
      3. Catalog Owner: Always True.
      4. Explicit permissions in catalog_permissions table:
         - READ: requires 'READ', 'WRITE', or 'ADMIN'
         - WRITE: requires 'WRITE' or 'ADMIN'
         - ADMIN: requires 'ADMIN'
    """
    role = user.get("role", "user")
    if role == "admin":
        return True

    # Primary warehouse catalog is public
    if catalog_id == "warehouse":
        return True

    # Owner always has full access
    if is_catalog_owner(user, catalog_id):
        return True

    # Standard check against catalog_permissions: the user's own grant and the grants of every group they belong to;
    # the highest one wins (groups only ever add access).
    user_id = user.get("id", "")
    username = user.get("username", "")
    principals = [user_id, username]
    try:
        from web import groups
        principals += [f"group:{g}" for g in groups.group_ids_for(user)]
    except Exception as exc:
        logger.warning(f"group membership unavailable for catalog check: {exc}")
    principals = [p for p in principals if p]
    if not principals:
        return False

    conn = get_db_connection()
    try:
        rows = conn.execute(
            f"SELECT permission FROM catalog_permissions WHERE catalog_id = ? AND user_id IN ({','.join('?' * len(principals))})",
            (catalog_id, *principals)).fetchall()
        perms = {r["permission"].upper() for r in rows}
        if not perms:
            return False
        act = action.upper()
        if act == "READ":
            return bool(perms & {"READ", "WRITE", "ADMIN"})
        if act == "WRITE":
            return bool(perms & {"WRITE", "ADMIN"})
        if act == "ADMIN":
            return "ADMIN" in perms
        return False
    finally:
        conn.close()


def list_catalog_permissions(catalog_id: str) -> Dict[str, Any]:
    """Returns permission list for a catalog along with owner info."""
    owner = get_catalog_owner(catalog_id)
    conn = get_db_connection()
    try:
        rows = conn.execute("""
        SELECT cp.id, cp.catalog_id, cp.user_id, cp.permission, cp.granted_by, cp.created_at,
               u.username, u.display_name, u.role
        FROM catalog_permissions cp
        LEFT JOIN users u ON cp.user_id = u.id OR cp.user_id = u.username
        WHERE cp.catalog_id = ?
        ORDER BY cp.created_at DESC
        """, (catalog_id,)).fetchall()

        grants = []
        for r in rows:
            is_group = str(r["user_id"]).startswith("group:")
            gname = None
            if is_group:
                gr = conn.execute("SELECT name FROM user_groups WHERE id = ?", (r["user_id"][len("group:"):],)).fetchone() \
                    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_groups'").fetchone() else None
                gname = gr["name"] if gr else r["user_id"]
            grants.append({
                "id": r["id"],
                "catalog_id": r["catalog_id"],
                "user_id": r["user_id"],
                "principal_type": "group" if is_group else "user",
                "username": gname or r["username"] or r["user_id"],
                "display_name": gname or r["display_name"] or r["username"] or r["user_id"],
                "role": "group" if is_group else (r["role"] or "user"),
                "permission": r["permission"],
                "granted_by": r["granted_by"],
                "created_at": r["created_at"]
            })

        return {
            "catalog_id": catalog_id,
            "owner": owner,
            "permissions": grants
        }
    finally:
        conn.close()


def grant_catalog_permission(
    catalog_id: str,
    target_user_id: str,
    permission: str,
    granted_by_user: Dict[str, Any]
) -> Dict[str, Any]:
    """Grants or updates access for a user on a catalog."""
    if not can_user_manage_catalog(granted_by_user, catalog_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied: you do not have permission to manage access for catalog '{catalog_id}'."
        )

    clean_perm = permission.strip().upper()
    if clean_perm not in ("READ", "WRITE", "ADMIN"):
        raise HTTPException(status_code=400, detail="Invalid permission. Must be READ, WRITE, or ADMIN.")

    target_clean = target_user_id.strip()
    if target_clean.startswith("group:"):
        from web import groups
        if not groups.get_group(target_clean[len("group:"):]):
            raise HTTPException(status_code=404, detail="That group does not exist.")
        effective_user_id = target_clean
    else:
        target_user = get_user_by_username(target_clean) or get_user_by_id(target_clean)
        effective_user_id = target_user["id"] if target_user else target_clean

    conn = get_db_connection()
    try:
        now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with conn:
            conn.execute("""
            INSERT INTO catalog_permissions (catalog_id, user_id, permission, granted_by, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(catalog_id, user_id) DO UPDATE SET
                permission = excluded.permission,
                granted_by = excluded.granted_by,
                created_at = excluded.created_at
            """, (catalog_id, effective_user_id, clean_perm, granted_by_user.get("username", "admin"), now_str))

        return {
            "success": True,
            "catalog_id": catalog_id,
            "user_id": effective_user_id,
            "permission": clean_perm,
            "granted_by": granted_by_user.get("username", "admin"),
            "created_at": now_str
        }
    finally:
        conn.close()


def revoke_catalog_permission(
    catalog_id: str,
    target_user_id: str,
    revoked_by_user: Dict[str, Any]
) -> bool:
    """Revokes access for a user from a catalog."""
    if not can_user_manage_catalog(revoked_by_user, catalog_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied: you do not have permission to manage access for catalog '{catalog_id}'."
        )

    target_clean = target_user_id.strip()
    if target_clean.startswith("group:"):
        target_id = target_name = target_clean
    else:
        target_user = get_user_by_username(target_clean) or get_user_by_id(target_clean)
        target_id = target_user["id"] if target_user else target_clean
        target_name = target_user["username"] if target_user else target_clean

    conn = get_db_connection()
    try:
        with conn:
            res = conn.execute("""
            DELETE FROM catalog_permissions WHERE catalog_id = ? AND (user_id = ? OR user_id = ?)
            """, (catalog_id, target_id, target_name))
            return res.rowcount > 0
    finally:
        conn.close()


def filter_catalogs_for_user(catalogs_tree: List[Dict[str, Any]], user: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Filters Unity Catalog tree hierarchy, preserving only catalogs the user can access."""
    if user.get("role") == "admin":
        annotated = []
        for cat in catalogs_tree:
            cat_copy = dict(cat)
            cat_id = cat_copy.get("id")
            cat_copy["user_can_manage"] = True
            cat_copy["user_can_write"] = True
            cat_copy["user_can_delete"] = can_user_delete_catalog(user, cat_id)
            annotated.append(cat_copy)
        return annotated

    filtered = []
    try:
        from web import table_access
        scope = table_access.granted_scope(user)
    except Exception as exc:
        logger.warning(f"table grants unavailable for the catalog listing: {exc}")
        scope = {}
    for cat in catalogs_tree:
        cat_id = cat.get("id")
        if not can_user_access_catalog(user, cat_id, action="READ") and cat_id in scope:
            # Access to some tables or schemas only: show just those, read-only.
            partial = table_access.prune_catalog(cat, scope[cat_id])
            if partial["schemas"]:
                partial.update(user_can_manage=False, user_can_write=False, user_can_delete=False, partial_access=True)
                filtered.append(partial)
            continue
        if can_user_access_catalog(user, cat_id, action="READ"):
            # Attach user-specific capability flags to the catalog response
            cat_copy = dict(cat)
            cat_copy["user_can_manage"] = can_user_manage_catalog(user, cat_id)
            cat_copy["user_can_write"] = can_user_access_catalog(user, cat_id, action="WRITE")
            cat_copy["user_can_delete"] = can_user_delete_catalog(user, cat_id)
            filtered.append(cat_copy)
    return filtered


# ==============================================================================
# SQL ZERO-TRUST PARSER & QUERY FENCING
# ==============================================================================

# A statement starting with one of these only reads; a catalog is referenced in it through a dotted name. Any OTHER statement (USE, ATTACH,
# DETACH, SET, CALL, PRAGMA, COPY, CREATE, ...) can name a catalog on its own (`USE sales` switches the default catalog for the rest of the
# request, after which `select * from dbo.customers` reads it with no catalog in sight), so there a bare occurrence of the name counts.
_QUERY_STARTS = {"select", "with", "from", "values", "table", "explain", "describe", "show", "summarize", "pivot", "unpivot", "("}


def _catalogs_from_tokens(sql_query: str, all_catalogs: Set[str]) -> Set[str]:
    """Catalogs referenced according to the SQL tokenizer. Unlike a regex this cannot be fooled by quoted identifiers (`"sales"."dbo"."t"`),
    comments or whitespace inside a name (`sales/**/.dbo.t`); string literals are not identifiers. Dotted use (`catalog.schema.table`) counts
    in every statement; a bare name counts in statements that are not plain queries (see _QUERY_STARTS)."""
    import sqlglot
    from sqlglot.tokens import TokenType
    try:
        toks = sqlglot.tokenize(sql_query, read="duckdb")
    except Exception:
        return set(all_catalogs) if all_catalogs else set()      # cannot tokenize (e.g. unterminated string): assume every catalog is touched
    found: Set[str] = set()
    segment_start = 0
    for i, t in enumerate(toks):
        if t.token_type == TokenType.SEMICOLON:
            segment_start = i + 1
            continue
        if t.token_type == TokenType.STRING or t.text.lower() not in all_catalogs:
            continue
        if i > 0 and toks[i - 1].token_type == TokenType.DOT:
            continue                                   # `x.catalog`: a schema/column part of another name
        dotted = i + 1 < len(toks) and toks[i + 1].token_type == TokenType.DOT
        first = toks[segment_start].text.lower() if segment_start < len(toks) else ""
        if dotted or first not in _QUERY_STARTS:
            found.add(t.text.lower())
    return found


def extract_catalogs_from_sql(sql_query: str) -> Set[str]:
    """
    Extracts referenced catalog names from a SQL statement.
    Detects 3-part names (catalog.schema.table), catalog-qualified function calls,
    or attached catalog references. Union of a tokenizer pass (robust against quoting and comments)
    and the original regexes (which also flag a schema that shares a catalog's name: over-cautious, kept).
    """
    all_catalogs = get_all_catalog_ids()
    referenced = set(_catalogs_from_tokens(sql_query, all_catalogs))

    # Match patterns like: `catalog`.`schema`.`table` or catalog.schema.table
    pattern = re.compile(r'\b([a-zA-Z0-9_]+)\s*\.\s*([a-zA-Z0-9_]+)\s*\.\s*([a-zA-Z0-9_]+)\b')
    for match in pattern.finditer(sql_query):
        candidate_cat = match.group(1).lower()
        if candidate_cat in all_catalogs:
            referenced.add(candidate_cat)

    # Also check single catalog references like FROM dev_catalog.table or delta_scan references
    for cat in all_catalogs:
        if cat != "warehouse":
            cat_pattern = re.compile(rf'\b{re.escape(cat)}\b\s*\.', re.IGNORECASE)
            if cat_pattern.search(sql_query):
                referenced.add(cat)

    return referenced


def enforce_sql_permissions(sql_query: str, user: Dict[str, Any], action: str = "READ"):
    """
    Inspects SQL query and verifies that user has appropriate permissions
    for every referenced catalog. Raises HTTPException(403) if unauthorized.

    A catalog the user has no access to as a whole can still be queried when the statement is one plain SELECT whose every reference to it is
    a fully qualified table (or schema) the user, or one of their groups, was granted SELECT on (web/table_access.py). Anything that cannot
    be verified that way is refused.
    """
    if user.get("role") == "admin":
        return

    referenced_catalogs = extract_catalogs_from_sql(sql_query)
    denied = [c for c in sorted(referenced_catalogs) if not can_user_access_catalog(user, c, action=action)]
    if not denied:
        return
    if action.upper() == "READ":
        from web import table_access
        if table_access.sql_covered(sql_query, user, denied):
            return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Access denied: User '{user.get('username')}' does not have permission to query catalog '{denied[0]}'."
    )
