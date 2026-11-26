"""
Dashboard Permissions - Fine-grained access control for dashboards.

Leverages existing RBAC system to provide dashboard-level permissions.
"""

import os
import json
from typing import Dict, Any, List, Optional
from datetime import datetime

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
DASHBOARD_PERMISSIONS_FILE = os.path.join(METADATA_DIR, "dashboard_permissions.json")

# Permission levels
PERMISSION_LEVELS = {
    "owner": 4,      # Full control - delete, share, edit, view
    "editor": 3,     # Edit and view, cannot delete or change permissions
    "viewer": 2,     # View only, cannot edit
    "none": 0        # No access
}

# {dashboard_id: {owner, permissions: [{user, role, level, granted_by, granted_at}], is_public}}
DASHBOARD_PERMISSIONS = {}


def load_permissions():
    """Load dashboard permissions from file."""
    global DASHBOARD_PERMISSIONS
    if os.path.exists(DASHBOARD_PERMISSIONS_FILE):
        try:
            with open(DASHBOARD_PERMISSIONS_FILE, "r") as f:
                DASHBOARD_PERMISSIONS = json.load(f)
        except Exception as e:
            print(f"Error loading dashboard permissions: {e}")
            DASHBOARD_PERMISSIONS = {}
    return DASHBOARD_PERMISSIONS


def save_permissions():
    """Save dashboard permissions to file."""
    try:
        os.makedirs(METADATA_DIR, exist_ok=True)
        with open(DASHBOARD_PERMISSIONS_FILE, "w") as f:
            json.dump(DASHBOARD_PERMISSIONS, f, indent=2)
    except Exception as e:
        print(f"Error saving dashboard permissions: {e}")


def initialize_dashboard_permissions(dashboard_id: str, owner: str):
    """Initialize permissions for a new dashboard."""
    load_permissions()

    if dashboard_id not in DASHBOARD_PERMISSIONS:
        DASHBOARD_PERMISSIONS[dashboard_id] = {
            "owner": owner,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "permissions": [],
            "is_public": False  # Private by default
        }
        save_permissions()


def get_dashboard_permissions(dashboard_id: str) -> Optional[Dict[str, Any]]:
    """Get permissions for a dashboard."""
    load_permissions()
    return DASHBOARD_PERMISSIONS.get(dashboard_id)


def get_user_permission_level(dashboard_id: str, username: str, user_role: str) -> str:
    """Get the effective permission level for a user on a dashboard."""
    load_permissions()

    dashboard_perms = DASHBOARD_PERMISSIONS.get(dashboard_id)
    if not dashboard_perms:
        # No permissions defined - admins can access, others cannot
        return "owner" if user_role == "admin" else "none"

    # Dashboard owner has full control
    if dashboard_perms["owner"] == username:
        return "owner"

    # Admins always have owner-level access
    if user_role == "admin":
        return "owner"

    # Check explicit user permissions
    max_level = "none"
    max_level_value = 0

    try:
        from web import groups
        my_groups = groups.group_ids_for_username(username)
    except Exception:
        my_groups = set()

    for perm in dashboard_perms.get("permissions", []):
        # Match by group membership (a user takes the highest level of their own grant, their role's and their groups')
        if perm.get("group") and perm["group"] in my_groups:
            level = perm.get("level", "none")
            if PERMISSION_LEVELS.get(level, 0) > max_level_value:
                max_level = level
                max_level_value = PERMISSION_LEVELS[level]
            continue

        # Match by username
        if perm.get("user") == username:
            level = perm.get("level", "none")
            if PERMISSION_LEVELS.get(level, 0) > max_level_value:
                max_level = level
                max_level_value = PERMISSION_LEVELS[level]

        # Match by role
        elif perm.get("role") == user_role:
            level = perm.get("level", "none")
            if PERMISSION_LEVELS.get(level, 0) > max_level_value:
                max_level = level
                max_level_value = PERMISSION_LEVELS[level]

    # Check if dashboard is public (grants viewer access to all)
    if dashboard_perms.get("is_public", False) and max_level == "none":
        return "viewer"

    return max_level


def can_view_dashboard(dashboard_id: str, username: str, user_role: str) -> bool:
    """Check if user can view a dashboard."""
    level = get_user_permission_level(dashboard_id, username, user_role)
    return PERMISSION_LEVELS.get(level, 0) >= PERMISSION_LEVELS["viewer"]


def can_edit_dashboard(dashboard_id: str, username: str, user_role: str) -> bool:
    """Check if user can edit a dashboard."""
    level = get_user_permission_level(dashboard_id, username, user_role)
    return PERMISSION_LEVELS.get(level, 0) >= PERMISSION_LEVELS["editor"]


def can_delete_dashboard(dashboard_id: str, username: str, user_role: str) -> bool:
    """Check if user can delete a dashboard."""
    level = get_user_permission_level(dashboard_id, username, user_role)
    return PERMISSION_LEVELS.get(level, 0) >= PERMISSION_LEVELS["owner"]


def can_manage_permissions(dashboard_id: str, username: str, user_role: str) -> bool:
    """Check if user can manage dashboard permissions."""
    level = get_user_permission_level(dashboard_id, username, user_role)
    return PERMISSION_LEVELS.get(level, 0) >= PERMISSION_LEVELS["owner"]


def _same_principal(perm: Dict[str, Any], user: Optional[str], role: Optional[str], group: Optional[str]) -> bool:
    """Does an existing entry belong to the principal being granted/revoked? (Compares only the key that was given: an entry without
    a `role` must not match a grant whose role is also absent, or granting to one user would overwrite another's entry.)"""
    if user:
        return perm.get("user") == user
    if role:
        return perm.get("role") == role
    if group:
        return perm.get("group") == group
    return False


def grant_permission(
    dashboard_id: str,
    user: Optional[str] = None,
    role: Optional[str] = None,
    level: str = "viewer",
    granted_by: str = "admin",
    group: Optional[str] = None
) -> bool:
    """Grant permission to a user, a role or a group (group id)."""
    load_permissions()

    if dashboard_id not in DASHBOARD_PERMISSIONS:
        return False

    if not user and not role and not group:
        return False

    if level not in PERMISSION_LEVELS:
        return False

    permissions = DASHBOARD_PERMISSIONS[dashboard_id].get("permissions", [])

    for perm in permissions:
        if _same_principal(perm, user, role, group):
            perm["level"] = level
            perm["granted_by"] = granted_by
            perm["granted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_permissions()
            return True

    new_perm = {
        "level": level,
        "granted_by": granted_by,
        "granted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    if user:
        new_perm["user"] = user
    elif role:
        new_perm["role"] = role
    elif group:
        new_perm["group"] = group
        try:
            from web import groups
            g = groups.get_group(group)
            if g:
                new_perm["group_name"] = g["name"]
        except Exception:
            pass

    DASHBOARD_PERMISSIONS[dashboard_id]["permissions"].append(new_perm)
    save_permissions()
    return True


def revoke_permission(
    dashboard_id: str,
    user: Optional[str] = None,
    role: Optional[str] = None,
    group: Optional[str] = None
) -> bool:
    """Revoke permission from a user, a role or a group."""
    load_permissions()

    if dashboard_id not in DASHBOARD_PERMISSIONS:
        return False

    permissions = DASHBOARD_PERMISSIONS[dashboard_id].get("permissions", [])
    original_count = len(permissions)

    DASHBOARD_PERMISSIONS[dashboard_id]["permissions"] = [p for p in permissions if not _same_principal(p, user, role, group)]

    if len(DASHBOARD_PERMISSIONS[dashboard_id]["permissions"]) < original_count:
        save_permissions()
        return True

    return False


def remove_group_everywhere(group_id: str) -> None:
    """A deleted group loses its dashboard grants (called by groups.delete_group)."""
    load_permissions()
    changed = False
    for perms in DASHBOARD_PERMISSIONS.values():
        kept = [p for p in perms.get("permissions", []) if p.get("group") != group_id]
        if len(kept) != len(perms.get("permissions", [])):
            perms["permissions"] = kept
            changed = True
    if changed:
        save_permissions()


def set_dashboard_public(dashboard_id: str, is_public: bool) -> bool:
    """Set whether a dashboard is publicly viewable."""
    load_permissions()

    if dashboard_id not in DASHBOARD_PERMISSIONS:
        return False

    DASHBOARD_PERMISSIONS[dashboard_id]["is_public"] = is_public
    save_permissions()
    return True


def transfer_ownership(dashboard_id: str, new_owner: str, transferred_by: str) -> bool:
    """Transfer dashboard ownership to another user."""
    load_permissions()

    if dashboard_id not in DASHBOARD_PERMISSIONS:
        return False

    old_owner = DASHBOARD_PERMISSIONS[dashboard_id]["owner"]
    DASHBOARD_PERMISSIONS[dashboard_id]["owner"] = new_owner
    DASHBOARD_PERMISSIONS[dashboard_id]["transferred_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    DASHBOARD_PERMISSIONS[dashboard_id]["transferred_by"] = transferred_by

    # Give old owner editor access
    grant_permission(dashboard_id, user=old_owner, level="editor", granted_by=transferred_by)

    save_permissions()
    return True


def list_user_dashboards(username: str, user_role: str) -> List[str]:
    """List all dashboard IDs the user has access to."""
    load_permissions()

    accessible_dashboards = []

    for dashboard_id, perms in DASHBOARD_PERMISSIONS.items():
        if can_view_dashboard(dashboard_id, username, user_role):
            accessible_dashboards.append(dashboard_id)

    return accessible_dashboards


def list_dashboard_users(dashboard_id: str) -> List[Dict[str, Any]]:
    """List all users with access to a dashboard."""
    load_permissions()

    dashboard_perms = DASHBOARD_PERMISSIONS.get(dashboard_id)
    if not dashboard_perms:
        return []

    users = []

    # Add owner
    users.append({
        "user": dashboard_perms["owner"],
        "level": "owner",
        "granted_by": "system",
        "granted_at": dashboard_perms.get("created_at", "")
    })

    # Add explicit permissions
    for perm in dashboard_perms.get("permissions", []):
        users.append(perm)

    return users


def get_permission_summary() -> Dict[str, Any]:
    """Get summary of dashboard permissions."""
    load_permissions()

    total_dashboards = len(DASHBOARD_PERMISSIONS)
    public_dashboards = sum(1 for p in DASHBOARD_PERMISSIONS.values() if p.get("is_public", False))

    owners = {}
    for perms in DASHBOARD_PERMISSIONS.values():
        owner = perms.get("owner", "unknown")
        owners[owner] = owners.get(owner, 0) + 1

    total_permissions = sum(len(p.get("permissions", [])) for p in DASHBOARD_PERMISSIONS.values())

    return {
        "total_dashboards": total_dashboards,
        "public_dashboards": public_dashboards,
        "private_dashboards": total_dashboards - public_dashboards,
        "total_permissions": total_permissions,
        "owners": owners
    }


def cleanup_dashboard_permissions(dashboard_id: str) -> bool:
    """Remove all permissions for a deleted dashboard."""
    load_permissions()

    if dashboard_id in DASHBOARD_PERMISSIONS:
        del DASHBOARD_PERMISSIONS[dashboard_id]
        save_permissions()
        return True

    return False
