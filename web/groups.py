"""Groups: named sets of users (local or external: LDAP / OIDC accounts alike) that access can be granted to instead of to individuals.

A user's effective access to a resource is the highest of what they hold directly and what any of their groups hold. Groups add access,
they never remove it, and they never change a user's *role* (admin / power_user / user): the role still decides what kind of user someone
is, groups decide which resources they can reach.

Tables (in auth.db, next to `users` and `catalog_permissions`)
  user_groups          id, name (unique, case-insensitive), description
  user_group_members   (group_id, user_id): one row per member
  resource_grants      (resource_type, resource_id, principal_type user|group, principal_id, permission): generic grants used by resources that
                       had no sharing of their own (saved queries, pipelines). Catalogs keep `catalog_permissions` (a group is stored there as
                       user_id `group:<id>`), dashboards keep their own permission file (an entry with a `group` key).

Everything that changes membership or grants is written to the governance audit log (`GROUP_*`, `GRANT_*`; ids and names, never secrets).
"""
import datetime
import logging
import re
import sqlite3
import uuid
from typing import Any, Dict, List, Optional, Set

from web.auth import get_db_connection

logger = logging.getLogger("localspark.groups")

NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.\-]{0,59}$")
# Permission ladders per generic resource type (lowest first). A higher entry includes the lower ones.
RESOURCE_TYPES: Dict[str, tuple] = {
    "saved_query": ("VIEW", "EDIT"),
    "pipeline": ("RUN", "MANAGE"),
}


class GroupError(ValueError):
    """Invalid input or a refused operation; the message is safe to show."""


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _conn() -> sqlite3.Connection:
    conn = get_db_connection()
    conn.execute("""CREATE TABLE IF NOT EXISTS user_groups (
        id TEXT PRIMARY KEY, name TEXT NOT NULL COLLATE NOCASE UNIQUE, description TEXT DEFAULT '', created_by TEXT, created_at TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS user_group_members (
        group_id TEXT NOT NULL, user_id TEXT NOT NULL, added_by TEXT, added_at TEXT, PRIMARY KEY (group_id, user_id))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_group_members_user ON user_group_members(user_id)")
    conn.execute("""CREATE TABLE IF NOT EXISTS resource_grants (
        id INTEGER PRIMARY KEY AUTOINCREMENT, resource_type TEXT NOT NULL, resource_id TEXT NOT NULL,
        principal_type TEXT NOT NULL CHECK(principal_type IN ('user', 'group')), principal_id TEXT NOT NULL,
        permission TEXT NOT NULL, granted_by TEXT, created_at TEXT,
        UNIQUE(resource_type, resource_id, principal_type, principal_id))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_grants_principal ON resource_grants(principal_type, principal_id, resource_type)")
    conn.commit()
    return conn


def _audit(actor: str, action: str, obj: str, detail: Dict[str, Any]) -> None:
    try:
        from web.governance import store
        store.init_governance_db()
        c = store.get_db()
        try:
            store.write_audit(c, actor, action, obj, detail)
            c.commit()
        finally:
            c.close()
    except Exception as exc:
        logger.warning(f"could not audit {action}: {exc}")


# ---------------------------------------------------------------- groups

def _public(row) -> Dict[str, Any]:
    return {"id": row["id"], "name": row["name"], "description": row["description"] or "", "created_by": row["created_by"], "created_at": row["created_at"]}


def list_groups() -> List[Dict[str, Any]]:
    c = _conn()
    try:
        counts = {r[0]: r[1] for r in c.execute("""SELECT m.group_id, COUNT(*) FROM user_group_members m JOIN users u ON u.id = m.user_id
                                                   WHERE u.deleted_at IS NULL GROUP BY m.group_id""")}
        return [{**_public(r), "member_count": counts.get(r["id"], 0)} for r in c.execute("SELECT * FROM user_groups ORDER BY name COLLATE NOCASE")]
    finally:
        c.close()


def get_group(group_id: str) -> Optional[Dict[str, Any]]:
    c = _conn()
    try:
        r = c.execute("SELECT * FROM user_groups WHERE id = ?", (group_id,)).fetchone()
        return _public(r) if r else None
    finally:
        c.close()


def _clean_name(name: str) -> str:
    name = (name or "").strip()
    if not NAME_RE.match(name):
        raise GroupError("The group name must be 1-60 characters: letters, digits, spaces, '_', '.' or '-', starting with a letter or digit.")
    return name


def create_group(name: str, description: str, actor: str) -> Dict[str, Any]:
    name = _clean_name(name)
    c = _conn()
    try:
        if c.execute("SELECT 1 FROM user_groups WHERE name = ?", (name,)).fetchone():
            raise GroupError(f"A group named '{name}' already exists.")
        gid = f"grp_{uuid.uuid4().hex[:8]}"
        c.execute("INSERT INTO user_groups VALUES (?,?,?,?,?)", (gid, name, (description or "").strip()[:300], actor, _now()))
        c.commit()
    finally:
        c.close()
    _audit(actor, "GROUP_CREATE", f"group:{name}", {"group_id": gid})
    return get_group(gid)


def update_group(group_id: str, name: Optional[str], description: Optional[str], actor: str) -> Dict[str, Any]:
    cur = get_group(group_id)
    if not cur:
        raise LookupError("Group not found.")
    new_name = _clean_name(name) if name is not None else cur["name"]
    c = _conn()
    try:
        clash = c.execute("SELECT id FROM user_groups WHERE name = ? AND id != ?", (new_name, group_id)).fetchone()
        if clash:
            raise GroupError(f"A group named '{new_name}' already exists.")
        c.execute("UPDATE user_groups SET name = ?, description = ? WHERE id = ?",
                  (new_name, cur["description"] if description is None else description.strip()[:300], group_id))
        c.commit()
    finally:
        c.close()
    _audit(actor, "GROUP_UPDATE", f"group:{new_name}", {"group_id": group_id, "was": cur["name"]})
    return get_group(group_id)


def delete_group(group_id: str, actor: str) -> None:
    """Deleting a group removes every grant it held, so nobody keeps access through a group that no longer exists."""
    cur = get_group(group_id)
    if not cur:
        raise LookupError("Group not found.")
    c = _conn()
    try:
        c.execute("DELETE FROM user_group_members WHERE group_id = ?", (group_id,))
        c.execute("DELETE FROM resource_grants WHERE principal_type = 'group' AND principal_id = ?", (group_id,))
        c.execute("DELETE FROM catalog_permissions WHERE user_id = ?", (f"group:{group_id}",))
        c.execute("DELETE FROM user_groups WHERE id = ?", (group_id,))
        c.commit()
    finally:
        c.close()
    try:
        from web import dashboard_permissions
        dashboard_permissions.remove_group_everywhere(group_id)
    except Exception as exc:
        logger.warning(f"dashboard permissions of group {group_id} not cleaned: {exc}")
    _audit(actor, "GROUP_DELETE", f"group:{cur['name']}", {"group_id": group_id})


# ---------------------------------------------------------------- membership

def list_members(group_id: str) -> List[Dict[str, Any]]:
    c = _conn()
    try:
        rows = c.execute("""SELECT u.id, u.username, u.display_name, u.role, u.auth_source, u.is_active, m.added_by, m.added_at
                            FROM user_group_members m JOIN users u ON u.id = m.user_id
                            WHERE m.group_id = ? AND u.deleted_at IS NULL ORDER BY u.username""", (group_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        c.close()


def add_members(group_id: str, user_ids: List[str], actor: str) -> List[Dict[str, Any]]:
    cur = get_group(group_id)
    if not cur:
        raise LookupError("Group not found.")
    c = _conn()
    added = []
    try:
        for uid in dict.fromkeys(user_ids or []):
            u = c.execute("SELECT id, username FROM users WHERE (id = ? OR username = ?) AND deleted_at IS NULL", (uid, uid)).fetchone()
            if not u:
                raise GroupError(f"User '{uid}' does not exist.")
            if c.execute("INSERT OR IGNORE INTO user_group_members VALUES (?,?,?,?)", (group_id, u["id"], actor, _now())).rowcount:
                added.append(u["username"])
        c.commit()
    finally:
        c.close()
    if added:
        _audit(actor, "GROUP_MEMBER_ADD", f"group:{cur['name']}", {"group_id": group_id, "users": added})
    return list_members(group_id)


def remove_member(group_id: str, user_id: str, actor: str) -> None:
    cur = get_group(group_id)
    if not cur:
        raise LookupError("Group not found.")
    c = _conn()
    try:
        u = c.execute("SELECT id, username FROM users WHERE id = ? OR username = ?", (user_id, user_id)).fetchone()
        if not u or not c.execute("DELETE FROM user_group_members WHERE group_id = ? AND user_id = ?", (group_id, u["id"])).rowcount:
            raise LookupError("That user is not a member of the group.")
        c.commit()
    finally:
        c.close()
    _audit(actor, "GROUP_MEMBER_REMOVE", f"group:{cur['name']}", {"group_id": group_id, "user": u["username"]})


def groups_of_user(user_id: str) -> List[Dict[str, Any]]:
    c = _conn()
    try:
        return [{"id": r["id"], "name": r["name"]} for r in c.execute(
            "SELECT g.id, g.name FROM user_group_members m JOIN user_groups g ON g.id = m.group_id WHERE m.user_id = ? ORDER BY g.name", (user_id,))]
    finally:
        c.close()


def group_ids_for(user: Dict[str, Any]) -> Set[str]:
    """Ids of every group the user belongs to (by id, falling back to username for callers that only carry that)."""
    if not user:
        return set()
    c = _conn()
    try:
        uid = user.get("id")
        if not uid and user.get("username"):
            r = c.execute("SELECT id FROM users WHERE username = ?", (user["username"],)).fetchone()
            uid = r["id"] if r else None
        return {r[0] for r in c.execute("SELECT group_id FROM user_group_members WHERE user_id = ?", (uid,))} if uid else set()
    finally:
        c.close()


def group_ids_for_username(username: str) -> Set[str]:
    return group_ids_for({"username": username})


def memberships_by_user() -> Dict[str, List[Dict[str, str]]]:
    """{user_id: [{id, name}, ...]} for the users table (one query)."""
    c = _conn()
    try:
        out: Dict[str, List[Dict[str, str]]] = {}
        for r in c.execute("SELECT m.user_id, g.id, g.name FROM user_group_members m JOIN user_groups g ON g.id = m.group_id ORDER BY g.name"):
            out.setdefault(r["user_id"], []).append({"id": r["id"], "name": r["name"]})
        return out
    finally:
        c.close()


# ---------------------------------------------------------------- generic resource grants

def _ladder(resource_type: str) -> tuple:
    if resource_type not in RESOURCE_TYPES:
        raise GroupError(f"Unknown resource type '{resource_type}'.")
    return RESOURCE_TYPES[resource_type]


def _parse_principal(principal: str) -> tuple:
    kind, _, pid = (principal or "").partition(":")
    if kind not in ("user", "group") or not pid:
        raise GroupError("A principal looks like user:<id> or group:<id>.")
    return kind, pid


def grant(resource_type: str, resource_id: str, principal: str, permission: str, actor: str) -> Dict[str, Any]:
    ladder = _ladder(resource_type)
    permission = (permission or "").strip().upper()
    if permission not in ladder:
        raise GroupError(f"The permission for a {resource_type.replace('_', ' ')} must be one of {', '.join(ladder)}.")
    kind, pid = _parse_principal(principal)
    c = _conn()
    try:
        if kind == "group":
            g = c.execute("SELECT id, name FROM user_groups WHERE id = ?", (pid,)).fetchone()
            if not g:
                raise GroupError("That group does not exist.")
            label = g["name"]
        else:
            u = c.execute("SELECT id, username FROM users WHERE (id = ? OR username = ?) AND deleted_at IS NULL", (pid, pid)).fetchone()
            if not u:
                raise GroupError("That user does not exist.")
            pid, label = u["id"], u["username"]
        c.execute("""INSERT INTO resource_grants (resource_type, resource_id, principal_type, principal_id, permission, granted_by, created_at)
                     VALUES (?,?,?,?,?,?,?) ON CONFLICT(resource_type, resource_id, principal_type, principal_id)
                     DO UPDATE SET permission = excluded.permission, granted_by = excluded.granted_by, created_at = excluded.created_at""",
                  (resource_type, resource_id, kind, pid, permission, actor, _now()))
        c.commit()
    finally:
        c.close()
    _audit(actor, "GRANT_SET", f"{resource_type}:{resource_id}", {"principal": f"{kind}:{label}", "permission": permission})
    return {"resource_type": resource_type, "resource_id": resource_id, "principal_type": kind, "principal_id": pid, "permission": permission}


def revoke(resource_type: str, resource_id: str, principal: str, actor: str) -> bool:
    _ladder(resource_type)
    kind, pid = _parse_principal(principal)
    c = _conn()
    try:
        if kind == "user":
            u = c.execute("SELECT id FROM users WHERE id = ? OR username = ?", (pid, pid)).fetchone()
            pid = u["id"] if u else pid
        n = c.execute("DELETE FROM resource_grants WHERE resource_type = ? AND resource_id = ? AND principal_type = ? AND principal_id = ?",
                      (resource_type, resource_id, kind, pid)).rowcount
        c.commit()
    finally:
        c.close()
    if n:
        _audit(actor, "GRANT_REVOKE", f"{resource_type}:{resource_id}", {"principal": f"{kind}:{pid}"})
    return bool(n)


def list_grants(resource_type: str, resource_id: str) -> List[Dict[str, Any]]:
    _ladder(resource_type)
    c = _conn()
    try:
        out = []
        for r in c.execute("SELECT * FROM resource_grants WHERE resource_type = ? AND resource_id = ? ORDER BY created_at", (resource_type, resource_id)):
            if r["principal_type"] == "group":
                g = c.execute("SELECT name FROM user_groups WHERE id = ?", (r["principal_id"],)).fetchone()
                name, extra = (g["name"] if g else r["principal_id"]), {}
            else:
                u = c.execute("SELECT username, display_name FROM users WHERE id = ?", (r["principal_id"],)).fetchone()
                name, extra = (u["username"] if u else r["principal_id"]), {"display_name": u["display_name"] if u else None}
            out.append({"principal": f"{r['principal_type']}:{r['principal_id']}", "principal_type": r["principal_type"], "name": name,
                        "permission": r["permission"], "granted_by": r["granted_by"], "created_at": r["created_at"], **extra})
        return out
    finally:
        c.close()


def permission_of(user: Dict[str, Any], resource_type: str, resource_id: str) -> Optional[str]:
    """The highest permission the user holds on the resource, directly or through a group; None if none."""
    ladder = _ladder(resource_type)
    gids = group_ids_for(user)
    uid = (user or {}).get("id")
    c = _conn()
    try:
        best = -1
        for r in c.execute("SELECT principal_type, principal_id, permission FROM resource_grants WHERE resource_type = ? AND resource_id = ?", (resource_type, resource_id)):
            if (r["principal_type"] == "user" and r["principal_id"] == uid) or (r["principal_type"] == "group" and r["principal_id"] in gids):
                if r["permission"] in ladder:
                    best = max(best, ladder.index(r["permission"]))
        return ladder[best] if best >= 0 else None
    finally:
        c.close()


def has_permission(user: Dict[str, Any], resource_type: str, resource_id: str, needed: str) -> bool:
    ladder = _ladder(resource_type)
    have = permission_of(user, resource_type, resource_id)
    return have is not None and ladder.index(have) >= ladder.index(needed)


def granted_ids(user: Dict[str, Any], resource_type: str, at_least: str) -> Set[str]:
    """Ids of every resource of the type the user holds `at_least` on, directly or through a group."""
    ladder = _ladder(resource_type)
    allowed = ladder[ladder.index(at_least):]
    gids = group_ids_for(user)
    uid = (user or {}).get("id")
    c = _conn()
    try:
        out: Set[str] = set()
        for r in c.execute("SELECT resource_id, principal_type, principal_id FROM resource_grants WHERE resource_type = ? AND permission IN (%s)"
                           % ",".join("?" * len(allowed)), (resource_type, *allowed)):
            if (r["principal_type"] == "user" and r["principal_id"] == uid) or (r["principal_type"] == "group" and r["principal_id"] in gids):
                out.add(r["resource_id"])
        return out
    finally:
        c.close()


def delete_grants_for_resource(resource_type: str, resource_id: str) -> None:
    c = _conn()
    try:
        c.execute("DELETE FROM resource_grants WHERE resource_type = ? AND resource_id = ?", (resource_type, resource_id))
        c.commit()
    finally:
        c.close()
