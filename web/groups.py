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

Directory sync: a group can be *mapped* to an LDAP group (its DN) or an OIDC group-claim value. Members of the directory group are added to
the platform group by the sign-in / sync of that identity source (`sync_external_memberships`), marked origin `sync`; they leave it when the
directory says so. Manual members (origin `manual`) are never touched by sync, and a synced member cannot be removed by hand (the directory is
the source of truth). Sync is skipped, never destructive, when the directory could not be asked.

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
    # Table- and schema-level data access (web/table_access.py). The id is `catalog.schema.table` / `catalog.schema`, lower-case.
    # SELECT reads; MODIFY also writes. They only ever ADD to what catalog-level access already gives.
    "table": ("SELECT", "MODIFY"),
    "schema": ("SELECT", "MODIFY"),
}
_ID_PARTS = {"table": 3, "schema": 2}
_ID_PART_RE = re.compile(r"^[a-z0-9_]{1,128}$")


def _rid(resource_type: str, resource_id: str) -> str:
    """Canonical resource id. Table and schema ids are validated and lower-cased (identifiers are case-insensitive)."""
    if resource_type in _ID_PARTS:
        rid = (resource_id or "").strip().lower()
        parts = rid.split(".")
        if len(parts) != _ID_PARTS[resource_type] or not all(_ID_PART_RE.match(p) for p in parts):
            raise GroupError("A " + resource_type + " is named " + ("catalog.schema.table" if resource_type == "table" else "catalog.schema")
                             + " (letters, digits and underscores).")
        return rid
    return resource_id


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
    gcols = {r[1] for r in conn.execute("PRAGMA table_info(user_groups)")}
    if "source" not in gcols:                       # directory mapping: 'local' (none) | 'ldap' | 'oidc'
        conn.execute("ALTER TABLE user_groups ADD COLUMN source TEXT NOT NULL DEFAULT 'local'")
    if "external_ref" not in gcols:                 # normalised LDAP group DN / OIDC claim value
        conn.execute("ALTER TABLE user_groups ADD COLUMN external_ref TEXT")
    if "external_label" not in gcols:               # the value as the admin entered it (shown in the UI)
        conn.execute("ALTER TABLE user_groups ADD COLUMN external_label TEXT")
    mcols = {r[1] for r in conn.execute("PRAGMA table_info(user_group_members)")}
    if "origin" not in mcols:                       # 'manual' (added by an admin) | 'sync' (from the directory)
        conn.execute("ALTER TABLE user_group_members ADD COLUMN origin TEXT NOT NULL DEFAULT 'manual'")
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
    return {"id": row["id"], "name": row["name"], "description": row["description"] or "", "created_by": row["created_by"], "created_at": row["created_at"],
            "source": row["source"] or "local", "external_ref": row["external_label"] or row["external_ref"] or ""}


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
        c.execute("INSERT INTO user_groups (id, name, description, created_by, created_at) VALUES (?,?,?,?,?)", (gid, name, (description or "").strip()[:300], actor, _now()))
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
    try:
        from web.governance import policies
        policies.remove_group_everywhere(group_id, actor)
    except Exception as exc:
        logger.warning(f"governance policies of group {group_id} not cleaned: {exc}")
    _audit(actor, "GROUP_DELETE", f"group:{cur['name']}", {"group_id": group_id})


# ---------------------------------------------------------------- membership

def list_members(group_id: str) -> List[Dict[str, Any]]:
    c = _conn()
    try:
        rows = c.execute("""SELECT u.id, u.username, u.display_name, u.role, u.auth_source, u.is_active, m.added_by, m.added_at, m.origin
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
            if c.execute("INSERT OR IGNORE INTO user_group_members (group_id, user_id, added_by, added_at) VALUES (?,?,?,?)", (group_id, u["id"], actor, _now())).rowcount:
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
        row = c.execute("SELECT origin FROM user_group_members WHERE group_id = ? AND user_id = ?", (group_id, u["id"])).fetchone() if u else None
        if not row:
            raise LookupError("That user is not a member of the group.")
        if row["origin"] == "sync":
            raise GroupError("This member comes from the directory group this group is mapped to. Remove them from the directory group "
                             "(or clear the mapping); a manual removal would be undone by the next sync.")
        c.execute("DELETE FROM user_group_members WHERE group_id = ? AND user_id = ?", (group_id, u["id"]))
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


# ---------------------------------------------------------------- directory mapping and sync

SOURCES = ("local", "ldap", "oidc")


def normalise_ref(source: str, ref: str) -> str:
    """Comparable form of a directory reference: an LDAP DN ignores case and spaces around ',' and '='; an OIDC value ignores case."""
    ref = (ref or "").strip().lower()
    return re.sub(r"\s*([,=])\s*", r"\1", ref) if source == "ldap" else ref


def set_mapping(group_id: str, source: str, external_ref: str, actor: str) -> Dict[str, Any]:
    """Maps the group to an LDAP group DN / OIDC group value, or clears the mapping (source 'local'). Clearing removes the members that
    came from the directory (manual members stay): without the mapping nothing would ever remove them when they leave the directory."""
    cur = get_group(group_id)
    if not cur:
        raise LookupError("Group not found.")
    source = (source or "local").strip().lower()
    if source not in SOURCES:
        raise GroupError(f"The source must be one of {', '.join(SOURCES)}.")
    label = (external_ref or "").strip()
    ref = normalise_ref(source, label)
    if source != "local" and not ref:
        raise GroupError("Enter the directory group (an LDAP group DN, or the OIDC group value) this group is mapped to.")
    if len(label) > 500:
        raise GroupError("The directory reference is too long.")
    c = _conn()
    try:
        if source != "local":
            clash = c.execute("SELECT name FROM user_groups WHERE source = ? AND external_ref = ? AND id != ?", (source, ref, group_id)).fetchone()
            if clash:
                raise GroupError(f"'{clash['name']}' is already mapped to that directory group.")
        removed = 0
        if source == "local" or (cur["source"] != "local" and (cur["source"] != source or normalise_ref(source, cur["external_ref"]) != ref)):
            removed = c.execute("DELETE FROM user_group_members WHERE group_id = ? AND origin = 'sync'", (group_id,)).rowcount
        c.execute("UPDATE user_groups SET source = ?, external_ref = ?, external_label = ? WHERE id = ?",
                  (source, ref or None, label or None, group_id))
        c.commit()
    finally:
        c.close()
    _audit(actor, "GROUP_MAPPING", f"group:{cur['name']}", {"group_id": group_id, "source": source, "external_ref": label, "synced_members_removed": removed})
    return get_group(group_id)


def sync_external_memberships(user_id: str, source: str, refs, actor: str = "directory-sync") -> Dict[str, List[str]]:
    """Makes the user's *synced* memberships in every group mapped to `source` match the directory groups they are in now (`refs`: LDAP group
    DNs or OIDC group values). Adds what is missing, removes only synced rows that no longer apply; manual memberships are never touched.
    Callers pass `refs` only when the directory was actually asked: 'could not ask' must not look like 'in no groups'."""
    if source not in ("ldap", "oidc"):
        raise GroupError("Unknown directory source.")
    have = {normalise_ref(source, r) for r in (refs or []) if r}
    added: List[str] = []
    removed: List[str] = []
    c = _conn()
    try:
        u = c.execute("SELECT username FROM users WHERE id = ?", (user_id,)).fetchone()
        for g in c.execute("SELECT id, name, external_ref FROM user_groups WHERE source = ? AND external_ref IS NOT NULL", (source,)).fetchall():
            row = c.execute("SELECT origin FROM user_group_members WHERE group_id = ? AND user_id = ?", (g["id"], user_id)).fetchone()
            if g["external_ref"] in have:
                if row is None:
                    c.execute("INSERT INTO user_group_members (group_id, user_id, added_by, added_at, origin) VALUES (?,?,?,?,'sync')", (g["id"], user_id, actor, _now()))
                    added.append(g["name"])
            elif row is not None and row["origin"] == "sync":
                c.execute("DELETE FROM user_group_members WHERE group_id = ? AND user_id = ?", (g["id"], user_id))
                removed.append(g["name"])
        c.commit()
    finally:
        c.close()
    if added or removed:
        _audit(actor, "GROUP_SYNC", f"user:{u['username'] if u else user_id}", {"source": source, "added": added, "removed": removed})
    return {"added": added, "removed": removed}


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
    resource_id = _rid(resource_type, resource_id)
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
    resource_id = _rid(resource_type, resource_id)
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
    resource_id = _rid(resource_type, resource_id)
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
    resource_id = _rid(resource_type, resource_id) if resource_type in _ID_PARTS else resource_id
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


def granted_map(user: Dict[str, Any], resource_type: str) -> Dict[str, str]:
    """{resource_id: highest permission} of every resource of the type the user holds anything on, directly or through a group (one query)."""
    ladder = _ladder(resource_type)
    gids = group_ids_for(user)
    uid = (user or {}).get("id")
    c = _conn()
    try:
        best: Dict[str, int] = {}
        for r in c.execute("SELECT resource_id, principal_type, principal_id, permission FROM resource_grants WHERE resource_type = ?", (resource_type,)):
            if r["permission"] in ladder and ((r["principal_type"] == "user" and r["principal_id"] == uid) or (r["principal_type"] == "group" and r["principal_id"] in gids)):
                best[r["resource_id"]] = max(best.get(r["resource_id"], -1), ladder.index(r["permission"]))
        return {rid: ladder[i] for rid, i in best.items()}
    finally:
        c.close()


def list_grants_prefix(resource_types, prefix: str) -> List[Dict[str, Any]]:
    """Every grant of the given types whose resource id starts with `prefix` (e.g. 'sales.'): the grants of one catalog."""
    c = _conn()
    try:
        out = []
        for rt in resource_types:
            for r in c.execute("SELECT resource_id FROM resource_grants WHERE resource_type = ? AND resource_id LIKE ? ESCAPE '\\' GROUP BY resource_id ORDER BY resource_id",
                               (rt, prefix.replace("_", "\\_") + "%")):
                for g in list_grants(rt, r["resource_id"]):
                    out.append({**g, "resource_type": rt, "resource_id": r["resource_id"]})
        return out
    finally:
        c.close()


def delete_grants_prefix(resource_types, prefix: str) -> None:
    """Removes the grants under a prefix: a dropped table (`cat.schema.table`) or a deleted catalog (`cat.`)."""
    c = _conn()
    try:
        for rt in resource_types:
            c.execute("DELETE FROM resource_grants WHERE resource_type = ? AND (resource_id = ? OR resource_id LIKE ? ESCAPE '\\')",
                      (rt, prefix.rstrip("."), prefix.rstrip(".").replace("_", "\\_") + ".%"))
        c.commit()
    finally:
        c.close()
