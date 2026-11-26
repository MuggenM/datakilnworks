"""
Masking policies: CRUD, validation and per-column policy resolution.

A policy points at a *tag*, not at objects: "mask everything tagged pii for everyone except admins". Resolution for a
column and a principal:
  1. keep enabled policies whose tag_key (and tag_value, when set) matches the column's effective tags,
  2. drop policies whose type filter (applies_to_types) does not accept the column's type,
  3. drop policies the principal is exempt from (except_roles / except_users / system principal),
  4. the lowest `priority` number wins; equal priorities go to the more restrictive mask, then to the name.
"""

import json
import re
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from web.governance import masks, store, tags

ROLES = ("admin", "power_user", "user")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,99}$")
SYSTEM_USERNAME = "system"


class NotFound(tags.NotFound):
    """Unknown policy id (HTTP 404)."""


@dataclass
class Principal:
    username: str
    role: str = "user"
    user_id: str = ""
    is_system: bool = False
    groups: frozenset = frozenset()            # ids of the user's groups (web/groups.py): they can exempt from a policy or carry attributes

    @classmethod
    def from_user(cls, user: Dict[str, Any]) -> "Principal":
        try:
            from web import groups
            member_of = frozenset(groups.group_ids_for(user))
        except Exception:
            member_of = frozenset()            # cannot tell: no group-based exemption (the restrictive answer)
        return cls(username=user.get("username", ""), role=user.get("role", "user"), user_id=user.get("id", ""), groups=member_of)

    @classmethod
    def system(cls) -> "Principal":
        """Internal jobs (Auto-Loader, dbt runs, schedulers of trusted work). Never constructible from an HTTP request."""
        return cls(username=SYSTEM_USERNAME, role="admin", is_system=True)


@dataclass
class MaskSpec:
    """One masked column: what replaces it and why."""
    column: str
    data_type: str
    policy_id: str
    policy_name: str
    mask_type: str
    expression: str
    conflicts: List[str] = field(default_factory=list)


def _row(r) -> Dict[str, Any]:
    d = dict(r)
    d["enabled"] = bool(d["enabled"])
    for key in ("applies_to_types", "except_roles", "except_users", "except_groups"):
        d[key] = json.loads(d[key]) if d.get(key) else ([] if key != "applies_to_types" else None)
    return d


def _validate(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalises and validates a full policy record; raises ValueError with a precise message."""
    name = (data.get("name") or "").strip()
    if not NAME_RE.match(name):
        raise ValueError("Policy names are 1-100 characters (letters, digits, space, '_', '.', '-').")
    tag_key = tags.norm(data.get("tag_key"))
    definition = tags.get_definition(tag_key)
    tag_value = data.get("tag_value")
    tag_value = None if tag_value in (None, "") else str(tag_value).strip()
    if tag_value is not None and definition["allowed_values"] and tag_value not in definition["allowed_values"]:
        raise ValueError(f"'{tag_value}' is not an allowed value for tag '{tag_key}'.")
    mask_type = (data.get("mask_type") or "").strip().lower()
    if mask_type not in masks.MASK_TYPES:
        raise ValueError(f"mask_type must be one of: {', '.join(masks.MASK_TYPES)}.")
    applies = data.get("applies_to_types")
    if applies:
        bad = [f for f in applies if f not in masks.FAMILIES]
        if bad:
            raise ValueError(f"Unknown type families {bad}. Use: {', '.join(masks.FAMILIES)}.")
        applies = sorted(set(applies))
    else:
        applies = None
    mask_expr = (data.get("mask_expr") or "").strip() or None
    if mask_type == "custom":
        verdict = masks.validate_custom_expression(mask_expr or "", applies)
        if not verdict["ok"]:
            raise ValueError("Invalid custom expression: " + " ".join(verdict["errors"]))
    else:
        mask_expr = None
    except_roles = data.get("except_roles", ["admin"])
    if not isinstance(except_roles, list) or any(r not in ROLES for r in except_roles):
        raise ValueError(f"except_roles must be a list of: {', '.join(ROLES)}.")
    except_users = data.get("except_users", [])
    if not isinstance(except_users, list) or any(not isinstance(u, str) or not u.strip() for u in except_users):
        raise ValueError("except_users must be a list of usernames.")
    except_groups = clean_group_ids(data.get("except_groups", []))
    try:
        priority = int(data.get("priority", 100))
    except (TypeError, ValueError):
        raise ValueError("priority must be an integer.")
    if not 0 <= priority <= 10000:
        raise ValueError("priority must be between 0 and 10000 (lower number wins).")
    return {"name": name, "description": (data.get("description") or "").strip(), "tag_key": tag_key, "tag_value": tag_value,
            "mask_type": mask_type, "mask_expr": mask_expr, "applies_to_types": applies,
            "except_roles": sorted(set(except_roles)), "except_users": sorted({u.strip() for u in except_users}),
            "except_groups": except_groups, "priority": priority, "enabled": bool(data.get("enabled", True))}


def create_policy(data: Dict[str, Any], actor: str = "admin") -> Dict[str, Any]:
    store.init_governance_db()
    rec = _validate(data)
    pid = f"pol_{uuid.uuid4().hex[:8]}"
    now = store.utcnow()
    conn = store.get_db()
    try:
        if conn.execute("SELECT 1 FROM masking_policies WHERE name = ?", (rec["name"],)).fetchone():
            raise ValueError(f"A policy named '{rec['name']}' already exists.")
        conn.execute("""
            INSERT INTO masking_policies (id, name, description, tag_key, tag_value, mask_type, mask_expr, applies_to_types,
                                          except_roles, except_users, except_groups, priority, enabled, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (pid, rec["name"], rec["description"], rec["tag_key"], rec["tag_value"], rec["mask_type"], rec["mask_expr"],
              json.dumps(rec["applies_to_types"]) if rec["applies_to_types"] else None,
              json.dumps(rec["except_roles"]), json.dumps(rec["except_users"]), json.dumps(rec["except_groups"]),
              rec["priority"], int(rec["enabled"]), actor, now, now))
        store.bump_version(conn)
        store.write_audit(conn, actor, "POLICY_CREATE", pid, rec)
        conn.commit()
    finally:
        conn.close()
    return get_policy(pid)


def get_policy(policy_id: str) -> Dict[str, Any]:
    conn = store.get_db()
    try:
        row = conn.execute("SELECT * FROM masking_policies WHERE id = ?", (policy_id,)).fetchone()
        if not row:
            raise NotFound(f"Policy '{policy_id}' does not exist.")
        return _row(row)
    finally:
        conn.close()


def list_policies(enabled_only: bool = False) -> List[Dict[str, Any]]:
    store.init_governance_db()
    conn = store.get_db()
    try:
        where = "WHERE enabled = 1" if enabled_only else ""
        return [_row(r) for r in conn.execute(f"SELECT * FROM masking_policies {where} ORDER BY priority, name").fetchall()]
    finally:
        conn.close()


def update_policy(policy_id: str, changes: Dict[str, Any], actor: str = "admin") -> Dict[str, Any]:
    current = get_policy(policy_id)
    merged = {**current, **{k: v for k, v in changes.items() if k in current and k not in ("id", "created_by", "created_at", "updated_at")}}
    rec = _validate(merged)
    conn = store.get_db()
    try:
        clash = conn.execute("SELECT id FROM masking_policies WHERE name = ? AND id != ?", (rec["name"], policy_id)).fetchone()
        if clash:
            raise ValueError(f"A policy named '{rec['name']}' already exists.")
        conn.execute("""
            UPDATE masking_policies SET name=?, description=?, tag_key=?, tag_value=?, mask_type=?, mask_expr=?,
                   applies_to_types=?, except_roles=?, except_users=?, except_groups=?, priority=?, enabled=?, updated_at=? WHERE id=?
        """, (rec["name"], rec["description"], rec["tag_key"], rec["tag_value"], rec["mask_type"], rec["mask_expr"],
              json.dumps(rec["applies_to_types"]) if rec["applies_to_types"] else None,
              json.dumps(rec["except_roles"]), json.dumps(rec["except_users"]), json.dumps(rec["except_groups"]),
              rec["priority"], int(rec["enabled"]), store.utcnow(), policy_id))
        store.bump_version(conn)
        store.write_audit(conn, actor, "POLICY_UPDATE", policy_id, {"before": {k: current[k] for k in rec if k in current}, "after": rec})
        conn.commit()
    finally:
        conn.close()
    return get_policy(policy_id)


def delete_policy(policy_id: str, actor: str = "admin") -> None:
    current = get_policy(policy_id)
    conn = store.get_db()
    try:
        conn.execute("DELETE FROM masking_policies WHERE id = ?", (policy_id,))
        store.bump_version(conn)
        store.write_audit(conn, actor, "POLICY_DELETE", policy_id, {"name": current["name"]})
        conn.commit()
    finally:
        conn.close()


# ----------------------------------------------------------------------------
# Resolution (cached against the governance version)
# ----------------------------------------------------------------------------

_lock = threading.Lock()
_cache: Dict[str, Any] = {"version": -1, "policies": []}


def enabled_policies() -> List[Dict[str, Any]]:
    version = store.get_version()
    with _lock:
        if _cache["version"] != version:
            _cache["policies"] = list_policies(enabled_only=True)
            _cache["version"] = version
        return _cache["policies"]


def is_exempt(policy: Dict[str, Any], principal: Principal) -> bool:
    """Exempt when the principal is the system, has an exempt role, is an exempt user, or belongs to an exempt group (any one is enough)."""
    return (principal.is_system or principal.role in policy["except_roles"] or principal.username in policy["except_users"]
            or bool(principal.groups and principal.groups.intersection(policy.get("except_groups") or ())))


def clean_group_ids(value: Any) -> List[str]:
    """Validates a list of group ids (they must exist) for `except_groups`; returns them sorted and unique."""
    if value in (None, ""):
        return []
    if not isinstance(value, list) or any(not isinstance(g, str) or not g.strip() for g in value):
        raise ValueError("except_groups must be a list of group ids.")
    from web import groups
    ids = sorted({g.strip() for g in value})
    missing = [g for g in ids if not groups.get_group(g)]
    if missing:
        raise ValueError(f"Unknown group(s): {', '.join(missing)}.")
    return ids


def remove_group_everywhere(group_id: str, actor: str = "system") -> None:
    """A deleted group stops exempting from policies (the restrictive direction) and loses its row-filter attributes."""
    store.init_governance_db()
    conn = store.get_db()
    try:
        changed = False
        for table in ("masking_policies", "row_policies"):
            for r in conn.execute(f"SELECT id, except_groups FROM {table}").fetchall():
                ids = json.loads(r["except_groups"] or "[]")
                if group_id in ids:
                    conn.execute(f"UPDATE {table} SET except_groups = ? WHERE id = ?", (json.dumps([g for g in ids if g != group_id]), r["id"]))
                    changed = True
        if conn.execute("DELETE FROM principal_attributes WHERE principal_type = 'group' AND principal_value = ?", (group_id,)).rowcount:
            changed = True
        if changed:
            store.bump_version(conn)
            store.write_audit(conn, actor, "GROUP_REMOVED_FROM_POLICIES", f"group:{group_id}", {})
        conn.commit()
    finally:
        conn.close()


def resolve_column_policy(column: str, data_type: str, effective: Dict[str, Dict[str, str]],
                          principal: Principal) -> Optional[MaskSpec]:
    """The winning mask for one column, or None when it is visible to `principal`."""
    family = masks.type_family(data_type)
    candidates = []
    for pol in enabled_policies():
        tagged = effective.get(pol["tag_key"])
        if tagged is None:
            continue
        if pol["tag_value"] is not None and tagged["value"] != pol["tag_value"]:
            continue
        if pol["applies_to_types"] and family not in pol["applies_to_types"]:
            continue
        if is_exempt(pol, principal):
            continue
        candidates.append(pol)
    if not candidates:
        return None
    candidates.sort(key=lambda p: (p["priority"], -masks.RESTRICTIVENESS.get(p["mask_type"], 3), p["name"]))
    winner = candidates[0]
    tied = [p["name"] for p in candidates[1:] if p["priority"] == winner["priority"]]
    return MaskSpec(column=column, data_type=data_type, policy_id=winner["id"], policy_name=winner["name"],
                    mask_type=winner["mask_type"],
                    expression=masks.mask_expression(winner["mask_type"], data_type, column, winner["mask_expr"]),
                    conflicts=tied)


def masks_for_table(catalog: str, schema_name: str, table_name: str, columns: List[Dict[str, Any]],
                    principal: Principal) -> List[MaskSpec]:
    """Masks that apply to `principal` for the given columns ([{column, type}]). Empty when nothing is masked."""
    if not enabled_policies():
        return []
    eff = tags.effective_tags(catalog, schema_name, table_name, [c["column"] for c in columns])
    out = []
    for c in columns:
        spec = resolve_column_policy(c["column"], c["type"], eff[c["column"]], principal)
        if spec:
            out.append(spec)
    return out
