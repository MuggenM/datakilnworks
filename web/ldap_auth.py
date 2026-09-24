"""
Real LDAP / Active Directory authentication: service-account bind, user search, bind-as-user credential
verification, group-to-role mapping, and provisioning/sync of the local `users` row for directory accounts.

`web/auth_frameworks.py` only stores configuration and does a bare TCP/TLS connectivity check
(`test_ldap_connection`); this module is what actually authenticates someone and keeps their local account
(`auth_source='ldap'`) in sync with the directory. Login itself is wired in `web/app.py`'s `/api/auth/login`.

Flow for `authenticate(username, password)`:
  1. bind as the configured service account (`bind_dn`/`bind_password`);
  2. search `user_search_base` (or `base_dn`) with `user_search_filter` (the username is filter-escaped: it is
     attacker-controlled input reaching an LDAP filter, textbook LDAP injection otherwise);
  3. on exactly one match, re-bind *as that user's DN* with the password given -- this is the actual credential
     check, never a comparison against a cached hash, so a password change or account lock in the directory takes
     effect immediately;
  4. search `group_search_base` for groups whose membership includes the user's DN, and map `admin_group`/
     `power_user_group` to a role (first match wins; unmatched groups get `default_role`);
  5. create or update the local user row with `auth_source='ldap'` and a random, never-guessable password hash
     (so `verify_password` can never succeed against it -- an LDAP-sourced account can only ever authenticate
     through this module) via `web.auth.upsert_external_user`.

A username that already exists locally with `auth_source='local'` is refused here (`_LOCAL_ACCOUNT_CONFLICT`):
without this a login attempt for someone else's local username, if it happened to also match a directory account,
could silently take over that account.
"""

import logging
import os
import secrets
from typing import Any, Dict, List, Optional, Tuple

from ldap3 import ALL, SIMPLE, SUBTREE, Connection, Server
from ldap3.core.exceptions import LDAPException
from ldap3.utils.conv import escape_filter_chars

logger = logging.getLogger("localspark.ldap")

DEFAULT_GROUP_MEMBERSHIP_FILTER = "(|(member={user_dn})(uniqueMember={user_dn}))"
_LOCAL_ACCOUNT_CONFLICT = "A local account already uses this username; an administrator must resolve this before it can sign in via LDAP."


class LdapError(Exception):
    """Configuration or connectivity failure (as opposed to a plain bad-credentials result)."""


def _cfg_str(cfg: Dict[str, Any], key: str, default: str = "") -> str:
    return (cfg.get(key) or default).strip() if isinstance(cfg.get(key), str) else default


def _connect(cfg: Dict[str, Any], user: Optional[str] = None, password: Optional[str] = None,
            timeout: float = 5.0) -> Connection:
    host = _cfg_str(cfg, "server_host")
    if not host:
        raise LdapError("LDAP server host is not configured.")
    port = int(cfg.get("server_port") or 389)
    encryption = _cfg_str(cfg, "encryption", "none").lower()
    use_ssl = encryption == "ssl" or port == 636
    server = Server(host, port=port, use_ssl=use_ssl, get_info=ALL, connect_timeout=timeout)
    try:
        conn = Connection(server, user=user, password=password, authentication=SIMPLE if user else None, auto_bind=False)
        if not conn.bind():
            raise LDAPException(conn.result.get("description") or conn.result.get("message") or "bind failed")
        if encryption == "starttls":
            conn.start_tls()
        return conn
    except LDAPException as exc:
        raise LdapError(f"Could not bind to {host}:{port}: {exc}") from exc


def _service_connection(cfg: Dict[str, Any]) -> Connection:
    bind_dn = _cfg_str(cfg, "bind_dn")
    bind_password = cfg.get("bind_password") or ""
    if not bind_dn:
        raise LdapError("A service bind DN is required to search the directory.")
    return _connect(cfg, user=bind_dn, password=bind_password)


def _search_base(cfg: Dict[str, Any], key: str) -> str:
    return _cfg_str(cfg, key) or _cfg_str(cfg, "base_dn")


def find_user(conn: Connection, cfg: Dict[str, Any], username: str) -> Optional[Dict[str, Any]]:
    """The single directory entry matching `username`, or None. Raises LdapError on more than one match."""
    base = _search_base(cfg, "user_search_base")
    if not base:
        raise LdapError("No user search base / base DN is configured.")
    filt = _cfg_str(cfg, "user_search_filter", "(uid={username})")
    if "{username}" not in filt:
        raise LdapError("user_search_filter must contain the {username} placeholder.")
    filt = filt.replace("{username}", escape_filter_chars(username))
    conn.search(base, filt, search_scope=SUBTREE, attributes=["*"])
    if not conn.entries:
        return None
    if len(conn.entries) > 1:
        raise LdapError(f"The user filter matched more than one entry for '{username}'; narrow user_search_filter.")
    entry = conn.entries[0]

    def attr(*names: str) -> str:
        for name in names:
            if name in entry and entry[name].value:
                v = entry[name].value
                return v[0] if isinstance(v, list) else str(v)
        return ""

    return {"dn": entry.entry_dn, "username": attr("uid", "sAMAccountName", "user_id") or username,
            "display_name": attr("cn", "displayName", "display_name") or username,
            "email": attr("mail", "email")}


def find_groups(conn: Connection, cfg: Dict[str, Any], user_dn: str) -> List[str]:
    """DNs of every group `user_dn` belongs to, by reverse membership search (works for both AD- and
    OpenLDAP/lldap-style directories; a directory that instead exposes `memberOf` on the user entry is covered too
    since `find_user` already read every attribute -- `_map_role` also checks that)."""
    base = _search_base(cfg, "group_search_base")
    if not base:
        return []
    filt = _cfg_str(cfg, "group_membership_filter", DEFAULT_GROUP_MEMBERSHIP_FILTER)
    filt = filt.replace("{user_dn}", escape_filter_chars(user_dn))
    try:
        conn.search(base, filt, search_scope=SUBTREE, attributes=["cn"])
    except LDAPException as exc:
        logger.warning(f"LDAP group search failed: {exc}")
        return []
    return [e.entry_dn for e in conn.entries]


def _map_role(cfg: Dict[str, Any], group_dns: List[str]) -> str:
    admin_group = _cfg_str(cfg, "admin_group").lower()
    power_group = _cfg_str(cfg, "power_user_group").lower()
    lowered = {g.lower() for g in group_dns}
    if admin_group and admin_group in lowered:
        return "admin"
    if power_group and power_group in lowered:
        return "power_user"
    default = _cfg_str(cfg, "default_role", "user")
    return default if default in ("admin", "power_user", "user") else "user"


def authenticate(username: str, password: str, cfg: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Verifies `username`/`password` against the directory and provisions/updates the local account.
    Returns (user_dict, None) on success or (None, reason) on failure. Never raises for ordinary auth failures
    (wrong password, unknown user); configuration/connectivity problems are also returned as a reason, not raised,
    so a broken LDAP setup fails a login cleanly instead of 500ing.
    """
    from web import auth_frameworks
    cfg = cfg if cfg is not None else auth_frameworks.load_raw_config().get("ldap", {})
    if not cfg.get("enabled"):
        return None, "LDAP is not enabled."
    if not username or not password:
        return None, "Username and password are required."

    from web.auth import get_user_by_username
    local = get_user_by_username(username)
    if local and (local.get("auth_source") or "local") == "local":
        return None, _LOCAL_ACCOUNT_CONFLICT

    try:
        svc = _service_connection(cfg)
    except LdapError as exc:
        logger.warning(f"LDAP service bind failed: {exc}")
        return None, "The directory is not reachable right now."
    try:
        entry = find_user(svc, cfg, username)
    except LdapError as exc:
        logger.warning(f"LDAP user search failed: {exc}")
        return None, "The directory could not be searched."
    finally:
        svc.unbind()
    if entry is None:
        return None, "Invalid username or password."

    try:
        user_conn = _connect(cfg, user=entry["dn"], password=password)
    except LdapError:
        return None, "Invalid username or password."          # bad password, locked/disabled account, etc.

    try:
        groups = find_groups(user_conn, cfg, entry["dn"])
    finally:
        user_conn.unbind()
    role = _map_role(cfg, groups)

    from web.auth import upsert_external_user
    record = upsert_external_user(username=entry["username"], display_name=entry["display_name"], role=role,
                                  auth_source="ldap")
    return record, None


def sync_user(username: str, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Re-resolves one already-provisioned LDAP user's directory entry and role, without a password (an admin- or
    schedule-triggered refresh, not a login). Deactivates the local account if the entry no longer exists.
    """
    from web import auth_frameworks
    from web.auth import get_user_by_username, update_user, upsert_external_user
    cfg = cfg if cfg is not None else auth_frameworks.load_raw_config().get("ldap", {})
    local = get_user_by_username(username)
    if not local or (local.get("auth_source") or "local") != "ldap":
        return {"username": username, "status": "skipped", "reason": "not an LDAP-provisioned account"}
    if not cfg.get("enabled"):
        return {"username": username, "status": "skipped", "reason": "LDAP is not enabled"}
    try:
        svc = _service_connection(cfg)
    except LdapError as exc:
        return {"username": username, "status": "error", "reason": str(exc)}
    try:
        entry = find_user(svc, cfg, username)
        if entry is None:
            if local.get("is_active"):
                update_user(local["id"], is_active=False)
            return {"username": username, "status": "deactivated", "reason": "no longer found in the directory"}
        groups = find_groups(svc, cfg, entry["dn"])
        role = _map_role(cfg, groups)
        upsert_external_user(username=entry["username"], display_name=entry["display_name"], role=role, auth_source="ldap")
        if not local.get("is_active"):
            update_user(local["id"], is_active=True)
        changed = role != local.get("role") or entry["display_name"] != local.get("display_name") or not local.get("is_active")
        return {"username": username, "status": "updated" if changed else "unchanged", "role": role}
    except LdapError as exc:
        return {"username": username, "status": "error", "reason": str(exc)}
    finally:
        svc.unbind()


def sync_all(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Runs `sync_user` for every locally provisioned LDAP account. Best-effort: one failure never stops the rest."""
    from web import auth_frameworks
    from web.auth import list_users
    cfg = cfg if cfg is not None else auth_frameworks.load_raw_config().get("ldap", {})
    results = [sync_user(u["username"], cfg) for u in list_users() if (u.get("auth_source") or "local") == "ldap"]
    return {"checked": len(results), "updated": sum(1 for r in results if r["status"] == "updated"),
            "deactivated": sum(1 for r in results if r["status"] == "deactivated"),
            "errors": [r for r in results if r["status"] == "error"], "results": results}


def test_bind_and_search(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Deeper diagnostic than auth_frameworks.test_ldap_connection: service bind + a bounded user search."""
    try:
        svc = _service_connection(cfg)
    except LdapError as exc:
        return {"success": False, "message": str(exc)}
    try:
        base = _search_base(cfg, "user_search_base")
        count = None
        if base:
            svc.search(base, "(objectClass=*)", search_scope=SUBTREE, attributes=["1.1"], paged_size=50)
            count = len(svc.entries)
        return {"success": True, "message": f"Service bind succeeded" + (f"; {count} entr{'y' if count == 1 else 'ies'} visible under the user search base" if count is not None else "")}
    except LDAPException as exc:
        return {"success": False, "message": f"Bind succeeded but the search failed: {exc}"}
    finally:
        svc.unbind()
