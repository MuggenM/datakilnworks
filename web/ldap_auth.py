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
            timeout: int = 5) -> Connection:
    host = _cfg_str(cfg, "server_host")
    if not host:
        raise LdapError("LDAP server host is not configured.")
    port = int(cfg.get("server_port") or 389)
    encryption = _cfg_str(cfg, "encryption", "none").lower()
    use_ssl = encryption == "ssl" or port == 636
    server = Server(host, port=port, use_ssl=use_ssl, get_info=ALL, connect_timeout=timeout)
    try:
        # receive_timeout must be an int: ldap3 struct.packs it, and a float raises struct.error deep inside the
        # library. Without it, a host:port that accepts the TCP connection but never speaks LDAP (a typo'd port
        # that happens to hit some other service) hangs the calling thread until the OS's own TCP timeout, which
        # can be minutes -- every caller of this module (login included) needs bounded, predictable failure.
        conn = Connection(server, user=user, password=password, authentication=SIMPLE if user else None,
                          auto_bind=False, receive_timeout=timeout)
        if not conn.bind():
            raise LDAPException(conn.result.get("description") or conn.result.get("message") or "bind failed")
        if encryption == "starttls":
            conn.start_tls()
        return conn
    except LdapError:
        raise
    except Exception as exc:
        # Not just LDAPException: a server that answers on the port but doesn't actually speak LDAP (wrong port,
        # an HTTP service, ...) can make ldap3's BER decoder raise a raw KeyError/struct.error/etc. on the garbage
        # response, not one of its own exception types. Every failure mode here must become a clean LdapError, or
        # it reaches a FastAPI endpoint as an unhandled exception and comes back as a non-JSON 500.
        raise LdapError(f"Could not bind to {host}:{port}: {exc}") from exc


def _service_connection(cfg: Dict[str, Any]) -> Connection:
    bind_dn = _cfg_str(cfg, "bind_dn")
    bind_password = cfg.get("bind_password") or ""
    if not bind_dn:
        raise LdapError("A service bind DN is required to search the directory.")
    return _connect(cfg, user=bind_dn, password=bind_password)


def _search_base(cfg: Dict[str, Any], key: str) -> str:
    return _cfg_str(cfg, key) or _cfg_str(cfg, "base_dn")


def _entry_to_dict(entry, fallback_username: str = "") -> Dict[str, Any]:
    def attr(*names: str) -> str:
        for name in names:
            if name in entry and entry[name].value:
                v = entry[name].value
                return v[0] if isinstance(v, list) else str(v)
        return ""

    return {"dn": entry.entry_dn, "username": attr("uid", "sAMAccountName", "user_id") or fallback_username,
            "display_name": attr("cn", "displayName", "display_name") or fallback_username,
            "email": attr("mail", "email")}


def find_user(conn: Connection, cfg: Dict[str, Any], username: str) -> Optional[Dict[str, Any]]:
    """The single directory entry matching `username`, or None. Raises LdapError on more than one match."""
    base = _search_base(cfg, "user_search_base")
    if not base:
        raise LdapError("No user search base / base DN is configured.")
    filt = _cfg_str(cfg, "user_search_filter", "(uid={username})")
    if "{username}" not in filt:
        raise LdapError("user_search_filter must contain the {username} placeholder.")
    filt = filt.replace("{username}", escape_filter_chars(username))
    try:
        conn.search(base, filt, search_scope=SUBTREE, attributes=["*"])
    except LdapError:
        raise
    except Exception as exc:
        raise LdapError(f"The user search failed: {exc}") from exc
    if not conn.entries:
        return None
    if len(conn.entries) > 1:
        raise LdapError(f"The user filter matched more than one entry for '{username}'; narrow user_search_filter.")
    return _entry_to_dict(conn.entries[0], username)


# Presence of the username attribute AD (`sAMAccountName`) or OpenLDAP/lldap (`uid`) use, not an objectClass filter:
# some directories (lldap included) validate objectClass values in a filter against their own schema and reject an
# unrecognized one outright (e.g. "invalid class in objectClass attribute: user") rather than just matching nothing.
# Used only to *enumerate* candidates for bulk sync; `user_search_filter` (with its {username} placeholder) is still
# what a login checks a specific person against, and is unaffected by this.
BULK_USER_FILTER = "(|(uid=*)(sAMAccountName=*))"


def list_directory_users(conn: Connection, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every person entry under the user search base, for bulk discovery (see sync_all's `discover` option)."""
    base = _search_base(cfg, "user_search_base")
    if not base:
        raise LdapError("No user search base / base DN is configured.")
    try:
        conn.search(base, BULK_USER_FILTER, search_scope=SUBTREE, attributes=["*"])
    except Exception as exc:
        raise LdapError(f"The directory could not be enumerated: {exc}") from exc
    seen: Dict[str, Dict[str, Any]] = {}
    for entry in conn.entries:
        record = _entry_to_dict(entry)
        if record["username"]:
            seen[record["username"].lower()] = record       # de-duplicate: several matched objectClasses, one entry
    return list(seen.values())


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
    except Exception as exc:
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
    except Exception as exc:
        logger.warning(f"LDAP service bind failed: {exc}")
        return None, "The directory is not reachable right now."
    try:
        entry = find_user(svc, cfg, username)
    except Exception as exc:
        logger.warning(f"LDAP user search failed: {exc}")
        return None, "The directory could not be searched."
    finally:
        _safe_unbind(svc)
    if entry is None:
        return None, "Invalid username or password."

    try:
        user_conn = _connect(cfg, user=entry["dn"], password=password)
    except Exception:
        return None, "Invalid username or password."          # bad password, locked/disabled account, etc.

    try:
        groups = find_groups(user_conn, cfg, entry["dn"])
    finally:
        _safe_unbind(user_conn)
    role = _map_role(cfg, groups)

    from web.auth import upsert_external_user
    try:
        record = upsert_external_user(username=entry["username"], display_name=entry["display_name"], role=role,
                                      auth_source="ldap")
    except ValueError as exc:                    # e.g. the account was deleted and needs an admin to restore it
        return None, str(exc)
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
    if local.get("deleted_at"):
        return {"username": username, "status": "skipped", "reason": "deleted; an administrator must restore it first"}
    if not cfg.get("enabled"):
        return {"username": username, "status": "skipped", "reason": "LDAP is not enabled"}
    try:
        svc = _service_connection(cfg)
    except Exception as exc:
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
    except Exception as exc:
        return {"username": username, "status": "error", "reason": str(exc)}
    finally:
        _safe_unbind(svc)


def discover_and_provision(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Bulk-provisions every directory entry under the user search base that isn't already a local account (of any
    kind -- an existing local *or* ldap account is left untouched). This is what makes "sync" actually pull users
    in from the directory rather than only refreshing accounts someone has already logged into once: without it,
    an LDAP-provisioned account exists locally only after its first successful login, so a fresh install has
    nothing for `sync_user`/`sync_all` to refresh yet.
    """
    from web import auth_frameworks
    from web.auth import get_user_by_username, upsert_external_user
    cfg = cfg if cfg is not None else auth_frameworks.load_raw_config().get("ldap", {})
    if not cfg.get("enabled"):
        return {"discovered": [], "skipped": [], "error": "LDAP is not enabled."}
    try:
        svc = _service_connection(cfg)
    except Exception as exc:
        return {"discovered": [], "skipped": [], "error": str(exc)}
    discovered: List[str] = []
    skipped: List[str] = []
    try:
        try:
            entries = list_directory_users(svc, cfg)
        except Exception as exc:
            return {"discovered": [], "skipped": [], "error": str(exc)}
        for entry in entries:
            username = entry["username"]
            if not username or get_user_by_username(username) is not None:
                if username:
                    skipped.append(username)               # already local (local or ldap): never touched here
                continue
            groups = find_groups(svc, cfg, entry["dn"])
            role = _map_role(cfg, groups)
            try:
                upsert_external_user(username=username, display_name=entry["display_name"], role=role, auth_source="ldap")
                discovered.append(username)
            except ValueError:
                skipped.append(username)                    # a local account was created for this name meanwhile
    finally:
        _safe_unbind(svc)
    return {"discovered": discovered, "skipped": skipped, "error": None}


def sync_all(cfg: Optional[Dict[str, Any]] = None, discover: bool = True) -> Dict[str, Any]:
    """
    Full reconciliation against the directory: discovers and provisions new directory users (unless `discover` is
    False), then runs `sync_user` for every locally provisioned LDAP account -- including the ones just discovered,
    so their reported status is consistent -- to catch role changes and deactivate ones removed from the directory.
    Best effort: one failure never stops the rest.
    """
    from web import auth_frameworks
    from web.auth import list_users
    cfg = cfg if cfg is not None else auth_frameworks.load_raw_config().get("ldap", {})
    discovery = discover_and_provision(cfg) if discover else {"discovered": [], "skipped": [], "error": None}
    results = [sync_user(u["username"], cfg) for u in list_users() if (u.get("auth_source") or "local") == "ldap"]
    return {"checked": len(results), "discovered": len(discovery["discovered"]), "discovered_users": discovery["discovered"],
            "updated": sum(1 for r in results if r["status"] == "updated"),
            "deactivated": sum(1 for r in results if r["status"] == "deactivated"),
            "errors": [r for r in results if r["status"] == "error"] + ([{"reason": discovery["error"]}] if discovery["error"] else []),
            "results": results}


def _safe_unbind(conn: Connection) -> None:
    try:
        conn.unbind()
    except Exception:
        pass


def test_bind_and_search(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Deeper diagnostic than auth_frameworks.test_ldap_connection: service bind + a bounded user search."""
    try:
        svc = _service_connection(cfg)
    except Exception as exc:
        return {"success": False, "message": str(exc)}
    try:
        base = _search_base(cfg, "user_search_base")
        count = None
        if base:
            svc.search(base, "(objectClass=*)", search_scope=SUBTREE, attributes=["1.1"], paged_size=50)
            count = len(svc.entries)
        return {"success": True, "message": "Service bind succeeded" + (f"; {count} entr{'y' if count == 1 else 'ies'} visible under the user search base" if count is not None else "")}
    except Exception as exc:
        return {"success": False, "message": f"Bind succeeded but the search failed: {exc}"}
    finally:
        _safe_unbind(svc)
