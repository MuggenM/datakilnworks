"""SCIM 2.0 provisioning (RFC 7643 / 7644) for Microsoft Entra ID, Okta and other identity providers: /scim/v2/Users and /scim/v2/Groups mapped onto
this studio's local users and groups.

What SCIM may touch. Only accounts and groups that SCIM itself created (`users.scim_managed = 1`, `user_groups.source = 'scim'`). A local account, the
bootstrap admin, an LDAP account or a group made by hand does not exist as far as a SCIM client can see (404), and a SCIM create that collides with one is
refused (409): a stolen token can never take over or reveal a local account. SCIM-created users sign in through the identity provider configured as
`login_source` (OIDC or SAML), whose login code already accepts an existing account of its own source; that login no longer changes a SCIM user's role
or name (SCIM is authoritative for them, see auth.upsert_external_user).

Roles. A SCIM user gets `default_role`; the optional `roles` attribute and the mapping "IdP group name -> role" (`group_roles`) can raise it; nothing can
exceed `max_role` (default power_user: administrators are never provisioned unless an admin explicitly allows it, so a compromised IdP token cannot
mint administrators). Roles are recomputed whenever a user, a group membership or the configuration changes.

Groups. A SCIM group becomes a platform group with source 'scim' (the IdP's displayName is kept as its label and used by filters; the platform name is a
sanitised, unique version of it). SCIM sees and edits only the memberships it owns (origin 'sync'): members added by hand stay untouched and invisible.

Lifecycle. `active=false` deactivates (sessions stop working at once); DELETE soft-deletes like an administrator's delete; a later POST of the same userName
restores it. userName cannot be changed (grants and history refer to the account): a rename is refused with a clear reason.

Security. Bearer tokens (created by an administrator, shown once, stored only as a SHA-256 hash, individually revocable, optionally expiring) authenticate
the API; nothing else does. The whole API answers 403 while SCIM is switched off (default). Requests are bounded (body 1 MB, 200 per page). Every change is
audited as `SCIM_*` with the token's name as actor. The IP allowlist (web/ip_allowlist.py) applies like to every other route: allow the IdP's addresses.
"""
import datetime
import hashlib
import json
import logging
import re
import secrets
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("localspark.scim")

USER_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:User"
GROUP_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:Group"
LIST_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"
PATCH_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
ROLES = ("user", "power_user", "admin")
RANK = {r: i for i, r in enumerate(ROLES)}
USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9._@+-]{0,127}$")
GROUP_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.\-]{0,59}$")
MAX_PAGE = 200
MAX_BODY = 1_000_000
MEDIA_TYPE = "application/scim+json"
LOGIN_SOURCES = ("oidc", "saml")


class ScimError(Exception):
    def __init__(self, status: int, detail: str, scim_type: Optional[str] = None):
        super().__init__(detail)
        self.status, self.detail, self.scim_type = status, detail, scim_type

    def body(self) -> Dict[str, Any]:
        b = {"schemas": [ERROR_SCHEMA], "status": str(self.status), "detail": self.detail}
        if self.scim_type:
            b["scimType"] = self.scim_type
        return b


class ConfigError(ValueError):
    """Invalid configuration (the message is safe to show)."""


# ---------------------------------------------------------------- storage

def _conn():
    from web.auth import get_db_connection
    from web import groups
    groups._conn().close()                                  # creates / migrates user_groups and user_group_members
    conn = get_db_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scim_config (id INTEGER PRIMARY KEY CHECK (id = 1), enabled INTEGER NOT NULL DEFAULT 0, login_source TEXT NOT NULL DEFAULT 'oidc',
            default_role TEXT NOT NULL DEFAULT 'user', max_role TEXT NOT NULL DEFAULT 'power_user', use_roles_attribute INTEGER NOT NULL DEFAULT 0,
            group_roles TEXT NOT NULL DEFAULT '{}', base_url TEXT, updated_by TEXT, updated_at TEXT);
        CREATE TABLE IF NOT EXISTS scim_tokens (id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE, prefix TEXT, created_by TEXT,
            created_at TEXT, expires_at TEXT, last_used_at TEXT, revoked_at TEXT);
        CREATE TABLE IF NOT EXISTS scim_events (id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT, token TEXT, method TEXT, path TEXT, status INTEGER, detail TEXT);
    """)
    conn.execute("INSERT OR IGNORE INTO scim_config (id) VALUES (1)")
    gcols = {r[1] for r in conn.execute("PRAGMA table_info(user_groups)")}
    if "scim_external_id" not in gcols:
        conn.execute("ALTER TABLE user_groups ADD COLUMN scim_external_id TEXT")
    conn.commit()
    return conn


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _iso(ts: Optional[str]) -> Optional[str]:
    if not ts:
        return None
    return str(ts)[:19].replace(" ", "T") + "Z"


def _audit(actor: str, action: str, target: str, detail: Dict[str, Any]) -> None:
    try:
        from web.governance import store
        store.init_governance_db()
        c = store.get_db()
        try:
            store.write_audit(c, actor, action, target, detail)
            c.commit()
        finally:
            c.close()
    except Exception as exc:
        logger.warning(f"could not audit {action}: {exc}")


# ---------------------------------------------------------------- configuration and tokens (administrators, via /api/scim)

def get_config() -> Dict[str, Any]:
    c = _conn()
    try:
        r = dict(c.execute("SELECT * FROM scim_config WHERE id = 1").fetchone())
    finally:
        c.close()
    return {"enabled": bool(r["enabled"]), "login_source": r["login_source"], "default_role": r["default_role"], "max_role": r["max_role"],
            "use_roles_attribute": bool(r["use_roles_attribute"]), "group_roles": json.loads(r["group_roles"] or "{}"), "base_url": r["base_url"] or "",
            "updated_by": r["updated_by"], "updated_at": r["updated_at"]}


def set_config(data: Dict[str, Any], actor: str) -> Dict[str, Any]:
    cur = get_config()
    enabled = bool(data.get("enabled", cur["enabled"]))
    login_source = data.get("login_source", cur["login_source"])
    default_role, max_role = data.get("default_role", cur["default_role"]), data.get("max_role", cur["max_role"])
    if login_source not in LOGIN_SOURCES:
        raise ConfigError("Provisioned users sign in through OIDC or SAML; choose one.")
    if default_role not in ROLES or max_role not in ROLES:
        raise ConfigError("Roles must be user, power_user or admin.")
    if RANK[default_role] > RANK[max_role]:
        raise ConfigError("The default role cannot be higher than the highest role SCIM may grant.")
    gr = data.get("group_roles", cur["group_roles"])
    if not isinstance(gr, dict) or len(gr) > 100:
        raise ConfigError("The group-to-role mapping must be a list of group names and roles (at most 100).")
    clean_gr = {}
    for name, role in gr.items():
        if role not in ROLES or not str(name).strip():
            raise ConfigError(f"'{str(name)[:40]}': the role must be user, power_user or admin.")
        clean_gr[str(name).strip().lower()] = role
    base_url = str(data.get("base_url", cur["base_url"]) or "").strip().rstrip("/")
    if base_url and not re.match(r"^https?://[^\s/]+(/\S*)?$", base_url):
        raise ConfigError("The public base URL must start with http:// or https://.")
    c = _conn()
    try:
        c.execute("UPDATE scim_config SET enabled=?, login_source=?, default_role=?, max_role=?, use_roles_attribute=?, group_roles=?, base_url=?, updated_by=?, updated_at=? WHERE id=1",
                  (1 if enabled else 0, login_source, default_role, max_role, 1 if data.get("use_roles_attribute", cur["use_roles_attribute"]) else 0, json.dumps(clean_gr), base_url, actor, _now()))
        c.commit()
    finally:
        c.close()
    _audit(actor, "SCIM_CONFIG_UPDATE", "scim", {"enabled": enabled, "login_source": login_source, "default_role": default_role, "max_role": max_role, "group_roles": clean_gr})
    changed = recompute_roles(f"scim-config:{actor}")
    return {**get_config(), "roles_changed": changed}


def create_token(name: str, expires_days: Optional[int], actor: str) -> Dict[str, Any]:
    name = (name or "").strip()
    if not 1 <= len(name) <= 60:
        raise ConfigError("Give the token a name (1-60 characters), for example 'Entra ID production'.")
    exp = None
    if expires_days:
        if not 1 <= int(expires_days) <= 3650:
            raise ConfigError("A token lives 1 to 3650 days (leave empty for no expiry).")
        exp = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=int(expires_days))).strftime("%Y-%m-%d %H:%M:%S")
    token = "dkw_scim_" + secrets.token_hex(24)
    tid = f"sct_{uuid.uuid4().hex[:8]}"
    c = _conn()
    try:
        c.execute("INSERT INTO scim_tokens (id, name, token_hash, prefix, created_by, created_at, expires_at) VALUES (?,?,?,?,?,?,?)",
                  (tid, name, hashlib.sha256(token.encode()).hexdigest(), token[:13] + "...", actor, _now(), exp))
        c.commit()
    finally:
        c.close()
    _audit(actor, "SCIM_TOKEN_CREATE", f"scim-token:{name}", {"token_id": tid, "expires_at": exp})
    return {"id": tid, "name": name, "token": token, "expires_at": exp}


def list_tokens() -> List[Dict[str, Any]]:
    c = _conn()
    try:
        return [dict(r) for r in c.execute("SELECT id, name, prefix, created_by, created_at, expires_at, last_used_at, revoked_at FROM scim_tokens ORDER BY created_at DESC")]
    finally:
        c.close()


def revoke_token(token_id: str, actor: str) -> None:
    c = _conn()
    try:
        r = c.execute("SELECT name, revoked_at FROM scim_tokens WHERE id = ?", (token_id,)).fetchone()
        if not r:
            raise LookupError("Token not found.")
        c.execute("UPDATE scim_tokens SET revoked_at = COALESCE(revoked_at, ?) WHERE id = ?", (_now(), token_id))
        c.commit()
    finally:
        c.close()
    _audit(actor, "SCIM_TOKEN_REVOKE", f"scim-token:{r['name']}", {"token_id": token_id})


def authenticate(header: Optional[str]) -> Dict[str, Any]:
    """The token row for `Authorization: Bearer <token>`, or ScimError 401. `last_used_at` is refreshed at most once a minute."""
    if not header or not header.lower().startswith("bearer "):
        raise ScimError(401, "A bearer token is required.")
    token = header[7:].strip()
    if not token.startswith("dkw_scim_") or len(token) > 200:
        raise ScimError(401, "The token is not valid.")
    c = _conn()
    try:
        r = c.execute("SELECT * FROM scim_tokens WHERE token_hash = ?", (hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
        if not r or r["revoked_at"] or (r["expires_at"] and r["expires_at"] <= _now()):
            raise ScimError(401, "The token is not valid, has expired or was revoked.")
        if not r["last_used_at"] or r["last_used_at"] < (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S"):
            c.execute("UPDATE scim_tokens SET last_used_at = ? WHERE id = ?", (_now(), r["id"]))
            c.commit()
        return dict(r)
    finally:
        c.close()


_event_count = {"n": 0}


def log_event(token: str, method: str, path: str, status: int, detail: str) -> None:
    try:
        c = _conn()
        try:
            c.execute("INSERT INTO scim_events (at, token, method, path, status, detail) VALUES (?,?,?,?,?,?)", (_now(), token, method, path[:200], status, detail[:300]))
            _event_count["n"] += 1
            if _event_count["n"] % 50 == 0:
                c.execute("DELETE FROM scim_events WHERE id <= (SELECT MAX(id) FROM scim_events) - 500")
            c.commit()
        finally:
            c.close()
    except Exception as exc:
        logger.debug(f"scim event not logged: {exc}")


def status_summary() -> Dict[str, Any]:
    c = _conn()
    try:
        users = c.execute("SELECT COUNT(*) FROM users WHERE scim_managed = 1 AND deleted_at IS NULL").fetchone()[0]
        inactive = c.execute("SELECT COUNT(*) FROM users WHERE scim_managed = 1 AND deleted_at IS NULL AND is_active = 0").fetchone()[0]
        groups = c.execute("SELECT COUNT(*) FROM user_groups WHERE source = 'scim'").fetchone()[0]
        events = [dict(r) for r in c.execute("SELECT at, token, method, path, status, detail FROM scim_events ORDER BY id DESC LIMIT 50")]
    finally:
        c.close()
    return {"users": users, "users_inactive": inactive, "groups": groups, "last_request_at": events[0]["at"] if events else None, "events": events}


# ---------------------------------------------------------------- role computation

def _role_for(c, user_id: str, cfg: Dict[str, Any], scim_role: Optional[str]) -> str:
    cands = [cfg["default_role"]]
    if cfg["use_roles_attribute"] and scim_role in ROLES:
        cands.append(scim_role)
    if cfg["group_roles"]:
        for g in c.execute("SELECT g.external_label, g.name FROM user_group_members m JOIN user_groups g ON g.id = m.group_id "
                           "WHERE m.user_id = ? AND g.source = 'scim' AND m.origin = 'sync'", (user_id,)):
            for label in (g["external_label"], g["name"]):
                role = cfg["group_roles"].get(str(label or "").strip().lower())
                if role:
                    cands.append(role)
    best = max(cands, key=lambda r: RANK[r])
    return best if RANK[best] <= RANK[cfg["max_role"]] else cfg["max_role"]


def recompute_roles(actor: str, only_user_ids: Optional[List[str]] = None) -> int:
    """Sets every SCIM user's role from the current configuration, roles attribute and group memberships. Returns how many changed."""
    cfg = get_config()
    c = _conn()
    changed = []
    try:
        q = "SELECT id, username, role, scim_extra FROM users WHERE scim_managed = 1 AND deleted_at IS NULL"
        for u in c.execute(q).fetchall():
            if only_user_ids is not None and u["id"] not in only_user_ids:
                continue
            extra = json.loads(u["scim_extra"] or "{}")
            role = _role_for(c, u["id"], cfg, extra.get("role"))
            if role != u["role"]:
                c.execute("UPDATE users SET role = ? WHERE id = ?", (role, u["id"]))
                changed.append((u["username"], u["role"], role))
        c.commit()
    finally:
        c.close()
    for name, was, now in changed:
        _audit(actor, "SCIM_ROLE_CHANGE", f"user:{name}", {"from": was, "to": now})
    return len(changed)


# ---------------------------------------------------------------- filters (RFC 7644 3.4.2.2, the parts IdPs use)

_TOKEN_RE = re.compile(r'\s*(\(|\)|"(?:[^"\\]|\\.)*"|[^\s()"]+)')


def _tokenize(s: str) -> List[str]:
    out, pos = [], 0
    s = s.strip()
    while pos < len(s):
        m = _TOKEN_RE.match(s, pos)
        if not m:
            raise ScimError(400, "The filter could not be parsed.", "invalidFilter")
        out.append(m.group(1))
        pos = m.end()
    return out


def parse_filter(text: str) -> Callable[[Dict[str, Any]], bool]:
    toks = _tokenize(text)
    pos = [0]

    def peek():
        return toks[pos[0]] if pos[0] < len(toks) else None

    def take():
        t = peek()
        pos[0] += 1
        return t

    def literal(t: str):
        if t.startswith('"'):
            return json.loads(t)
        if t in ("true", "false"):
            return t == "true"
        if t == "null":
            return None
        try:
            return int(t)
        except ValueError:
            raise ScimError(400, f"Unsupported value '{t[:30]}' in the filter.", "invalidFilter")

    def norm(v):
        return v.lower() if isinstance(v, str) else v

    def expr_or():
        left = expr_and()
        while (peek() or "").lower() == "or":
            take()
            right = expr_and()
            left = (lambda a, b: lambda r: a(r) or b(r))(left, right)
        return left

    def expr_and():
        left = atom()
        while (peek() or "").lower() == "and":
            take()
            right = atom()
            left = (lambda a, b: lambda r: a(r) and b(r))(left, right)
        return left

    def atom():
        t = take()
        if t is None:
            raise ScimError(400, "The filter ended unexpectedly.", "invalidFilter")
        if t == "(":
            e = expr_or()
            if take() != ")":
                raise ScimError(400, "Missing ')' in the filter.", "invalidFilter")
            return e
        if t.lower() == "not":
            inner = atom()
            return lambda r: not inner(r)
        attr = t.lower()
        op = (take() or "").lower()
        if op == "pr":
            return lambda r: r.get(attr) not in (None, "", [])
        if op not in ("eq", "ne", "co", "sw", "ew", "gt", "ge", "lt", "le"):
            raise ScimError(400, f"Unsupported filter operator '{op[:10]}'.", "invalidFilter")
        val = norm(literal(take() or ""))

        def test(r):
            got = r.get(attr)
            vals = got if isinstance(got, list) else [got]
            for g in vals:
                g = norm(g)
                if op == "eq" and g == val: return True
                if op == "ne" and g != val: return True
                if isinstance(g, str) and isinstance(val, str):
                    if op == "co" and val in g: return True
                    if op == "sw" and g.startswith(val): return True
                    if op == "ew" and g.endswith(val): return True
                    if op == "gt" and g > val: return True
                    if op == "ge" and g >= val: return True
                    if op == "lt" and g < val: return True
                    if op == "le" and g <= val: return True
            return op == "ne" and got is None
        return test

    fn = expr_or()
    if peek() is not None:
        raise ScimError(400, "Unexpected text after the filter.", "invalidFilter")
    return fn


def _page(items: List[Dict[str, Any]], start: Any, count: Any, render: Callable[[Dict[str, Any]], Dict[str, Any]]) -> Dict[str, Any]:
    try:
        start = max(1, int(start or 1))
        count = min(MAX_PAGE, max(0, int(100 if count in (None, "") else count)))
    except (TypeError, ValueError):
        raise ScimError(400, "startIndex and count must be numbers.", "invalidValue")
    chunk = items[start - 1:start - 1 + count]
    return {"schemas": [LIST_SCHEMA], "totalResults": len(items), "startIndex": start, "itemsPerPage": len(chunk), "Resources": [render(x) for x in chunk]}


# ---------------------------------------------------------------- users

def _truthy(v: Any) -> bool:
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)


def _flat_for_filter(r) -> Dict[str, Any]:
    """The user's attributes under the lower-cased names a filter uses."""
    extra = json.loads(r["scim_extra"] or "{}")
    return {"id": r["id"], "username": r["username"], "externalid": r["scim_external_id"], "displayname": r["display_name"], "active": bool(r["is_active"]),
            "emails.value": extra.get("email"), "name.givenname": extra.get("givenName"), "name.familyname": extra.get("familyName")}


def _user_resource(r, base: str, c=None) -> Dict[str, Any]:
    extra = json.loads(r["scim_extra"] or "{}")
    name = {"formatted": r["display_name"]}
    if extra.get("givenName"): name["givenName"] = extra["givenName"]
    if extra.get("familyName"): name["familyName"] = extra["familyName"]
    res = {"schemas": [USER_SCHEMA], "id": r["id"], "userName": r["username"], "name": name, "displayName": r["display_name"], "active": bool(r["is_active"]),
           "meta": {"resourceType": "User", "created": _iso(r["created_at"]), "lastModified": _iso(r["scim_modified_at"] or r["created_at"]), "location": f"{base}/Users/{r['id']}"}}
    if r["scim_external_id"]:
        res["externalId"] = r["scim_external_id"]
    if extra.get("email"):
        res["emails"] = [{"value": extra["email"], "type": "work", "primary": True}]
    if c is not None:
        gs = c.execute("SELECT g.id, g.external_label, g.name FROM user_group_members m JOIN user_groups g ON g.id = m.group_id "
                       "WHERE m.user_id = ? AND g.source = 'scim' AND m.origin = 'sync' ORDER BY g.name", (r["id"],)).fetchall()
        if gs:
            res["groups"] = [{"value": g["id"], "display": g["external_label"] or g["name"], "$ref": f"{base}/Groups/{g['id']}"} for g in gs]
    return res


def _attrs_from_payload(p: Dict[str, Any]) -> Dict[str, Any]:
    """Flat attribute dict from a User resource (POST / PUT). Missing attributes stay None (PUT then clears them)."""
    name = p.get("name") if isinstance(p.get("name"), dict) else {}
    email = None
    emails = p.get("emails")
    if isinstance(emails, list) and emails:
        pick = next((e for e in emails if isinstance(e, dict) and _truthy(e.get("primary"))), None) or next((e for e in emails if isinstance(e, dict)), None)
        email = (pick or {}).get("value")
    roles = p.get("roles")
    role = None
    if isinstance(roles, list):
        for r in roles:
            v = (r.get("value") if isinstance(r, dict) else r)
            if isinstance(v, str) and v.strip().lower() in ROLES:
                role = v.strip().lower() if role is None or RANK[v.strip().lower()] > RANK[role] else role
    display = p.get("displayName") or name.get("formatted") or " ".join(x for x in (name.get("givenName"), name.get("familyName")) if x) or None
    return {"userName": p.get("userName"), "externalId": p.get("externalId"), "displayName": display, "givenName": name.get("givenName"), "familyName": name.get("familyName"),
            "email": email, "role": role, "active": _truthy(p["active"]) if "active" in p else True}


def _clean_username(v: Any) -> str:
    u = str(v or "").strip().lower()
    if not u:
        raise ScimError(400, "userName is required.", "invalidValue")
    if not USERNAME_RE.match(u):
        raise ScimError(400, "userName may contain lower-case letters, digits and . _ @ + - (max. 128 characters, starting with a letter or digit).", "invalidValue")
    return u


def _find_by_username(c, username: str):
    return c.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()


def _managed_user(c, uid: str):
    r = c.execute("SELECT * FROM users WHERE id = ? AND scim_managed = 1 AND deleted_at IS NULL", (uid,)).fetchone()
    if not r:
        raise ScimError(404, "User not found.")
    return r


def _apply_user(c, uid: str, a: Dict[str, Any], actor: str, cfg: Dict[str, Any]) -> None:
    """Writes the attributes of a managed user (display name, external id, e-mail, active) and recomputes the role."""
    r = c.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
    extra = json.loads(r["scim_extra"] or "{}")
    for key, attr in (("givenName", "givenName"), ("familyName", "familyName"), ("email", "email")):
        if a.get(attr):
            extra[key] = str(a[attr])[:200]
        else:
            extra.pop(key, None)
    if a.get("role"):
        extra["role"] = a["role"]
    else:
        extra.pop("role", None)
    ext = a.get("externalId")
    if ext:
        clash = c.execute("SELECT username FROM users WHERE scim_external_id = ? AND id != ? AND scim_managed = 1", (str(ext), uid)).fetchone()
        if clash:
            raise ScimError(409, "Another user already has this externalId.", "uniqueness")
    display = (str(a.get("displayName") or "").strip()[:120]) or r["username"]
    c.execute("UPDATE users SET display_name = ?, scim_external_id = ?, scim_extra = ?, is_active = ?, scim_modified_at = ? WHERE id = ?",
              (display, str(ext)[:200] if ext else None, json.dumps(extra), 1 if a.get("active", True) else 0, _now(), uid))
    role = _role_for(c, uid, cfg, extra.get("role"))
    if role != r["role"]:
        c.execute("UPDATE users SET role = ? WHERE id = ?", (role, uid))


def create_user(payload: Dict[str, Any], base: str, actor: str) -> Dict[str, Any]:
    from web import auth
    cfg = get_config()
    a = _attrs_from_payload(payload)
    username = _clean_username(a["userName"])
    c = _conn()
    try:
        existing = _find_by_username(c, username)
        if existing:
            if not existing["scim_managed"]:
                raise ScimError(409, f"'{username}' already exists here and is not managed by SCIM; it cannot be taken over.", "uniqueness")
            if not existing["deleted_at"]:
                raise ScimError(409, f"A user named '{username}' already exists.", "uniqueness")
            uid = existing["id"]
            auth.restore_user(uid)                              # provisioned again after a delete: the IdP is authoritative
            c.close(); c = _conn()
        else:
            try:
                u = auth.upsert_external_user(username, a.get("displayName") or username, "user", cfg["login_source"])
            except ValueError as exc:
                raise ScimError(400, str(exc), "invalidValue")
            uid = u["id"]
            c.close(); c = _conn()
            c.execute("UPDATE users SET scim_managed = 1 WHERE id = ?", (uid,))
        _apply_user(c, uid, a, actor, cfg)
        c.commit()
        r = c.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
        out = _user_resource(r, base, c)
    finally:
        c.close()
    _audit(actor, "SCIM_USER_CREATE", f"user:{username}", {"user_id": uid, "active": out["active"], "role": r["role"]})
    return out


def get_user(uid: str, base: str) -> Dict[str, Any]:
    c = _conn()
    try:
        r = _managed_user(c, uid)
        return _user_resource(r, base, c)
    finally:
        c.close()


def list_users(params: Dict[str, str], base: str) -> Dict[str, Any]:
    c = _conn()
    try:
        rows = c.execute("SELECT * FROM users WHERE scim_managed = 1 AND deleted_at IS NULL ORDER BY username").fetchall()
        if params.get("filter"):
            f = parse_filter(params["filter"])
            rows = [r for r in rows if f(_flat_for_filter(r))]
        return _page(rows, params.get("startIndex"), params.get("count"), lambda r: _user_resource(r, base, c))
    finally:
        c.close()


def replace_user(uid: str, payload: Dict[str, Any], base: str, actor: str) -> Dict[str, Any]:
    cfg = get_config()
    a = _attrs_from_payload(payload)
    c = _conn()
    try:
        r = _managed_user(c, uid)
        if a["userName"] and _clean_username(a["userName"]) != r["username"]:
            raise ScimError(400, "userName cannot be changed: grants and history refer to the account. Provision a new user instead.", "mutability")
        was = bool(r["is_active"])
        _apply_user(c, uid, a, actor, cfg)
        c.commit()
        r2 = c.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
        out = _user_resource(r2, base, c)
    finally:
        c.close()
    _audit(actor, "SCIM_USER_DEACTIVATE" if was and not out["active"] else ("SCIM_USER_REACTIVATE" if not was and out["active"] else "SCIM_USER_UPDATE"), f"user:{r['username']}", {"user_id": uid})
    return out


_EMAIL_PATH = re.compile(r"^emails(\[[^\]]*\])?(\.value)?$")
_ROLES_PATH = re.compile(r"^roles(\[[^\]]*\])?(\.value)?$")


def _patch_attrs(a: Dict[str, Any], op: str, path: Optional[str], value: Any) -> None:
    """Applies one PATCH operation to the flat attribute dict of a user (in place). Attributes this studio does not keep are ignored."""
    if path is None:
        if isinstance(value, dict):
            for k, v in value.items():
                _patch_attrs(a, op, k, v)
        return
    p = re.sub(r"^urn:[^\s]*:User:", "", path).strip()
    pl = p.lower()
    remove = op == "remove"
    if pl == "active":
        if not remove:
            a["active"] = _truthy(value)
    elif pl == "username":
        if not remove:
            a["userName"] = value
    elif pl == "externalid":
        a["externalId"] = None if remove else value
    elif pl == "displayname":
        a["displayName"] = None if remove else value
    elif pl in ("name.givenname", "name.familyname", "name.formatted", "name"):
        if pl == "name" and isinstance(value, dict):
            for k, v in value.items():
                _patch_attrs(a, op, "name." + k, v)
        elif pl == "name.givenname":
            a["givenName"] = None if remove else value
        elif pl == "name.familyname":
            a["familyName"] = None if remove else value
        elif pl == "name.formatted" and not remove:
            a["displayName"] = a.get("displayName") or value
    elif _EMAIL_PATH.match(pl):
        if remove:
            a["email"] = None
        else:
            v = value
            if isinstance(v, list):
                pick = next((e for e in v if isinstance(e, dict) and _truthy(e.get("primary"))), None) or next((e for e in v if isinstance(e, dict)), None)
                v = (pick or {}).get("value")
            elif isinstance(v, dict):
                v = v.get("value")
            a["email"] = v
    elif _ROLES_PATH.match(pl):
        if remove:
            a["role"] = None
        else:
            roles = value if isinstance(value, list) else [value]
            best = None
            for r in roles:
                v = (r.get("value") if isinstance(r, dict) else r)
                if isinstance(v, str) and v.strip().lower() in ROLES and (best is None or RANK[v.strip().lower()] > RANK[best]):
                    best = v.strip().lower()
            a["role"] = best
    else:
        logger.debug(f"SCIM PATCH: ignoring unsupported path '{path[:60]}'")


def _ops(payload: Dict[str, Any]) -> List[Tuple[str, Optional[str], Any]]:
    ops = payload.get("Operations") or payload.get("operations")
    if not isinstance(ops, list) or not ops:
        raise ScimError(400, "A PatchOp needs an Operations list.", "invalidSyntax")
    out = []
    for o in ops:
        op = str((o or {}).get("op", "")).strip().lower()
        if op not in ("add", "replace", "remove"):
            raise ScimError(400, f"Unsupported patch operation '{op[:20]}'.", "invalidSyntax")
        out.append((op, o.get("path"), o.get("value")))
    return out


def patch_user(uid: str, payload: Dict[str, Any], base: str, actor: str) -> Dict[str, Any]:
    ops = _ops(payload)
    cfg = get_config()
    c = _conn()
    try:
        r = _managed_user(c, uid)
        extra = json.loads(r["scim_extra"] or "{}")
        a = {"userName": r["username"], "externalId": r["scim_external_id"], "displayName": r["display_name"], "givenName": extra.get("givenName"),
             "familyName": extra.get("familyName"), "email": extra.get("email"), "role": extra.get("role"), "active": bool(r["is_active"])}
        for op, path, value in ops:
            _patch_attrs(a, op, path, value)
        if a["userName"] and _clean_username(a["userName"]) != r["username"]:
            raise ScimError(400, "userName cannot be changed: grants and history refer to the account. Provision a new user instead.", "mutability")
        was = bool(r["is_active"])
        _apply_user(c, uid, a, actor, cfg)
        c.commit()
        r2 = c.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
        out = _user_resource(r2, base, c)
    finally:
        c.close()
    _audit(actor, "SCIM_USER_DEACTIVATE" if was and not out["active"] else ("SCIM_USER_REACTIVATE" if not was and out["active"] else "SCIM_USER_UPDATE"), f"user:{r['username']}", {"user_id": uid})
    return out


def delete_user(uid: str, actor: str) -> None:
    from web import auth
    c = _conn()
    try:
        r = _managed_user(c, uid)
    finally:
        c.close()
    auth.delete_user(uid)
    _audit(actor, "SCIM_USER_DELETE", f"user:{r['username']}", {"user_id": uid})


# ---------------------------------------------------------------- groups

def _group_rows(c):
    return c.execute("SELECT * FROM user_groups WHERE source = 'scim' ORDER BY name COLLATE NOCASE").fetchall()


def _managed_group(c, gid: str):
    r = c.execute("SELECT * FROM user_groups WHERE id = ? AND source = 'scim'", (gid,)).fetchone()
    if not r:
        raise ScimError(404, "Group not found.")
    return r


def _sync_members(c, gid: str) -> List[Any]:
    return c.execute("SELECT u.id, u.username FROM user_group_members m JOIN users u ON u.id = m.user_id "
                     "WHERE m.group_id = ? AND m.origin = 'sync' AND u.deleted_at IS NULL AND u.scim_managed = 1 ORDER BY u.username", (gid,)).fetchall()


def _group_resource(r, base: str, c) -> Dict[str, Any]:
    res = {"schemas": [GROUP_SCHEMA], "id": r["id"], "displayName": r["external_label"] or r["name"],
           "members": [{"value": m["id"], "display": m["username"], "$ref": f"{base}/Users/{m['id']}"} for m in _sync_members(c, r["id"])],
           "meta": {"resourceType": "Group", "created": _iso(r["created_at"]), "lastModified": _iso(r["created_at"]), "location": f"{base}/Groups/{r['id']}"}}
    if r["scim_external_id"]:
        res["externalId"] = r["scim_external_id"]
    return res


def _platform_name(c, display: str, own_id: Optional[str] = None) -> str:
    base = re.sub(r"[^A-Za-z0-9 _.\-]", "_", display).strip()[:56] or "group"
    if not re.match(r"^[A-Za-z0-9]", base):
        base = "g" + base
    base = base[:60]
    name, n = base, 1
    while True:
        clash = c.execute("SELECT id FROM user_groups WHERE name = ? COLLATE NOCASE AND id != ?", (name, own_id or "")).fetchone()
        if not clash:
            return name
        n += 1
        name = f"{base[:52]} ({'SCIM' if n == 2 else 'SCIM ' + str(n - 1)})"


def _member_ids(c, values: Any) -> List[str]:
    ids = []
    for m in values or []:
        v = m.get("value") if isinstance(m, dict) else m
        if not v:
            continue
        r = c.execute("SELECT id FROM users WHERE id = ? AND scim_managed = 1 AND deleted_at IS NULL", (str(v),)).fetchone()
        if not r:
            raise ScimError(400, f"Member '{str(v)[:40]}' is not a user provisioned through SCIM.", "invalidValue")
        ids.append(r["id"])
    return list(dict.fromkeys(ids))


def _set_members(c, gid: str, want: List[str], actor: str) -> List[str]:
    """Makes the SCIM-owned (origin 'sync') members of a group exactly `want`; manual members are untouched. Returns affected user ids."""
    have = {m["id"] for m in _sync_members(c, gid)}
    affected = []
    for uid in set(want) - have:
        c.execute("INSERT OR REPLACE INTO user_group_members (group_id, user_id, added_by, added_at, origin) VALUES (?,?,?,?,'sync')", (gid, uid, actor, _now()))
        affected.append(uid)
    for uid in have - set(want):
        c.execute("DELETE FROM user_group_members WHERE group_id = ? AND user_id = ? AND origin = 'sync'", (gid, uid))
        affected.append(uid)
    return affected


def create_group(payload: Dict[str, Any], base: str, actor: str) -> Dict[str, Any]:
    display = str(payload.get("displayName") or "").strip()
    if not display:
        raise ScimError(400, "displayName is required.", "invalidValue")
    c = _conn()
    try:
        if c.execute("SELECT 1 FROM user_groups WHERE source = 'scim' AND external_label = ? COLLATE NOCASE", (display,)).fetchone():
            raise ScimError(409, f"A group named '{display}' already exists.", "uniqueness")
        want = _member_ids(c, payload.get("members"))
        gid = f"grp_{uuid.uuid4().hex[:8]}"
        c.execute("INSERT INTO user_groups (id, name, description, created_by, created_at, source, external_ref, external_label, scim_external_id) VALUES (?,?,?,?,?,'scim',?,?,?)",
                  (gid, _platform_name(c, display), "Provisioned by SCIM", actor, _now(), gid, display[:200], str(payload.get("externalId"))[:200] if payload.get("externalId") else None))
        affected = _set_members(c, gid, want, actor)
        c.commit()
        out = _group_resource(c.execute("SELECT * FROM user_groups WHERE id = ?", (gid,)).fetchone(), base, c)
    finally:
        c.close()
    recompute_roles(actor, affected)
    _audit(actor, "SCIM_GROUP_CREATE", f"group:{display}", {"group_id": gid, "members": len(want)})
    return out


def get_group(gid: str, base: str) -> Dict[str, Any]:
    c = _conn()
    try:
        return _group_resource(_managed_group(c, gid), base, c)
    finally:
        c.close()


def list_groups(params: Dict[str, str], base: str) -> Dict[str, Any]:
    c = _conn()
    try:
        rows = _group_rows(c)
        if params.get("filter"):
            f = parse_filter(params["filter"])
            rows = [r for r in rows if f({"displayname": r["external_label"] or r["name"], "id": r["id"], "externalid": r["scim_external_id"],
                                          "members.value": [m["id"] for m in _sync_members(c, r["id"])]})]
        excl = {x.strip().lower() for x in (params.get("excludedAttributes") or "").split(",")}
        def render(r):
            res = _group_resource(r, base, c)
            if "members" in excl:
                res.pop("members", None)
            return res
        return _page(rows, params.get("startIndex"), params.get("count"), render)
    finally:
        c.close()


def _rename(c, r, display: str) -> None:
    c.execute("UPDATE user_groups SET external_label = ?, name = ? WHERE id = ?", (display[:200], _platform_name(c, display, r["id"]), r["id"]))


def replace_group(gid: str, payload: Dict[str, Any], base: str, actor: str) -> Dict[str, Any]:
    c = _conn()
    try:
        r = _managed_group(c, gid)
        display = str(payload.get("displayName") or "").strip()
        if not display:
            raise ScimError(400, "displayName is required.", "invalidValue")
        _rename(c, r, display)
        c.execute("UPDATE user_groups SET scim_external_id = ? WHERE id = ?", (str(payload.get("externalId"))[:200] if payload.get("externalId") else None, gid))
        affected = _set_members(c, gid, _member_ids(c, payload.get("members")), actor)
        c.commit()
        out = _group_resource(c.execute("SELECT * FROM user_groups WHERE id = ?", (gid,)).fetchone(), base, c)
    finally:
        c.close()
    recompute_roles(actor, affected)
    _audit(actor, "SCIM_GROUP_UPDATE", f"group:{display}", {"group_id": gid})
    return out


_MEMBER_PATH = re.compile(r'^members(\[\s*value\s+eq\s+"([^"]+)"\s*\])?$', re.IGNORECASE)


def patch_group(gid: str, payload: Dict[str, Any], base: str, actor: str) -> Dict[str, Any]:
    ops = _ops(payload)
    c = _conn()
    affected: List[str] = []
    try:
        r = _managed_group(c, gid)
        members = [m["id"] for m in _sync_members(c, gid)]
        display = r["external_label"] or r["name"]

        def apply(op: str, path: Optional[str], value: Any):
            nonlocal members, display
            if path is None:
                if isinstance(value, dict):
                    for k, v in value.items():
                        apply(op, k, v)
                return
            pl = path.strip()
            if pl.lower() == "displayname":
                if op != "remove":
                    display = str(value or "").strip() or display
                return
            m = _MEMBER_PATH.match(pl)
            if not m:
                logger.debug(f"SCIM PATCH group: ignoring unsupported path '{path[:60]}'")
                return
            if m.group(2):                                              # members[value eq "id"]
                target = m.group(2)
                if op == "remove":
                    members = [x for x in members if x != target]
                else:
                    members = list(dict.fromkeys(members + _member_ids(c, [target])))
            elif op == "replace":
                members = _member_ids(c, value)
            elif op == "add":
                members = list(dict.fromkeys(members + _member_ids(c, value)))
            else:                                                       # remove with a value list, or all members
                gone = set(_member_ids(c, value)) if value else set(members)
                members = [x for x in members if x not in gone]

        for op, path, value in ops:
            apply(op, path, value)
        if display != (r["external_label"] or r["name"]):
            _rename(c, r, display)
        affected = _set_members(c, gid, members, actor)
        c.commit()
        out = _group_resource(c.execute("SELECT * FROM user_groups WHERE id = ?", (gid,)).fetchone(), base, c)
    finally:
        c.close()
    recompute_roles(actor, affected)
    _audit(actor, "SCIM_GROUP_UPDATE", f"group:{display}", {"group_id": gid, "membership_changes": len(affected)})
    return out


def delete_group(gid: str, actor: str) -> None:
    from web import groups
    c = _conn()
    try:
        r = _managed_group(c, gid)
        affected = [m["id"] for m in _sync_members(c, gid)]
    finally:
        c.close()
    groups.delete_group(gid, actor)
    recompute_roles(actor, affected)
    _audit(actor, "SCIM_GROUP_DELETE", f"group:{r['external_label'] or r['name']}", {"group_id": gid})


# ---------------------------------------------------------------- discovery documents

def service_provider_config(base: str) -> Dict[str, Any]:
    return {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"], "documentationUri": f"{base.rsplit('/scim/', 1)[0]}/docs/",
            "patch": {"supported": True}, "bulk": {"supported": False, "maxOperations": 0, "maxPayloadSize": 0}, "filter": {"supported": True, "maxResults": MAX_PAGE},
            "changePassword": {"supported": False}, "sort": {"supported": False}, "etag": {"supported": False},
            "authenticationSchemes": [{"type": "oauthbearertoken", "name": "Bearer token", "description": "A token created under Users & IAM > Provisioning (SCIM)", "primary": True}],
            "meta": {"resourceType": "ServiceProviderConfig", "location": f"{base}/ServiceProviderConfig"}}


def resource_types(base: str) -> Dict[str, Any]:
    rt = [{"schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"], "id": n, "name": n, "endpoint": f"/{n}s", "schema": s,
           "meta": {"resourceType": "ResourceType", "location": f"{base}/ResourceTypes/{n}"}} for n, s in (("User", USER_SCHEMA), ("Group", GROUP_SCHEMA))]
    return {"schemas": [LIST_SCHEMA], "totalResults": 2, "startIndex": 1, "itemsPerPage": 2, "Resources": rt}


def schemas(base: str) -> Dict[str, Any]:
    def attr(name, typ="string", **kw):
        return {"name": name, "type": typ, "multiValued": False, "required": False, "mutability": "readWrite", "returned": "default", "uniqueness": "none", **kw}
    user = {"id": USER_SCHEMA, "name": "User", "description": "User Account", "attributes": [
        attr("userName", required=True, uniqueness="server", mutability="immutable"), attr("externalId"), attr("displayName"),
        attr("name", "complex", subAttributes=[attr("formatted"), attr("givenName"), attr("familyName")]), attr("active", "boolean"),
        attr("emails", "complex", multiValued=True, subAttributes=[attr("value"), attr("type"), attr("primary", "boolean")]),
        attr("roles", "complex", multiValued=True, subAttributes=[attr("value")]), attr("groups", "complex", multiValued=True, mutability="readOnly")]}
    group = {"id": GROUP_SCHEMA, "name": "Group", "description": "Group", "attributes": [attr("displayName", required=True), attr("externalId"),
             attr("members", "complex", multiValued=True, subAttributes=[attr("value"), attr("display"), attr("$ref", "reference")])]}
    return {"schemas": [LIST_SCHEMA], "totalResults": 2, "startIndex": 1, "itemsPerPage": 2, "Resources": [user, group]}


# ---------------------------------------------------------------- HTTP routes (/scim/v2)

import asyncio
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

router = APIRouter()


def _base(request: Request) -> str:
    cfg = get_config()
    root = cfg["base_url"] or str(request.base_url).rstrip("/")
    return root.rstrip("/") + ("" if root.rstrip("/").endswith("/scim/v2") else "/scim/v2")


def _reply(status: int, body: Optional[Dict[str, Any]], location: Optional[str] = None) -> Response:
    headers = {"Location": location} if location else {}
    if body is None:
        return Response(status_code=status, headers=headers)
    return JSONResponse(status_code=status, content=body, media_type=MEDIA_TYPE, headers=headers)


async def _handle(request: Request, fn: Callable[..., Any], success: int = 200, needs_body: bool = False, describe: str = "") -> Response:
    token_name = "-"
    status = 500
    detail = ""
    try:
        try:
            tok = await asyncio.to_thread(authenticate, request.headers.get("authorization"))
            token_name = tok["name"]
        except ScimError as exc:
            status, detail = exc.status, exc.detail
            resp = JSONResponse(status_code=401, content=exc.body(), media_type=MEDIA_TYPE, headers={"WWW-Authenticate": 'Bearer realm="scim"'})
            await asyncio.to_thread(log_event, "-", request.method, str(request.url.path), 401, detail)
            return resp
        if not get_config()["enabled"]:
            raise ScimError(403, "SCIM provisioning is turned off on this studio.")
        payload = None
        if needs_body:
            raw = await request.body()
            if len(raw) > MAX_BODY:
                raise ScimError(413, "The request body is too large.", "tooMany")
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except (ValueError, UnicodeDecodeError):
                raise ScimError(400, "The request body is not valid JSON.", "invalidSyntax")
            if not isinstance(payload, dict):
                raise ScimError(400, "The request body must be a JSON object.", "invalidSyntax")
        result = await asyncio.to_thread(fn, payload, dict(request.query_params), _base(request), f"scim:{token_name}")
        status = success
        detail = describe
        loc = result.get("meta", {}).get("location") if isinstance(result, dict) and success == 201 else None
        return _reply(success, result, loc)
    except ScimError as exc:
        status, detail = exc.status, exc.detail
        return JSONResponse(status_code=exc.status, content=exc.body(), media_type=MEDIA_TYPE)
    except Exception as exc:                                    # a bug must not leak internals to the IdP
        logger.error(f"SCIM {request.method} {request.url.path} failed: {exc}", exc_info=True)
        status, detail = 500, "internal error"
        return JSONResponse(status_code=500, content=ScimError(500, "The studio could not process this request.").body(), media_type=MEDIA_TYPE)
    finally:
        if token_name != "-":
            q = request.url.query
            await asyncio.to_thread(log_event, token_name, request.method, str(request.url.path) + (("?" + q) if q else ""), status, detail)


@router.get("/scim/v2/ServiceProviderConfig")
async def r_spc(request: Request):
    return await _handle(request, lambda p, q, base, actor: service_provider_config(base))


@router.get("/scim/v2/ResourceTypes")
async def r_rt(request: Request):
    return await _handle(request, lambda p, q, base, actor: resource_types(base))


@router.get("/scim/v2/Schemas")
async def r_schemas(request: Request):
    return await _handle(request, lambda p, q, base, actor: schemas(base))


@router.get("/scim/v2/Users")
async def r_users_list(request: Request):
    return await _handle(request, lambda p, q, base, actor: list_users(q, base))


@router.post("/scim/v2/Users")
async def r_users_create(request: Request):
    return await _handle(request, lambda p, q, base, actor: create_user(p, base, actor), success=201, needs_body=True, describe="user created")


@router.get("/scim/v2/Users/{uid}")
async def r_user_get(uid: str, request: Request):
    return await _handle(request, lambda p, q, base, actor: get_user(uid, base))


@router.put("/scim/v2/Users/{uid}")
async def r_user_put(uid: str, request: Request):
    return await _handle(request, lambda p, q, base, actor: replace_user(uid, p, base, actor), needs_body=True, describe="user replaced")


@router.patch("/scim/v2/Users/{uid}")
async def r_user_patch(uid: str, request: Request):
    return await _handle(request, lambda p, q, base, actor: patch_user(uid, p, base, actor), needs_body=True, describe="user patched")


@router.delete("/scim/v2/Users/{uid}")
async def r_user_delete(uid: str, request: Request):
    def run(p, q, base, actor):
        delete_user(uid, actor)
        return None
    return await _handle(request, run, success=204, describe="user deleted")


@router.get("/scim/v2/Groups")
async def r_groups_list(request: Request):
    return await _handle(request, lambda p, q, base, actor: list_groups(q, base))


@router.post("/scim/v2/Groups")
async def r_groups_create(request: Request):
    return await _handle(request, lambda p, q, base, actor: create_group(p, base, actor), success=201, needs_body=True, describe="group created")


@router.get("/scim/v2/Groups/{gid}")
async def r_group_get(gid: str, request: Request):
    return await _handle(request, lambda p, q, base, actor: get_group(gid, base))


@router.put("/scim/v2/Groups/{gid}")
async def r_group_put(gid: str, request: Request):
    return await _handle(request, lambda p, q, base, actor: replace_group(gid, p, base, actor), needs_body=True, describe="group replaced")


@router.patch("/scim/v2/Groups/{gid}")
async def r_group_patch(gid: str, request: Request):
    return await _handle(request, lambda p, q, base, actor: patch_group(gid, p, base, actor), needs_body=True, describe="group patched")


@router.delete("/scim/v2/Groups/{gid}")
async def r_group_delete(gid: str, request: Request):
    def run(p, q, base, actor):
        delete_group(gid, actor)
        return None
    return await _handle(request, run, success=204, describe="group deleted")
