"""SQL `GRANT`, `REVOKE` and `SHOW GRANTS` for catalogs, schemas and tables, backed by the same grants the UI manages:
catalog ACLs (`catalog_permissions`: READ / WRITE / ADMIN) and table / schema grants (`resource_grants`: SELECT / MODIFY, web/table_access.py).
No SQL engine understands this syntax, so `execute_sql` hands such a statement to this module before governance and dispatch (like SHALLOW CLONE).

    GRANT SELECT ON TABLE sales.dbo.orders TO GROUP analysts;
    GRANT SELECT, MODIFY ON SCHEMA sales.dbo TO USER alice, USER bob;
    GRANT SELECT ON FUTURE TABLES IN SCHEMA sales.dbo TO GROUP analysts;      -- a schema grant: it covers tables created later too
    GRANT ALL PRIVILEGES ON CATALOG sales TO GROUP data_owners;
    REVOKE SELECT ON TABLE sales.dbo.orders FROM GROUP analysts;
    SHOW GRANTS ON TABLE sales.dbo.orders;   SHOW GRANTS TO GROUP analysts;   SHOW GRANTS;   -- the last one lists your own

Privileges (case-insensitive):  SELECT | USAGE | USE CATALOG | USE SCHEMA | READ  -> read;   MODIFY | INSERT | UPDATE | DELETE | CREATE | WRITE -> write;
ALL [PRIVILEGES] | MANAGE | ADMIN -> everything (on a catalog: ADMIN; on a schema or table: MODIFY). Levels are a ladder (write includes read), so GRANT
only ever raises what someone holds, REVOKE of a privilege also removes the ones above it, and REVOKE ALL removes the grant.
Principals: `USER name`, `GROUP name`, or a bare name when it is unambiguous. Roles (admin / power_user / user) are not grantable: use a group.
Not supported, and refused with an explanation rather than ignored: WITH GRANT OPTION, GRANT OPTION FOR, column-level grants, and a privilege on a
catalog other than the catalog ACL levels. Who may grant: an administrator, or the owner of the catalog (a power user) -- the same rule as the UI.
"""
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("localspark.sql_grants")

READ_WORDS = {"SELECT", "USAGE", "USE", "READ", "USE CATALOG", "USE SCHEMA", "BROWSE"}
WRITE_WORDS = {"MODIFY", "INSERT", "UPDATE", "DELETE", "CREATE", "WRITE", "CREATE TABLE", "CREATE SCHEMA"}
ALL_WORDS = {"ALL", "ALL PRIVILEGES", "MANAGE", "ADMIN"}
_TOKEN = re.compile(r'''\s*(?:(?P<q>"(?:[^"]|"")*"|`(?:[^`]|``)*`)|(?P<w>[A-Za-z0-9_$@\-]+)|(?P<p>[,.;()]))''')


class GrantSqlError(ValueError):
    """A statement this module recognises but cannot (or may not) run; the message is safe to show."""


def _tokens(sql: str) -> List[Tuple[str, str]]:
    out, pos = [], 0
    sql = sql.strip()
    while pos < len(sql):
        m = _TOKEN.match(sql, pos)
        if not m or m.end() == pos:
            raise GrantSqlError(f"Unexpected text near '{sql[pos:pos + 20]}'.")
        if m.group("q"):
            raw = m.group("q")
            out.append(("id", raw[1:-1].replace(raw[0] * 2, raw[0])))
        elif m.group("w"):
            out.append(("w", m.group("w")))
        else:
            out.append(("p", m.group("p")))
        pos = m.end()
    while out and out[-1] == ("p", ";"):
        out.pop()
    if any(t == ("p", ";") for t in out):
        raise GrantSqlError("Run one GRANT / REVOKE / SHOW GRANTS statement at a time.")
    return out


class _P:
    def __init__(self, toks: List[Tuple[str, str]]):
        self.t, self.i = toks, 0

    def peek(self, n: int = 0) -> Optional[Tuple[str, str]]:
        return self.t[self.i + n] if self.i + n < len(self.t) else None

    def word(self, *words: str) -> Optional[str]:
        """Consumes the next token if it is one of `words` (unquoted, case-insensitive)."""
        tk = self.peek()
        if tk and tk[0] == "w" and tk[1].upper() in words:
            self.i += 1
            return tk[1].upper()
        return None

    def expect(self, *words: str) -> str:
        w = self.word(*words)
        if not w:
            got = self.peek()
            raise GrantSqlError(f"Expected {' or '.join(words)} but found {got[1] if got else 'the end of the statement'}.")
        return w

    def name(self) -> str:
        tk = self.peek()
        if not tk or tk[0] not in ("w", "id"):
            raise GrantSqlError("A name is expected here.")
        self.i += 1
        return tk[1]

    def dotted(self) -> List[str]:
        parts = [self.name()]
        while self.peek() == ("p", "."):
            self.i += 1
            parts.append(self.name())
        return parts

    def comma(self) -> bool:
        if self.peek() == ("p", ","):
            self.i += 1
            return True
        return False

    def done(self) -> bool:
        return self.i >= len(self.t)


def _privileges(p: _P) -> List[str]:
    privs: List[str] = []
    while True:
        first = p.name().upper()
        phrase = first
        if first in ("ALL", "USE", "CREATE") and p.peek() and p.peek()[0] == "w" and p.peek()[1].upper() in ("PRIVILEGES", "CATALOG", "SCHEMA", "TABLE"):
            phrase = f"{first} {p.name().upper()}"
        if p.peek() == ("p", "("):
            raise GrantSqlError("Column-level grants are not supported. Use a masking policy to restrict columns.")
        privs.append(phrase)
        if not p.comma():
            return privs


def _target(p: _P) -> Dict[str, Any]:
    fa = p.word("FUTURE", "ALL")
    if fa:
        p.expect("TABLES")
        p.expect("IN")
        scope = p.expect("SCHEMA", "CATALOG")
        if scope == "CATALOG":
            raise GrantSqlError("Grants on all tables of a catalog are not supported; grant the catalog itself (GRANT SELECT ON CATALOG x ...) or one schema at a time.")
        return {"kind": "schema", "parts": p.dotted(), "future": fa}
    kind = p.word("CATALOG", "SCHEMA", "DATABASE", "TABLE", "VIEW") or None
    if kind in ("DATABASE",):
        kind = "SCHEMA"
    if kind == "VIEW":
        kind = "TABLE"
    parts = p.dotted()
    if kind is None:
        if len(parts) != 3:
            raise GrantSqlError("Say what the object is: ON TABLE <catalog.schema.table>, ON SCHEMA <catalog.schema> or ON CATALOG <catalog>.")
        kind = "TABLE"
    return {"kind": kind.lower(), "parts": parts}


def _principal(p: _P) -> Dict[str, Any]:
    kind = p.word("USER", "GROUP", "ROLE")
    return {"kind": kind.lower() if kind else None, "name": p.name()}


def parse(sql: str) -> Optional[Dict[str, Any]]:
    """The statement as a dict, None when `sql` is not GRANT / REVOKE / SHOW GRANTS; raises GrantSqlError for a malformed one."""
    head = re.match(r"\s*(GRANT|REVOKE|SHOW\s+GRANTS)\b", sql or "", re.I)
    if not head:
        return None
    op = re.sub(r"\s+", " ", head.group(1).upper())
    p = _P(_tokens(sql))
    p.i = 2 if op == "SHOW GRANTS" else 1
    if op == "SHOW GRANTS":
        stmt: Dict[str, Any] = {"op": "show", "target": None, "principal": None}
        if p.word("ON"):
            stmt["target"] = _target(p)
        if p.word("TO", "FOR"):
            stmt["principal"] = _principal(p)
        elif not stmt["target"] and not p.done():
            stmt["principal"] = _principal(p)          # SHOW GRANTS alice
        if not p.done():
            raise GrantSqlError("Unexpected text after SHOW GRANTS.")
        return stmt
    if op == "REVOKE" and p.word("GRANT"):
        raise GrantSqlError("REVOKE GRANT OPTION FOR is not supported: grants here cannot be re-granted by their holders.")
    privs = _privileges(p)
    p.expect("ON")
    target = _target(p)
    p.expect("TO" if op == "GRANT" else "FROM")
    principals = [_principal(p)]
    while p.comma():
        principals.append(_principal(p))
    if op == "GRANT" and p.word("WITH"):
        raise GrantSqlError("WITH GRANT OPTION is not supported: only administrators and catalog owners grant access.")
    if not p.done():
        raise GrantSqlError("Unexpected text at the end of the statement.")
    return {"op": op.lower(), "privileges": privs, "target": target, "principals": principals}


# ---------------------------------------------------------------- resolution

def _level(privs: List[str], kind: str) -> str:
    """The highest level the privilege list stands for, in the target's own vocabulary."""
    best = 0
    for pv in privs:
        if pv in ALL_WORDS:
            best = max(best, 2)
        elif pv in WRITE_WORDS:
            best = max(best, 1)
        elif pv in READ_WORDS:
            best = max(best, 0)
        else:
            raise GrantSqlError(f"'{pv}' is not a privilege here. Use SELECT / USAGE (read), MODIFY (write) or ALL PRIVILEGES.")
    return (("READ", "WRITE", "ADMIN") if kind == "catalog" else ("SELECT", "MODIFY", "MODIFY"))[best]


def _object(target: Dict[str, Any], default_catalog: str) -> Tuple[str, str, str]:
    """(kind, resource id, catalog) with identifiers lower-cased."""
    parts = [x.lower() for x in target["parts"]]
    kind = target["kind"]
    if kind == "catalog":
        if len(parts) != 1:
            raise GrantSqlError("A catalog is named by itself: ON CATALOG <catalog>.")
        return "catalog", parts[0], parts[0]
    if kind == "schema":
        if len(parts) == 1:
            parts = [default_catalog.lower(), parts[0]]
        if len(parts) != 2:
            raise GrantSqlError("A schema is named <catalog>.<schema> (or just <schema> in the current catalog).")
        return "schema", ".".join(parts), parts[0]
    if len(parts) == 2:
        parts = [default_catalog.lower()] + parts
    elif len(parts) == 1:
        parts = [default_catalog.lower(), "dbo"] + parts
    if len(parts) != 3:
        raise GrantSqlError("A table is named <catalog>.<schema>.<table> (or <schema>.<table> in the current catalog).")
    return "table", ".".join(parts), parts[0]


def _exists(kind: str, rid: str) -> bool:
    """A grant on something that does not exist is almost always a typo. Local catalogs are checked on disk; mounts are trusted."""
    from web.warehouses import get_catalog
    parts = rid.split(".")
    cat = get_catalog(parts[0])
    if not cat:
        return False
    if kind == "catalog" or cat.get("is_mounted"):
        return True
    import os
    base = cat.get("path") or ""
    if kind == "schema":
        return os.path.isdir(os.path.join(base, parts[1]))
    return os.path.isdir(os.path.join(base, parts[1], parts[2], "_delta_log"))


def _resolve_principal(pr: Dict[str, Any]) -> Tuple[str, str, str]:
    """(kind 'user'|'group', id, display name)."""
    from web import groups
    from web.auth import get_db_connection
    if pr["kind"] == "role":
        raise GrantSqlError("Roles (admin, power_user, user) cannot be granted access. Put the people in a group and grant the group.")
    name = pr["name"]
    conn = get_db_connection()
    try:
        u = conn.execute("SELECT id, username FROM users WHERE lower(username) = lower(?) AND deleted_at IS NULL", (name,)).fetchone() if pr["kind"] in (None, "user") else None
    finally:
        conn.close()
    g = next((x for x in groups.list_groups() if x["name"].lower() == name.lower()), None) if pr["kind"] in (None, "group") else None
    if u and g:
        raise GrantSqlError(f"'{name}' is both a user and a group: say USER {name} or GROUP {name}.")
    if u:
        return "user", u["id"], u["username"]
    if g:
        return "group", g["id"], g["name"]
    raise GrantSqlError(f"No {pr['kind'] or 'user or group'} named '{name}'.")


def _may_manage(user: Dict[str, Any], catalog: str) -> None:
    from web.permissions import can_user_manage_catalog
    if not can_user_manage_catalog(user, catalog):
        raise GrantSqlError(f"Only an administrator or the owner of catalog '{catalog}' can change its access.")


def _current(kind: str, rid: str, principal: Tuple[str, str, str]) -> Optional[str]:
    """The level the principal holds directly on the object (None when nothing)."""
    from web.auth import get_db_connection
    if kind == "catalog":
        pid = principal[1] if principal[0] == "user" else f"group:{principal[1]}"
        conn = get_db_connection()
        try:
            r = conn.execute("SELECT permission FROM catalog_permissions WHERE catalog_id = ? AND (user_id = ? OR user_id = ?)", (rid, pid, principal[2] if principal[0] == "user" else pid)).fetchone()
        finally:
            conn.close()
        return r["permission"].upper() if r else None
    from web import groups
    for g in groups.list_grants(kind, rid):
        if g["principal"] == f"{principal[0]}:{principal[1]}":
            return g["permission"]
    return None


_LADDER = {"catalog": ("READ", "WRITE", "ADMIN"), "table": ("SELECT", "MODIFY"), "schema": ("SELECT", "MODIFY")}


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


def _apply(op: str, kind: str, rid: str, catalog: str, principal: Tuple[str, str, str], level: str, user: Dict[str, Any]) -> str:
    """One principal, one object. Returns a sentence describing what changed."""
    ladder = _LADDER[kind]
    cur = _current(kind, rid, principal)
    who = f"{principal[0]} {principal[2]}"
    show = {"READ": "SELECT", "WRITE": "MODIFY", "ADMIN": "ALL PRIVILEGES"}.get(level, level)
    if op == "grant":
        if cur and ladder.index(cur) >= ladder.index(level):
            return f"{who} already holds {ladder[ladder.index(cur)]} on {kind} {rid}."
        if kind == "catalog":
            from web.permissions import grant_catalog_permission
            grant_catalog_permission(rid, principal[1] if principal[0] == "user" else f"group:{principal[1]}", level, user)
        else:
            from web import groups
            groups.grant(kind, rid, f"{principal[0]}:{principal[1]}", level, user.get("username", "admin"))
        return f"Granted {show} on {kind} {rid} to {who}."
    # revoke: the level asked for and everything above it goes; what is left is the level just below it (or nothing)
    if not cur or ladder.index(cur) < ladder.index(level):
        return f"{who} does not hold {show} on {kind} {rid}."
    remaining = ladder[ladder.index(level) - 1] if ladder.index(level) > 0 else None
    if kind == "catalog":
        from web.permissions import grant_catalog_permission, revoke_catalog_permission
        target = principal[1] if principal[0] == "user" else f"group:{principal[1]}"
        revoke_catalog_permission(rid, target, user)
        if remaining:
            grant_catalog_permission(rid, target, remaining, user)
    else:
        from web import groups
        actor = user.get("username", "admin")
        if remaining:
            groups.grant(kind, rid, f"{principal[0]}:{principal[1]}", remaining, actor)
        else:
            groups.revoke(kind, rid, f"{principal[0]}:{principal[1]}", actor)
    return f"Revoked {show} on {kind} {rid} from {who}" + (f" (still holds {remaining})." if remaining else ".")


def run(stmt: Dict[str, Any], sql: str, user: Dict[str, Any], default_catalog: str = "warehouse") -> Dict[str, Any]:
    """Executes a parsed statement as `user`. GRANT / REVOKE -> {'message'}; SHOW GRANTS -> {'columns', 'rows'}. Raises GrantSqlError."""
    if stmt["op"] == "show":
        return _show(stmt, user, default_catalog)
    kind, rid, catalog = _object(stmt["target"], default_catalog)
    _may_manage(user, catalog)
    level = _level(stmt["privileges"], kind)
    if stmt["op"] == "revoke" and any(pv in ALL_WORDS for pv in stmt["privileges"]):
        level = _LADDER[kind][0]                     # REVOKE ALL: the whole grant goes, whatever level it was
    if not _exists(kind, rid):
        raise GrantSqlError(f"{kind.capitalize()} '{rid}' does not exist.")
    principals = [_resolve_principal(pr) for pr in stmt["principals"]]          # resolved first: nothing is half-applied on a typo
    notes = [_apply(stmt["op"], kind, rid, catalog, pr, level, user) for pr in principals]
    if stmt["target"].get("future") == "FUTURE" or stmt["target"].get("future") == "ALL":
        notes.append("(A schema grant covers the tables in the schema now and any created later.)")
    _audit(user.get("username", "admin"), "SQL_" + stmt["op"].upper(), f"{kind}:{rid}", {"statement": sql.strip()[:300], "principals": [f"{p[0]}:{p[2]}" for p in principals]})
    return {"message": " ".join(notes)}


# ---------------------------------------------------------------- SHOW GRANTS

COLUMNS = ["principal", "principal_type", "object_type", "object", "privilege", "granted_by", "granted_at"]


def _show(stmt: Dict[str, Any], user: Dict[str, Any], default_catalog: str) -> Dict[str, Any]:
    from web.auth import get_db_connection
    from web import groups
    from web.permissions import can_user_manage_catalog
    is_admin = user.get("role") == "admin"
    rows: List[List[Any]] = []
    names = {u["id"]: u["username"] for u in _all_users()}
    gnames = {g["id"]: g["name"] for g in groups.list_groups()}
    priv = {"READ": "SELECT", "WRITE": "MODIFY", "ADMIN": "ALL PRIVILEGES"}

    def who(pid: str) -> Tuple[str, str]:
        if pid.startswith("group:"):
            return gnames.get(pid[6:], pid), "group"
        return names.get(pid, pid), "user"

    target = _object(stmt["target"], default_catalog) if stmt["target"] else None
    principal = _resolve_principal(stmt["principal"]) if stmt["principal"] else None
    if target:
        if not (is_admin or can_user_manage_catalog(user, target[2])):
            raise GrantSqlError(f"Only an administrator or the owner of catalog '{target[2]}' can list who has access to it.")
    elif principal is None:
        principal = ("user", user.get("id", ""), user.get("username", ""))
    elif not is_admin and not (principal[0] == "user" and principal[1] == user.get("id")):
        raise GrantSqlError("You can list your own grants; listing another principal's needs an administrator.")

    conn = get_db_connection()
    try:
        q, params = "SELECT catalog_id, user_id, permission, granted_by, created_at FROM catalog_permissions", []
        conds = []
        if target:
            conds.append("catalog_id = ?"); params.append(target[2])
        if principal:
            ids = [principal[1], principal[2]] if principal[0] == "user" else [f"group:{principal[1]}"]
            conds.append(f"user_id IN ({','.join('?' * len(ids))})"); params += ids
        for r in conn.execute(q + (" WHERE " + " AND ".join(conds) if conds else "") + " ORDER BY catalog_id, user_id", params):
            n, t = who(r["user_id"])
            if target and target[0] != "catalog":
                continue                                   # a table/schema was asked for: catalog-wide access is shown below as inherited
            rows.append([n, t, "catalog", r["catalog_id"], priv.get(r["permission"].upper(), r["permission"]), r["granted_by"], r["created_at"]])
        for rt in ("schema", "table"):
            for g in (groups.list_grants(rt, target[1]) if target and target[0] == rt else groups.list_grants_prefix((rt,), (target[2] + ".") if target else "")):
                if principal and g["principal"] != f"{principal[0]}:{principal[1]}":
                    continue
                pid = g["principal"].split(":", 1)[1]
                n, t = (gnames.get(pid, pid), "group") if g["principal_type"] == "group" else (g["name"], "user")
                rows.append([n, t, rt, g.get("resource_id") or target[1], g["permission"], g["granted_by"], g["created_at"]])
        if target and target[0] != "catalog":                # catalog-level grants apply to the object too: show them as inherited
            for r in conn.execute("SELECT user_id, permission, granted_by, created_at FROM catalog_permissions WHERE catalog_id = ?", (target[2],)):
                n, t = who(r["user_id"])
                if principal and (n, t) != (principal[2], principal[0]):
                    continue
                rows.append([n, t, "catalog (inherited)", target[2], priv.get(r["permission"].upper(), r["permission"]), r["granted_by"], r["created_at"]])
    finally:
        conn.close()
    return {"columns": [{"name": c, "type": "object"} for c in COLUMNS], "rows": rows, "row_count": len(rows)}


def _all_users() -> List[Dict[str, Any]]:
    from web.auth import get_db_connection
    conn = get_db_connection()
    try:
        return [dict(r) for r in conn.execute("SELECT id, username FROM users")]
    finally:
        conn.close()
