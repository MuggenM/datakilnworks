"""Delta Sharing server: share Delta tables with people OUTSIDE the organisation over the open Delta Sharing protocol (any client: the
`delta-sharing` Python package, Spark, pandas, Power BI, Tableau, Databricks...).

Model    A SHARE is a named set of tables (`schema.table` names of your choosing, each pointing at a `catalog.schema.table`). A RECIPIENT is an
         outside party with a bearer token (shown once with a downloadable profile file, only its SHA-256 is stored, optional expiry, revocable,
         rotatable) and the list of shares it may read. Everything is administered by admins only and audited (`SHARING_*` in governance_audit).
Protocol REST under /delta-sharing (list shares / schemas / tables, table version, metadata, query). Query answers with pre-signed file URLs:
         short-lived (`DELTA_SHARING_URL_TTL`, default 900 s) HMAC-signed links to /delta-sharing/files/<token> on this server, which streams the
         Parquet file. A file link is re-checked on every fetch (recipient not revoked or expired, table still in the share, governance still fine),
         so revoking a recipient stops downloads at once.
Governance  A Delta Sharing recipient receives the raw Parquet files, so no masking or row filter can be applied. A table is therefore only shareable
         when NO masking policy and NO row filter policy would apply to a non-exempt principal (this also refuses dbt output that is still
         "closed by default"). It is checked when the table is added AND on every metadata / query / file request, so a policy created later
         cuts the sharing off (fail closed, with the reason). Tables with reader features beyond protocol 1 (deletion vectors, column mapping) are
         refused: the Parquet-format response cannot express them. Only local Delta tables are shared (not S3 mounts).
History  Older versions, time travel and the change feed expose rows that may have been deleted since, so they are OFF by default: a table is shared
         "with history" only when the administrator says so (`history` on the share table). Without it only the latest version can be read.
Change data feed  GET .../changes?startingVersion=&endingVersion= (or timestamps) reads the Delta log itself: a commit that wrote change-data files
         (delta.enableChangeDataFeed) yields `cdf` actions, other commits yield `add` / `remove` actions (dataChange only). It needs history sharing.
         Removed and change-data files are signed like any other file; a removed file that VACUUM has deleted answers 404.
Response formats  `delta-sharing-capabilities: responseformat=delta` (the client asks for Delta alone) gets the Delta format (delta protocol / metadata /
         `deltaSingleAction`); when the client lists parquet as well, or says nothing, the answer is the Parquet format. Both are built from the same file list. Tables whose reader protocol needs features (deletion vectors,
         column mapping) are still refused in BOTH formats: their file layout could not be checked here.
Hints    `predicateHints` (SQL such as `region = 'EMEA' AND id > 5`) and `jsonPredicateHints` prune files by partition values (exact) and by min/max /
         null counts (integers, short strings, dates). A hint that is not understood keeps the file (a hint can only skip files that PROVABLY hold no
         matching row). `limitHint` stops after enough files, but only when no inexact predicate is involved.
Stats    numRecords, nullCount and min / max for integer, short-string (< 32 chars) and date columns. Floats, decimals, timestamps and nested columns are
         left out on purpose: a wrong bound would make a client silently skip rows.
Recipient IP rules  a recipient may carry a list of CIDRs; its token and its file links then work only from those addresses (the client address is
         resolved like the global allowlist does: only trusted proxies' X-Forwarded-For is believed).
Not implemented  `startingVersion` inside a table query (use /changes), historical metadata inside a change range (`includeHistoricalMetadata` is
         ignored: the current metadata is sent), OIDC federation.

DELTA_SHARING_ENDPOINT  public base URL of the protocol (default: this request's origin + /delta-sharing) written into recipient profiles and file URLs
DELTA_SHARING_URL_TTL   lifetime of a file URL in seconds (default 900)
DELTA_SHARING_MAX_FILES refuse to answer a query with more files than this (default 50000; compact the table)
DELTA_SHARING=off       switches the protocol endpoints off (404)
"""
import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import sqlite3
import threading
import time
import uuid
import ipaddress
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("localspark.sharing")

URL_TTL = int(os.getenv("DELTA_SHARING_URL_TTL", "900"))
MAX_FILES = int(os.getenv("DELTA_SHARING_MAX_FILES", "50000"))
MAX_CHANGE_VERSIONS = int(os.getenv("DELTA_SHARING_MAX_CHANGE_VERSIONS", "1000"))
MAX_TABLES_PER_SHARE = 500
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_LOCK = threading.Lock()


class SharingError(Exception):
    """A refused request; the message is safe to show. `code` maps to the protocol's errorCode."""
    def __init__(self, message: str, code: str = "INVALID_PARAMETER_VALUE", status: int = 400):
        super().__init__(message)
        self.code, self.status = code, status


class NotFound(SharingError):
    def __init__(self, message: str = "The resource does not exist."):
        super().__init__(message, "RESOURCE_DOES_NOT_EXIST", 404)


def enabled() -> bool:
    return os.getenv("DELTA_SHARING", "on").strip().lower() not in ("off", "false", "0", "no")


def _meta_dir() -> str:
    d = os.path.join(os.getenv("WAREHOUSE_DIR", "/workspace/warehouse"), ".metadata")
    os.makedirs(d, exist_ok=True)
    return d


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(os.path.join(_meta_dir(), "sharing.db"), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS shares (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, comment TEXT, created_by TEXT, created_at TEXT);
        CREATE TABLE IF NOT EXISTS share_tables (id TEXT PRIMARY KEY, share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
            schema_name TEXT NOT NULL, table_name TEXT NOT NULL, source TEXT NOT NULL, added_by TEXT, added_at TEXT, UNIQUE (share_id, schema_name, table_name));
        CREATE TABLE IF NOT EXISTS recipients (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, comment TEXT, token_hash TEXT NOT NULL UNIQUE, token_prefix TEXT,
            expires_at TEXT, created_by TEXT, created_at TEXT, revoked_at TEXT, last_used_at TEXT, bytes_served INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS recipient_shares (recipient_id TEXT NOT NULL REFERENCES recipients(id) ON DELETE CASCADE,
            share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE, PRIMARY KEY (recipient_id, share_id));
        CREATE TABLE IF NOT EXISTS sharing_log (id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT, recipient TEXT, action TEXT, share TEXT, tbl TEXT, detail TEXT);
    """)
    for table, col, ddl in (("share_tables", "history", "INTEGER DEFAULT 0"), ("recipients", "allowed_cidrs", "TEXT")):
        if col not in [r["name"] for r in conn.execute(f"PRAGMA table_info({table})")]:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
    conn.commit()
    return conn


def init_sharing_db() -> None:
    _db().close()


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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


def _log(recipient: str, action: str, share: str = "", tbl: str = "", detail: str = "") -> None:
    try:
        c = _db()
        try:
            c.execute("INSERT INTO sharing_log (at, recipient, action, share, tbl, detail) VALUES (?,?,?,?,?,?)", (_now(), recipient, action, share, tbl, detail[:300]))
            c.execute("DELETE FROM sharing_log WHERE id <= (SELECT MAX(id) FROM sharing_log) - 5000")
            c.commit()
        finally:
            c.close()
    except Exception as exc:
        logger.warning(f"could not log {action}: {exc}")


def _clean_name(value: str, what: str) -> str:
    v = (value or "").strip()
    if not _NAME.match(v):
        raise SharingError(f"A {what} name starts with a letter or underscore and has only letters, digits and underscores (max 64).")
    return v


# ---------------------------------------------------------------- source tables and governance

def _split_source(source: str) -> Tuple[str, str, str]:
    parts = [p.strip() for p in (source or "").split(".")]
    if len(parts) != 3 or not all(_NAME.match(p) for p in parts):
        raise SharingError("The source must be catalog.schema.table.")
    return parts[0], parts[1], parts[2]


def table_root(source: str) -> str:
    """Local directory of the Delta table; refuses S3 and anything that is not a Delta table."""
    from deltalake import DeltaTable
    from web import time_travel
    cat, sch, tbl = _split_source(source)
    path, _ = time_travel.resolve_table_path(sch, tbl, cat)
    if "://" in path:
        raise SharingError("Only Delta tables on local storage can be shared (not tables in an S3 mount).")
    if not os.path.isdir(path) or not DeltaTable.is_deltatable(path):
        raise SharingError(f"{source} is not a Delta table.")
    return os.path.realpath(path)


_DUCK = {"long": "BIGINT", "integer": "INTEGER", "short": "SMALLINT", "byte": "TINYINT", "string": "VARCHAR", "double": "DOUBLE", "float": "FLOAT",
         "boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMP WITH TIME ZONE", "timestamp_ntz": "TIMESTAMP", "binary": "BLOB"}


def _duck_type(t: Any) -> str:
    """A Delta schema type as the DuckDB type name the mask resolver expects."""
    if isinstance(t, str):
        return _DUCK.get(t, t.upper() if t.startswith("decimal") else "VARCHAR")
    return "STRUCT"


def governance_problem(source: str, dt=None) -> Optional[str]:
    """Why this table must not be handed out as raw files, or None. Fail closed: an error in the check is a problem."""
    try:
        from deltalake import DeltaTable
        from web.governance import policies, row_filters
        cat, sch, tbl = (p.lower() for p in _split_source(source))
        dt = dt or DeltaTable(table_root(source))
        cols = [{"column": f["name"], "type": _duck_type(f["type"])} for f in json.loads(dt.schema().to_json())["fields"]]
        ghost = policies.Principal(username="\u0000sharing", role="user")
        masks = policies.masks_for_table(cat, sch, tbl, cols, ghost)
        if masks:
            return f"masking policy '{masks[0].policy_name}' applies to column '{masks[0].column}': raw files cannot be masked."
        flt = row_filters.filters_for_table(cat, sch, tbl, cols, ghost)
        if flt:
            return f"row filter policy '{flt[0].policy_name}' applies to this table: raw files cannot be filtered."
        prot = dt.protocol()
        if prot.min_reader_version > 1 or prot.reader_features:
            return f"the table needs Delta reader protocol {prot.min_reader_version} (features: {prot.reader_features}); the Parquet sharing format supports only version 1."
        return None
    except SharingError as exc:
        return str(exc)
    except Exception as exc:
        logger.error(f"governance check for sharing {source} failed: {exc}", exc_info=True)
        return "the governance check failed, so the table is treated as not shareable."


# ---------------------------------------------------------------- administration

def _share_row(c, name: str):
    r = c.execute("SELECT * FROM shares WHERE name = ?", (name,)).fetchone()
    if not r:
        raise NotFound(f"Share '{name}' does not exist.")
    return r


def list_shares() -> List[Dict[str, Any]]:
    c = _db()
    try:
        out = []
        for s in c.execute("SELECT * FROM shares ORDER BY name"):
            tabs = [{**dict(t), "history": bool(t["history"])} for t in c.execute("SELECT schema_name, table_name, source, history, added_by, added_at FROM share_tables WHERE share_id = ? ORDER BY schema_name, table_name", (s["id"],))]
            recips = [r["name"] for r in c.execute("SELECT r.name FROM recipient_shares rs JOIN recipients r ON r.id = rs.recipient_id WHERE rs.share_id = ? ORDER BY r.name", (s["id"],))]
            out.append({"id": s["id"], "name": s["name"], "comment": s["comment"], "created_by": s["created_by"], "created_at": s["created_at"], "tables": tabs, "recipients": recips})
        return out
    finally:
        c.close()


def create_share(name: str, comment: str, actor: str) -> Dict[str, Any]:
    name = _clean_name(name, "share")
    c = _db()
    try:
        try:
            sid = uuid.uuid4().hex
            c.execute("INSERT INTO shares (id, name, comment, created_by, created_at) VALUES (?,?,?,?,?)", (sid, name, (comment or "")[:300], actor, _now()))
            c.commit()
        except sqlite3.IntegrityError:
            raise SharingError(f"A share named '{name}' already exists.", "RESOURCE_ALREADY_EXISTS", 409)
    finally:
        c.close()
    _audit(actor, "SHARING_SHARE_CREATE", f"share:{name}", {})
    return {"id": sid, "name": name}


def delete_share(name: str, actor: str) -> None:
    c = _db()
    try:
        s = _share_row(c, name)
        c.execute("DELETE FROM shares WHERE id = ?", (s["id"],))
        c.commit()
    finally:
        c.close()
    _audit(actor, "SHARING_SHARE_DELETE", f"share:{name}", {})


def add_table(share: str, source: str, actor: str, schema_alias: Optional[str] = None, table_alias: Optional[str] = None, history: bool = False) -> Dict[str, Any]:
    from deltalake import DeltaTable
    cat, sch, tbl = _split_source(source)
    source = f"{cat}.{sch}.{tbl}"
    dt = DeltaTable(table_root(source))
    problem = governance_problem(source, dt)
    if problem:
        _audit(actor, "SHARING_TABLE_REFUSED", f"share:{share}", {"source": source, "reason": problem})
        raise SharingError(f"{source} cannot be shared: {problem}")
    s_alias, t_alias = _clean_name(schema_alias or sch, "schema"), _clean_name(table_alias or tbl, "table")
    c = _db()
    try:
        s = _share_row(c, share)
        if c.execute("SELECT COUNT(*) FROM share_tables WHERE share_id = ?", (s["id"],)).fetchone()[0] >= MAX_TABLES_PER_SHARE:
            raise SharingError(f"A share holds at most {MAX_TABLES_PER_SHARE} tables.")
        try:
            c.execute("INSERT INTO share_tables (id, share_id, schema_name, table_name, source, history, added_by, added_at) VALUES (?,?,?,?,?,?,?,?)",
                      (uuid.uuid4().hex, s["id"], s_alias, t_alias, source, 1 if history else 0, actor, _now()))
            c.commit()
        except sqlite3.IntegrityError:
            raise SharingError(f"{s_alias}.{t_alias} is already in the share.", "RESOURCE_ALREADY_EXISTS", 409)
    finally:
        c.close()
    _audit(actor, "SHARING_TABLE_ADD", f"share:{share}", {"source": source, "as": f"{s_alias}.{t_alias}", "history": bool(history)})
    return {"share": share, "schema": s_alias, "table": t_alias, "source": source, "history": bool(history)}


def set_table_history(share: str, schema: str, table: str, on: bool, actor: str) -> None:
    """Turns history sharing (older versions, time travel, change feed) on or off for one shared table."""
    c = _db()
    try:
        s = _share_row(c, share)
        cur = c.execute("UPDATE share_tables SET history = ? WHERE share_id = ? AND schema_name = ? AND table_name = ?", (1 if on else 0, s["id"], schema, table))
        c.commit()
        if not cur.rowcount:
            raise NotFound(f"{schema}.{table} is not in the share.")
    finally:
        c.close()
    _audit(actor, "SHARING_TABLE_HISTORY", f"share:{share}", {"table": f"{schema}.{table}", "history": bool(on)})


def remove_table(share: str, schema: str, table: str, actor: str) -> None:
    c = _db()
    try:
        s = _share_row(c, share)
        cur = c.execute("DELETE FROM share_tables WHERE share_id = ? AND schema_name = ? AND table_name = ?", (s["id"], schema, table))
        c.commit()
        if not cur.rowcount:
            raise NotFound(f"{schema}.{table} is not in the share.")
    finally:
        c.close()
    _audit(actor, "SHARING_TABLE_REMOVE", f"share:{share}", {"table": f"{schema}.{table}"})


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _new_token() -> str:
    return "dkw_dsh_" + secrets.token_hex(24)


def _recipient_view(c, r) -> Dict[str, Any]:
    shares = [x["name"] for x in c.execute("SELECT s.name FROM recipient_shares rs JOIN shares s ON s.id = rs.share_id WHERE rs.recipient_id = ? ORDER BY s.name", (r["id"],))]
    state = "revoked" if r["revoked_at"] else ("expired" if r["expires_at"] and r["expires_at"] < _now() else "active")
    return {"id": r["id"], "name": r["name"], "comment": r["comment"], "prefix": r["token_prefix"], "expires_at": r["expires_at"], "created_by": r["created_by"],
            "created_at": r["created_at"], "revoked_at": r["revoked_at"], "last_used_at": r["last_used_at"], "bytes_served": r["bytes_served"] or 0, "shares": shares, "state": state,
            "allowed_cidrs": json.loads(r["allowed_cidrs"] or "[]")}


def list_recipients() -> List[Dict[str, Any]]:
    c = _db()
    try:
        return [_recipient_view(c, r) for r in c.execute("SELECT * FROM recipients ORDER BY name")]
    finally:
        c.close()


def _set_shares(c, rid: str, shares: List[str]) -> None:
    ids = []
    for n in shares:
        ids.append(_share_row(c, n)["id"])
    c.execute("DELETE FROM recipient_shares WHERE recipient_id = ?", (rid,))
    for sid in dict.fromkeys(ids):
        c.execute("INSERT INTO recipient_shares (recipient_id, share_id) VALUES (?,?)", (rid, sid))


def _expiry(days: Optional[int]) -> Optional[str]:
    if days in (None, "", 0):
        return None
    try:
        n = int(days)
    except (TypeError, ValueError):
        raise SharingError("expires_in_days must be a whole number.")
    if not 1 <= n <= 3650:
        raise SharingError("expires_in_days must be between 1 and 3650.")
    return (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=n)).strftime("%Y-%m-%d %H:%M:%S")


def create_recipient(name: str, comment: str, shares: List[str], expires_in_days: Optional[int], actor: str) -> Dict[str, Any]:
    name = (name or "").strip()
    if not re.match(r"^[A-Za-z0-9][A-Za-z0-9 _.\-]{0,63}$", name):
        raise SharingError("A recipient name has letters, digits, spaces, dots, dashes and underscores (max 64).")
    token, rid, exp = _new_token(), uuid.uuid4().hex, _expiry(expires_in_days)
    c = _db()
    try:
        try:
            c.execute("INSERT INTO recipients (id, name, comment, token_hash, token_prefix, expires_at, created_by, created_at) VALUES (?,?,?,?,?,?,?,?)",
                      (rid, name, (comment or "")[:300], _hash(token), token[:14], exp, actor, _now()))
        except sqlite3.IntegrityError:
            raise SharingError(f"A recipient named '{name}' already exists.", "RESOURCE_ALREADY_EXISTS", 409)
        _set_shares(c, rid, shares or [])
        c.commit()
        view = _recipient_view(c, c.execute("SELECT * FROM recipients WHERE id = ?", (rid,)).fetchone())
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    _audit(actor, "SHARING_RECIPIENT_CREATE", f"recipient:{name}", {"shares": shares or [], "expires_at": exp})
    return {"recipient": view, "token": token, "expiration_time": exp}


def _recipient(c, rid: str):
    r = c.execute("SELECT * FROM recipients WHERE id = ?", (rid,)).fetchone()
    if not r:
        raise NotFound("Recipient not found.")
    return r


def rotate_token(rid: str, expires_in_days: Optional[int], actor: str) -> Dict[str, Any]:
    token, exp = _new_token(), _expiry(expires_in_days)
    c = _db()
    try:
        r = _recipient(c, rid)
        c.execute("UPDATE recipients SET token_hash = ?, token_prefix = ?, expires_at = ?, revoked_at = NULL WHERE id = ?", (_hash(token), token[:14], exp, rid))
        c.commit()
        view = _recipient_view(c, _recipient(c, rid))
    finally:
        c.close()
    _audit(actor, "SHARING_RECIPIENT_ROTATE", f"recipient:{r['name']}", {"expires_at": exp})
    return {"recipient": view, "token": token, "expiration_time": exp}


def set_recipient_shares(rid: str, shares: List[str], actor: str) -> Dict[str, Any]:
    c = _db()
    try:
        r = _recipient(c, rid)
        _set_shares(c, rid, shares or [])
        c.commit()
        view = _recipient_view(c, _recipient(c, rid))
    finally:
        c.close()
    _audit(actor, "SHARING_RECIPIENT_SHARES", f"recipient:{r['name']}", {"shares": shares or []})
    return view


def clean_cidrs(items: Any) -> List[str]:
    if isinstance(items, str):
        items = [x for x in re.split(r"[\s,;]+", items) if x]
    if not isinstance(items, list) or len(items) > 50:
        raise SharingError("At most 50 addresses or networks.")
    out: List[str] = []
    for x in items:
        try:
            net = ipaddress.ip_network(str(x).strip(), strict=False)
        except ValueError:
            raise SharingError(f"'{x}' is not an IP address or CIDR network.")
        if net.prefixlen == 0:
            raise SharingError("A /0 network would allow every address; leave the list empty for no restriction.")
        if str(net) not in out:
            out.append(str(net))
    return out


def set_recipient_ips(rid: str, cidrs: Any, actor: str) -> Dict[str, Any]:
    nets = clean_cidrs(cidrs)
    c = _db()
    try:
        r = _recipient(c, rid)
        c.execute("UPDATE recipients SET allowed_cidrs = ? WHERE id = ?", (json.dumps(nets) if nets else None, rid))
        c.commit()
        view = _recipient_view(c, _recipient(c, rid))
    finally:
        c.close()
    _audit(actor, "SHARING_RECIPIENT_IPS", f"recipient:{r['name']}", {"allowed_cidrs": nets})
    return view


def _ip_ok(row, client_ip) -> bool:
    nets = json.loads(row["allowed_cidrs"] or "[]")
    if not nets:
        return True
    if client_ip is None:
        return False
    return any(client_ip in ipaddress.ip_network(n) for n in nets)


def revoke_recipient(rid: str, actor: str) -> None:
    c = _db()
    try:
        r = _recipient(c, rid)
        c.execute("UPDATE recipients SET revoked_at = ? WHERE id = ?", (_now(), rid))
        c.commit()
    finally:
        c.close()
    _audit(actor, "SHARING_RECIPIENT_REVOKE", f"recipient:{r['name']}", {})


def delete_recipient(rid: str, actor: str) -> None:
    c = _db()
    try:
        r = _recipient(c, rid)
        c.execute("DELETE FROM recipients WHERE id = ?", (rid,))
        c.commit()
    finally:
        c.close()
    _audit(actor, "SHARING_RECIPIENT_DELETE", f"recipient:{r['name']}", {})


def recent_log(limit: int = 100) -> List[Dict[str, Any]]:
    c = _db()
    try:
        return [dict(r) for r in c.execute("SELECT at, recipient, action, share, tbl, detail FROM sharing_log ORDER BY id DESC LIMIT ?", (limit,))]
    finally:
        c.close()


def profile(endpoint: str, token: str, expiration: Optional[str]) -> Dict[str, Any]:
    p: Dict[str, Any] = {"shareCredentialsVersion": 1, "endpoint": endpoint, "bearerToken": token}
    if expiration:
        p["expirationTime"] = expiration.replace(" ", "T") + ".000Z"
    return p


# ---------------------------------------------------------------- recipient authentication

def authenticate(header: Optional[str], client_ip=None) -> Dict[str, Any]:
    tok = (header or "").strip()
    if tok.lower().startswith("bearer "):
        tok = tok[7:].strip()
    if not tok:
        raise SharingError("A bearer token is required.", "UNAUTHENTICATED", 401)
    h = _hash(tok)
    c = _db()
    try:
        r = c.execute("SELECT * FROM recipients WHERE token_hash = ?", (h,)).fetchone()
        if not r or not hmac.compare_digest(r["token_hash"], h):
            raise SharingError("The token is not valid.", "UNAUTHENTICATED", 401)
        if r["revoked_at"]:
            raise SharingError("The token was revoked.", "UNAUTHENTICATED", 401)
        if r["expires_at"] and r["expires_at"] < _now():
            raise SharingError("The token has expired.", "UNAUTHENTICATED", 401)
        if not _ip_ok(r, client_ip):
            _log(r["name"], "REFUSED", "", "", f"address {client_ip} is not allowed for this recipient")
            raise SharingError("This address is not allowed for this recipient.", "PERMISSION_DENIED", 403)
        c.execute("UPDATE recipients SET last_used_at = ? WHERE id = ?", (_now(), r["id"]))
        c.commit()
        return {"id": r["id"], "name": r["name"]}
    finally:
        c.close()


def _visible_share(c, rec: Dict[str, Any], name: str):
    s = c.execute("SELECT s.* FROM shares s JOIN recipient_shares rs ON rs.share_id = s.id WHERE rs.recipient_id = ? AND s.name = ?", (rec["id"], name)).fetchone()
    if not s:
        raise NotFound(f"Share '{name}' does not exist.")
    return s


# ---------------------------------------------------------------- protocol: listings

def _page(items: List[Dict[str, Any]], max_results: Optional[int], token: Optional[str]) -> Dict[str, Any]:
    try:
        start = int(base64.urlsafe_b64decode(token + "==").decode()) if token else 0
    except Exception:
        raise SharingError("Invalid pageToken.")
    size = max(1, min(int(max_results), 1000)) if max_results else 1000
    chunk = items[start:start + size]
    out: Dict[str, Any] = {"items": chunk}
    if start + size < len(items):
        out["nextPageToken"] = base64.urlsafe_b64encode(str(start + size).encode()).decode().rstrip("=")
    return out


def list_shares_for(rec, max_results=None, page_token=None):
    c = _db()
    try:
        rows = c.execute("SELECT s.id, s.name FROM shares s JOIN recipient_shares rs ON rs.share_id = s.id WHERE rs.recipient_id = ? ORDER BY s.name", (rec["id"],)).fetchall()
        return _page([{"name": r["name"], "id": r["id"]} for r in rows], max_results, page_token)
    finally:
        c.close()


def get_share_for(rec, share):
    c = _db()
    try:
        s = _visible_share(c, rec, share)
        return {"share": {"name": s["name"], "id": s["id"]}}
    finally:
        c.close()


def list_schemas_for(rec, share, max_results=None, page_token=None):
    c = _db()
    try:
        s = _visible_share(c, rec, share)
        rows = c.execute("SELECT DISTINCT schema_name FROM share_tables WHERE share_id = ? ORDER BY schema_name", (s["id"],)).fetchall()
        return _page([{"name": r[0], "share": share} for r in rows], max_results, page_token)
    finally:
        c.close()


def list_tables_for(rec, share, schema=None, max_results=None, page_token=None):
    c = _db()
    try:
        s = _visible_share(c, rec, share)
        q, args = "SELECT id, schema_name, table_name FROM share_tables WHERE share_id = ?", [s["id"]]
        if schema is not None:
            q += " AND schema_name = ?"; args.append(schema)
        rows = c.execute(q + " ORDER BY schema_name, table_name", args).fetchall()
        if schema is not None and not rows and not c.execute("SELECT 1 FROM share_tables WHERE share_id = ? AND schema_name = ?", (s["id"], schema)).fetchone():
            raise NotFound(f"Schema '{schema}' does not exist in the share.")
        return _page([{"name": r["table_name"], "schema": r["schema_name"], "share": share, "shareId": s["id"], "id": r["id"]} for r in rows], max_results, page_token)
    finally:
        c.close()


def _resolve(rec, share: str, schema: str, table: str) -> Dict[str, Any]:
    """(share table row, table root) for a recipient; 404 when not visible; 403-like governance problem raised as SharingError."""
    c = _db()
    try:
        s = _visible_share(c, rec, share)
        t = c.execute("SELECT * FROM share_tables WHERE share_id = ? AND schema_name = ? AND table_name = ?", (s["id"], schema, table)).fetchone()
        if not t:
            raise NotFound(f"Table '{schema}.{table}' does not exist in the share.")
        return dict(t)
    finally:
        c.close()


def _open(rec, share, schema, table, version=None, timestamp=None, need_history=False):
    from deltalake import DeltaTable
    t = _resolve(rec, share, schema, table)
    try:
        root = table_root(t["source"])
    except SharingError:
        raise NotFound("The shared table is no longer available.")
    dt = DeltaTable(root)
    problem = governance_problem(t["source"], dt)
    if problem:
        _log(rec["name"], "REFUSED", share, f"{schema}.{table}", problem)
        raise SharingError(f"This table is not shareable right now: {problem}", "PERMISSION_DENIED", 403)
    latest = dt.version()
    if need_history and not t["history"]:
        raise SharingError("History is not shared for this table (an administrator has to enable it).", "PERMISSION_DENIED", 403)
    if version is not None:
        try:
            dt = DeltaTable(root, version=int(version))
        except Exception:
            raise SharingError(f"Version {version} does not exist or is no longer available.")
    elif timestamp:
        try:
            ts = datetime.datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            dt.load_as_version(ts)
        except Exception:
            raise SharingError("The timestamp is invalid or earlier than the table's first version.")
    if dt.version() != latest and not t["history"]:
        raise SharingError("History is not shared for this table: only the latest version can be read.", "PERMISSION_DENIED", 403)
    return t, root, dt


def table_version(rec, share, schema, table, starting_timestamp=None) -> int:
    _, _, dt = _open(rec, share, schema, table, timestamp=starting_timestamp)
    _log(rec["name"], "VERSION", share, f"{schema}.{table}")
    return dt.version()


def _metadata_lines(dt, fmt: str = "parquet", size: Optional[int] = None, num_files: Optional[int] = None) -> List[Dict[str, Any]]:
    m, prot = dt.metadata(), dt.protocol()
    meta: Dict[str, Any] = {"id": m.id, "format": {"provider": "parquet"}, "schemaString": dt.schema().to_json(),
                            "partitionColumns": list(m.partition_columns), "configuration": dict(m.configuration or {})}
    if m.name:
        meta["name"] = m.name
    if m.description:
        meta["description"] = m.description
    if m.created_time:
        meta["createdTime"] = m.created_time
    if fmt == "delta":
        meta["format"] = {"provider": "parquet", "options": {}}
        extra: Dict[str, Any] = {"version": dt.version()}
        if size is not None:
            extra["size"] = size
        if num_files is not None:
            extra["numFiles"] = num_files
        return [{"protocol": {"deltaProtocol": {"minReaderVersion": prot.min_reader_version, "minWriterVersion": prot.min_writer_version}}},
                {"metaData": {"deltaMetadata": meta, **extra}}]
    return [{"protocol": {"minReaderVersion": 1}}, {"metaData": meta}]


def response_format(capabilities: Optional[str]) -> str:
    """`delta-sharing-capabilities: responseformat=delta,parquet;readerfeatures=...` -> parquet or delta.
    The formats a client lists are what it CAN read, not a preference: when parquet is among them the server answers in parquet (every table we share
    is readable that way); the Delta format is used when the client asks for it alone."""
    for part in (capabilities or "").split(";"):
        k, _, v = part.partition("=")
        if k.strip().lower() == "responseformat":
            fmts = [x.strip().lower() for x in v.split(",")]
            if "parquet" in fmts:
                return "parquet"
            if "delta" in fmts:
                return "delta"
    return "parquet"


def table_metadata(rec, share, schema, table, version=None, fmt: str = "parquet") -> Tuple[int, List[Dict[str, Any]]]:
    _, _, dt = _open(rec, share, schema, table, version=version)
    _log(rec["name"], "METADATA", share, f"{schema}.{table}")
    return dt.version(), _metadata_lines(dt, fmt)


# ---------------------------------------------------------------- signed file URLs

def _key() -> bytes:
    p = os.path.join(_meta_dir(), "sharing.key")
    with _LOCK:
        if not os.path.exists(p):
            fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as f:
                f.write(secrets.token_bytes(32))
        with open(p, "rb") as f:
            return f.read()


def sign_file(recipient_id: str, share_table_id: str, rel_path: str, ttl: int = URL_TTL) -> str:
    body = base64.urlsafe_b64encode(json.dumps({"r": recipient_id, "t": share_table_id, "f": rel_path, "e": int(time.time()) + ttl}, separators=(",", ":")).encode()).decode().rstrip("=")
    sig = base64.urlsafe_b64encode(hmac.new(_key(), body.encode(), hashlib.sha256).digest()).decode().rstrip("=")
    return f"{body}.{sig}"


def verify_file(token: str, client_ip=None) -> Dict[str, Any]:
    """The payload of a valid, unexpired link whose recipient and table are still in force; else SharingError (401/404)."""
    try:
        body, sig = token.split(".", 1)
        want = base64.urlsafe_b64encode(hmac.new(_key(), body.encode(), hashlib.sha256).digest()).decode().rstrip("=")
        if not hmac.compare_digest(sig, want):
            raise ValueError
        p = json.loads(base64.urlsafe_b64decode(body + "=="))
    except Exception:
        raise SharingError("The link is not valid.", "UNAUTHENTICATED", 401)
    if p.get("e", 0) < time.time():
        raise SharingError("The link has expired; ask for the table again.", "UNAUTHENTICATED", 401)
    c = _db()
    try:
        r = c.execute("SELECT * FROM recipients WHERE id = ?", (p["r"],)).fetchone()
        if not r or r["revoked_at"] or (r["expires_at"] and r["expires_at"] < _now()):
            raise SharingError("The recipient is no longer active.", "UNAUTHENTICATED", 401)
        if not _ip_ok(r, client_ip):
            raise SharingError("This address is not allowed for this recipient.", "PERMISSION_DENIED", 403)
        t = c.execute("SELECT st.* FROM share_tables st JOIN recipient_shares rs ON rs.share_id = st.share_id WHERE st.id = ? AND rs.recipient_id = ?", (p["t"], p["r"])).fetchone()
        if not t:
            raise NotFound("The table is no longer shared with this recipient.")
        p["source"], p["recipient"] = t["source"], r["name"]
        return p
    finally:
        c.close()


def file_path_for(p: Dict[str, Any]) -> str:
    """Absolute path of the file a valid link points at: inside the table directory, a Parquet file, never a symlink out."""
    root = table_root(p["source"])
    rel = p["f"]
    if not rel or rel.startswith("/") or ".." in rel.split("/") or "\\" in rel or not rel.endswith(".parquet") or rel.startswith("_delta_log"):
        raise NotFound("No such file.")
    full = os.path.realpath(os.path.join(root, rel))
    if os.path.commonpath([root, full]) != root or not os.path.isfile(full):
        raise NotFound("No such file.")
    problem = governance_problem(p["source"])
    if problem:
        raise SharingError(f"This table is not shareable right now: {problem}", "PERMISSION_DENIED", 403)
    return full


def add_bytes(recipient: str, n: int) -> None:
    try:
        c = _db()
        try:
            c.execute("UPDATE recipients SET bytes_served = COALESCE(bytes_served, 0) + ? WHERE name = ?", (n, recipient))
            c.commit()
        finally:
            c.close()
    except Exception:
        pass


_INT_TYPES = ("long", "integer", "short", "byte")


def _col_types(dt) -> Dict[str, str]:
    """Top-level primitive columns: name -> Delta type name (nested and complex columns are absent)."""
    return {f["name"]: f["type"] for f in json.loads(dt.schema().to_json())["fields"] if isinstance(f["type"], str)}


def _stat_value(ctype: str, v: Any) -> Any:
    """A min/max value in the form it is sent, or None when it must be left out (see the module docstring)."""
    if v is None or isinstance(v, bool):
        return None
    if ctype in _INT_TYPES and isinstance(v, int):
        return v
    if ctype == "string" and isinstance(v, str) and len(v) < 32:
        return v
    if ctype == "date":
        return v.isoformat() if hasattr(v, "isoformat") else (v if isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", v) else None)
    return None


def clean_stats(num_records, mins: Optional[dict], maxs: Optional[dict], nulls: Optional[dict], ctypes: Dict[str, str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"numRecords": num_records}
    if isinstance(nulls, dict):
        out["nullCount"] = {k: v for k, v in nulls.items() if k in ctypes and isinstance(v, int)}
    mn, mx = {}, {}
    for col, ct in ctypes.items():
        a, b = _stat_value(ct, (mins or {}).get(col)), _stat_value(ct, (maxs or {}).get(col))
        if a is not None and b is not None:                                    # both bounds or none
            mn[col], mx[col] = a, b
    if mn:
        out["minValues"], out["maxValues"] = mn, mx
    return out


def _stats_json(stats: Dict[str, Any]) -> str:
    return json.dumps(stats, default=str, separators=(",", ":"))


# ---- predicate hints (a hint may only skip a file that provably holds no matching row)

def _coerce(ctype: str, v: Any) -> Any:
    if ctype in _INT_TYPES:
        if isinstance(v, bool):
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, str) and re.match(r"^-?\d+$", v.strip()):
            return int(v)
        return None
    if ctype == "string":
        return v if isinstance(v, str) else None
    if ctype == "date":
        return v if isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", v) else None
    return None


_SQL_TERM = re.compile(r"^\(?\s*[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s*(=|==|<>|!=|<=|>=|<|>)\s*(.+?)\s*\)?$")
_SQL_NULL = re.compile(r"^\(?\s*[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s+is\s+(not\s+)?null\s*\)?$", re.I)
_OPS = {"=": "eq", "==": "eq", "<>": "ne", "!=": "ne", "<": "lt", "<=": "le", ">": "gt", ">=": "ge"}
_JSON_OPS = {"equal": "eq", "lessThan": "lt", "lessThanOrEqual": "le", "greaterThan": "gt", "greaterThanOrEqual": "ge"}
_FLIP = {"lt": "gt", "le": "ge", "gt": "lt", "ge": "le", "eq": "eq", "ne": "ne"}


def _sql_literal(text: str) -> Any:
    text = text.strip()
    m = re.match(r"^'((?:[^']|'')*)'$", text)
    if m:
        return m.group(1).replace("''", "'")
    if re.match(r"^-?\d+$", text):
        return int(text)
    return None


def parse_sql_hints(hints: Any) -> List[Optional[tuple]]:
    """Each hint string -> a conjunction of ('cmp', op, col, literal) / ('null', col, is_null) terms; None for a term that is not understood."""
    out: List[Optional[tuple]] = []
    for h in hints if isinstance(hints, list) else []:
        for term in re.split(r"\s+and\s+", str(h), flags=re.I):
            m = _SQL_NULL.match(term.strip())
            if m:
                out.append(("null", m.group(1), not m.group(2)))
                continue
            m = _SQL_TERM.match(term.strip())
            lit = _sql_literal(m.group(3)) if m else None
            out.append(("cmp", _OPS[m.group(2)], m.group(1), lit) if m and lit is not None else None)
    return out


def parse_json_hint(text: Any) -> Optional[tuple]:
    """The jsonPredicateHints tree -> ('and'|'or', [..]) / ('cmp', ..) / ('null', ..) / None (not understood)."""
    try:
        node = json.loads(text) if isinstance(text, str) else text
    except ValueError:
        return None
    def conv(n) -> Optional[tuple]:
        if not isinstance(n, dict):
            return None
        op, kids = n.get("op"), n.get("children") or []
        if op in ("and", "or"):
            parts = [conv(k) for k in kids]
            return (op, parts)
        if op in _JSON_OPS and len(kids) == 2:
            a, b = kids
            if a.get("op") == "column" and b.get("op") == "literal":
                return ("cmp", _JSON_OPS[op], a.get("name"), b.get("value"))
            if a.get("op") == "literal" and b.get("op") == "column":
                return ("cmp", _FLIP[_JSON_OPS[op]], b.get("name"), a.get("value"))
            return None
        if op == "isNull" and len(kids) == 1 and kids[0].get("op") == "column":
            return ("null", kids[0].get("name"), True)
        return None
    return conv(node)


def _may_match(node: Optional[tuple], f: Dict[str, Any], ctypes: Dict[str, str], parts: List[str]) -> bool:
    """False only when file `f` provably holds no row matching `node`."""
    if node is None:
        return True
    kind = node[0]
    if kind == "and":
        return all(_may_match(n, f, ctypes, parts) for n in node[1])
    if kind == "or":
        return any(_may_match(n, f, ctypes, parts) for n in node[1]) if node[1] else True
    col = node[2] if kind == "cmp" else node[1]
    ct = ctypes.get(col)
    if ct is None:
        return True
    if col in parts:                                           # exact: one value for the whole file
        raw = f["partitionValues"].get(col)
        if kind == "null":
            return (raw is None) == node[2]
        if raw is None:
            return False                                       # NULL never satisfies a comparison
        val, lit = _coerce(ct, raw), _coerce(ct, node[3])
        if val is None or lit is None:
            return True
        return {"eq": val == lit, "ne": val != lit, "lt": val < lit, "le": val <= lit, "gt": val > lit, "ge": val >= lit}[node[1]]
    st = f.get("_stats") or {}
    if kind == "null":
        nc, n = (st.get("nullCount") or {}).get(col), st.get("numRecords")
        if node[2]:
            return not (nc == 0)
        return not (nc is not None and n is not None and nc == n)
    lo, hi = _coerce(ct, (st.get("minValues") or {}).get(col)), _coerce(ct, (st.get("maxValues") or {}).get(col))
    lit = _coerce(ct, node[3])
    if lo is None or hi is None or lit is None:
        return True
    op = node[1]
    if op == "eq":
        return lo <= lit <= hi
    if op == "ne":
        return not (lo == hi == lit)
    if op == "lt":
        return lo < lit
    if op == "le":
        return lo <= lit
    if op == "gt":
        return hi > lit
    return hi >= lit


def _partition_only(node: Optional[tuple], parts: List[str]) -> bool:
    if node is None:
        return False
    if node[0] in ("and", "or"):
        return all(_partition_only(n, parts) for n in node[1])
    return (node[2] if node[0] == "cmp" else node[1]) in parts


def prune_files(files: List[Dict[str, Any]], body: Dict[str, Any], ctypes: Dict[str, str], parts: List[str]) -> List[Dict[str, Any]]:
    """Applies predicateHints / jsonPredicateHints (file skipping) and then limitHint. The limit is applied only when every hint was
    understood and refers to partition columns only (so the skipped files are exactly those without matching rows)."""
    sql = parse_sql_hints(body.get("predicateHints"))
    js = parse_json_hint(body.get("jsonPredicateHints")) if body.get("jsonPredicateHints") else None
    nodes = [n for n in sql] + ([js] if body.get("jsonPredicateHints") else [])
    kept = [f for f in files if all(_may_match(n, f, ctypes, parts) for n in nodes)]
    exact = all(_partition_only(n, parts) for n in nodes)
    limit = body.get("limitHint")
    if isinstance(limit, int) and not isinstance(limit, bool) and limit > 0 and exact and all(f.get("_stats", {}).get("numRecords") is not None for f in kept):
        out, total = [], 0
        for f in kept:
            if total >= limit:
                break
            out.append(f)
            total += f["_stats"]["numRecords"]
        kept = out
    return kept


# ---- files of a snapshot and of a change range

def _iso_or_str(v: Any) -> Any:
    return None if v is None else (v.isoformat() if hasattr(v, "isoformat") else str(v))


def snapshot_files(dt) -> List[Dict[str, Any]]:
    import pyarrow as pa
    ctypes = _col_types(dt)
    out = []
    for r in pa.table(dt.get_add_actions(flatten=False)).to_pylist():
        out.append({"path": r["path"], "size": r["size_bytes"], "modificationTime": r["modification_time"],
                    "partitionValues": {k: _iso_or_str(v) for k, v in (r.get("partition") or {}).items()},
                    "_stats": clean_stats(r.get("num_records"), r.get("min"), r.get("max"), r.get("null_count"), ctypes)})
    return out


def _file_line(rec, t, f: Dict[str, Any], base_url: str, fmt: str, version: int, kind: str = "add", timestamp: Optional[int] = None) -> Dict[str, Any]:
    """One file entry in the requested response format. kind: add | remove | cdf."""
    url = f"{base_url}/files/{sign_file(rec['id'], t['id'], f['path'])}"
    fid = hashlib.md5(f["path"].encode()).hexdigest()
    expires = int((time.time() + URL_TTL) * 1000)
    stats = _stats_json(f["_stats"]) if f.get("_stats") is not None else None
    if fmt == "delta":
        if kind == "add":
            act = {"add": {"path": url, "partitionValues": f["partitionValues"], "size": f["size"], "modificationTime": f.get("modificationTime") or 0, "dataChange": True,
                           **({"stats": stats} if stats else {})}}
        elif kind == "remove":
            act = {"remove": {"path": url, "partitionValues": f["partitionValues"], "size": f["size"], "deletionTimestamp": timestamp or 0, "dataChange": True}}
        else:
            act = {"cdc": {"path": url, "partitionValues": f["partitionValues"], "size": f["size"], "dataChange": False}}
        return {"file": {"id": fid, "version": version, "timestamp": timestamp or f.get("modificationTime") or 0, "expirationTimestamp": expires, "deltaSingleAction": act}}
    body = {"url": url, "id": fid, "partitionValues": f["partitionValues"], "size": f["size"], "version": version, "expirationTimestamp": expires}
    if timestamp is not None:
        body["timestamp"] = timestamp
    if kind == "add" and stats:
        body["stats"] = stats
    return {"file" if fmt == "parquet" and kind == "add" and timestamp is None else kind if kind != "cdf" else "cdf": body}


def query_table(rec, share, schema, table, base_url: str, body: Dict[str, Any], fmt: str = "parquet") -> Tuple[int, List[Dict[str, Any]]]:
    for k in ("startingVersion", "endingVersion"):
        if body.get(k) is not None:
            raise SharingError("Version ranges are not part of a table query on this server: use the /changes endpoint.")
    t, root, dt = _open(rec, share, schema, table, version=body.get("version"), timestamp=body.get("timestamp"))
    files = snapshot_files(dt)
    if len(files) > MAX_FILES:
        raise SharingError(f"The table has {len(files)} files (limit {MAX_FILES}); compact it (OPTIMIZE) before sharing.")
    total = len(files)
    files = prune_files(files, body, _col_types(dt), list(dt.metadata().partition_columns))
    ver = dt.version()
    lines = _metadata_lines(dt, fmt, size=sum(f["size"] for f in files), num_files=len(files))
    for f in files:
        lines.append(_file_line(rec, t, f, base_url, fmt, ver))
    _log(rec["name"], "QUERY", share, f"{schema}.{table}", f"version {ver}, {len(files)} of {total} files, {fmt} format")
    return ver, lines


# ---- change data feed

def _read_commit(root: str, version: int) -> List[Dict[str, Any]]:
    p = os.path.join(root, "_delta_log", f"{version:020d}.json")
    if not os.path.isfile(p):
        raise SharingError(f"Version {version} of the table log is no longer available (cleaned up).")
    out = []
    with open(p, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _commit_ts(actions: List[Dict[str, Any]], root: str, version: int) -> int:
    for a in actions:
        if "commitInfo" in a and a["commitInfo"].get("timestamp"):
            return int(a["commitInfo"]["timestamp"])
    return int(os.path.getmtime(os.path.join(root, "_delta_log", f"{version:020d}.json")) * 1000)


def _resolve_version_range(root: str, latest: int, q: Dict[str, Any]) -> Tuple[int, int]:
    def num(name):
        v = q.get(name)
        if v in (None, ""):
            return None
        try:
            return int(v)
        except ValueError:
            raise SharingError(f"{name} must be a whole number.")
    def ts(name):
        v = q.get(name)
        if not v:
            return None
        try:
            return int(datetime.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
        except ValueError:
            raise SharingError(f"{name} must be an ISO-8601 timestamp.")
    sv, ev, st, et = num("startingVersion"), num("endingVersion"), ts("startingTimestamp"), ts("endingTimestamp")
    if sv is None and st is None:
        raise SharingError("startingVersion or startingTimestamp is required.")
    times = None
    if st is not None or et is not None:
        if latest > 10000:
            raise SharingError("The table has too many versions to resolve a timestamp; use versions.")
        times = [_commit_ts(_read_commit(root, v), root, v) for v in range(0, latest + 1)]
    if sv is None:
        sv = next((v for v, t in enumerate(times) if t >= st), None)
        if sv is None:
            raise SharingError("startingTimestamp is after the latest commit.")
    if ev is None:
        ev = max((v for v, t in enumerate(times) if t <= et), default=None) if et is not None else latest
        if ev is None:
            raise SharingError("endingTimestamp is before the first commit.")
    if sv < 0 or sv > latest:
        raise SharingError(f"startingVersion {sv} is outside the table's versions (latest {latest}).")
    if ev < sv or ev > latest:
        raise SharingError(f"endingVersion {ev} must be between startingVersion and the latest version ({latest}).")
    if ev - sv + 1 > MAX_CHANGE_VERSIONS:
        raise SharingError(f"At most {MAX_CHANGE_VERSIONS} versions per request; ask for a smaller range.")
    return sv, ev


def table_changes(rec, share, schema, table, base_url: str, q: Dict[str, Any], fmt: str = "parquet") -> Tuple[int, List[Dict[str, Any]]]:
    """The change feed of a table shared with history, from the Delta log."""
    t, root, dt = _open(rec, share, schema, table, need_history=True)
    latest = dt.version()
    sv, ev = _resolve_version_range(root, latest, q)
    ctypes = _col_types(dt)
    lines = _metadata_lines(dt, fmt)
    nfiles = 0
    for v in range(sv, ev + 1):
        acts = _read_commit(root, v)
        ts = _commit_ts(acts, root, v)
        def entry(a: Dict[str, Any], with_stats: bool) -> Dict[str, Any]:
            st = None
            if with_stats:
                try:
                    raw = json.loads(a.get("stats") or "{}")
                except ValueError:
                    raw = {}
                st = clean_stats(raw.get("numRecords"), raw.get("minValues"), raw.get("maxValues"), raw.get("nullCount"), ctypes)
            return {"path": a["path"], "size": a.get("size") or 0, "modificationTime": a.get("modificationTime"),
                    "partitionValues": {k: (None if val is None else str(val)) for k, val in (a.get("partitionValues") or {}).items()}, "_stats": st}
        cdc = [a["cdc"] for a in acts if "cdc" in a]
        if cdc:                                                 # a commit with change-data files: only those describe it
            for a in cdc:
                lines.append(_change_line(rec, t, entry(a, False), base_url, fmt, v, "cdf", ts))
                nfiles += 1
        else:
            for a in acts:
                if "remove" in a and a["remove"].get("dataChange", True):
                    lines.append(_change_line(rec, t, entry(a["remove"], False), base_url, fmt, v, "remove", ts))
                    nfiles += 1
                elif "add" in a and a["add"].get("dataChange", True):
                    lines.append(_change_line(rec, t, entry(a["add"], True), base_url, fmt, v, "add", ts))
                    nfiles += 1
        if nfiles > MAX_FILES:
            raise SharingError(f"The range holds more than {MAX_FILES} files; ask for a smaller range.")
    _log(rec["name"], "CHANGES", share, f"{schema}.{table}", f"versions {sv}-{ev}, {nfiles} files, {fmt} format")
    return latest, lines


def _change_line(rec, t, f, base_url, fmt, version, kind, ts):
    line = _file_line(rec, t, f, base_url, fmt, version, kind, ts)
    if fmt == "parquet":                                        # the parquet format names the action itself: {"add"|"remove"|"cdf": {...}}
        (body,) = line.values()
        return {kind: body}
    return line


# ---------------------------------------------------------------- HTTP: the protocol

import asyncio
from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse, Response

router = APIRouter(prefix="/delta-sharing")


def _err(exc: SharingError) -> JSONResponse:
    headers = {"WWW-Authenticate": 'Bearer realm="delta-sharing"'} if exc.status == 401 else None
    return JSONResponse(status_code=exc.status, content={"errorCode": exc.code, "message": str(exc)}, headers=headers)


def endpoint_for(request: Request) -> str:
    env = os.getenv("DELTA_SHARING_ENDPOINT", "").strip().rstrip("/")
    return env or str(request.base_url).rstrip("/") + "/delta-sharing"


def _ndjson(version: Optional[int], lines: List[Dict[str, Any]], fmt: str = "parquet") -> Response:
    headers = {"delta-sharing-capabilities": f"responseformat={fmt}"}
    if version is not None:
        headers["Delta-Table-Version"] = str(version)
    return Response(content="\n".join(json.dumps(x, separators=(",", ":")) for x in lines) + "\n", media_type="application/x-ndjson; charset=utf-8", headers=headers)


def _client_ip(request: Request):
    """The caller's address, resolved like the global IP allowlist does (X-Forwarded-For only from trusted proxies)."""
    try:
        from web import ip_allowlist
        return ip_allowlist.resolve_client(request.client.host if request.client else None, dict(request.headers))["ip"]
    except Exception:
        return None


def _fmt(request: Request) -> str:
    return response_format(request.headers.get("delta-sharing-capabilities"))


async def _guarded(request: Request, fn):
    if not enabled():
        return JSONResponse(status_code=404, content={"errorCode": "RESOURCE_DOES_NOT_EXIST", "message": "Delta Sharing is switched off."})
    try:
        rec = await asyncio.to_thread(authenticate, request.headers.get("authorization"), _client_ip(request))
        return await asyncio.to_thread(fn, rec)
    except SharingError as exc:
        return _err(exc)
    except Exception as exc:
        logger.error(f"delta sharing request failed: {exc}", exc_info=True)
        return JSONResponse(status_code=500, content={"errorCode": "INTERNAL_ERROR", "message": "The request failed."})


def _q(request: Request):
    mr = request.query_params.get("maxResults")
    try:
        return (int(mr) if mr else None), request.query_params.get("pageToken")
    except ValueError:
        raise SharingError("maxResults must be a number.")


@router.get("/shares")
async def api_shares(request: Request):
    return await _guarded(request, lambda rec: list_shares_for(rec, *_q(request)))


@router.get("/shares/{share}")
async def api_share(request: Request, share: str):
    return await _guarded(request, lambda rec: get_share_for(rec, share))


@router.get("/shares/{share}/schemas")
async def api_schemas(request: Request, share: str):
    return await _guarded(request, lambda rec: list_schemas_for(rec, share, *_q(request)))


@router.get("/shares/{share}/schemas/{schema}/tables")
async def api_tables(request: Request, share: str, schema: str):
    return await _guarded(request, lambda rec: list_tables_for(rec, share, schema, *_q(request)))


@router.get("/shares/{share}/all-tables")
async def api_all_tables(request: Request, share: str):
    return await _guarded(request, lambda rec: list_tables_for(rec, share, None, *_q(request)))


@router.get("/shares/{share}/schemas/{schema}/tables/{table}/version")
async def api_version(request: Request, share: str, schema: str, table: str):
    def run(rec):
        v = table_version(rec, share, schema, table, request.query_params.get("startingTimestamp"))
        return Response(content="", media_type="application/json", headers={"Delta-Table-Version": str(v)})
    return await _guarded(request, run)


@router.get("/shares/{share}/schemas/{schema}/tables/{table}/metadata")
async def api_metadata(request: Request, share: str, schema: str, table: str):
    fmt = _fmt(request)
    return await _guarded(request, lambda rec: _ndjson(*table_metadata(rec, share, schema, table, fmt=fmt), fmt=fmt))


@router.post("/shares/{share}/schemas/{schema}/tables/{table}/query")
async def api_query(request: Request, share: str, schema: str, table: str):
    raw = await request.body()
    try:
        body = json.loads(raw.decode() or "{}")
        if not isinstance(body, dict):
            raise ValueError
    except ValueError:
        return _err(SharingError("The body must be a JSON object."))
    base, fmt = endpoint_for(request), _fmt(request)
    return await _guarded(request, lambda rec: _ndjson(*query_table(rec, share, schema, table, base, body, fmt), fmt=fmt))


@router.get("/shares/{share}/schemas/{schema}/tables/{table}/changes")
async def api_changes(request: Request, share: str, schema: str, table: str):
    base, fmt, q = endpoint_for(request), _fmt(request), dict(request.query_params)
    def run(rec):
        ver, lines = table_changes(rec, share, schema, table, base, q, fmt)
        return _ndjson(ver, lines, fmt)
    return await _guarded(request, run)


@router.api_route("/files/{token}", methods=["GET", "HEAD"])
async def api_file(request: Request, token: str):
    """The pre-signed file link: the signature is the credential (no session, no bearer)."""
    if not enabled():
        return JSONResponse(status_code=404, content={"errorCode": "RESOURCE_DOES_NOT_EXIST", "message": "Delta Sharing is switched off."})
    try:
        def check():
            p = verify_file(token, _client_ip(request))
            return p, file_path_for(p)
        p, path = await asyncio.to_thread(check)
    except SharingError as exc:
        return _err(exc)
    except Exception as exc:
        logger.error(f"delta sharing file failed: {exc}", exc_info=True)
        return JSONResponse(status_code=500, content={"errorCode": "INTERNAL_ERROR", "message": "The request failed."})
    if request.method == "GET" and not request.headers.get("range"):
        add_bytes(p["recipient"], os.path.getsize(path))
    return FileResponse(path, media_type="application/octet-stream")
