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
Not implemented  change data feed (/changes), Delta-format responses (`responseformat=delta`), predicate / limit hints (they are hints; all files of
         the version are returned), per-recipient IP rules (the global IP allowlist applies to /delta-sharing too).

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
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("localspark.sharing")

URL_TTL = int(os.getenv("DELTA_SHARING_URL_TTL", "900"))
MAX_FILES = int(os.getenv("DELTA_SHARING_MAX_FILES", "50000"))
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
            tabs = [dict(t) for t in c.execute("SELECT schema_name, table_name, source, added_by, added_at FROM share_tables WHERE share_id = ? ORDER BY schema_name, table_name", (s["id"],))]
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


def add_table(share: str, source: str, actor: str, schema_alias: Optional[str] = None, table_alias: Optional[str] = None) -> Dict[str, Any]:
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
            c.execute("INSERT INTO share_tables (id, share_id, schema_name, table_name, source, added_by, added_at) VALUES (?,?,?,?,?,?,?)",
                      (uuid.uuid4().hex, s["id"], s_alias, t_alias, source, actor, _now()))
            c.commit()
        except sqlite3.IntegrityError:
            raise SharingError(f"{s_alias}.{t_alias} is already in the share.", "RESOURCE_ALREADY_EXISTS", 409)
    finally:
        c.close()
    _audit(actor, "SHARING_TABLE_ADD", f"share:{share}", {"source": source, "as": f"{s_alias}.{t_alias}"})
    return {"share": share, "schema": s_alias, "table": t_alias, "source": source}


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
            "created_at": r["created_at"], "revoked_at": r["revoked_at"], "last_used_at": r["last_used_at"], "bytes_served": r["bytes_served"] or 0, "shares": shares, "state": state}


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

def authenticate(header: Optional[str]) -> Dict[str, Any]:
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


def _open(rec, share, schema, table, version=None, timestamp=None):
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
    if version is not None:
        dt = DeltaTable(root, version=int(version))
    elif timestamp:
        try:
            ts = datetime.datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            dt.load_as_version(ts)
        except Exception:
            raise SharingError("The timestamp is invalid or earlier than the table's first version.")
    return t, root, dt


def table_version(rec, share, schema, table, starting_timestamp=None) -> int:
    _, _, dt = _open(rec, share, schema, table, timestamp=starting_timestamp)
    _log(rec["name"], "VERSION", share, f"{schema}.{table}")
    return dt.version()


def _metadata_lines(dt) -> List[Dict[str, Any]]:
    m = dt.metadata()
    meta: Dict[str, Any] = {"id": m.id, "format": {"provider": "parquet"}, "schemaString": dt.schema().to_json(),
                            "partitionColumns": list(m.partition_columns), "configuration": dict(m.configuration or {})}
    if m.name:
        meta["name"] = m.name
    if m.description:
        meta["description"] = m.description
    if m.created_time:
        meta["createdTime"] = m.created_time
    return [{"protocol": {"minReaderVersion": 1}}, {"metaData": meta}]


def table_metadata(rec, share, schema, table, version=None) -> Tuple[int, List[Dict[str, Any]]]:
    _, _, dt = _open(rec, share, schema, table, version=version)
    _log(rec["name"], "METADATA", share, f"{schema}.{table}")
    return dt.version(), _metadata_lines(dt)


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


def verify_file(token: str) -> Dict[str, Any]:
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
    if not rel or rel.startswith("/") or ".." in rel.split("/") or "\\" in rel or not rel.endswith(".parquet"):
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


def _stats(row: Dict[str, Any]) -> str:
    s: Dict[str, Any] = {"numRecords": row.get("num_records")}
    nc = row.get("null_count")
    if isinstance(nc, dict):
        s["nullCount"] = nc
    return json.dumps(s, default=str, separators=(",", ":"))


def query_table(rec, share, schema, table, base_url: str, body: Dict[str, Any]) -> Tuple[int, List[Dict[str, Any]]]:
    import pyarrow as pa
    for k in ("startingVersion", "endingVersion"):
        if body.get(k) is not None:
            raise SharingError("Change data feed / version ranges are not supported by this server.")
    t, root, dt = _open(rec, share, schema, table, version=body.get("version"), timestamp=body.get("timestamp"))
    rows = pa.table(dt.get_add_actions(flatten=False)).to_pylist()
    if len(rows) > MAX_FILES:
        raise SharingError(f"The table has {len(rows)} files (limit {MAX_FILES}); compact it (OPTIMIZE) before sharing.")
    lines = _metadata_lines(dt)
    ver, mtime = dt.version(), None
    for r in rows:
        pv = {k: (None if v is None else (v.isoformat() if hasattr(v, "isoformat") else str(v))) for k, v in (r.get("partition") or {}).items()}
        lines.append({"file": {"url": f"{base_url}/files/{sign_file(rec['id'], t['id'], r['path'])}", "id": hashlib.md5(r["path"].encode()).hexdigest(),
                               "partitionValues": pv, "size": r["size_bytes"], "stats": _stats(r), "version": ver}})
    _log(rec["name"], "QUERY", share, f"{schema}.{table}", f"version {ver}, {len(rows)} files")
    return ver, lines


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


def _ndjson(version: int, lines: List[Dict[str, Any]]) -> Response:
    return Response(content="\n".join(json.dumps(x, separators=(",", ":")) for x in lines) + "\n", media_type="application/x-ndjson; charset=utf-8",
                    headers={"Delta-Table-Version": str(version)})


async def _guarded(request: Request, fn):
    if not enabled():
        return JSONResponse(status_code=404, content={"errorCode": "RESOURCE_DOES_NOT_EXIST", "message": "Delta Sharing is switched off."})
    try:
        rec = await asyncio.to_thread(authenticate, request.headers.get("authorization"))
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
    return await _guarded(request, lambda rec: _ndjson(*table_metadata(rec, share, schema, table)))


@router.post("/shares/{share}/schemas/{schema}/tables/{table}/query")
async def api_query(request: Request, share: str, schema: str, table: str):
    raw = await request.body()
    try:
        body = json.loads(raw.decode() or "{}")
        if not isinstance(body, dict):
            raise ValueError
    except ValueError:
        return _err(SharingError("The body must be a JSON object."))
    base = endpoint_for(request)
    return await _guarded(request, lambda rec: _ndjson(*query_table(rec, share, schema, table, base, body)))


@router.get("/shares/{share}/schemas/{schema}/tables/{table}/changes")
async def api_changes(request: Request, share: str, schema: str, table: str):
    return await _guarded(request, lambda rec: (_ for _ in ()).throw(SharingError("Change data feed is not supported by this server.")))


@router.api_route("/files/{token}", methods=["GET", "HEAD"])
async def api_file(request: Request, token: str):
    """The pre-signed file link: the signature is the credential (no session, no bearer)."""
    if not enabled():
        return JSONResponse(status_code=404, content={"errorCode": "RESOURCE_DOES_NOT_EXIST", "message": "Delta Sharing is switched off."})
    try:
        def check():
            p = verify_file(token)
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
