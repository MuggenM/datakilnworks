"""Connections: named, reusable definitions of an external source (an HTTP(S) server / REST API, or an SFTP server) that Auto-Loader
pipelines refer to as `conn://<name>/<path>`.

Why a separate object instead of a URL with a password in the pipeline: the secret (token, password, private key) is stored once,
encrypted with the install's key (`secrets_store.encrypt_json`), and never returned by any API or written to a log; pipelines carry only
the connection's name and a *relative* path. Only administrators create or change connections; the pipeline's path can only ever
extend the connection's base URL / directory (see `autoloader_conn.safe_path`), so a pipeline author cannot point a stored credential at
another host.

Types
  http  base_url (http/https), auth none | bearer | basic | header | oauth2, timeout_seconds. Used for a file download or a REST/JSON API. `oauth2` is the
        client-credentials grant (web/oauth_client.py): token_url, client_id, scope, client_auth (basic | body), extra_params; the client_secret is the secret.
  kafka bootstrap_servers, security_protocol (PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL), SASL mechanism/user, optional CA certificate;
        the password is the secret. Read by streams (`web/streaming.py`), not by Auto-Loader pipelines.
  sftp  host, port, username, auth password | key, and `host_key_sha256`: the server's host key fingerprint, REQUIRED. It is
        checked on every connect (a changed key is refused, never silently trusted); the Test action shows the fingerprint to pin.
"""
import datetime
import json
import logging
import os
import re
import sqlite3
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from web import secrets_store

logger = logging.getLogger("localspark.connections")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,39}$")
TYPES = ("http", "sftp", "kafka")
KAFKA_PROTOCOLS = ("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")
KAFKA_MECHANISMS = ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
HTTP_AUTH = ("none", "bearer", "basic", "header", "oauth2")
SFTP_AUTH = ("password", "key")
FINGERPRINT_RE = re.compile(r"^SHA256:[A-Za-z0-9+/]{43}$")
# Secret fields per type: accepted on write, stored encrypted, never read back.
SECRET_FIELDS = {"http": ("token", "password", "header_value", "client_secret"), "sftp": ("password", "private_key", "passphrase"), "kafka": ("password", "registry_password")}


def _db_path() -> str:
    return os.path.join(os.getenv("WAREHOUSE_DIR", WAREHOUSE_DIR), ".metadata", "connections.db")


def _db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(_db_path()), exist_ok=True)
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS connections (
        id TEXT PRIMARY KEY, name TEXT UNIQUE NOT NULL, type TEXT NOT NULL, description TEXT, config_json TEXT NOT NULL,
        secret_enc TEXT, created_by TEXT, created_at TEXT, updated_by TEXT, updated_at TEXT)""")
    return conn


class ConnectionError_(ValueError):
    """Invalid connection definition or unknown connection (the message is safe to show)."""


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _audit(actor: str, action: str, name: str, detail: Dict[str, Any]) -> None:
    try:
        from web.governance import store
        store.init_governance_db()
        c = store.get_db()
        try:
            store.write_audit(c, actor, action, f"connection:{name}", detail)     # never the secret
            c.commit()
        finally:
            c.close()
    except Exception as exc:
        logger.warning(f"could not audit {action}: {exc}")


# ---------------------------------------------------------------- validation

def _validate_config(kind: str, cfg: Dict[str, Any], secret: Dict[str, Any], for_test: bool = False) -> Dict[str, Any]:
    """Returns the cleaned non-secret config; raises ConnectionError_ with a user-safe message. `for_test` lets an SFTP definition
    omit the fingerprint (Test is how you learn it)."""
    if kind == "http":
        url = str(cfg.get("base_url") or "").strip()
        u = urlparse(url)
        if u.scheme not in ("http", "https") or not u.hostname:
            raise ConnectionError_("The base URL must start with http:// or https://.")
        if u.username or u.password or u.query or u.fragment:
            raise ConnectionError_("The base URL must not contain credentials, a query string or a fragment.")
        auth = cfg.get("auth") or "none"
        if auth not in HTTP_AUTH:
            raise ConnectionError_(f"Authentication must be one of {', '.join(HTTP_AUTH)}.")
        insecure = bool(cfg.get("allow_insecure"))
        if auth != "none" and u.scheme == "http" and not insecure:
            raise ConnectionError_("Credentials over plain http:// are refused. Use https://, or tick 'allow insecure' for a trusted internal server.")
        need = {"bearer": "token", "basic": "password", "header": "header_value", "oauth2": "client_secret"}.get(auth)
        if need and not secret.get(need):
            raise ConnectionError_(f"The {need.replace('_', ' ')} is required for '{auth}' authentication.")
        if auth == "basic" and not str(cfg.get("username") or "").strip():
            raise ConnectionError_("A user name is required for basic authentication.")
        header = str(cfg.get("header_name") or "").strip()
        if auth == "header" and not re.fullmatch(r"[A-Za-z0-9-]{1,64}", header):
            raise ConnectionError_("A header name (letters, digits, dashes) is required for header authentication.")
        oauth: Dict[str, Any] = {}
        if auth == "oauth2":
            tu = urlparse(str(cfg.get("token_url") or "").strip())
            if tu.scheme not in ("http", "https") or not tu.hostname or tu.username or tu.password or tu.fragment:
                raise ConnectionError_("The token URL must be an http(s) URL without credentials or a fragment (e.g. https://login.example.com/oauth2/token).")
            if tu.scheme == "http" and not insecure:
                raise ConnectionError_("The client secret would be sent to the token endpoint over plain http://. Use https://, or tick 'allow insecure' for a trusted internal server.")
            cid = str(cfg.get("client_id") or "").strip()
            if not cid or len(cid) > 300 or re.search(r"[\s\x00-\x1f]", cid):
                raise ConnectionError_("The client ID is required (no spaces).")
            scope = str(cfg.get("scope") or "").strip()
            if len(scope) > 500 or re.search(r"[\x00-\x1f\"\\]", scope):
                raise ConnectionError_("The scope must be a plain space-separated list (max. 500 characters).")
            method = cfg.get("client_auth") or "basic"
            if method not in ("basic", "body"):
                raise ConnectionError_("Client authentication must be 'basic' (HTTP Basic header) or 'body' (client_id / client_secret in the request).")
            extra = cfg.get("extra_params") or {}
            if not isinstance(extra, dict) or len(extra) > 10:
                raise ConnectionError_("Extra token parameters are up to 10 name/value pairs.")
            clean_extra = {}
            for k, v in extra.items():
                k, v = str(k).strip(), str(v).strip()
                if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", k) or k in ("grant_type", "client_id", "client_secret", "scope") or len(v) > 500 or re.search(r"[\x00-\x1f]", v):
                    raise ConnectionError_(f"'{k[:30]}' is not an allowed extra token parameter (grant_type, client_id, client_secret and scope have their own fields).")
                clean_extra[k] = v
            oauth = {"token_url": str(cfg["token_url"]).strip(), "client_id": cid, "scope": scope, "client_auth": method, "extra_params": clean_extra}
        try:
            timeout = max(1, min(int(cfg.get("timeout_seconds") or 30), 120))
        except (TypeError, ValueError):
            raise ConnectionError_("The timeout must be a number of seconds.")
        return {"base_url": url.rstrip("/") + "/", "auth": auth, "username": str(cfg.get("username") or "").strip(),
                "header_name": header, "allow_insecure": insecure, "timeout_seconds": timeout, **oauth}
    if kind == "sftp":
        host = str(cfg.get("host") or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,253}", host):
            raise ConnectionError_("A host name or address is required.")
        try:
            port = int(cfg.get("port") or 22)
        except (TypeError, ValueError):
            raise ConnectionError_("The port must be a number.")
        if not 1 <= port <= 65535:
            raise ConnectionError_("The port must be between 1 and 65535.")
        user = str(cfg.get("username") or "").strip()
        if not user or len(user) > 128 or re.search(r"\s", user):
            raise ConnectionError_("A user name is required.")
        auth = cfg.get("auth") or "password"
        if auth not in SFTP_AUTH:
            raise ConnectionError_("Authentication must be 'password' or 'key'.")
        if auth == "password" and not secret.get("password"):
            raise ConnectionError_("The password is required.")
        if auth == "key" and not str(secret.get("private_key") or "").strip():
            raise ConnectionError_("The private key is required.")
        fp = str(cfg.get("host_key_sha256") or "").strip()
        if for_test and not fp:
            return {"host": host, "port": port, "username": user, "auth": auth, "host_key_sha256": ""}
        if not FINGERPRINT_RE.match(fp):
            raise ConnectionError_("The server's host key fingerprint is required (SHA256:...). Use Test to see it, check it against the server, then save.")
        return {"host": host, "port": port, "username": user, "auth": auth, "host_key_sha256": fp}
    if kind == "kafka":
        servers = [x.strip() for x in str(cfg.get("bootstrap_servers") or "").split(",") if x.strip()]
        if not servers or len(servers) > 20 or not all(re.fullmatch(r"[A-Za-z0-9._-]{1,253}:\d{1,5}", x) and 1 <= int(x.rsplit(":", 1)[1]) <= 65535 for x in servers):
            raise ConnectionError_("Bootstrap servers are required as host:port, comma-separated (e.g. kafka:9092).")
        proto = cfg.get("security_protocol") or "PLAINTEXT"
        if proto not in KAFKA_PROTOCOLS:
            raise ConnectionError_(f"The security protocol must be one of {', '.join(KAFKA_PROTOCOLS)}.")
        out = {"bootstrap_servers": ",".join(servers), "security_protocol": proto}
        if proto.startswith("SASL"):
            mech = cfg.get("sasl_mechanism") or "PLAIN"
            if mech not in KAFKA_MECHANISMS:
                raise ConnectionError_(f"The SASL mechanism must be one of {', '.join(KAFKA_MECHANISMS)}.")
            user = str(cfg.get("username") or "").strip()
            if not user or len(user) > 128:
                raise ConnectionError_("A user name is required for SASL.")
            if not secret.get("password"):
                raise ConnectionError_("The password is required for SASL.")
            if proto == "SASL_PLAINTEXT" and not cfg.get("allow_insecure"):
                raise ConnectionError_("A password over SASL_PLAINTEXT is sent unencrypted. Use SASL_SSL, or tick 'allow insecure' for a trusted internal broker.")
            out.update(sasl_mechanism=mech, username=user, allow_insecure=bool(cfg.get("allow_insecure")))
        reg = str(cfg.get("schema_registry_url") or "").strip()
        if reg:
            u = urlparse(reg)
            if u.scheme not in ("http", "https") or not u.hostname or u.username or u.password or u.query or u.fragment:
                raise ConnectionError_("The Schema Registry URL must be http(s)://host[:port] without credentials, query or fragment.")
            out["schema_registry_url"] = reg.rstrip("/")
            ruser = str(cfg.get("registry_username") or "").strip()
            if ruser:
                if not secret.get("registry_password"):
                    raise ConnectionError_("The Schema Registry password (or API secret) is required with a user name (or API key).")
                if u.scheme == "http" and not cfg.get("allow_insecure"):
                    raise ConnectionError_("Credentials to the Schema Registry over plain http:// are refused. Use https://, or tick 'allow insecure' for a trusted internal registry.")
                out["registry_username"] = ruser
                out["allow_insecure"] = bool(cfg.get("allow_insecure"))
        ca = str(cfg.get("ssl_ca_pem") or "").strip()
        if ca:
            if "BEGIN CERTIFICATE" not in ca or len(ca) > 40000:
                raise ConnectionError_("The CA certificate must be PEM text (-----BEGIN CERTIFICATE-----).")
            if not proto.endswith("SSL") and not reg.startswith("https://"):
                raise ConnectionError_("A CA certificate only applies to the SSL and SASL_SSL protocols or an https:// Schema Registry.")
            out["ssl_ca_pem"] = ca
        return out
    raise ConnectionError_(f"Unknown connection type '{kind}'.")


def _clean_secret(kind: str, secret: Optional[Dict[str, Any]]) -> Dict[str, str]:
    return {k: str(v) for k, v in (secret or {}).items() if k in SECRET_FIELDS.get(kind, ()) and v not in (None, "")}


def _public(row: sqlite3.Row) -> Dict[str, Any]:
    cfg = json.loads(row["config_json"])
    return {"id": row["id"], "name": row["name"], "type": row["type"], "description": row["description"] or "", "config": cfg,
            "has_secret": bool(row["secret_enc"]), "created_by": row["created_by"], "created_at": row["created_at"], "updated_at": row["updated_at"]}


# ---------------------------------------------------------------- CRUD

def list_connections() -> List[Dict[str, Any]]:
    c = _db()
    try:
        return [_public(r) for r in c.execute("SELECT * FROM connections ORDER BY name")]
    finally:
        c.close()


def get_connection(name_or_id: str) -> Optional[Dict[str, Any]]:
    c = _db()
    try:
        r = c.execute("SELECT * FROM connections WHERE name = ? OR id = ?", (name_or_id, name_or_id)).fetchone()
        return _public(r) if r else None
    finally:
        c.close()


def get_with_secret(name_or_id: str) -> Optional[Dict[str, Any]]:
    """Internal: the public view plus the decrypted secret. Only the code that opens the connection may call this."""
    c = _db()
    try:
        r = c.execute("SELECT * FROM connections WHERE name = ? OR id = ?", (name_or_id, name_or_id)).fetchone()
        if not r:
            return None
        out = _public(r)
        out["secret"] = secrets_store.decrypt_json(r["secret_enc"]) if r["secret_enc"] else {}
        return out
    finally:
        c.close()


def create_connection(data: Dict[str, Any], actor: str) -> Dict[str, Any]:
    name = str(data.get("name") or "").strip().lower()
    if not NAME_RE.match(name):
        raise ConnectionError_("The name must be 1-40 characters: lowercase letters, digits, '_' or '-', starting with a letter or digit.")
    kind = data.get("type")
    secret = _clean_secret(kind, data.get("secret"))
    cfg = _validate_config(kind, data.get("config") or {}, secret)
    c = _db()
    try:
        if c.execute("SELECT 1 FROM connections WHERE name = ?", (name,)).fetchone():
            raise ConnectionError_(f"A connection named '{name}' already exists.")
        cid = f"conn_{uuid.uuid4().hex[:8]}"
        c.execute("INSERT INTO connections VALUES (?,?,?,?,?,?,?,?,?,?)",
                  (cid, name, kind, str(data.get("description") or "")[:300], json.dumps(cfg),
                   secrets_store.encrypt_json(secret) if secret else None, actor, _now(), actor, _now()))
        c.commit()
    finally:
        c.close()
    _audit(actor, "CONNECTION_CREATE", name, {"type": kind})
    return get_connection(cid)


def update_connection(name_or_id: str, data: Dict[str, Any], actor: str) -> Dict[str, Any]:
    """The name never changes (pipelines refer to it). Secret fields left out keep their stored value; send an empty string to clear."""
    cur = get_with_secret(name_or_id)
    if not cur:
        raise LookupError("Connection not found.")
    kind = cur["type"]
    secret = dict(cur["secret"])
    for k, v in (data.get("secret") or {}).items():
        if k not in SECRET_FIELDS[kind] or v is None:
            continue                                   # unknown field, or not sent: keep what is stored
        if v == "":
            secret.pop(k, None)                        # an explicit empty string clears it
        else:
            secret[k] = str(v)
    cfg = _validate_config(kind, {**cur["config"], **(data.get("config") or {})}, secret)
    c = _db()
    try:
        c.execute("UPDATE connections SET description=?, config_json=?, secret_enc=?, updated_by=?, updated_at=? WHERE id=?",
                  (str(data.get("description", cur["description"]) or "")[:300], json.dumps(cfg),
                   secrets_store.encrypt_json(secret) if secret else None, actor, _now(), cur["id"]))
        c.commit()
    finally:
        c.close()
    _audit(actor, "CONNECTION_UPDATE", cur["name"], {"type": kind})
    return get_connection(cur["id"])


def delete_connection(name_or_id: str, actor: str) -> None:
    cur = get_connection(name_or_id)
    if not cur:
        raise LookupError("Connection not found.")
    try:
        from web import autoloader
        users = [p["name"] for p in autoloader.list_pipelines() if str(p.get("source_volume_path", "")).startswith(f"conn://{cur['name']}/")]
    except Exception:
        users = []
    try:
        from web import streaming
        users += [x["name"] for x in streaming.list_streams() if x["connection"] == cur["name"]]
    except Exception:
        pass
    if users:
        raise ConnectionError_(f"Used by pipeline(s) or stream(s): {', '.join(users)}. Delete or change those first.")
    c = _db()
    try:
        c.execute("DELETE FROM connections WHERE id = ?", (cur["id"],))
        c.commit()
    finally:
        c.close()
    _audit(actor, "CONNECTION_DELETE", cur["name"], {"type": cur["type"]})


def definition_for_test(data: Dict[str, Any]) -> Dict[str, Any]:
    """A connection definition to try out (unsaved, or saved with some fields changed). Secret fields left out fall back to the saved
    connection's (`data['id']`), so the form does not have to hold the secret again."""
    kind = data.get("type")
    secret: Dict[str, Any] = {}
    cfg = dict(data.get("config") or {})
    if data.get("id"):
        cur = get_with_secret(data["id"])
        if not cur:
            raise LookupError("Connection not found.")
        kind = cur["type"]
        secret, cfg = dict(cur["secret"]), {**cur["config"], **cfg}
    secret.update(_clean_secret(kind, data.get("secret")))
    return {"type": kind, "config": _validate_config(kind, cfg, secret, for_test=True), "secret": secret}
