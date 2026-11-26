"""Auto-Loader sources behind a Connection (web/connections.py): HTTP(S) files, REST/JSON APIs and SFTP directories.

A pipeline whose `source_volume_path` is `conn://<connection>/<path>` is *polled*. Each cycle asks this module for candidates, and for
every candidate whose identity is not yet checkpointed downloads it into a per-pipeline staging directory and hands the staged file to
the normal `process_single_file` (schema policies, merge / append, exactly-once, quarantine, history are the shared code). Nothing is
ever written to, moved on or deleted from the remote side.

Identity (what "the same file" means, so it is loaded exactly once)
  http file   url + ETag / Last-Modified / length when the server sends any (checked with HEAD, so an unchanged file is not downloaded);
              otherwise the SHA-256 of the downloaded content.
  REST/JSON   SHA-256 of the snapshot (the records fetched this cycle as NDJSON): an unchanged API response is skipped, a changed one is
              loaded as a whole. Pick merge or overwrite for an API that returns the full current state; append would add it again.
  sftp        server + path + size + mtime, and only files that have not changed for `settle_seconds` (an upload in progress is skipped).

Safety
  * The pipeline path can only extend the connection's base URL / directory (`safe_path`): no scheme, no host, no `..`, so a stored
    credential can never be sent to another host. Redirects and `next` links are followed only within the same origin.
  * SFTP verifies the server's host key against the fingerprint pinned in the connection on EVERY connect; a mismatch is refused.
  * Downloads are capped (AUTOLOADER_MAX_DOWNLOAD_MB, default 1024) and every request has a timeout.
  * Error messages never contain the secret (they are built from status codes and library error names).
"""
import base64
import fnmatch
import hashlib
import hmac
import io
import json
import logging
import os
import posixpath
import re
import shutil
import socket
import stat as stat_mod
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from urllib.parse import parse_qsl, urljoin, urlparse

logger = logging.getLogger("localspark.autoloader.conn")

SCHEME = "conn://"
MAX_BYTES = int(os.getenv("AUTOLOADER_MAX_DOWNLOAD_MB", "1024")) * 1024 * 1024
MAX_REDIRECTS = 5
SUPPORTED_EXT = {"csv", "tsv", "txt", "parquet", "json", "jsonl", "ndjson"}
CONTENT_TYPE_EXT = {"text/csv": "csv", "text/tab-separated-values": "tsv", "application/json": "json", "application/x-ndjson": "ndjson",
                    "application/jsonl": "jsonl", "application/vnd.apache.parquet": "parquet", "application/x-parquet": "parquet"}
PAGINATION = ("none", "page", "next_link", "offset")


class SourceError(Exception):
    """A problem with the source (unreachable, refused, bad path/response); the message is user-safe."""


@dataclass
class Candidate:
    rel: str                                          # shown in history
    identity: Optional[str]                           # checkpoint hash; None = only known after downloading (then the content hash)
    size: int
    fetch: Callable[..., Tuple[str, str]]             # (staging_dir, cap_bytes=None) -> (local_path, content_sha256); a preview passes a small cap


# ---------------------------------------------------------------- references and paths

def is_conn_path(path: Optional[str]) -> bool:
    return bool(path) and path.strip().lower().startswith(SCHEME)


def parse_ref(path: str) -> Tuple[str, str]:
    """('name', 'rest/of/path') from conn://name/rest/of/path."""
    p = (path or "").strip()
    if not p.lower().startswith(SCHEME):
        raise SourceError("A connection source looks like conn://<connection>/<path>.")
    name, _, rest = p[len(SCHEME):].partition("/")
    if not name:
        raise SourceError("The connection name is missing (conn://<connection>/<path>).")
    return name.lower(), rest


def safe_path(rest: str, kind: str) -> str:
    """The part after the connection name, verified to be a plain relative path (no scheme, host, `..`, backslash, control chars)."""
    rest = (rest or "").strip()
    if re.search(r"[\x00-\x1f\x7f\\]", rest) or "://" in rest or rest.startswith("//") or "#" in rest:
        raise SourceError("The path contains characters that are not allowed.")
    if ".." in rest.replace("\\", "/").split("/") or (kind == "http" and "@" in rest.split("?", 1)[0]):
        raise SourceError("The path must stay inside the connection (no '..').")
    return rest.lstrip("/")


def resolve(path: str) -> Tuple[Dict[str, Any], str]:
    """(connection with its secret, safe relative path) for a `conn://` source."""
    from web import connections
    name, rest = parse_ref(path)
    conn = connections.get_with_secret(name)
    if conn is None:
        raise SourceError(f"The connection '{name}' does not exist (it may have been deleted).")
    return conn, safe_path(rest, conn["type"])


def validate_source(path: str, options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Create/update-time check: the connection exists, the path is safe, the options fit the type. Returns the cleaned options."""
    conn, rest = resolve(path)
    return validate_options(conn["type"], options or {}, rest)


def validate_options(kind: str, options: Dict[str, Any], rest: str = "") -> Dict[str, Any]:
    if kind == "sftp":
        try:
            settle = max(0, min(int(options.get("settle_seconds", 10)), 3600))
        except (TypeError, ValueError):
            raise SourceError("The settle time must be a number of seconds.")
        return {"recursive": bool(options.get("recursive")), "settle_seconds": settle}
    mode = options.get("mode") or "file"
    if mode not in ("file", "api"):
        raise SourceError("The HTTP source mode must be 'file' or 'api'.")
    if mode == "file":
        if not rest:
            raise SourceError("Give the path of the file on the server (e.g. exports/daily.csv).")
        return {"mode": "file"}
    params = options.get("params") or {}
    if not isinstance(params, dict) or len(params) > 30 or any(not isinstance(k, str) or not isinstance(v, (str, int, float, bool)) for k, v in params.items()):
        raise SourceError("Query parameters must be a small set of name/value pairs.")
    pag = dict(options.get("pagination") or {"type": "none"})
    ptype = pag.get("type", "none")
    if ptype not in PAGINATION:
        raise SourceError(f"Pagination must be one of {', '.join(PAGINATION)}.")
    clean_pag: Dict[str, Any] = {"type": ptype}
    ident = re.compile(r"^[A-Za-z_][A-Za-z0-9_.\-]{0,63}$")
    def name_field(key, default=None, required=False):
        v = str(pag.get(key) or default or "").strip()
        if required and not v:
            raise SourceError(f"Pagination '{ptype}' needs '{key}'.")
        if v and not ident.match(v):
            raise SourceError(f"'{key}' is not a valid name.")
        return v
    def int_field(key, default, lo=1, hi=100000):
        try:
            return max(lo, min(int(pag.get(key, default)), hi))
        except (TypeError, ValueError):
            raise SourceError(f"'{key}' must be a number.")
    if ptype == "page":
        clean_pag.update(page_param=name_field("page_param", "page"), size_param=name_field("size_param"), page_size=int_field("page_size", 100), start=int_field("start", 1, 0))
    elif ptype == "offset":
        clean_pag.update(offset_param=name_field("offset_param", "offset"), limit_param=name_field("limit_param", "limit"), page_size=int_field("page_size", 100))
    elif ptype == "next_link":
        clean_pag.update(next_path=name_field("next_path", required=True))
    rp = str(options.get("records_path") or "").strip()
    if rp and not re.fullmatch(r"[A-Za-z0-9_\-]+(\.[A-Za-z0-9_\-]+)*", rp):
        raise SourceError("The records path is a dotted path such as data.items.")
    try:
        max_pages = max(1, min(int(options.get("max_pages", 100)), 1000))
    except (TypeError, ValueError):
        raise SourceError("Max pages must be a number.")
    return {"mode": "api", "params": params, "records_path": rp, "pagination": clean_pag, "max_pages": max_pages}


# ---------------------------------------------------------------- HTTP

def _origin(url: str) -> Tuple[str, str]:
    u = urlparse(url)
    return (u.scheme.lower(), u.netloc.lower())


def _auth_headers(conn: Dict[str, Any]) -> Tuple[Dict[str, str], Any]:
    cfg, secret = conn["config"], conn.get("secret") or {}
    headers = {"User-Agent": "DataKilnWorks-AutoLoader/1"}
    auth = None
    if cfg.get("auth") == "bearer":
        headers["Authorization"] = f"Bearer {secret.get('token', '')}"
    elif cfg.get("auth") == "header":
        headers[cfg.get("header_name", "")] = secret.get("header_value", "")
    elif cfg.get("auth") == "basic":
        from requests.auth import HTTPBasicAuth
        auth = HTTPBasicAuth(cfg.get("username", ""), secret.get("password", ""))
    elif cfg.get("auth") == "oauth2":
        from web import oauth_client
        try:
            token, ttype = oauth_client.get_token(conn)
        except oauth_client.OAuthError as exc:
            raise SourceError(str(exc))
        headers["Authorization"] = f"{ttype} {token}"
    return headers, auth


def _http(conn: Dict[str, Any], url: str, method: str = "GET", params: Optional[Dict[str, Any]] = None, stream: bool = False):
    """One request with the connection's auth. Redirects are followed by hand, only within the base URL's origin."""
    import requests
    cfg = conn["config"]
    base = cfg["base_url"]
    headers, auth = _auth_headers(conn)
    timeout = (min(10, cfg.get("timeout_seconds", 30)), cfg.get("timeout_seconds", 30))
    if _origin(url) != _origin(base) or not urlparse(url).path.startswith(urlparse(base).path):
        raise SourceError("The request would leave the connection's base URL.")
    refreshed = False
    for _ in range(MAX_REDIRECTS + 2):
        try:
            r = requests.request(method, url, params=params, headers=headers, auth=auth, timeout=timeout, stream=stream, allow_redirects=False)
        except requests.RequestException as exc:
            raise SourceError(f"Could not reach the server ({type(exc).__name__}).")
        if r.is_redirect or r.is_permanent_redirect:
            target = urljoin(url, r.headers.get("location", ""))
            r.close()
            if _origin(target) != _origin(base):
                raise SourceError("The server redirects to another host; that is not followed.")
            url, params = target, None
            continue
        if r.status_code == 401 and cfg.get("auth") == "oauth2" and not refreshed:
            r.close()                                            # the token was revoked or expired early: one fresh token, one retry
            refreshed = True
            from web import oauth_client
            oauth_client.invalidate(conn)
            headers, auth = _auth_headers(conn)
            continue
        if r.status_code in (401, 403):
            r.close()
            raise SourceError(f"The server refused the credentials (HTTP {r.status_code}).")
        if r.status_code == 404:
            r.close()
            raise SourceError("Not found (HTTP 404): check the path.")
        if r.status_code >= 400:
            r.close()
            raise SourceError(f"The server answered HTTP {r.status_code}.")
        return r
    raise SourceError("Too many redirects.")


def _url(conn: Dict[str, Any], rest: str) -> str:
    return conn["config"]["base_url"] + rest


def _ext_for(name: str, content_type: str = "") -> str:
    ext = os.path.splitext(urlparse(name).path)[1].lower().lstrip(".")
    if ext in SUPPORTED_EXT:
        return ext
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct in CONTENT_TYPE_EXT:
        return CONTENT_TYPE_EXT[ct]
    raise SourceError("The file format cannot be told from the URL or the server's content type. Use a URL ending in .csv, .json, .parquet, ...")


def _limit_text(cap: Optional[int] = None) -> str:
    cap = cap or MAX_BYTES
    return f"{cap // (1024 * 1024)} MB" if cap >= 1024 * 1024 else f"{cap} bytes"


def _safe_name(name: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9._-]", "_", os.path.basename(name))[:80] or "download"
    return stem


def _stream_to(resp, dest: str, cap: Optional[int] = None) -> str:
    h, total = hashlib.sha256(), 0
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(1 << 20):
            total += len(chunk)
            if total > (cap or MAX_BYTES):
                resp.close()
                raise SourceError(f"The download is larger than {_limit_text(cap)}" + (" (too large to preview)." if cap else " (AUTOLOADER_MAX_DOWNLOAD_MB)."))
            h.update(chunk)
            f.write(chunk)
    resp.close()
    return h.hexdigest()


def _http_file_candidates(conn, rest, pipe) -> List[Candidate]:
    url = _url(conn, rest)
    identity = None
    size = 0
    try:
        head = _http(conn, url, "HEAD")
        etag, lm, size = head.headers.get("ETag"), head.headers.get("Last-Modified"), int(head.headers.get("Content-Length") or 0)
        head.close()
        if etag or lm:
            identity = hashlib.sha256(f"http|{url}|{etag}|{lm}|{size}".encode()).hexdigest()
    except SourceError as exc:
        if "HTTP 405" not in str(exc) and "HTTP 501" not in str(exc):
            raise                                              # a real problem (auth, 404, unreachable); only HEAD-unsupported falls through

    def fetch(staging: str, cap: Optional[int] = None) -> Tuple[str, str]:
        r = _http(conn, url, "GET", stream=True)
        ext = _ext_for(rest, r.headers.get("Content-Type", ""))
        dest = os.path.join(staging, _safe_name(rest) if os.path.splitext(rest)[1].lower().lstrip(".") in SUPPORTED_EXT else f"{_safe_name(rest)}.{ext}")
        return dest, _stream_to(r, dest, cap)
    return [Candidate(rel=rest, identity=identity, size=size, fetch=fetch)]


# ---------------------------------------------------------------- REST / JSON

def _dig(obj: Any, dotted: str) -> Any:
    for part in [p for p in (dotted or "").split(".") if p]:
        if isinstance(obj, dict):
            obj = obj.get(part)
        elif isinstance(obj, list) and part.isdigit() and int(part) < len(obj):
            obj = obj[int(part)]
        else:
            return None
    return obj


def _records(payload: Any, records_path: str) -> List[Any]:
    data = _dig(payload, records_path) if records_path else payload
    if data is None:
        raise SourceError(f"The records path '{records_path}' was not found in the response." if records_path else "The response is empty.")
    if isinstance(data, dict):
        return [data]
    if not isinstance(data, list):
        raise SourceError("The response does not contain a list of records (set the records path, e.g. data.items).")
    return [r if isinstance(r, dict) else {"value": r} for r in data]


def _api_pages(conn, rest, opts) -> Iterator[List[Any]]:
    url = _url(conn, rest)
    pag, params = opts["pagination"], dict(opts.get("params") or {})
    ptype, pages = pag["type"], 0
    n = pag.get("start", 1)
    offset = 0
    while pages < opts["max_pages"]:
        q = dict(params)
        if ptype == "page":
            q[pag["page_param"]] = n
            if pag.get("size_param"):
                q[pag["size_param"]] = pag["page_size"]
        elif ptype == "offset":
            q[pag["offset_param"]], q[pag["limit_param"]] = offset, pag["page_size"]
        r = _http(conn, url, "GET", params=q or None)
        try:
            if int(r.headers.get("Content-Length") or 0) > MAX_BYTES:
                raise SourceError("A response is larger than the download limit.")
            payload = r.json()
        except ValueError:
            raise SourceError("The response is not valid JSON.")
        finally:
            r.close()
        recs = _records(payload, opts.get("records_path", ""))
        pages += 1
        if recs:
            yield recs
        if ptype == "none":
            return
        if ptype == "next_link":
            nxt = _dig(payload, pag["next_path"])
            if not nxt or not isinstance(nxt, str):
                return
            url, params = urljoin(url, nxt), {}             # the link carries its own query; origin is re-checked in _http
        elif ptype == "page":
            if not recs or (pag.get("size_param") and len(recs) < pag["page_size"]):
                return
            n += 1
        else:
            if not recs or len(recs) < pag["page_size"]:
                return
            offset += len(recs)
    logger.warning(f"REST source stopped at max_pages={opts['max_pages']}")


def _api_candidates(conn, rest, opts) -> List[Candidate]:
    def fetch(staging: str, cap: Optional[int] = None) -> Tuple[str, str]:
        dest, h, total = os.path.join(staging, "snapshot.jsonl"), hashlib.sha256(), 0
        with open(dest, "w", encoding="utf-8") as f:
            for page in _api_pages(conn, rest, opts):
                for rec in page:
                    line = json.dumps(rec, default=str, ensure_ascii=False, sort_keys=True) + "\n"
                    total += len(line)
                    if total > MAX_BYTES:
                        raise SourceError("The API returned more data than the download limit.")
                    h.update(line.encode("utf-8"))
                    f.write(line)
        if total == 0:
            raise SourceError("The API returned no records.")
        return dest, h.hexdigest()
    return [Candidate(rel=f"{rest or '/'} (API snapshot)", identity=None, size=0, fetch=fetch)]


# ---------------------------------------------------------------- SFTP

def fingerprint_of(key) -> str:
    return "SHA256:" + base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")


def _load_key(pem: str, passphrase: Optional[str]):
    import paramiko
    for cls in (paramiko.Ed25519Key, paramiko.ECDSAKey, paramiko.RSAKey):
        try:
            return cls.from_private_key(io.StringIO(pem), password=passphrase or None)
        except (paramiko.SSHException, ValueError):
            continue
    raise SourceError("The private key could not be read (wrong format or passphrase).")


def discover_host_key(host: str, port: int) -> str:
    """Connects without authenticating and returns the server's host key fingerprint (to be checked by a person, then pinned)."""
    import paramiko
    try:
        sock = socket.create_connection((host, port), timeout=15)
        t = paramiko.Transport(sock)
        t.start_client(timeout=15)
        fp = fingerprint_of(t.get_remote_server_key())
        t.close()
        return fp
    except (OSError, paramiko.SSHException) as exc:
        raise SourceError(f"Could not connect to {host}:{port} ({type(exc).__name__}).")


def open_sftp(conn: Dict[str, Any]):
    """(transport, sftp client) after verifying the pinned host key and authenticating. Caller closes the transport."""
    import paramiko
    cfg, secret = conn["config"], conn.get("secret") or {}
    try:
        sock = socket.create_connection((cfg["host"], cfg["port"]), timeout=15)
        t = paramiko.Transport(sock)
        t.banner_timeout = 15
        t.start_client(timeout=15)
        got = fingerprint_of(t.get_remote_server_key())
        if not hmac.compare_digest(got, cfg["host_key_sha256"]):
            t.close()
            raise SourceError(f"The server's host key does not match the pinned fingerprint (server presented {got}). "
                              "If the key was changed on purpose, edit the connection and pin the new fingerprint.")
        if cfg["auth"] == "key":
            t.auth_publickey(cfg["username"], _load_key(secret.get("private_key", ""), secret.get("passphrase")))
        else:
            t.auth_password(cfg["username"], secret.get("password", ""))
        return t, paramiko.SFTPClient.from_transport(t)
    except paramiko.AuthenticationException:
        raise SourceError("The SFTP server refused the credentials.")
    except (OSError, paramiko.SSHException, EOFError) as exc:
        raise SourceError(f"SFTP connection failed ({type(exc).__name__}).")


def _sftp_walk(sftp, base: str, recursive: bool, pattern: str, settle: int) -> List[Tuple[str, int, float]]:
    out, now, stack = [], time.time(), [base]
    while stack:
        d = stack.pop()
        try:
            entries = sftp.listdir_attr(d)
        except (OSError, IOError) as exc:
            raise SourceError(f"Cannot list '{d}' on the server ({type(exc).__name__}).")
        for e in entries:
            name = e.filename
            if name.startswith(".") or name == "_quarantine":
                continue
            full = posixpath.join(d, name)
            if stat_mod.S_ISDIR(e.st_mode or 0):
                if recursive:
                    stack.append(full)
                continue
            if name.endswith((".tmp", ".part")) or not (pattern == "*" or fnmatch.fnmatch(name.lower(), pattern.lower())):
                continue
            if (e.st_mtime or 0) > now - settle:
                continue                                   # still being written (or just finished): next cycle
            out.append((full, int(e.st_size or 0), float(e.st_mtime or 0)))
    return sorted(out, key=lambda x: x[2])


def _sftp_candidates(conn, rest, opts, pipe) -> List[Candidate]:
    base = "/" + rest.strip("/")
    t, sftp = open_sftp(conn)
    try:
        files = _sftp_walk(sftp, base, opts.get("recursive", False), pipe.get("file_pattern") or "*", opts.get("settle_seconds", 10))
    finally:
        t.close()
    host = conn["config"]["host"]

    def make(path: str, size: int, mtime: float) -> Candidate:
        def fetch(staging: str, cap: Optional[int] = None) -> Tuple[str, str]:
            t2, s2 = open_sftp(conn)
            try:
                dest = os.path.join(staging, _safe_name(path))
                h, total = hashlib.sha256(), 0
                with s2.open(path, "rb") as src, open(dest, "wb") as out:
                    src.prefetch()
                    while True:
                        chunk = src.read(1 << 20)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > (cap or MAX_BYTES):
                            raise SourceError(f"The file is larger than {_limit_text(cap)}" + (" (too large to preview)." if cap else " (AUTOLOADER_MAX_DOWNLOAD_MB)."))
                        h.update(chunk)
                        out.write(chunk)
                return dest, h.hexdigest()
            except (OSError, IOError) as exc:
                raise SourceError(f"Could not download '{path}' ({type(exc).__name__}).")
            finally:
                t2.close()
        rel = path[len(base):].lstrip("/") or posixpath.basename(path)
        return Candidate(rel=rel, identity=hashlib.sha256(f"sftp|{host}|{path}|{size}|{int(mtime)}".encode()).hexdigest(), size=size, fetch=fetch)
    return [make(*f) for f in files]


# ---------------------------------------------------------------- entry points

def staging_dir(pipeline_id: str) -> str:
    d = os.path.join(os.getenv("WAREHOUSE_DIR", "/workspace/warehouse"), ".metadata", "autoloader_staging", re.sub(r"[^A-Za-z0-9_-]", "_", pipeline_id))
    os.makedirs(d, exist_ok=True)
    for name in os.listdir(d):                              # leftovers of an interrupted cycle (quarantine is kept for inspection)
        if name != "_quarantine":
            try:
                p = os.path.join(d, name)
                shutil.rmtree(p) if os.path.isdir(p) else os.remove(p)
            except OSError:
                pass
    return d


def discover(pipe: Dict[str, Any]) -> List[Candidate]:
    """What this pipeline could load now. Raises SourceError for a missing connection, unreachable server, refused credentials..."""
    conn, rest = resolve(pipe["source_volume_path"])
    opts = pipe.get("source_options") or {}
    if conn["type"] == "sftp":
        return _sftp_candidates(conn, rest, validate_options("sftp", opts), pipe)
    o = validate_options("http", opts, rest)
    return _http_file_candidates(conn, rest, pipe) if o["mode"] == "file" else _api_candidates(conn, rest, o)


def test_connection(conn: Dict[str, Any]) -> Dict[str, Any]:
    """Checks a (possibly unsaved) connection. SFTP without a pinned fingerprint returns the one the server presents."""
    cfg = conn["config"]
    try:
        if conn["type"] == "http":
            prefix = ""
            if cfg.get("auth") == "oauth2":
                from web import oauth_client
                try:
                    prefix = f"Token obtained (valid for {oauth_client.test(conn)['lifetime']} s). "
                except oauth_client.OAuthError as exc:
                    return {"ok": False, "message": str(exc)}
            r = _http(conn, cfg["base_url"], "GET", stream=True)
            code = r.status_code
            r.close()
            return {"ok": True, "message": f"{prefix}Reached {urlparse(cfg['base_url']).netloc} (HTTP {code})."}
        if not cfg.get("host_key_sha256"):
            fp = discover_host_key(cfg["host"], int(cfg.get("port") or 22))
            return {"ok": False, "fingerprint": fp, "message": f"The server presents host key {fp}. Check it against the server, then save with this fingerprint."}
        t, sftp = open_sftp(conn)
        try:
            sftp.listdir("/")
        finally:
            t.close()
        return {"ok": True, "fingerprint": cfg["host_key_sha256"], "message": "Connected; host key matches and the login works."}
    except SourceError as exc:
        msg = str(exc)
        if conn["type"] == "http" and (msg.startswith("Not found") or msg.startswith("The server answered HTTP")):
            if cfg.get("auth") == "oauth2":
                msg = "The token was issued. " + msg
            # The server is there and did not refuse the login; an API's bare base URL often has no page of its own.
            return {"ok": True, "message": f"Reached {urlparse(cfg['base_url']).netloc}. {msg} (fine if the pipeline's path adds the resource)."}
        return {"ok": False, "message": msg}


# ---------------------------------------------------------------- preview

PREVIEW_ROWS = 10
PREVIEW_BYTES = 20 * 1024 * 1024
PREVIEW_RECORDS = 200          # records of an API's first page that are read to infer the columns


def _jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (bytes, bytearray)):
        return f"<{len(v)} bytes>"
    return str(v)


def preview(path: str, options: Optional[Dict[str, Any]] = None, file_pattern: str = "*", limit: int = PREVIEW_ROWS) -> Dict[str, Any]:
    """What the first rows of a connection source look like, BEFORE a pipeline exists: nothing is checkpointed, loaded or kept.

    HTTP file: the file (up to 20 MB). REST API: the first page only (the pipeline itself fetches every page). SFTP: the oldest matching file
    of the folder, plus the names of the others. Raises SourceError with a user-safe message."""
    import tempfile
    import duckdb
    from web import autoloader
    conn, rest = resolve(path)
    opts = validate_options(conn["type"], options or {}, rest)
    notes: List[str] = []
    files: List[str] = []
    limit = max(1, min(int(limit or PREVIEW_ROWS), 50))
    with tempfile.TemporaryDirectory(prefix="dkw_preview_") as tmp:
        if conn["type"] == "sftp":
            cands = _sftp_candidates(conn, rest, {**opts, "settle_seconds": 0}, {"file_pattern": file_pattern or "*"})
            if not cands:
                raise SourceError("No file in that folder matches the pattern.")
            files = [c.rel for c in cands[:20]]
            notes.append(f"{len(cands)} matching file(s); showing the first ({cands[0].rel}). The pipeline loads each file once.")
            sample = cands[0].rel
            local, _ = cands[0].fetch(tmp, PREVIEW_BYTES)
            kind = "sftp"
        elif opts["mode"] == "file":
            cand = _http_file_candidates(conn, rest, {})[0]
            local, _ = cand.fetch(tmp, PREVIEW_BYTES)
            sample, kind = rest, "file"
        else:
            local, kind, sample = os.path.join(tmp, "first_page.jsonl"), "api", f"{rest or '/'} (first page)"
            pages = _api_pages(conn, rest, opts)
            first = next(pages, None)
            if not first:
                raise SourceError("The API returned no records on its first page.")
            with open(local, "w", encoding="utf-8") as f:
                for rec in first[:PREVIEW_RECORDS]:
                    f.write(json.dumps(rec, default=str, ensure_ascii=False) + "\n")
            notes.append(f"First page only ({len(first)} record{'s' if len(first) != 1 else ''}); each poll fetches every page as one snapshot.")
            pages.close()
        ext = os.path.splitext(local)[1].lower().lstrip(".")
        duck = duckdb.connect(":memory:")
        try:
            reader = autoloader._open_source_reader(duck, local, ext, limit=limit + 1)
            table = reader.read_all()
        except Exception as exc:
            raise SourceError(f"The data could not be read as {ext.upper() or 'a table'} ({str(exc).splitlines()[0][:160]}).")
        finally:
            duck.close()
        more = table.num_rows > limit
        table = table.slice(0, limit)
        return {"ok": True, "source": kind, "sample": sample, "files": files, "notes": notes, "truncated": more,
                "columns": [{"name": f.name, "type": str(f.type)} for f in table.schema],
                "rows": [[_jsonable(v) for v in row] for row in zip(*[c.to_pylist() for c in table.columns])] if table.num_columns else []}
