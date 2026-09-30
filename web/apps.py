"""
Embedded HTML apps: a folder of static HTML/CSS/JS, uploaded as a zip, served read-only inside a sandboxed iframe.

No server-side runtime here -- an app is exactly the bytes that were uploaded, served as-is (see CLAUDE.md for why
a Databricks-Apps-style dynamic runtime is a separate, much larger feature). Publishing (create/redeploy) is
restricted to admin/power_user, since it ships arbitrary JavaScript that will execute in front of other users'
browsers; *viewing* is grant-gated like a pipeline (RESOURCE_TYPES["app"] in web/groups.py) or open to everyone
when the app's own visibility is 'public'.

Isolation, the actual point of this module's design:
  - An app is only ever meant to be loaded inside `<iframe sandbox="allow-scripts allow-forms">` (the DKW shell,
    web/templates/index.html) -- no `allow-same-origin`, so the browser gives the iframe's document a unique
    OPAQUE origin. An opaque origin cannot read the viewer's DKW session cookie (document.cookie), cannot read
    localStorage, and -- the part that actually matters -- a fetch() it makes is treated as cross-site, so a
    SameSite=Lax session cookie is never attached to it either. httponly alone would NOT have stopped that: it
    only blocks JS from *reading* the cookie directly, not the browser from *sending* it on a same-origin request,
    which is exactly what a same-origin (non-sandboxed) request would still do.
  - Because the sandboxed origin therefore cannot use the session cookie at all, an app that wants governed data
    gets its own short-lived, purpose-scoped bearer token instead (mint_data_token / verify_data_token below):
    minted by the /apps/{id}/ route in app.py (which has already checked the VIEW grant against the real session)
    and handed to the iframe through the URL fragment, never a query string -- a fragment is never sent to any
    server and never appears in a Referer header, unlike everything after a `?`. The app's own JS reads it from
    `location.hash`.
  - `sandbox` is an attribute the PARENT page sets on the <iframe> tag; it does nothing if a browser is pointed at
    an app's URL directly as a top-level navigation (typed in, a bookmark, a raw link) -- at that point the app
    *is* running at the real DKW origin, same-site cookie rules apply normally, and a same-origin fetch from it
    would include a Lax session cookie same as any other page script would. The serving route in app.py closes
    that specific gap with the Fetch Metadata `Sec-Fetch-Dest` request header (near-universal in current
    browsers): a request whose Sec-Fetch-Dest is present and is not `iframe` is a top-level (or other non-frame)
    load and is redirected to the DKW shell instead of served directly, so an app is in practice unreachable
    except through the sandboxed iframe. Older browsers that omit the header are not covered by this specific
    check (there is nothing in the request to check); the response's own CSP `frame-ancestors 'self'` and
    `X-Frame-Options: SAMEORIGIN` headers are unrelated defense-in-depth (they stop a *different* site from
    framing the app, not a same-origin top-level visit).
"""

import datetime
import io
import logging
import os
import re
import shutil
import sqlite3
import stat
import time
import uuid
import zipfile
from typing import Any, Dict, List, Optional

import jwt
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from pydantic import BaseModel

from web.auth import JWT_ALGORITHM, JWT_SECRET_KEY, get_db_connection, require_role, resolve_principal

logger = logging.getLogger("localspark.apps")
router = APIRouter(tags=["apps"])

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
APPS_DIR = os.path.join(WAREHOUSE_DIR, ".metadata", "apps")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.\-]{0,79}$")
VISIBILITIES = ("private", "shared", "public")
MAX_ZIP_BYTES = 50 * 1024 * 1024           # 50 MB uploaded zip
MAX_UNPACKED_BYTES = 200 * 1024 * 1024     # 200 MB unpacked (zip-bomb guard)
MAX_FILES = 5000
DATA_TOKEN_TTL_MINUTES = 120
MAX_QUERY_ROWS = 50_000


class AppError(ValueError):
    """Invalid input or a refused operation; the message is safe to show."""


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _conn() -> sqlite3.Connection:
    conn = get_db_connection()
    conn.execute("""CREATE TABLE IF NOT EXISTS apps (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, owner TEXT NOT NULL,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        visibility TEXT NOT NULL DEFAULT 'private')""")
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


def _dir_for(app_id: str) -> str:
    return os.path.join(APPS_DIR, app_id)


def _clean_name(name: str) -> str:
    name = (name or "").strip()
    if not NAME_RE.match(name):
        raise AppError("The app name must be 1-80 characters: letters, digits, spaces, '_', '.' or '-', starting with a letter or digit.")
    return name


def _public(row) -> Dict[str, Any]:
    return {"id": row["id"], "name": row["name"], "owner": row["owner"], "created_at": row["created_at"],
            "updated_at": row["updated_at"], "visibility": row["visibility"]}


def list_apps() -> List[Dict[str, Any]]:
    c = _conn()
    try:
        return [_public(r) for r in c.execute("SELECT * FROM apps ORDER BY name COLLATE NOCASE")]
    finally:
        c.close()


def get_app(app_id: str) -> Optional[Dict[str, Any]]:
    c = _conn()
    try:
        r = c.execute("SELECT * FROM apps WHERE id = ?", (app_id,)).fetchone()
        return _public(r) if r else None
    finally:
        c.close()


def _extract_zip(zip_bytes: bytes, dest_dir: str) -> None:
    """Unpacks `zip_bytes` into `dest_dir`. Refuses path traversal (`..`, absolute paths, a symlink entry), caps
    entry count and total uncompressed size (a zip bomb), and requires a root-level index.html. Extraction happens
    into a sibling temp directory that is only swapped in on full success, so a rejected or failed upload never
    leaves the live app half-replaced."""
    if len(zip_bytes) > MAX_ZIP_BYTES:
        raise AppError(f"The zip is larger than the {MAX_ZIP_BYTES // (1024 * 1024)} MB limit.")
    tmp_dir = dest_dir + f".tmp-{uuid.uuid4().hex[:8]}"
    os.makedirs(tmp_dir, exist_ok=True)
    try:
        try:
            zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        except zipfile.BadZipFile:
            raise AppError("That file is not a valid zip archive.")
        with zf:
            infos = zf.infolist()
            if len(infos) > MAX_FILES:
                raise AppError(f"The zip has more than {MAX_FILES} entries.")
            total = 0
            tmp_real = os.path.realpath(tmp_dir)
            for info in infos:
                total += info.file_size
                if total > MAX_UNPACKED_BYTES:
                    raise AppError(f"Unpacked, the zip is larger than the {MAX_UNPACKED_BYTES // (1024 * 1024)} MB limit.")
                name = info.filename.replace("\\", "/")
                if name.endswith("/"):
                    continue  # directory entry: created implicitly below when a file needs it
                if os.path.isabs(name) or ":" in name:
                    raise AppError(f"Refusing an unsafe path in the zip: {name!r}")
                norm = os.path.normpath(name)
                if norm == ".." or norm.startswith(".." + os.sep):
                    raise AppError(f"Refusing an unsafe path in the zip: {name!r}")
                mode = (info.external_attr >> 16) & 0xFFFF
                if mode and stat.S_ISLNK(mode):
                    raise AppError(f"Refusing a symlink in the zip: {name!r}")
                target = os.path.join(tmp_dir, norm)
                if not (os.path.realpath(target) == tmp_real or os.path.realpath(target).startswith(tmp_real + os.sep)):
                    raise AppError(f"Refusing an unsafe path in the zip: {name!r}")
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with zf.open(info) as src, open(target, "wb") as out:
                    shutil.copyfileobj(src, out)
        if not os.path.isfile(os.path.join(tmp_dir, "index.html")):
            raise AppError("The zip must have an index.html at its root.")
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    os.makedirs(os.path.dirname(dest_dir), exist_ok=True)
    os.rename(tmp_dir, dest_dir)


def create_app(name: str, zip_bytes: bytes, owner: str, visibility: str = "private") -> Dict[str, Any]:
    name = _clean_name(name)
    if visibility not in VISIBILITIES:
        raise AppError(f"visibility must be one of {', '.join(VISIBILITIES)}.")
    app_id = f"app_{uuid.uuid4().hex[:10]}"
    _extract_zip(zip_bytes, _dir_for(app_id))
    c = _conn()
    try:
        c.execute("INSERT INTO apps (id, name, owner, created_at, updated_at, visibility) VALUES (?,?,?,?,?,?)",
                  (app_id, name, owner, _now(), _now(), visibility))
        c.commit()
    except Exception:
        shutil.rmtree(_dir_for(app_id), ignore_errors=True)
        raise
    finally:
        c.close()
    _audit(owner, "APP_CREATE", f"app:{name}", {"app_id": app_id, "visibility": visibility})
    return get_app(app_id)


def update_app_files(app_id: str, zip_bytes: bytes, actor: str) -> Dict[str, Any]:
    cur = get_app(app_id)
    if not cur:
        raise LookupError("App not found.")
    _extract_zip(zip_bytes, _dir_for(app_id))
    c = _conn()
    try:
        c.execute("UPDATE apps SET updated_at = ? WHERE id = ?", (_now(), app_id))
        c.commit()
    finally:
        c.close()
    _audit(actor, "APP_REDEPLOY", f"app:{cur['name']}", {"app_id": app_id})
    return get_app(app_id)


def update_app_meta(app_id: str, name: Optional[str], visibility: Optional[str], actor: str) -> Dict[str, Any]:
    cur = get_app(app_id)
    if not cur:
        raise LookupError("App not found.")
    new_name = _clean_name(name) if name is not None else cur["name"]
    new_vis = cur["visibility"]
    if visibility is not None:
        if visibility not in VISIBILITIES:
            raise AppError(f"visibility must be one of {', '.join(VISIBILITIES)}.")
        new_vis = visibility
    c = _conn()
    try:
        clash = c.execute("SELECT id FROM apps WHERE name = ? AND id != ?", (new_name, app_id)).fetchone()
        if clash:
            raise AppError(f"An app named '{new_name}' already exists.")
        c.execute("UPDATE apps SET name = ?, visibility = ?, updated_at = ? WHERE id = ?", (new_name, new_vis, _now(), app_id))
        c.commit()
    finally:
        c.close()
    _audit(actor, "APP_UPDATE", f"app:{new_name}", {"app_id": app_id, "was": cur["name"], "visibility": new_vis})
    return get_app(app_id)


def delete_app(app_id: str, actor: str) -> None:
    cur = get_app(app_id)
    if not cur:
        raise LookupError("App not found.")
    c = _conn()
    try:
        c.execute("DELETE FROM apps WHERE id = ?", (app_id,))
        c.commit()
    finally:
        c.close()
    shutil.rmtree(_dir_for(app_id), ignore_errors=True)
    from web import groups
    groups.delete_grants_for_resource("app", app_id)
    _audit(actor, "APP_DELETE", f"app:{cur['name']}", {"app_id": app_id})


def app_access(user: Dict[str, Any], app: Dict[str, Any]) -> Optional[str]:
    """'manage' | 'view' | None. Admin/power_user and the owner manage every app; visibility 'public' gives every
    authenticated user VIEW; otherwise VIEW/MANAGE come from a grant, direct or through a group (web/groups.py)."""
    from web import groups
    if user.get("role") in ("admin", "power_user") or user.get("username") == app.get("owner"):
        return "manage"
    granted = groups.permission_of(user, "app", app["id"])
    if granted == "MANAGE":
        return "manage"
    if granted == "VIEW" or app.get("visibility") == "public":
        return "view"
    return None


def resolve_file(app_id: str, rel_path: str) -> Optional[str]:
    """Absolute path of `rel_path` within the app's own directory, or None if it would escape that directory
    (traversal) or does not resolve to a file. `rel_path` is untrusted, straight from the URL."""
    base = os.path.realpath(_dir_for(app_id))
    rel_path = (rel_path or "").lstrip("/") or "index.html"
    target = os.path.realpath(os.path.join(base, rel_path))
    if not (target == base or target.startswith(base + os.sep)):
        return None
    if os.path.isdir(target):
        target = os.path.join(target, "index.html")
    return target if os.path.isfile(target) else None


# ---------------------------------------------------------------- the data token (see module docstring)

def mint_data_token(app_id: str, username: str) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    payload = {"purpose": "app_data", "app_id": app_id, "username": username, "iat": int(now.timestamp()),
               "exp": int((now + datetime.timedelta(minutes=DATA_TOKEN_TTL_MINUTES)).timestamp())}
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def verify_data_token(token: str, app_id: str) -> Optional[str]:
    """The username the token was minted for, or None (expired, wrong purpose, wrong app, or malformed)."""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError:
        return None
    if payload.get("purpose") != "app_data" or payload.get("app_id") != app_id:
        return None
    return payload.get("username")


# ================================================================== routes

class AppMetaPayload(BaseModel):
    name: Optional[str] = None
    visibility: Optional[str] = None


class AppQueryPayload(BaseModel):
    sql: str
    max_rows: Optional[int] = None


def _api_call(fn, *args, **kw):
    try:
        return fn(*args, **kw)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except AppError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


def _require_access(app_id: str, user: Dict[str, Any], need: str) -> Dict[str, Any]:
    app = get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="App not found.")
    access = app_access(user, app)
    if access is None or (need == "manage" and access != "manage"):
        raise HTTPException(status_code=403, detail="You don't have access to this app.")
    return app


@router.get("/api/apps")
async def list_apps_endpoint(request: Request):
    user = await resolve_principal(request)
    out = []
    for app in list_apps():
        access = app_access(user, app)
        if access:
            out.append({**app, "my_access": access})
    return {"apps": out}


@router.get("/api/apps/{app_id}")
async def get_app_endpoint(app_id: str, request: Request):
    user = await resolve_principal(request)
    app = _require_access(app_id, user, "view")
    return {**app, "my_access": app_access(user, app)}


@router.post("/api/apps")
async def create_app_endpoint(name: str = Form(...), visibility: str = Form("private"), file: UploadFile = File(...),
                              current_user: Dict[str, Any] = Depends(require_role(["admin", "power_user"]))):
    """Publishing an app ships code that will run in front of other users' browsers, so it needs the same trust
    level as creating a SQL warehouse or a connection, not merely viewing one (see web/apps.py's module docstring
    and CLAUDE.md)."""
    content = await file.read()
    app = _api_call(create_app, name, content, current_user["username"], visibility)
    return app


@router.put("/api/apps/{app_id}")
async def update_app_meta_endpoint(app_id: str, payload: AppMetaPayload, request: Request):
    user = await resolve_principal(request)
    _require_access(app_id, user, "manage")
    return _api_call(update_app_meta, app_id, payload.name, payload.visibility, user["username"])


@router.put("/api/apps/{app_id}/files")
async def redeploy_app_endpoint(app_id: str, request: Request, file: UploadFile = File(...)):
    user = await resolve_principal(request)
    _require_access(app_id, user, "manage")
    content = await file.read()
    return _api_call(update_app_files, app_id, content, user["username"])


@router.delete("/api/apps/{app_id}")
async def delete_app_endpoint(app_id: str, request: Request):
    user = await resolve_principal(request)
    _require_access(app_id, user, "manage")
    _api_call(delete_app, app_id, user["username"])
    return {"success": True}


# ---------------------------------------------------------------- serving (sandboxed iframe only, see module docstring)

_MIME_BY_EXT = {
    ".html": "text/html; charset=utf-8", ".htm": "text/html; charset=utf-8", ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8", ".mjs": "text/javascript; charset=utf-8", ".json": "application/json",
    ".svg": "image/svg+xml", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif",
    ".webp": "image/webp", ".ico": "image/x-icon", ".woff": "font/woff", ".woff2": "font/woff2", ".map": "application/json",
    ".txt": "text/plain; charset=utf-8", ".wasm": "application/wasm",
}

# A response can only ever be embedded by this studio itself (never another site framing it), and never sniffed
# into executing as something its declared Content-Type isn't.
_APP_HEADERS = {
    "Content-Security-Policy": "frame-ancestors 'self'",
    "X-Frame-Options": "SAMEORIGIN",
    "X-Content-Type-Options": "nosniff",
}


@router.api_route("/apps/{app_id}", methods=["GET"], include_in_schema=False)
async def serve_app_root(app_id: str, request: Request):
    return await serve_app_file(app_id, "", request)


@router.api_route("/apps/{app_id}/{path:path}", methods=["GET"], include_in_schema=False)
async def serve_app_file(app_id: str, path: str, request: Request):
    # See the module docstring: `sandbox` is set by the parent page and is invisible to the server, so this is the
    # one place that can refuse a request that isn't actually coming from inside that sandboxed iframe. Fetch
    # Metadata headers are sent by current Chrome/Firefox/Edge/Safari; absent (older browser), this check is a
    # no-op rather than a false refusal -- see the docstring for what that means and doesn't mean.
    dest = request.headers.get("sec-fetch-dest")
    if dest and dest != "iframe":
        return RedirectResponse(url=f"/?open_app={app_id}")
    user = await resolve_principal(request)
    app = get_app(app_id)
    if not app or not app_access(user, app):
        return Response(status_code=404, content="App not found.", headers=_APP_HEADERS)
    target = resolve_file(app_id, path)
    if not target:
        return Response(status_code=404, content="Not found.", headers=_APP_HEADERS)
    ext = os.path.splitext(target)[1].lower()
    media_type = _MIME_BY_EXT.get(ext, "application/octet-stream")
    headers = dict(_APP_HEADERS)
    if ext in (".html", ".htm"):
        # Any HTML page of the app the iframe might navigate to: hand it this viewer's own governed-data token in the
        # URL fragment (never sent to any server, never in a Referer -- see the docstring) so the page's own JS
        # can read it from location.hash without a round trip.
        token = mint_data_token(app_id, user["username"])
        try:
            with open(target, "rb") as f:
                body = f.read()
            marker = b"</head>"
            inject = f'<script>window.DKW_APP_TOKEN={token!r};window.DKW_APP_ID={app_id!r};</script>'.encode()
            body = body.replace(marker, inject + marker, 1) if marker in body else body + inject
            return Response(content=body, media_type=media_type, headers=headers)
        except OSError:
            return Response(status_code=404, content="Not found.", headers=headers)
    return FileResponse(target, media_type=media_type, headers=headers)


# ---------------------------------------------------------------- governed data gateway (bearer app-data token only)

_CORS_HEADERS = {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Authorization, Content-Type",
                 "Access-Control-Allow-Methods": "POST, OPTIONS"}


@router.options("/api/apps/{app_id}/query", include_in_schema=False)
async def query_app_preflight(app_id: str):
    # The sandboxed iframe's document has an opaque origin ("null"), so the browser always treats this POST as
    # cross-origin and preflights it. There is no session cookie involved (the opaque origin cannot send one
    # anyway, see the module docstring), so a wildcard origin here does not widen what a credentialed request
    # could do -- the bearer token, not an origin check, is what authorizes the call.
    return Response(status_code=204, headers=_CORS_HEADERS)


def _run_app_query(sql: str, username: str, max_rows: int):
    """Governed execution on a private cursor; returns (columns, rows) as plain JSON-able values."""
    from web import app as app_module
    from web.audit import log_query
    from web.auth import get_user_by_username
    from web.governance import gateway as gov_gateway
    from web.permissions import enforce_sql_permissions
    user = get_user_by_username(username)
    if not user or user.get("is_active", 1) != 1:
        raise HTTPException(status_code=401, detail="This account is not active.")
    try:
        enforce_sql_permissions(sql, user, action="READ")
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    gov = gov_gateway.govern_sql(sql, user, client="app")
    if gov.blocked:
        log_query(query_text=sql, duration_ms=0, rows_produced=0, status="FAILED", error_message=gov.blocked,
                  client="EMBEDDED_APP", user=username)
        raise HTTPException(status_code=403, detail=gov.blocked)
    started = time.perf_counter()
    cur = app_module.get_duckrun_conn().con.cursor()
    try:
        cur.execute('USE "warehouse"')
        cur.execute(gov.sql)
        if not cur.description:
            columns, rows = [], []
        else:
            columns = [d[0] for d in cur.description]
            rows = cur.fetchmany(max_rows + 1)
            if len(rows) > max_rows:
                raise HTTPException(status_code=413, detail=(
                    f"The result has more than {max_rows:,} rows. Filter or aggregate in the query instead."))
            rows = [[(v.isoformat() if hasattr(v, "isoformat") else v) for v in row] for row in rows]
    except HTTPException:
        raise
    except Exception as exc:
        log_query(query_text=sql, duration_ms=(time.perf_counter() - started) * 1000, rows_produced=0, status="FAILED",
                  error_message=str(exc), client="EMBEDDED_APP", user=username)
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        cur.close()
    log_query(query_text=sql, duration_ms=(time.perf_counter() - started) * 1000, rows_produced=len(rows),
              status="SUCCESS", client="EMBEDDED_APP", user=username)
    return columns, rows


@router.post("/api/apps/{app_id}/query")
async def query_app_data(app_id: str, payload: AppQueryPayload, request: Request):
    """The only door an app's own JS has to warehouse data: governed SQL (masking/row filters apply exactly as in
    the SQL editor), authenticated by the token the serving route minted for this viewer -- never the session
    cookie, which the sandboxed origin cannot present anyway (see the module docstring)."""
    import asyncio
    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        return JSONResponse(status_code=401, content={"detail": "App data token required."}, headers=_CORS_HEADERS)
    username = verify_data_token(auth_header[7:].strip(), app_id)
    if not username:
        return JSONResponse(status_code=401, content={"detail": "Invalid or expired app token. Reload the app."}, headers=_CORS_HEADERS)
    if not get_app(app_id):
        return JSONResponse(status_code=404, content={"detail": "App not found."}, headers=_CORS_HEADERS)
    max_rows = min(payload.max_rows or MAX_QUERY_ROWS, MAX_QUERY_ROWS)
    try:
        columns, rows = await asyncio.to_thread(_run_app_query, payload.sql, username, max_rows)
    except HTTPException as exc:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=_CORS_HEADERS)
    return JSONResponse(content={"columns": columns, "rows": rows}, headers=_CORS_HEADERS)
