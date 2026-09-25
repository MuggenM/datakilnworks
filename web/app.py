import os
import re
import time
import math
import uuid
import shutil
import datetime
import decimal
import json
import logging
from typing import Dict, Any, List, Optional, Union

import numpy as np
import pandas as pd
import duckdb
import duckrun
from deltalake import DeltaTable, write_deltalake
import asyncio
from fastapi import FastAPI, Request, Response, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from web.auth import (
    get_current_user, resolve_principal, require_role, create_access_token, verify_password,
    get_user_by_username, get_user_by_id, list_users, create_user, update_user, reset_user_password,
    delete_user, restore_user, record_user_login, COOKIE_NAME, get_db_connection, init_auth_db, decode_access_token
)
from web import auth_frameworks, llm_settings
from web.compute_auth import compute_headers
from web.governance import gateway as gov_gateway
from web.governance.enforce import GovernanceBlocked
from web import onelake
from web.permissions import (
    can_user_access_catalog, can_user_manage_catalog, can_user_delete_catalog,
    delete_all_catalog_permissions, filter_catalogs_for_user,
    list_catalog_permissions, grant_catalog_permission, revoke_catalog_permission,
    enforce_sql_permissions, get_catalog_owner
)
from web.audit import log_query, get_query_history, get_query_by_id, clear_query_history, save_query_profile
from web.profiler import execute_profiled_query, parse_profile_json
from web.dashboards import (
    DEFAULT_DASHBOARDS, load_dashboards_store, save_dashboards_store,
    execute_widget_query, resolve_query_parameters, get_dashboard_filter_options
)
from web.workflow import (
    load_jobs, get_job, create_or_update_job, delete_job,
    run_pipeline, get_job_runs, get_run_detail, cron_scheduler_loop
)
from web.genie import (
    get_available_providers, extract_schema_context,
    load_chats, get_chat, create_chat, delete_chat, ask_genie
)
from web.warehouses import (
    CLUSTER_SIZES,
    load_sql_warehouses,
    save_sql_warehouses,
    get_sql_warehouse,
    create_sql_warehouse,
    update_sql_warehouse,
    start_sql_warehouse,
    stop_sql_warehouse,
    delete_sql_warehouse,
    apply_warehouse_compute,
    get_compute_nodes_status,
    load_catalogs,
    save_catalogs,
    get_catalog,
    create_catalog,
    delete_catalog,
    create_catalog_schema,
    sync_catalogs_with_duckrun,
    scan_all_catalogs_and_tables
)
from web.ray_engine import ray_manager, RAY_INSTALLED

logger = logging.getLogger("databricks_studio")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Data Kiln Works Studio", version="2.4.0", docs_url="/api/docs", redoc_url="/api/redoc")

async def _governance_reconcile_loop(interval_seconds: int = 6 * 3600):
    """Flags tags whose table/column no longer exists (orphans) at startup and every few hours; never deletes them."""
    await asyncio.sleep(20)
    while True:
        try:
            from web.governance import tags as gov_tags
            cur = get_duckrun_conn().con.cursor()
            try:
                result = await asyncio.to_thread(gov_tags.reconcile, cur, "system")
            finally:
                cur.close()
            if result["orphaned"] or result["restored"]:
                logger.info(f"Governance reconcile: {result}")
        except asyncio.CancelledError:
            break
        except Exception as e_rec:
            logger.warning(f"Governance reconcile failed: {e_rec}")
        await asyncio.sleep(interval_seconds)


def _log_security_posture():
    """Warns about configurations that undermine the governance trust boundary."""
    from web.auth import governance_require_auth
    from web.notebook_access import execution_mode
    if not governance_require_auth():
        logger.warning("Governance: GOVERNANCE_REQUIRE_AUTH is off; requests without credentials run as the local admin.")
    if execution_mode() == "all":
        logger.warning("Governance: GOVERNANCE_NOTEBOOK_EXECUTION=all; notebook code can read warehouse files directly, "
                       "so column masking does not apply to it.")


@app.on_event("startup")
async def startup_event():
    _log_security_posture()
    from web.governance.store import init_governance_db
    init_governance_db()
    asyncio.create_task(_governance_reconcile_loop())
    asyncio.create_task(cron_scheduler_loop())
    from web import warehouse_lifecycle
    asyncio.create_task(warehouse_lifecycle.autosuspend_loop(_warehouse_has_active_queries))
    try:
        from web.dbt_service import ensure_project
        ensure_project()
    except Exception as e_dbt:
        logger.warning(f"dbt project check failed: {e_dbt}")
    init_auth_db()
    from web.alerts import alerts_scheduler_loop, init_alerts_db
    init_alerts_db()
    asyncio.create_task(alerts_scheduler_loop())
    from web.experiments import init_experiments_db
    init_experiments_db()
    from web.playground import init_playground_db
    init_playground_db()
    try:
        from web.lineage import init_lineage_db, scan_and_sync_all_assets
        init_lineage_db()
        asyncio.create_task(asyncio.to_thread(scan_and_sync_all_assets))
    except Exception as e_lin:
        logger.warning(f"Failed to auto-scan lineage on startup: {e_lin}")
    # Initialize scheduled exports
    from web.scheduled_exports import init_scheduler
    init_scheduler()
    # Initialize Volume Auto-Loader daemon
    try:
        from web.volumes import ensure_default_volumes
        from web.autoloader import init_autoloader_db, autoloader_daemon_loop, seed_demo_pipeline
        ensure_default_volumes()
        init_autoloader_db()
        seed_demo_pipeline()
        asyncio.create_task(autoloader_daemon_loop())
    except Exception as e_al:
        logger.warning(f"Failed to auto-start autoloader daemon on startup: {e_al}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    from web.scheduled_exports import shutdown_scheduler
    shutdown_scheduler()

from web import sandbox_client, sandbox_gateway
from web.governance import routes as governance_routes
app.include_router(governance_routes.router)
app.include_router(sandbox_gateway.router)


@app.middleware("http")
async def _sandbox_isolation(request: Request, call_next):
    """
    Kernels in the notebook sandbox may only call /api/sandbox/*. Without this, a credential-less request from a kernel
    would be the local admin in the default single-user mode and could read unmasked data through any other route.
    """
    if not request.url.path.startswith("/api/sandbox/") and sandbox_client.is_sandbox_peer(request.client.host if request.client else None):
        return JSONResponse(status_code=403, content={"detail": "Notebook sandboxes may only use /api/sandbox/*."})
    return await call_next(request)


_MUST_CHANGE_PASSWORD_ALLOWED = {"/api/auth/change-password", "/api/auth/logout", "/api/auth/me", "/api/auth/login"}


@app.middleware("http")
async def _must_change_password_gate(request: Request, call_next):
    """
    A password an account holder didn't choose themselves (the INIT_ADMIN_* bootstrap, or an admin's reset) must be
    changed before that session can do anything else -- enforced here, not just in the UI, so a direct API call
    can't skip it. Only touches /api/*; the SPA shell and static assets stay reachable so the browser can load and
    show the "change your password" screen in the first place.
    """
    path = request.url.path
    if not path.startswith("/api/") or path in _MUST_CHANGE_PASSWORD_ALLOWED or path.startswith("/api/sandbox/"):
        return await call_next(request)
    token = request.cookies.get(COOKIE_NAME)
    if token:
        payload = decode_access_token(token)
        if payload and "sub" in payload:
            user = get_user_by_id(payload["sub"])
            # Mirror resolve_principal's own staleness check: a session issued before the account's last
            # password change is not this user's live session, so leave it to the normal auth layer to
            # reject (401) instead of misreading it as "the current user, who must change their password".
            changed = user.get("password_changed_at") if user else None
            stale = bool(changed and int(payload.get("iat", 0)) < int(changed))
            if user and user.get("is_active", 1) == 1 and not stale and user.get("must_change_password"):
                return JSONResponse(status_code=403, content={
                    "detail": "This account's password must be changed before continuing.",
                    "must_change_password": True})
    return await call_next(request)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")
UPLOADS_DIR = "/tmp/uploads"

os.makedirs(UPLOADS_DIR, exist_ok=True)

templates = Jinja2Templates(directory=TEMPLATES_DIR)
if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Data Kiln Works Documentation & Manual Portal
DOCS_DIR = os.getenv("DOCS_DIR", "/workspace/docs")
if not os.path.exists(DOCS_DIR) or not os.path.exists(os.path.join(DOCS_DIR, "index.html")):
    DOCS_DIR = os.path.join(os.path.dirname(BASE_DIR), "docs")
if not os.path.exists(DOCS_DIR) or not os.path.exists(os.path.join(DOCS_DIR, "index.html")):
    DOCS_DIR = os.path.join(BASE_DIR, "docs")

if os.path.exists(DOCS_DIR) and os.path.exists(os.path.join(DOCS_DIR, "index.html")):
    app.mount("/docs", StaticFiles(directory=DOCS_DIR, html=True), name="docs")
    logger.info(f"Mounted Data Kiln Works documentation portal from {DOCS_DIR}")

@app.api_route("/documentation", methods=["GET", "HEAD"], response_class=RedirectResponse, include_in_schema=False)
async def documentation_redirect():
    return RedirectResponse(url="/docs/")

@app.api_route("/manual", methods=["GET", "HEAD"], response_class=RedirectResponse, include_in_schema=False)
async def manual_redirect():
    return RedirectResponse(url="/docs/")

@app.api_route("/tutorial", methods=["GET", "HEAD"], response_class=RedirectResponse, include_in_schema=False)
async def tutorial_redirect():
    return RedirectResponse(url="/docs/#hands-on-tutorial")


WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
NOTEBOOKS_DIR = os.getenv("NOTEBOOKS_DIR", "/workspace/notebooks")

os.makedirs(WAREHOUSE_DIR, exist_ok=True)
os.makedirs(NOTEBOOKS_DIR, exist_ok=True)

# Shared Duckrun connection
_duckrun_conn = None

def get_duckrun_conn():
    global _duckrun_conn
    if _duckrun_conn is None:
        _duckrun_conn = duckrun.connect(WAREHOUSE_DIR, read_only=False)
        try:
            _duckrun_conn.sql("SET max_memory = '2GB';")
            _duckrun_conn.sql("SET preserve_insertion_order = false;")
        except Exception:
            pass
        sync_catalogs_with_duckrun(_duckrun_conn)
        try:
            from web.ai_sql import register_duckdb_ai_functions
            register_duckdb_ai_functions(_duckrun_conn.con)
        except Exception as e:
            logger.warning(f"Failed registering DuckDB AI UDFs: {e}")
        try:
            from web.governance.macros import install_governance_macros
            install_governance_macros(_duckrun_conn.con)
        except Exception as e:
            logger.error(f"Failed installing governance masks (masking policies will fail closed): {e}")
    else:
        sync_catalogs_with_duckrun(_duckrun_conn)
    return _duckrun_conn


# Governance runs its metadata lookups (existence checks, column listings) on an isolated cursor.
governance_routes.set_connection_provider(lambda: get_duckrun_conn().con.cursor())


def _gov_or_403(sql: str, user, *, catalog: Optional[str] = None, client: str = "sql", trusted: bool = False) -> str:
    """Runs `sql` through the governance gateway for `user`; returns the SQL to execute or raises HTTP 403."""
    try:
        return gov_gateway.governed_sql_or_raise(sql, user, catalog=catalog, client=client, trusted=trusted)
    except GovernanceBlocked as exc:
        raise HTTPException(status_code=403, detail=str(exc))

def clean_json_value(v: Any) -> Any:
    """Sanitizes individual values for RFC 7159/8259 compliant JSON serialization, replacing NaNs/Infs/NAs with None."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    elif isinstance(v, (np.floating,)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v)
    elif isinstance(v, (np.integer,)):
        return int(v)
    elif isinstance(v, (np.bool_,)):
        return bool(v)
    elif isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return v.isoformat()
    elif isinstance(v, decimal.Decimal):
        if v.is_nan() or v.is_infinite():
            return None
        return float(v)
    elif isinstance(v, bytes):
        return v.hex()
    elif isinstance(v, (list, tuple, set)):
        return [clean_json_value(x) for x in v]
    elif isinstance(v, dict):
        return {str(k): clean_json_value(sub_v) for k, sub_v in v.items()}
    return v

def json_serializable_row(row_dict: Any) -> Any:
    """Recursively converts datetimes, timestamps, decimals, NaN/Infs, numpy types, and bytes to JSON serializable objects."""
    if not isinstance(row_dict, dict):
        return clean_json_value(row_dict)
    return {str(k): clean_json_value(v) for k, v in row_dict.items()}

def scan_delta_tables() -> List[Dict[str, Any]]:
    """Walks the warehouse directory and returns metadata for all Delta Lake tables."""
    tables = []
    if not os.path.exists(WAREHOUSE_DIR):
        return tables

    for root, dirs, files in os.walk(WAREHOUSE_DIR):
        dirs[:] = [d for d in dirs if not d.startswith(".")]     # .metadata, .dbt (private dbt output), ...: never catalog schemas
        if "_delta_log" in dirs:
            rel = os.path.relpath(root, WAREHOUSE_DIR)
            parts = rel.split(os.sep)
            if len(parts) >= 2:
                schema_name = parts[0]
                table_name = parts[1]
            else:
                schema_name = "dbo"
                table_name = parts[0]

            total_size = sum(os.path.getsize(os.path.join(root, f)) for f in files)
            version = 0
            num_files = 0
            try:
                dt = DeltaTable(root)
                fields = [f.name for f in dt.schema().fields]
                if fields == ["__duckrun_deleted__"] or "__duckrun_deleted__" in fields:
                    continue
                version = dt.version()
                num_files = len(dt.file_uris())
            except Exception as e:
                logger.warning(f"Error loading DeltaTable at {root}: {e}")
                continue

            tables.append({
                "schema": schema_name,
                "name": table_name,
                "full_name": f"{schema_name}.{table_name}",
                "path": root,
                "version": version,
                "num_files": num_files,
                "size_bytes": total_size
            })
    return tables

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    resp = templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"warehouse_dir": WAREHOUSE_DIR}
    )
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.get("/api/status")
async def get_status():
    conn = get_duckrun_conn()
    tables = scan_delta_tables()
    return {
        "engine": "DuckDB + duckrun",
        "duckdb_version": duckdb.__version__,
        "duckrun_version": getattr(duckrun, "__version__", "0.4.68"),
        "warehouse_dir": WAREHOUSE_DIR,
        "table_count": len(tables),
        "status": "RUNNING"
    }

# ==============================================================================
# MULTI-USER RBAC & AUTHENTICATION ENDPOINTS
# ==============================================================================

class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreateRequest(BaseModel):
    username: str
    password: str
    display_name: Optional[str] = None
    full_name: Optional[str] = None
    email: Optional[str] = None
    role: str = "user"

class UserUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    full_name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None

class PasswordResetRequest(BaseModel):
    new_password: str

class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str

class CatalogPermissionRequest(BaseModel):
    user_id: Optional[str] = None
    username: Optional[str] = None
    permission: str = "READ"  # READ, WRITE, ADMIN


@app.post("/api/auth/login")
async def login_endpoint(payload: LoginRequest):
    u = get_user_by_username(payload.username, include_password_hash=True)
    source = (u.get("auth_source") or "local") if u else None
    if u and source != "ldap":
        # Every account except an LDAP-provisioned one authenticates against its own stored hash (unchanged from
        # before LDAP support existed). LDAP accounts never have a usable hash by design (see
        # auth.upsert_external_user), so they always fall to the branch below instead.
        if not verify_password(payload.password, u["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid username or password")
    else:
        # No local account at all, or one already provisioned from LDAP: the only way in is a fresh directory
        # bind-as-user, so credential changes and account locks in LDAP take effect immediately rather than being
        # cached here. See ldap_auth._LOCAL_ACCOUNT_CONFLICT for why an existing *local* username is never reached
        # by this branch's auto-provisioning path.
        from web import ldap_auth
        ldap_user, reason = await asyncio.to_thread(ldap_auth.authenticate, payload.username, payload.password)
        if ldap_user is None:
            logger.info(f"LDAP login failed for '{payload.username}': {reason}")
            raise HTTPException(status_code=401, detail="Invalid username or password")
        u = ldap_user
    if u.get("is_active", 1) != 1:
        raise HTTPException(status_code=403, detail="Account is deactivated. Contact an administrator.")

    from web import mfa
    if mfa.is_enabled(u["id"]):
        # Password was right, but a second factor is required: no session yet, only a short-lived token that is
        # good for nothing except POST /api/auth/login/mfa.
        return JSONResponse(content={"success": False, "mfa_required": True, "mfa_token": mfa.create_mfa_token(u)})
    return _session_response(u)


def _session_response(u: Dict[str, Any]) -> JSONResponse:
    """Records the login and returns the session (cookie + user), shared by every password-based sign-in path."""
    record_user_login(u["id"])
    token = create_access_token(u)
    safe_user = {
        "id": u["id"],
        "username": u["username"],
        "display_name": u["display_name"],
        "full_name": u.get("display_name") or u["username"],
        "email": f"{u['username']}@localspark.lakehouse",
        "role": u["role"],
        "created_at": u["created_at"],
        "last_login_at": u["last_login_at"],
        "must_change_password": bool(u.get("must_change_password")),
        "auth_source": u.get("auth_source") or "local",
        "mfa_enabled": bool(u.get("mfa_enabled"))
    }
    resp = JSONResponse(content={"success": True, "token": token, "user": safe_user})
    resp.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=86400,
        httponly=True,
        samesite="lax",
        secure=False
    )
    return resp


class MfaLoginRequest(BaseModel):
    mfa_token: str
    code: str


class MfaCodeRequest(BaseModel):
    code: str
    password: Optional[str] = None


@app.post("/api/auth/login/mfa")
async def login_mfa_endpoint(payload: MfaLoginRequest):
    """Second step of a two-step sign-in: the mfa_token from /api/auth/login plus a TOTP or backup code."""
    from web import mfa
    u = mfa.user_from_mfa_token(payload.mfa_token)
    if u is None:
        raise HTTPException(status_code=401, detail="This sign-in expired. Please sign in again.")
    ok, reason = mfa.verify_login(u["id"], payload.code)
    if not ok:
        raise HTTPException(status_code=401 if "not valid" in reason else 429, detail=reason)
    return _session_response(u)


@app.get("/api/auth/mfa/status")
async def mfa_status(request: Request):
    from web import mfa
    user = await get_current_user(request)
    return mfa.status(user["id"])


@app.post("/api/auth/mfa/setup")
async def mfa_setup(request: Request):
    """Starts enrolment: returns the secret and otpauth:// URI for the authenticator app (nothing is enabled yet)."""
    from web import mfa
    user = await get_current_user(request)
    if (user.get("auth_source") or "local") == "oidc":
        raise HTTPException(status_code=403, detail="This account signs in through an external identity provider; use its two-factor settings.")
    try:
        return mfa.begin_setup(user["id"], user["username"])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/auth/mfa/enable")
async def mfa_enable(payload: MfaCodeRequest, request: Request):
    """Confirms enrolment with a code from the app; returns the backup codes, shown only this once."""
    from web import mfa
    user = await get_current_user(request)
    try:
        return {"success": True, "backup_codes": mfa.confirm_setup(user["id"], payload.code)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


async def _require_second_factor(request: Request, payload: MfaCodeRequest) -> Dict[str, Any]:
    """For sensitive MFA changes: a stolen session alone is not enough -- a current code (or backup code) is needed,
    plus the password for a local account."""
    from web import mfa
    from web.auth import verify_password
    user = await get_current_user(request)
    if not mfa.is_enabled(user["id"]):
        raise HTTPException(status_code=400, detail="Two-factor authentication is not enabled.")
    if (user.get("auth_source") or "local") == "local":
        full = get_user_by_username(user["username"], include_password_hash=True)
        if not payload.password or not verify_password(payload.password, full["password_hash"]):
            raise HTTPException(status_code=403, detail="The password is incorrect.")
    ok, reason = mfa.verify_login(user["id"], payload.code)
    if not ok:
        raise HTTPException(status_code=403, detail=reason)
    return user


@app.post("/api/auth/mfa/disable")
async def mfa_disable(payload: MfaCodeRequest, request: Request):
    from web import mfa
    user = await _require_second_factor(request, payload)
    mfa.disable(user["id"])
    return {"success": True}


@app.post("/api/auth/mfa/backup-codes")
async def mfa_regenerate_backup_codes(payload: MfaCodeRequest, request: Request):
    """Replaces all backup codes with a fresh set (the old ones stop working)."""
    from web import mfa
    user = await _require_second_factor(request, payload)
    return {"success": True, "backup_codes": mfa.regenerate_backup_codes(user["id"])}


@app.post("/api/users/{user_id}/mfa/reset")
async def admin_reset_mfa(user_id: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """A lost device: an administrator switches another user's two-factor authentication off so they can re-enrol."""
    from web import mfa
    if user_id == current_user["id"]:
        raise HTTPException(status_code=400, detail="Use 'Disable' in your own two-factor settings.")
    if not mfa.disable(user_id):
        raise HTTPException(status_code=404, detail="User not found")
    logger.warning(f"MFA reset for user {user_id} by admin {current_user['username']}")
    return {"success": True}


def _oidc_redirect_uri(cfg: Dict[str, Any], request: Request) -> str:
    """The registered redirect URI is authoritative (it must match the IdP's registration exactly); only when it
    is blank do we derive one from the request."""
    return (cfg.get("redirect_uri") or "").strip() or str(request.url_for("oidc_callback"))


@app.get("/api/auth/sso")
async def sso_providers():
    """Public: which single sign-on buttons the login screen should show (no secrets, no config detail)."""
    from web import oidc_auth, saml_auth
    cfg = auth_frameworks.load_raw_config().get("oidc", {})
    scfg = auth_frameworks.load_raw_config().get("saml", {})
    return {"oidc": {"enabled": oidc_auth.is_configured(cfg), "provider_name": (cfg.get("provider_name") or "SSO").strip()},
            "saml": {"enabled": saml_auth.is_configured(scfg), "provider_name": (scfg.get("provider_name") or "SAML SSO").strip()}}


def _sso_failure(message: str) -> RedirectResponse:
    from urllib.parse import quote
    resp = RedirectResponse(url=f"/?sso_error={quote(message)}", status_code=302)
    resp.delete_cookie("dkw_oidc", path="/api/auth/oidc")
    return resp


@app.get("/api/auth/oidc/login")
async def oidc_login(request: Request):
    """Starts the OpenID Connect authorization-code (PKCE) flow: redirects the browser to the identity provider."""
    from web import oidc_auth
    cfg = auth_frameworks.load_raw_config().get("oidc", {})
    if not oidc_auth.is_configured(cfg):
        return _sso_failure("OpenID Connect sign-in is not enabled.")
    redirect_uri = _oidc_redirect_uri(cfg, request)
    try:
        url, state_cookie = await asyncio.to_thread(oidc_auth.begin_login, cfg, redirect_uri)
    except oidc_auth.OidcError as exc:
        return _sso_failure(str(exc))
    resp = RedirectResponse(url=url, status_code=302)
    resp.set_cookie(oidc_auth.STATE_COOKIE, state_cookie, max_age=oidc_auth.STATE_TTL_SECONDS, httponly=True,
                    samesite="lax", secure=redirect_uri.startswith("https://"), path="/api/auth/oidc")
    return resp


@app.get("/api/auth/oidc/callback", name="oidc_callback")
async def oidc_callback(request: Request, code: str = "", state: str = "", error: str = "", error_description: str = ""):
    """The identity provider sends the browser back here: validate, provision the local account, start a session."""
    from web import oidc_auth
    cfg = auth_frameworks.load_raw_config().get("oidc", {})
    if error:
        logger.info(f"OIDC provider returned an error: {error} {error_description}")
        return _sso_failure("The identity provider did not complete the sign-in.")
    redirect_uri = _oidc_redirect_uri(cfg, request)
    try:
        u = await asyncio.to_thread(oidc_auth.complete_login, cfg, code, state,
                                    request.cookies.get(oidc_auth.STATE_COOKIE), redirect_uri)
    except oidc_auth.OidcError as exc:
        return _sso_failure(str(exc))
    record_user_login(u["id"])
    resp = RedirectResponse(url="/", status_code=302)
    resp.set_cookie(key=COOKIE_NAME, value=create_access_token(u), max_age=86400, httponly=True, samesite="lax",
                    secure=redirect_uri.startswith("https://"))
    resp.delete_cookie(oidc_auth.STATE_COOKIE, path="/api/auth/oidc")
    return resp


def _saml_base(cfg: Dict[str, Any], request: Request) -> str:
    return (cfg.get("sp_base_url") or "").strip().rstrip("/") or f"{request.url.scheme}://{request.url.netloc}"


@app.get("/api/auth/saml/login")
async def saml_login(request: Request):
    """Starts SAML sign-in: redirects the browser to the identity provider with an AuthnRequest."""
    from web import saml_auth
    cfg = auth_frameworks.load_raw_config().get("saml", {})
    base = _saml_base(cfg, request)
    try:
        url, browser = await asyncio.to_thread(saml_auth.begin_login, cfg, base, base.startswith("https://"))
    except saml_auth.SamlError as exc:
        return _sso_failure(str(exc))
    resp = RedirectResponse(url=url, status_code=302)
    if browser:   # the ACS is a cross-site POST: only a SameSite=None cookie reaches it, and that needs https
        resp.set_cookie(saml_auth.STATE_COOKIE, browser, max_age=saml_auth.STATE_TTL_SECONDS, httponly=True, samesite="none", secure=True, path="/api/auth/saml")
    return resp


@app.post("/api/auth/saml/acs")
async def saml_acs(request: Request):
    """The identity provider POSTs its signed response here: validate, provision the account, start a session."""
    from web import saml_auth
    cfg = auth_frameworks.load_raw_config().get("saml", {})
    base = _saml_base(cfg, request)
    form = await request.form()
    try:
        u = await asyncio.to_thread(saml_auth.complete_login, cfg, base, str(form.get("SAMLResponse") or ""), str(form.get("RelayState") or ""),
                                    request.cookies.get(saml_auth.STATE_COOKIE), base.startswith("https://"))
    except saml_auth.SamlError as exc:
        resp = _sso_failure(str(exc))
        resp.status_code = 303
        return resp
    record_user_login(u["id"])
    resp = RedirectResponse(url="/", status_code=303)             # 303: the browser must GET / after this POST
    resp.set_cookie(key=COOKIE_NAME, value=create_access_token(u), max_age=86400, httponly=True, samesite="lax", secure=base.startswith("https://"))
    resp.delete_cookie(saml_auth.STATE_COOKIE, path="/api/auth/saml")
    return resp


@app.get("/api/auth/saml/metadata")
async def saml_metadata(request: Request):
    """Service-provider metadata XML to hand to the identity provider's administrator (public; it holds no secret)."""
    from web import saml_auth
    from fastapi.responses import Response
    cfg = auth_frameworks.load_raw_config().get("saml", {})
    try:
        return Response(content=await asyncio.to_thread(saml_auth.metadata_xml, cfg, _saml_base(cfg, request)), media_type="application/samlmetadata+xml")
    except saml_auth.SamlError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


class SamlMetadataImport(BaseModel):
    url: Optional[str] = ""
    xml: Optional[str] = ""


@app.post("/api/auth/frameworks/saml/import-metadata")
async def saml_import_metadata_endpoint(payload: SamlMetadataImport, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Reads the IdP's entity id, SSO URL and signing certificate from its metadata (URL or pasted XML) for the settings form."""
    from web import saml_auth
    try:
        return await asyncio.to_thread(saml_auth.import_idp_metadata, payload.url or "", payload.xml or "")
    except saml_auth.SamlError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/auth/change-password")
async def change_password_endpoint(payload: PasswordChangeRequest, request: Request):
    """A signed-in user changes their own password (local accounts only; needs the current password)."""
    from web.auth import change_own_password
    user = await get_current_user(request)          # never resolve_principal: no credentials must not mean "local admin"
    try:
        change_own_password(user["id"], payload.current_password, payload.new_password)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    fresh = get_user_by_username(user["username"])
    resp = JSONResponse(content={"success": True, "message": "Password changed. Other sessions were signed out."})
    resp.set_cookie(key=COOKIE_NAME, value=create_access_token(fresh), max_age=86400, httponly=True, samesite="lax", secure=False)
    return resp


@app.post("/api/auth/logout")
async def logout_endpoint():
    resp = JSONResponse(content={"success": True, "message": "Successfully logged out"})
    resp.delete_cookie(COOKIE_NAME)
    return resp


@app.get("/api/auth/me")
async def get_current_user_profile(request: Request):
    try:
        current_user = await get_current_user(request)
        if current_user:
            if "full_name" not in current_user or not current_user["full_name"]:
                current_user["full_name"] = current_user.get("display_name") or current_user.get("username", "admin")
            if "email" not in current_user or not current_user["email"]:
                current_user["email"] = f"{current_user.get('username', 'admin')}@localspark.lakehouse"
            return {
                "authenticated": True,
                "user": current_user,
                "is_admin": current_user.get("role") == "admin",
                "is_power_user": current_user.get("role") in ("admin", "power_user")
            }
    except Exception:
        pass
    return {
        "authenticated": False,
        "user": None,
        "is_admin": False,
        "is_power_user": False
    }


# ==============================================================================
# USER MANAGEMENT & IAM (ADMIN ONLY)
# ==============================================================================

@app.get("/api/users")
async def get_users_endpoint(include_deleted: bool = False, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import groups
    membership = groups.memberships_by_user()
    return {"users": [{**u, "groups": membership.get(u.get("id"), [])} for u in list_users(include_deleted=include_deleted)]}


@app.post("/api/users")
async def create_user_endpoint(
    payload: UserCreateRequest,
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    try:
        name = payload.display_name or payload.full_name or payload.username
        new_u = create_user(
            username=payload.username,
            password=payload.password,
            display_name=name,
            role=payload.role
        )
        new_u["full_name"] = new_u.get("display_name") or new_u["username"]
        new_u["email"] = payload.email or f"{new_u['username']}@localspark.lakehouse"
        return {"success": True, "user": new_u}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/api/users/{user_id}")
async def update_user_endpoint(
    user_id: str,
    payload: UserUpdateRequest,
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    try:
        name = payload.display_name or payload.full_name
        updated = update_user(
            user_id,
            display_name=name,
            role=payload.role,
            is_active=payload.is_active
        )
        if not updated:
            raise HTTPException(status_code=404, detail="User not found")
        updated["full_name"] = updated.get("display_name") or updated["username"]
        updated["email"] = payload.email or f"{updated['username']}@localspark.lakehouse"
        return {"success": True, "user": updated}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/users/{user_id}/reset-password")
async def reset_password_endpoint(
    user_id: str,
    payload: PasswordResetRequest,
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    try:
        ok = reset_user_password(user_id, payload.new_password)
        if not ok:
            raise HTTPException(status_code=404, detail="User not found")
        return {"success": True, "message": "Password successfully reset"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/users/{user_id}")
async def delete_user_endpoint(
    user_id: str,
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    try:
        ok = delete_user(user_id)
        if not ok:
            raise HTTPException(status_code=404, detail="User not found")
        return {"success": True, "message": "User deleted (hidden from the list; restorable by an admin)"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/users/{user_id}/restore")
async def restore_user_endpoint(user_id: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    ok = restore_user(user_id)
    if not ok:
        raise HTTPException(status_code=404, detail="User not found")
    return {"success": True, "message": "User restored"}


# ==============================================================================
# APP SETTINGS ENDPOINTS (ADMIN ONLY)
# ==============================================================================

@app.get("/api/settings")
async def get_settings_endpoint(current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT key, value_json, updated_by, updated_at FROM app_settings").fetchall()
        settings = {}
        for r in rows:
            try:
                settings[r["key"]] = json.loads(r["value_json"])
            except Exception:
                settings[r["key"]] = r["value_json"]
        return {"settings": settings}
    finally:
        conn.close()


@app.post("/api/settings")
async def update_settings_endpoint(
    payload: Dict[str, Any],
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    conn = get_db_connection()
    try:
        now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with conn:
            for k, v in payload.items():
                val_json = json.dumps(v)
                conn.execute("""
                INSERT INTO app_settings (key, value_json, updated_by, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_by = excluded.updated_by,
                    updated_at = excluded.updated_at
                """, (k, val_json, current_user.get("username", "admin"), now_str))
        return {"success": True, "message": "Settings updated successfully"}
    finally:
        conn.close()


# ==============================================================================
# PLATFORM LLM ENDPOINTS & CLOUD API KEYS (ADMIN ONLY)
# ==============================================================================

class PlatformLlmPayload(BaseModel):
    ollama_host: Optional[str] = None
    lmstudio_host: Optional[str] = None
    openai_api_key: Optional[str] = None
    gemini_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    default_provider: Optional[str] = None
    default_model: Optional[str] = None
    auto_load_models: Optional[bool] = None

class PlatformLlmTestPayload(BaseModel):
    provider: str
    host: Optional[str] = None
    api_key: Optional[str] = None
    model: Optional[str] = None

@app.get("/api/settings/llm")
async def get_platform_llm_settings(current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Returns current platform LLM configuration with secrets masked (admin only)."""
    cfg = llm_settings.get_masked_llm_config()
    return {"success": True, "config": cfg}

@app.post("/api/settings/llm")
async def update_platform_llm_settings(
    payload: PlatformLlmPayload,
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    """Updates platform LLM configuration and syncs runtime environments (admin only)."""
    try:
        updated = llm_settings.save_llm_config(
            payload.dict(exclude_unset=True),
            updated_by=current_user.get("username", "admin")
        )
        return {"success": True, "config": updated, "message": "LLM endpoints and API keys saved successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/settings/llm/test")
async def test_platform_llm_connection(
    payload: PlatformLlmTestPayload,
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    """Tests connectivity and authentication for a specific LLM provider (admin only)."""
    result = llm_settings.test_llm_connection(
        provider=payload.provider,
        host=payload.host,
        api_key=payload.api_key,
        model=payload.model
    )
    return result

@app.post("/api/settings/llm/reset")
async def reset_platform_llm_settings(
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    """Resets platform LLM configuration to defaults (admin only)."""
    cfg = llm_settings.reset_llm_config(updated_by=current_user.get("username", "admin"))
    return {"success": True, "config": cfg, "message": "LLM settings reset to defaults successfully"}


# ==============================================================================
# AUTHENTICATION FRAMEWORKS & SSO (ADMIN ONLY)
# ==============================================================================

@app.get("/api/auth/frameworks/config")
async def get_auth_frameworks_config(current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Returns the current auth frameworks configuration with sensitive secrets masked."""
    return auth_frameworks.get_public_config()


@app.post("/api/auth/frameworks/config")
async def save_auth_frameworks_config(
    payload: Dict[str, Any],
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    """Saves authentication frameworks configuration, preserving masked secrets."""
    success = auth_frameworks.save_config(payload)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to save authentication configuration")
    return {"success": True, "message": "Authentication framework settings saved successfully"}


@app.post("/api/auth/frameworks/ldap/test")
async def test_ldap_endpoint(
    payload: Dict[str, Any],
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    """Tests LDAP server connectivity and TLS handshake."""
    result = auth_frameworks.test_ldap_connection(payload)
    return result


@app.post("/api/auth/frameworks/ldap/test-bind")
async def test_ldap_bind_endpoint(
    payload: Dict[str, Any],
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    """Beyond TCP/TLS: binds as the configured service account and runs one bounded search."""
    from web import ldap_auth
    # A masked bind_password ("••••••••") from the settings form means "unchanged": use what's saved.
    cfg = dict(payload)
    if cfg.get("bind_password") == "••••••••":
        cfg["bind_password"] = auth_frameworks.load_raw_config().get("ldap", {}).get("bind_password", "")
    return await asyncio.to_thread(ldap_auth.test_bind_and_search, cfg)


@app.post("/api/auth/frameworks/ldap/sync")
async def sync_ldap_users_endpoint(current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Re-resolves every LDAP-provisioned account's role and active status against the directory now."""
    from web import ldap_auth
    return await asyncio.to_thread(ldap_auth.sync_all)


@app.post("/api/auth/frameworks/oidc/test")
async def test_oidc_endpoint(
    payload: Dict[str, Any],
    current_user: Dict[str, Any] = Depends(require_role(["admin"]))
):
    """Tests OpenID Connect Discovery metadata from issuer."""
    result = auth_frameworks.test_oidc_discovery(payload)
    return result


# ==============================================================================
# CATALOG GOVERNANCE & PERMISSIONS
# ==============================================================================

@app.get("/api/catalogs")
async def get_catalogs(request: Request):
    current_user = await resolve_principal(request)

    conn = get_duckrun_conn()
    sync_catalogs_with_duckrun(conn)
    all_cats = scan_all_catalogs_and_tables(conn)
    filtered = filter_catalogs_for_user(all_cats.get("catalogs", []), current_user)
    return {
        "catalogs": filtered,
        "active_catalog": "warehouse",
        "current_user_role": current_user.get("role", "user")
    }


@app.post("/api/catalogs")
async def create_catalog_endpoint(
    payload: Dict[str, Any],
    request: Request
):
    current_user = await resolve_principal(request)

    if current_user.get("role") not in ("admin", "power_user"):
        raise HTTPException(
            status_code=403,
            detail=f"Access denied: role '{current_user.get('role')}' cannot create catalogs. Requires admin or power_user."
        )

    name = payload.get("name")
    cat_id = payload.get("id")
    if not name or not cat_id:
        raise HTTPException(status_code=400, detail="Catalog name and ID are required")
    desc = payload.get("description", "")
    try:
        new_cat = create_catalog(
            name=name,
            cat_id=cat_id,
            description=desc,
            owner=current_user.get("username", "admin")
        )
        conn = get_duckrun_conn()
        sync_catalogs_with_duckrun(conn)
        return new_cat
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/catalogs/{cat_id}")
async def delete_catalog_endpoint(cat_id: str, request: Request):
    current_user = await resolve_principal(request)

    if cat_id == "warehouse":
        raise HTTPException(
            status_code=400,
            detail="The default primary catalog 'warehouse' cannot be deleted."
        )

    if not can_user_delete_catalog(current_user, cat_id):
        role = current_user.get("role", "user")
        if role == "power_user":
            detail = f"Access denied: power users can only delete catalogs where they are the owner."
        elif role == "user":
            detail = f"Access denied: regular users cannot delete catalogs."
        else:
            detail = f"Access denied: only administrators or the catalog owner can delete catalog '{cat_id}'."
        raise HTTPException(status_code=403, detail=detail)

    ok = delete_catalog(cat_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Cannot delete default catalog or catalog not found")

    try:
        delete_all_catalog_permissions(cat_id)
        try:
            from web import groups as _groups
            _groups.delete_grants_prefix(("table", "schema"), f"{cat_id}.".lower())
        except Exception as e_grants:
            logger.warning(f"Could not remove table grants of catalog '{cat_id}': {e_grants}")
    except Exception as e:
        logger.warning(f"Failed cleaning permissions for deleted catalog {cat_id}: {e}")

    try:
        conn = get_duckrun_conn()
        sync_catalogs_with_duckrun(conn)
    except Exception as e:
        logger.warning(f"Failed syncing duckrun after catalog deletion: {e}")

    return {"success": True, "deleted_id": cat_id}


@app.get("/api/catalogs/{cat_id}/permissions")
async def get_catalog_permissions_endpoint(cat_id: str, request: Request):
    current_user = await resolve_principal(request)

    if not can_user_manage_catalog(current_user, cat_id):
        raise HTTPException(
            status_code=403,
            detail=f"Access denied: you do not have permission to inspect access lists for catalog '{cat_id}'."
        )
    return list_catalog_permissions(cat_id)


@app.post("/api/catalogs/{cat_id}/permissions")
async def grant_catalog_permission_endpoint(
    cat_id: str,
    payload: CatalogPermissionRequest,
    request: Request
):
    current_user = await resolve_principal(request)

    target = payload.user_id or payload.username
    if not target:
        raise HTTPException(status_code=400, detail="Target user_id or username is required")

    return grant_catalog_permission(
        catalog_id=cat_id,
        target_user_id=target,
        permission=payload.permission,
        granted_by_user=current_user
    )


@app.delete("/api/catalogs/{cat_id}/permissions/{target_user_id}")
async def revoke_catalog_permission_endpoint(
    cat_id: str,
    target_user_id: str,
    request: Request
):
    current_user = await resolve_principal(request)

    ok = revoke_catalog_permission(
        catalog_id=cat_id,
        target_user_id=target_user_id,
        revoked_by_user=current_user
    )
    return {"success": ok}

@app.post("/api/catalogs/{cat_id}/schemas")
async def create_catalog_schema_endpoint(cat_id: str, payload: Dict[str, Any]):
    schema_name = payload.get("schema_name")
    if not schema_name:
        raise HTTPException(status_code=400, detail="Schema name is required")
    try:
        path = create_catalog_schema(cat_id, schema_name)
        return {"success": True, "catalog": cat_id, "schema": schema_name, "path": path}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ==================== ONELAKE EXTERNAL CATALOGS ====================

class OneLakeMountRequest(BaseModel):
    workspace: str
    lakehouse: str
    tenant_id: str
    client_id: str
    client_secret: str
    catalog_id: Optional[str] = None

@app.post("/api/catalogs/onelake/mount")
async def mount_onelake_catalog_endpoint(payload: OneLakeMountRequest, request: Request):
    """Mount OneLake lakehouse as read-only external catalog."""
    current_user = await resolve_principal(request)

    # Only admins can mount external catalogs
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only administrators can mount external catalogs")

    try:
        catalog = onelake.mount_onelake_catalog(
            workspace=payload.workspace,
            lakehouse=payload.lakehouse,
            tenant_id=payload.tenant_id,
            client_id=payload.client_id,
            client_secret=payload.client_secret,
            catalog_id=payload.catalog_id
        )

        # List tables
        tables = catalog.list_tables()

        return {
            "success": True,
            "catalog_id": catalog.catalog_id,
            "workspace": catalog.workspace,
            "lakehouse": catalog.lakehouse,
            "type": "onelake",
            "read_only": True,
            "table_count": len(tables),
            "tables": [
                {
                    "name": table,
                    "catalog": catalog.catalog_id,
                    "source": "onelake",
                    "read_only": True
                }
                for table in tables
            ],
            "message": f"Successfully mounted OneLake catalog '{catalog.catalog_id}' with {len(tables)} tables"
        }

    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {str(e)}")
    except Exception as e:
        logger.exception("Failed to mount OneLake catalog")
        raise HTTPException(status_code=500, detail=f"Failed to mount OneLake catalog: {str(e)}")

@app.get("/api/catalogs/onelake")
async def list_onelake_catalogs_endpoint():
    """List all mounted OneLake catalogs."""
    try:
        catalogs = onelake.list_onelake_catalogs()

        # Add table lists
        result = []
        for cat in catalogs:
            catalog_obj = onelake.get_onelake_catalog(cat['catalog_id'])
            if catalog_obj:
                tables = catalog_obj.list_tables()
                cat['tables'] = tables
                cat['table_count'] = len(tables)
            result.append(cat)

        return {
            "catalogs": result,
            "count": len(result)
        }

    except Exception as e:
        logger.exception("Failed to list OneLake catalogs")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/catalogs/onelake/{catalog_id}")
async def get_onelake_catalog_endpoint(catalog_id: str):
    """Get details of a specific OneLake catalog."""
    catalog = onelake.get_onelake_catalog(catalog_id)

    if not catalog:
        raise HTTPException(status_code=404, detail=f"OneLake catalog '{catalog_id}' not found")

    try:
        tables = catalog.list_tables()

        return {
            "catalog_id": catalog.catalog_id,
            "workspace": catalog.workspace,
            "lakehouse": catalog.lakehouse,
            "type": "onelake",
            "read_only": True,
            "table_count": len(tables),
            "tables": tables,
            "base_url": catalog.base_url
        }

    except Exception as e:
        logger.exception(f"Failed to get OneLake catalog {catalog_id}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/catalogs/onelake/{catalog_id}/tables")
async def list_onelake_tables_endpoint(catalog_id: str, force_refresh: bool = False):
    """List tables in OneLake catalog."""
    catalog = onelake.get_onelake_catalog(catalog_id)

    if not catalog:
        raise HTTPException(status_code=404, detail=f"OneLake catalog '{catalog_id}' not found")

    try:
        tables = catalog.list_tables(force_refresh=force_refresh)

        return {
            "catalog_id": catalog_id,
            "tables": [
                {
                    "name": table,
                    "catalog": catalog_id,
                    "source": "onelake",
                    "read_only": True
                }
                for table in tables
            ],
            "count": len(tables)
        }

    except Exception as e:
        logger.exception(f"Failed to list tables for OneLake catalog {catalog_id}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/catalogs/onelake/{catalog_id}/tables/{table_name}")
async def get_onelake_table_metadata_endpoint(catalog_id: str, table_name: str):
    """Get metadata for a specific OneLake table."""
    catalog = onelake.get_onelake_catalog(catalog_id)

    if not catalog:
        raise HTTPException(status_code=404, detail=f"OneLake catalog '{catalog_id}' not found")

    try:
        metadata = catalog.get_table_metadata(table_name)
        return metadata

    except Exception as e:
        logger.exception(f"Failed to get metadata for {catalog_id}.{table_name}")
        raise HTTPException(status_code=500, detail=str(e))

class OneLakeQueryRequest(BaseModel):
    table_name: str
    sql: Optional[str] = None
    limit: Optional[int] = 100
    filters: Optional[List] = None

@app.post("/api/catalogs/onelake/{catalog_id}/query")
async def query_onelake_table_endpoint(catalog_id: str, payload: OneLakeQueryRequest, request: Request):
    """Query OneLake table."""
    onelake_user = await resolve_principal(request)
    if not can_user_access_catalog(onelake_user, catalog_id, action="READ"):
        raise HTTPException(status_code=403, detail=f"Access denied: you cannot query catalog '{catalog_id}'.")
    try:
        gov_gateway.deny_if_subject(onelake_user, "Direct OneLake queries")
    except GovernanceBlocked as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    catalog = onelake.get_onelake_catalog(catalog_id)

    if not catalog:
        raise HTTPException(status_code=404, detail=f"OneLake catalog '{catalog_id}' not found")

    try:
        if payload.sql:
            # Execute custom SQL query
            df = catalog.query_with_duckdb(payload.sql)
        else:
            # Simple table read
            df = catalog.read_table(
                table_name=payload.table_name,
                limit=payload.limit,
                filters=payload.filters
            )

        return {
            "catalog_id": catalog_id,
            "table": payload.table_name,
            "rows": len(df),
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient='records')
        }

    except Exception as e:
        logger.exception(f"Failed to query OneLake table {catalog_id}.{payload.table_name}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/catalogs/onelake/{catalog_id}")
async def unmount_onelake_catalog_endpoint(catalog_id: str, request: Request):
    """Unmount OneLake catalog."""
    current_user = await resolve_principal(request)

    # Only admins can unmount external catalogs
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only administrators can unmount external catalogs")

    success = onelake.unmount_onelake_catalog(catalog_id)

    if not success:
        raise HTTPException(status_code=404, detail=f"OneLake catalog '{catalog_id}' not found")

    return {
        "success": True,
        "catalog_id": catalog_id,
        "message": f"OneLake catalog '{catalog_id}' unmounted successfully"
    }

@app.post("/api/catalogs/onelake/{catalog_id}/test")
async def test_onelake_connection_endpoint(catalog_id: str):
    """Test OneLake catalog connection."""
    catalog = onelake.get_onelake_catalog(catalog_id)

    if not catalog:
        raise HTTPException(status_code=404, detail=f"OneLake catalog '{catalog_id}' not found")

    try:
        success = catalog.test_connection()

        return {
            "catalog_id": catalog_id,
            "connected": success,
            "message": "Connection successful" if success else "Connection failed"
        }

    except Exception as e:
        return {
            "catalog_id": catalog_id,
            "connected": False,
            "error": str(e)
        }

# ==================== SQL WAREHOUSES (COMPUTE) APIS ====================

@app.get("/api/cluster/nodes")
async def list_cluster_nodes():
    nodes = get_compute_nodes_status()
    online_cnt = sum(1 for n in nodes if n.get("status") == "ONLINE")
    return {
        "success": True,
        "nodes": nodes,
        "total_nodes": len(nodes),
        "online_nodes": online_cnt
    }

@app.get("/api/sql-warehouses")
async def list_sql_warehouses():
    from web import container_control, warehouse_lifecycle
    warehouses = load_sql_warehouses()
    ray_status = ray_manager.get_status() if RAY_INSTALLED else {"available": False}
    control_ok = await asyncio.to_thread(container_control.available)
    for w in warehouses:
        wh_id = w.get("id")
        active_pool = ray_manager.actor_pools.get(wh_id, []) if RAY_INSTALLED else []
        w["active_ray_workers"] = len(active_pool)
        if len(active_pool) > 0:
            w["ray_status"] = "RUNNING"
        else:
            w["ray_status"] = "IDLE" if w.get("state") == "RUNNING" else "STOPPED"
        w["container"] = await asyncio.to_thread(warehouse_lifecycle.describe, w) if control_ok else {"controllable": False}
    return {
        "warehouses": warehouses,
        "cluster_sizes": CLUSTER_SIZES,
        "ray_telemetry": ray_status,
        "container_control": {"available": control_ok, "configured": container_control.configured(), "mode": container_control.suspend_mode()}
    }

@app.post("/api/sql-warehouses")
async def create_sql_warehouse_endpoint(payload: Dict[str, Any]):
    name = payload.get("name")
    if not name or not name.strip():
        raise HTTPException(status_code=400, detail="Warehouse name is required")
    cs = payload.get("cluster_size", "Small")
    threads = payload.get("threads")
    max_memory = payload.get("max_memory")
    auto_stop = payload.get("auto_stop_mins", 10)
    is_def = payload.get("is_default", False)
    endpoint = payload.get("endpoint")
    ray_workers = payload.get("ray_workers", 1)
    
    wh = create_sql_warehouse(
        name=name,
        cluster_size=cs,
        threads=threads,
        max_memory=max_memory,
        auto_stop_mins=auto_stop,
        is_default=is_def,
        endpoint=endpoint,
        ray_workers=ray_workers
    )
    return wh

@app.get("/api/sql-warehouses/{wh_id}")
async def get_sql_warehouse_endpoint(wh_id: str):
    wh = get_sql_warehouse(wh_id)
    if not wh:
        raise HTTPException(status_code=404, detail="Warehouse not found")
    active_pool = ray_manager.actor_pools.get(wh_id, []) if RAY_INSTALLED else []
    wh["active_ray_workers"] = len(active_pool)
    return wh

@app.put("/api/sql-warehouses/{wh_id}")
async def update_sql_warehouse_endpoint(wh_id: str, payload: Dict[str, Any]):
    wh = update_sql_warehouse(wh_id, payload)
    if not wh:
        raise HTTPException(status_code=404, detail="Warehouse not found")
    return wh

@app.post("/api/sql-warehouses/{wh_id}/start")
async def start_sql_warehouse_endpoint(wh_id: str, current_user: Dict[str, Any] = Depends(require_role(["admin", "power_user"]))):
    """Resumes a warehouse: for a managed one this really starts (or unpauses) its compute-node container and waits for it."""
    from web import warehouse_lifecycle
    res = await asyncio.to_thread(warehouse_lifecycle.resume, wh_id)
    if not res.get("ok"):
        raise HTTPException(status_code=404 if res.get("error") == "Warehouse not found" else 502, detail=res.get("error"))
    wh = res["warehouse"]
    if RAY_INSTALLED and wh.get("ray_workers", 0) > 0:
        try:
            ray_manager.scale_warehouse(wh_id, wh.get("ray_workers", 1))
        except Exception as e:
            logger.warning(f"Could not autoscale Ray pool for {wh_id}: {e}")
    active_pool = ray_manager.actor_pools.get(wh_id, []) if RAY_INSTALLED else []
    wh["active_ray_workers"] = len(active_pool)
    return {"success": True, "warehouse": wh, "container": res.get("container", False), "resume_ms": res.get("resume_ms"), "warning": res.get("warning")}

@app.post("/api/sql-warehouses/{wh_id}/stop")
async def stop_sql_warehouse_endpoint(wh_id: str, current_user: Dict[str, Any] = Depends(require_role(["admin", "power_user"]))):
    """Suspends a warehouse: for a managed one this really stops (or pauses) its compute-node container."""
    from web import warehouse_lifecycle
    res = await asyncio.to_thread(warehouse_lifecycle.suspend, wh_id, "manual")
    if not res.get("ok"):
        raise HTTPException(status_code=404 if res.get("error") == "Warehouse not found" else 502, detail=res.get("error"))
    wh = res["warehouse"]
    wh["active_ray_workers"] = 0
    return {"success": True, "warehouse": wh, "container": res.get("container", False)}

@app.delete("/api/sql-warehouses/{wh_id}")
async def delete_sql_warehouse_endpoint(wh_id: str):
    if RAY_INSTALLED:
        try:
            ray_manager.scale_warehouse(wh_id, 0)
        except Exception:
            pass
    ok = delete_sql_warehouse(wh_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Cannot delete default warehouse or warehouse not found")
    return {"success": True, "deleted_id": wh_id}

# ==================== RAY COMPUTE ENGINE & DISTRIBUTED SCALING ====================

@app.get("/api/compute/ray/status")
async def get_ray_cluster_status():
    """Returns Ray cluster health, physical/virtual resources, and active actor pools."""
    return ray_manager.get_status()

@app.post("/api/compute/ray/start")
async def start_ray_cluster_endpoint(payload: Optional[Dict[str, Any]] = None):
    """Initializes or connects to Ray cluster."""
    num_cpus = payload.get("num_cpus") if payload else None
    success = ray_manager.initialize_ray(num_cpus=num_cpus)
    status = ray_manager.get_status()
    return {"success": success, "status": status}

@app.post("/api/compute/ray/stop")
async def stop_ray_cluster_endpoint():
    """Stops all warehouse worker actors and shuts down the Ray cluster."""
    if not RAY_INSTALLED:
        return {"success": False, "error": "Ray library not installed"}
    try:
        import ray
        for wh_id in list(ray_manager.actor_pools.keys()):
            ray_manager.scale_warehouse(wh_id, 0)
        if ray.is_initialized():
            ray.shutdown()
        return {"success": True, "message": "Ray cluster and all actor pools stopped"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/compute/warehouses/{wh_id}/scale")
async def scale_warehouse_endpoint(wh_id: str, payload: Dict[str, Any]):
    """Dynamically scales the Ray DuckDBWorkerActor pool for a warehouse on the fly."""
    target_workers = payload.get("target_workers")
    if target_workers is None:
        raise HTTPException(status_code=400, detail="target_workers parameter is required")
    try:
        target_workers = int(target_workers)
    except ValueError:
        raise HTTPException(status_code=400, detail="target_workers must be an integer")

    wh = get_sql_warehouse(wh_id)
    if not wh:
        raise HTTPException(status_code=404, detail=f"Warehouse '{wh_id}' not found")

    res = ray_manager.scale_warehouse(
        warehouse_id=wh_id,
        target_workers=target_workers,
        max_memory=wh.get("max_memory", "2GB"),
        threads=wh.get("threads", 2)
    )
    update_sql_warehouse(wh_id, {"ray_workers": target_workers})
    wh_updated = get_sql_warehouse(wh_id)
    if wh_updated:
        active_pool = ray_manager.actor_pools.get(wh_id, [])
        wh_updated["active_ray_workers"] = len(active_pool)
    res["warehouse"] = wh_updated
    return res

@app.post("/api/compute/warehouses/{wh_id}/distributed-query")
async def execute_distributed_delta_query(wh_id: str, payload: Dict[str, Any], request: Request):
    """Executes a distributed Map-Reduce query across Delta Lake Parquet partitions using Ray tasks."""
    scan_user = await resolve_principal(request)
    try:
        gov_gateway.deny_if_subject(scan_user, "Distributed Delta scans")
    except GovernanceBlocked as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    table_path = payload.get("table_path")
    select_clause = payload.get("select", "*")
    where_clause = payload.get("where", "")

    if not table_path:
        raise HTTPException(status_code=400, detail="table_path parameter is required")

    if not os.path.isabs(table_path):
        table_path = os.path.join(WAREHOUSE_DIR, table_path)
    if scan_user.get("role") != "admin":
        from web.governance.enforce import resolve_path
        if resolve_path(table_path, "warehouse").kind != "table":
            raise HTTPException(status_code=403, detail="Only warehouse tables can be scanned.")

    res = ray_manager.execute_distributed_delta_scan(
        warehouse_id=wh_id,
        table_path=table_path,
        select_clause=select_clause,
        where_clause=where_clause
    )
    return res

# ==============================================================================
# DELTA SHALLOW CLONE
# ==============================================================================

class TableClonePayload(BaseModel):
    target_table: str
    target_schema: Optional[str] = None       # default: the source's schema
    target_catalog: Optional[str] = None      # default: the source's catalog
    catalog: Optional[str] = "warehouse"      # the source's catalog
    version: Optional[int] = None
    timestamp: Optional[str] = None
    replace: bool = False
    if_not_exists: bool = False


def _clone_table_dir(catalog: str, schema: str, table: str, *, must_exist: bool) -> str:
    """Local directory of catalog.schema.table for a clone endpoint (mounted/S3 catalogs are not supported)."""
    if catalog == "warehouse":
        schema_dir = os.path.join(WAREHOUSE_DIR, schema)
    else:
        cat = get_catalog(catalog)
        if not cat:
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog}' not found")
        if cat.get("is_mounted") or not cat.get("path"):
            raise HTTPException(status_code=400, detail=f"Catalog '{catalog}' is external storage; shallow clone works between local catalogs only.")
        schema_dir = os.path.join(cat["path"], schema)
    if not os.path.isdir(schema_dir):
        raise HTTPException(status_code=404, detail=f"Schema '{schema}' does not exist in catalog '{catalog}'.")
    path = os.path.join(schema_dir, table)
    if must_exist and not os.path.isdir(path):
        raise HTTPException(status_code=404, detail=f"Table {catalog}.{schema}.{table} not found")
    return path


def _copy_table_tags(src: tuple, dst: tuple, actor: str) -> set:
    """Governance: the clone shares the source's raw files, so it must carry the source's *effective* tags (including
    ones inherited from its schema/catalog, which the clone may not inherit) before it exists. Returns what was set."""
    from web.governance import tags as gov_tags
    if not gov_tags.table_has_any_tags(*src):
        return set()
    desired = set()
    for key, info in gov_tags.effective_table_tags(*src).items():
        gov_tags.set_tag(catalog=dst[0], schema_name=dst[1], table_name=dst[2], tag_key=key, tag_value=info["value"], actor=actor, source="clone")
        desired.add((key, ""))
    from deltalake import DeltaTable
    cols = [f.name for f in DeltaTable(_clone_table_dir(*src, must_exist=True)).schema().fields]
    for col, tag_map in gov_tags.effective_tags(*src, cols).items():
        for key, info in tag_map.items():
            if info["level"] == "column":
                gov_tags.set_tag(catalog=dst[0], schema_name=dst[1], table_name=dst[2], column_name=col, tag_key=key,
                                 tag_value=info["value"], actor=actor, source="clone")
                desired.add((key, col))
    return desired


def _do_shallow_clone(user: Dict[str, Any], src: tuple, dst: tuple, *, version=None, timestamp=None, replace=False,
                      if_not_exists=False) -> Dict[str, Any]:
    """Synchronous; run in a thread. src/dst are (catalog, schema, table). Raises HTTPException."""
    from web import table_clone
    from web.governance import tags as gov_tags
    actor = user.get("username", "admin")
    from web import table_access
    if not table_access.can_access_table(user, src[0], src[1], src[2], "READ"):
        raise HTTPException(status_code=403, detail=f"Access denied: you cannot read '{src[0]}.{src[1]}.{src[2]}'.")
    if not can_user_access_catalog(user, dst[0], action="WRITE"):
        raise HTTPException(status_code=403, detail=f"Access denied: you cannot modify catalog '{dst[0]}'.")
    try:
        # A clone shares the source's raw files, so it would hand a masked / row-filtered user the unmasked data.
        gov_gateway.deny_if_subject(user, "Cloning a table")
    except GovernanceBlocked as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    src_dir = _clone_table_dir(*src, must_exist=True)
    dst_dir = _clone_table_dir(*dst, must_exist=False)
    existed = os.path.exists(dst_dir)
    if existed and if_not_exists:
        return {"created": False, "message": "Target already exists; nothing to do (IF NOT EXISTS)."}
    try:
        desired = _copy_table_tags(src, dst, actor)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not copy the source's governance tags, so no clone was made: {exc}")
    try:
        result = table_clone.shallow_clone(src_dir, dst_dir, version=version, timestamp=timestamp, replace=replace,
                                           if_not_exists=if_not_exists, source_label=".".join(src), actor=actor)
    except table_clone.CloneError as exc:
        if not existed:
            gov_tags.drop_object(*dst, actor=actor)
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        if not existed:
            gov_tags.drop_object(*dst, actor=actor)
        raise
    if existed:                                     # replaced: drop tags the old table had that the new one doesn't
        try:
            for a in gov_tags.list_assignments(catalog=dst[0]):
                if a.get("schema_name") == dst[1] and a.get("table_name") == dst[2] and (a["tag_key"], a.get("column_name") or "") not in desired:
                    gov_tags.unset_tag(catalog=dst[0], schema_name=dst[1], table_name=dst[2], column_name=a.get("column_name") or "",
                                       tag_key=a["tag_key"], actor=actor)
        except Exception as exc:
            logger.warning(f"Could not prune stale tags of replaced table {'.'.join(dst)}: {exc}")
    try:
        get_duckrun_conn().refresh()
    except Exception:
        pass
    try:
        from web.lineage import make_table_id, upsert_node, upsert_edge
        sid, did = make_table_id(*src), make_table_id(*dst)
        upsert_node(sid, src[2], "TABLE", catalog=src[0], schema_name=src[1])
        upsert_node(did, dst[2], "TABLE", catalog=dst[0], schema_name=dst[1])
        upsert_edge(sid, did, edge_type="TRANSFORMS_TO", query_text=f"CREATE TABLE {'.'.join(dst)} SHALLOW CLONE {'.'.join(src)}")
    except Exception as exc:
        logger.warning(f"Could not record clone lineage: {exc}")
    result["source"], result["target"] = ".".join(src), ".".join(dst)
    return result


@app.post("/api/table/{schema_name}/{table_name}/clone")
async def clone_table_api(schema_name: str, table_name: str, payload: TableClonePayload, request: Request):
    """Delta shallow clone: a new table sharing the source's data files (hard links), optionally at a past version."""
    user = await resolve_principal(request)
    src_cat = payload.catalog or "warehouse"
    src = (src_cat, sanitize_identifier(schema_name), sanitize_identifier(table_name))
    dst = (payload.target_catalog or src_cat, sanitize_identifier(payload.target_schema or schema_name), sanitize_identifier(payload.target_table))
    return await asyncio.to_thread(_do_shallow_clone, user, src, dst, version=payload.version, timestamp=payload.timestamp,
                                   replace=payload.replace, if_not_exists=payload.if_not_exists)


async def _execute_clone_statement(payload, query: str, stmt: Dict[str, Any], user: Dict[str, Any]) -> Dict[str, Any]:
    """`CREATE [OR REPLACE] TABLE t SHALLOW CLONE src [VERSION|TIMESTAMP AS OF ...]` from the SQL editor."""
    start = time.perf_counter()
    default_cat = payload.catalog or "warehouse"

    def locate(parts):
        if len(parts) == 3:
            return (parts[0], sanitize_identifier(parts[1]), sanitize_identifier(parts[2]))
        if len(parts) == 2:
            return (default_cat, sanitize_identifier(parts[0]), sanitize_identifier(parts[1]))
        raise HTTPException(status_code=400, detail="Name the schema too: schema.table or catalog.schema.table.")

    try:
        src, dst = locate(stmt["source"]), locate(stmt["target"])
        result = await asyncio.to_thread(_do_shallow_clone, user, src, dst, version=stmt["version"], timestamp=stmt["timestamp"],
                                         replace=stmt["replace"], if_not_exists=stmt["if_not_exists"])
        ok, message = True, result["message"]
    except HTTPException as exc:
        ok, message = False, str(exc.detail)
    elapsed = round((time.perf_counter() - start) * 1000, 2)
    wh_id = payload.warehouse_id or "wh_starter"
    qid = log_query(query_text=query, duration_ms=elapsed, rows_produced=0, status="SUCCESS" if ok else "FAILED",
                    error_message=None if ok else message, client="SQL_EDITOR", is_mutation=True, warehouse_id=wh_id,
                    catalog=default_cat, user=user.get("username", "admin"), executed_by="Studio (shallow clone)")
    if not ok:
        return {"success": False, "query_id": qid, "error": message, "elapsed_ms": elapsed, "warehouse_id": wh_id}
    return {"success": True, "query_id": qid, "is_mutation": True, "message": message, "elapsed_ms": elapsed, "row_count": 0,
            "warehouse_id": wh_id, "executed_by": "Studio (shallow clone)"}


async def _execute_grant_statement(payload, query: str, stmt: Dict[str, Any], user: Dict[str, Any]) -> Dict[str, Any]:
    """`GRANT ... ON ... TO ...`, `REVOKE ... ON ... FROM ...` and `SHOW GRANTS` from the SQL editor."""
    from web import sql_grants
    start = time.perf_counter()
    default_cat = payload.catalog or "warehouse"
    wh_id = payload.warehouse_id or "wh_starter"
    is_show = stmt["op"] == "show"
    try:
        result = await asyncio.to_thread(sql_grants.run, stmt, query, user, default_cat)
        ok, message = True, result.get("message", "")
    except (sql_grants.GrantSqlError, HTTPException) as exc:
        ok, message, result = False, str(getattr(exc, "detail", exc)), {}
    elapsed = round((time.perf_counter() - start) * 1000, 2)
    qid = log_query(query_text=query, duration_ms=elapsed, rows_produced=result.get("row_count", 0) if ok else 0, status="SUCCESS" if ok else "FAILED",
                    error_message=None if ok else message, client="SQL_EDITOR", is_mutation=not is_show, warehouse_id=wh_id,
                    catalog=default_cat, user=user.get("username", "admin"), executed_by="Studio (access management)")
    if not ok:
        return {"success": False, "query_id": qid, "error": message, "elapsed_ms": elapsed, "warehouse_id": wh_id}
    if is_show:
        names = [c["name"] for c in result["columns"]]          # the grid reads row[column name]
        return {"success": True, "query_id": qid, "is_mutation": False, "masked_columns": [], "columns": result["columns"], "rows": [dict(zip(names, r)) for r in result["rows"]],
                "row_count": result["row_count"], "elapsed_ms": elapsed, "warehouse_id": wh_id, "executed_by": "Studio (access management)"}
    return {"success": True, "query_id": qid, "is_mutation": True, "message": message, "elapsed_ms": elapsed, "row_count": 0,
            "warehouse_id": wh_id, "executed_by": "Studio (access management)"}


@app.delete("/api/table/{schema_name}/{table_name}")
async def drop_table_api(schema_name: str, table_name: str, request: Request, catalog: Optional[str] = "warehouse"):
    schema_clean = sanitize_identifier(schema_name)
    table_clean = sanitize_identifier(table_name)
    target_catalog = catalog or "warehouse"
    drop_user = await resolve_principal(request)
    if not can_user_access_catalog(drop_user, target_catalog, action="WRITE"):
        raise HTTPException(status_code=403, detail=f"Access denied: you cannot modify catalog '{target_catalog}'.")

    conn = get_duckrun_conn()
    if target_catalog != "warehouse":
        cat = get_catalog(target_catalog)
        if not cat:
            raise HTTPException(status_code=404, detail=f"Catalog '{target_catalog}' not found")
        table_ref = f"{cat['id']}.{schema_clean}.{table_clean}"
        dt_path = os.path.join(cat["path"], schema_clean, table_clean)
    else:
        table_ref = f"{schema_clean}.{table_clean}"
        dt_path = os.path.join(WAREHOUSE_DIR, schema_clean, table_clean)

    try:
        conn.sql(f"DROP TABLE IF EXISTS {table_ref}")
        conn.refresh()
    except Exception as e:
        logger.warning(f"Error executing DROP TABLE {table_ref}: {e}")

    try:
        if os.path.exists(dt_path):
            shutil.rmtree(dt_path)
            logger.info(f"Purged dropped table directory: {dt_path}")
    except Exception as e:
        logger.warning(f"Error removing dropped table directory {dt_path}: {e}")

    try:
        from web import groups as _groups
        _groups.delete_grants_prefix(("table",), f"{target_catalog}.{schema_clean}.{table_clean}".lower())   # a new table of the same name must not inherit access
    except Exception as e_grants:
        logger.warning(f"Could not remove table grants of {table_ref}: {e_grants}")

    try:
        from web.governance import tags as gov_tags
        removed = gov_tags.drop_object(target_catalog, schema_clean, table_clean, actor=drop_user.get("username", "system"))
        if removed:
            logger.info(f"Removed {removed} governance tag(s) of dropped table {table_ref}")
    except Exception as e_tags:
        logger.warning(f"Could not clean governance tags of {table_ref}: {e_tags}")

    return {
        "success": True,
        "message": f"Successfully dropped table {table_ref}"
    }

@app.get("/api/table/{schema_name}/{table_name}")
async def get_table_details(schema_name: str, table_name: str, catalog: Optional[str] = None, request: Request = None):
    cat_id = catalog or "warehouse"
    current_user = await resolve_principal(request)
    from web import table_access
    if not table_access.can_access_table(current_user, cat_id, schema_name, table_name, "READ"):
        raise HTTPException(status_code=403, detail=f"Access denied: User '{current_user.get('username')}' cannot view '{cat_id}.{schema_name}.{table_name}'.")
    # Check if catalog is an external storage mount
    from web.mounts import load_mounts
    mounts = {m["catalog_name"]: m for m in load_mounts() if m.get("enabled", True)}
    if catalog in mounts:
        m = mounts[catalog]
        conn = get_duckrun_conn()
        sync_catalogs_with_duckrun(conn)
        raw_conn = getattr(conn, "con", conn)
        try:
            is_delta = False
            target_s3_path = None
            history = []
            version = 1

            if m["type"] == "s3":
                bucket = m["config"].get("bucket", "localspark")
                s3_uri_schema = f"s3://{bucket}/{schema_name}/{table_name}"
                s3_uri_flat = f"s3://{bucket}/{table_name}"

                try:
                    delta_check = raw_conn.execute(f"SELECT file FROM glob('{s3_uri_schema}/_delta_log/0*.json') LIMIT 1").fetchall()
                    if delta_check:
                        is_delta = True
                        target_s3_path = s3_uri_schema
                    else:
                        delta_check_flat = raw_conn.execute(f"SELECT file FROM glob('{s3_uri_flat}/_delta_log/0*.json') LIMIT 1").fetchall()
                        if delta_check_flat:
                            is_delta = True
                            target_s3_path = s3_uri_flat
                except Exception:
                    pass

                if is_delta and target_s3_path:
                    query_target = f"delta_scan('{target_s3_path}')"
                    location = target_s3_path
                    try:
                        from web.mounts import get_s3_storage_options
                        storage_options = get_s3_storage_options(m["config"])
                        dt_s3 = DeltaTable(target_s3_path, storage_options=storage_options)
                        version = dt_s3.version()
                        for h in dt_s3.history():
                            ts = h.get("timestamp", 0)
                            iso_ts = datetime.datetime.fromtimestamp(ts / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if ts else ""
                            history.append({
                                "version": h.get("version", 0),
                                "timestamp": ts,
                                "timestamp_iso": iso_ts,
                                "operation": h.get("operation", "UNKNOWN"),
                                "operationParameters": h.get("operationParameters", {}),
                                "operationMetrics": h.get("operationMetrics", {}),
                                "clientVersion": h.get("clientVersion", ""),
                                "engineInfo": h.get("engineInfo", "")
                            })
                    except Exception as e:
                        logger.debug(f"Could not load Delta history for S3 table: {e}")
                else:
                    query_target = f"read_parquet('s3://{bucket}/{table_name}')"
                    location = f"s3://{bucket}/{table_name}"
            else:
                query_target = f"{catalog}.{schema_name}.{table_name}"
                location = f"{catalog}.{schema_name}.{table_name}"

            desc_rows = raw_conn.execute(f"DESCRIBE SELECT * FROM {query_target} LIMIT 0").fetchall()
            columns = [{"name": r[0], "type": str(r[1]).upper(), "nullable": True, "metadata": {}} for r in desc_rows]

            row_count = 0
            try:
                row_count = raw_conn.execute(f"SELECT COUNT(*) FROM {query_target}").fetchone()[0]
            except Exception:
                pass

            is_partitioned = False
            is_child_partition = False
            parent_table = None
            child_partitions = []

            if m["type"] == "postgres":
                try:
                    q_part = f"""
                    SELECT c.relkind, c.relispartition, p.relname AS parent_name
                    FROM {catalog}.pg_catalog.pg_class c
                    JOIN {catalog}.pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
                    LEFT JOIN {catalog}.pg_catalog.pg_inherits i ON (i.inhrelid = c.oid)
                    LEFT JOIN {catalog}.pg_catalog.pg_class p ON (i.inhparent = p.oid)
                    WHERE n.nspname = '{schema_name}' AND c.relname = '{table_name}';
                    """
                    part_info = raw_conn.execute(q_part).fetchone()
                    if part_info:
                        relkind, relispartition, p_name = part_info
                        is_partitioned = (relkind == 'p')
                        is_child_partition = bool(relispartition)
                        parent_table = p_name

                        if is_partitioned:
                            q_children = f"""
                            SELECT c.relname
                            FROM {catalog}.pg_catalog.pg_inherits i
                            JOIN {catalog}.pg_catalog.pg_class c ON (i.inhrelid = c.oid)
                            JOIN {catalog}.pg_catalog.pg_class p ON (i.inhparent = p.oid)
                            JOIN {catalog}.pg_catalog.pg_namespace n ON (p.relnamespace = n.oid)
                            WHERE n.nspname = '{schema_name}' AND p.relname = '{table_name}'
                            ORDER BY c.relname;
                            """
                            child_partitions = [r[0] for r in raw_conn.execute(q_children).fetchall()]
                except Exception as e_part:
                    logger.debug(f"Could not inspect postgres partition status: {e_part}")

            return {
                "catalog": catalog,
                "schema_name": schema_name,
                "table_name": table_name,
                "full_name": f"{catalog}.{schema_name}.{table_name}",
                "location": location,
                "format": "delta" if is_delta else ("parquet" if m["type"] == "s3" else m["type"]),
                "is_delta": is_delta,
                "columns": columns,
                "column_count": len(columns),
                "version": version,
                "history": history,
                "num_files": len(history) if is_delta else (1 if m["type"] == "s3" else 0),
                "size_bytes": 0,
                "is_federated": True,
                "is_partitioned": is_partitioned,
                "is_child_partition": is_child_partition,
                "parent_table": parent_table,
                "child_partitions": child_partitions,
                "partition_count": len(child_partitions),
                "mount_type": m["type"],
                "mount_name": m["name"],
                "row_count": row_count,
                "properties": {
                    "Federation Type": f"Zero-Copy {m['type'].upper()} Mount",
                    "Mount Name": m["name"],
                    "Storage Location": location,
                    "Format": "Delta Lake (ACID)" if is_delta else ("Parquet" if m["type"] == "s3" else ("PostgreSQL Declarative Partitioned" if is_partitioned else m["type"].upper())),
                    "Attached Catalog": catalog
                }
            }
        except Exception as e:
            logger.error(f"Error inspecting federated table: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to inspect federated table: {str(e)}")

    target_path = None
    if catalog == "warehouse":
        cand = os.path.join(WAREHOUSE_DIR, schema_name, table_name)
        if os.path.exists(cand):
            target_path = cand
        else:
            alt = os.path.join(WAREHOUSE_DIR, table_name)
            if os.path.exists(alt):
                target_path = alt
    elif catalog:
        cat = get_catalog(catalog)
        if cat:
            cand = os.path.join(cat["path"], schema_name, table_name)
            if os.path.exists(cand):
                target_path = cand
            else:
                alt = os.path.join(cat["path"], table_name)
                if os.path.exists(alt):
                    target_path = alt
    else:
        cand = os.path.join(WAREHOUSE_DIR, schema_name, table_name)
        if os.path.exists(cand):
            target_path = cand
            catalog = "warehouse"
        else:
            alt = os.path.join(WAREHOUSE_DIR, table_name)
            if os.path.exists(alt):
                target_path = alt
                catalog = "warehouse"
            else:
                for c in load_catalogs():
                    p = os.path.join(c["path"], schema_name, table_name)
                    if os.path.exists(p):
                        target_path = p
                        catalog = c["id"]
                        break

    if not target_path or not os.path.exists(target_path):
        raise HTTPException(status_code=404, detail=f"Table {schema_name}.{table_name} not found in catalog '{catalog or 'warehouse'}'")

    try:
        dt = DeltaTable(target_path)
        fields = [f.name for f in dt.schema().fields]
        if fields == ["__duckrun_deleted__"] or "__duckrun_deleted__" in fields:
            raise HTTPException(status_code=404, detail=f"Table {schema_name}.{table_name} has been dropped")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open Delta table: {str(e)}")

    columns = []
    try:
        dt_schema = dt.schema()
        for field in dt_schema.fields:
            type_str = str(field.type)
            columns.append({
                "name": field.name,
                "type": type_str,
                "nullable": field.nullable,
                "metadata": field.metadata
            })
    except Exception as e:
        logger.warning(f"Error reading schema: {e}")

    history = []
    try:
        raw_history = dt.history()
        for h in raw_history:
            ts = h.get("timestamp", 0)
            iso_ts = datetime.datetime.fromtimestamp(ts / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if ts else ""
            history.append({
                "version": h.get("version", 0),
                "timestamp": ts,
                "timestamp_iso": iso_ts,
                "operation": h.get("operation", "UNKNOWN"),
                "operationParameters": h.get("operationParameters", {}),
                "operationMetrics": h.get("operationMetrics", {}),
                "clientVersion": h.get("clientVersion", ""),
                "engineInfo": h.get("engineInfo", "")
            })
    except Exception as e:
        logger.warning(f"Error reading history: {e}")

    files = dt.file_uris()
    total_size = sum(os.path.getsize(f) for f in files if os.path.exists(f))
    full_name = f"{catalog}.{schema_name}.{table_name}" if catalog and catalog != "warehouse" else f"{schema_name}.{table_name}"

    return {
        "catalog": catalog or "warehouse",
        "schema_name": schema_name,
        "table_name": table_name,
        "full_name": full_name,
        "location": target_path,
        "format": "delta",
        "version": dt.version(),
        "columns": columns,
        "history": history,
        "details": {
            "catalog": catalog or "warehouse",
            "num_files": len(files),
            "size_bytes": total_size,
            "version": dt.version(),
            "location": target_path,
            "format": "Delta Lake (Parquet + ACID Log)"
        }
    }

@app.get("/api/table/{schema_name}/{table_name}/preview")
async def preview_table(schema_name: str, table_name: str, limit: int = 50, version: Optional[int] = None, catalog: Optional[str] = None, request: Request = None):
    cat_id = catalog or "warehouse"
    if request:
        current_user = await resolve_principal(request)
        from web import table_access
        if not table_access.can_access_table(current_user, cat_id, schema_name, table_name, "READ"):
            raise HTTPException(status_code=403, detail=f"Access denied: User '{current_user.get('username')}' cannot view '{cat_id}.{schema_name}.{table_name}'.")
    conn = get_duckrun_conn()
    sync_catalogs_with_duckrun(conn)

    # Check if catalog is an external storage mount
    from web.mounts import load_mounts
    mounts = {m["catalog_name"]: m for m in load_mounts() if m.get("enabled", True)}
    if catalog in mounts:
        m = mounts[catalog]
        try:
            if m["type"] == "s3":
                bucket = m["config"].get("bucket", "localspark")
                s3_uri_schema = f"s3://{bucket}/{schema_name}/{table_name}"
                s3_uri_flat = f"s3://{bucket}/{table_name}"
                is_delta = False
                target_s3_path = None
                raw_conn = getattr(conn, "con", conn)
                try:
                    delta_check = raw_conn.execute(f"SELECT file FROM glob('{s3_uri_schema}/_delta_log/0*.json') LIMIT 1").fetchall()
                    if delta_check:
                        is_delta = True
                        target_s3_path = s3_uri_schema
                    else:
                        delta_check_flat = raw_conn.execute(f"SELECT file FROM glob('{s3_uri_flat}/_delta_log/0*.json') LIMIT 1").fetchall()
                        if delta_check_flat:
                            is_delta = True
                            target_s3_path = s3_uri_flat
                except Exception:
                    pass

                if is_delta and target_s3_path:
                    query_target = f"delta_scan('{target_s3_path}')"
                else:
                    query_target = f"read_parquet('s3://{bucket}/{table_name}')"
            else:
                query_target = f"{catalog}.{schema_name}.{table_name}"

            preview_sql = _gov_or_403(f"SELECT * FROM {query_target} LIMIT {int(limit)}", current_user, catalog=catalog, client="preview")
            df = conn.sql(preview_sql).df()
            df_clean = df.replace({np.nan: None, np.inf: None, -np.inf: None})
            rows = [
                {col: clean_json_value(val) for col, val in row.items()}
                for row in df_clean.to_dict(orient="records")
            ]
            columns = [{"name": str(col), "type": str(df[col].dtype).upper()} for col in df.columns]

            return {
                "columns": columns,
                "rows": rows,
                "total_rows": len(rows),
                "limit": limit,
                "is_federated": True,
                "mount_type": m["type"]
            }
        except Exception as e:
            logger.error(f"Error previewing federated table: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to preview federated table: {str(e)}")

    target_path = None
    if catalog == "warehouse":
        cand = os.path.join(WAREHOUSE_DIR, schema_name, table_name)
        if os.path.exists(cand):
            target_path = cand
        else:
            alt = os.path.join(WAREHOUSE_DIR, table_name)
            if os.path.exists(alt):
                target_path = alt
    elif catalog:
        cat = get_catalog(catalog)
        if cat:
            cand = os.path.join(cat["path"], schema_name, table_name)
            if os.path.exists(cand):
                target_path = cand
            else:
                alt = os.path.join(cat["path"], table_name)
                if os.path.exists(alt):
                    target_path = alt
    else:
        cand = os.path.join(WAREHOUSE_DIR, schema_name, table_name)
        if os.path.exists(cand):
            target_path = cand
            catalog = "warehouse"
        else:
            alt = os.path.join(WAREHOUSE_DIR, table_name)
            if os.path.exists(alt):
                target_path = alt
                catalog = "warehouse"
            else:
                for c in load_catalogs():
                    p = os.path.join(c["path"], schema_name, table_name)
                    if os.path.exists(p):
                        target_path = p
                        catalog = c["id"]
                        break

    if not target_path or not os.path.exists(target_path):
        raise HTTPException(status_code=404, detail=f"Table {schema_name}.{table_name} not found in catalog '{catalog or 'warehouse'}'")

    try:
        dt = DeltaTable(target_path)
        fields = [f.name for f in dt.schema().fields]
        if fields == ["__duckrun_deleted__"] or "__duckrun_deleted__" in fields:
            raise HTTPException(status_code=404, detail=f"Table {schema_name}.{table_name} has been dropped")
    except HTTPException:
        raise
    except Exception:
        pass

    try:
        if version is not None:
            query = f"SELECT * FROM delta_scan('{target_path}', version => {version}) LIMIT {limit}"
        else:
            query = f"SELECT * FROM delta_scan('{target_path}') LIMIT {limit}"

        res = conn.sql(_gov_or_403(query, current_user, catalog=catalog, client="preview"))
        df = res.df()
        columns = [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
        rows = [json_serializable_row(row) for row in df.to_dict(orient="records")]
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Active SQL query registry for query cancellation
ACTIVE_QUERIES: Dict[str, Dict[str, Any]] = {}


def _warehouse_has_active_queries(wh_id: str) -> bool:
    """True while any SQL-editor query registered for this warehouse is still running (the auto-suspend guard)."""
    return any(q.get("warehouse_id") == wh_id for q in list(ACTIVE_QUERIES.values()))

class QueryRequest(BaseModel):
    query: str
    warehouse_id: Optional[str] = None
    catalog: Optional[str] = None
    saved_query_id: Optional[str] = None
    execution_id: Optional[str] = None

@app.post("/api/sql/execute")
async def execute_sql(payload: QueryRequest, request: Request):
    query = payload.query.strip()
    if not query:
        return {"success": False, "error": "Empty query"}

    current_user = await resolve_principal(request)

    from web import table_clone
    clone_stmt = table_clone.parse_clone_sql(query)
    if clone_stmt:                       # not SQL any engine understands: handled (and governed) here, never dispatched
        return await _execute_clone_statement(payload, query, clone_stmt, current_user)

    from web import sql_grants
    try:
        grant_stmt = sql_grants.parse(query)
    except sql_grants.GrantSqlError as exc:
        return {"success": False, "error": str(exc), "warehouse_id": payload.warehouse_id or "wh_starter"}
    if grant_stmt:                       # GRANT / REVOKE / SHOW GRANTS: access management, not data (see web/sql_grants.py)
        return await _execute_grant_statement(payload, query, grant_stmt, current_user)

    # Enforce zero-trust catalog permissions
    try:
        enforce_sql_permissions(query, current_user, action="READ")
    except HTTPException as e_perm:
        return {
            "success": False,
            "error": e_perm.detail,
            "elapsed_ms": 0,
            "warehouse_id": payload.warehouse_id or "wh_starter"
        }

    conn = get_duckrun_conn()
    from web import warehouse_lifecycle
    # A suspended warehouse (its container stopped by auto-suspend, a manual stop or by hand) is brought back first; concurrent
    # queries wait for that one resume. If it cannot be resumed the query still runs (the studio executes it locally).
    resume_info = await asyncio.to_thread(warehouse_lifecycle.ensure_running, payload.warehouse_id)
    wh = apply_warehouse_compute(conn, payload.warehouse_id)
    start_time = time.perf_counter()

    # Column masking / statement gating. `query` stays the user's text (history, lineage); `run_sql` is what executes,
    # and it is what every dispatch path below (worker, Ray, local) receives.
    fallback_note = "Studio (local DuckDB)"        # executed_by label when no worker/Ray pool ran the query
    gov = await asyncio.to_thread(gov_gateway.govern_sql, query, current_user, catalog=payload.catalog, client="sql_editor")
    if gov.blocked:
        qid = log_query(query_text=query, duration_ms=0, rows_produced=0, status="FAILED", error_message=gov.blocked,
                        client="SQL_EDITOR", warehouse_id=wh["id"] if wh else "wh_starter", catalog=payload.catalog or "warehouse",
                        user=current_user.get("username", "admin"))
        return {"success": False, "query_id": qid, "error": gov.blocked, "governance_blocked": True, "elapsed_ms": 0,
                "warehouse_id": wh["id"] if wh else "wh_starter"}
    run_sql = gov.sql
    masked_info = gov_gateway.masked_columns_payload(gov)

    execution_id = payload.execution_id or f"exec_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
    active_entry = {
        "execution_id": execution_id,
        "query": query,
        "user": current_user.get("username", "admin"),
        "started_at": time.time(),
        "warehouse_id": wh["id"] if wh else "wh_starter",
        "catalog": payload.catalog or "warehouse",
        "cursor": None,
        "cancelled": False
    }
    ACTIVE_QUERIES[execution_id] = active_entry

    if payload.saved_query_id:
        try:
            from web.saved_queries import record_saved_query_run
            record_saved_query_run(payload.saved_query_id)
        except Exception as e_rec:
            logger.warning(f"Could not record run for saved query {payload.saved_query_id}: {e_rec}")

    try:
        def _execute_sync():
            # 1. Attempt Clustered Worker Dispatch (if warehouse defines an endpoint)
            if wh and wh.get("endpoint"):
                try:
                    import httpx
                    endpoints_to_try = [wh["endpoint"]]
                    if "compute-node-01" in wh["endpoint"]:
                        endpoints_to_try.append("http://localhost:8001")
                    elif "compute-node-02" in wh["endpoint"]:
                        endpoints_to_try.append("http://localhost:8002")
                    elif "compute-node-03" in wh["endpoint"]:
                        endpoints_to_try.append("http://localhost:8003")

                    for ep in endpoints_to_try:
                        try:
                            active_entry["endpoint"] = ep
                            with httpx.Client(timeout=45.0, headers=compute_headers()) as client:
                                resp = client.post(
                                    f"{ep}/api/compute/execute",
                                    json={
                                        "query": run_sql,
                                        "warehouse_id": wh["id"],
                                        "catalog": payload.catalog or "warehouse",
                                        "execution_id": execution_id
                                    }
                                )
                            if resp.status_code == 200:
                                return ("remote", resp.json())
                        except Exception:
                            continue
                except Exception as e_rem:
                    logger.warning(f"Remote compute node dispatch failed for {wh.get('endpoint')}: {e_rem}")

            # 2. Attempt Ray Actor Pool Dispatch (if Ray is active or warehouse has ray_workers configured)
            if wh and RAY_INSTALLED and (wh.get("id") in ray_manager.actor_pools or wh.get("ray_workers", 0) > 0):
                try:
                    ray_res = ray_manager.execute_query(wh["id"], run_sql)
                    if ray_res and ray_res.get("success"):
                        return ("remote", {
                            "success": True,
                            "columns": ray_res.get("columns", []),
                            "rows": ray_res.get("rows", []),
                            "row_count": ray_res.get("row_count", 0),
                            "elapsed_ms": ray_res.get("duration_ms", round((time.perf_counter() - start_time) * 1000, 2)),
                            "executed_by": ray_res.get("actor_id", f"ray-worker-{wh['id']}"),
                            "is_mutation": False
                        })
                except Exception as e_ray:
                    logger.warning(f"Ray actor pool dispatch failed for {wh.get('id')}: {e_ray}")

            # 3. Local In-Process DuckDB Execution (with dedicated cursor isolation & interrupt support)
            is_delta_special = (
                any(k in run_sql.lower() for k in ["describe detail", "describe history", "restore table", "vacuum"])
                or any(run_sql.strip().lower().startswith(p) for p in ["insert ", "update ", "delete ", "merge "])
            )
            if not is_delta_special:
                cur = conn.con.cursor()
                active_entry["cursor"] = cur
                try:
                    res = cur.sql(run_sql)
                    if res is not None and hasattr(res, "df"):
                        df = res.df()
                        columns = [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
                        rows = [json_serializable_row(row) for row in df.to_dict(orient="records")]
                        return ("select", {
                            "columns": columns,
                            "rows": rows,
                            "row_count": len(rows)
                        })
                    else:
                        return ("mutation", {
                            "message": "Statement executed and committed successfully."
                        })
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
            else:
                active_entry["cursor"] = conn.con
                res = conn.sql(run_sql)
                if res is not None and hasattr(res, "df"):
                    df = res.df()
                    columns = [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
                    rows = [json_serializable_row(row) for row in df.to_dict(orient="records")]
                    return ("select", {
                        "columns": columns,
                        "rows": rows,
                        "row_count": len(rows)
                    })
                else:
                    return ("mutation", {
                        "message": "Statement executed and committed successfully."
                    })

        res_kind, res_data = await asyncio.to_thread(_execute_sync)
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

        # An exempt user who materialises raw tagged columns into a new table must not create an untagged raw copy.
        if gov.exempt_reads and (res_kind in ("select", "mutation") or (res_kind == "remote" and res_data.get("success"))):
            try:
                await asyncio.to_thread(gov_gateway.propagate_tags, query, current_user, gov, catalog=payload.catalog)
            except Exception as e_prop:
                logger.warning(f"Tag propagation failed: {e_prop}")

        try:
            from web.lineage import record_query_lineage
            record_query_lineage(query, client="SQL_EDITOR")
        except Exception:
            pass

        if res_kind == "remote":
            node_id = res_data.get("executed_by", "compute-worker")
            if res_data.get("cancelled"):
                qid = log_query(
                    query_text=query,
                    duration_ms=elapsed_ms,
                    rows_produced=0,
                    status="CANCELLED",
                    error_message=res_data.get("error", "Query execution was cancelled by user."),
                    client="SQL_EDITOR",
                    warehouse_id=wh["id"] if wh else "wh_starter",
                    catalog=payload.catalog or "warehouse",
                    user=current_user.get("username", "admin"),
                    executed_by=node_id
                )
                res_data["query_id"] = qid
                res_data["execution_id"] = execution_id
                return res_data
            elif res_data.get("success"):
                res_data["masked_columns"] = masked_info
                qid = log_query(
                    query_text=query,
                    duration_ms=elapsed_ms,
                    rows_produced=res_data.get("row_count", 0),
                    status="SUCCESS",
                    client="SQL_EDITOR",
                    is_mutation=res_data.get("is_mutation", False),
                    warehouse_id=wh["id"] if wh else "wh_starter",
                    catalog=payload.catalog or "warehouse",
                    user=current_user.get("username", "admin"),
                    executed_by=node_id,
                    masked_columns=len(masked_info)
                )
                res_data["query_id"] = qid
                res_data["execution_id"] = execution_id
                res_data.update(_resume_fields(resume_info))
                res_data["warehouse_name"] = wh["name"] if wh else "Starter"
                res_data["cluster_size"] = wh.get("cluster_size", "Small") if wh else "Small"
                return res_data
            else:
                qid = log_query(
                    query_text=query,
                    duration_ms=elapsed_ms,
                    rows_produced=0,
                    status="FAILED",
                    error_message=res_data.get("error", "Worker execution error"),
                    client="SQL_EDITOR",
                    warehouse_id=wh["id"] if wh else "wh_starter",
                    catalog=payload.catalog or "warehouse",
                    user=current_user.get("username", "admin"),
                    executed_by=node_id
                )
                res_data["query_id"] = qid
                res_data["execution_id"] = execution_id
                return res_data

        elif res_kind == "select":
            qid = log_query(
                query_text=query,
                duration_ms=elapsed_ms,
                rows_produced=res_data["row_count"],
                status="SUCCESS",
                client="SQL_EDITOR",
                is_mutation=False,
                warehouse_id=wh["id"] if wh else "wh_starter",
                catalog=payload.catalog or "warehouse",
                user=current_user.get("username", "admin"),
                executed_by=fallback_note,
                masked_columns=len(masked_info)
            )
            return {
                **_resume_fields(resume_info),
                "success": True,
                "masked_columns": masked_info,
                "query_id": qid,
                "execution_id": execution_id,
                "is_mutation": False,
                "columns": res_data["columns"],
                "rows": res_data["rows"],
                "row_count": res_data["row_count"],
                "elapsed_ms": elapsed_ms,
                "warehouse_id": wh["id"] if wh else "wh_starter",
                "warehouse_name": wh["name"] if wh else "Starter",
                "cluster_size": wh.get("cluster_size", "Small") if wh else "Small",
                "executed_by": fallback_note
            }
        else:
            try:
                conn.refresh()
            except Exception:
                pass
            qid = log_query(
                query_text=query,
                duration_ms=elapsed_ms,
                rows_produced=0,
                status="SUCCESS",
                client="SQL_EDITOR",
                is_mutation=True,
                warehouse_id=wh["id"] if wh else "wh_starter",
                catalog=payload.catalog or "warehouse",
                user=current_user.get("username", "admin"),
                executed_by=fallback_note
            )
            return {
                **_resume_fields(resume_info),
                "success": True,
                "query_id": qid,
                "execution_id": execution_id,
                "is_mutation": True,
                "message": res_data.get("message", "Statement executed and committed successfully."),
                "elapsed_ms": elapsed_ms,
                "row_count": 0,
                "warehouse_id": wh["id"] if wh else "wh_starter",
                "warehouse_name": wh["name"] if wh else "Starter",
                "cluster_size": wh.get("cluster_size", "Small") if wh else "Small",
                "executed_by": fallback_note
            }
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        is_interrupted = (
            active_entry.get("cancelled", False)
            or "interrupted" in str(e).lower()
            or "interruptexception" in type(e).__name__.lower()
        )
        if is_interrupted:
            qid = log_query(
                query_text=query,
                duration_ms=elapsed_ms,
                rows_produced=0,
                status="CANCELLED",
                error_message="Query execution was cancelled by user.",
                client="SQL_EDITOR",
                warehouse_id=wh["id"] if 'wh' in locals() and wh else "wh_starter",
                catalog=payload.catalog or "warehouse",
                user=current_user.get("username", "admin"),
                executed_by=fallback_note
            )
            return {
                "success": False,
                "cancelled": True,
                "query_id": qid,
                "execution_id": execution_id,
                "error": "Query execution was cancelled by user.",
                "message": "Query cancelled.",
                "elapsed_ms": elapsed_ms,
                "warehouse_id": wh["id"] if 'wh' in locals() and wh else "wh_starter",
                "executed_by": fallback_note
            }
        else:
            qid = log_query(
                query_text=query,
                duration_ms=elapsed_ms,
                rows_produced=0,
                status="FAILED",
                error_message=str(e),
                client="SQL_EDITOR",
                warehouse_id=wh["id"] if 'wh' in locals() and wh else "wh_starter",
                catalog=payload.catalog or "warehouse",
                user=current_user.get("username", "admin"),
                executed_by=fallback_note
            )
            return {
                "success": False,
                "query_id": qid,
                "execution_id": execution_id,
                "error": str(e),
                "elapsed_ms": elapsed_ms,
                "warehouse_id": wh["id"] if 'wh' in locals() and wh else "wh_starter",
                "executed_by": fallback_note
            }
    finally:
        ACTIVE_QUERIES.pop(execution_id, None)
        if wh and wh.get("id"):
            from web.warehouses import mark_sql_warehouse_active
            mark_sql_warehouse_active(wh["id"])         # the auto-suspend idle clock starts when work ends, not when it began


def _resume_fields(info: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Tells the client why a query was slow (its warehouse was suspended and resumed for it) or that resuming failed."""
    if not info:
        return {}
    if info.get("error"):
        return {"warehouse_resume_error": info["error"]}
    out = {"warehouse_resumed_ms": info["resume_ms"]}
    if info.get("warning"):
        out["warehouse_resume_warning"] = info["warning"]
    return out


@app.post("/api/sql/cancel/{execution_id}")
async def cancel_query(execution_id: str, request: Request):
    """Cancels an ongoing SQL query execution via DuckDB cursor interrupt."""
    current_user = await resolve_principal(request)

    active = ACTIVE_QUERIES.get(execution_id)
    if not active:
        return {
            "success": False,
            "message": "Query execution not found or already completed.",
            "execution_id": execution_id
        }

    # RBAC: Only admin or the user who initiated the query can cancel
    if current_user.get("role") != "admin" and active.get("user") != current_user.get("username"):
        raise HTTPException(status_code=403, detail="Permission denied to cancel this query.")

    active["cancelled"] = True
    cursor = active.get("cursor")
    interrupted = False
    if cursor is not None:
        try:
            cursor.interrupt()
            interrupted = True
            logger.info(f"Query {execution_id} successfully cancelled via cursor.interrupt()")
        except Exception as e:
            logger.warning(f"Failed calling interrupt on cursor for {execution_id}: {e}")

    endpoint = active.get("endpoint")
    if endpoint:
        try:
            import httpx
            with httpx.Client(timeout=5.0, headers=compute_headers()) as client:
                resp = client.post(f"{endpoint}/api/compute/cancel/{execution_id}")
                if resp.status_code == 200 and resp.json().get("success"):
                    interrupted = True
                    logger.info(f"Query {execution_id} cancelled on remote compute worker {endpoint}")
        except Exception as e_fwd:
            logger.warning(f"Failed forwarding cancellation to compute worker {endpoint}: {e_fwd}")

    return {
        "success": True,
        "interrupted": interrupted,
        "message": "Cancellation request processed successfully.",
        "execution_id": execution_id
    }


@app.get("/api/sql/active")
async def get_active_queries(request: Request):
    """Lists currently executing SQL queries across the instance."""
    now = time.time()
    results = []
    for eid, info in list(ACTIVE_QUERIES.items()):
        results.append({
            "execution_id": eid,
            "query": info.get("query", "")[:120],
            "user": info.get("user", "admin"),
            "warehouse_id": info.get("warehouse_id", "wh_starter"),
            "catalog": info.get("catalog", "warehouse"),
            "elapsed_seconds": round(now - info.get("started_at", now), 2),
            "started_at": info.get("started_at")
        })
    return {"queries": results, "count": len(results)}


@app.post("/api/sql/export/parquet")
async def export_sql_parquet_endpoint(payload: Dict[str, Any], request: Request):
    """Export SQL query results or active query execution to an Apache Parquet binary file."""
    rows = payload.get("rows", [])
    query = payload.get("query", "").strip()
    filename = payload.get("filename", "").strip()
    
    if not filename:
        filename = f"query_result_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    if not filename.endswith(".parquet"):
        filename += ".parquet"

    # Strategy 1: If rows are provided from the UI table
    if rows:
        import pandas as pd
        import duckdb
        import tempfile
        try:
            def do_export_rows():
                df = pd.DataFrame(rows)
                c = duckdb.connect()
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                    tmp_path = tmp.name
                try:
                    c.execute(f"COPY df TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION SNAPPY);")
                    with open(tmp_path, "rb") as f:
                        return f.read()
                finally:
                    if os.path.exists(tmp_path):
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            pass
            
            data = await asyncio.to_thread(do_export_rows)
            from fastapi.responses import Response
            return Response(
                content=data,
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"',
                    "Content-Length": str(len(data))
                }
            )
        except Exception as e:
            logger.error(f"Error exporting rows to parquet: {e}")
            if not query:
                raise HTTPException(status_code=500, detail=f"Failed to export Parquet: {e}")

    # Strategy 2: If query is provided, execute COPY via active DuckDB session
    if query:
        import tempfile
        export_user = await resolve_principal(request)
        try:
            enforce_sql_permissions(query, export_user, action="READ")
        except HTTPException as e_perm:
            raise HTTPException(status_code=403, detail=e_perm.detail)
        governed_query = await asyncio.to_thread(_gov_or_403, query.rstrip("; \t\n"), export_user, client="export")
        try:
            def do_export_query():
                conn = get_duckrun_conn()
                raw_conn = getattr(conn, "con", conn)
                clean_q = governed_query
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                    tmp_path = tmp.name
                try:
                    raw_conn.execute(f"COPY ({clean_q}) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION SNAPPY);")
                    with open(tmp_path, "rb") as f:
                        return f.read()
                finally:
                    if os.path.exists(tmp_path):
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            pass

            data = await asyncio.to_thread(do_export_query)
            from fastapi.responses import Response
            return Response(
                content=data,
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"',
                    "Content-Length": str(len(data))
                }
            )
        except Exception as e:
            logger.error(f"Error exporting query to parquet: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to export query to Parquet: {e}")

    raise HTTPException(status_code=400, detail="No rows or query provided to export")


@app.post("/api/sql/profile")
async def profile_sql(payload: QueryRequest, request: Request):
    query = payload.query.strip()
    if not query:
        return {"success": False, "error": "Empty query"}

    current_user = await resolve_principal(request)

    # Enforce zero-trust catalog permissions
    try:
        enforce_sql_permissions(query, current_user, action="READ")
    except HTTPException as e_perm:
        return {
            "success": False,
            "error": e_perm.detail,
            "elapsed_ms": 0,
            "warehouse_id": payload.warehouse_id or "wh_starter"
        }

    conn = get_duckrun_conn()
    wh = apply_warehouse_compute(conn, payload.warehouse_id)
    start_time = time.perf_counter()

    gov = await asyncio.to_thread(gov_gateway.govern_sql, query, current_user, catalog=payload.catalog, client="profile")
    if gov.blocked:
        return {"success": False, "error": gov.blocked, "governance_blocked": True, "elapsed_ms": 0, "profile": None}

    try:
        res = execute_profiled_query(conn, gov.sql)
        elapsed_ms = res["elapsed_ms"]
        profile_obj = res.get("profile")
        profile_json_str = json.dumps(profile_obj) if profile_obj else None

        if res["success"]:
            try:
                from web.lineage import record_query_lineage
                record_query_lineage(query, client="SQL_EDITOR")
            except Exception:
                pass

            qid = log_query(
                query_text=query,
                duration_ms=elapsed_ms,
                rows_produced=res.get("row_count", 0),
                status="SUCCESS",
                client="SQL_EDITOR",
                is_mutation=False,
                warehouse_id=wh["id"],
                catalog=payload.catalog or "warehouse",
                profile_json=profile_json_str,
                user=current_user.get("username", "admin")
            )
            return {
                "success": True,
                "query_id": qid,
                "columns": res.get("columns", []),
                "rows": res.get("rows", []),
                "row_count": res.get("row_count", 0),
                "elapsed_ms": elapsed_ms,
                "profile": profile_obj,
                "warehouse_id": wh["id"],
                "warehouse_name": wh["name"],
                "cluster_size": wh.get("cluster_size", "Small")
            }
        else:
            qid = log_query(
                query_text=query,
                duration_ms=elapsed_ms,
                rows_produced=0,
                status="FAILED",
                error_message=res.get("error", "Execution failed"),
                client="SQL_EDITOR",
                warehouse_id=wh["id"],
                catalog=payload.catalog or "warehouse"
            )
            return {
                "success": False,
                "query_id": qid,
                "error": res.get("error", "Execution failed"),
                "elapsed_ms": elapsed_ms,
                "profile": None
            }
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        return {"success": False, "error": str(e), "elapsed_ms": elapsed_ms, "profile": None}


# ==================== MONACO AUTOCOMPLETE & INLINE COPILOT ====================

class CopilotGenerateRequest(BaseModel):
    prompt: str
    current_query: Optional[str] = None
    selection: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None

@app.get("/api/sql/autocomplete-metadata")
async def get_autocomplete_metadata_api():
    from web.copilot import get_autocomplete_metadata
    conn = get_duckrun_conn()
    return get_autocomplete_metadata(conn)

@app.post("/api/sql/copilot/generate")
async def generate_copilot_sql_api(payload: CopilotGenerateRequest):
    prompt = payload.prompt.strip()
    if not prompt:
        return {"success": False, "error": "Prompt cannot be empty"}
    from web.copilot import generate_copilot_sql
    conn = get_duckrun_conn()
    return generate_copilot_sql(
        prompt=prompt,
        current_query=payload.current_query,
        selection=payload.selection,
        provider=payload.provider,
        model=payload.model,
        conn=conn
    )

@app.get("/api/sql/copilot/providers")
async def get_copilot_providers_api():
    from web.genie import get_available_providers
    return get_available_providers()


@app.get("/api/workspace/files")
async def get_workspace_files(request: Request):
    await resolve_principal(request)
    files = []
    if os.path.exists(NOTEBOOKS_DIR):
        for name in sorted(os.listdir(NOTEBOOKS_DIR)):
            full = os.path.join(NOTEBOOKS_DIR, name)
            if os.path.isfile(full):
                files.append({
                    "name": name,
                    "size_bytes": os.path.getsize(full),
                    "modified": datetime.datetime.fromtimestamp(os.path.getmtime(full)).strftime("%Y-%m-%d %H:%M"),
                    "is_notebook": name.endswith(".ipynb")
                })
    return {"files": files}

# ==================== DATA INGESTION APIS ====================

def get_source_sql_for_file(filepath: str, ext: str) -> str:
    ext_clean = ext.lower().lstrip(".")
    if ext_clean in ["csv", "tsv"]:
        return f"read_csv_auto('{filepath}', header=true)"
    elif ext_clean in ["parquet", "pq", "parq"]:
        return f"read_parquet('{filepath}')"
    elif ext_clean in ["json", "jsonl", "ndjson"]:
        return f"read_json_auto('{filepath}')"
    else:
        raise ValueError(f"Unsupported file format '.{ext_clean}'. Supported formats: CSV, TSV, Parquet (.parquet, .pq, .parq), JSON (.json, .jsonl, .ndjson).")

def normalize_uploaded_file(temp_path: str, ext: str, conn: Any) -> None:
    """
    Detects if an uploaded Parquet file erroneously contains a single string column
    whose column name and data rows are actually delimited (CSV/TSV/etc.) lines.
    If detected, automatically unpacks it into a proper multi-column Parquet file.
    """
    ext_clean = ext.lower().lstrip(".")
    if ext_clean not in ["parquet", "pq", "parq"]:
        return

    try:
        df_sample = conn.sql(f"SELECT * FROM read_parquet('{temp_path}') LIMIT 1").df()
        if len(df_sample.columns) != 1:
            return

        col_name = str(df_sample.columns[0])
        first_val = str(df_sample.iloc[0, 0]) if len(df_sample) > 0 else ""

        delim_found = None
        for d in [",", ";", "\t", "|"]:
            if col_name.count(d) >= 1 and first_val.count(d) >= 1:
                delim_found = d
                break

        if not delim_found:
            return

        logger.info(f"Auto-detect: Single-column Parquet '{temp_path}' contains delimited data (delim={repr(delim_found)}). Unpacking into multi-column dataset...")
        csv_temp = temp_path + ".unpack.csv"
        body_temp = temp_path + ".unpack.body"
        fixed_parquet = temp_path + ".unpacked.parquet"

        try:
            conn.sql(f"""
                COPY (SELECT * FROM read_parquet('{temp_path}'))
                TO '{body_temp}' (HEADER FALSE, DELIMITER '\n', QUOTE '', ESCAPE '')
            """)
            with open(csv_temp, "w", encoding="utf-8", errors="replace") as out_f:
                out_f.write(col_name + "\n")
                with open(body_temp, "r", encoding="utf-8", errors="replace") as in_f:
                    shutil.copyfileobj(in_f, out_f)

            csv_desc = conn.sql(f"DESCRIBE SELECT * FROM read_csv_auto('{csv_temp}')").df()
            time_cols = csv_desc[csv_desc['column_type'].str.upper().str.contains('TIME') & ~csv_desc['column_type'].str.upper().str.contains('TIMESTAMP')]['column_name'].tolist()
            if time_cols:
                escaped = [f'"{c}"::VARCHAR AS "{c}"' for c in time_cols]
                select_expr = f"SELECT * REPLACE ({', '.join(escaped)}) FROM read_csv_auto('{csv_temp}')"
            else:
                select_expr = f"SELECT * FROM read_csv_auto('{csv_temp}')"

            conn.sql(f"""
                COPY ({select_expr})
                TO '{fixed_parquet}' (FORMAT PARQUET)
            """)

            if os.path.exists(fixed_parquet):
                os.replace(fixed_parquet, temp_path)
                logger.info(f"Successfully unpacked '{temp_path}' into multi-column Parquet file.")
        finally:
            for p in [csv_temp, body_temp, fixed_parquet]:
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
    except Exception as e:
        logger.warning(f"Could not auto-unpack single-column Parquet: {e}")

def get_delta_compatible_source_sql(conn: Any, source_sql: str) -> str:
    """
    Checks if any columns in source_sql are of type TIME (which Delta Lake protocol rejects).
    Casts them to VARCHAR so delta-rs can serialize them cleanly into Lakehouse tables.
    """
    try:
        desc_df = conn.sql(f"DESCRIBE SELECT * FROM {source_sql}").df()
        time_cols = desc_df[desc_df['column_type'].str.upper().str.contains('TIME') & ~desc_df['column_type'].str.upper().str.contains('TIMESTAMP')]['column_name'].tolist()
        if time_cols:
            escaped = [f'"{c}"::VARCHAR AS "{c}"' for c in time_cols]
            return f"(SELECT * REPLACE ({', '.join(escaped)}) FROM {source_sql})"
    except Exception as e:
        logger.warning(f"Could not inspect/cast TIME columns in source SQL: {e}")
    return source_sql

def sanitize_identifier(name: str) -> str:
    s = re.sub(r'[^a-zA-Z0-9_]', '_', name.strip().lower())
    s = re.sub(r'_+', '_', s).strip('_')
    if not s or s[0].isdigit():
        s = 'table_' + s
    return s

def get_column_name_pattern_score(col_name: str) -> float:
    """
    Knowledge base of common filter column patterns.
    Returns a heuristic score (0-50) based on how likely this column name is to be filtered.
    Solves the cold-start problem for new tables with no query history.
    """
    col_lower = col_name.lower()

    # Tier 1: Temporal columns - MOST commonly filtered (40-50 points)
    temporal_patterns = {
        'date': 50, 'time': 50, 'timestamp': 50,
        'created_at': 48, 'updated_at': 48, 'modified_at': 48,
        'year': 45, 'month': 45, 'quarter': 45, 'day': 45,
        'week': 42, 'fiscal_year': 45, 'fiscal_period': 45,
        'effective_date': 48, 'transaction_date': 48,
        'order_date': 48, 'ship_date': 46, 'due_date': 46
    }

    # Tier 2: Geographic - Very commonly filtered (30-40 points)
    geographic_patterns = {
        'region': 40, 'country': 40, 'state': 38, 'province': 38,
        'city': 35, 'location': 38, 'territory': 38, 'zone': 36,
        'area': 35, 'district': 36, 'market': 38
    }

    # Tier 3: Status/Category - Commonly filtered (25-35 points)
    categorical_patterns = {
        'status': 38, 'state': 36, 'type': 35, 'category': 36,
        'class': 32, 'tier': 32, 'level': 30, 'grade': 30,
        'priority': 32, 'severity': 30, 'stage': 32, 'phase': 30
    }

    # Tier 4: Organizational - Often filtered (20-30 points)
    organizational_patterns = {
        'department': 35, 'division': 32, 'team': 30, 'unit': 28,
        'branch': 30, 'office': 28, 'channel': 32, 'source': 30
    }

    # Tier 5: Business entities - Moderately filtered (15-25 points)
    business_patterns = {
        'product_type': 28, 'product_category': 28, 'brand': 26,
        'customer_segment': 28, 'customer_type': 28, 'account_type': 26,
        'user_type': 26, 'subscription_type': 26, 'plan': 25,
        'currency': 22, 'payment_method': 24, 'shipping_method': 22
    }

    # Anti-patterns: Never partition on these (negative scores)
    anti_patterns = {
        'id': -50, 'uuid': -50, 'guid': -50, 'key': -40,
        'hash': -45, 'token': -50, 'code': -30, 'number': -35,
        'email': -45, 'phone': -45, 'ssn': -50, 'name': -40,
        'description': -45, 'notes': -45, 'comments': -45,
        'amount': -35, 'price': -35, 'cost': -35, 'value': -35
    }

    # Check exact matches first
    all_patterns = {**temporal_patterns, **geographic_patterns, **categorical_patterns,
                    **organizational_patterns, **business_patterns, **anti_patterns}

    if col_lower in all_patterns:
        return all_patterns[col_lower]

    # Check partial matches (column contains pattern)
    for pattern, score in all_patterns.items():
        if pattern in col_lower:
            # Reduce score slightly for partial matches (80% of full score)
            return score * 0.8

    # No pattern match
    return 0.0

def get_column_query_frequency(table_name: str = None) -> Dict[str, int]:
    """
    Analyzes query history to find which columns are frequently used in WHERE clauses.
    Returns a dict of {column_name: frequency_count}.
    """
    from web.audit import get_db_connection as get_audit_conn
    import re

    column_freq = {}
    try:
        with get_audit_conn() as conn:
            # Get recent successful queries (last 1000)
            cursor = conn.execute("""
                SELECT query_text FROM query_history
                WHERE status = 'SUCCESS'
                ORDER BY executed_at DESC
                LIMIT 1000
            """)
            queries = [row[0] for row in cursor.fetchall()]

        # Parse WHERE clauses to find filtered columns
        for query in queries:
            query_upper = query.upper()

            # Extract WHERE clause (simple regex pattern)
            where_match = re.search(r'\bWHERE\b(.+?)(?:\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)', query_upper, re.IGNORECASE | re.DOTALL)
            if where_match:
                where_clause = where_match.group(1)

                # Find column names in WHERE clause (before =, <, >, IN, LIKE, BETWEEN, etc.)
                # Pattern: word followed by comparison operator
                col_patterns = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|<|>|<=|>=|!=|<>|\bIN\b|\bLIKE\b|\bBETWEEN\b|\bIS\b)', where_clause, re.IGNORECASE)
                for col in col_patterns:
                    col_clean = col.lower()
                    # Skip SQL keywords
                    if col_clean not in ['and', 'or', 'not', 'null', 'true', 'false', 'case', 'when', 'then', 'else', 'end']:
                        column_freq[col_clean] = column_freq.get(col_clean, 0) + 1

            # Also check GROUP BY (often indicates important categorization columns)
            group_match = re.search(r'\bGROUP BY\b\s+([a-zA-Z_][a-zA-Z0-9_,\s]*)', query_upper, re.IGNORECASE)
            if group_match:
                group_cols = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', group_match.group(1))
                for col in group_cols:
                    col_clean = col.lower()
                    # GROUP BY columns get bonus points (half weight)
                    column_freq[col_clean] = column_freq.get(col_clean, 0) + 0.5

    except Exception as e:
        logger.warning(f"Failed to analyze query frequency: {e}")

    return column_freq

def analyze_partition_suitability(df: pd.DataFrame, conn, source_sql: Optional[str] = None, total_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Analyzes columns to determine partition suitability based on:
    1. Data type and true dataset cardinality (via approx_count_distinct when source_sql provided)
    2. Historical query patterns (WHERE clause frequency)
    Returns list of columns with partition_score, partition_rank, and cardinality_warning.
    """
    column_scores = []

    # Get historical query frequency data
    query_freq = get_column_query_frequency()
    max_freq = max(query_freq.values()) if query_freq else 1

    # Approximate distinct counts across full dataset if source_sql is available
    approx_distinct_map = {}
    if source_sql and conn:
        try:
            exprs = [f'approx_count_distinct("{c}")' for c in df.columns]
            agg_sql = f"SELECT {', '.join(exprs)} FROM {source_sql}"
            agg_res = conn.sql(agg_sql).fetchone()
            if agg_res and len(agg_res) == len(df.columns):
                approx_distinct_map = {col: int(agg_res[i]) for i, col in enumerate(df.columns)}
        except Exception as e:
            logger.debug(f"approx_count_distinct query notice: {e}")

    effective_total_rows = total_rows if (total_rows and total_rows > 0) else len(df)

    for col in df.columns:
        try:
            dtype = str(df[col].dtype).lower()
            if col in approx_distinct_map:
                distinct_count = approx_distinct_map[col]
            else:
                distinct_count = int(df[col].nunique())

            cardinality_ratio = distinct_count / max(effective_total_rows, 1)

            score = 0.0
            cardinality_warning = None

            # High-cardinality warning threshold (anti-pattern in Lakehouses due to OOM & small files)
            if distinct_count > 200:
                cardinality_warning = f"High cardinality (~{distinct_count:,} unique values). Partitioning will create thousands of small files and risk out-of-memory errors."
            elif distinct_count > 50:
                cardinality_warning = f"Moderate cardinality (~{distinct_count:,} unique values). Consider partitioning on a coarser column."

            # Type and cardinality scoring
            if any(t in dtype for t in ['datetime', 'timestamp', 'date']):
                if distinct_count <= 20:
                    score += 95
                elif distinct_count <= 50:
                    score += 80
                elif distinct_count <= 200:
                    score += 30
                else:
                    score -= 80  # High-frequency timestamps cause partition explosion & OOM
            elif any(t in dtype for t in ['bool', 'boolean']) or distinct_count == 2:
                score += 75  # Good - binary partition
            elif any(t in dtype for t in ['str', 'string', 'object', 'varchar']):
                if distinct_count <= 1:
                    score -= 50  # Constant column
                elif distinct_count <= 10:
                    score += 85  # Ideal category count
                elif distinct_count <= 30:
                    score += 70
                elif distinct_count <= 50:
                    score += 55
                elif distinct_count <= 100:
                    score += 25
                elif distinct_count <= 500:
                    score -= 30
                else:
                    score -= 80  # High cardinality string / ID
            elif any(t in dtype for t in ['int', 'integer', 'int64', 'int32', 'int16', 'int8']):
                if distinct_count <= 1:
                    score -= 50
                elif distinct_count <= 10:
                    score += 70
                elif distinct_count <= 50:
                    score += 50
                elif distinct_count <= 100:
                    score += 20
                else:
                    score -= 60
            else:
                score -= 40  # Float or other types

            # Cardinality ratio bonus/penalty
            if distinct_count > 1 and cardinality_ratio < 0.001:
                score += 25
            elif distinct_count > 1 and cardinality_ratio < 0.01:
                score += 15
            elif cardinality_ratio > 0.5:
                score -= 50

            # Column name patterns (only rewarding low/moderate cardinality columns)
            col_lower = str(col).lower()
            if distinct_count <= 100:
                if any(pattern in col_lower for pattern in ['category', 'type', 'status', 'region', 'country', 'state', 'year', 'month']):
                    score += 20
                elif any(pattern in col_lower for pattern in ['date', 'day']):
                    score += 10

            if col_lower in ['id', 'uuid', 'guid'] or col_lower.endswith('_id') or col_lower.startswith('_'):
                score -= 60

            # Query pattern analysis
            col_query_freq = query_freq.get(col_lower, 0)
            pattern_score = get_column_name_pattern_score(str(col))

            if col_query_freq > 0 and distinct_count <= 100:
                normalized_freq = min(50, (col_query_freq / max_freq) * 50)
                score += normalized_freq
                query_usage_note = f"Used in {int(col_query_freq)} queries"
            else:
                if distinct_count <= 100 and pattern_score > 0:
                    score += pattern_score * 0.5
                    query_usage_note = f"Not yet queried (pattern match: +{int(pattern_score * 0.5)} pts)"
                else:
                    query_usage_note = "Not yet queried"

            item = {
                'name': str(col),
                'distinct_count': distinct_count,
                'cardinality_ratio': round(cardinality_ratio, 4),
                'partition_score': round(score, 2),
                'query_frequency': int(col_query_freq),
                'query_usage': query_usage_note
            }
            if cardinality_warning:
                item['cardinality_warning'] = cardinality_warning
            column_scores.append(item)
        except Exception as e:
            logger.warning(f"Failed to analyze column {col}: {e}")
            column_scores.append({
                'name': str(col),
                'distinct_count': 0,
                'cardinality_ratio': 0,
                'partition_score': -100
            })

    # Sort by score descending and assign ranks (only positive scores get ranks)
    column_scores.sort(key=lambda x: x['partition_score'], reverse=True)
    rank = 1
    for col_score in column_scores:
        if col_score['partition_score'] > 20 and col_score.get('distinct_count', 0) <= 100:
            col_score['partition_rank'] = rank
            rank += 1
        else:
            col_score['partition_rank'] = None

    return column_scores

@app.post("/api/ingest/preview")
async def ingest_preview(file: UploadFile = File(...)):
    filename = file.filename or "uploaded_data.csv"
    _, ext = os.path.splitext(filename)
    if not ext:
        ext = ".csv"

    file_id = f"upload_{uuid.uuid4().hex[:12]}{ext}"
    temp_path = os.path.join(UPLOADS_DIR, file_id)

    try:
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to store uploaded file: {str(e)}")
    finally:
        await file.close()

    # Use DuckDB to inspect schema and sample rows
    conn = get_duckrun_conn()
    try:
        normalize_uploaded_file(temp_path, ext, conn)
        file_size = int(os.path.getsize(temp_path))
        source_sql = get_source_sql_for_file(temp_path, ext)
        source_sql = get_delta_compatible_source_sql(conn, source_sql)
        res = conn.sql(f"SELECT * FROM {source_sql} LIMIT 25")
        df = res.df()

        # Get total row count estimate
        count_res = conn.sql(f"SELECT COUNT(*) FROM {source_sql}").fetchone()
        total_rows = int(count_res[0]) if count_res else int(len(df))

        # Analyze partition suitability with true dataset cardinality
        partition_analysis = analyze_partition_suitability(df, conn, source_sql=source_sql, total_rows=total_rows)

        # Merge partition analysis with column info
        columns = []
        for col in df.columns:
            col_analysis = next((p for p in partition_analysis if p['name'] == str(col)), None)
            col_info = {
                "name": str(col),
                "type": str(df[col].dtype)
            }
            if col_analysis:
                col_info.update({
                    "partition_score": col_analysis['partition_score'],
                    "partition_rank": col_analysis.get('partition_rank'),
                    "distinct_count": col_analysis['distinct_count'],
                    "cardinality_ratio": col_analysis['cardinality_ratio'],
                    "query_frequency": col_analysis.get('query_frequency', 0),
                    "query_usage": col_analysis.get('query_usage', 'Not analyzed')
                })
                if 'cardinality_warning' in col_analysis:
                    col_info["cardinality_warning"] = col_analysis["cardinality_warning"]
            columns.append(col_info)

        sample_rows = [json_serializable_row(row) for row in df.to_dict(orient="records")]

        suggested_name = sanitize_identifier(os.path.splitext(filename)[0])

        return {
            "file_id": file_id,
            "filename": filename,
            "extension": ext.lower().lstrip("."),
            "file_size_bytes": file_size,
            "suggested_table_name": suggested_name,
            "total_rows": total_rows,
            "columns": columns,
            "sample_rows": sample_rows
        }
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        logger.exception("Failed to parse uploaded file for preview")
        raise HTTPException(status_code=400, detail=f"Failed to parse data: {str(e)}")

class IngestCommitRequest(BaseModel):
    file_id: str
    catalog: Optional[str] = "warehouse"
    schema_name: str = "dbo"
    table_name: str
    mode: str = "overwrite" # overwrite | append
    partition_columns: Optional[List[str]] = None

@app.post("/api/ingest/create")
async def ingest_create(payload: IngestCommitRequest, request: Request):
    # file_id comes back from the client: it must be exactly the id /api/ingest/preview handed out, or the wizard
    # becomes an arbitrary file reader (e.g. ingesting a tagged table's parquet files into an untagged table).
    if not re.fullmatch(r"upload_[0-9a-f]{12}\.[A-Za-z0-9]{1,8}", payload.file_id or ""):
        raise HTTPException(status_code=400, detail="Invalid upload id.")
    temp_path = os.path.join(UPLOADS_DIR, payload.file_id)
    if not os.path.exists(temp_path):
        raise HTTPException(status_code=404, detail="Uploaded file session expired or not found. Please upload again.")

    current_user = await resolve_principal(request)

    target_catalog = payload.catalog or "warehouse"
    if not can_user_access_catalog(current_user, target_catalog, action="WRITE"):
        raise HTTPException(
            status_code=403,
            detail=f"Access denied: User '{current_user.get('username')}' does not have write/ingest permissions on catalog '{target_catalog}'."
        )

    _, ext = os.path.splitext(payload.file_id)
    conn = get_duckrun_conn()
    normalize_uploaded_file(temp_path, ext, conn)
    try:
        source_sql = get_source_sql_for_file(temp_path, ext)
        source_sql = get_delta_compatible_source_sql(conn, source_sql)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Validate and filter partition columns against source schema
    raw_partitions = payload.partition_columns or []
    valid_partition_cols = []
    if raw_partitions and payload.mode.lower() != "append":
        try:
            source_desc = conn.sql(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
            available_cols = [r[0] for r in source_desc]
            valid_partition_cols = [c for c in raw_partitions if c in available_cols]
        except Exception as e:
            logger.warning(f"Could not validate partition columns: {e}")
            valid_partition_cols = [c.strip() for c in raw_partitions if c and c.strip()]

        if valid_partition_cols:
            parts_expr = ", ".join(f'"{c}"' for c in valid_partition_cols)
            try:
                distinct_comb_count = int(conn.sql(f"SELECT COUNT(DISTINCT ({parts_expr})) FROM {source_sql}").fetchone()[0])
            except Exception:
                distinct_comb_count = None

            if distinct_comb_count and distinct_comb_count > 200:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Partition safety limit exceeded: Selected partition columns ({', '.join(valid_partition_cols)}) "
                        f"contain {distinct_comb_count:,} unique partition combinations (safety limit: 200). "
                        f"Partitioning on high-cardinality keys (such as timestamps or IDs) causes severe "
                        f"memory exhaustion (OOM), generates thousands of tiny files, and degrades query performance. "
                        f"Please partition by a low-cardinality categorical column (< 50 distinct values, e.g. status, region) or remove partition columns."
                    )
                )

    schema_clean = sanitize_identifier(payload.schema_name or "dbo")
    table_clean = sanitize_identifier(payload.table_name)
    target_catalog = payload.catalog or "warehouse"
    start_time = time.perf_counter()

    if target_catalog != "warehouse":
        cat = get_catalog(target_catalog)
        if not cat:
            raise HTTPException(status_code=404, detail=f"Catalog '{target_catalog}' not found.")

        if cat.get("is_mounted"):
            if cat.get("read_only", False):
                raise HTTPException(status_code=400, detail=f"Mounted catalog '{cat['name']}' is configured as read-only. Ingestion is not permitted.")

            m_type = cat.get("type")
            if m_type == "s3":
                from web.mounts import get_s3_storage_options, attach_mount_to_duckdb

                cfg = cat.get("config", {})
                bucket = cfg.get("bucket", "localspark")
                storage_options = get_s3_storage_options(cfg)

                s3_table_uri = f"s3://{bucket}/{schema_clean}/{table_clean}"
                target_rel = f"{cat['id']}.{schema_clean}.{table_clean}"

                # Compute row count first, then open streaming Arrow reader so cursor is not invalidated
                row_count = int(conn.sql(f"SELECT COUNT(*) FROM {source_sql}").fetchone()[0])
                arrow_reader = conn.sql(f"SELECT * FROM {source_sql}").arrow()

                mode = "append" if payload.mode.lower() == "append" else "overwrite"
                schema_mode = "merge" if mode == "append" else "overwrite"
                partition_by = valid_partition_cols if valid_partition_cols else None
                write_deltalake(s3_table_uri, arrow_reader, storage_options=storage_options, mode=mode, schema_mode=schema_mode, partition_by=partition_by)

                dt = DeltaTable(s3_table_uri, storage_options=storage_options)
                version = dt.version()
                actual_partitions = list(dt.metadata().partition_columns or []) if hasattr(dt, "metadata") else (partition_by or [])
                elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

                # Remove temp uploaded file
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception:
                    pass

                # Ensure mount secret attached to DuckDB and create a convenient view
                attach_mount_to_duckdb(conn, cat)
                clean_view = f"{target_catalog}_{schema_clean}_{table_clean}".replace("-", "_").replace(".", "_")
                try:
                    conn.sql(f"CREATE OR REPLACE VIEW {clean_view} AS SELECT * FROM delta_scan('{s3_table_uri}')")
                except Exception as e:
                    logger.debug(f"Notice registering S3 delta view: {e}")

                part_msg = f" (Partitioned by: {', '.join(actual_partitions)})" if actual_partitions else ""
                query = f"-- Materialized Delta Lake Table in S3: {s3_table_uri}\nCREATE OR REPLACE TABLE {target_rel} (Delta v{version}, {row_count} rows{part_msg})"
                log_query(
                    query_text=query,
                    duration_ms=elapsed_ms,
                    rows_produced=row_count,
                    status="SUCCESS",
                    client="INGESTION",
                    is_mutation=True,
                    catalog=target_catalog
                )

                try:
                    from web.lineage import upsert_node, upsert_edge, make_table_id
                    f_name = os.path.basename(payload.file_id)
                    f_id = f"file:{f_name}"
                    upsert_node(f_id, f_name, "FILE", layer="RAW_FILE")
                    t_id = make_table_id(target_catalog, schema_clean, table_clean)
                    upsert_node(t_id, table_clean, "TABLE", catalog=target_catalog, schema_name=schema_clean)
                    upsert_edge(f_id, t_id, edge_type="INGESTS_TO")
                except Exception:
                    pass

                return {
                    "success": True,
                    "catalog": target_catalog,
                    "schema_name": schema_clean,
                    "table_name": table_clean,
                    "full_name": target_rel,
                    "location": s3_table_uri,
                    "version": version,
                    "rows_ingested": row_count,
                    "partition_columns": actual_partitions,
                    "elapsed_ms": elapsed_ms,
                    "message": f"Successfully created Delta table {target_rel} on S3 bucket '{bucket}' with {row_count} rows (Version {version}){part_msg}"
                }
            elif m_type == "postgres":
                from web.mounts import ingest_into_postgres_mount

                pg_result = ingest_into_postgres_mount(
                    conn=conn,
                    mount=cat,
                    schema_name=schema_clean,
                    table_name=table_clean,
                    source_sql=source_sql,
                    mode=payload.mode.lower(),
                    partition_columns=valid_partition_cols,
                    max_partitions=50
                )
                elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

                # Remove temp uploaded file
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception:
                    pass

                # Record lineage
                try:
                    from web.lineage import upsert_node, upsert_edge, make_table_id
                    f_name = os.path.basename(payload.file_id)
                    f_id = f"file:{f_name}"
                    upsert_node(f_id, f_name, "FILE", layer="RAW_FILE")
                    t_id = make_table_id(target_catalog, schema_clean, table_clean)
                    upsert_node(t_id, table_clean, "TABLE", catalog=target_catalog, schema_name=schema_clean)
                    upsert_edge(f_id, t_id, edge_type="INGESTS_TO")
                except Exception:
                    pass

                part_strat = pg_result.get("partition_strategy")
                c_count = pg_result.get("child_partitions_count", 0)
                part_msg = f" (PostgreSQL {part_strat} Partitioned: {c_count} child tables)" if part_strat else ""

                log_query(
                    query_text=f"-- Ingested into PostgreSQL catalog '{target_catalog}'\n{pg_result.get('ddl_summary', '')}",
                    duration_ms=elapsed_ms,
                    rows_produced=pg_result["rows_ingested"],
                    status="SUCCESS",
                    client="INGESTION",
                    is_mutation=True,
                    catalog=target_catalog
                )

                return {
                    "success": True,
                    "catalog": target_catalog,
                    "schema_name": schema_clean,
                    "table_name": table_clean,
                    "full_name": pg_result["full_name"],
                    "location": f"postgres://{cat.get('config', {}).get('host', 'localhost')}:{cat.get('config', {}).get('port', 5432)}/{cat.get('config', {}).get('database', '')}/{schema_clean}/{table_clean}",
                    "version": 1,
                    "rows_ingested": pg_result["rows_ingested"],
                    "partition_strategy": part_strat,
                    "partition_columns": pg_result.get("partition_columns", []),
                    "child_partitions": pg_result.get("child_partitions", []),
                    "child_partitions_count": c_count,
                    "elapsed_ms": elapsed_ms,
                    "message": f"Successfully ingested {pg_result['rows_ingested']} rows into PostgreSQL table '{pg_result['full_name']}'{part_msg}"
                }
            else:
                raise HTTPException(status_code=400, detail=f"Ingestion into external mount type '{m_type}' is not supported.")

        target_rel = f"{cat['id']}.{schema_clean}.{table_clean}"
        dt_path = os.path.join(cat["path"], schema_clean, table_clean)
        os.makedirs(os.path.join(cat["path"], schema_clean), exist_ok=True)
    else:
        target_rel = f"{schema_clean}.{table_clean}"
        dt_path = os.path.join(WAREHOUSE_DIR, schema_clean, table_clean)

    conn = get_duckrun_conn()
    start_time = time.perf_counter()

    try:
        if payload.mode.lower() == "append":
            query = f"INSERT INTO {target_rel} SELECT * FROM {source_sql}"
        else:
            if valid_partition_cols:
                parts_clause = ", ".join(f'"{c}"' for c in valid_partition_cols)
                query = f"CREATE OR REPLACE TABLE {target_rel} PARTITIONED BY ({parts_clause}) AS SELECT * FROM {source_sql}"
            else:
                query = f"CREATE OR REPLACE TABLE {target_rel} AS SELECT * FROM {source_sql}"

        conn.sql(query)
        conn.refresh()

        # Verify created table
        if not os.path.exists(dt_path):
            if target_catalog == "warehouse":
                dt_path = os.path.join(WAREHOUSE_DIR, table_clean)

        dt = DeltaTable(dt_path)
        version = dt.version()
        actual_partitions = list(dt.metadata().partition_columns or []) if hasattr(dt, "metadata") else valid_partition_cols
        row_count = conn.sql(f"SELECT COUNT(*) FROM delta_scan('{dt_path}')").fetchone()[0]
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

        # Remove temp uploaded file
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass

        part_msg = f" (Partitioned by: {', '.join(actual_partitions)})" if actual_partitions else ""

        log_query(
            query_text=query,
            duration_ms=elapsed_ms,
            rows_produced=row_count,
            status="SUCCESS",
            client="INGESTION",
            is_mutation=True,
            catalog=target_catalog
        )

        try:
            from web.lineage import upsert_node, upsert_edge, make_table_id
            f_name = os.path.basename(payload.file_id)
            f_id = f"file:{f_name}"
            upsert_node(f_id, f_name, "FILE", layer="RAW_FILE")
            t_id = make_table_id(target_catalog, schema_clean, table_clean)
            upsert_node(t_id, table_clean, "TABLE", catalog=target_catalog, schema_name=schema_clean)
            upsert_edge(f_id, t_id, edge_type="INGESTS_TO")
        except Exception:
            pass

        return {
            "success": True,
            "catalog": target_catalog,
            "schema_name": schema_clean,
            "table_name": table_clean,
            "full_name": target_rel,
            "version": version,
            "rows_ingested": row_count,
            "partition_columns": actual_partitions,
            "elapsed_ms": elapsed_ms,
            "message": f"Successfully created Delta table {target_rel} in catalog '{target_catalog}' with {row_count} rows (Version {version}){part_msg}"
        }
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        log_query(
            query_text=query if 'query' in locals() else f"Ingest {payload.file_id}",
            duration_ms=elapsed_ms,
            rows_produced=0,
            status="FAILED",
            error_message=str(e),
            client="INGESTION",
            is_mutation=True,
            catalog=target_catalog
        )
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")

# ==================== LAKEVIEW DASHBOARDS APIS ====================

# Dashboards logic and store managed via web.dashboards

@app.get("/api/dashboards")
async def list_dashboards():
    dashboards = load_dashboards_store()
    return {
        "dashboards": [
            {
                "id": d["id"],
                "name": d["name"],
                "description": d.get("description", ""),
                "created_at": d.get("created_at", ""),
                "widget_count": len(d.get("widgets", []))
            }
            for d in dashboards
        ]
    }

@app.get("/api/dashboards/{dashboard_id}")
async def get_dashboard(dashboard_id: str, request: Request, params: Optional[str] = None):
    dashboards = load_dashboards_store()
    target = next((d for d in dashboards if d["id"] == dashboard_id), None)
    if not target:
        if dashboards:
            target = dashboards[0]
        else:
            raise HTTPException(status_code=404, detail="Dashboard not found")

    parsed_params = {}
    if params:
        try:
            parsed_params = json.loads(params)
        except Exception:
            pass

    current_user = await resolve_principal(request)
    conn = get_duckrun_conn()
    filter_data = get_dashboard_filter_options(conn, target["id"], principal=current_user)
    hydrated_widgets = []
    for w in target.get("widgets", []):
        w_copy = dict(w)
        exec_res = execute_widget_query(conn, w["query"], parsed_params, principal=current_user)
        w_copy["result"] = exec_res
        hydrated_widgets.append(w_copy)

    return {
        "id": target["id"],
        "name": target["name"],
        "description": target.get("description", ""),
        "created_at": target.get("created_at", ""),
        "filters": filter_data.get("filters", []),
        "filter_options": filter_data.get("options", {}),
        "widgets": hydrated_widgets
    }

class DashboardQueryParams(BaseModel):
    parameters: Optional[Dict[str, Any]] = {}

@app.post("/api/dashboards/{dashboard_id}/query")
async def query_dashboard(dashboard_id: str, payload: DashboardQueryParams, request: Request):
    dashboards = load_dashboards_store()
    target = next((d for d in dashboards if d["id"] == dashboard_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    current_user = await resolve_principal(request)
    conn = get_duckrun_conn()
    hydrated_widgets = []
    for w in target.get("widgets", []):
        w_copy = dict(w)
        exec_res = execute_widget_query(conn, w["query"], payload.parameters or {}, principal=current_user)
        w_copy["result"] = exec_res
        hydrated_widgets.append(w_copy)

    return {
        "id": target["id"],
        "widgets": hydrated_widgets
    }

@app.get("/api/dashboards/{dashboard_id}/filters")
async def get_dashboard_filters(dashboard_id: str, request: Request):
    current_user = await resolve_principal(request)
    conn = get_duckrun_conn()
    return get_dashboard_filter_options(conn, dashboard_id, principal=current_user)

class CreateDashboardRequest(BaseModel):
    name: str
    description: Optional[str] = ""

@app.post("/api/dashboards")
async def create_dashboard(payload: CreateDashboardRequest, current_user: dict = Depends(get_current_user)):
    dashboards = load_dashboards_store()
    new_id = f"dash_{uuid.uuid4().hex[:10]}"
    new_dash = {
        "id": new_id,
        "name": payload.name.strip() or "Untitled Dashboard",
        "description": payload.description or "",
        "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "created_by": current_user["username"],
        "widgets": []
    }
    dashboards.append(new_dash)
    save_dashboards_store(dashboards)

    # Initialize permissions for new dashboard
    from web.dashboard_permissions import initialize_dashboard_permissions
    initialize_dashboard_permissions(new_id, current_user["username"])

    return new_dash

@app.delete("/api/dashboards/{dashboard_id}")
async def delete_dashboard(dashboard_id: str, current_user: dict = Depends(get_current_user)):
    # Check permissions
    from web.dashboard_permissions import can_delete_dashboard
    if not can_delete_dashboard(dashboard_id, current_user["username"], current_user["role"]):
        raise HTTPException(status_code=403, detail="You don't have permission to delete this dashboard")

    dashboards = load_dashboards_store()
    dashboards = [d for d in dashboards if d["id"] != dashboard_id]
    save_dashboards_store(dashboards)

    # Cleanup permissions
    from web.dashboard_permissions import cleanup_dashboard_permissions
    cleanup_dashboard_permissions(dashboard_id)

    return {"success": True, "deleted_id": dashboard_id}

class WidgetPayload(BaseModel):
    title: str
    type: str  # kpi | big_number_trendline | bar | line | area | pie | scatter | bubble | mixed | waterfall | funnel | gauge | heatmap | treemap | sunburst | boxplot | radar | gantt | graph | tree | table | pivot_table | world_map
    query: str
    x_col: Optional[str] = None
    y_col: Optional[str] = None
    z_col: Optional[str] = None
    unit: Optional[str] = ""
    width: Optional[str] = "col-span-2"
    query_source: Optional[str] = "custom"
    saved_query_id: Optional[str] = None
    history_query_id: Optional[str] = None

@app.post("/api/dashboards/{dashboard_id}/widgets")
async def add_widget(dashboard_id: str, payload: WidgetPayload, request: Request):
    dashboards = load_dashboards_store()
    target = next((d for d in dashboards if d["id"] == dashboard_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    widget_id = f"w_{uuid.uuid4().hex[:8]}"
    default_w = "col-span-1" if payload.type in ["kpi", "big_number_trendline"] else ("col-span-4" if payload.type in ["table", "pivot_table", "world_map"] else "col-span-2")
    new_widget = {
        "id": widget_id,
        "title": payload.title.strip() or "New Metric",
        "type": payload.type,
        "query": payload.query.strip(),
        "x_col": payload.x_col,
        "y_col": payload.y_col,
        "z_col": payload.z_col,
        "unit": payload.unit or "",
        "width": payload.width or default_w,
        "query_source": payload.query_source or "custom",
        "saved_query_id": payload.saved_query_id,
        "history_query_id": payload.history_query_id
    }

    if "widgets" not in target:
        target["widgets"] = []
    target["widgets"].append(new_widget)
    save_dashboards_store(dashboards)

    current_user = await resolve_principal(request)
    conn = get_duckrun_conn()
    new_widget_copy = dict(new_widget)
    new_widget_copy["result"] = execute_widget_query(conn, new_widget["query"], principal=current_user)
    return new_widget_copy

@app.delete("/api/dashboards/{dashboard_id}/widgets/{widget_id}")
async def delete_widget(dashboard_id: str, widget_id: str):
    dashboards = load_dashboards_store()
    target = next((d for d in dashboards if d["id"] == dashboard_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    target["widgets"] = [w for w in target.get("widgets", []) if w["id"] != widget_id]
    save_dashboards_store(dashboards)
    return {"success": True, "deleted_widget_id": widget_id}

@app.get("/api/dashboards/{dashboard_id}/widgets/{widget_id}/export")
async def export_widget(dashboard_id: str, widget_id: str, request: Request, format: str = "csv", params: Optional[str] = None):
    """
    Export widget data in CSV or Parquet format.
    Params:
      - format: 'csv' or 'parquet'
      - params: JSON string of filter parameters
    """
    from fastapi.responses import StreamingResponse
    import io

    dashboards = load_dashboards_store()
    target_dashboard = next((d for d in dashboards if d["id"] == dashboard_id), None)
    if not target_dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    target_widget = next((w for w in target_dashboard.get("widgets", []) if w["id"] == widget_id), None)
    if not target_widget:
        raise HTTPException(status_code=404, detail="Widget not found")

    # Parse filter parameters
    filter_params = {}
    if params:
        try:
            filter_params = json.loads(params)
        except:
            filter_params = {}

    # Execute the query
    current_user = await resolve_principal(request)
    conn = get_duckrun_conn()
    query = target_widget.get("query", "")
    result = execute_widget_query(conn, query, filter_params, principal=current_user)

    if not result.get("success"):
        raise HTTPException(status_code=500, detail=f"Query failed: {result.get('error')}")

    # Convert to DataFrame
    rows = result.get("rows", [])
    if not rows:
        raise HTTPException(status_code=404, detail="No data to export")

    df = pd.DataFrame(rows)

    # Export based on format
    if format.lower() == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)

        filename = f"{widget_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode('utf-8')),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    elif format.lower() == "parquet":
        output = io.BytesIO()
        df.to_parquet(output, index=False, engine='pyarrow')
        output.seek(0)

        filename = f"{widget_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        return StreamingResponse(
            output,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    else:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {format}. Use 'csv' or 'parquet'")

@app.post("/api/dashboards/cache/clear")
async def clear_dashboard_cache(current_user: dict = Depends(get_current_user)):
    """Clear all cached dashboard query results."""
    from web.dashboards import clear_query_cache, QUERY_CACHE
    cache_size = len(QUERY_CACHE)
    clear_query_cache()
    return {"success": True, "message": f"Cleared {cache_size} cached queries"}

@app.get("/api/dashboards/cache/stats")
async def get_cache_stats(current_user: dict = Depends(get_current_user)):
    """Get query cache statistics."""
    from web.dashboards import QUERY_CACHE, CACHE_TTL_SECONDS, MAX_CACHE_SIZE
    return {
        "cached_queries": len(QUERY_CACHE),
        "max_cache_size": MAX_CACHE_SIZE,
        "default_ttl_seconds": CACHE_TTL_SECONDS
    }

@app.post("/api/dashboards/{dashboard_id}/share")
async def create_share_link(dashboard_id: str, expires_in_days: int = 30, current_user: dict = Depends(get_current_user)):
    """Create a shareable link for a dashboard."""
    from web.dashboards import create_dashboard_share, load_dashboards_store

    # Verify dashboard exists
    dashboards = load_dashboards_store()
    dashboard = next((d for d in dashboards if d["id"] == dashboard_id), None)
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    share_token = create_dashboard_share(dashboard_id, current_user["username"], expires_in_days)

    return {
        "success": True,
        "share_token": share_token,
        "share_url": f"/share/{share_token}",
        "expires_in_days": expires_in_days
    }

@app.get("/api/dashboards/shares")
async def list_shares(dashboard_id: Optional[str] = None, current_user: dict = Depends(get_current_user)):
    """List all dashboard shares."""
    from web.dashboards import list_dashboard_shares
    shares = list_dashboard_shares(dashboard_id)
    return {"success": True, "shares": shares}

@app.delete("/api/dashboards/share/{share_token}")
async def revoke_share(share_token: str, current_user: dict = Depends(get_current_user)):
    """Revoke a dashboard share."""
    from web.dashboards import revoke_dashboard_share
    success = revoke_dashboard_share(share_token)
    if not success:
        raise HTTPException(status_code=404, detail="Share token not found")
    return {"success": True, "message": "Share revoked"}

@app.get("/share/{share_token}")
async def view_shared_dashboard(share_token: str):
    """View a shared dashboard (no auth required)."""
    from web.dashboards import get_dashboard_by_share_token
    dashboard = get_dashboard_by_share_token(share_token)

    if not dashboard:
        raise HTTPException(status_code=404, detail="Share link not found or expired")

    # Return the main index page with share token in URL
    # The frontend will detect the share token and load the dashboard
    with open("/workspace/web/templates/index.html", "r") as f:
        html_content = f.read()

    return HTMLResponse(content=html_content)

@app.get("/api/dashboards/templates")
async def list_dashboard_templates(current_user: dict = Depends(get_current_user)):
    """List all available dashboard templates."""
    from web.dashboard_templates import get_all_templates
    templates = get_all_templates()
    return {"success": True, "templates": templates}

@app.get("/api/dashboards/templates/{template_id}")
async def get_dashboard_template(template_id: str, current_user: dict = Depends(get_current_user)):
    """Get a specific dashboard template."""
    from web.dashboard_templates import get_template_by_id
    template = get_template_by_id(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    return {"success": True, "template": template}

@app.post("/api/dashboards/from-template/{template_id}")
async def create_dashboard_from_template(
    template_id: str,
    dashboard_name: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Create a new dashboard from a template."""
    from web.dashboard_templates import instantiate_template
    from web.dashboards import load_dashboards_store, save_dashboards_store

    dashboard = instantiate_template(template_id, dashboard_name)
    if not dashboard:
        raise HTTPException(status_code=404, detail="Template not found")

    # Add to dashboards store
    dashboards = load_dashboards_store()
    dashboards.append(dashboard)
    save_dashboards_store(dashboards)

    return {
        "success": True,
        "dashboard_id": dashboard["id"],
        "message": f"Dashboard created from template: {dashboard['name']}"
    }

@app.post("/api/dashboards/{dashboard_id}/version")
async def create_dashboard_version(
    dashboard_id: str,
    comment: str = "",
    current_user: dict = Depends(get_current_user)
):
    """Create a version snapshot of a dashboard."""
    from web.dashboards import load_dashboards_store
    from web.dashboard_versions import create_version

    dashboards = load_dashboards_store()
    dashboard = next((d for d in dashboards if d["id"] == dashboard_id), None)
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    version_id = create_version(dashboard_id, dashboard, current_user["username"], comment)
    return {"success": True, "version_id": version_id}

@app.get("/api/dashboards/{dashboard_id}/versions")
async def list_dashboard_versions(dashboard_id: str, current_user: dict = Depends(get_current_user)):
    """List all versions of a dashboard."""
    from web.dashboard_versions import list_versions
    versions = list_versions(dashboard_id)
    return {"success": True, "versions": versions}

@app.post("/api/dashboards/{dashboard_id}/restore/{version_id}")
async def restore_dashboard_version(
    dashboard_id: str,
    version_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Restore a dashboard to a specific version."""
    from web.dashboard_versions import restore_version
    from web.dashboards import load_dashboards_store, save_dashboards_store

    dashboard_config = restore_version(dashboard_id, version_id)
    if not dashboard_config:
        raise HTTPException(status_code=404, detail="Version not found")

    dashboards = load_dashboards_store()
    for i, d in enumerate(dashboards):
        if d["id"] == dashboard_id:
            dashboards[i] = dashboard_config
            break

    save_dashboards_store(dashboards)
    return {"success": True, "message": "Dashboard restored"}

@app.get("/api/themes")
async def list_themes():
    """List all available themes."""
    from web.dashboard_themes import get_all_themes
    themes = get_all_themes()
    return {"success": True, "themes": themes}

@app.get("/api/themes/{theme_id}")
async def get_theme(theme_id: str):
    """Get a specific theme."""
    from web.dashboard_themes import get_theme as get_theme_data
    theme = get_theme_data(theme_id)
    if not theme:
        raise HTTPException(status_code=404, detail="Theme not found")
    return {"success": True, "theme": theme}

@app.get("/api/folders")
async def list_folders(parent_id: Optional[str] = None, current_user: dict = Depends(get_current_user)):
    """List dashboard folders."""
    from web.dashboard_folders import list_folders as list_folders_func
    folders = list_folders_func(parent_id)
    return {"success": True, "folders": folders}

@app.get("/api/folders/tree")
async def get_folder_tree(current_user: dict = Depends(get_current_user)):
    """Get folder hierarchy tree."""
    from web.dashboard_folders import get_folder_tree
    tree = get_folder_tree()
    return {"success": True, "tree": tree}

@app.post("/api/folders")
async def create_folder(name: str, parent_id: str = "folder_root", icon: str = "folder", current_user: dict = Depends(get_current_user)):
    """Create a new folder."""
    from web.dashboard_folders import create_folder as create_folder_func
    folder_id = create_folder_func(name, parent_id, icon, current_user["username"])
    return {"success": True, "folder_id": folder_id}

@app.post("/api/dashboards/{dashboard_id}/move")
async def move_dashboard(dashboard_id: str, folder_id: str, current_user: dict = Depends(get_current_user)):
    """Move dashboard to a folder."""
    from web.dashboard_folders import move_dashboard_to_folder
    success = move_dashboard_to_folder(dashboard_id, folder_id)
    if not success:
        raise HTTPException(status_code=404, detail="Folder not found")
    return {"success": True}

@app.get("/api/widgets/{widget_id}/comments")
async def list_widget_comments(widget_id: str, current_user: dict = Depends(get_current_user)):
    """List comments on a widget."""
    from web.widget_comments import list_comments
    comments = list_comments(widget_id)
    return {"success": True, "comments": comments}

@app.post("/api/widgets/{widget_id}/comments")
async def add_widget_comment(widget_id: str, text: str, current_user: dict = Depends(get_current_user)):
    """Add a comment to a widget."""
    from web.widget_comments import add_comment
    comment_id = add_comment(widget_id, text, current_user["username"])
    return {"success": True, "comment_id": comment_id}

@app.put("/api/widgets/{widget_id}/comments/{comment_id}")
async def update_widget_comment(widget_id: str, comment_id: str, text: str, current_user: dict = Depends(get_current_user)):
    """Update a comment."""
    from web.widget_comments import update_comment
    success = update_comment(widget_id, comment_id, text, current_user["username"])
    if not success:
        raise HTTPException(status_code=404, detail="Comment not found")
    return {"success": True}

@app.delete("/api/widgets/{widget_id}/comments/{comment_id}")
async def delete_widget_comment(widget_id: str, comment_id: str, current_user: dict = Depends(get_current_user)):
    """Delete a comment."""
    from web.widget_comments import delete_comment
    success = delete_comment(widget_id, comment_id)
    if not success:
        raise HTTPException(status_code=404, detail="Comment not found")
    return {"success": True}

# ==================== DASHBOARD PERMISSIONS APIS ====================

@app.get("/api/dashboards/{dashboard_id}/permissions")
async def get_dashboard_permissions(dashboard_id: str, current_user: dict = Depends(get_current_user)):
    """Get permissions for a dashboard."""
    from web.dashboard_permissions import get_dashboard_permissions as get_perms, can_manage_permissions

    # Check if user can manage permissions
    if not can_manage_permissions(dashboard_id, current_user["username"], current_user["role"]):
        raise HTTPException(status_code=403, detail="You don't have permission to view permissions")

    perms = get_perms(dashboard_id)
    if not perms:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    return {"success": True, "permissions": perms}

class DashboardGrantPayload(BaseModel):
    user: Optional[str] = None
    role: Optional[str] = None
    group: Optional[str] = None
    level: Optional[str] = None


@app.post("/api/dashboards/{dashboard_id}/permissions/grant")
async def grant_dashboard_permission(
    dashboard_id: str,
    payload: Optional[DashboardGrantPayload] = None,
    user: Optional[str] = None,
    role: Optional[str] = None,
    level: str = "viewer",
    group: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Grant permission to a user, a role or a group (`group` = the group's id). Values come from the JSON body (what the UI sends) or, as
    before, from query parameters."""
    from web.dashboard_permissions import grant_permission, can_manage_permissions
    if payload:
        user, role, group, level = payload.user or user, payload.role or role, payload.group or group, payload.level or level

    # Check if user can manage permissions
    if not can_manage_permissions(dashboard_id, current_user["username"], current_user["role"]):
        raise HTTPException(status_code=403, detail="You don't have permission to grant permissions")
    if group:
        from web import groups
        if not groups.get_group(group):
            raise HTTPException(status_code=404, detail="That group does not exist.")

    success = grant_permission(
        dashboard_id=dashboard_id,
        user=user,
        role=role,
        level=level,
        granted_by=current_user["username"],
        group=group
    )

    if not success:
        raise HTTPException(status_code=400, detail="Failed to grant permission")

    return {"success": True, "message": "Permission granted"}

@app.post("/api/dashboards/{dashboard_id}/permissions/revoke")
async def revoke_dashboard_permission(
    dashboard_id: str,
    payload: Optional[DashboardGrantPayload] = None,
    user: Optional[str] = None,
    role: Optional[str] = None,
    group: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Revoke permission from a user, a role or a group."""
    from web.dashboard_permissions import revoke_permission, can_manage_permissions
    if payload:
        user, role, group = payload.user or user, payload.role or role, payload.group or group

    # Check if user can manage permissions
    if not can_manage_permissions(dashboard_id, current_user["username"], current_user["role"]):
        raise HTTPException(status_code=403, detail="You don't have permission to revoke permissions")

    success = revoke_permission(dashboard_id=dashboard_id, user=user, role=role, group=group)

    if not success:
        raise HTTPException(status_code=400, detail="Failed to revoke permission")

    return {"success": True, "message": "Permission revoked"}

@app.post("/api/dashboards/{dashboard_id}/permissions/public")
async def set_dashboard_public_status(
    dashboard_id: str,
    is_public: bool,
    current_user: dict = Depends(get_current_user)
):
    """Set whether a dashboard is publicly viewable."""
    from web.dashboard_permissions import set_dashboard_public, can_manage_permissions

    # Check if user can manage permissions
    if not can_manage_permissions(dashboard_id, current_user["username"], current_user["role"]):
        raise HTTPException(status_code=403, detail="You don't have permission to change public status")

    success = set_dashboard_public(dashboard_id, is_public)

    if not success:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    return {"success": True, "is_public": is_public}

@app.post("/api/dashboards/{dashboard_id}/permissions/transfer")
async def transfer_dashboard_ownership(
    dashboard_id: str,
    new_owner: str,
    current_user: dict = Depends(get_current_user)
):
    """Transfer dashboard ownership to another user."""
    from web.dashboard_permissions import transfer_ownership, can_manage_permissions

    # Check if user can manage permissions
    if not can_manage_permissions(dashboard_id, current_user["username"], current_user["role"]):
        raise HTTPException(status_code=403, detail="You don't have permission to transfer ownership")

    success = transfer_ownership(dashboard_id, new_owner, current_user["username"])

    if not success:
        raise HTTPException(status_code=400, detail="Failed to transfer ownership")

    return {"success": True, "message": f"Ownership transferred to {new_owner}"}

@app.get("/api/dashboards/{dashboard_id}/permissions/users")
async def list_dashboard_users(dashboard_id: str, current_user: dict = Depends(get_current_user)):
    """List all users with access to a dashboard."""
    from web.dashboard_permissions import list_dashboard_users as list_users, can_view_dashboard

    # Check if user can view dashboard
    if not can_view_dashboard(dashboard_id, current_user["username"], current_user["role"]):
        raise HTTPException(status_code=403, detail="You don't have permission to view this dashboard")

    users = list_users(dashboard_id)
    return {"success": True, "users": users}

@app.get("/api/permissions/summary")
async def get_permissions_summary(current_user: dict = Depends(require_role("admin"))):
    """Get summary of dashboard permissions (admin only)."""
    from web.dashboard_permissions import get_permission_summary
    summary = get_permission_summary()
    return {"success": True, "summary": summary}

# ==================== SCHEDULED EXPORTS APIS ====================

@app.get("/api/schedules")
async def list_schedules(dashboard_id: Optional[str] = None, current_user: dict = Depends(get_current_user)):
    """List all scheduled exports."""
    from web.scheduled_exports import list_schedules as list_schedules_func
    schedules = list_schedules_func(dashboard_id)
    return {"success": True, "schedules": schedules}

@app.get("/api/schedules/{schedule_id}")
async def get_schedule(schedule_id: str, current_user: dict = Depends(get_current_user)):
    """Get a specific scheduled export."""
    from web.scheduled_exports import get_schedule as get_schedule_func
    schedule = get_schedule_func(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"success": True, "schedule": schedule}

@app.post("/api/schedules")
async def create_schedule(
    dashboard_id: str,
    name: str,
    frequency: str,
    format: str = "csv",
    widget_ids: Optional[List[str]] = None,
    hour: int = 0,
    minute: int = 0,
    day_of_week: int = 0,
    day_of_month: int = 1,
    cron_expression: Optional[str] = None,
    enabled: bool = True,
    email_enabled: bool = False,
    email_recipients: Optional[List[str]] = None,
    email_cc: Optional[List[str]] = None,
    email_message: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Create a new scheduled export."""
    from web.scheduled_exports import create_schedule as create_schedule_func
    schedule_id = create_schedule_func(
        dashboard_id=dashboard_id,
        name=name,
        frequency=frequency,
        format=format,
        widget_ids=widget_ids,
        created_by=current_user["username"],
        hour=hour,
        minute=minute,
        day_of_week=day_of_week,
        day_of_month=day_of_month,
        cron_expression=cron_expression,
        enabled=enabled,
        email_enabled=email_enabled,
        email_recipients=email_recipients,
        email_cc=email_cc,
        email_message=email_message
    )
    return {"success": True, "schedule_id": schedule_id}

@app.put("/api/schedules/{schedule_id}")
async def update_schedule(
    schedule_id: str,
    name: Optional[str] = None,
    frequency: Optional[str] = None,
    format: Optional[str] = None,
    widget_ids: Optional[List[str]] = None,
    enabled: Optional[bool] = None,
    hour: Optional[int] = None,
    minute: Optional[int] = None,
    day_of_week: Optional[int] = None,
    day_of_month: Optional[int] = None,
    cron_expression: Optional[str] = None,
    email_enabled: Optional[bool] = None,
    email_recipients: Optional[List[str]] = None,
    email_cc: Optional[List[str]] = None,
    email_message: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Update a scheduled export."""
    from web.scheduled_exports import update_schedule as update_schedule_func
    success = update_schedule_func(
        schedule_id=schedule_id,
        name=name,
        frequency=frequency,
        format=format,
        widget_ids=widget_ids,
        enabled=enabled,
        hour=hour,
        minute=minute,
        day_of_week=day_of_week,
        day_of_month=day_of_month,
        cron_expression=cron_expression,
        email_enabled=email_enabled,
        email_recipients=email_recipients,
        email_cc=email_cc,
        email_message=email_message
    )
    if not success:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"success": True}

@app.delete("/api/schedules/{schedule_id}")
async def delete_schedule(schedule_id: str, current_user: dict = Depends(get_current_user)):
    """Delete a scheduled export."""
    from web.scheduled_exports import delete_schedule as delete_schedule_func
    success = delete_schedule_func(schedule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"success": True}

@app.post("/api/schedules/{schedule_id}/trigger")
async def trigger_schedule(schedule_id: str, current_user: dict = Depends(get_current_user)):
    """Manually trigger a scheduled export."""
    from web.scheduled_exports import trigger_schedule_now
    success = trigger_schedule_now(schedule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"success": True, "message": "Export triggered"}

@app.get("/api/schedules/{schedule_id}/history")
async def get_schedule_history(schedule_id: str, limit: int = 10, current_user: dict = Depends(get_current_user)):
    """Get export history for a schedule."""
    from web.scheduled_exports import get_export_history
    history = get_export_history(schedule_id, limit)
    return {"success": True, "history": history}

# ==================== EMAIL REPORTS APIS ====================

@app.get("/api/email/config")
async def get_email_config(current_user: dict = Depends(require_role("admin"))):
    """Get email configuration (admin only)."""
    from web.email_reports import get_email_config
    config = get_email_config()
    return {"success": True, "config": config}

@app.post("/api/email/config")
async def update_email_config(
    smtp_server: Optional[str] = None,
    smtp_port: Optional[int] = None,
    use_tls: Optional[bool] = None,
    sender_email: Optional[str] = None,
    sender_password: Optional[str] = None,
    sender_name: Optional[str] = None,
    enabled: Optional[bool] = None,
    current_user: dict = Depends(require_role("admin"))
):
    """Update email configuration (admin only)."""
    from web.email_reports import update_email_config
    success = update_email_config(
        smtp_server=smtp_server,
        smtp_port=smtp_port,
        use_tls=use_tls,
        sender_email=sender_email,
        sender_password=sender_password,
        sender_name=sender_name,
        enabled=enabled
    )
    return {"success": success, "message": "Email configuration updated"}

@app.post("/api/email/test")
async def test_email_connection(current_user: dict = Depends(require_role("admin"))):
    """Test email connection (admin only)."""
    from web.email_reports import test_email_connection
    result = test_email_connection()
    return result

@app.post("/api/email/send")
async def send_email_report(
    recipients: List[str],
    subject: str,
    body: str,
    cc: Optional[List[str]] = None,
    bcc: Optional[List[str]] = None,
    current_user: dict = Depends(get_current_user)
):
    """Send an email."""
    from web.email_reports import send_email
    result = send_email(
        recipients=recipients,
        subject=subject,
        body_html=body,
        cc=cc,
        bcc=bcc
    )
    return result

@app.post("/api/dashboards/{dashboard_id}/email")
async def email_dashboard_report(
    dashboard_id: str,
    recipients: List[str],
    include_attachments: bool = True,
    attachment_format: str = "csv",
    custom_message: Optional[str] = None,
    cc: Optional[List[str]] = None,
    current_user: dict = Depends(get_current_user)
):
    """Email a dashboard report."""
    from web.email_reports import send_dashboard_report
    from web.dashboards import load_dashboards_store

    # Get dashboard name
    dashboards = load_dashboards_store()
    dashboard = None
    for d in dashboards:
        if d["id"] == dashboard_id:
            dashboard = d
            break

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    result = send_dashboard_report(
        dashboard_id=dashboard_id,
        dashboard_name=dashboard.get("name", "Dashboard"),
        recipients=recipients,
        include_attachments=include_attachments,
        attachment_format=attachment_format,
        custom_message=custom_message,
        cc=cc
    )
    return result

@app.get("/api/email/history")
async def get_email_history(limit: int = 50, current_user: dict = Depends(get_current_user)):
    """Get email send history."""
    from web.email_reports import get_email_history
    history = get_email_history(limit)
    return {"success": True, "history": history}


# ==================== SLACK INTEGRATION APIS ====================

@app.get("/api/slack/config")
async def get_slack_config(current_user: dict = Depends(require_role("admin"))):
    """Get Slack configuration (admin only)."""
    from web.slack_integration import load_config
    config = load_config()
    # Don't send webhook URLs to frontend for security
    safe_config = config.copy()
    safe_webhooks = []
    for webhook in config.get("webhooks", []):
        safe_webhook = webhook.copy()
        if "webhook_url" in safe_webhook:
            # Mask webhook URL
            url = safe_webhook["webhook_url"]
            if len(url) > 20:
                safe_webhook["webhook_url"] = url[:10] + "..." + url[-10:]
            else:
                safe_webhook["webhook_url"] = "***"
        safe_webhooks.append(safe_webhook)
    safe_config["webhooks"] = safe_webhooks
    return {"success": True, "config": safe_config}


@app.post("/api/slack/config")
async def update_slack_config(request: Request, current_user: dict = Depends(require_role("admin"))):
    """Update Slack configuration (admin only)."""
    from web.slack_integration import load_config, save_config
    data = await request.json()

    config = load_config()

    # Update allowed fields
    if "enabled" in data:
        config["enabled"] = data["enabled"]
    if "default_webhook" in data:
        config["default_webhook"] = data["default_webhook"]
    if "mention_users" in data:
        config["mention_users"] = data["mention_users"]
    if "include_charts" in data:
        config["include_charts"] = data["include_charts"]

    from web.slack_integration import save_config
    if save_config(config):
        return {"success": True, "message": "Slack configuration updated"}
    else:
        return {"success": False, "error": "Failed to save configuration"}


@app.post("/api/slack/webhooks")
async def create_slack_webhook(request: Request, current_user: dict = Depends(require_role("admin"))):
    """Add a new Slack webhook (admin only)."""
    from web.slack_integration import add_webhook
    data = await request.json()

    result = add_webhook(
        name=data.get("name", ""),
        webhook_url=data.get("webhook_url", ""),
        channel=data.get("channel", "#general"),
        description=data.get("description", "")
    )

    return result


@app.put("/api/slack/webhooks/{webhook_id}")
async def update_slack_webhook_endpoint(webhook_id: str, request: Request, current_user: dict = Depends(require_role("admin"))):
    """Update a Slack webhook (admin only)."""
    from web.slack_integration import update_webhook
    data = await request.json()

    result = update_webhook(webhook_id, data)
    return result


@app.delete("/api/slack/webhooks/{webhook_id}")
async def delete_slack_webhook(webhook_id: str, current_user: dict = Depends(require_role("admin"))):
    """Delete a Slack webhook (admin only)."""
    from web.slack_integration import remove_webhook
    result = remove_webhook(webhook_id)
    return result


@app.get("/api/slack/webhooks")
async def list_slack_webhooks(current_user: dict = Depends(get_current_user)):
    """List all Slack webhooks."""
    from web.slack_integration import get_webhooks
    webhooks = get_webhooks()

    # Mask webhook URLs for security
    safe_webhooks = []
    for webhook in webhooks:
        safe_webhook = webhook.copy()
        if "webhook_url" in safe_webhook:
            url = safe_webhook["webhook_url"]
            if len(url) > 20:
                safe_webhook["webhook_url"] = url[:10] + "..." + url[-10:]
            else:
                safe_webhook["webhook_url"] = "***"
        safe_webhooks.append(safe_webhook)

    return {"success": True, "webhooks": safe_webhooks}


@app.post("/api/slack/test/{webhook_id}")
async def test_slack_webhook_endpoint(webhook_id: str, current_user: dict = Depends(require_role("admin"))):
    """Test a Slack webhook (admin only)."""
    from web.slack_integration import get_webhook, test_webhook

    webhook = get_webhook(webhook_id)
    if not webhook:
        return {"success": False, "error": "Webhook not found"}

    result = test_webhook(webhook["webhook_url"])
    return result


@app.post("/api/slack/notify")
async def send_slack_notification(request: Request, current_user: dict = Depends(get_current_user)):
    """Send a notification to Slack."""
    from web.slack_integration import send_notification
    data = await request.json()

    result = send_notification(
        message=data.get("message", ""),
        webhook_id=data.get("webhook_id"),
        title=data.get("title"),
        fields=data.get("fields"),
        color=data.get("color", "#36a64f"),
        footer=data.get("footer")
    )

    return result


@app.post("/api/slack/alert")
async def send_slack_alert(request: Request, current_user: dict = Depends(get_current_user)):
    """Send an alert to Slack."""
    from web.slack_integration import send_alert
    data = await request.json()

    result = send_alert(
        alert_name=data.get("alert_name", ""),
        condition=data.get("condition", ""),
        current_value=data.get("current_value"),
        threshold=data.get("threshold"),
        dashboard_name=data.get("dashboard_name"),
        query_name=data.get("query_name"),
        severity=data.get("severity", "warning"),
        webhook_id=data.get("webhook_id")
    )

    return result


# ==================== GENERIC WEBHOOK APIS ====================

@app.get("/api/webhooks/config")
async def get_webhook_config(current_user: dict = Depends(require_role("admin"))):
    """Get webhook configuration (admin only)."""
    from web.webhook_alerts import load_config
    config = load_config()

    # Mask webhook URLs and auth tokens for security
    safe_config = config.copy()
    safe_webhooks = []
    for webhook in config.get("webhooks", []):
        safe_webhook = webhook.copy()
        if "url" in safe_webhook:
            url = safe_webhook["url"]
            if len(url) > 20:
                safe_webhook["url"] = url[:15] + "..." + url[-10:]
            else:
                safe_webhook["url"] = "***"
        if "auth_token" in safe_webhook and safe_webhook["auth_token"]:
            safe_webhook["auth_token"] = "***MASKED***"
        safe_webhooks.append(safe_webhook)
    safe_config["webhooks"] = safe_webhooks

    return {"success": True, "config": safe_config}


@app.post("/api/webhooks/config")
async def update_webhook_config(request: Request, current_user: dict = Depends(require_role("admin"))):
    """Update webhook configuration (admin only)."""
    from web.webhook_alerts import load_config, save_config
    data = await request.json()

    config = load_config()

    # Update allowed fields
    if "enabled" in data:
        config["enabled"] = data["enabled"]
    if "max_retries" in data:
        config["max_retries"] = data["max_retries"]
    if "retry_delay" in data:
        config["retry_delay"] = data["retry_delay"]
    if "timeout" in data:
        config["timeout"] = data["timeout"]

    from web.webhook_alerts import save_config
    if save_config(config):
        return {"success": True, "message": "Webhook configuration updated"}
    else:
        return {"success": False, "error": "Failed to save configuration"}


@app.get("/api/webhooks/types")
async def get_webhook_types(current_user: dict = Depends(get_current_user)):
    """Get available webhook types."""
    from web.webhook_alerts import get_webhook_types
    types = get_webhook_types()
    return {"success": True, "types": types}


@app.post("/api/webhooks")
async def create_webhook(request: Request, current_user: dict = Depends(require_role("admin"))):
    """Add a new webhook (admin only)."""
    from web.webhook_alerts import add_webhook
    data = await request.json()

    result = add_webhook(
        name=data.get("name", ""),
        url=data.get("url", ""),
        webhook_type=data.get("type", "generic"),
        custom_headers=data.get("custom_headers"),
        custom_template=data.get("custom_template"),
        description=data.get("description", ""),
        auth_type=data.get("auth_type", "none"),
        auth_token=data.get("auth_token", "")
    )

    return result


@app.put("/api/webhooks/{webhook_id}")
async def update_webhook_endpoint(webhook_id: str, request: Request, current_user: dict = Depends(require_role("admin"))):
    """Update a webhook (admin only)."""
    from web.webhook_alerts import update_webhook
    data = await request.json()

    result = update_webhook(webhook_id, data)
    return result


@app.delete("/api/webhooks/{webhook_id}")
async def delete_webhook(webhook_id: str, current_user: dict = Depends(require_role("admin"))):
    """Delete a webhook (admin only)."""
    from web.webhook_alerts import remove_webhook
    result = remove_webhook(webhook_id)
    return result


@app.get("/api/webhooks")
async def list_webhooks(current_user: dict = Depends(get_current_user)):
    """List all webhooks."""
    from web.webhook_alerts import get_webhooks
    webhooks = get_webhooks()

    # Mask URLs and auth tokens for security
    safe_webhooks = []
    for webhook in webhooks:
        safe_webhook = webhook.copy()
        if "url" in safe_webhook:
            url = safe_webhook["url"]
            if len(url) > 20:
                safe_webhook["url"] = url[:15] + "..." + url[-10:]
            else:
                safe_webhook["url"] = "***"
        if "auth_token" in safe_webhook and safe_webhook["auth_token"]:
            safe_webhook["auth_token"] = "***MASKED***"
        safe_webhooks.append(safe_webhook)

    return {"success": True, "webhooks": safe_webhooks}


@app.post("/api/webhooks/{webhook_id}/test")
async def test_webhook_endpoint(webhook_id: str, current_user: dict = Depends(require_role("admin"))):
    """Test a webhook (admin only)."""
    from web.webhook_alerts import test_webhook
    result = test_webhook(webhook_id)
    return result


@app.post("/api/webhooks/{webhook_id}/send")
async def send_webhook_notification(webhook_id: str, request: Request, current_user: dict = Depends(get_current_user)):
    """Send a webhook notification."""
    from web.webhook_alerts import send_webhook
    data = await request.json()

    result = send_webhook(
        webhook_id=webhook_id,
        title=data.get("title", ""),
        message=data.get("message", ""),
        data=data.get("data"),
        severity=data.get("severity", "info"),
        color=data.get("color")
    )

    return result


@app.get("/api/webhooks/history")
async def get_webhook_history(limit: int = 100, current_user: dict = Depends(get_current_user)):
    """Get webhook execution history."""
    from web.webhook_alerts import load_history
    history = load_history(limit)
    return {"success": True, "history": history}


@app.delete("/api/webhooks/history")
async def clear_webhook_history(current_user: dict = Depends(require_role("admin"))):
    """Clear webhook execution history (admin only)."""
    from web.webhook_alerts import clear_history
    success = clear_history()
    return {"success": success, "message": "History cleared" if success else "Failed to clear history"}


# ==================== BRAND CUSTOMIZATION APIS ====================

@app.get("/api/brand/config")
async def get_brand_config(current_user: dict = Depends(get_current_user)):
    """Get brand configuration."""
    from web.brand_customization import load_config
    config = load_config()
    return {"success": True, "config": config}


@app.post("/api/brand/config")
async def update_brand_config(request: Request, current_user: dict = Depends(require_role("admin"))):
    """Update brand configuration (admin only)."""
    from web.brand_customization import load_config, save_config
    data = await request.json()

    config = load_config()

    # Update allowed fields
    allowed_fields = [
        "company_name", "app_title", "footer_text", "show_footer",
        "login_message", "enable_custom_theme", "custom_css",
        "primary_color", "secondary_color", "accent_color",
        "success_color", "warning_color", "error_color",
        "sidebar_bg", "panel_bg", "border_color"
    ]

    for field in allowed_fields:
        if field in data:
            config[field] = data[field]

    from web.brand_customization import save_config
    if save_config(config):
        return {"success": True, "config": config}
    else:
        return {"success": False, "error": "Failed to save configuration"}


@app.post("/api/brand/logo/upload")
async def upload_logo(request: Request, current_user: dict = Depends(require_role("admin"))):
    """Upload logo image (admin only)."""
    from web.brand_customization import save_logo
    data = await request.json()

    logo_type = data.get("logo_type", "light")  # light, dark, or favicon
    image_data = data.get("image_data", "")

    if not image_data:
        return {"success": False, "error": "No image data provided"}

    result = save_logo(image_data, logo_type)
    return result


@app.delete("/api/brand/logo/{logo_type}")
async def delete_logo_endpoint(logo_type: str, current_user: dict = Depends(require_role("admin"))):
    """Delete logo (admin only)."""
    from web.brand_customization import delete_logo

    if logo_type not in ["light", "dark", "favicon"]:
        return {"success": False, "error": "Invalid logo type"}

    result = delete_logo(logo_type)
    return result


@app.get("/api/brand/assets/{filename}")
async def get_brand_asset(filename: str):
    """Serve brand asset files."""
    from web.brand_customization import get_asset_path
    from fastapi.responses import FileResponse

    filepath = get_asset_path(filename)
    if filepath:
        # Determine media type from extension
        media_types = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".svg": "image/svg+xml",
            ".ico": "image/x-icon"
        }

        ext = os.path.splitext(filename)[1].lower()
        media_type = media_types.get(ext, "application/octet-stream")

        return FileResponse(filepath, media_type=media_type)
    else:
        raise HTTPException(status_code=404, detail="Asset not found")


@app.post("/api/brand/theme/colors")
async def update_theme_colors(request: Request, current_user: dict = Depends(require_role("admin"))):
    """Update theme colors (admin only)."""
    from web.brand_customization import update_theme_colors
    data = await request.json()

    result = update_theme_colors(data)
    return result


@app.post("/api/brand/reset")
async def reset_brand_config(current_user: dict = Depends(require_role("admin"))):
    """Reset brand configuration to defaults (admin only)."""
    from web.brand_customization import reset_to_defaults
    result = reset_to_defaults()
    return result


@app.get("/api/brand/css")
async def get_brand_css():
    """Get custom CSS variables for branding."""
    from web.brand_customization import get_css_variables
    css = get_css_variables()
    return Response(content=css, media_type="text/css")


# ==================== DASHBOARD EMBED APIS ====================

@app.get("/embed/dashboard/{dashboard_id}")
async def get_embedded_dashboard(dashboard_id: str, request: Request, theme: str = "auto"):
    """Serve dashboard in embeddable iframe view (minimal UI)."""
    templates = Jinja2Templates(directory="web/templates")

    # Load dashboard
    dashboards = load_dashboards_store()
    dashboard = next((d for d in dashboards if d["id"] == dashboard_id), None)

    if not dashboard:
        return HTMLResponse(content="<html><body><h1>Dashboard not found</h1></body></html>", status_code=404)

    # Return embedded view with minimal UI
    return templates.TemplateResponse("embed.html", {
        "request": request,
        "dashboard": dashboard,
        "dashboard_id": dashboard_id,
        "theme": theme
    })


@app.get("/api/dashboards/{dashboard_id}/embed-code")
async def get_dashboard_embed_code(
    dashboard_id: str,
    width: str = "100%",
    height: str = "600px",
    theme: str = "auto",
    current_user: dict = Depends(get_current_user)
):
    """Generate iframe embed code for dashboard."""
    from web.dashboard_permissions import can_view_dashboard

    # Check if user can view dashboard
    if not can_view_dashboard(dashboard_id, current_user["username"], current_user["role"]):
        raise HTTPException(status_code=403, detail="You don't have permission to embed this dashboard")

    # Get base URL from environment or use localhost
    base_url = os.getenv("BASE_URL", "http://localhost:8891")

    # Generate embed URL
    embed_url = f"{base_url}/embed/dashboard/{dashboard_id}?theme={theme}"

    # Generate iframe code
    iframe_code = f'''<iframe
  src="{embed_url}"
  width="{width}"
  height="{height}"
  frameborder="0"
  style="border: none; border-radius: 8px;"
  allowfullscreen>
</iframe>'''

    # Generate responsive version
    responsive_code = f'''<div style="position: relative; padding-bottom: 56.25%; height: 0; overflow: hidden;">
  <iframe
    src="{embed_url}"
    style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; border: none; border-radius: 8px;"
    frameborder="0"
    allowfullscreen>
  </iframe>
</div>'''

    return {
        "success": True,
        "embed_url": embed_url,
        "iframe_code": iframe_code,
        "responsive_code": responsive_code,
        "dashboard_id": dashboard_id
    }


# ==================== INCREMENTAL REFRESH APIS ====================

@app.get("/api/incremental/watermarks/{widget_id}")
async def get_widget_watermark(widget_id: str, current_user: dict = Depends(get_current_user)):
    """Get watermark data for a widget."""
    from web.incremental_refresh import get_watermark
    watermark = get_watermark(widget_id)
    return {"success": True, "watermark": watermark}

@app.delete("/api/incremental/watermarks/{widget_id}")
async def clear_widget_watermark(widget_id: str, current_user: dict = Depends(get_current_user)):
    """Clear watermark for a widget (force full refresh)."""
    from web.incremental_refresh import clear_watermark
    success = clear_watermark(widget_id)
    return {"success": success, "message": "Watermark cleared" if success else "Watermark not found"}

@app.post("/api/incremental/detect-columns")
async def detect_timestamp_columns(rows: List[Dict[str, Any]], current_user: dict = Depends(get_current_user)):
    """Detect timestamp columns from query results."""
    from web.incremental_refresh import detect_timestamp_columns
    columns = detect_timestamp_columns(rows)
    return {"success": True, "timestamp_columns": columns}

@app.get("/api/incremental/stats")
async def get_incremental_stats(current_user: dict = Depends(get_current_user)):
    """Get incremental refresh statistics."""
    from web.incremental_refresh import get_watermark_stats
    stats = get_watermark_stats()
    return {"success": True, "stats": stats}

# ==================== WIDGET EXPORT APIS ====================

@app.post("/api/widgets/{widget_id}/export/csv")
async def export_widget_as_csv(
    widget_id: str,
    widget_title: str,
    rows: List[Dict[str, Any]],
    current_user: dict = Depends(get_current_user)
):
    """Export widget data as CSV."""
    from web.widget_export import export_widget_csv
    result = export_widget_csv(widget_id, widget_title, rows)
    if result["success"]:
        from fastapi.responses import FileResponse
        return FileResponse(
            path=result["filepath"],
            filename=result["filename"],
            media_type="text/csv"
        )
    else:
        raise HTTPException(status_code=400, detail=result.get("error", "Export failed"))

@app.post("/api/widgets/{widget_id}/export/parquet")
async def export_widget_as_parquet(
    widget_id: str,
    widget_title: str,
    rows: List[Dict[str, Any]],
    current_user: dict = Depends(get_current_user)
):
    """Export widget data as Parquet."""
    from web.widget_export import export_widget_parquet
    result = export_widget_parquet(widget_id, widget_title, rows)
    if result["success"]:
        from fastapi.responses import FileResponse
        return FileResponse(
            path=result["filepath"],
            filename=result["filename"],
            media_type="application/octet-stream"
        )
    else:
        raise HTTPException(status_code=400, detail=result.get("error", "Export failed"))

@app.post("/api/widgets/{widget_id}/export/json")
async def export_widget_as_json(
    widget_id: str,
    widget_title: str,
    rows: List[Dict[str, Any]],
    current_user: dict = Depends(get_current_user)
):
    """Export widget data as JSON."""
    from web.widget_export import export_widget_json
    result = export_widget_json(widget_id, widget_title, rows)
    if result["success"]:
        from fastapi.responses import FileResponse
        return FileResponse(
            path=result["filepath"],
            filename=result["filename"],
            media_type="application/json"
        )
    else:
        raise HTTPException(status_code=400, detail=result.get("error", "Export failed"))

@app.post("/api/widgets/{widget_id}/export/excel")
async def export_widget_as_excel(
    widget_id: str,
    widget_title: str,
    rows: List[Dict[str, Any]],
    current_user: dict = Depends(get_current_user)
):
    """Export widget data as Excel."""
    from web.widget_export import export_widget_excel
    result = export_widget_excel(widget_id, widget_title, rows)
    if result["success"]:
        from fastapi.responses import FileResponse
        return FileResponse(
            path=result["filepath"],
            filename=result["filename"],
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        raise HTTPException(status_code=400, detail=result.get("error", "Export failed"))

@app.post("/api/widgets/{widget_id}/export/png")
async def export_chart_as_png(
    widget_id: str,
    widget_title: str,
    image_data: str,
    current_user: dict = Depends(get_current_user)
):
    """Export chart as PNG."""
    from web.widget_export import export_chart_png
    result = export_chart_png(widget_id, widget_title, image_data)
    if result["success"]:
        from fastapi.responses import FileResponse
        return FileResponse(
            path=result["filepath"],
            filename=result["filename"],
            media_type="image/png"
        )
    else:
        raise HTTPException(status_code=400, detail=result.get("error", "Export failed"))

@app.post("/api/widgets/{widget_id}/export/svg")
async def export_chart_as_svg(
    widget_id: str,
    widget_title: str,
    svg_content: str,
    current_user: dict = Depends(get_current_user)
):
    """Export chart as SVG."""
    from web.widget_export import export_chart_svg
    result = export_chart_svg(widget_id, widget_title, svg_content)
    if result["success"]:
        from fastapi.responses import FileResponse
        return FileResponse(
            path=result["filepath"],
            filename=result["filename"],
            media_type="image/svg+xml"
        )
    else:
        raise HTTPException(status_code=400, detail=result.get("error", "Export failed"))

@app.get("/api/widgets/exports/history")
async def get_widget_export_history(limit: int = 20, current_user: dict = Depends(get_current_user)):
    """Get widget export history."""
    from web.widget_export import get_export_history
    history = get_export_history(limit)
    return {"success": True, "history": history}

@app.post("/api/widgets/exports/cleanup")
async def cleanup_widget_exports(days: int = 7, current_user: dict = Depends(require_role("admin"))):
    """Cleanup old widget exports (admin only)."""
    from web.widget_export import cleanup_old_exports
    result = cleanup_old_exports(days)
    return result

class PreviewWidgetRequest(BaseModel):
    query: str

@app.post("/api/dashboards/preview-widget")
async def preview_widget(payload: PreviewWidgetRequest, request: Request):
    current_user = await resolve_principal(request)
    conn = get_duckrun_conn()
    exec_res = execute_widget_query(conn, payload.query.strip(), principal=current_user)
    return exec_res


# ==================== SAVED QUERIES APIS ====================

class SavedQueryCreateRequest(BaseModel):
    name: str
    description: Optional[str] = ""
    query_text: str
    warehouse_id: Optional[str] = "wh_starter"
    catalog: Optional[str] = "warehouse"
    schema_name: Optional[str] = "dbo"
    tags: Optional[List[str]] = []

class SavedQueryUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    query_text: Optional[str] = None
    warehouse_id: Optional[str] = None
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    tags: Optional[List[str]] = None

def _saved_query_access(q: Dict[str, Any], user: Dict[str, Any]) -> Optional[str]:
    """owner | edit | view | None for a user on a saved query. Sharing (web/groups.py) can grant VIEW or EDIT to users and groups; only the
    owner or an admin changes who it is shared with or deletes it. Starter/default queries are visible to everyone and editable by admins."""
    from web.groups import permission_of
    from web.saved_queries import get_default_saved_queries
    if user.get("role") == "admin":
        return "owner"
    name = user.get("username", "")
    if name and name in (q.get("owner"), q.get("created_by")):
        return "owner"
    granted = permission_of(user, "saved_query", q.get("id", ""))
    if granted == "EDIT":
        return "edit"
    if granted == "VIEW" or q.get("is_starter") or q.get("id") in {d["id"] for d in get_default_saved_queries()}:
        return "view"
    return None


@app.get("/api/queries")
async def list_saved_queries(request: Request, q: Optional[str] = None, tag: Optional[str] = None):
    from web.saved_queries import get_saved_queries
    from web import groups
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    try:
        queries = get_saved_queries(q=q, tag=tag, user_id=username, is_admin=is_admin,
                                    shared_ids=groups.granted_ids(current_user, "saved_query", "VIEW"))
        return {"queries": [{**item, "my_access": _saved_query_access(item, current_user)} for item in queries]}
    except Exception as e:
        logger.error(f"Failed to list saved queries: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/queries")
async def create_new_saved_query(payload: SavedQueryCreateRequest, request: Request):
    from web.saved_queries import create_saved_query
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    try:
        d = payload.dict()
        d["owner"] = username
        d["created_by"] = username
        new_q = create_saved_query(d)
        return new_q
    except Exception as e:
        logger.error(f"Failed to create saved query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/queries/{query_id}")
async def get_single_saved_query(query_id: str, request: Request):
    from web.saved_queries import get_saved_query
    current_user = await resolve_principal(request)
    q = get_saved_query(query_id)
    access = _saved_query_access(q, current_user) if q else None
    if not q or not access:
        raise HTTPException(status_code=404, detail="Saved query not found")
    return {**q, "my_access": access}

@app.put("/api/queries/{query_id}")
async def update_existing_saved_query(query_id: str, payload: SavedQueryUpdateRequest, request: Request):
    from web.saved_queries import update_saved_query, get_saved_query
    current_user = await resolve_principal(request)
    q = get_saved_query(query_id)
    access = _saved_query_access(q, current_user) if q else None
    if not q or not access:
        raise HTTPException(status_code=404, detail="Saved query not found")
    if access not in ("owner", "edit") or (q.get("is_starter") and current_user.get("role") != "admin"):
        raise HTTPException(status_code=403, detail="You can view this query but not change it.")
    data_dict = {k: v for k, v in payload.dict().items() if v is not None}
    updated = update_saved_query(query_id, data_dict)
    if not updated:
        raise HTTPException(status_code=404, detail="Saved query not found")
    return updated

@app.delete("/api/queries/{query_id}")
async def delete_existing_saved_query(query_id: str, request: Request):
    from web.saved_queries import delete_saved_query, get_saved_query
    from web import groups
    current_user = await resolve_principal(request)
    q = get_saved_query(query_id)
    access = _saved_query_access(q, current_user) if q else None
    if not q or not access:
        raise HTTPException(status_code=404, detail="Saved query not found")
    if access != "owner":
        raise HTTPException(status_code=403, detail="Only the owner (or an administrator) can delete a saved query.")
    deleted = delete_saved_query(query_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Saved query not found")
    groups.delete_grants_for_resource("saved_query", query_id)
    return {"success": True, "deleted_query_id": query_id}

@app.post("/api/queries/{query_id}/duplicate")
async def duplicate_existing_saved_query(query_id: str, request: Request):
    from web.saved_queries import duplicate_saved_query, get_saved_query
    current_user = await resolve_principal(request)
    q = get_saved_query(query_id)
    if not q or not _saved_query_access(q, current_user):
        raise HTTPException(status_code=404, detail="Saved query not found")
    cloned = duplicate_saved_query(query_id, owner=current_user.get("username", "admin"))
    if not cloned:
        raise HTTPException(status_code=404, detail="Saved query not found")
    return cloned


# ==================== QUERY HISTORY & AUDIT LOGGING APIS ====================

@app.get("/api/history")
async def list_query_history(
    request: Request,
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = None,
    client: Optional[str] = None,
    search: Optional[str] = None,
    min_duration_ms: Optional[float] = None,
    user: Optional[str] = None
):
    from web.auth import get_current_user
    current_user = await resolve_principal(request)

    target_user = user
    if current_user and current_user.get("role") == "user":
        target_user = current_user.get("username", "admin")

    res = get_query_history(
        limit=limit,
        offset=offset,
        status=status,
        client=client,
        search=search,
        min_duration_ms=min_duration_ms,
        user=target_user
    )
    res["current_user_role"] = current_user.get("role", "admin") if current_user else "admin"
    res["active_user_filter"] = target_user or "ALL"
    return res

class QualifySqlPayload(BaseModel):
    sql: str
    catalog: Optional[str] = None


@app.post("/api/sql/qualify")
async def qualify_sql_endpoint(payload: QualifySqlPayload, current_user: Dict[str, Any] = Depends(get_current_user)):
    """Completes table names to catalog.schema.table by editing the text in place (formatting and comments stay). Nothing is executed."""
    from web import sql_qualify
    if len(payload.sql) > 200_000:
        raise HTTPException(status_code=413, detail="The SQL is too large.")
    try:
        return await asyncio.to_thread(sql_qualify.qualify, payload.sql, payload.catalog)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@app.get("/api/history/{query_id}")
async def get_single_query_history(query_id: str):
    record = get_query_by_id(query_id)
    if not record:
        raise HTTPException(status_code=404, detail="Query audit record not found")
    return record

@app.get("/api/history/{query_id}/profile")
async def get_history_query_profile(query_id: str, request: Request):
    profile_user = await resolve_principal(request)
    record = get_query_by_id(query_id)
    if not record:
        raise HTTPException(status_code=404, detail="Query audit record not found")

    profile_json = record.get("profile_json")
    if profile_json:
        try:
            return {"success": True, "query_id": query_id, "profile": json.loads(profile_json)}
        except Exception:
            pass

    # If not previously profiled, execute profiled query on-demand
    conn = get_duckrun_conn()
    governed = await asyncio.to_thread(gov_gateway.govern_sql, record["query_text"], profile_user, client="history-profile")
    if governed.blocked:
        return {"success": False, "error": governed.blocked}
    res = execute_profiled_query(conn, governed.sql)
    if res.get("profile"):
        save_query_profile(query_id, json.dumps(res["profile"]))
        return {"success": True, "query_id": query_id, "profile": res["profile"]}
    else:
        return {"success": False, "error": res.get("error", "Profiling unavailable for this query")}

def _audit_history_deletion(actor: str, action: str, detail: Dict[str, Any]) -> None:
    """The query history is an audit trail, so removing from it is itself recorded (who, how many, which ids; never the SQL)."""
    from web.governance import store
    store.init_governance_db()
    conn = store.get_db()
    try:
        store.write_audit(conn, actor, action, "query_history", detail)
        conn.commit()
    finally:
        conn.close()


class HistoryDeletePayload(BaseModel):
    query_ids: List[str]


@app.delete("/api/history")
async def clear_history(current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Removes the entire history (admins only)."""
    from web.audit import get_query_history
    total = get_query_history(limit=1).get("total_count")
    clear_query_history()
    _audit_history_deletion(current_user.get("username", "admin"), "HISTORY_CLEAR", {"records": total})
    return {"success": True, "message": "Query history cleared"}

@app.post("/api/history/delete")
async def delete_history_entries(payload: HistoryDeletePayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Removes the selected history entries (admins only). Everyone else's history is append-only."""
    from web.audit import delete_queries
    if not payload.query_ids:
        raise HTTPException(status_code=400, detail="No queries selected.")
    if len(payload.query_ids) > 5000:
        raise HTTPException(status_code=413, detail="Too many queries in one request (max 5000).")
    deleted = await asyncio.to_thread(delete_queries, payload.query_ids)
    _audit_history_deletion(current_user.get("username", "admin"), "HISTORY_DELETE", {"deleted": deleted, "query_ids": payload.query_ids[:200]})
    return {"success": True, "deleted": deleted}

class ManualLogPayload(BaseModel):
    query_text: str
    duration_ms: float
    rows_produced: Optional[int] = 0
    status: Optional[str] = "SUCCESS"
    error_message: Optional[str] = None
    client: Optional[str] = "NOTEBOOK"
    is_mutation: Optional[bool] = False
    user: Optional[str] = "admin"

@app.post("/api/history/log")
async def manual_log_query(payload: ManualLogPayload):
    qid = log_query(
        query_text=payload.query_text,
        duration_ms=payload.duration_ms,
        rows_produced=payload.rows_produced or 0,
        status=payload.status or "SUCCESS",
        error_message=payload.error_message,
        client=payload.client or "NOTEBOOK",
        is_mutation=payload.is_mutation or False,
        user=payload.user or "admin"
    )
    return {"success": True, "query_id": qid}

# ==================== JOBS & PIPELINES (WORKFLOWS) APIS ====================

@app.get("/api/jobs")
async def list_jobs_endpoint():
    jobs = load_jobs()
    enriched = []
    for j in jobs:
        j_copy = dict(j)
        runs = get_job_runs(job_id=j["id"], limit=1)
        j_copy["last_run"] = runs[0] if runs else None
        enriched.append(j_copy)
    return {"jobs": enriched}

@app.post("/api/jobs")
async def save_job_endpoint(payload: Dict[str, Any], request: Request):
    user = await resolve_principal(request)
    existing = get_job(payload.get("id")) if payload.get("id") else None
    if existing and existing.get("created_by") and user.get("role") != "admin" and existing["created_by"] != user.get("username"):
        raise HTTPException(status_code=403, detail="Only the job's owner or an admin can change it.")
    # Ownership is server-side: jobs run as their owner, so the client must not be able to name one.
    payload["created_by"] = (existing or {}).get("created_by") or user.get("username")
    saved = create_or_update_job(payload)
    return saved

@app.get("/api/jobs/{job_id}")
async def get_job_endpoint(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    runs = get_job_runs(job_id=job_id, limit=20)
    return {"job": job, "runs": runs}

@app.delete("/api/jobs/{job_id}")
async def delete_job_endpoint(job_id: str, request: Request):
    user = await resolve_principal(request)
    existing = get_job(job_id)
    if existing and existing.get("created_by") and user.get("role") != "admin" and existing["created_by"] != user.get("username"):
        raise HTTPException(status_code=403, detail="Only the job's owner or an admin can delete it.")
    ok = delete_job(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"success": True, "deleted_id": job_id}

@app.post("/api/jobs/{job_id}/run")
async def trigger_job_run_endpoint(job_id: str, request: Request):
    await resolve_principal(request)          # authentication only: the job itself runs as its owner
    try:
        res = run_pipeline(job_id, trigger="MANUAL")
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/jobs/{job_id}/runs")
async def list_job_runs_endpoint(job_id: str, limit: int = 50):
    return {"runs": get_job_runs(job_id=job_id, limit=limit)}

@app.get("/api/jobs/runs/{run_id}")
async def get_single_run_endpoint(run_id: str):
    detail = get_run_detail(run_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Run not found")
    return detail

# ==================== DBT CORE / TRANSFORMATIONS APIS ====================

class DbtRunRequest(BaseModel):
    action: str = "run"
    select: Optional[str] = None
    full_refresh: bool = False
    target: str = "dev"

@app.get("/api/dbt/status")
async def get_dbt_status_endpoint(request: Request):
    from web.dbt_service import get_dbt_status
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    return get_dbt_status(user=username, is_admin=is_admin)

@app.get("/api/dbt/models")
async def list_dbt_models_endpoint():
    from web.dbt_service import list_dbt_models
    from web import dbt_governance
    data = list_dbt_models()
    if dbt_governance.enabled():
        catalog = dbt_governance._dest_catalog()
        for m in data.get("models", []):
            if catalog and m.get("materialization") in dbt_governance.OUTPUT_MATERIALIZATIONS:
                try:
                    m["lakehouse_table"] = f'{catalog}.{m["schema"]}.{m.get("alias") or m["name"]}'
                    m["access"] = dbt_governance.access_state(m["schema"], m.get("alias") or m["name"])
                except Exception as exc:
                    logger.debug(f"access state of dbt model {m.get('name')} unavailable: {exc}")
    return data

class DbtConfigPayload(BaseModel):
    content: str


@app.get("/api/dbt/config")
async def list_dbt_config_files(current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """The dbt project's editable configuration files (admins only: they decide what dbt does as the system)."""
    from web import dbt_config
    return {"files": [dbt_config.read(n) for n in dbt_config.FILES],
            "s3_mounts": [{"id": m["id"], "name": m.get("name") or m["id"], "bucket": (m.get("config") or {}).get("bucket"),
                           "catalog": m.get("catalog_name")} for m in dbt_config.s3_mounts()]}

class DbtSettingsPayload(BaseModel):
    profiles: str
    project: str
    settings: Optional[Dict[str, Any]] = None


@app.post("/api/dbt/config/settings")
async def read_dbt_settings(payload: DbtSettingsPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """The few settings the guided form edits, read from the given (possibly unsaved) file texts."""
    from web import dbt_config
    try:
        return dbt_config.read_settings(payload.profiles, payload.project)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read the settings from these files: {exc}")

@app.post("/api/dbt/config/settings/apply")
async def apply_dbt_settings(payload: DbtSettingsPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Applies the form's settings to the given file texts as minimal text edits (comments kept). Returns the proposed files;
    nothing is saved until they go through the normal validated save."""
    from web import dbt_config
    try:
        return dbt_config.apply_settings(payload.profiles, payload.project, payload.settings or {})
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@app.post("/api/dbt/config/s3-profile/{mount_id}")
async def dbt_s3_profile_endpoint(mount_id: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """profiles.yml with its target pointed at an S3 mount (returned for review, not saved)."""
    from web import dbt_config
    try:
        return {"name": "profiles.yml", "content": dbt_config.s3_profile(mount_id)}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@app.get("/api/dbt/config/{name}/versions/{version_id}")
async def get_dbt_config_version(name: str, version_id: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import dbt_config
    try:
        return dbt_config.read_version(name, version_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

@app.post("/api/dbt/config/{name}/validate")
async def validate_dbt_config(name: str, payload: DbtConfigPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import dbt_config
    try:
        return await asyncio.to_thread(dbt_config.validate, name, payload.content)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

@app.put("/api/dbt/config/{name}")
async def save_dbt_config(name: str, payload: DbtConfigPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Validates (YAML, cross-checks, and `dbt parse` on a scratch copy), keeps the previous version and audits. Invalid = nothing written."""
    from web import dbt_config
    try:
        return await asyncio.to_thread(dbt_config.save, name, payload.content, current_user.get("username", "admin"))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except dbt_config.ConfigError as exc:
        raise HTTPException(status_code=422, detail={"message": "The configuration was not saved.", "errors": exc.errors})

class GitCommitPayload(BaseModel):
    message: str
    branch: Optional[str] = None            # pull-request mode: name of the change branch a first commit opens (default dkw/<user>-<time>)


class GitPrPayload(BaseModel):
    title: Optional[str] = ""
    body: Optional[str] = ""


class GitChangePayload(BaseModel):
    name: Optional[str] = None


async def _git_call(fn, *args):
    from web import git_sync
    try:
        return await asyncio.to_thread(fn, *args)
    except git_sync.GitError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


def _git_repo(kind: str):
    from web import git_sync
    try:
        return git_sync.get(kind)
    except git_sync.GitError:
        raise HTTPException(status_code=404, detail="Unknown repository.")


# Two repositories share these routes: `dbt` (the dbt project) and `notebooks` (notebooks/Shared only, never the private Users/ folders).
# The original un-prefixed paths keep meaning `dbt`.
@app.get("/api/git/status")
async def git_status_endpoint(fetch: bool = False, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Sync state of the dbt project with its remote repository (admins only). `fetch=true` asks the remote first."""
    return await _git_call(_git_repo("dbt").status, fetch)

@app.post("/api/git/connect")
async def git_connect_endpoint(current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo("dbt").connect, current_user.get("username", "admin"))

@app.post("/api/git/pull")
async def git_pull_endpoint(current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo("dbt").pull, current_user.get("username", "admin"))

@app.post("/api/git/commit")
async def git_commit_endpoint(payload: GitCommitPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo("dbt").commit, current_user.get("username", "admin"), payload.message, payload.branch)

@app.post("/api/git/push")
async def git_push_endpoint(current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo("dbt").push, current_user.get("username", "admin"))

@app.get("/api/git/repos/{kind}/status")
async def git_repo_status_endpoint(kind: str, fetch: bool = False, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo(kind).status, fetch)

@app.post("/api/git/repos/{kind}/connect")
async def git_repo_connect_endpoint(kind: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo(kind).connect, current_user.get("username", "admin"))

@app.post("/api/git/repos/{kind}/pull")
async def git_repo_pull_endpoint(kind: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo(kind).pull, current_user.get("username", "admin"))

@app.post("/api/git/repos/{kind}/commit")
async def git_repo_commit_endpoint(kind: str, payload: GitCommitPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo(kind).commit, current_user.get("username", "admin"), payload.message, payload.branch)

# Pull-request mode (GIT_MODE=pull_request): change branches and pull requests on the forge (Gitea API).
@app.post("/api/git/repos/{kind}/change")
async def git_repo_start_change_endpoint(kind: str, payload: GitChangePayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo(kind).start_change, current_user.get("username", "admin"), payload.name)

@app.post("/api/git/repos/{kind}/pr")
async def git_repo_open_pr_endpoint(kind: str, payload: GitPrPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo(kind).open_pull_request, current_user.get("username", "admin"), payload.title or "", payload.body or "")

@app.post("/api/git/repos/{kind}/finish")
async def git_repo_finish_endpoint(kind: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo(kind).finish_change, current_user.get("username", "admin"))

@app.post("/api/git/repos/{kind}/abandon")
async def git_repo_abandon_endpoint(kind: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo(kind).abandon_change, current_user.get("username", "admin"))

@app.post("/api/git/repos/{kind}/update")
async def git_repo_update_endpoint(kind: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo(kind).update_from_base, current_user.get("username", "admin"))

@app.post("/api/git/repos/{kind}/push")
async def git_repo_push_endpoint(kind: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    return await _git_call(_git_repo(kind).push, current_user.get("username", "admin"))

@app.post("/api/dbt/models/{model_name}/open")
async def open_dbt_model_endpoint(model_name: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """An administrator deliberately opens one dbt table to users (refreshes its carried-over source tags first)."""
    from web import dbt_governance
    try:
        return await asyncio.to_thread(dbt_governance.open_model, model_name, current_user.get("username", "admin"))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@app.post("/api/dbt/models/{model_name}/close")
async def close_dbt_model_endpoint(model_name: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import dbt_governance
    try:
        return await asyncio.to_thread(dbt_governance.close_model, model_name, current_user.get("username", "admin"))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@app.get("/api/dbt/models/{model_name}")
async def get_dbt_model_endpoint(model_name: str):
    from web.dbt_service import get_dbt_model_detail
    detail = get_dbt_model_detail(model_name)
    if not detail:
        raise HTTPException(status_code=404, detail="Model not found")
    return detail

@app.post("/api/dbt/run")
async def run_dbt_endpoint(payload: DbtRunRequest, request: Request):
    from web.dbt_service import run_dbt_cli
    current_user = await resolve_principal(request)
    try:
        # dbt executes model SQL as the system and materialises raw results into an ungoverned database
        gov_gateway.deny_if_subject(current_user, "Running dbt")
    except GovernanceBlocked as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    username = current_user.get("username", "admin")
    res = run_dbt_cli(
        action=payload.action,
        select=payload.select,
        full_refresh=payload.full_refresh,
        target=payload.target,
        user=username
    )
    return res

@app.get("/api/dbt/runs")
async def list_dbt_runs_endpoint(request: Request):
    from web.dbt_service import _load_runs_history
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    return {"runs": _load_runs_history(user=username, is_admin=is_admin)}

@app.get("/api/dbt/runs/{run_id}")
async def get_dbt_run_endpoint(run_id: str, request: Request):
    from web.dbt_service import _load_runs_history
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    runs = _load_runs_history(user=username, is_admin=is_admin)
    matched = next((r for r in runs if r["run_id"] == run_id), None)
    if not matched:
        raise HTTPException(status_code=404, detail="Run record not found")
    return matched

@app.get("/api/dbt/preview/{model_name}")
async def preview_dbt_model_endpoint(model_name: str, request: Request, limit: int = 50):
    try:
        gov_gateway.deny_if_subject(await resolve_principal(request), "dbt model preview")
    except GovernanceBlocked as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    from web.dbt_service import preview_dbt_model_data
    return preview_dbt_model_data(model_name, limit=limit)

@app.get("/api/dbt/cte-preview/{model_name}/{cte_name}")
async def preview_dbt_cte_endpoint(model_name: str, cte_name: str, request: Request, limit: int = 50):
    try:
        gov_gateway.deny_if_subject(await resolve_principal(request), "dbt CTE preview")
    except GovernanceBlocked as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    from web.dbt_service import preview_cte_step
    return preview_cte_step(model_name, cte_name, limit=limit)

class DbtAddTestRequest(BaseModel):
    model_name: str
    column_name: Optional[str] = None
    test_type: str = "not_null"
    parameters: Optional[Dict[str, Any]] = None
    sql_text: Optional[str] = None
    test_name: Optional[str] = None

class DbtDeleteTestRequest(BaseModel):
    model_name: str
    column_name: Optional[str] = None
    test_type: Optional[str] = None
    test_name: Optional[str] = None

@app.post("/api/dbt/tests")
async def add_dbt_test_endpoint(payload: DbtAddTestRequest):
    from web.dbt_service import add_dbt_test
    try:
        res = add_dbt_test(
            model_name=payload.model_name,
            column_name=payload.column_name,
            test_type=payload.test_type,
            parameters=payload.parameters,
            sql_text=payload.sql_text,
            test_name=payload.test_name
        )
        return res
    except Exception as e:
        logger.error(f"Error adding dbt test: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/dbt/tests")
async def delete_dbt_test_endpoint(payload: DbtDeleteTestRequest):
    from web.dbt_service import delete_dbt_test
    try:
        res = delete_dbt_test(
            model_name=payload.model_name,
            column_name=payload.column_name,
            test_type=payload.test_type,
            test_name=payload.test_name
        )
        return res
    except Exception as e:
        logger.error(f"Error deleting dbt test: {e}")
        raise HTTPException(status_code=400, detail=str(e))

class DbtSourceAddRequest(BaseModel):
    source_name: str = "lakehouse"
    table_name: str
    description: Optional[str] = None
    external_location: Optional[str] = None

class DbtSourceUpdateRequest(BaseModel):
    source_name: str
    table_name: str
    new_table_name: Optional[str] = None
    description: Optional[str] = None

class DbtSourceDeleteRequest(BaseModel):
    source_name: str
    table_name: str

class DbtModelCreateRequest(BaseModel):
    name: str
    layer: str = "staging"
    materialization: str = "view"
    sql_content: Optional[str] = None
    description: Optional[str] = ""

class DbtModelUpdateRequest(BaseModel):
    sql_content: str
    description: Optional[str] = None

@app.get("/api/dbt/available-delta-tables")
async def list_available_delta_tables_endpoint():
    from web.dbt_service import list_available_delta_tables
    return {"tables": list_available_delta_tables()}

@app.post("/api/dbt/sources")
async def add_dbt_source_endpoint(payload: DbtSourceAddRequest):
    from web.dbt_service import add_dbt_source
    try:
        res = add_dbt_source(
            source_name=payload.source_name,
            table_name=payload.table_name,
            description=payload.description,
            external_location=payload.external_location
        )
        return res
    except Exception as e:
        logger.error(f"Error adding dbt source: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.put("/api/dbt/sources")
async def update_dbt_source_endpoint(payload: DbtSourceUpdateRequest):
    from web.dbt_service import update_dbt_source
    try:
        res = update_dbt_source(
            source_name=payload.source_name,
            table_name=payload.table_name,
            new_table_name=payload.new_table_name,
            description=payload.description
        )
        return res
    except Exception as e:
        logger.error(f"Error updating dbt source: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/dbt/sources")
async def delete_dbt_source_endpoint(payload: DbtSourceDeleteRequest):
    from web.dbt_service import delete_dbt_source
    try:
        res = delete_dbt_source(
            source_name=payload.source_name,
            table_name=payload.table_name
        )
        return res
    except Exception as e:
        logger.error(f"Error deleting dbt source: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/dbt/sources/{source_name}/{table_name}/preview")
async def preview_dbt_source_endpoint(source_name: str, table_name: str, request: Request, limit: int = 50):
    try:
        gov_gateway.deny_if_subject(await resolve_principal(request), "dbt source preview")
    except GovernanceBlocked as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    from web.dbt_service import preview_dbt_source
    return preview_dbt_source(source_name=source_name, table_name=table_name, limit=limit)

@app.post("/api/dbt/models")
async def create_dbt_model_endpoint(payload: DbtModelCreateRequest):
    from web.dbt_service import create_dbt_model
    try:
        res = create_dbt_model(
            name=payload.name,
            layer=payload.layer,
            materialization=payload.materialization,
            sql_content=payload.sql_content,
            description=payload.description
        )
        return res
    except Exception as e:
        logger.error(f"Error creating dbt model: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.put("/api/dbt/models/{model_name}")
async def update_dbt_model_code_endpoint(model_name: str, payload: DbtModelUpdateRequest):
    from web.dbt_service import update_dbt_model_code
    try:
        res = update_dbt_model_code(
            model_name=model_name,
            sql_content=payload.sql_content,
            description=payload.description
        )
        return res
    except Exception as e:
        logger.error(f"Error updating dbt model: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/dbt/models/{model_name}")
async def delete_dbt_model_endpoint(model_name: str):
    from web.dbt_service import delete_dbt_model
    try:
        res = delete_dbt_model(model_name=model_name)
        return res
    except Exception as e:
        logger.error(f"Error deleting dbt model: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ==================== DATABRICKS GENIE (TEXT-TO-SQL) APIS ====================

class GenieAskPayload(BaseModel):
    prompt: str
    provider: Optional[str] = None
    model: Optional[str] = None
    chat_id: Optional[str] = None

class CreateChatPayload(BaseModel):
    title: Optional[str] = "New Exploration"

@app.get("/api/genie/config")
async def genie_config_endpoint():
    config = get_available_providers()
    schema_info = extract_schema_context()
    config["tables"] = [
        {
            "name": t["table_name"],
            "columns": len(t["columns"]),
            "column_names": [c["name"] for c in t["columns"]]
        }
        for t in schema_info["tables"]
    ]
    return config

@app.get("/api/genie/chats")
async def list_genie_chats_endpoint(request: Request):
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    scope_user = None if is_admin else username
    chats = load_chats(user=scope_user, is_admin=is_admin)
    return {"chats": chats}

@app.post("/api/genie/chats")
async def create_genie_chat_endpoint(request: Request, payload: Optional[CreateChatPayload] = None):
    title = payload.title if payload and payload.title else "New Exploration"
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    new_chat = create_chat(title=title, user=username)
    return new_chat

@app.get("/api/genie/chats/{chat_id}")
async def get_genie_chat_endpoint(chat_id: str, request: Request):
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    chat = get_chat(chat_id, user=username, is_admin=is_admin)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    return chat

@app.delete("/api/genie/chats/{chat_id}")
async def delete_genie_chat_endpoint(chat_id: str, request: Request):
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    ok = delete_chat(chat_id, user=username, is_admin=is_admin)
    if not ok:
        raise HTTPException(status_code=404, detail="Chat not found")
    return {"success": True, "deleted_id": chat_id}

@app.post("/api/genie/chats/{chat_id}/ask")
async def ask_genie_in_chat_endpoint(chat_id: str, payload: GenieAskPayload, request: Request):
    if not payload.prompt.strip():
        raise HTTPException(status_code=400, detail="Prompt cannot be empty")
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    try:
        res = ask_genie(chat_id, payload.prompt, payload.provider, payload.model, user=username, is_admin=is_admin, principal=current_user)
        return res
    except Exception as e:
        logger.error(f"Genie ask failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/genie/ask")
async def quick_ask_genie_endpoint(payload: GenieAskPayload, request: Request):
    if not payload.prompt.strip():
        raise HTTPException(status_code=400, detail="Prompt cannot be empty")
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    chat_id = payload.chat_id
    if not chat_id:
        new_chat = create_chat(title=payload.prompt[:35] + ("..." if len(payload.prompt) > 35 else ""), user=username)
        chat_id = new_chat["id"]
    try:
        res = ask_genie(chat_id, payload.prompt, payload.provider, payload.model, user=username, is_admin=is_admin, principal=current_user)
        return res
    except Exception as e:
        logger.error(f"Genie ask failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== UNIVERSAL SEARCH API (CTRL+P) ====================

@app.get("/api/search")
async def search_endpoint(
    q: str = "",
    category: str = "ALL",
    limit: int = 25
):
    """
    Executes a high-speed unified fuzzy search across Delta tables, schemas, columns,
    notebooks, queries, dashboards, workflows, and SQL warehouses.
    """
    from web.search import universal_search
    try:
        return universal_search(query=q, category=category, limit=min(max(limit, 1), 100))
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== WORKSPACE BROWSER APIS ====================

@app.get("/api/workspace/tree")
async def get_workspace_tree_endpoint(request: Request):
    from web.workspace import get_workspace_tree
    from web.auth import get_current_user
    current_user = await resolve_principal(request)
    try:
        user_home = f"Users/{current_user['username']}" if current_user else "Users/admin"
        return {
            "tree": get_workspace_tree(current_user=current_user),
            "user_home": user_home,
            "username": current_user.get("username", "admin") if current_user else "admin"
        }
    except Exception as e:
        logger.error(f"Failed to fetch workspace tree: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/workspace/file")
async def get_workspace_file_endpoint(path: str, request: Request):
    from web.workspace import get_file_details, can_access_workspace_path
    from web.auth import get_current_user
    current_user = await resolve_principal(request)
    if not can_access_workspace_path(path, current_user):
        raise HTTPException(status_code=403, detail="Access denied: Cannot access another user's private workspace.")
    try:
        return get_file_details(path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    except Exception as e:
        logger.error(f"Failed to get workspace file details: {e}")
        raise HTTPException(status_code=400, detail=str(e))

class WorkspaceCreatePayload(BaseModel):
    target_dir: Optional[str] = ""
    name: str
    type: Optional[str] = "notebook"
    notebook_template: Optional[str] = "pyspark"

@app.post("/api/workspace/item")
async def create_workspace_item_endpoint(payload: WorkspaceCreatePayload, request: Request):
    from web.workspace import create_workspace_item, can_access_workspace_path
    from web.auth import get_current_user
    current_user = await resolve_principal(request)
    # If target_dir is empty, default to user's home folder
    target_dir = payload.target_dir or (f"Users/{current_user['username']}" if current_user else "")
    if not can_access_workspace_path(target_dir, current_user, write=True):
        raise HTTPException(status_code=403, detail="Access denied: Cannot create items in another user's private workspace.")
    try:
        return create_workspace_item(
            target_dir,
            payload.name,
            payload.type,
            payload.notebook_template or "pyspark"
        )
    except Exception as e:
        logger.error(f"Failed to create workspace item: {e}")
        raise HTTPException(status_code=400, detail=str(e))

class WorkspaceRenamePayload(BaseModel):
    old_rel_path: str
    new_name: str

@app.put("/api/workspace/item/rename")
async def rename_workspace_item_endpoint(payload: WorkspaceRenamePayload, request: Request):
    from web.workspace import rename_workspace_item, can_access_workspace_path
    from web.auth import get_current_user
    current_user = await resolve_principal(request)
    if not can_access_workspace_path(payload.old_rel_path, current_user, write=True):
        raise HTTPException(status_code=403, detail="Access denied: Cannot rename another user's private workspace items.")
    try:
        return rename_workspace_item(payload.old_rel_path, payload.new_name)
    except Exception as e:
        logger.error(f"Failed to rename workspace item: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/workspace/item")
async def delete_workspace_item_endpoint(path: str, request: Request):
    from web.workspace import delete_workspace_item, can_access_workspace_path
    from web.auth import get_current_user
    current_user = await resolve_principal(request)
    if not can_access_workspace_path(path, current_user, write=True):
        raise HTTPException(status_code=403, detail="Access denied: Cannot delete another user's private workspace items.")
    try:
        return delete_workspace_item(path)
    except Exception as e:
        logger.error(f"Failed to delete workspace item: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/workspace/upload")
async def upload_workspace_file_endpoint(file: UploadFile = File(...), target_dir: str = Form("")):
    from web.workspace import get_safe_path, NOTEBOOKS_DIR
    try:
        dir_full = get_safe_path(target_dir)
        os.makedirs(dir_full, exist_ok=True)
        dest_file = os.path.join(dir_full, file.filename)
        with open(dest_file, "wb") as f:
            shutil.copyfileobj(file.file, f)
        rel = os.path.relpath(dest_file, NOTEBOOKS_DIR).replace("\\", "/")
        return {"success": True, "rel_path": rel, "filename": file.filename}
    except Exception as e:
        logger.error(f"Failed to upload file to workspace: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ==================== NOTEBOOK EXECUTION APIS (NATIVE RUNNER) ====================

async def _notebook_user(request: Request, path: str, *, write: bool = False, execute: bool = False) -> Dict[str, Any]:
    """
    Every notebook endpoint: authenticated, restricted to the caller's own Users/<name> folder and Shared (admins see all),
    and code execution limited to principals that masking policies do not apply to (see web/notebook_access.py).
    """
    from web.notebook_access import execution_allowed, execution_denied_message
    from web.workspace import can_access_workspace_path
    user = await resolve_principal(request)
    if not can_access_workspace_path(path, user, write=write or execute):
        raise HTTPException(status_code=403, detail="Access denied: you cannot access this notebook.")
    if execute and not execution_allowed(user):
        raise HTTPException(status_code=403, detail=execution_denied_message())
    return user


def _notebook_sandboxed(user: Dict[str, Any]) -> bool:
    """True when this user's kernels must live in the notebook sandbox (a masking policy applies to them)."""
    from web.notebook_access import SANDBOX, execution_route
    return execution_route(user) == SANDBOX


class NotebookCellRunPayload(BaseModel):
    path: str
    cell_index: int
    source: Optional[str] = None

@app.post("/api/workspace/notebook/cell/run")
async def run_notebook_cell_endpoint(payload: NotebookCellRunPayload, request: Request):
    from web.notebook_runner import execute_single_cell
    user = await _notebook_user(request, payload.path, execute=True)
    try:
        return await asyncio.to_thread(execute_single_cell, payload.path, payload.cell_index, payload.source,
                                       user.get("username", "anonymous"), _notebook_sandboxed(user))
    except Exception as e:
        logger.error(f"Error executing notebook cell: {e}")
        raise HTTPException(status_code=400, detail=str(e))

class NotebookRunAllPayload(BaseModel):
    path: str

@app.post("/api/workspace/notebook/run_all")
async def run_all_notebook_cells_endpoint(payload: NotebookRunAllPayload, request: Request):
    from web.notebook_runner import execute_all_cells
    user = await _notebook_user(request, payload.path, execute=True)
    try:
        return await asyncio.to_thread(execute_all_cells, payload.path, user.get("username", "anonymous"), _notebook_sandboxed(user))
    except Exception as e:
        logger.error(f"Error running all notebook cells: {e}")
        raise HTTPException(status_code=400, detail=str(e))

class NotebookKernelPayload(BaseModel):
    path: str

@app.post("/api/workspace/notebook/kernel/restart")
async def restart_notebook_kernel_endpoint(payload: NotebookKernelPayload, request: Request):
    from web.notebook_runner import restart_notebook_kernel
    user = await _notebook_user(request, payload.path, execute=True)
    try:
        return await asyncio.to_thread(restart_notebook_kernel, payload.path, user.get("username", "anonymous"), _notebook_sandboxed(user))
    except Exception as e:
        logger.error(f"Error restarting notebook kernel: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/workspace/notebook/kernel/status")
async def get_notebook_kernel_status_endpoint(path: str, request: Request):
    from web.notebook_runner import get_kernel_status
    user = await _notebook_user(request, path)
    try:
        return await asyncio.to_thread(get_kernel_status, path, user.get("username", "anonymous"), _notebook_sandboxed(user))
    except Exception as e:
        logger.error(f"Error checking notebook kernel status: {e}")
        raise HTTPException(status_code=400, detail=str(e))

class NotebookCellSavePayload(BaseModel):
    path: str
    cell_index: int
    source: str

@app.post("/api/workspace/notebook/cell/save")
async def save_notebook_cell_endpoint(payload: NotebookCellSavePayload, request: Request):
    from web.notebook_runner import save_cell_source
    await _notebook_user(request, payload.path, write=True)
    try:
        return save_cell_source(payload.path, payload.cell_index, payload.source)
    except Exception as e:
        logger.error(f"Error saving notebook cell: {e}")
        raise HTTPException(status_code=400, detail=str(e))

class NotebookCellAddPayload(BaseModel):
    path: str
    after_index: int = 0
    type: str = "code"

@app.post("/api/workspace/notebook/cell/add")
async def add_notebook_cell_endpoint(payload: NotebookCellAddPayload, request: Request):
    from web.notebook_runner import add_new_cell
    await _notebook_user(request, payload.path, write=True)
    try:
        return add_new_cell(payload.path, payload.after_index, payload.type)
    except Exception as e:
        logger.error(f"Error adding notebook cell: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/workspace/notebook/cell")
async def delete_notebook_cell_endpoint(path: str, cell_index: int, request: Request):
    from web.notebook_runner import delete_cell
    await _notebook_user(request, path, write=True)
    try:
        return delete_cell(path, cell_index)
    except Exception as e:
        logger.error(f"Error deleting notebook cell: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/workspace/notebook/clear_outputs")
async def clear_notebook_outputs_endpoint(payload: NotebookKernelPayload, request: Request):
    from web.notebook_runner import clear_notebook_outputs
    await _notebook_user(request, payload.path, write=True)
    try:
        return clear_notebook_outputs(payload.path)
    except Exception as e:
        logger.error(f"Error clearing notebook outputs: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/notebooks/access")
async def get_notebook_access(request: Request):
    """Whether the caller may run notebook code (masked users can open and edit notebooks, not run them)."""
    from web.notebook_access import SANDBOX, execution_mode, execution_route
    user = await resolve_principal(request)
    route = await asyncio.to_thread(execution_route, user)
    return {"execution_allowed": route is not None, "mode": execution_mode(), "sandboxed": route == SANDBOX}


# ==================== RECENTS APIS (MULTI-USER TRACKING) ====================

class RecentRecordPayload(BaseModel):
    item_type: str
    item_id: str
    title: str
    subtitle: Optional[str] = ""
    metadata: Optional[Dict[str, Any]] = None
    user: Optional[str] = "admin"

@app.post("/api/recents")
async def record_recent_endpoint(payload: RecentRecordPayload, request: Request):
    from web.recents import record_recent
    current_user = await resolve_principal(request)
    user = current_user.get("username", "admin")
    try:
        return record_recent(
            item_type=payload.item_type,
            item_id=payload.item_id,
            title=payload.title,
            subtitle=payload.subtitle or "",
            metadata=payload.metadata,
            user_id=user
        )
    except Exception as e:
        logger.error(f"Failed to record recent item: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/recents")
async def get_recents_endpoint(
    request: Request,
    type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 50,
    user: Optional[str] = None
):
    from web.recents import get_recents
    current_user = await resolve_principal(request)
    user_id = current_user.get("username", "admin")
    try:
        return get_recents(user_id=user_id, item_type=type, search=search, limit=limit)
    except Exception as e:
        logger.error(f"Failed to get recents: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class RecentPinPayload(BaseModel):
    item_type: str
    item_id: str
    user: Optional[str] = None

@app.post("/api/recents/pin")
async def toggle_pin_recent_endpoint(payload: RecentPinPayload, request: Request):
    from web.recents import toggle_pin_recent
    current_user = await resolve_principal(request)
    user = current_user.get("username", "admin")
    try:
        return toggle_pin_recent(item_type=payload.item_type, item_id=payload.item_id, user_id=user)
    except Exception as e:
        logger.error(f"Failed to toggle pin for recent item: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/recents")
async def delete_recent_endpoint(
    request: Request,
    item_type: str,
    item_id: str,
    user: Optional[str] = None
):
    from web.recents import delete_recent
    current_user = await resolve_principal(request)
    user_id = current_user.get("username", "admin")
    try:
        success = delete_recent(item_type=item_type, item_id=item_id, user_id=user_id)
        return {"success": success, "item_type": item_type, "item_id": item_id}
    except Exception as e:
        logger.error(f"Failed to delete recent item: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/recents/clear")
async def clear_recents_endpoint(
    request: Request,
    type: Optional[str] = None,
    include_pinned: bool = False,
    user: Optional[str] = None
):
    from web.recents import clear_recents
    current_user = await resolve_principal(request)
    user_id = current_user.get("username", "admin")
    try:
        count = clear_recents(user_id=user_id, item_type=type, include_pinned=include_pinned)
        return {"success": True, "deleted_count": count}
    except Exception as e:
        logger.error(f"Failed to clear recents: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# SQL Alerts & Lakehouse Monitoring Endpoints
# ==============================================================================

class AlertCreatePayload(BaseModel):
    name: str
    description: Optional[str] = ""
    query_id: Optional[str] = None
    custom_query: Optional[str] = None
    warehouse_id: Optional[str] = "wh_starter"
    catalog: Optional[str] = "warehouse"
    schema_name: Optional[str] = "dbo"
    target_column: Optional[str] = "count"
    operator: Optional[str] = ">"
    threshold_value: Optional[Union[str, float, int]] = "0"
    schedule_interval: Optional[str] = "5m"
    notify_on_state_change_only: Optional[bool] = True
    is_enabled: Optional[bool] = True
    is_muted: Optional[bool] = False
    is_shared: Optional[bool] = True
    user: Optional[str] = "admin"


class AlertUpdatePayload(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    query_id: Optional[str] = None
    custom_query: Optional[str] = None
    target_column: Optional[str] = None
    operator: Optional[str] = None
    threshold_value: Optional[Union[str, float, int]] = None
    schedule_interval: Optional[str] = None
    notify_on_state_change_only: Optional[bool] = None
    is_enabled: Optional[bool] = None
    is_muted: Optional[bool] = None
    is_shared: Optional[bool] = None
    user: Optional[str] = "admin"


@app.get("/api/alerts/summary")
async def get_alerts_summary_endpoint(request: Request, user: Optional[str] = "admin"):
    from web.alerts import get_alerts_summary
    user_id = request.headers.get("X-User") or user or "admin"
    try:
        return get_alerts_summary(user_id=user_id)
    except Exception as e:
        logger.error(f"Failed to get alerts summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/alerts")
async def get_alerts_endpoint(
    request: Request,
    state: Optional[str] = None,
    search: Optional[str] = None,
    user: Optional[str] = "admin"
):
    from web.alerts import get_alerts
    user_id = request.headers.get("X-User") or user or "admin"
    try:
        return get_alerts(user_id=user_id, state_filter=state, search=search)
    except Exception as e:
        logger.error(f"Failed to get alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/alerts")
async def create_alert_endpoint(payload: AlertCreatePayload, request: Request):
    from web.alerts import create_alert
    from web.recents import record_recent
    user_id = request.headers.get("X-User") or payload.user or "admin"
    try:
        alert = create_alert(payload.dict(), user_id=user_id)
        try:
            record_recent(
                item_type="alert",
                item_id=alert["id"],
                title=alert.get("name", "SQL Alert"),
                subtitle=f"{alert.get('target_column', '')} {alert.get('operator', '')} {alert.get('threshold_value', '')}",
                metadata={"state": alert.get("state"), "schedule": alert.get("schedule_interval")},
                user_id=user_id
            )
        except Exception:
            pass
        return alert
    except Exception as e:
        logger.error(f"Failed to create alert: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/alerts/{alert_id}")
async def get_alert_endpoint(alert_id: str, request: Request, user: Optional[str] = "admin"):
    from web.alerts import get_alert
    from web.recents import record_recent
    user_id = request.headers.get("X-User") or user or "admin"
    alert = get_alert(alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    try:
        record_recent(
            item_type="alert",
            item_id=alert_id,
            title=alert.get("name", "SQL Alert"),
            subtitle=f"{alert.get('target_column', '')} {alert.get('operator', '')} {alert.get('threshold_value', '')}",
            metadata={"state": alert.get("state"), "schedule": alert.get("schedule_interval")},
            user_id=user_id
        )
    except Exception:
        pass
    return alert


@app.put("/api/alerts/{alert_id}")
async def update_alert_endpoint(alert_id: str, payload: AlertUpdatePayload, request: Request):
    from web.alerts import update_alert
    user_id = request.headers.get("X-User") or payload.user or "admin"
    data = payload.dict(exclude_unset=True)
    alert = update_alert(alert_id, data, user_id=user_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


@app.delete("/api/alerts/{alert_id}")
async def delete_alert_endpoint(alert_id: str, request: Request, user: Optional[str] = "admin"):
    from web.alerts import delete_alert
    from web.recents import delete_recent
    user_id = request.headers.get("X-User") or user or "admin"
    success = delete_alert(alert_id, user_id=user_id)
    if not success:
        raise HTTPException(status_code=404, detail="Alert not found or already deleted")
    try:
        delete_recent("alert", alert_id, user_id=user_id)
    except Exception:
        pass
    return {"success": True, "alert_id": alert_id}


@app.post("/api/alerts/{alert_id}/run")
async def run_alert_check_endpoint(alert_id: str, request: Request, user: Optional[str] = "admin"):
    from web.alerts import execute_alert_check
    user_id = request.headers.get("X-User") or user or "admin"
    try:
        result = execute_alert_check(alert_id, triggered_by="manual", user_id=user_id)
        return result
    except Exception as e:
        logger.error(f"Failed to run alert check for {alert_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/alerts/{alert_id}/mute")
async def toggle_mute_alert_endpoint(alert_id: str, request: Request, user: Optional[str] = "admin"):
    from web.alerts import toggle_mute_alert
    user_id = request.headers.get("X-User") or user or "admin"
    alert = toggle_mute_alert(alert_id, user_id=user_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


@app.post("/api/alerts/{alert_id}/toggle")
async def toggle_enable_alert_endpoint(alert_id: str, request: Request, user: Optional[str] = "admin"):
    from web.alerts import toggle_enable_alert
    user_id = request.headers.get("X-User") or user or "admin"
    alert = toggle_enable_alert(alert_id, user_id=user_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


@app.get("/api/alerts/{alert_id}/history")
async def get_alert_history_endpoint(alert_id: str, limit: int = 50):
    from web.alerts import get_alert_evaluations
    return get_alert_evaluations(alert_id, limit=limit)


# =========================================================================
# MLflow 2.0 REST API & Studio Experiments Endpoints
# =========================================================================

@app.post("/api/2.0/mlflow/experiments/create")
async def mlflow_create_experiment_api(request: Request):
    from web.experiments import mlflow_create_experiment
    from web.auth import get_current_user
    body = await request.json()
    current_user = await resolve_principal(request)
    user_id = current_user.get("username") if current_user else (request.headers.get("X-User") or "admin")
    try:
        res = mlflow_create_experiment(body.get("name", ""), body.get("artifact_location"), user_id=user_id)
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/2.0/mlflow/experiments/list")
async def mlflow_list_experiments_api(request: Request, view_type: str = "ACTIVE_ONLY"):
    from web.experiments import mlflow_list_experiments
    from web.auth import get_current_user
    current_user = await resolve_principal(request)
    user_id = current_user.get("username") if current_user else None
    is_admin = (current_user.get("role") == "admin") if current_user else True
    return {"experiments": mlflow_list_experiments(view_type=view_type, user_id=user_id, is_admin=is_admin)}

@app.get("/api/2.0/mlflow/experiments/search")
@app.post("/api/2.0/mlflow/experiments/search")
async def mlflow_search_experiments_api(request: Request, view_type: str = "ACTIVE_ONLY"):
    from web.experiments import mlflow_list_experiments
    from web.auth import get_current_user
    current_user = await resolve_principal(request)
    user_id = current_user.get("username") if current_user else None
    is_admin = (current_user.get("role") == "admin") if current_user else True
    return {"experiments": mlflow_list_experiments(view_type=view_type, user_id=user_id, is_admin=is_admin)}

@app.get("/api/2.0/mlflow/experiments/get")
async def mlflow_get_experiment_api(experiment_id: str):
    from web.experiments import mlflow_get_experiment
    exp = mlflow_get_experiment(experiment_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    return {"experiment": exp}

@app.get("/api/2.0/mlflow/experiments/get-by-name")
@app.post("/api/2.0/mlflow/experiments/get-by-name")
async def mlflow_get_experiment_by_name_api(request: Request, experiment_name: Optional[str] = None):
    from web.experiments import mlflow_get_experiment_by_name
    name = experiment_name
    if not name and request.method == "POST":
        try:
            body = await request.json()
            name = body.get("experiment_name")
        except Exception:
            pass
    if not name:
        raise HTTPException(status_code=400, detail="Missing experiment_name")
    exp = mlflow_get_experiment_by_name(name)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    return {"experiment": exp}

@app.post("/api/2.0/mlflow/experiments/delete")
async def mlflow_delete_experiment_api(request: Request):
    from web.experiments import mlflow_delete_experiment
    body = await request.json()
    mlflow_delete_experiment(body.get("experiment_id", ""))
    return {}

@app.post("/api/2.0/mlflow/experiments/update")
async def mlflow_update_experiment_api(request: Request):
    from web.experiments import mlflow_update_experiment
    body = await request.json()
    try:
        mlflow_update_experiment(body.get("experiment_id", ""), body.get("new_name", ""))
        return {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/2.0/mlflow/experiments/restore")
async def mlflow_restore_experiment_api(request: Request):
    from web.experiments import mlflow_restore_experiment
    body = await request.json()
    try:
        mlflow_restore_experiment(body.get("experiment_id", ""))
        return {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/2.0/mlflow/experiments/set-experiment-tag")
async def mlflow_set_experiment_tag_api(request: Request):
    from web.experiments import mlflow_set_experiment_tag
    body = await request.json()
    try:
        mlflow_set_experiment_tag(body.get("experiment_id", ""), body.get("key", ""), body.get("value", ""))
        return {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/2.0/mlflow/runs/create")
async def mlflow_create_run_api(request: Request):
    from web.experiments import mlflow_create_run
    from web.auth import get_current_user
    body = await request.json()
    current_user = await resolve_principal(request)
    user_id = current_user.get("username") if current_user else (request.headers.get("X-User") or "admin")
    try:
        run_data = mlflow_create_run(
            experiment_id=body.get("experiment_id", "0"),
            run_name=body.get("run_name"),
            start_time=body.get("start_time"),
            user_id=user_id,
            tags=body.get("tags")
        )
        return {"run": run_data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/2.0/mlflow/runs/get")
async def mlflow_get_run_api(run_id: str):
    from web.experiments import mlflow_get_run
    run_data = mlflow_get_run(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"run": run_data}

@app.post("/api/2.0/mlflow/runs/update")
async def mlflow_update_run_api(request: Request):
    from web.experiments import mlflow_update_run
    body = await request.json()
    try:
        run_data = mlflow_update_run(
            run_id=body.get("run_id"),
            status=body.get("status", "FINISHED"),
            end_time=body.get("end_time")
        )
        return {"run_info": run_data.get("info", {})}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/2.0/mlflow/runs/delete")
async def mlflow_delete_run_api(request: Request):
    from web.experiments import mlflow_delete_run
    body = await request.json()
    mlflow_delete_run(body.get("run_id", ""))
    return {}

@app.post("/api/2.0/mlflow/runs/restore")
async def mlflow_restore_run_api(request: Request):
    from web.experiments import mlflow_restore_run
    body = await request.json()
    try:
        mlflow_restore_run(body.get("run_id", ""))
        return {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/2.0/mlflow/runs/search")
@app.post("/api/2.0/mlflow/runs/search")
async def mlflow_search_runs_api(request: Request):
    from web.experiments import mlflow_search_runs
    if request.method == "POST":
        try:
            body = await request.json()
        except Exception:
            body = {}
        exp_ids = body.get("experiment_ids", ["0"])
        flt = body.get("filter") or body.get("filter_string")
        order_by = body.get("order_by")
        max_results = body.get("max_results", 100)
    else:
        q = request.query_params
        exp_ids = q.get("experiment_ids", "0").split(",")
        flt = q.get("filter") or q.get("filter_string")
        order_by = [q.get("order_by")] if q.get("order_by") else None
        max_results = int(q.get("max_results", 100))

    runs = mlflow_search_runs(
        experiment_ids=exp_ids,
        filter_string=flt,
        order_by=order_by,
        max_results=max_results
    )
    return {"runs": runs}

@app.post("/api/2.0/mlflow/runs/log-parameter")
async def mlflow_log_param_api(request: Request):
    from web.experiments import mlflow_log_param
    body = await request.json()
    try:
        mlflow_log_param(body["run_id"], body["key"], body["value"])
        return {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/2.0/mlflow/runs/log-metric")
async def mlflow_log_metric_api(request: Request):
    from web.experiments import mlflow_log_metric
    body = await request.json()
    try:
        mlflow_log_metric(body["run_id"], body["key"], body["value"], body.get("timestamp"), body.get("step", 0))
        return {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/2.0/mlflow/runs/log-batch")
async def mlflow_log_batch_api(request: Request):
    from web.experiments import mlflow_log_batch
    body = await request.json()
    try:
        mlflow_log_batch(body["run_id"], body.get("metrics"), body.get("params"), body.get("tags"))
        return {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/2.0/mlflow/runs/set-tag")
async def mlflow_set_tag_api(request: Request):
    from web.experiments import mlflow_set_tag
    body = await request.json()
    try:
        mlflow_set_tag(body["run_id"], body["key"], body["value"])
        return {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/2.0/mlflow/runs/delete-tag")
async def mlflow_delete_tag_api(request: Request):
    from web.experiments import mlflow_delete_tag
    body = await request.json()
    try:
        mlflow_delete_tag(body.get("run_id", ""), body.get("key", ""))
        return {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/2.0/mlflow/runs/log-inputs")
async def mlflow_log_inputs_api(request: Request):
    from web.experiments import mlflow_log_inputs
    body = await request.json()
    try:
        mlflow_log_inputs(body.get("run_id", ""), body.get("datasets", []))
        return {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/2.0/mlflow/metrics/get-history")
async def mlflow_get_metric_history_api(run_id: str, metric_key: str):
    from web.experiments import mlflow_get_metric_history
    history = mlflow_get_metric_history(run_id, metric_key)
    return {"metrics": history}

@app.post("/api/2.0/mlflow/artifacts/log")
async def mlflow_log_artifact_api(request: Request):
    from web.experiments import mlflow_log_artifact
    body = await request.json()
    try:
        res = mlflow_log_artifact(body["run_id"], body["local_file"], body.get("artifact_path"))
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/2.0/mlflow/artifacts/list")
async def mlflow_list_artifacts_api(run_id: str, path: Optional[str] = None):
    from web.experiments import mlflow_list_artifacts
    artifacts = mlflow_list_artifacts(run_id, path=path)
    return {"run_id": run_id, "files": artifacts}

@app.get("/api/2.0/mlflow/artifacts/get")
@app.get("/api/2.0/mlflow/artifacts/download")
async def mlflow_get_artifact_file_api(run_id: str, path: str):
    from fastapi.responses import FileResponse
    from web.experiments import mlflow_get_artifact_path
    fpath = mlflow_get_artifact_path(run_id, path)
    if not fpath or not os.path.isfile(fpath):
        raise HTTPException(status_code=404, detail=f"Artifact '{path}' for run '{run_id}' not found")
    return FileResponse(fpath, filename=os.path.basename(fpath))

@app.get("/api/experiments/runs/{run_id}/artifacts/content")
async def get_studio_artifact_content(run_id: str, artifact_path: str):
    from web.experiments import mlflow_get_artifact_content
    res = mlflow_get_artifact_content(run_id, artifact_path)
    if not res:
        raise HTTPException(status_code=404, detail="Artifact not found")
    return res

@app.get("/api/experiments/runs/{run_id}/artifacts/file")
async def download_studio_artifact_file(run_id: str, artifact_path: str):
    from fastapi.responses import FileResponse
    from web.experiments import mlflow_get_artifact_path
    fpath = mlflow_get_artifact_path(run_id, artifact_path)
    if not fpath or not os.path.isfile(fpath):
        raise HTTPException(status_code=404, detail="Artifact file not found")
    return FileResponse(fpath, filename=os.path.basename(fpath))

# -------------------------------------------------------------------------
# MLflow 2.14+ GenAI & LLM Tracing Endpoints
# -------------------------------------------------------------------------

@app.post("/api/2.0/mlflow/traces/log")
@app.post("/api/2.0/mlflow/traces")
async def mlflow_log_trace_api(request: Request):
    from web.experiments import mlflow_log_trace
    body = await request.json()
    try:
        return mlflow_log_trace(body)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/2.0/mlflow/traces/search")
@app.post("/api/2.0/mlflow/traces/search")
@app.get("/api/2.0/mlflow/traces")
@app.get("/api/experiments/traces")
async def mlflow_search_traces_api(
    request: Request,
    status: Optional[str] = None,
    model: Optional[str] = None,
    min_duration: Optional[float] = None,
    max_duration: Optional[float] = None,
    search_term: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    from web.experiments import mlflow_search_traces
    exp_ids = None
    if request.method == "POST":
        try:
            body = await request.json()
            exp_ids = body.get("experiment_ids")
            status = body.get("status", status)
            model = body.get("model", model)
            min_duration = body.get("min_duration", min_duration)
            max_duration = body.get("max_duration", max_duration)
            search_term = body.get("search_term") or body.get("filter_string", search_term)
            limit = int(body.get("limit") or body.get("max_results", limit))
            offset = int(body.get("offset", offset))
        except Exception:
            pass
    else:
        exp_id_param = request.query_params.get("experiment_id")
        if exp_id_param:
            exp_ids = [exp_id_param]

    return mlflow_search_traces(
        experiment_ids=exp_ids,
        status=status,
        model=model,
        min_duration=min_duration,
        max_duration=max_duration,
        search_term=search_term,
        limit=limit,
        offset=offset
    )

@app.get("/api/2.0/mlflow/traces/get")
@app.get("/api/2.0/mlflow/traces/{request_id}")
@app.get("/api/experiments/traces/{request_id}")
async def mlflow_get_trace_api(request_id: Optional[str] = None, request: Request = None):
    from web.experiments import mlflow_get_trace
    rid = request_id or (request.query_params.get("request_id") if request else None)
    if not rid:
        raise HTTPException(status_code=400, detail="request_id is required")
    trace = mlflow_get_trace(rid)
    if not trace:
        raise HTTPException(status_code=404, detail="Trace not found")
    return trace

@app.post("/api/2.0/mlflow/traces/delete")
@app.delete("/api/2.0/mlflow/traces/{request_id}")
@app.delete("/api/experiments/traces/{request_id}")
async def mlflow_delete_trace_api(request_id: Optional[str] = None, request: Request = None):
    from web.experiments import mlflow_delete_trace
    rid = request_id
    if not rid and request:
        try:
            body = await request.json()
            rid = body.get("request_id")
        except Exception:
            rid = request.query_params.get("request_id")
    if not rid:
        raise HTTPException(status_code=400, detail="request_id is required")
    deleted = mlflow_delete_trace(rid)
    return {"deleted": deleted, "request_id": rid}

@app.post("/api/2.0/mlflow/traces/assessments/log")
@app.post("/api/2.0/mlflow/traces/{request_id}/assessments")
@app.post("/api/experiments/traces/{request_id}/assessments")
async def mlflow_log_assessment_api(request: Request, request_id: Optional[str] = None):
    from web.experiments import mlflow_log_assessment
    body = await request.json()
    tid = request_id or body.get("trace_id") or body.get("request_id")
    if not tid:
        raise HTTPException(status_code=400, detail="trace_id is required")
    res = mlflow_log_assessment(
        trace_id=tid,
        name=body.get("name", "user_feedback"),
        value=body.get("value", "1"),
        rationale=body.get("rationale", ""),
        source_type=body.get("source_type", "HUMAN"),
        source_id=body.get("source_id", "admin")
    )
    return res

@app.get("/api/2.0/mlflow/traces/{request_id}/assessments")
@app.get("/api/experiments/traces/{request_id}/assessments")
async def mlflow_get_assessments_api(request_id: str):
    from web.experiments import mlflow_get_assessments
    return {"assessments": mlflow_get_assessments(request_id)}

# -------------------------------------------------------------------------
# Studio UI Convenience Endpoints
# -------------------------------------------------------------------------

@app.get("/api/experiments/summary")
async def get_experiments_summary_endpoint():
    from web.experiments import get_experiments_summary
    return get_experiments_summary()

@app.get("/api/experiments")
async def list_studio_experiments():
    from web.experiments import mlflow_list_experiments
    return {"experiments": mlflow_list_experiments()}

@app.post("/api/experiments")
async def create_studio_experiment(request: Request):
    from web.experiments import mlflow_create_experiment
    body = await request.json()
    user_id = request.headers.get("X-User", "admin")
    try:
        res = mlflow_create_experiment(body.get("name", ""), body.get("artifact_location"), user_id=user_id)
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/experiments/{experiment_id}")
async def get_studio_experiment(experiment_id: str):
    from web.experiments import mlflow_get_experiment, mlflow_search_runs
    exp = mlflow_get_experiment(experiment_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    runs = mlflow_search_runs([experiment_id], max_results=200)
    return {"experiment": exp, "runs": runs}

@app.delete("/api/experiments/{experiment_id}")
async def delete_studio_experiment(experiment_id: str):
    from web.experiments import mlflow_delete_experiment
    mlflow_delete_experiment(experiment_id)
    return {"status": "SUCCESS"}

@app.get("/api/experiments/runs/compare")
async def compare_studio_runs(run_ids: str):
    from web.experiments import compare_runs
    ids = [r.strip() for r in run_ids.split(",") if r.strip()]
    return compare_runs(ids)

@app.get("/api/experiments/runs/{run_id}")
async def get_studio_run(run_id: str):
    from web.experiments import mlflow_get_run
    r = mlflow_get_run(run_id)
    if not r:
        raise HTTPException(status_code=404, detail="Run not found")
    return r

@app.delete("/api/experiments/runs/{run_id}")
async def delete_studio_run(run_id: str):
    from web.experiments import mlflow_delete_run
    mlflow_delete_run(run_id)
    return {"status": "SUCCESS"}

@app.get("/api/experiments/runs/{run_id}/metrics/{metric_key}")
async def get_studio_run_metric_history(run_id: str, metric_key: str):
    from web.experiments import mlflow_get_metric_history
    history = mlflow_get_metric_history(run_id, metric_key)
    return {"run_id": run_id, "metric_key": metric_key, "history": history}

@app.post("/api/experiments/seed_demo")
async def seed_demo_experiments_endpoint():
    from web.experiments import seed_demo_experiments
    return seed_demo_experiments()


# ==============================================================================
# AI PROMPT PLAYGROUND ROUTES
# ==============================================================================

class PlaygroundSingleRunRequest(BaseModel):
    model: str
    provider: str
    host: Optional[str] = None
    prompt: str
    raw_prompt: Optional[str] = None
    system_prompt: Optional[str] = ""
    temperature: Optional[float] = 0.7
    top_p: Optional[float] = 0.9
    max_tokens: Optional[int] = 1024
    variables: Optional[Dict[str, str]] = None

class PlaygroundCompareRunRequest(BaseModel):
    config_a: Dict[str, Any]
    config_b: Dict[str, Any]
    prompt: str
    raw_prompt: Optional[str] = None
    system_prompt: Optional[str] = ""
    variables: Optional[Dict[str, str]] = None

class PlaygroundTemplateRequest(BaseModel):
    id: Optional[str] = None
    title: str
    description: Optional[str] = ""
    category: Optional[str] = "Custom"
    system_prompt: Optional[str] = ""
    user_prompt: str
    temperature: Optional[float] = 0.7
    top_p: Optional[float] = 0.9
    max_tokens: Optional[int] = 1024
    variables: Optional[List[str]] = None
    is_builtin: Optional[int] = 0

@app.get("/api/playground/models")
async def api_playground_models():
    from web.playground import get_available_models
    return await get_available_models()

@app.post("/api/playground/run")
async def api_playground_run(req: PlaygroundSingleRunRequest, request: Request):
    from web.playground import run_and_record_single
    raw = req.raw_prompt if req.raw_prompt is not None else req.prompt
    config = {
        "model": req.model,
        "provider": req.provider,
        "host": req.host,
        "temperature": req.temperature,
        "top_p": req.top_p,
        "max_tokens": req.max_tokens
    }
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    return await run_and_record_single(
        config=config,
        rendered_prompt=req.prompt,
        raw_prompt=raw,
        system_prompt=req.system_prompt or "",
        variables=req.variables,
        user_id=username
    )

@app.post("/api/playground/compare")
async def api_playground_compare(req: PlaygroundCompareRunRequest, request: Request):
    from web.playground import run_comparison_prompts
    raw = req.raw_prompt if req.raw_prompt is not None else req.prompt
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    return await run_comparison_prompts(
        config_a=req.config_a,
        config_b=req.config_b,
        rendered_prompt=req.prompt,
        raw_prompt=raw,
        system_prompt=req.system_prompt or "",
        variables=req.variables,
        user_id=username
    )

@app.get("/api/playground/templates")
async def api_playground_get_templates(request: Request, category: Optional[str] = None):
    from web.playground import get_templates
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    return get_templates(category=category, user_id=username, is_admin=is_admin)

@app.post("/api/playground/templates")
async def api_playground_save_template(req: PlaygroundTemplateRequest, request: Request):
    from web.playground import save_template
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    return save_template(req.dict(), user_id=username)

@app.delete("/api/playground/templates/{template_id}")
async def api_playground_delete_template(template_id: str, request: Request):
    from web.playground import delete_template
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    ok = delete_template(template_id, user_id=username, is_admin=is_admin)
    if not ok:
        raise HTTPException(status_code=400, detail="Cannot delete template (might be built-in, not found, or not owned by you)")
    return {"status": "SUCCESS", "id": template_id}

@app.get("/api/playground/history")
async def api_playground_get_history(request: Request, limit: int = 50):
    from web.playground import get_history
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    return get_history(limit=limit, user_id=username, is_admin=is_admin)

@app.delete("/api/playground/history/{hist_id}")
async def api_playground_delete_history(hist_id: str, request: Request):
    from web.playground import delete_history_item
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    ok = delete_history_item(hist_id, user_id=username, is_admin=is_admin)
    return {"status": "SUCCESS" if ok else "NOT_FOUND"}

@app.delete("/api/playground/history")
async def api_playground_clear_history(request: Request):
    from web.playground import clear_history
    current_user = await resolve_principal(request)
    username = current_user.get("username", "admin")
    is_admin = current_user.get("role") == "admin"
    clear_history(user_id=username, is_admin=is_admin)
    return {"status": "SUCCESS"}

@app.get("/api/playground/schema-tables")
async def api_playground_schema_tables():
    from web.playground import get_schema_tables_summary
    return get_schema_tables_summary()


# ==============================================================================
# DELTA TIME-TRAVEL RESTORE & VISUAL DIFF ENDPOINTS
# ==============================================================================

class TableRestorePayload(BaseModel):
    target_version: int
    catalog: Optional[str] = "warehouse"


@app.get("/api/table/{schema_name}/{table_name}/diff")
async def get_table_diff_endpoint(
    schema_name: str,
    table_name: str,
    v1: int,
    v2: int,
    request: Request,
    catalog: Optional[str] = "warehouse",
    limit: int = 50
):
    from web.time_travel import resolve_table_path, compare_table_versions
    diff_user = await resolve_principal(request)
    from web import table_access
    if not table_access.can_access_table(diff_user, catalog or "warehouse", schema_name, table_name, "READ"):
        raise HTTPException(status_code=403, detail=f"Access denied: you cannot view '{catalog or 'warehouse'}.{schema_name}.{table_name}'.")
    path, cat_id = resolve_table_path(schema_name, table_name, catalog)
    if not (path.startswith("s3://") or os.path.exists(path)):
        raise HTTPException(status_code=404, detail=f"Table {schema_name}.{table_name} not found")
    try:
        return compare_table_versions(path, v1, v2, sample_limit=limit, user=diff_user, catalog=cat_id or catalog or "warehouse",
                                      schema_name=schema_name, table_name=table_name)
    except Exception as e:
        logger.error(f"Error diffing table versions: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/table/{schema_name}/{table_name}/restore")
async def restore_table_endpoint(
    schema_name: str,
    table_name: str,
    payload: TableRestorePayload,
    request: Request
):
    from web.time_travel import resolve_table_path, restore_table_to_version
    user = request.headers.get("X-User") or "admin"
    path, cat_id = resolve_table_path(schema_name, table_name, payload.catalog)
    if not (path.startswith("s3://") or os.path.exists(path)):
        raise HTTPException(status_code=404, detail=f"Table {schema_name}.{table_name} not found")
    try:
        res = restore_table_to_version(path, payload.target_version, user=user)
        try:
            from web.lineage import record_query_lineage
            record_query_lineage(f"RESTORE TABLE {schema_name}.{table_name} TO VERSION AS OF {payload.target_version};")
        except Exception:
            pass
        return res
    except Exception as e:
        logger.error(f"Error restoring table: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/table/{schema_name}/{table_name}/version/{version}/preview")
async def preview_table_version_endpoint(
    schema_name: str,
    table_name: str,
    version: int,
    request: Request,
    catalog: Optional[str] = "warehouse",
    limit: int = 50
):
    from web.time_travel import resolve_table_path, get_version_preview
    preview_user = await resolve_principal(request)
    from web import table_access
    if not table_access.can_access_table(preview_user, catalog or "warehouse", schema_name, table_name, "READ"):
        raise HTTPException(status_code=403, detail=f"Access denied: you cannot view '{catalog or 'warehouse'}.{schema_name}.{table_name}'.")
    path, cat_id = resolve_table_path(schema_name, table_name, catalog)
    if not (path.startswith("s3://") or os.path.exists(path)):
        raise HTTPException(status_code=404, detail=f"Table {schema_name}.{table_name} not found")
    try:
        return get_version_preview(path, version, limit=limit, user=preview_user, catalog=cat_id or catalog or "warehouse",
                                   schema_name=schema_name, table_name=table_name)
    except Exception as e:
        logger.error(f"Error previewing version: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ==============================================================================
# AUTOMATED DATA LINEAGE ENDPOINTS
# ==============================================================================

@app.get("/api/lineage/global")
async def get_global_lineage_endpoint(
    request: Request,
    layer: Optional[str] = None,
    schema: Optional[str] = None,
    search: Optional[str] = None
):
    from web.lineage import get_global_lineage
    allowed_catalogs = None
    current_user = await resolve_principal(request)
    if current_user.get("role") == "user":
        perms = current_user.get("catalog_permissions") or []
        allowed_catalogs = [p["catalog_id"] for p in perms] + ["warehouse", "dbt_analytics"]
    try:
        return get_global_lineage(layer=layer, schema=schema, search=search, allowed_catalogs=allowed_catalogs)
    except Exception as e:
        logger.error(f"Error getting global lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/lineage/table/{schema_name}/{table_name}")
async def get_table_lineage_endpoint(
    schema_name: str,
    table_name: str,
    depth: int = 2
):
    from web.lineage import get_table_lineage
    try:
        return get_table_lineage(schema_name, table_name, depth=depth)
    except Exception as e:
        logger.error(f"Error getting table lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/lineage/node/{node_id:path}/impact")
async def get_node_impact_endpoint(node_id: str):
    from web.lineage import get_node_impact_analysis
    try:
        return get_node_impact_analysis(node_id)
    except Exception as e:
        logger.error(f"Error analyzing node impact: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/lineage/refresh")
async def refresh_lineage_endpoint():
    from web.lineage import scan_and_sync_all_assets, get_global_lineage
    try:
        scan_and_sync_all_assets()
        return {"status": "SUCCESS", "graph": get_global_lineage()}
    except Exception as e:
        logger.error(f"Error refreshing lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# STORAGE MOUNTS & ZERO-COPY FEDERATION APIS
# ==============================================================================

@app.get("/api/mounts")
async def list_mounts_endpoint():
    from web.mounts import load_mounts, mask_mount_record
    try:
        raw_mounts = load_mounts()
        mounts_out = [mask_mount_record(m) for m in raw_mounts]
        # Include OneLake catalogs if mounted
        try:
            from web import onelake
            for cat in onelake.list_onelake_catalogs():
                mounts_out.append({
                    "id": f"onelake_{cat['catalog_id']}",
                    "name": f"OneLake: {cat['lakehouse']}",
                    "type": "onelake",
                    "catalog_name": cat['catalog_id'],
                    "read_only": True,
                    "enabled": True,
                    "description": f"Fabric lakehouse {cat['workspace']}/{cat['lakehouse']}",
                    "config": {
                        "workspace": cat["workspace"],
                        "lakehouse": cat["lakehouse"],
                        "tenant_id": cat["tenant_id"],
                        "client_id": cat["client_id"],
                        "client_secret": "********"
                    },
                    "status": "ACTIVE"
                })
        except Exception as e_ol:
            logger.debug(f"OneLake mounts append notice: {e_ol}")
        return {"mounts": mounts_out}
    except Exception as e:
        logger.error(f"Error listing mounts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/mounts")
async def create_or_update_mount_endpoint(payload: Dict[str, Any], request: Request):
    current_user = await resolve_principal(request)

    if current_user.get("role") not in ("admin", "power_user"):
        raise HTTPException(
            status_code=403,
            detail=f"Access denied: role '{current_user.get('role')}' cannot mount storage. Requires admin or power_user."
        )

    from web.mounts import create_or_update_mount, mask_mount_record
    try:
        payload_copy = dict(payload)
        payload_copy["owner"] = current_user.get("username", "admin")
        res = await asyncio.to_thread(create_or_update_mount, payload_copy)
        # Re-sync active duckrun connection asynchronously with a timeout
        def do_sync():
            try:
                conn = get_duckrun_conn()
                sync_catalogs_with_duckrun(conn)
            except Exception as e_s:
                logger.warning(f"Catalog sync warning after mount: {e_s}")

        try:
            await asyncio.wait_for(asyncio.to_thread(do_sync), timeout=12.0)
        except Exception as e_sync:
            logger.warning(f"Mount sync timeout or notice: {e_sync}")

        return {"success": True, "mount": mask_mount_record(res)}
    except Exception as e:
        from web.mounts import sanitize_connection_error
        clean_err = sanitize_connection_error(str(e), payload.get("config"), payload.get("type"))
        logger.error(f"Error creating/updating mount: {clean_err}")
        raise HTTPException(status_code=400, detail=clean_err)


@app.post("/api/mounts/{mount_id}/duplicate")
async def duplicate_mount_endpoint(mount_id: str, request: Request):
    current_user = await resolve_principal(request)

    if current_user.get("role") not in ("admin", "power_user"):
        raise HTTPException(
            status_code=403,
            detail=f"Access denied: role '{current_user.get('role')}' cannot duplicate mounts. Requires admin or power_user."
        )

    from web.mounts import duplicate_mount, mask_mount_record
    res = await asyncio.to_thread(duplicate_mount, mount_id)
    if not res:
        raise HTTPException(status_code=404, detail="Source mount not found")

    def do_sync():
        try:
            conn = get_duckrun_conn()
            sync_catalogs_with_duckrun(conn)
        except Exception as e_s:
            logger.warning(f"Catalog sync warning after duplicate: {e_s}")

    try:
        await asyncio.wait_for(asyncio.to_thread(do_sync), timeout=12.0)
    except Exception as e_sync:
        logger.warning(f"Mount sync timeout or notice: {e_sync}")

    return {"success": True, "mount": mask_mount_record(res)}


@app.delete("/api/mounts/{mount_id}")
async def delete_mount_endpoint(mount_id: str, request: Request):
    current_user = await resolve_principal(request)

    if mount_id.startswith("onelake_"):
        cat_id = mount_id.replace("onelake_", "")
        try:
            from web import onelake
            onelake.unmount_onelake_catalog(cat_id)
            return {"success": True, "deleted_id": mount_id}
        except Exception as e_ol:
            logger.error(f"Error unmounting OneLake catalog {cat_id}: {e_ol}")
            raise HTTPException(status_code=500, detail=str(e_ol))

    from web.mounts import load_mounts, delete_mount
    mounts = await asyncio.to_thread(load_mounts)
    target = next((m for m in mounts if m["id"] == mount_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Mount not found")

    if current_user.get("role") != "admin" and target.get("owner") != current_user.get("username"):
        raise HTTPException(status_code=403, detail="Access denied: only administrators or the mount owner can delete this mount.")

    try:
        ok = await asyncio.to_thread(delete_mount, mount_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Mount not found")

        # Re-sync active duckrun connection to detach the deleted mount
        def do_sync():
            try:
                conn = get_duckrun_conn()
                sync_catalogs_with_duckrun(conn)
            except Exception as e_s:
                logger.warning(f"Catalog sync warning after delete mount: {e_s}")

        try:
            await asyncio.wait_for(asyncio.to_thread(do_sync), timeout=12.0)
        except Exception as e_sync:
            logger.warning(f"Mount sync timeout or notice: {e_sync}")

        return {"success": True, "deleted_id": mount_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting mount {mount_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/mounts/test")
async def test_mount_endpoint(payload: Dict[str, Any]):
    from web.mounts import test_mount_connection, sanitize_connection_error
    try:
        res = await asyncio.wait_for(
            asyncio.to_thread(test_mount_connection, payload),
            timeout=15.0
        )
        return res
    except asyncio.TimeoutError:
        return {
            "success": False,
            "error": "Connection test timed out after 15 seconds. Please check the network endpoint and host accessibility."
        }
    except Exception as e:
        clean_err = sanitize_connection_error(str(e), payload.get("config"), payload.get("type"))
        logger.error(f"Error testing mount: {clean_err}")
        return {"success": False, "error": clean_err}


# ==============================================================================
# MLFLOW MODEL REGISTRY & LOCAL HTTP SERVING APIS
# ==============================================================================

@app.get("/api/2.0/mlflow/registered-models")
async def list_registered_models_endpoint():
    from web.serving import list_registered_models
    try:
        models = list_registered_models()
        return {"registered_models": models}
    except Exception as e:
        logger.error(f"Error listing registered models: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/2.0/mlflow/registered-models/get")
async def get_registered_model_by_query_endpoint(name: str):
    from web.serving import get_registered_model
    try:
        model = get_registered_model(name)
        if not model:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found")
        return {"registered_model": model}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting registered model {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/2.0/mlflow/registered-models/get-model-version-by-alias")
async def get_model_version_by_alias_endpoint(name: str, alias: str):
    from web.serving import get_registered_model, get_model_version
    try:
        m = get_registered_model(name)
        if not m:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found")
        clean_alias = alias.strip().lstrip("@").lower()
        version = m.get("aliases", {}).get(clean_alias)
        if version is None:
            raise HTTPException(status_code=404, detail=f"Alias '{alias}' not found on model '{name}'")
        v = get_model_version(name, version)
        return {"model_version": v}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting model version by alias: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/2.0/mlflow/registered-models/{name}")
async def get_registered_model_endpoint(name: str):
    from web.serving import get_registered_model
    try:
        model = get_registered_model(name)
        if not model:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found")
        return {"registered_model": model}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting registered model {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/2.0/mlflow/registered-models")
@app.post("/api/2.0/mlflow/registered-models/create")
async def create_registered_model_endpoint(payload: Dict[str, Any]):
    from web.serving import create_registered_model
    name = payload.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Model name is required")
    try:
        m = create_registered_model(
            name=name,
            catalog_name=payload.get("catalog_name", "warehouse"),
            schema_name=payload.get("schema_name", "dbo"),
            description=payload.get("description", ""),
            tags=payload.get("tags")
        )
        return {"registered_model": m}
    except Exception as e:
        logger.error(f"Error creating registered model: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/2.0/mlflow/registered-models/{name}")
async def delete_registered_model_endpoint(name: str):
    from web.serving import delete_registered_model
    try:
        ok = delete_registered_model(name)
        if not ok:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found")
        return {"success": True, "deleted_name": name}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting registered model {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/2.0/mlflow/model-versions")
@app.post("/api/2.0/mlflow/model-versions/create")
async def create_model_version_endpoint(payload: Dict[str, Any]):
    from web.serving import create_model_version
    name = payload.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Model name is required")
    try:
        v = create_model_version(
            name=name,
            run_id=payload.get("run_id"),
            stage=payload.get("stage", "None"),
            algorithm=payload.get("algorithm", "custom"),
            metrics=payload.get("metrics"),
            signature=payload.get("signature"),
            description=payload.get("description", ""),
            source=payload.get("source", "")
        )
        return {"model_version": v}
    except Exception as e:
        logger.error(f"Error creating model version: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/2.0/mlflow/model-versions/get")
async def get_model_version_endpoint(name: str, version: int):
    from web.serving import get_model_version
    try:
        v = get_model_version(name, int(version))
        if not v:
            raise HTTPException(status_code=404, detail=f"Model version '{name}' v{version} not found")
        return {"model_version": v}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting model version: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/2.0/mlflow/model-versions/transition-stage")
async def transition_stage_endpoint(payload: Dict[str, Any]):
    from web.serving import transition_model_version_stage
    name = payload.get("name")
    version = payload.get("version")
    stage = payload.get("stage")
    if not name or version is None or not stage:
        raise HTTPException(status_code=400, detail="Name, version, and stage are required")
    try:
        v = transition_model_version_stage(
            name=name,
            version=int(version),
            stage=stage,
            archive_existing_versions=payload.get("archive_existing_versions", True)
        )
        return {"model_version": v}
    except Exception as e:
        logger.error(f"Error transitioning stage: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/2.0/mlflow/model-versions/{name}/{version}")
async def delete_model_version_endpoint(name: str, version: int):
    from web.serving import delete_model_version
    try:
        ok = delete_model_version(name, int(version))
        if not ok:
            raise HTTPException(status_code=404, detail=f"Model version {name} v{version} not found")
        return {"success": True, "name": name, "version": version}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting model version: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/models/endpoints")
async def list_serving_endpoints_api():
    from web.serving import list_serving_endpoints
    try:
        return {"endpoints": list_serving_endpoints()}
    except Exception as e:
        logger.error(f"Error listing serving endpoints: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/models/{name}/serving")
async def configure_model_serving_endpoint(name: str, payload: Dict[str, Any]):
    from web.serving import create_or_update_serving_endpoint
    try:
        stage = payload.get("stage", "Production")
        version = payload.get("version")
        state = payload.get("state", "READY")
        ep = create_or_update_serving_endpoint(
            model_name=name,
            stage=stage,
            version=int(version) if version is not None else None,
            state=state,
            endpoint_name=payload.get("endpoint_name")
        )
        return {"endpoint": ep}
    except Exception as e:
        logger.error(f"Error configuring model serving: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/models/{name}/score")
async def score_model_endpoint(name: str, payload: Dict[str, Any], version: Optional[int] = None):
    from web.serving import score_model
    try:
        res = score_model(model_name=name, payload=payload, version=version)
        return res
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        logger.error(f"Error scoring model {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/models/{name}/aliases")
async def get_model_aliases_endpoint(name: str):
    from web.serving import get_model_aliases
    try:
        aliases = get_model_aliases(name)
        return {"aliases": aliases}
    except Exception as e:
        logger.error(f"Error fetching aliases for model {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/models/{name}/aliases")
async def set_model_alias_endpoint(name: str, payload: Dict[str, Any]):
    from web.serving import set_model_alias
    alias = payload.get("alias")
    version = payload.get("version")
    if not alias or version is None:
        raise HTTPException(status_code=400, detail="Both 'alias' and 'version' are required")
    try:
        set_model_alias(name, str(alias).strip().lstrip("@"), int(version))
        return {"success": True, "model": name, "alias": alias, "version": int(version)}
    except Exception as e:
        logger.error(f"Error setting alias {alias} for {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/models/{name}/aliases/{alias}")
async def delete_model_alias_endpoint(name: str, alias: str):
    from web.serving import delete_model_alias
    try:
        ok = delete_model_alias(name, str(alias).strip().lstrip("@"))
        return {"success": ok, "model": name, "alias": alias}
    except Exception as e:
        logger.error(f"Error deleting alias {alias} for {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/2.0/mlflow/registered-models/alias")
@app.post("/api/2.0/mlflow/registered-models/set-alias")
async def mlflow_set_model_alias(payload: Dict[str, Any]):
    from web.serving import set_model_alias
    name = payload.get("name")
    alias = payload.get("alias")
    version = payload.get("version")
    if not name or not alias or version is None:
        raise HTTPException(status_code=400, detail="Fields 'name', 'alias', and 'version' are required")
    try:
        set_model_alias(name, str(alias).strip().lstrip("@"), int(version))
        return {"success": True, "name": name, "alias": alias, "version": int(version)}
    except Exception as e:
        logger.error(f"Error setting MLflow alias: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/2.0/mlflow/registered-models/alias")
@app.post("/api/2.0/mlflow/registered-models/delete-alias")
async def mlflow_delete_model_alias(payload: Optional[Dict[str, Any]] = None, name: Optional[str] = None, alias: Optional[str] = None):
    from web.serving import delete_model_alias
    if payload:
        name = payload.get("name", name)
        alias = payload.get("alias", alias)
    if not name or not alias:
        raise HTTPException(status_code=400, detail="Fields 'name' and 'alias' are required")
    try:
        ok = delete_model_alias(name, str(alias).strip().lstrip("@"))
        return {"success": ok, "name": name, "alias": alias}
    except Exception as e:
        logger.error(f"Error deleting MLflow alias: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# UNITY CATALOG VOLUMES & AUTO-LOADER PIPELINES
# ==============================================================================

@app.get("/api/volumes")
async def get_volumes_endpoint(catalog: Optional[str] = None, schema: Optional[str] = None):
    from web.volumes import list_volumes
    try:
        vols = list_volumes(catalog, schema)
        return {"volumes": vols}
    except Exception as e:
        logger.error(f"Error listing volumes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/volumes")
async def create_volume_endpoint(payload: Dict[str, Any], request: Request):
    from web.volumes import create_volume
    current_user = await resolve_principal(request)
    
    if current_user.get("role") not in ("admin", "power_user"):
        raise HTTPException(status_code=403, detail="Only admins and power users can create volumes.")

    cat = payload.get("catalog") or "warehouse"
    sch = payload.get("schema") or "raw"
    name = payload.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Volume name is required.")

    try:
        vol = create_volume(
            catalog=cat,
            schema=sch,
            name=name,
            description=payload.get("description", ""),
            volume_type=payload.get("volume_type", "MANAGED"),
            external_location=payload.get("external_location"),
            owner=current_user.get("username", "admin")
        )
        return vol
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Error creating volume: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/volumes/{catalog}/{schema}/{volume_name}")
async def delete_volume_endpoint(catalog: str, schema: str, volume_name: str, request: Request):
    from web.volumes import delete_volume
    current_user = await resolve_principal(request)

    if current_user.get("role") not in ("admin", "power_user"):
        raise HTTPException(status_code=403, detail="Only admins and power users can delete volumes.")

    ok = delete_volume(catalog, schema, volume_name)
    if not ok:
        raise HTTPException(status_code=404, detail="Volume not found.")
    return {"success": True, "message": f"Volume '{catalog}.{schema}.{volume_name}' deleted."}


@app.get("/api/volumes/{catalog}/{schema}/{volume_name}/files")
async def get_volume_files_endpoint(catalog: str, schema: str, volume_name: str, subpath: str = ""):
    from web.volumes import list_volume_files
    try:
        files = list_volume_files(catalog, schema, volume_name, subpath=subpath)
        return {
            "catalog": catalog,
            "schema": schema,
            "volume_name": volume_name,
            "volume_path": f"/Volumes/{catalog}/{schema}/{volume_name}",
            "subpath": subpath,
            "files": files
        }
    except Exception as e:
        logger.error(f"Error listing volume files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/volumes/{catalog}/{schema}/{volume_name}/upload")
async def upload_volume_file_endpoint(
    catalog: str,
    schema: str,
    volume_name: str,
    file: UploadFile = File(...),
    subpath: str = Form("")
):
    from web.volumes import upload_file_to_volume
    try:
        content = await file.read()
        res = upload_file_to_volume(catalog, schema, volume_name, file.filename, content, subpath=subpath)
        return res
    except Exception as e:
        logger.error(f"Error uploading file to volume: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/volumes/{catalog}/{schema}/{volume_name}/files")
async def delete_volume_file_endpoint(
    catalog: str,
    schema: str,
    volume_name: str,
    path: str,
    request: Request
):
    from web.volumes import delete_file_from_volume
    current_user = await resolve_principal(request)

    if current_user.get("role") not in ("admin", "power_user"):
        raise HTTPException(status_code=403, detail="Only admins and power users can delete files from volumes.")

    try:
        ok = delete_file_from_volume(catalog, schema, volume_name, path)
        if not ok:
            raise HTTPException(status_code=404, detail="File not found in volume.")
        return {"success": True, "message": f"Deleted '{path}'"}
    except Exception as e:
        logger.error(f"Error deleting volume file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/volumes/{catalog}/{schema}/{volume_name}/preview")
async def preview_volume_file_endpoint(
    catalog: str,
    schema: str,
    volume_name: str,
    path: str,
    limit: int = 10
):
    from web.volumes import preview_volume_file
    try:
        res = preview_volume_file(catalog, schema, volume_name, path, limit=limit)
        return res
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        logger.error(f"Error previewing volume file: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ------------------------------------------------------------------------------
# Auto-Loader Pipeline Endpoints
# ------------------------------------------------------------------------------

def _pipeline_access(user: Dict[str, Any], pipe: Dict[str, Any]) -> Optional[str]:
    """manage | run | None. Admins and power users manage every pipeline (as before); a plain user can be given RUN or MANAGE on a
    pipeline directly or through a group (Share dialog / IAM > Groups)."""
    from web.groups import permission_of
    if user.get("role") in ("admin", "power_user") or user.get("username") == pipe.get("created_by"):
        return "manage"
    granted = permission_of(user, "pipeline", pipe.get("id", ""))
    return {"MANAGE": "manage", "RUN": "run"}.get(granted or "")


@app.get("/api/autoloader/pipelines")
async def get_autoloader_pipelines(request: Request):
    from web.autoloader import list_pipelines
    current_user = await resolve_principal(request)
    try:
        pipes = [{**p, "my_access": _pipeline_access(current_user, p)} for p in list_pipelines()]
        return {"pipelines": pipes}
    except Exception as e:
        logger.error(f"Error listing autoloader pipelines: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------- Groups (web/groups.py) and generic resource grants
class GroupPayload(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class GroupMembersPayload(BaseModel):
    user_ids: List[str]


class GrantPayload(BaseModel):
    principal: str            # user:<id or username> | group:<id>
    permission: str


def _groups_call(fn, *args, **kw):
    from web import groups
    try:
        return fn(*args, **kw)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except groups.GroupError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/groups")
async def list_groups_endpoint(current_user: Dict[str, Any] = Depends(require_role(["admin", "power_user"]))):
    from web import groups
    return {"groups": groups.list_groups()}

@app.post("/api/groups")
async def create_group_endpoint(payload: GroupPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import groups
    return _groups_call(groups.create_group, payload.name or "", payload.description or "", current_user.get("username", "admin"))

@app.put("/api/groups/{group_id}")
async def update_group_endpoint(group_id: str, payload: GroupPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import groups
    return _groups_call(groups.update_group, group_id, payload.name, payload.description, current_user.get("username", "admin"))

@app.delete("/api/groups/{group_id}")
async def delete_group_endpoint(group_id: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import groups
    _groups_call(groups.delete_group, group_id, current_user.get("username", "admin"))
    return {"success": True}

@app.get("/api/groups/{group_id}/members")
async def list_group_members_endpoint(group_id: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import groups
    if not groups.get_group(group_id):
        raise HTTPException(status_code=404, detail="Group not found.")
    return {"members": groups.list_members(group_id)}

@app.post("/api/groups/{group_id}/members")
async def add_group_members_endpoint(group_id: str, payload: GroupMembersPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import groups
    return {"members": _groups_call(groups.add_members, group_id, payload.user_ids, current_user.get("username", "admin"))}

@app.delete("/api/groups/{group_id}/members/{user_id}")
async def remove_group_member_endpoint(group_id: str, user_id: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import groups
    _groups_call(groups.remove_member, group_id, user_id, current_user.get("username", "admin"))
    return {"success": True}

class GroupMappingPayload(BaseModel):
    source: str = "local"
    external_ref: Optional[str] = ""


@app.put("/api/groups/{group_id}/mapping")
async def set_group_mapping_endpoint(group_id: str, payload: GroupMappingPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Maps the group to an LDAP group DN / OIDC group value (members then follow the directory), or clears it (source 'local')."""
    from web import groups
    return _groups_call(groups.set_mapping, group_id, payload.source, payload.external_ref or "", current_user.get("username", "admin"))

@app.get("/api/groups/directory/ldap")
async def list_ldap_groups_endpoint(q: str = "", current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """LDAP groups to choose from when mapping (admins only; uses the configured service account)."""
    from web import ldap_auth
    try:
        return {"groups": await asyncio.to_thread(ldap_auth.list_directory_groups, None, q)}
    except ldap_auth.LdapError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@app.get("/api/principals")
async def search_principals_endpoint(q: str = "", current_user: Dict[str, Any] = Depends(get_current_user)):
    """Users and groups to pick from in a share dialog (at most 20 of each; users need at least 2 typed characters)."""
    from web import groups
    from web.auth import get_db_connection
    q = (q or "").strip().lower()
    users = []
    if len(q) >= 2:
        conn = get_db_connection()
        try:
            users = [{"principal": f"user:{r['id']}", "type": "user", "name": r["username"], "display_name": r["display_name"], "auth_source": r["auth_source"]}
                     for r in conn.execute("SELECT id, username, display_name, auth_source FROM users WHERE deleted_at IS NULL AND is_active = 1 "
                                           "AND (lower(username) LIKE ? OR lower(display_name) LIKE ?) ORDER BY username LIMIT 20", (f"%{q}%", f"%{q}%"))]
        finally:
            conn.close()
    gs = [{"principal": f"group:{g['id']}", "type": "group", "name": g["name"], "member_count": g["member_count"]}
          for g in groups.list_groups() if not q or q in g["name"].lower()][:20]
    return {"users": users, "groups": gs}


def _may_manage_grants(user: Dict[str, Any], resource_type: str, resource_id: str) -> bool:
    if user.get("role") == "admin":
        return True
    if resource_type == "saved_query":
        from web.saved_queries import get_saved_query
        q = get_saved_query(resource_id)
        return bool(q) and user.get("username") in (q.get("owner"), q.get("created_by"))
    if resource_type in ("table", "schema"):
        # Data access is decided by whoever governs the catalog: an administrator, or the catalog's owner (a power user).
        return can_user_manage_catalog(user, (resource_id or "").split(".")[0].lower())
    if resource_type == "pipeline":
        from web.autoloader import get_pipeline
        p = get_pipeline(resource_id)
        return bool(p) and _pipeline_access(user, p) == "manage"
    return False


@app.get("/api/catalogs/{cat_id}/table-grants")
async def list_catalog_table_grants_endpoint(cat_id: str, request: Request):
    """Every table- and schema-level grant inside a catalog (for whoever manages the catalog)."""
    from web import groups
    current_user = await resolve_principal(request)
    if not can_user_manage_catalog(current_user, cat_id):
        raise HTTPException(status_code=403, detail=f"Access denied: you do not have permission to inspect access lists for catalog '{cat_id}'.")
    return {"grants": groups.list_grants_prefix(("schema", "table"), cat_id.lower() + ".")}


@app.get("/api/grants/{resource_type}/{resource_id}")
async def list_grants_endpoint(resource_type: str, resource_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    from web import groups
    if not _may_manage_grants(current_user, resource_type, resource_id):
        raise HTTPException(status_code=403, detail="Only the owner or an administrator can see who this is shared with.")
    return {"permissions": list(groups.RESOURCE_TYPES.get(resource_type, ())), "grants": _groups_call(groups.list_grants, resource_type, resource_id)}

@app.post("/api/grants/{resource_type}/{resource_id}")
async def set_grant_endpoint(resource_type: str, resource_id: str, payload: GrantPayload, current_user: Dict[str, Any] = Depends(get_current_user)):
    from web import groups
    if not _may_manage_grants(current_user, resource_type, resource_id):
        raise HTTPException(status_code=403, detail="Only the owner or an administrator can share this.")
    return _groups_call(groups.grant, resource_type, resource_id, payload.principal, payload.permission, current_user.get("username", "admin"))

@app.delete("/api/grants/{resource_type}/{resource_id}")
async def revoke_grant_endpoint(resource_type: str, resource_id: str, principal: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    from web import groups
    if not _may_manage_grants(current_user, resource_type, resource_id):
        raise HTTPException(status_code=403, detail="Only the owner or an administrator can change who this is shared with.")
    if not _groups_call(groups.revoke, resource_type, resource_id, principal, current_user.get("username", "admin")):
        raise HTTPException(status_code=404, detail="That grant does not exist.")
    return {"success": True}


# ---------------------------------------------------------------- Connections (web/connections.py): HTTP(S)/REST and SFTP sources
class ConnectionPayload(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = ""
    config: Optional[Dict[str, Any]] = None
    secret: Optional[Dict[str, Any]] = None


@app.get("/api/connections")
async def list_connections_endpoint(current_user: Dict[str, Any] = Depends(require_role(["admin", "power_user"]))):
    """Names, types and non-secret settings (never a secret; `has_secret` says whether one is stored)."""
    from web import connections
    return {"connections": connections.list_connections()}

@app.post("/api/connections")
async def create_connection_endpoint(payload: ConnectionPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import connections
    try:
        return connections.create_connection(payload.dict(), current_user.get("username", "admin"))
    except connections.ConnectionError_ as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@app.post("/api/connections/test")
async def test_connection_endpoint(payload: ConnectionPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    """Tries a definition (saved or not) and reports reachability; for SFTP without a pinned host key, the fingerprint the server presents."""
    from web import connections, autoloader_conn
    try:
        definition = connections.definition_for_test(payload.dict())
    except connections.ConnectionError_ as exc:
        return {"ok": False, "message": str(exc)}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return await asyncio.to_thread(autoloader_conn.test_connection, definition)

@app.put("/api/connections/{conn_id}")
async def update_connection_endpoint(conn_id: str, payload: ConnectionPayload, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import connections
    try:
        return connections.update_connection(conn_id, payload.dict(), current_user.get("username", "admin"))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except connections.ConnectionError_ as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@app.delete("/api/connections/{conn_id}")
async def delete_connection_endpoint(conn_id: str, current_user: Dict[str, Any] = Depends(require_role(["admin"]))):
    from web import connections
    try:
        connections.delete_connection(conn_id, current_user.get("username", "admin"))
        return {"success": True}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except connections.ConnectionError_ as exc:
        raise HTTPException(status_code=409, detail=str(exc))


class SourcePreviewPayload(BaseModel):
    source_volume_path: str
    source_options: Optional[Dict[str, Any]] = None
    file_pattern: Optional[str] = "*"
    limit: Optional[int] = 10


@app.post("/api/autoloader/preview-source")
async def preview_autoloader_source_endpoint(payload: SourcePreviewPayload, current_user: Dict[str, Any] = Depends(require_role(["admin", "power_user"]))):
    """The first rows of a connection source (HTTP file, REST API first page, SFTP file) as the pipeline would read them. Nothing is stored."""
    from web import autoloader_conn
    if not autoloader_conn.is_conn_path(payload.source_volume_path):
        raise HTTPException(status_code=400, detail="A preview needs a connection source (conn://<connection>/<path>).")
    try:
        return await asyncio.to_thread(autoloader_conn.preview, payload.source_volume_path, payload.source_options, payload.file_pattern or "*", payload.limit or 10)
    except autoloader_conn.SourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/autoloader/target-catalogs")
async def get_autoloader_target_catalogs(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Catalogs a pipeline can load into: writable local catalogs and writable S3 mounts (the create dialog's dropdown)."""
    from web.autoloader import target_catalogs
    return {"catalogs": await asyncio.to_thread(target_catalogs)}


@app.post("/api/autoloader/pipelines")
async def create_autoloader_pipeline_endpoint(payload: Dict[str, Any], request: Request):
    from web.autoloader import create_pipeline
    current_user = await resolve_principal(request)

    if current_user.get("role") not in ("admin", "power_user"):
        raise HTTPException(status_code=403, detail="Only admins and power users can create Auto-Loader pipelines.")

    try:
        pipe = create_pipeline(payload, created_by=current_user.get("username", "admin"))
        return pipe
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Error creating pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/autoloader/pipelines/{pipeline_id}")
async def get_autoloader_pipeline_endpoint(pipeline_id: str):
    from web.autoloader import get_pipeline
    pipe = get_pipeline(pipeline_id)
    if not pipe:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipe


@app.put("/api/autoloader/pipelines/{pipeline_id}")
async def update_autoloader_pipeline_endpoint(pipeline_id: str, payload: Dict[str, Any], request: Request):
    from web.autoloader import update_pipeline
    from web.autoloader import get_pipeline as _get_pipe
    current_user = await resolve_principal(request)

    existing = _get_pipe(pipeline_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    if _pipeline_access(current_user, existing) != "manage":
        raise HTTPException(status_code=403, detail="Only admins, power users and users with Manage access can modify Auto-Loader pipelines.")

    try:
        pipe = update_pipeline(pipeline_id, payload)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    if not pipe:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipe


@app.delete("/api/autoloader/pipelines/{pipeline_id}")
async def delete_autoloader_pipeline_endpoint(pipeline_id: str, request: Request):
    from web.autoloader import delete_pipeline, get_pipeline as _get_pipe
    from web import groups
    current_user = await resolve_principal(request)

    existing = _get_pipe(pipeline_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    if _pipeline_access(current_user, existing) != "manage":
        raise HTTPException(status_code=403, detail="Only admins, power users and users with Manage access can delete Auto-Loader pipelines.")

    ok = delete_pipeline(pipeline_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    groups.delete_grants_for_resource("pipeline", pipeline_id)
    return {"success": True, "message": f"Pipeline '{pipeline_id}' deleted."}


@app.post("/api/autoloader/pipelines/{pipeline_id}/run")
@app.post("/api/autoloader/pipelines/{pipeline_id}/run-now")
async def run_autoloader_pipeline_now(pipeline_id: str, request: Request):
    from web.autoloader import run_pipeline_cycle, get_pipeline as _get_pipe
    current_user = await resolve_principal(request)
    existing = _get_pipe(pipeline_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    if _pipeline_access(current_user, existing) is None:
        raise HTTPException(status_code=403, detail="You need Run or Manage access on this pipeline (ask an administrator to add you or one of your groups).")
    try:
        res = run_pipeline_cycle(pipeline_id)
        if "error" in res and res.get("files_found") is None:
            raise HTTPException(status_code=400, detail=res["error"])
        return res
    except Exception as e:
        logger.error(f"Error running pipeline '{pipeline_id}': {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/autoloader/pipelines/{pipeline_id}/reset")
async def reset_autoloader_pipeline_checkpoints(pipeline_id: str, request: Request):
    from web.autoloader import reset_pipeline_checkpoints
    from web.autoloader import get_pipeline as _get_pipe
    current_user = await resolve_principal(request)

    existing = _get_pipe(pipeline_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    if _pipeline_access(current_user, existing) != "manage":
        raise HTTPException(status_code=403, detail="Only admins, power users and users with Manage access can reset checkpoints.")

    res = reset_pipeline_checkpoints(pipeline_id)
    return res


@app.get("/api/autoloader/pipelines/{pipeline_id}/history")
async def get_autoloader_pipeline_history(pipeline_id: str, limit: int = 50):
    from web.autoloader import get_pipeline_history
    hist = get_pipeline_history(pipeline_id, limit=limit)
    return {"history": hist}


@app.get("/api/autoloader/stats")
async def get_autoloader_stats_endpoint():
    from web.autoloader import get_autoloader_stats
    stats = get_autoloader_stats()
    return stats











