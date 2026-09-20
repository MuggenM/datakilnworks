"""
Multi-User Role-Based Access Control (RBAC) & Authentication Engine for Localspark.
Provides PBKDF2-HMAC-SHA256 password hashing, PyJWT token generation/verification,
SQLite persistence for users and settings, and FastAPI authentication dependencies.
"""

import os
import sqlite3
import datetime
import hashlib
import secrets
import logging
from typing import Dict, Any, List, Optional
from datetime import timedelta

import jwt
from fastapi import Request, HTTPException, Depends, status

from web.secrets_store import load_or_create_secret

logger = logging.getLogger("localspark.auth")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
AUTH_DB_FILE = os.path.join(METADATA_DIR, "auth.db")



# The previous hard-coded default key lived in the public repository, so anyone could forge an admin token.
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY") or load_or_create_secret("jwt_secret")
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours

COOKIE_NAME = "localspark_session"


def get_db_connection() -> sqlite3.Connection:
    """Returns a SQLite connection to the auth database."""
    os.makedirs(METADATA_DIR, exist_ok=True)
    conn = sqlite3.connect(AUTH_DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_auth_db():
    """Initializes auth.db tables and seeds default users if empty."""
    os.makedirs(METADATA_DIR, exist_ok=True)
    conn = get_db_connection()
    try:
        with conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin', 'power_user', 'user')),
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                last_login_at TEXT
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS catalog_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                catalog_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                permission TEXT NOT NULL CHECK(permission IN ('READ', 'WRITE', 'ADMIN')),
                granted_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(catalog_id, user_id)
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_by TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)

            now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            # Seed default users if users table is empty
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            count = cur.fetchone()[0]

            if count == 0:
                logger.info("Seeding default multi-user accounts in auth.db...")

                seed_users = [
                    (
                        "u_admin_01",
                        "admin",
                        hash_password("adminpassword123"),
                        "System Administrator",
                        "admin",
                        1,
                        now_str
                    ),
                    (
                        "u_power_02",
                        "lead_engineer",
                        hash_password("powerpassword123"),
                        "Lead Data Engineer",
                        "power_user",
                        1,
                        now_str
                    ),
                    (
                        "u_user_03",
                        "analyst_bob",
                        hash_password("userpassword123"),
                        "Bob the Analyst",
                        "user",
                        1,
                        now_str
                    ),
                    (
                        "u_admin_martin",
                        "martin",
                        hash_password("adminpassword123"),
                        "Martin (Admin)",
                        "admin",
                        1,
                        now_str
                    )
                ]

                cur.executemany("""
                INSERT INTO users (id, username, password_hash, display_name, role, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, seed_users)

            # Ensure martin exists if db was already created
            cur.execute("SELECT COUNT(*) FROM users WHERE username = 'martin'")
            if cur.fetchone()[0] == 0:
                now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                cur.execute("""
                    INSERT INTO users (id, username, password_hash, display_name, role, is_active, created_at)
                    VALUES ('u_admin_martin', 'martin', ?, 'Martin (Admin)', 'admin', 1, ?)
                """, (hash_password("adminpassword123"), now_str))

            # Seed initial default settings
            conn.execute("""
                INSERT OR IGNORE INTO app_settings (key, value_json, updated_by, updated_at)
                VALUES 
                    ('workspace_name', '"Localspark Lakehouse Studio"', 'admin', ?),
                    ('allow_guest_mode', 'false', 'admin', ?),
                    ('default_warehouse', '"wh_starter"', 'admin', ?),
                    ('session_timeout_minutes', '1440', 'admin', ?)
                """, (now_str, now_str, now_str, now_str))
            logger.info("Successfully initialized and seeded auth.db")
    finally:
        conn.close()


def hash_password(password: str) -> str:
    """Hashes a password using PBKDF2-HMAC-SHA256 with 100,000 iterations and a 16-byte random salt."""
    salt = secrets.token_hex(16)
    pw_hash = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt.encode('utf-8'),
        100000
    ).hex()
    return f"pbkdf2_sha256$100000${salt}${pw_hash}"


def verify_password(password: str, hashed: str) -> bool:
    """Verifies a plain password against the stored PBKDF2 hash."""
    try:
        parts = hashed.split('$')
        if len(parts) != 4 or parts[0] != "pbkdf2_sha256":
            return False
        iterations = int(parts[1])
        salt = parts[2]
        expected_hash = parts[3]

        candidate_hash = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            iterations
        ).hex()

        return secrets.compare_digest(candidate_hash, expected_hash)
    except Exception as e:
        logger.warning(f"Error verifying password: {e}")
        return False


def create_access_token(user: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """Encodes a signed JWT access token carrying sub, username, display_name, and role."""
    now = datetime.datetime.now(datetime.timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode = {
        "sub": user["id"],
        "username": user["username"],
        "display_name": user["display_name"],
        "role": user["role"],
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp())
    }

    return jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> Optional[Dict[str, Any]]:
    """Decodes and validates a JWT token, returning payload dict or None if invalid/expired."""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except Exception:
        return None


# ==============================================================================
# USER CRUD OPERATIONS
# ==============================================================================

def get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    """Fetches user record by ID."""
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if row:
            u = dict(row)
            u.pop("password_hash", None)
            return u
        return None
    finally:
        conn.close()


def get_user_by_username(username: str, include_password_hash: bool = False) -> Optional[Dict[str, Any]]:
    """Fetches user record by username."""
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username.strip().lower(),)).fetchone()
        if row:
            u = dict(row)
            if not include_password_hash:
                u.pop("password_hash", None)
            return u
        return None
    finally:
        conn.close()


def list_users() -> List[Dict[str, Any]]:
    """Returns all registered users (excluding password hashes)."""
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT id, username, display_name, role, is_active, created_at, last_login_at FROM users ORDER BY created_at ASC").fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["full_name"] = d.get("display_name") or d["username"]
            d["email"] = f"{d['username']}@localspark.lakehouse"
            result.append(d)
        return result
    finally:
        conn.close()


def create_user(username: str, password: str, display_name: str, role: str = "user") -> Dict[str, Any]:
    """Creates a new user record in auth.db."""
    clean_username = username.strip().lower()
    if not clean_username:
        raise ValueError("Username cannot be empty")
    if role not in ('admin', 'power_user', 'user'):
        raise ValueError(f"Invalid role '{role}'. Allowed roles: admin, power_user, user")
    if len(password) < 4:
        raise ValueError("Password must be at least 4 characters long")

    conn = get_db_connection()
    try:
        existing = conn.execute("SELECT id FROM users WHERE username = ?", (clean_username,)).fetchone()
        if existing:
            raise ValueError(f"Username '{clean_username}' is already taken.")

        user_id = f"u_{clean_username}_{secrets.token_hex(4)}"
        now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        pw_hash = hash_password(password)

        with conn:
            conn.execute("""
            INSERT INTO users (id, username, password_hash, display_name, role, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, 1, ?)
            """, (user_id, clean_username, pw_hash, display_name.strip() or clean_username, role, now_str))

        return {
            "id": user_id,
            "username": clean_username,
            "display_name": display_name.strip() or clean_username,
            "role": role,
            "is_active": 1,
            "created_at": now_str
        }
    finally:
        conn.close()


def update_user(user_id: str, display_name: Optional[str] = None, role: Optional[str] = None, is_active: Optional[bool] = None) -> Optional[Dict[str, Any]]:
    """Updates user display_name, role, or active status."""
    conn = get_db_connection()
    try:
        existing = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if not existing:
            return None

        fields = []
        params = []
        if display_name is not None:
            fields.append("display_name = ?")
            params.append(display_name.strip())
        if role is not None:
            if role not in ('admin', 'power_user', 'user'):
                raise ValueError(f"Invalid role '{role}'")
            fields.append("role = ?")
            params.append(role)
        if is_active is not None:
            fields.append("is_active = ?")
            params.append(1 if is_active else 0)

        if not fields:
            return get_user_by_id(user_id)

        params.append(user_id)
        with conn:
            conn.execute(f"UPDATE users SET {', '.join(fields)} WHERE id = ?", params)

        return get_user_by_id(user_id)
    finally:
        conn.close()


def reset_user_password(user_id: str, new_password: str) -> bool:
    """Updates password hash for the specified user."""
    if len(new_password) < 4:
        raise ValueError("Password must be at least 4 characters long")
    conn = get_db_connection()
    try:
        pw_hash = hash_password(new_password)
        with conn:
            res = conn.execute("UPDATE users SET password_hash = ? WHERE id = ?", (pw_hash, user_id))
            return res.rowcount > 0
    finally:
        conn.close()


def delete_user(user_id: str) -> bool:
    """Deactivates a user (soft delete). Prevents deleting the primary admin."""
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT username, role FROM users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            return False
        if row["username"] == "admin":
            raise ValueError("The primary 'admin' account cannot be deactivated or deleted.")

        with conn:
            conn.execute("UPDATE users SET is_active = 0 WHERE id = ?", (user_id,))
            return True
    finally:
        conn.close()


def record_user_login(user_id: str):
    """Records the timestamp of a successful user login."""
    conn = get_db_connection()
    try:
        now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with conn:
            conn.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (now_str, user_id))
    finally:
        conn.close()


# ==============================================================================
# FASTAPI DEPENDENCIES & AUTH MIDDLEWARE HELPERS
# ==============================================================================

async def get_current_user(request: Request) -> Dict[str, Any]:
    """
    FastAPI dependency to extract and authenticate the current user.
    Checks:
      1. Cookie: localspark_session
      2. Authorization: Bearer <token>
      3. X-User header (fallback for backward compatibility/CLI, validates against users table)
    """
    token = request.cookies.get(COOKIE_NAME)

    auth_header = request.headers.get("Authorization")
    if not token and auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()

    if token:
        payload = decode_access_token(token)
        if payload and "sub" in payload:
            user = get_user_by_id(payload["sub"])
            if user and user.get("is_active", 1) == 1:
                return user

    # Fallback to X-User header if supplied. It carries no credential, so it is disabled once auth is required.
    x_user = request.headers.get("X-User")
    if x_user and not governance_require_auth():
        user = get_user_by_username(x_user)
        if user and user.get("is_active", 1) == 1:
            return user

    # If auth db hasn't been initialized or dev fallback, check if guest mode allowed
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT value_json FROM app_settings WHERE key = 'allow_guest_mode'").fetchone()
        if row and row[0] == "true":
            # Return guest admin user
            admin_u = get_user_by_username("admin")
            if admin_u:
                return admin_u
    finally:
        conn.close()

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required. Please log in.",
        headers={"WWW-Authenticate": "Bearer"}
    )


def governance_require_auth() -> bool:
    """When true, requests without credentials are anonymous (least privilege) instead of the local admin."""
    return os.getenv("GOVERNANCE_REQUIRE_AUTH", "false").strip().lower() in ("1", "true", "yes", "on")


LOCAL_ADMIN = {"role": "admin", "username": "admin", "id": "u_admin_01"}
ANONYMOUS = {"role": "user", "username": "anonymous", "id": "anonymous"}


def _presented_credentials(request: Request) -> bool:
    """True when the request tried to authenticate (cookie, bearer token or X-User), valid or not."""
    if request.cookies.get(COOKIE_NAME):
        return True
    header = request.headers.get("Authorization") or ""
    if header.startswith("Bearer ") and header[7:].strip():
        return True
    return bool(request.headers.get("X-User")) and not governance_require_auth()


async def resolve_principal(request: Request) -> Dict[str, Any]:
    """
    Single source of truth for "who is calling" in endpoints that tolerate anonymous access.

    - Valid credentials -> that user.
    - Credentials presented but invalid/expired/unknown/inactive -> 401. NEVER admin.
    - No credentials at all -> the local single-user admin (the documented no-login mode), or the
      least-privilege `anonymous` user when GOVERNANCE_REQUIRE_AUTH is set.
    - Unexpected errors propagate (500) instead of silently granting admin.
    """
    try:
        return dict(await get_current_user(request))
    except HTTPException as exc:
        if exc.status_code != status.HTTP_401_UNAUTHORIZED or _presented_credentials(request):
            raise
        return dict(ANONYMOUS if governance_require_auth() else LOCAL_ADMIN)


def require_role(allowed_roles: List[str]):
    """FastAPI dependency factory enforcing that the authenticated user holds one of allowed_roles."""
    async def role_checker(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
        user_role = current_user.get("role", "user")
        if user_role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied: role '{user_role}' does not have required permissions ({', '.join(allowed_roles)})."
            )
        return current_user
    return role_checker


# Auto-initialize database tables on module load
init_auth_db()
