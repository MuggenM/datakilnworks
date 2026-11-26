"""
SQLite persistence for governance metadata ($WAREHOUSE_DIR/.metadata/governance.db).

Follows the repo convention of one SQLite file per feature with its own init function. Every write bumps
`governance_meta.version`; readers cache derived indexes against that number, which keeps caches correct across
uvicorn reloads and multiple processes sharing the warehouse volume.
"""

import datetime
import json
import logging
import os
import sqlite3
import threading
from typing import Any, Dict, Optional

logger = logging.getLogger("localspark.governance")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
GOV_DB_PATH = os.path.join(METADATA_DIR, "governance.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS governance_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tag_definitions (
    tag_key TEXT PRIMARY KEY,
    description TEXT,
    allowed_values TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS object_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    catalog TEXT NOT NULL,
    schema_name TEXT NOT NULL DEFAULT '',
    table_name TEXT NOT NULL DEFAULT '',
    column_name TEXT NOT NULL DEFAULT '',
    tag_key TEXT NOT NULL REFERENCES tag_definitions(tag_key),
    tag_value TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'manual',
    orphaned INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (catalog, schema_name, table_name, column_name, tag_key)
);
CREATE INDEX IF NOT EXISTS idx_object_tags_obj ON object_tags(catalog, schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_object_tags_key ON object_tags(tag_key, tag_value);

CREATE TABLE IF NOT EXISTS masking_policies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    tag_key TEXT NOT NULL REFERENCES tag_definitions(tag_key),
    tag_value TEXT,
    mask_type TEXT NOT NULL CHECK (mask_type IN
        ('redact','hash','partial','email','null','generalize','custom')),
    mask_expr TEXT,
    applies_to_types TEXT,
    except_roles TEXT NOT NULL DEFAULT '["admin"]',
    except_users TEXT NOT NULL DEFAULT '[]',
    except_groups TEXT NOT NULL DEFAULT '[]',
    priority INTEGER NOT NULL DEFAULT 100,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS row_policies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    tag_key TEXT NOT NULL REFERENCES tag_definitions(tag_key),
    tag_value TEXT,
    filter_column TEXT NOT NULL,
    filter_mode TEXT NOT NULL CHECK (filter_mode IN ('owner','attribute','custom')),
    attribute_key TEXT,
    filter_expr TEXT,
    except_roles TEXT NOT NULL DEFAULT '["admin"]',
    except_users TEXT NOT NULL DEFAULT '[]',
    except_groups TEXT NOT NULL DEFAULT '[]',
    priority INTEGER NOT NULL DEFAULT 100,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS principal_attributes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user','role','group')),
    principal_value TEXT NOT NULL,
    attribute_key TEXT NOT NULL,
    attribute_value TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (principal_type, principal_value, attribute_key, attribute_value)
);
CREATE INDEX IF NOT EXISTS idx_principal_attributes ON principal_attributes(attribute_key, principal_type, principal_value);

CREATE TABLE IF NOT EXISTS governance_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    object TEXT,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_gov_audit_ts ON governance_audit(ts);
"""

# Well-known definitions seeded once so a fresh install can tag immediately.
SEED_TAG_DEFINITIONS = [
    ("pii", "Personally identifiable information. The value says what kind.",
     ["email", "phone", "ssn", "name", "address", "dob", "ip", "financial"]),
    ("sensitivity", "Business sensitivity classification.",
     ["public", "internal", "confidential", "restricted", "unclassified"]),
]


def utcnow() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_db() -> sqlite3.Connection:
    os.makedirs(METADATA_DIR, exist_ok=True)
    conn = sqlite3.connect(GOV_DB_PATH, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    """Additive upgrades of databases created by earlier versions (group support: `except_groups` on both policy tables, and the
    principal_attributes CHECK that now admits 'group', which SQLite can only change by rebuilding the table)."""
    for table in ("masking_policies", "row_policies"):
        cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        if "except_groups" not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN except_groups TEXT NOT NULL DEFAULT '[]'")
    row = conn.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'principal_attributes'").fetchone()
    if row and "'group'" not in (row[0] or ""):
        conn.executescript("""
            BEGIN;
            ALTER TABLE principal_attributes RENAME TO principal_attributes_old;
            CREATE TABLE principal_attributes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                principal_type TEXT NOT NULL CHECK (principal_type IN ('user','role','group')),
                principal_value TEXT NOT NULL,
                attribute_key TEXT NOT NULL,
                attribute_value TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (principal_type, principal_value, attribute_key, attribute_value)
            );
            INSERT INTO principal_attributes (id, principal_type, principal_value, attribute_key, attribute_value, created_by, created_at)
                SELECT id, principal_type, principal_value, attribute_key, attribute_value, created_by, created_at FROM principal_attributes_old;
            DROP TABLE principal_attributes_old;
            CREATE INDEX IF NOT EXISTS idx_principal_attributes ON principal_attributes(attribute_key, principal_type, principal_value);
            COMMIT;
        """)


def init_governance_db() -> None:
    """Creates the schema, the version counter and the seed tag definitions (once). Idempotent."""
    conn = get_db()
    try:
        conn.executescript(_SCHEMA)
        _migrate(conn)
        conn.execute("INSERT OR IGNORE INTO governance_meta (key, value) VALUES ('version', '1')")
        seeded = conn.execute("SELECT value FROM governance_meta WHERE key = 'seeded'").fetchone()
        if not seeded:
            now = utcnow()
            for key, description, values in SEED_TAG_DEFINITIONS:
                conn.execute(
                    "INSERT OR IGNORE INTO tag_definitions (tag_key, description, allowed_values, created_by, created_at) "
                    "VALUES (?, ?, ?, 'system', ?)", (key, description, json.dumps(values), now))
            conn.execute("INSERT INTO governance_meta (key, value) VALUES ('seeded', '1')")
        conn.commit()
    finally:
        conn.close()


_tls = threading.local()


def _reader() -> sqlite3.Connection:
    """A per-thread, per-path read connection: opening SQLite (plus PRAGMAs) on every version check cost ~0.4 ms."""
    conn = getattr(_tls, "conn", None)
    if conn is None or getattr(_tls, "path", None) != GOV_DB_PATH:
        os.makedirs(METADATA_DIR, exist_ok=True)
        conn = sqlite3.connect(GOV_DB_PATH, timeout=15.0)
        conn.row_factory = sqlite3.Row
        _tls.conn, _tls.path = conn, GOV_DB_PATH
    return conn


def get_version() -> int:
    """Cheap point read used to validate in-process caches."""
    try:
        row = _reader().execute("SELECT value FROM governance_meta WHERE key = 'version'").fetchone()
    except sqlite3.OperationalError:
        # database not initialised yet (or replaced): initialise and retry once
        _tls.conn = None
        init_governance_db()
        row = _reader().execute("SELECT value FROM governance_meta WHERE key = 'version'").fetchone()
    return int(row["value"]) if row else 0


def bump_version(conn: sqlite3.Connection) -> None:
    """Call inside the writing transaction so readers never see new data with an old version number."""
    conn.execute("UPDATE governance_meta SET value = CAST(value AS INTEGER) + 1 WHERE key = 'version'")


def write_audit(conn: sqlite3.Connection, actor: str, action: str, obj: Optional[str] = None,
                detail: Optional[Dict[str, Any]] = None) -> None:
    conn.execute(
        "INSERT INTO governance_audit (ts, actor, action, object, detail) VALUES (?, ?, ?, ?, ?)",
        (utcnow(), actor or "unknown", action, obj, json.dumps(detail or {}, default=str)))


def list_audit(since: Optional[str] = None, actor: Optional[str] = None, action: Optional[str] = None,
               limit: int = 200) -> list:
    clauses, params = [], []
    if since:
        clauses.append("ts >= ?")
        params.append(since)
    if actor:
        clauses.append("actor = ?")
        params.append(actor)
    if action:
        clauses.append("action = ?")
        params.append(action)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    conn = get_db()
    try:
        rows = conn.execute(
            f"SELECT * FROM governance_audit {where} ORDER BY id DESC LIMIT ?", (*params, min(max(limit, 1), 1000))
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["detail"] = json.loads(d["detail"] or "{}")
            except ValueError:
                pass
            out.append(d)
        return out
    finally:
        conn.close()
