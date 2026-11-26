import os
import time
import math
import uuid
import json
import sqlite3
import datetime
import logging
from typing import Dict, Any, List, Optional, Union, Tuple

logger = logging.getLogger("localspark.serving")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
if not os.path.exists(WAREHOUSE_DIR):
    local_alt = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "warehouse"))
    if os.path.exists(local_alt):
        WAREHOUSE_DIR = local_alt

METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
DB_PATH = os.path.join(METADATA_DIR, "experiments.db")


def get_db() -> sqlite3.Connection:
    os.makedirs(METADATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_serving_db():
    """Initializes tables for MLflow registered models, model versions, model aliases, and serving endpoints."""
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS registered_models (
                name TEXT PRIMARY KEY,
                catalog_name TEXT NOT NULL DEFAULT 'warehouse',
                schema_name TEXT NOT NULL DEFAULT 'dbo',
                description TEXT DEFAULT '',
                user_id TEXT DEFAULT 'admin',
                tags TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
        """)

        # Migrations for existing DB instances
        try:
            conn.execute("ALTER TABLE registered_models ADD COLUMN catalog_name TEXT NOT NULL DEFAULT 'warehouse';")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE registered_models ADD COLUMN schema_name TEXT NOT NULL DEFAULT 'dbo';")
        except Exception:
            pass

        conn.execute("UPDATE registered_models SET catalog_name = 'warehouse' WHERE catalog_name IS NULL OR catalog_name = '';")
        conn.execute("UPDATE registered_models SET schema_name = 'dbo' WHERE schema_name IS NULL OR schema_name = '';")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS model_versions (
                name TEXT NOT NULL,
                version INTEGER NOT NULL,
                run_id TEXT,
                current_stage TEXT NOT NULL DEFAULT 'None',
                source TEXT DEFAULT '',
                status TEXT DEFAULT 'READY',
                flavor TEXT DEFAULT 'python_function',
                algorithm TEXT DEFAULT 'xgboost',
                description TEXT DEFAULT '',
                metrics TEXT DEFAULT '{}',
                signature TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (name, version),
                FOREIGN KEY (name) REFERENCES registered_models(name) ON DELETE CASCADE
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mv_stage ON model_versions(name, current_stage);")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS model_aliases (
                model_name TEXT NOT NULL,
                alias TEXT NOT NULL,
                version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (model_name, alias),
                FOREIGN KEY (model_name) REFERENCES registered_models(name) ON DELETE CASCADE
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_model_aliases ON model_aliases(model_name, version);")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS serving_endpoints (
                endpoint_id TEXT PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                model_name TEXT NOT NULL,
                version INTEGER DEFAULT NULL,
                stage TEXT DEFAULT 'Production',
                state TEXT DEFAULT 'READY',
                request_count INTEGER DEFAULT 0,
                last_served_at TEXT DEFAULT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (model_name) REFERENCES registered_models(name) ON DELETE CASCADE
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ep_model ON serving_endpoints(model_name);")

        # Check if seed models exist; if not, seed them
        cursor = conn.execute("SELECT COUNT(*) FROM registered_models;")
        if cursor.fetchone()[0] == 0:
            seed_default_models(conn)
        else:
            # Check if aliases need default seeding
            cur_a = conn.execute("SELECT COUNT(*) FROM model_aliases;")
            if cur_a.fetchone()[0] == 0:
                now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                conn.execute("INSERT OR REPLACE INTO model_aliases (model_name, alias, version, created_at, updated_at) VALUES ('employee_turnover_predictor', 'champion', 2, ?, ?);", (now_str, now_str))
                conn.execute("INSERT OR REPLACE INTO model_aliases (model_name, alias, version, created_at, updated_at) VALUES ('employee_turnover_predictor', 'challenger', 1, ?, ?);", (now_str, now_str))
                conn.execute("INSERT OR REPLACE INTO model_aliases (model_name, alias, version, created_at, updated_at) VALUES ('equipment_failure_forecaster', 'champion', 1, ?, ?);", (now_str, now_str))


def seed_default_models(conn: sqlite3.Connection):
    """Seeds production-grade demonstration models and endpoints into MLflow registry."""
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 1. employee_turnover_predictor
    conn.execute(
        "INSERT INTO registered_models (name, description, user_id, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?);",
        (
            "employee_turnover_predictor",
            "Predicts probability of employee attrition and voluntary turnover risk based on tenure, salary, and satisfaction telemetry.",
            "admin",
            json.dumps({"domain": "people_analytics", "framework": "xgboost", "task": "binary_classification"}),
            now_str,
            now_str
        )
    )

    turnover_sig = json.dumps({
        "inputs": [
            {"name": "tenure_years", "type": "double", "example": 2.5},
            {"name": "salary", "type": "long", "example": 92000},
            {"name": "satisfaction_score", "type": "double", "example": 0.42},
            {"name": "overtime_hours", "type": "double", "example": 14.5}
        ],
        "outputs": [
            {"name": "turnover_probability", "type": "double"},
            {"name": "risk_tier", "type": "string"},
            {"name": "recommendation", "type": "string"}
        ]
    })

    conn.execute("""
        INSERT INTO model_versions (name, version, run_id, current_stage, source, status, flavor, algorithm, description, metrics, signature, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        "employee_turnover_predictor",
        1,
        "aca979a9b4d140948cac6dd6400ca781",
        "Staging",
        "/workspace/warehouse/mlflow/artifacts/98763edf/aca979a9b4d140948cac6dd6400ca781/artifacts/model",
        "READY",
        "python_function",
        "RandomForestClassifier",
        "Initial tree ensemble benchmark on historical personnel records",
        json.dumps({"accuracy": 0.884, "auc_roc": 0.912, "f1_score": 0.865}),
        turnover_sig,
        now_str,
        now_str
    ))

    conn.execute("""
        INSERT INTO model_versions (name, version, run_id, current_stage, source, status, flavor, algorithm, description, metrics, signature, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        "employee_turnover_predictor",
        2,
        "eae654bf2d284fe19a6489d86f775b4f",
        "Production",
        "/workspace/warehouse/mlflow/artifacts/98763edf/eae654bf2d284fe19a6489d86f775b4f/artifacts/model",
        "READY",
        "python_function",
        "XGBClassifier",
        "Fine-tuned gradient boosting model with early stopping and balanced class weighting",
        json.dumps({"accuracy": 0.946, "auc_roc": 0.968, "f1_score": 0.938}),
        turnover_sig,
        now_str,
        now_str
    ))

    conn.execute("""
        INSERT INTO serving_endpoints (endpoint_id, name, model_name, version, stage, state, request_count, last_served_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        "ep_turnover_prod",
        "employee-turnover-predictor-prod",
        "employee_turnover_predictor",
        2,
        "Production",
        "READY",
        148,
        now_str,
        now_str,
        now_str
    ))

    # 2. equipment_failure_forecaster
    conn.execute(
        "INSERT INTO registered_models (name, description, user_id, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?);",
        (
            "equipment_failure_forecaster",
            "Real-time industrial IoT telemetry failure forecasting and predictive maintenance time-to-failure scoring.",
            "admin",
            json.dumps({"domain": "iot_manufacturing", "framework": "lightgbm", "task": "anomaly_detection"}),
            now_str,
            now_str
        )
    )

    sensor_sig = json.dumps({
        "inputs": [
            {"name": "vibration_rms", "type": "double", "example": 4.1},
            {"name": "temperature_c", "type": "double", "example": 82.3},
            {"name": "pressure_psi", "type": "double", "example": 128.5},
            {"name": "operating_hours", "type": "long", "example": 4200}
        ],
        "outputs": [
            {"name": "failure_probability", "type": "double"},
            {"name": "alert_level", "type": "string"},
            {"name": "hours_to_failure", "type": "double"}
        ]
    })

    conn.execute("""
        INSERT INTO model_versions (name, version, run_id, current_stage, source, status, flavor, algorithm, description, metrics, signature, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        "equipment_failure_forecaster",
        1,
        "22b243763b6546deb1c564c01abed94d",
        "Production",
        "/workspace/warehouse/mlflow/artifacts/13c30f05/22b243763b6546deb1c564c01abed94d/artifacts/model",
        "READY",
        "python_function",
        "LGBMClassifier",
        "Anomaly detection classifier on vibration and temperature sensors",
        json.dumps({"accuracy": 0.971, "auc_roc": 0.985, "precision": 0.962}),
        sensor_sig,
        now_str,
        now_str
    ))

    conn.execute("""
        INSERT INTO serving_endpoints (endpoint_id, name, model_name, version, stage, state, request_count, last_served_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        "ep_equipment_prod",
        "equipment-failure-forecaster-prod",
        "equipment_failure_forecaster",
        1,
        "Production",
        "READY",
        892,
        now_str,
        now_str,
        now_str
    ))


def list_registered_models() -> List[Dict[str, Any]]:
    init_serving_db()
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM registered_models ORDER BY updated_at DESC;").fetchall()
        result = []
        for r in rows:
            m = dict(r)
            m["tags"] = json.loads(m["tags"]) if m["tags"] else {}
            m["catalog_name"] = m.get("catalog_name") or "warehouse"
            m["schema_name"] = m.get("schema_name") or "dbo"
            m["full_name"] = f"{m['catalog_name']}.{m['schema_name']}.{m['name']}"

            # Fetch aliases
            alias_rows = conn.execute(
                "SELECT alias, version FROM model_aliases WHERE model_name = ?;",
                (m["name"],)
            ).fetchall()
            aliases_map = {ar["alias"]: ar["version"] for ar in alias_rows}
            m["aliases"] = aliases_map
            m["champion_version"] = aliases_map.get("champion")
            m["challenger_version"] = aliases_map.get("challenger")

            # Fetch versions
            v_rows = conn.execute(
                "SELECT * FROM model_versions WHERE name = ? ORDER BY version DESC;",
                (m["name"],)
            ).fetchall()
            versions = []
            latest_stage_version = {}
            for vr in v_rows:
                v_dict = dict(vr)
                v_dict["metrics"] = json.loads(v_dict["metrics"]) if v_dict["metrics"] else {}
                v_dict["signature"] = json.loads(v_dict["signature"]) if v_dict["signature"] else {}
                # Attach aliases for this specific version
                v_dict["aliases"] = [a for a, ver in aliases_map.items() if ver == v_dict["version"]]
                versions.append(v_dict)
                st = v_dict["current_stage"]
                if st not in latest_stage_version:
                    latest_stage_version[st] = v_dict["version"]

            m["latest_versions"] = versions
            m["version_count"] = len(versions)
            m["production_version"] = latest_stage_version.get("Production") or aliases_map.get("champion")
            m["staging_version"] = latest_stage_version.get("Staging") or aliases_map.get("challenger")

            # Fetch serving endpoint
            ep_row = conn.execute(
                "SELECT * FROM serving_endpoints WHERE model_name = ? LIMIT 1;",
                (m["name"],)
            ).fetchone()
            m["serving_endpoint"] = dict(ep_row) if ep_row else None

            result.append(m)
        return result


def parse_model_namespace(
    model_spec: str,
    default_catalog: Optional[str] = "warehouse",
    default_schema: Optional[str] = "dbo"
) -> Tuple[str, str, str, Optional[str]]:
    """
    Parses Unity Catalog 3-level or 2-level model specifications, with optional @alias or @version suffix.
    Returns (catalog_name, schema_name, clean_model_name, alias_or_version).

    Examples:
    - 'marketing.analytics.churn_risk@champion' -> ('marketing', 'analytics', 'churn_risk', 'champion')
    - 'marketing.analytics.churn_risk' -> ('marketing', 'analytics', 'churn_risk', None)
    - 'analytics.churn_risk' -> ('warehouse', 'analytics', 'churn_risk', None)
    - 'churn_risk' -> ('warehouse', 'dbo', 'churn_risk', None)
    """
    raw = (model_spec or "").strip()
    alias_part = None
    if "@" in raw:
        base_part, alias_part = raw.split("@", 1)
        base_part = base_part.strip()
        alias_part = alias_part.strip()
    else:
        base_part = raw

    catalog_name = (default_catalog or "warehouse").strip()
    schema_name = (default_schema or "dbo").strip()
    clean_name = base_part

    if "." in base_part:
        parts = [p.strip() for p in base_part.split(".") if p.strip()]
        if len(parts) >= 3:
            catalog_name, schema_name, clean_name = parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            schema_name, clean_name = parts[0], parts[1]
        elif len(parts) == 1:
            clean_name = parts[0]

    clean_name = clean_name.replace(" ", "_")
    return catalog_name, schema_name, clean_name, alias_part


def get_registered_model(name: str) -> Optional[Dict[str, Any]]:
    init_serving_db()
    cat, sch, clean_name, _ = parse_model_namespace(name)

    with get_db() as conn:
        row = None
        if "." in (name or ""):
            row = conn.execute(
                "SELECT * FROM registered_models WHERE name = ? AND catalog_name = ? AND schema_name = ?;",
                (clean_name, cat, sch)
            ).fetchone()

        if not row:
            row = conn.execute("SELECT * FROM registered_models WHERE name = ?;", (clean_name,)).fetchone()

        if not row:
            return None

        m = dict(row)
        m["tags"] = json.loads(m["tags"]) if m["tags"] else {}
        m["catalog_name"] = m.get("catalog_name") or "warehouse"
        m["schema_name"] = m.get("schema_name") or "dbo"
        m["full_name"] = f"{m['catalog_name']}.{m['schema_name']}.{m['name']}"

        # Fetch aliases
        alias_rows = conn.execute(
            "SELECT alias, version FROM model_aliases WHERE model_name = ?;",
            (m["name"],)
        ).fetchall()
        aliases_map = {ar["alias"]: ar["version"] for ar in alias_rows}
        m["aliases"] = aliases_map
        m["champion_version"] = aliases_map.get("champion")
        m["challenger_version"] = aliases_map.get("challenger")

        v_rows = conn.execute(
            "SELECT * FROM model_versions WHERE name = ? ORDER BY version DESC;",
            (m["name"],)
        ).fetchall()
        versions = []
        latest_stage_version = {}
        for vr in v_rows:
            v_dict = dict(vr)
            v_dict["metrics"] = json.loads(v_dict["metrics"]) if v_dict["metrics"] else {}
            v_dict["signature"] = json.loads(v_dict["signature"]) if v_dict["signature"] else {}
            v_dict["aliases"] = [a for a, ver in aliases_map.items() if ver == v_dict["version"]]
            versions.append(v_dict)
            st = v_dict["current_stage"]
            if st not in latest_stage_version:
                latest_stage_version[st] = v_dict["version"]

        m["latest_versions"] = versions
        m["version_count"] = len(versions)
        m["production_version"] = latest_stage_version.get("Production") or aliases_map.get("champion")
        m["staging_version"] = latest_stage_version.get("Staging") or aliases_map.get("challenger")

        ep_row = conn.execute("SELECT * FROM serving_endpoints WHERE model_name = ?;", (m["name"],)).fetchone()
        m["serving_endpoint"] = dict(ep_row) if ep_row else None
        return m


def create_registered_model(
    name: str,
    catalog_name: Optional[str] = "warehouse",
    schema_name: Optional[str] = "dbo",
    description: str = "",
    tags: Optional[Dict] = None
) -> Dict[str, Any]:
    init_serving_db()
    cat, sch, clean_name, _ = parse_model_namespace(name, default_catalog=catalog_name, default_schema=schema_name)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        existing = conn.execute("SELECT name FROM registered_models WHERE name = ?;", (clean_name,)).fetchone()
        if existing:
            conn.execute("""
                UPDATE registered_models
                SET catalog_name = ?,
                    schema_name = ?,
                    description = CASE WHEN ? != '' THEN ? ELSE description END,
                    tags = CASE WHEN ? != '{}' THEN ? ELSE tags END,
                    updated_at = ?
                WHERE name = ?;
            """, (cat, sch, description.strip(), description.strip(), json.dumps(tags or {}), json.dumps(tags or {}), now_str, clean_name))
        else:
            conn.execute("""
                INSERT INTO registered_models (name, catalog_name, schema_name, description, user_id, tags, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                clean_name,
                cat,
                sch,
                description.strip(),
                "admin",
                json.dumps(tags or {}),
                now_str,
                now_str
            ))
    return get_registered_model(clean_name)


# =========================================================================
# Unity Catalog Model Aliases & 3-Level Name Resolution
# =========================================================================

def set_model_alias(model_name: str, alias: str, version: int) -> Dict[str, Any]:
    """Sets or moves an alias (e.g. champion, challenger) to a specific version of a model."""
    init_serving_db()
    _, _, clean_name, _ = parse_model_namespace(model_name)
    clean_alias = alias.strip().lstrip("@").lower()
    if not clean_alias:
        raise ValueError("Alias cannot be empty.")
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        v_exists = conn.execute(
            "SELECT 1 FROM model_versions WHERE name = ? AND version = ?;",
            (clean_name, version)
        ).fetchone()
        if not v_exists:
            raise ValueError(f"Version {version} does not exist for model '{clean_name}'.")

        conn.execute("""
            INSERT OR REPLACE INTO model_aliases (model_name, alias, version, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?);
        """, (clean_name, clean_alias, version, now_str, now_str))
        logger.info(f"Set model alias '{clean_alias}' -> version {version} for model '{clean_name}'")
    return get_registered_model(clean_name)


def delete_model_alias(model_name: str, alias: str) -> bool:
    """Deletes an alias from a model."""
    init_serving_db()
    _, _, clean_name, _ = parse_model_namespace(model_name)
    clean_alias = alias.strip().lstrip("@").lower()
    with get_db() as conn:
        c = conn.execute("DELETE FROM model_aliases WHERE model_name = ? AND alias = ?;", (clean_name, clean_alias))
        return c.rowcount > 0


def get_model_aliases(model_name: str) -> Dict[str, int]:
    """Returns a dictionary of all aliases for a model mapping to version integers."""
    init_serving_db()
    _, _, clean_name, _ = parse_model_namespace(model_name)
    with get_db() as conn:
        rows = conn.execute("SELECT alias, version FROM model_aliases WHERE model_name = ?;", (clean_name,)).fetchall()
        return {r["alias"]: r["version"] for r in rows}


def resolve_model_and_version(
    model_spec: str,
    requested_version: Optional[int] = None
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Resolves Unity Catalog 3-level model specifiers and model aliases:
    Supported formats:
    - 'employee_turnover_predictor' (defaults to @champion or Production stage)
    - 'employee_turnover_predictor@champion' (resolves alias)
    - 'employee_turnover_predictor@challenger'
    - 'employee_turnover_predictor@v2' or 'employee_turnover_predictor@2'
    - 'warehouse.dbo.employee_turnover_predictor'
    - 'warehouse.dbo.employee_turnover_predictor@champion'
    - 'localspark.models.equipment_failure_forecaster@champion'
    """
    raw = model_spec.strip()
    target_alias = None
    target_version = requested_version

    # Check for @alias or @version suffix
    if "@" in raw:
        base_name, alias_part = raw.split("@", 1)
        base_name = base_name.strip()
        alias_part = alias_part.strip().lstrip("v")
        if alias_part.isdigit():
            target_version = int(alias_part)
        else:
            target_alias = alias_part.lower()
    else:
        base_name = raw

    # Check for 3-level or 2-level namespace: catalog.schema.model
    catalog_filter = None
    schema_filter = None
    if "." in base_name:
        parts = base_name.split(".")
        if len(parts) == 3:
            catalog_filter, schema_filter, model_name = parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            schema_filter, model_name = parts[0], parts[1]
        else:
            model_name = parts[-1]
    else:
        model_name = base_name

    model = get_registered_model(model_name)
    if not model:
        models = list_registered_models()
        for m in models:
            if m["name"].lower() == model_name.lower():
                model = m
                break
            if catalog_filter and schema_filter:
                if (m.get("catalog_name") == catalog_filter and
                    m.get("schema_name") == schema_filter and
                    m["name"].lower() == model_name.lower()):
                    model = m
                    break
    if not model:
        return None, None

    # Resolve version
    selected_version = None
    if target_alias:
        aliases = model.get("aliases", {})
        if target_alias in aliases:
            selected_version = get_model_version(model["name"], aliases[target_alias])
        else:
            for v in model.get("latest_versions", []):
                if v["current_stage"].lower() == target_alias:
                    selected_version = v
                    break
    elif target_version is not None:
        selected_version = get_model_version(model["name"], target_version)
    else:
        # Priority: champion alias -> production version -> latest version
        aliases = model.get("aliases", {})
        if "champion" in aliases:
            selected_version = get_model_version(model["name"], aliases["champion"])
        elif model.get("production_version"):
            selected_version = get_model_version(model["name"], model["production_version"])
        elif model.get("latest_versions"):
            selected_version = model["latest_versions"][0]

    return model, selected_version


def delete_registered_model(name: str) -> bool:
    init_serving_db()
    _, _, clean_name, _ = parse_model_namespace(name)
    with get_db() as conn:
        c = conn.execute("DELETE FROM registered_models WHERE name = ?;", (clean_name,))
        return c.rowcount > 0


def create_model_version(
    name: str,
    run_id: Optional[str] = None,
    stage: str = "None",
    algorithm: str = "custom",
    metrics: Optional[Dict] = None,
    signature: Optional[Dict] = None,
    description: str = "",
    source: str = ""
) -> Dict[str, Any]:
    init_serving_db()
    cat, sch, clean_name, _ = parse_model_namespace(name)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        # Auto-create registered model in Unity Catalog if it does not already exist
        reg = conn.execute("SELECT name FROM registered_models WHERE name = ?;", (clean_name,)).fetchone()
        if not reg:
            conn.execute("""
                INSERT INTO registered_models (name, catalog_name, schema_name, description, user_id, tags, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                clean_name,
                cat,
                sch,
                description or f"Registered model {cat}.{sch}.{clean_name}",
                "admin",
                "{}",
                now_str,
                now_str
            ))

        # Determine next version
        curr_max = conn.execute(
            "SELECT COALESCE(MAX(version), 0) FROM model_versions WHERE name = ?;",
            (clean_name,)
        ).fetchone()[0]
        new_version = curr_max + 1

        conn.execute("""
            INSERT INTO model_versions (
                name, version, run_id, current_stage, source, status,
                flavor, algorithm, description, metrics, signature, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            clean_name,
            new_version,
            run_id,
            stage,
            source or f"/workspace/warehouse/mlflow/models/{clean_name}/v{new_version}",
            "READY",
            "python_function",
            algorithm,
            description,
            json.dumps(metrics or {}),
            json.dumps(signature or {}),
            now_str,
            now_str
        ))

        # If stage is Production or Staging, update endpoint if configured
        conn.execute("UPDATE registered_models SET updated_at = ? WHERE name = ?;", (now_str, clean_name))

    return get_model_version(clean_name, new_version)


def get_model_version(name: str, version: int) -> Optional[Dict[str, Any]]:
    init_serving_db()
    _, _, clean_name, _ = parse_model_namespace(name)
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM model_versions WHERE name = ? AND version = ?;",
            (clean_name, version)
        ).fetchone()
        if not row:
            return None
        v = dict(row)
        v["metrics"] = json.loads(v["metrics"]) if v["metrics"] else {}
        v["signature"] = json.loads(v["signature"]) if v["signature"] else {}
        return v


def transition_model_version_stage(
    name: str,
    version: int,
    stage: str,
    archive_existing_versions: bool = True
) -> Dict[str, Any]:
    """Promotes or transitions a model version to a stage ('Production', 'Staging', 'Archived', 'None')."""
    init_serving_db()
    _, _, clean_name, _ = parse_model_namespace(name)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        if archive_existing_versions and stage in ["Production", "Staging"]:
            conn.execute(
                "UPDATE model_versions SET current_stage = 'Archived', updated_at = ? WHERE name = ? AND current_stage = ? AND version != ?;",
                (now_str, clean_name, stage, version)
            )

        conn.execute(
            "UPDATE model_versions SET current_stage = ?, updated_at = ? WHERE name = ? AND version = ?;",
            (stage, now_str, clean_name, version)
        )
        conn.execute("UPDATE registered_models SET updated_at = ? WHERE name = ?;", (now_str, clean_name))

        # Also update any serving endpoint attached to this model and stage
        if stage == "Production":
            conn.execute(
                "UPDATE serving_endpoints SET version = ?, updated_at = ? WHERE model_name = ? AND stage = 'Production';",
                (version, now_str, clean_name)
            )

    return get_model_version(clean_name, version)


def delete_model_version(name: str, version: int) -> bool:
    init_serving_db()
    _, _, clean_name, _ = parse_model_namespace(name)
    with get_db() as conn:
        c = conn.execute("DELETE FROM model_versions WHERE name = ? AND version = ?;", (clean_name, version))
        return c.rowcount > 0


def list_serving_endpoints() -> List[Dict[str, Any]]:
    init_serving_db()
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM serving_endpoints ORDER BY updated_at DESC;").fetchall()
        return [dict(r) for r in rows]


def get_serving_endpoint(endpoint_id_or_name: str) -> Optional[Dict[str, Any]]:
    init_serving_db()
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM serving_endpoints WHERE endpoint_id = ? OR name = ? OR model_name = ?;",
            (endpoint_id_or_name, endpoint_id_or_name, endpoint_id_or_name)
        ).fetchone()
        return dict(row) if row else None


def create_or_update_serving_endpoint(
    model_name: str,
    stage: str = "Production",
    version: Optional[int] = None,
    state: str = "READY",
    endpoint_name: Optional[str] = None
) -> Dict[str, Any]:
    init_serving_db()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ep_name = endpoint_name or f"{model_name.replace('_', '-')}-service"
    ep_id = f"ep_{uuid.uuid4().hex[:8]}"

    with get_db() as conn:
        existing = conn.execute(
            "SELECT * FROM serving_endpoints WHERE model_name = ?;",
            (model_name,)
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE serving_endpoints
                SET stage = ?, version = ?, state = ?, updated_at = ?
                WHERE endpoint_id = ?;
            """, (stage, version, state, now_str, existing["endpoint_id"]))
            ep_id = existing["endpoint_id"]
        else:
            conn.execute("""
                INSERT INTO serving_endpoints (
                    endpoint_id, name, model_name, version, stage, state, request_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?);
            """, (ep_id, ep_name, model_name, version, stage, state, now_str, now_str))

    return get_serving_endpoint(ep_id)


def toggle_serving_endpoint_state(endpoint_id: str, new_state: str) -> Optional[Dict[str, Any]]:
    init_serving_db()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        conn.execute(
            "UPDATE serving_endpoints SET state = ?, updated_at = ? WHERE endpoint_id = ?;",
            (new_state, now_str, endpoint_id)
        )
    return get_serving_endpoint(endpoint_id)


# In-memory LRU model artifact cache for ultra-fast SQL vectorized inference
_LOADED_MODEL_CACHE: Dict[str, Tuple[float, Any]] = {}


def _find_model_artifact_path(version_info: Dict[str, Any], model_name: str) -> Optional[str]:
    """Resolves the physical on-disk path to the model artifact (.pkl or .joblib)."""
    source = (version_info.get("source") or "").strip()
    run_id = (version_info.get("run_id") or "").strip()

    candidate_paths = []

    # 1. Resolve runs:/ URI via MLflow experiment artifact directory
    if source.startswith("runs:/"):
        parts = source[len("runs:/"):].split("/", 1)
        r_id = parts[0]
        subpath = parts[1] if len(parts) > 1 else "model"
        try:
            from web import experiments as exp_mod
            for fn in ["model.pkl", "model.joblib", "model"]:
                target_sub = os.path.join(subpath, fn) if not subpath.endswith(fn) else subpath
                p = exp_mod.mlflow_get_artifact_path(r_id, target_sub)
                if p and os.path.exists(p):
                    candidate_paths.append(p)
                p2 = exp_mod.mlflow_get_artifact_path(r_id, fn)
                if p2 and os.path.exists(p2):
                    candidate_paths.append(p2)
        except Exception:
            pass

    # 2. If run_id exists, check experiments artifact store directly
    if run_id:
        try:
            from web import experiments as exp_mod
            for candidate in [
                "model/model.pkl",
                "model/model.joblib",
                "model.pkl",
                "model.joblib"
            ]:
                p = exp_mod.mlflow_get_artifact_path(run_id, candidate)
                if p and os.path.exists(p):
                    candidate_paths.append(p)
        except Exception:
            pass

    # 3. Direct filesystem path resolution (handling container /workspace/warehouse mapping)
    if source and not source.startswith("runs:/"):
        resolved_source = source
        if not os.path.exists(resolved_source) and resolved_source.startswith("/workspace/warehouse"):
            resolved_source = resolved_source.replace("/workspace/warehouse", WAREHOUSE_DIR)

        if os.path.exists(resolved_source):
            if os.path.isdir(resolved_source):
                for fn in ["model.pkl", "model.joblib"]:
                    fp = os.path.join(resolved_source, fn)
                    if os.path.exists(fp):
                        candidate_paths.append(fp)
            elif os.path.isfile(resolved_source):
                candidate_paths.append(resolved_source)

    # 4. Standard warehouse models directory
    ver = version_info.get("version", 1)
    std_paths = [
        os.path.join(WAREHOUSE_DIR, "mlflow", "models", model_name, f"v{ver}", "model.pkl"),
        os.path.join(WAREHOUSE_DIR, "mlflow", "models", model_name, "model.pkl"),
        os.path.join(WAREHOUSE_DIR, "mlflow", "models", model_name, f"v{ver}", "model.joblib"),
    ]
    for sp in std_paths:
        if os.path.exists(sp):
            candidate_paths.append(sp)

    for p in candidate_paths:
        if os.path.isfile(p):
            return os.path.abspath(p)
        if os.path.isdir(p):
            for fn in ["model.pkl", "model.joblib"]:
                fp = os.path.join(p, fn)
                if os.path.isfile(fp):
                    return os.path.abspath(fp)

    return None


def _load_model_artifact(artifact_path: str, model_name: str, version: int) -> Optional[Any]:
    """Loads and caches trained model object using joblib."""
    global _LOADED_MODEL_CACHE
    try:
        mtime = os.path.getmtime(artifact_path)
        cache_key = f"{model_name}:{version}:{artifact_path}"
        if cache_key in _LOADED_MODEL_CACHE:
            cached_mtime, cached_obj = _LOADED_MODEL_CACHE[cache_key]
            if cached_mtime == mtime:
                return cached_obj

        import joblib
        model_obj = joblib.load(artifact_path)

        # LRU eviction if cache grows beyond 32 models
        if len(_LOADED_MODEL_CACHE) >= 32:
            oldest_key = next(iter(_LOADED_MODEL_CACHE))
            del _LOADED_MODEL_CACHE[oldest_key]

        _LOADED_MODEL_CACHE[cache_key] = (mtime, model_obj)
        return model_obj
    except Exception as e:
        logger.warning(f"Failed to load model artifact at '{artifact_path}': {e}")
        return None


def _score_with_trained_model(
    loaded_model: Any,
    records: List[Dict[str, Any]],
    selected_version: Dict[str, Any],
    clean_model_name: str
) -> Optional[List[Dict[str, Any]]]:
    """
    Executes native inference using a loaded Scikit-Learn, XGBoost, or PyFunc model artifact.
    Aligns record features against model signature or feature_names_in_.
    """
    if not records:
        return []

    try:
        import numpy as np
        import pandas as pd

        # Determine expected feature column order
        cols = None
        if hasattr(loaded_model, "feature_names_in_"):
            cols = [str(c) for c in loaded_model.feature_names_in_]
        elif hasattr(loaded_model, "get_booster") and hasattr(loaded_model.get_booster(), "feature_names"):
            booster_cols = loaded_model.get_booster().feature_names
            if booster_cols:
                cols = [str(c) for c in booster_cols]
        elif selected_version.get("signature") and isinstance(selected_version["signature"], dict):
            sig_inputs = selected_version["signature"].get("inputs", [])
            if sig_inputs and isinstance(sig_inputs, list):
                sig_cols = [inp["name"] for inp in sig_inputs if isinstance(inp, dict) and "name" in inp]
                if sig_cols:
                    cols = sig_cols

        # Convert records to DataFrame with aligned columns
        if cols:
            aligned_rows = []
            for r in records:
                aligned_rows.append({c: r.get(c, 0.0) for c in cols})
            df = pd.DataFrame(aligned_rows, columns=cols)
            for c in cols:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df = pd.DataFrame(records)
            for c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # Execute predictions
        try:
            preds = loaded_model.predict(df)
        except Exception:
            try:
                preds = loaded_model.predict(df.values)
            except Exception as e_pred:
                logger.warning(f"Inference error with DataFrame and numpy values: {e_pred}")
                return None

        # Execute probabilities if classification model
        probs = None
        if hasattr(loaded_model, "predict_proba"):
            try:
                raw_probs = loaded_model.predict_proba(df)
            except Exception:
                try:
                    raw_probs = loaded_model.predict_proba(df.values)
                except Exception:
                    raw_probs = None

            if raw_probs is not None:
                try:
                    arr = np.asarray(raw_probs)
                    if len(arr.shape) == 2:
                        if arr.shape[1] == 2:
                            probs = arr[:, 1]
                        else:
                            probs = np.max(arr, axis=1)
                    elif len(arr.shape) == 1:
                        probs = arr
                except Exception:
                    pass

        elif hasattr(loaded_model, "decision_function"):
            try:
                raw_df = loaded_model.decision_function(df)
                arr = np.asarray(raw_df)
                if len(arr.shape) == 1 or (len(arr.shape) == 2 and arr.shape[1] == 1):
                    flat = arr.flatten()
                    probs = 1.0 / (1.0 + np.exp(-np.clip(flat, -15.0, 15.0)))
            except Exception:
                pass

        results: List[Dict[str, Any]] = []
        for idx, rec in enumerate(records):
            p_val = preds[idx]
            if hasattr(p_val, "item"):
                p_val = p_val.item()

            prob_val = None
            if probs is not None and idx < len(probs):
                pv = probs[idx]
                if hasattr(pv, "item"):
                    pv = pv.item()
                prob_val = round(float(pv), 4)

            tier = None
            alert = None
            if prob_val is not None and 0.0 <= prob_val <= 1.0:
                if prob_val >= 0.70:
                    tier = "HIGH"
                    alert = "CRITICAL"
                elif prob_val >= 0.35:
                    tier = "MEDIUM"
                    alert = "WARNING"
                else:
                    tier = "LOW"
                    alert = "NORMAL"

            results.append({
                "record_index": idx,
                "prediction": p_val,
                "class": p_val,
                "label": str(p_val),
                "score": prob_val if prob_val is not None else (round(float(p_val), 4) if isinstance(p_val, (int, float)) else 0.0),
                "probability": prob_val,
                "confidence": round(abs(prob_val - 0.5) * 2, 4) if (prob_val is not None and 0.0 <= prob_val <= 1.0) else 1.0,
                "risk_tier": tier or str(p_val),
                "alert_level": alert or ("WARNING" if str(p_val) in ["1", "true", "True"] else "NORMAL"),
                "recommendation": f"Model prediction: {p_val}" + (f" (confidence {prob_val})" if prob_val is not None else ""),
                "input_features": rec
            })

        return results
    except Exception as e:
        logger.warning(f"Error executing trained model scoring: {e}")
        return None


def score_model(
    model_name: str,
    payload: Dict[str, Any],
    version: Optional[int] = None
) -> Dict[str, Any]:
    """In-process local prediction scoring endpoint supporting MLflow DataFrame split or records format."""
    start_time = time.perf_counter()
    init_serving_db()

    model, selected_version = resolve_model_and_version(model_name, requested_version=version)
    if not model:
        raise ValueError(f"Model '{model_name}' not found in Unity Catalog registry.")
    if not selected_version:
        raise ValueError(f"No active version found for model '{model_name}'.")

    # Use canonized model name for telemetry and feature scoring
    clean_model_name = model["name"]

    # Parse inputs from payload
    records: List[Dict[str, Any]] = []
    if "dataframe_split" in payload:
        ds = payload["dataframe_split"]
        cols = ds.get("columns", [])
        data = ds.get("data", [])
        for row in data:
            records.append(dict(zip(cols, row)))
    elif "dataframe_records" in payload:
        records = payload["dataframe_records"]
    elif "inputs" in payload:
        inputs = payload["inputs"]
        if isinstance(inputs, list):
            if inputs and isinstance(inputs[0], dict):
                records = inputs
            else:
                records = [{"input": x} for x in inputs]
        elif isinstance(inputs, dict):
            records = [inputs]
    elif "records" in payload:
        records = payload["records"]
    else:
        # Check if top-level dict is a single record
        if any(k in payload for k in ["tenure_years", "vibration_rms", "vibration", "salary", "satisfaction_score"]):
            records = [payload]
        else:
            records = [payload]

    predictions: List[Dict[str, Any]] = []

    # 1. Attempt dynamic scoring with trained model artifact (.pkl / .joblib)
    artifact_path = _find_model_artifact_path(selected_version, clean_model_name)
    if artifact_path:
        loaded_model = _load_model_artifact(artifact_path, clean_model_name, selected_version["version"])
        if loaded_model is not None:
            trained_preds = _score_with_trained_model(loaded_model, records, selected_version, clean_model_name)
            if trained_preds is not None and len(trained_preds) == len(records):
                predictions = trained_preds

    # 2. Fallback to domain heuristics if no trained artifact is available or artifact scoring failed
    if not predictions:
        m_name_lower = clean_model_name.lower()
        for idx, rec in enumerate(records):
            if "turnover" in m_name_lower or "churn" in m_name_lower:
                # Employee turnover prediction model
                tenure = float(rec.get("tenure_years", rec.get("tenure", 2.0)))
                salary = float(rec.get("salary", 75000.0))
                satisfaction = float(rec.get("satisfaction_score", rec.get("satisfaction", 0.5)))
                overtime = float(rec.get("overtime_hours", rec.get("overtime", 0.0)))
                
                # Feature logic
                logit = 0.8 + (overtime * 0.09) - (satisfaction * 3.2) - (salary / 100000.0 * 0.6) + (0.5 if tenure < 2.0 else -0.3)
                prob = 1.0 / (1.0 + math.exp(-max(-10.0, min(10.0, logit))))
                tier = "HIGH" if prob >= 0.60 else ("MEDIUM" if prob >= 0.35 else "LOW")
                rec_action = (
                    "Immediate retention intervention: schedule 1-on-1 and review compensation"
                    if tier == "HIGH"
                    else ("Monitor engagement and project allocation" if tier == "MEDIUM" else "Satisfied; standard retention tracking")
                )

                predictions.append({
                    "record_index": idx,
                    "turnover_probability": round(prob, 4),
                    "risk_tier": tier,
                    "confidence": round(abs(prob - 0.5) * 2, 4),
                    "recommendation": rec_action,
                    "input_features": rec
                })

            elif "equipment" in m_name_lower or "failure" in m_name_lower or "sensor" in m_name_lower or "anomaly" in m_name_lower:
                # Industrial equipment predictive maintenance model
                vib = float(rec.get("vibration_rms", rec.get("vibration", 2.0)))
                temp = float(rec.get("temperature_c", rec.get("temperature", 65.0)))
                psi = float(rec.get("pressure_psi", rec.get("pressure", 100.0)))
                hours = float(rec.get("operating_hours", rec.get("hours", 2000.0)))

                stress = (max(0.0, vib - 2.0) * 0.55) + (max(0.0, temp - 70.0) * 0.04) + (max(0.0, psi - 105.0) * 0.03) + (hours / 15000.0 * 0.4)
                prob = min(0.999, max(0.001, 1.0 / (1.0 + math.exp(-stress + 1.2))))
                alert = "CRITICAL" if prob >= 0.70 else ("WARNING" if prob >= 0.35 else "NORMAL")
                time_to_fail = round(max(2.0, (1.0 - prob) * 1400.0), 1)

                predictions.append({
                    "record_index": idx,
                    "failure_probability": round(prob, 4),
                    "alert_level": alert,
                    "hours_to_failure": time_to_fail,
                    "input_features": rec
                })

            else:
                # Generic model prediction
                numeric_sum = sum(float(v) for v in rec.values() if isinstance(v, (int, float)))
                prob = 1.0 / (1.0 + math.exp(-numeric_sum / 100.0))
                predictions.append({
                    "record_index": idx,
                    "score": round(prob, 4),
                    "class": 1 if prob >= 0.5 else 0,
                    "input_features": rec
                })

    elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Record telemetry on serving endpoint
    try:
        with get_db() as conn:
            conn.execute("""
                UPDATE serving_endpoints
                SET request_count = request_count + ?, last_served_at = ?
                WHERE model_name = ?;
            """, (len(records), now_str, clean_model_name))
    except Exception as e:
        logger.debug(f"Telemetry update notice: {e}")

    return {
        "predictions": predictions,
        "model_name": clean_model_name,
        "full_name": model.get("full_name", clean_model_name),
        "model_version": selected_version["version"],
        "stage": selected_version["current_stage"],
        "aliases": selected_version.get("aliases", []),
        "algorithm": selected_version.get("algorithm", "xgboost"),
        "latency_ms": elapsed_ms,
        "served_at": now_str,
        "record_count": len(predictions)
    }
