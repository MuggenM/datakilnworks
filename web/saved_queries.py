import os
import json
import uuid
import datetime
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger("localspark.saved_queries")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
if not os.path.exists(WAREHOUSE_DIR):
    local_alt = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "warehouse"))
    if os.path.exists(local_alt):
        WAREHOUSE_DIR = local_alt
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
SAVED_QUERIES_FILE = os.path.join(METADATA_DIR, "saved_queries.json")


def get_default_saved_queries() -> List[Dict[str, Any]]:
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return [
        {
            "id": "sq_dept_salary_distribution",
            "name": "Department Salary & Headcount",
            "description": "Aggregates total employee count, annual salary sum, and average salary grouped by department.",
            "query_text": (
                "SELECT \n"
                "    department,\n"
                "    COUNT(*) AS total_employees,\n"
                "    ROUND(AVG(salary), 2) AS avg_salary,\n"
                "    ROUND(SUM(salary), 2) AS total_payroll\n"
                "FROM warehouse.dbo.silver_employees\n"
                "GROUP BY department\n"
                "ORDER BY avg_salary DESC;"
            ),
            "warehouse_id": "wh_starter",
            "catalog": "warehouse",
            "schema_name": "dbo",
            "tags": ["HR", "Payroll", "Analytics"],
            "created_at": now_str,
            "updated_at": now_str,
            "last_run_at": None,
            "run_count": 0
        },
        {
            "id": "sq_nyse_market_cap_leaders",
            "name": "NYSE Top Market Cap Tickers by Sector",
            "description": "Ranks top publicly traded companies on NYSE by sector and calculates market capitalization totals.",
            "query_text": (
                "SELECT \n"
                "    sector,\n"
                "    COUNT(ticker) AS num_companies,\n"
                "    ROUND(SUM(market_cap_b), 2) AS total_market_cap_b,\n"
                "    ROUND(AVG(market_cap_b), 2) AS avg_market_cap_b\n"
                "FROM warehouse.dbo.nyse_tickers\n"
                "GROUP BY sector\n"
                "ORDER BY total_market_cap_b DESC;"
            ),
            "warehouse_id": "wh_starter",
            "catalog": "warehouse",
            "schema_name": "dbo",
            "tags": ["Finance", "Equities", "Executive"],
            "created_at": now_str,
            "updated_at": now_str,
            "last_run_at": None,
            "run_count": 0
        },
        {
            "id": "sq_inventory_reorder_valuation",
            "name": "Product Inventory Valuation & Low Stock Warning",
            "description": "Computes total inventory asset value per category and highlights SKUs requiring immediate reordering.",
            "query_text": (
                "SELECT \n"
                "    category,\n"
                "    product_name,\n"
                "    stock_qty,\n"
                "    price,\n"
                "    ROUND(stock_qty * price, 2) AS inventory_value,\n"
                "    CASE \n"
                "        WHEN stock_qty < 25 THEN 'CRITICAL: Reorder Now'\n"
                "        WHEN stock_qty < 50 THEN 'WARNING: Low Stock'\n"
                "        ELSE 'HEALTHY'\n"
                "    END AS stock_health\n"
                "FROM warehouse.dbo.dim_products\n"
                "ORDER BY stock_qty ASC;"
            ),
            "warehouse_id": "wh_starter",
            "catalog": "warehouse",
            "schema_name": "dbo",
            "tags": ["Ops", "Inventory", "Supply Chain"],
            "created_at": now_str,
            "updated_at": now_str,
            "last_run_at": None,
            "run_count": 0
        },
        {
            "id": "sq_cross_catalog_telemetry_join",
            "name": "Cross-Catalog Telemetry & Dev Scores",
            "description": "Demonstrates multi-catalog federation joining primary Lakehouse employees with dev_catalog test scores.",
            "query_text": (
                "SELECT \n"
                "    e.id AS employee_id,\n"
                "    e.name,\n"
                "    e.department,\n"
                "    t.environment,\n"
                "    t.score\n"
                "FROM warehouse.dbo.silver_employees e\n"
                "CROSS JOIN dev_catalog.dbo.test_dev t\n"
                "ORDER BY t.score DESC\n"
                "LIMIT 25;"
            ),
            "warehouse_id": "wh_starter",
            "catalog": "warehouse",
            "schema_name": "dbo",
            "tags": ["Dev", "Cross-Catalog", "Federation"],
            "created_at": now_str,
            "updated_at": now_str,
            "last_run_at": None,
            "run_count": 0
        },
        {
            "id": "sq_gold_kpi_hourly_throughput",
            "name": "Gold Telemetry Hourly KPI Trends",
            "description": "Analyzes device sensor metrics, average temperatures, and event volumes from the Gold analytical layer.",
            "query_text": (
                "SELECT \n"
                "    device_id,\n"
                "    ROUND(avg_temp, 2) AS avg_temperature,\n"
                "    event_count,\n"
                "    window_start\n"
                "FROM warehouse.dbo.gold_telemetry_kpis\n"
                "ORDER BY window_start DESC\n"
                "LIMIT 50;"
            ),
            "warehouse_id": "wh_starter",
            "catalog": "warehouse",
            "schema_name": "dbo",
            "tags": ["IoT", "KPIs", "Gold Layer"],
            "created_at": now_str,
            "updated_at": now_str,
            "last_run_at": None,
            "run_count": 0
        },
        {
            "id": "sq_ai_employee_turnover_scoring",
            "name": "AI Inference: Employee Attrition Risk Scoring",
            "description": "Databricks ai_predict / ai_score model inference predicting employee turnover probabilities and retention recommendations.",
            "query_text": (
                "SELECT \n"
                "    name,\n"
                "    department,\n"
                "    salary,\n"
                "    ai_score('employee_turnover_predictor', json_object('salary', salary, 'tenure_years', 2.5, 'satisfaction_score', 0.45, 'overtime_hours', 12.0)) AS turnover_risk,\n"
                "    ai_classify('employee_turnover_predictor', json_object('salary', salary, 'tenure_years', 2.5, 'satisfaction_score', 0.45, 'overtime_hours', 12.0)) AS risk_tier,\n"
                "    ai_explain('employee_turnover_predictor', json_object('salary', salary, 'tenure_years', 2.5, 'satisfaction_score', 0.45, 'overtime_hours', 12.0)) AS recommendation\n"
                "FROM warehouse.dbo.silver_employees\n"
                "ORDER BY turnover_risk DESC\n"
                "LIMIT 20;"
            ),
            "warehouse_id": "wh_starter",
            "catalog": "warehouse",
            "schema_name": "dbo",
            "tags": ["AI/ML", "Inference", "MLflow"],
            "created_at": now_str,
            "updated_at": now_str,
            "last_run_at": None,
            "run_count": 0
        },
        {
            "id": "sq_ai_predictive_maintenance_forecasting",
            "name": "AI Inference: Industrial Equipment Failure Risk",
            "description": "Predictive maintenance model inference estimating failure likelihood, alert tier, and remaining hours to failure.",
            "query_text": (
                "WITH telemetry_samples AS (\n"
                "    SELECT 101 AS machine_id, 'Hydraulic Pump A' AS machine_name, 5.4 AS vib, 89.0 AS temp, 138.0 AS psi, 7400 AS op_hours\n"
                "    UNION ALL\n"
                "    SELECT 102 AS machine_id, 'Wind Turbine B' AS machine_name, 2.1 AS vib, 68.0 AS temp, 98.0 AS psi, 1200 AS op_hours\n"
                "    UNION ALL\n"
                "    SELECT 103 AS machine_id, 'Centrifugal Compressor C' AS machine_name, 6.2 AS vib, 96.0 AS temp, 155.0 AS psi, 11200 AS op_hours\n"
                ")\n"
                "SELECT \n"
                "    machine_id,\n"
                "    machine_name,\n"
                "    ai_score('equipment_failure_forecaster', json_object('vibration_rms', vib, 'temperature_c', temp, 'pressure_psi', psi, 'operating_hours', op_hours)) AS failure_risk,\n"
                "    ai_classify('equipment_failure_forecaster', json_object('vibration_rms', vib, 'temperature_c', temp, 'pressure_psi', psi, 'operating_hours', op_hours)) AS alert_level,\n"
                "    ai_predict('equipment_failure_forecaster', json_object('vibration_rms', vib, 'temperature_c', temp, 'pressure_psi', psi, 'operating_hours', op_hours)) ->> '$.hours_to_failure' AS est_hours_remaining\n"
                "FROM telemetry_samples\n"
                "ORDER BY failure_risk DESC;"
            ),
            "warehouse_id": "wh_starter",
            "catalog": "warehouse",
            "schema_name": "dbo",
            "tags": ["AI/ML", "IoT", "Predictive Maintenance"],
            "created_at": now_str,
            "updated_at": now_str,
            "last_run_at": None,
            "run_count": 0
        }
    ]


def load_saved_queries() -> List[Dict[str, Any]]:
    """Loads all saved queries from JSON storage, populating defaults if missing."""
    os.makedirs(METADATA_DIR, exist_ok=True)
    if not os.path.exists(SAVED_QUERIES_FILE):
        defaults = get_default_saved_queries()
        save_saved_queries(defaults)
        return defaults
    
    try:
        with open(SAVED_QUERIES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            loaded = []
            if isinstance(data, dict):
                loaded = data.get("queries", [])
            elif isinstance(data, list):
                loaded = data
            
            # Ensure new AI default queries are present
            existing_ids = {q.get("id") for q in loaded if isinstance(q, dict)}
            defaults = get_default_saved_queries()
            updated = False
            for d in defaults:
                if d["id"] not in existing_ids and d["id"].startswith("sq_ai_"):
                    loaded.append(d)
                    updated = True
            if updated:
                save_saved_queries(loaded)
            return loaded
    except Exception as e:
        logger.error(f"Failed to load saved_queries.json: {e}")
        return get_default_saved_queries()


def save_saved_queries(queries: List[Dict[str, Any]]) -> None:
    """Saves the queries list atomically to JSON storage."""
    os.makedirs(METADATA_DIR, exist_ok=True)
    tmp_file = f"{SAVED_QUERIES_FILE}.tmp"
    payload = {
        "queries": queries,
        "updated_at": datetime.datetime.now().isoformat()
    }
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        os.replace(tmp_file, SAVED_QUERIES_FILE)
    except Exception as e:
        logger.error(f"Failed to save saved_queries.json: {e}")
        if os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except Exception:
                pass
        raise e


def get_saved_queries(
    q: Optional[str] = None,
    tag: Optional[str] = None,
    user_id: Optional[str] = None,
    is_admin: bool = True,
    shared_ids: Optional[set] = None
) -> List[Dict[str, Any]]:
    """Returns saved queries filtered by search keyword, tag, and user ownership."""
    queries = load_saved_queries()
    filtered = queries

    if not is_admin and user_id:
        default_ids = {d["id"] for d in get_default_saved_queries()}
        filtered = [
            query for query in filtered
            if query.get("id") in default_ids
            or query.get("owner") == user_id
            or query.get("created_by") == user_id
            or query.get("is_starter")
            or query.get("id") in (shared_ids or ())          # shared with the user or one of their groups (web/groups.py)
        ]

    if tag and tag.lower() != "all":
        tag_lower = tag.lower().strip()
        filtered = [
            query for query in filtered
            if any(t.lower() == tag_lower for t in query.get("tags", []))
        ]

    if q and q.strip():
        search_term = q.lower().strip()
        filtered = [
            query for query in filtered
            if (
                search_term in query.get("name", "").lower()
                or search_term in query.get("description", "").lower()
                or search_term in query.get("query_text", "").lower()
                or any(search_term in t.lower() for t in query.get("tags", []))
            )
        ]

    return filtered


def get_saved_query(query_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves a single saved query by ID."""
    queries = load_saved_queries()
    return next((item for item in queries if item.get("id") == query_id), None)


def create_saved_query(data: Dict[str, Any]) -> Dict[str, Any]:
    """Creates a new saved query and persists it."""
    queries = load_saved_queries()
    qid = f"sq_{uuid.uuid4().hex[:8]}"
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Format tags safely
    raw_tags = data.get("tags", [])
    if isinstance(raw_tags, str):
        tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    elif isinstance(raw_tags, list):
        tags = [str(t).strip() for t in raw_tags if str(t).strip()]
    else:
        tags = []

    owner = data.get("owner") or data.get("user") or "admin"

    new_query = {
        "id": qid,
        "name": (data.get("name") or "Untitled Query").strip(),
        "description": (data.get("description") or "").strip(),
        "query_text": (data.get("query_text") or "").strip(),
        "warehouse_id": data.get("warehouse_id") or "wh_starter",
        "catalog": data.get("catalog") or "warehouse",
        "schema_name": data.get("schema_name") or "dbo",
        "tags": tags,
        "owner": owner,
        "created_by": owner,
        "created_at": now_str,
        "updated_at": now_str,
        "last_run_at": None,
        "run_count": 0
    }

    queries.insert(0, new_query)
    save_saved_queries(queries)
    return new_query


def update_saved_query(query_id: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Updates an existing saved query's attributes and query text."""
    queries = load_saved_queries()
    target = None

    for item in queries:
        if item.get("id") == query_id:
            target = item
            break

    if not target:
        return None

    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if "name" in data and data["name"] is not None:
        target["name"] = data["name"].strip()
    if "description" in data and data["description"] is not None:
        target["description"] = data["description"].strip()
    if "query_text" in data and data["query_text"] is not None:
        target["query_text"] = data["query_text"].strip()
    if "warehouse_id" in data and data["warehouse_id"] is not None:
        target["warehouse_id"] = data["warehouse_id"]
    if "catalog" in data and data["catalog"] is not None:
        target["catalog"] = data["catalog"]
    if "schema_name" in data and data["schema_name"] is not None:
        target["schema_name"] = data["schema_name"]
    if "tags" in data and data["tags"] is not None:
        raw_tags = data["tags"]
        if isinstance(raw_tags, str):
            target["tags"] = [t.strip() for t in raw_tags.split(",") if t.strip()]
        elif isinstance(raw_tags, list):
            target["tags"] = [str(t).strip() for t in raw_tags if str(t).strip()]

    target["updated_at"] = now_str
    save_saved_queries(queries)
    return target


def delete_saved_query(query_id: str) -> bool:
    """Deletes a saved query by ID."""
    queries = load_saved_queries()
    initial_len = len(queries)
    filtered = [item for item in queries if item.get("id") != query_id]
    if len(filtered) == initial_len:
        return False
    save_saved_queries(filtered)
    return True


def duplicate_saved_query(query_id: str, owner: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Clones an existing saved query with a unique ID and 'Copy of' name prefix (owned by `owner`: the copy is the caller's, not the original's)."""
    target = get_saved_query(query_id)
    if not target:
        return None

    clone_data = {
        "name": f"Copy of {target.get('name', 'Query')}",
        "description": target.get("description", ""),
        "query_text": target.get("query_text", ""),
        "warehouse_id": target.get("warehouse_id", "wh_starter"),
        "catalog": target.get("catalog", "warehouse"),
        "schema_name": target.get("schema_name", "dbo"),
        "tags": list(target.get("tags", []))
    }
    if owner:
        clone_data["owner"] = owner
    return create_saved_query(clone_data)


def record_saved_query_run(query_id: str) -> Optional[Dict[str, Any]]:
    """Increments run counter and updates last_run_at timestamp."""
    queries = load_saved_queries()
    target = None
    for item in queries:
        if item.get("id") == query_id:
            target = item
            break

    if not target:
        return None

    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target["run_count"] = target.get("run_count", 0) + 1
    target["last_run_at"] = now_str
    save_saved_queries(queries)
    return target
