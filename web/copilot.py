"""
Databricks Copilot & Context-Aware Autocomplete Engine for Localspark Studio.
Provides:
1. In-memory cached Unity Catalog autocomplete metadata for Monaco SQL Editor
   (catalogs, schemas, tables, columns with data types, SQL keywords, DuckDB functions).
2. Inline Copilot (Ctrl+I) natural-language-to-SQL generation with schema injection,
   multi-model provider support (Ollama, LM Studio, Cloud APIs), and offline heuristic fallback.
"""

import os
import time
import json
import re
import logging
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger("localspark.copilot")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
if not os.path.exists(WAREHOUSE_DIR):
    local_alt = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "warehouse"))
    if os.path.exists(local_alt):
        WAREHOUSE_DIR = local_alt

# ==================== AUTOCOMPLETE METADATA ====================

_AUTOCOMPLETE_CACHE: Dict[str, Any] = {"data": None, "cached_at": 0.0}
AUTOCOMPLETE_CACHE_TTL = 10.0  # seconds

DUCKDB_KEYWORDS = [
    "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET",
    "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "CROSS JOIN",
    "ON", "USING", "AS", "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE", "ILIKE",
    "IS NULL", "IS NOT NULL", "DISTINCT", "ALL", "UNION", "UNION ALL", "INTERSECT", "EXCEPT",
    "WITH", "CREATE TABLE", "CREATE VIEW", "INSERT INTO", "MERGE INTO", "UPDATE", "DELETE",
    "DROP TABLE", "ALTER TABLE", "SHOW TABLES", "DESCRIBE", "EXPLAIN", "PRAGMA",
    "CASE", "WHEN", "THEN", "ELSE", "END", "CAST", "TRUE", "FALSE", "NULL", "ASC", "DESC"
]

DUCKDB_FUNCTIONS = [
    {"name": "COUNT", "signature": "COUNT(*)", "detail": "Aggregate: Counts rows or non-null values", "snippet": "COUNT(${1:*})"},
    {"name": "SUM", "signature": "SUM(col)", "detail": "Aggregate: Computes sum of numeric column", "snippet": "SUM(${1:column})"},
    {"name": "AVG", "signature": "AVG(col)", "detail": "Aggregate: Computes arithmetic mean", "snippet": "AVG(${1:column})"},
    {"name": "MIN", "signature": "MIN(col)", "detail": "Aggregate: Returns minimum value", "snippet": "MIN(${1:column})"},
    {"name": "MAX", "signature": "MAX(col)", "detail": "Aggregate: Returns maximum value", "snippet": "MAX(${1:column})"},
    {"name": "ROUND", "signature": "ROUND(val, [decimals])", "detail": "Math: Rounds numeric expression", "snippet": "ROUND(${1:val}, ${2:2})"},
    {"name": "COALESCE", "signature": "COALESCE(val1, val2, ...)", "detail": "Conditional: Returns first non-null argument", "snippet": "COALESCE(${1:val1}, ${2:val2})"},
    {"name": "NULLIF", "signature": "NULLIF(val1, val2)", "detail": "Conditional: Returns null if arguments are equal", "snippet": "NULLIF(${1:val1}, ${2:val2})"},
    {"name": "DATE_TRUNC", "signature": "DATE_TRUNC('part', timestamp)", "detail": "Date: Truncates timestamp to specified granularity", "snippet": "DATE_TRUNC('${1:month}', ${2:timestamp})"},
    {"name": "DATE_DIFF", "signature": "DATE_DIFF('part', start, end)", "detail": "Date: Computes difference between dates", "snippet": "DATE_DIFF('${1:day}', ${2:start_date}, ${3:end_date})"},
    {"name": "STRFTIME", "signature": "STRFTIME(ts, 'format')", "detail": "Date: Formats timestamp as string", "snippet": "STRFTIME(${1:timestamp}, '${2:%Y-%m-%d}')"},
    {"name": "NOW", "signature": "NOW()", "detail": "Date: Returns current date and time", "snippet": "NOW()"},
    {"name": "CONCAT", "signature": "CONCAT(str1, str2, ...)", "detail": "String: Concatenates string arguments", "snippet": "CONCAT(${1:str1}, ${2:str2})"},
    {"name": "SUBSTRING", "signature": "SUBSTRING(str, start, [len])", "detail": "String: Extracts substring", "snippet": "SUBSTRING(${1:str}, ${2:1}, ${3:10})"},
    {"name": "UPPER", "signature": "UPPER(str)", "detail": "String: Converts string to uppercase", "snippet": "UPPER(${1:str})"},
    {"name": "LOWER", "signature": "LOWER(str)", "detail": "String: Converts string to lowercase", "snippet": "LOWER(${1:str})"},
    {"name": "TRIM", "signature": "TRIM(str)", "detail": "String: Strips leading and trailing whitespace", "snippet": "TRIM(${1:str})"},
    {"name": "REGEXP_MATCHES", "signature": "REGEXP_MATCHES(str, 'regex')", "detail": "Regex: Pattern matching predicate", "snippet": "REGEXP_MATCHES(${1:str}, '${2:pattern}')"},
    {"name": "ROW_NUMBER", "signature": "ROW_NUMBER() OVER (...)", "detail": "Window: Assigns sequential integer per partition", "snippet": "ROW_NUMBER() OVER (PARTITION BY ${1:dept} ORDER BY ${2:salary} DESC)"},
    {"name": "DENSE_RANK", "signature": "DENSE_RANK() OVER (...)", "detail": "Window: Assigns rank without gaps", "snippet": "DENSE_RANK() OVER (ORDER BY ${1:salary} DESC)"},
    {"name": "delta_scan", "signature": "delta_scan(path, [version => N])", "detail": "Delta Lake: Vectorized Delta table scan with optional time-travel", "snippet": "delta_scan('${1:path}', version => ${2:0})"},
    {"name": "read_parquet", "signature": "read_parquet('path/*.parquet')", "detail": "DuckDB: Direct scan of Parquet files", "snippet": "read_parquet('${1:path/*.parquet}')"},
    {"name": "predict", "signature": "predict('model_name', features)", "detail": "Databricks MLflow: Evaluates model and returns predicted class or regression value as VARCHAR", "snippet": "predict('${1:employee_turnover_predictor}', {'${2:salary}': ${3:85000}})"},
    {"name": "predict_score", "signature": "predict_score('model_name', features)", "detail": "Databricks MLflow: Evaluates model and returns primary probability or score as DOUBLE", "snippet": "predict_score('${1:employee_turnover_predictor}', {'${2:salary}': ${3:85000}})"},
    {"name": "ai_predict", "signature": "ai_predict('model_name', json_object(...))", "detail": "MLflow Inference: Runs model prediction and returns JSON result with metadata", "snippet": "ai_predict('${1:employee_turnover_predictor}', json_object('${2:key}', ${3:val}))"},
    {"name": "ai_score", "signature": "ai_score('model_name', json_object(...))", "detail": "MLflow Inference: Returns primary continuous probability/score as DOUBLE", "snippet": "ai_score('${1:employee_turnover_predictor}', json_object('${2:key}', ${3:val}))"},
    {"name": "ai_classify", "signature": "ai_classify('model_name', json_object(...))", "detail": "MLflow Inference: Returns predicted category or risk tier as VARCHAR", "snippet": "ai_classify('${1:employee_turnover_predictor}', json_object('${2:key}', ${3:val}))"},
    {"name": "ai_explain", "signature": "ai_explain('model_name', json_object(...))", "detail": "MLflow Inference: Returns recommendation and explanation text as VARCHAR", "snippet": "ai_explain('${1:employee_turnover_predictor}', json_object('${2:key}', ${3:val}))"},
    {"name": "ai_query", "signature": "ai_query('model_or_endpoint', json_object(...))", "detail": "Databricks ai_query(): Queries registered ML model or LLM endpoint", "snippet": "ai_query('${1:employee_turnover_predictor}', json_object('${2:key}', ${3:val}))"}
]


def invalidate_autocomplete_cache():
    """Invalidates the in-memory autocomplete cache."""
    _AUTOCOMPLETE_CACHE["data"] = None
    _AUTOCOMPLETE_CACHE["cached_at"] = 0.0


def get_autocomplete_metadata(conn=None) -> Dict[str, Any]:
    """
    Returns complete Unity Catalog metadata structured for Monaco's completion provider:
    - catalogs: List of registered catalogs and their schemas
    - tables: List of table objects with column schemas and data types
    - columns: Unique column index mapping columns to parent tables
    - keywords: DuckDB SQL keywords
    - functions: Analytical, date, window, and delta functions with snippets
    """
    now = time.time()
    if _AUTOCOMPLETE_CACHE["data"] is not None and (now - _AUTOCOMPLETE_CACHE["cached_at"]) < AUTOCOMPLETE_CACHE_TTL:
        return _AUTOCOMPLETE_CACHE["data"]

    from web.warehouses import sync_catalogs_with_duckrun, load_catalogs

    should_close = False
    if conn is None:
        try:
            import duckrun
            conn = duckrun.connect(WAREHOUSE_DIR, read_only=True)
            sync_catalogs_with_duckrun(conn)
            should_close = True
        except Exception as e:
            logger.warning(f"Could not open duckrun connection for autocomplete: {e}")
            conn = None

    catalogs_dict: Dict[str, Dict[str, Any]] = {}
    tables_list: List[Dict[str, Any]] = []
    columns_flat: List[Dict[str, Any]] = []

    # Initialize registered catalogs
    try:
        registered_catalogs = load_catalogs()
        for cat in registered_catalogs:
            cat_id = cat.get("id", "warehouse")
            catalogs_dict[cat_id] = {
                "id": cat_id,
                "name": cat.get("name", cat_id),
                "schemas": {}
            }
    except Exception:
        catalogs_dict["warehouse"] = {"id": "warehouse", "name": "Local Warehouse", "schemas": {}}

    if "warehouse" not in catalogs_dict:
        catalogs_dict["warehouse"] = {"id": "warehouse", "name": "Local Warehouse", "schemas": {}}

    if conn is not None:
        try:
            rows = conn.sql("SHOW ALL TABLES").fetchall()
            for row in rows:
                cat_name = row[0]
                schema_name = row[1]
                tbl_name = row[2]
                col_names = row[3]
                col_types = row[4]

                if tbl_name.startswith("__") or tbl_name.startswith("sqlite_"):
                    continue

                full_ident = f"{cat_name}.{schema_name}.{tbl_name}" if cat_name != "warehouse" else tbl_name
                canonical_ident = f"{cat_name}.{schema_name}.{tbl_name}"

                cols = []
                for cname, ctype in zip(col_names, col_types):
                    clean_type = str(ctype).upper()
                    col_obj = {"name": cname, "type": clean_type}
                    cols.append(col_obj)
                    columns_flat.append({
                        "name": cname,
                        "type": clean_type,
                        "table_name": tbl_name,
                        "full_table_name": full_ident,
                        "canonical_table_name": canonical_ident
                    })

                tbl_obj = {
                    "name": tbl_name,
                    "full_name": full_ident,
                    "canonical_name": canonical_ident,
                    "catalog": cat_name,
                    "schema": schema_name,
                    "columns": cols,
                    "column_count": len(cols)
                }
                tables_list.append(tbl_obj)

                if cat_name not in catalogs_dict:
                    catalogs_dict[cat_name] = {"id": cat_name, "name": cat_name, "schemas": {}}
                if schema_name not in catalogs_dict[cat_name]["schemas"]:
                    catalogs_dict[cat_name]["schemas"][schema_name] = []
                catalogs_dict[cat_name]["schemas"][schema_name].append(tbl_name)

        except Exception as e:
            logger.error(f"Error executing SHOW ALL TABLES for autocomplete: {e}")
        finally:
            if should_close:
                try:
                    conn.close()
                except Exception:
                    pass

    # Transform catalogs to nested list
    catalogs_list = []
    for cat_id, cat_info in catalogs_dict.items():
        schemas_list = []
        for s_name, s_tables in cat_info["schemas"].items():
            schemas_list.append({"name": s_name, "tables": s_tables})
        catalogs_list.append({
            "id": cat_id,
            "name": cat_info["name"],
            "schemas": schemas_list
        })

    result = {
        "catalogs": catalogs_list,
        "tables": tables_list,
        "columns": columns_flat,
        "keywords": DUCKDB_KEYWORDS,
        "functions": DUCKDB_FUNCTIONS
    }

    _AUTOCOMPLETE_CACHE["data"] = result
    _AUTOCOMPLETE_CACHE["cached_at"] = now
    return result


# ==================== INLINE COPILOT (Ctrl+I) ====================

def call_copilot_heuristic(prompt: str, current_query: Optional[str], schema_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Intelligent rule-based NL-to-SQL synthesis fallback when LLM models are unavailable.
    Provides immediate response for key exploration and editing requests.
    """
    p = prompt.strip().lower()
    tables = [t["table_name"] for t in schema_info.get("tables", [])]

    # Check if prompt asks to modify/filter an existing query
    if current_query and current_query.strip() and any(k in p for k in ["order by", "sort by", "filter", "where", "limit", "group by"]):
        cleaned = current_query.strip().rstrip(";")
        if "order by" in p or "sort" in p:
            desc = "desc" if any(k in p for k in ["desc", "highest", "top", "greatest"]) else "asc"
            m = re.search(r"(?:order by|sort by)\s+([a-zA-Z0-9_]+)", p)
            col = m.group(1) if m else "1"
            if "order by" not in cleaned.lower():
                modified_sql = f"{cleaned}\nORDER BY {col} {desc.upper()};"
            else:
                modified_sql = re.sub(r"order\s+by\s+[^;]+", f"ORDER BY {col} {desc.upper()}", cleaned, flags=re.IGNORECASE) + ";"
            return {
                "sql": modified_sql,
                "explanation": f"Updated sorting clause to order by {col} {desc.upper()}.",
                "tables_used": []
            }
        if "limit" in p:
            m = re.search(r"\b(\d+)\b", p)
            lim = int(m.group(1)) if m else 10
            if "limit" in cleaned.lower():
                modified_sql = re.sub(r"limit\s+\d+", f"LIMIT {lim}", cleaned, flags=re.IGNORECASE) + ";"
            else:
                modified_sql = f"{cleaned}\nLIMIT {lim};"
            return {
                "sql": modified_sql,
                "explanation": f"Updated query result limit to {lim} rows.",
                "tables_used": []
            }

    # Pattern 1: Employees, Salaries, Departments, Payroll
    if any(k in p for k in ["employee", "salary", "department", "earner", "earn", "paid", "workforce", "headcount"]):
        if any(k in p for k in ["top", "highest", "best paid", "max"]):
            m = re.search(r"\b(\d+)\b", p)
            limit = int(m.group(1)) if m else 5
            dept_match = re.search(r"in\s+([a-zA-Z]+)", p)
            where_clause = f"WHERE department = '{dept_match.group(1).title()}' " if dept_match and dept_match.group(1).lower() not in ["the", "desc", "asc"] else ""
            return {
                "sql": f"SELECT name, department, salary, hire_date, rank\nFROM warehouse.dbo.silver_employees\n{where_clause}ORDER BY salary DESC\nLIMIT {limit};",
                "explanation": f"Selects the top {limit} highest paid employees sorted by salary descending.",
                "tables_used": ["warehouse.dbo.silver_employees"]
            }
        elif any(k in p for k in ["avg", "average", "by department", "breakdown"]):
            having_clause = "HAVING COUNT(*) > 2\n" if any(k in p for k in ["> 2", "more than 2", ">2"]) else ""
            return {
                "sql": f"SELECT \n    department,\n    COUNT(*) AS total_employees,\n    ROUND(AVG(salary), 2) AS avg_salary,\n    ROUND(SUM(salary), 2) AS total_payroll\nFROM warehouse.dbo.silver_employees\nGROUP BY department\n{having_clause}ORDER BY avg_salary DESC;",
                "explanation": "Aggregates employee count, average salary, and total payroll per department.",
                "tables_used": ["warehouse.dbo.silver_employees"]
            }
        else:
            return {
                "sql": "SELECT name, department, salary, hire_date FROM warehouse.dbo.silver_employees LIMIT 20;",
                "explanation": "Lists employees with department and salary details.",
                "tables_used": ["warehouse.dbo.silver_employees"]
            }

    # Pattern 2: Stocks, Market Cap, NYSE, Tickers, Valuation
    if any(k in p for k in ["stock", "ticker", "market cap", "company", "nyse", "pe ratio", "valuation"]):
        if any(k in p for k in ["sector", "by sector"]):
            return {
                "sql": "SELECT \n    sector,\n    COUNT(*) AS company_count,\n    ROUND(SUM(market_cap_b), 1) AS total_market_cap_billions,\n    ROUND(AVG(pe_ratio), 1) AS avg_pe_ratio\nFROM nyse_tickers\nGROUP BY sector\nORDER BY total_market_cap_billions DESC;",
                "explanation": "Aggregates NYSE companies by market sector with total market capitalization and average P/E ratio.",
                "tables_used": ["nyse_tickers"]
            }
        else:
            m = re.search(r"\b(\d+)\b", p)
            limit = int(m.group(1)) if m else 10
            return {
                "sql": f"SELECT ticker, company, sector, market_cap_b, pe_ratio\nFROM nyse_tickers\nORDER BY market_cap_b DESC\nLIMIT {limit};",
                "explanation": f"Retrieves the top {limit} companies by market capitalization on the NYSE.",
                "tables_used": ["nyse_tickers"]
            }

    # Pattern 3: Products, Inventory, Stock, Catalog
    if any(k in p for k in ["product", "inventory", "stock", "price", "catalog"]):
        return {
            "sql": "SELECT \n    category,\n    COUNT(*) AS product_count,\n    ROUND(AVG(price), 2) AS avg_price,\n    ROUND(SUM(price * stock_qty), 2) AS total_inventory_valuation\nFROM dim_products\nGROUP BY category\nORDER BY total_inventory_valuation DESC;",
            "explanation": "Calculates product counts, average prices, and total inventory value per category.",
            "tables_used": ["dim_products"]
        }

    # Pattern 4: Customers, Spends, Accounts, Sales
    if any(k in p for k in ["customer", "spend", "revenue", "segment", "accounts"]):
        return {
            "sql": "SELECT \n    segment,\n    COUNT(*) AS total_customers,\n    ROUND(AVG(annual_spend), 2) AS avg_annual_spend,\n    ROUND(SUM(annual_spend), 2) AS total_segment_spend\nFROM dim_customers\nGROUP BY segment\nORDER BY total_segment_spend DESC;",
            "explanation": "Summarizes customer segments by customer count and total annual spending.",
            "tables_used": ["dim_customers"]
        }

    # Pattern 5: Telemetry, IoT, Sensor Readings, Temperatures
    if any(k in p for k in ["sensor", "temp", "telemetry", "iot", "anomaly", "alert"]):
        return {
            "sql": "SELECT \n    sensor_type,\n    COUNT(*) AS readings_count,\n    ROUND(AVG(temp_c), 2) AS avg_temp_c,\n    ROUND(MAX(temp_c), 2) AS max_temp_c,\n    SUM(CASE WHEN alert_status != 'NORMAL' THEN 1 ELSE 0 END) AS alerts_count\nFROM silver_telemetry\nGROUP BY sensor_type\nORDER BY alerts_count DESC;",
            "explanation": "Computes sensor reading statistics, average temperatures, and critical alert counts grouped by sensor type.",
            "tables_used": ["silver_telemetry"]
        }

    # Fallback to first discovered table
    first_tbl = "warehouse.dbo.silver_employees"
    for t in schema_info.get("tables", []):
        if not t["name"].startswith("__"):
            first_tbl = t["table_name"]
            break

    return {
        "sql": f"SELECT * FROM {first_tbl} LIMIT 25;",
        "explanation": f"Generates a 25-row preview sample from table '{first_tbl}'.",
        "tables_used": [first_tbl]
    }


def generate_copilot_sql(
    prompt: str,
    current_query: Optional[str] = None,
    selection: Optional[str] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    conn=None
) -> Dict[str, Any]:
    """
    Synthesizes DuckDB SQL from a natural language request using the best available LLM
    or rule-based heuristic engine with schema context injection.
    """
    from web.genie import (
        extract_schema_context, get_available_providers,
        call_ollama, call_lmstudio, call_openai, call_gemini, call_anthropic
    )

    schema_info = extract_schema_context(conn)
    providers_info = get_available_providers()

    selected_provider = provider or providers_info.get("default_provider", "heuristic")
    selected_model = model or providers_info.get("default_model", "builtin-heuristic")

    logger.info(f"Copilot generating SQL with provider={selected_provider}, model={selected_model}")

    # If provider is explicitly heuristic or no LLM provider detected
    if selected_provider == "heuristic" or not providers_info.get("providers"):
        res = call_copilot_heuristic(prompt, current_query, schema_info)
        res["provider"] = "heuristic"
        res["model"] = "builtin-heuristic"
        res["success"] = True
        return res

    # Build contextual prompt for LLM
    context_text = ""
    if selection and selection.strip():
        context_text = f"\nUser has highlighted the following query selection in editor:\n```sql\n{selection.strip()}\n```\n"
    elif current_query and current_query.strip():
        context_text = f"\nCurrent query in editor:\n```sql\n{current_query.strip()}\n```\n"

    system_prompt = f"""You are Databricks Copilot, an expert in-editor SQL coding assistant for a local Delta Lakehouse powered by DuckDB.
Your task is to generate or edit fast, accurate, DuckDB-compatible SQL based on user instructions.

Database Schema Information:
{schema_info.get('schema_summary', '')}

CRITICAL RULES:
1. You MUST use the EXACT table names and column names from the Database Schema Information above (e.g. use `silver_employees`, NOT `employees`; use `nyse_tickers`, NOT `stocks`).
2. Generate valid DuckDB SQL syntax.
3. If modifying an existing query, preserve the user's intent and update clauses cleanly.
1. Always output ONLY valid JSON matching this schema:
{{
  "sql": "SELECT ...",
  "explanation": "Brief 1-sentence explanation of what the query does.",
  "tables_used": ["warehouse.dbo.silver_employees", ...]
}}
2. Target DuckDB SQL dialect (support standard SQL-92/99, CTEs, window functions).
3. Do NOT invent columns or tables. Only use tables from the lakehouse context provided.
4. Output MUST be valid parseable JSON. Do NOT wrap in markdown code blocks like ```json ... ```. Output raw JSON directly.
"""

    user_message = f"{prompt.strip()}{context_text}"
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message}
    ]

    try:
        if selected_provider == "ollama" and providers_info.get("ollama_host"):
            raw = call_ollama(providers_info["ollama_host"], selected_model, messages)
        elif selected_provider == "lmstudio" and providers_info.get("lmstudio_host"):
            raw = call_lmstudio(providers_info["lmstudio_host"], selected_model, messages)
        elif selected_provider == "openai":
            raw = call_openai(selected_model, messages)
        elif selected_provider == "gemini":
            raw = call_gemini(selected_model, messages)
        elif selected_provider == "anthropic":
            raw = call_anthropic(selected_model, messages)
        else:
            raw = call_copilot_heuristic(prompt, current_query, schema_info)

        sql = raw.get("sql", "").strip()
        explanation = raw.get("explanation", "Generated SQL based on request.")
        tables_used = raw.get("tables_used", [])

        # Schema normalizer: map common LLM hallucinations to exact lakehouse tables
        table_aliases = {
            r"\bfrom\s+employees\b": "FROM silver_employees",
            r"\bjoin\s+employees\b": "JOIN silver_employees",
            r"\bfrom\s+employee\b": "FROM silver_employees",
            r"\bjoin\s+employee\b": "JOIN silver_employees",
            r"\bfrom\s+customers\b": "FROM dim_customers",
            r"\bjoin\s+customers\b": "JOIN dim_customers",
            r"\bfrom\s+products\b": "FROM dim_products",
            r"\bjoin\s+products\b": "JOIN dim_products",
            r"\bfrom\s+stocks\b": "FROM nyse_tickers",
            r"\bjoin\s+stocks\b": "JOIN nyse_tickers",
            r"\bfrom\s+telemetry\b": "FROM silver_telemetry",
            r"\bjoin\s+telemetry\b": "JOIN silver_telemetry"
        }
        for pat, repl in table_aliases.items():
            sql = re.sub(pat, repl, sql, flags=re.IGNORECASE)

        # Normalize silver_employees column hallucinations
        if "silver_employees" in sql:
            sql = re.sub(r"\b(?:FirstName|First_Name|LastName|Last_Name|EmployeeID|Emp_Id)\b", "name", sql, flags=re.IGNORECASE)
            sql = re.sub(r"\bname\s*,\s*name\b", "name", sql, flags=re.IGNORECASE)
            sql = re.sub(r"\bcount\s*\(\s*(?:employee_id|emp_id|id)\s*\)", "COUNT(*)", sql, flags=re.IGNORECASE)

        # Detect tables used
        tables_used_clean = []
        for t in schema_info.get("tables", []):
            t_name = t.get("name", "")
            if t_name and re.search(rf"\b{t_name}\b", sql, re.IGNORECASE):
                if t_name not in tables_used_clean:
                    tables_used_clean.append(t_name)

        if not tables_used_clean and tables_used:
            tables_used_clean = tables_used

        return {
            "success": True,
            "sql": sql,
            "explanation": explanation,
            "tables_used": tables_used_clean,
            "provider": selected_provider,
            "model": selected_model
        }

    except Exception as e:
        logger.warning(f"LLM generation failed ({e}), falling back to heuristic engine.")
        fallback = call_copilot_heuristic(prompt, current_query, schema_info)
        fallback["provider"] = "heuristic (fallback)"
        fallback["model"] = "builtin-heuristic"
        fallback["success"] = True
        fallback["error_warning"] = str(e)
        return fallback
