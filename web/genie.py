"""
Databricks Genie: Conversational Text-to-SQL Engine for Local Lakehouse.
Provides natural language querying over local Delta Lakehouse tables using
local Ollama models, remote LLM APIs (Gemini/OpenAI/Anthropic), or smart heuristics.
"""

import os
import sys
import json
import time
import re
import uuid
import logging
import datetime
from typing import Dict, Any, List, Optional, Tuple
import requests

logger = logging.getLogger("localspark.genie")

WAREHOUSE_DIR = os.environ.get("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
CHATS_FILE = os.path.join(METADATA_DIR, "genie_chats.json")
DB_PATH = os.path.join(METADATA_DIR, "history.db")

os.makedirs(METADATA_DIR, exist_ok=True)

# Candidate host addresses for Ollama (handles host networking, docker rootless, bridge)
OLLAMA_CANDIDATE_HOSTS = [
    os.environ.get("OLLAMA_HOST", ""),
    "http://10.0.2.2:11434",        # Docker rootless default host gateway
    "http://host.docker.internal:11434",
    "http://172.17.0.1:11434",      # Standard docker0 host gateway
    "http://localhost:11434",       # Direct host / port forward
    "http://127.0.0.1:11434"
]

def detect_ollama() -> Tuple[Optional[str], List[str]]:
    """Detects active Ollama server and lists available models."""
    for host in OLLAMA_CANDIDATE_HOSTS:
        if not host:
            continue
        host = host.rstrip("/")
        if not host.startswith("http"):
            host = f"http://{host}"
        try:
            res = requests.get(f"{host}/api/tags", timeout=1.5)
            if res.status_code == 200:
                data = res.json()
                models = [m["name"] for m in data.get("models", [])]
                return host, models
        except Exception:
            continue
    return None, []

# Candidate host addresses for LM Studio (port 1234)
LMSTUDIO_CANDIDATE_HOSTS = [
    os.environ.get("LMSTUDIO_HOST", ""),
    os.environ.get("LM_STUDIO_HOST", ""),
    "http://10.0.2.2:1234",          # Docker rootless default host gateway
    "http://host.docker.internal:1234",
    "http://172.17.0.1:1234",        # Standard docker0 host gateway
    "http://localhost:1234",         # Direct host / port forward
    "http://127.0.0.1:1234"
]

def detect_lmstudio() -> Tuple[Optional[str], List[str], List[str]]:
    """Detects active LM Studio server, listing loaded and available models."""
    for host in LMSTUDIO_CANDIDATE_HOSTS:
        if not host:
            continue
        host = host.rstrip("/")
        if not host.startswith("http"):
            host = f"http://{host}"
        try:
            # Try LM Studio native /api/v0/models to check loaded state
            res = requests.get(f"{host}/api/v0/models", timeout=1.5)
            if res.status_code == 200:
                data = res.json()
                raw_models = data.get("data", [])
                loaded_models = [m["id"] for m in raw_models if m.get("state") == "loaded"]
                other_models = [m["id"] for m in raw_models if m.get("state") != "loaded"]
                all_models = loaded_models + other_models
                return host, all_models, loaded_models
            
            # Fallback to standard OpenAI-compatible /v1/models
            res = requests.get(f"{host}/v1/models", timeout=1.5)
            if res.status_code == 200:
                data = res.json()
                models = [m["id"] for m in data.get("data", [])]
                return host, models, []
        except Exception:
            continue
    return None, [], []

def get_available_providers() -> Dict[str, Any]:
    """Returns detected LLM providers and configuration status."""
    ollama_host, ollama_models = detect_ollama()
    lmstudio_host, lmstudio_models, lmstudio_loaded = detect_lmstudio()
    
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()

    providers = []
    default_provider = "heuristic"
    default_model = "builtin-heuristic"

    # 1. Ollama (Local)
    if ollama_host and ollama_models:
        providers.append({
            "id": "ollama",
            "name": "Ollama (Local LLM)",
            "available": True,
            "host": ollama_host,
            "models": ollama_models
        })
        default_provider = "ollama"
        preferred = ["qwen2.5-coder:latest", "llama3.2:3b", "qwen3.6:latest", "deepseek-r1:latest"]
        for p in preferred:
            if p in ollama_models:
                default_model = p
                break
        else:
            default_model = ollama_models[0]

    # 2. LM Studio (Local)
    if lmstudio_host and lmstudio_models:
        providers.append({
            "id": "lmstudio",
            "name": "LM Studio (Local Server)",
            "available": True,
            "host": lmstudio_host,
            "models": lmstudio_models,
            "loaded_models": lmstudio_loaded
        })
        # If no default set yet or if user prefers LM Studio
        if default_provider == "heuristic":
            default_provider = "lmstudio"
            default_model = lmstudio_loaded[0] if lmstudio_loaded else lmstudio_models[0]

    # 3. Cloud Providers
    if openai_key:
        providers.append({
            "id": "openai",
            "name": "OpenAI",
            "available": True,
            "models": ["gpt-4o-mini", "gpt-4o"]
        })
        if default_provider == "heuristic":
            default_provider = "openai"
            default_model = "gpt-4o-mini"

    if gemini_key:
        providers.append({
            "id": "gemini",
            "name": "Google Gemini",
            "available": True,
            "models": ["gemini-1.5-flash", "gemini-1.5-pro"]
        })

    if anthropic_key:
        providers.append({
            "id": "anthropic",
            "name": "Anthropic Claude",
            "available": True,
            "models": ["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307"]
        })

    # Always provide Heuristic rule-based fallback
    providers.append({
        "id": "heuristic",
        "name": "Local Rule-Based Parser (Offline Fallback)",
        "available": True,
        "models": ["builtin-heuristic"]
    })

    # Check if a custom default provider has been configured in platform settings
    try:
        from web.llm_settings import load_llm_config
        cfg = load_llm_config()
        configured_default = cfg.get("default_provider")
        if configured_default and configured_default != "auto":
            for p in providers:
                if p["id"] == configured_default:
                    default_provider = configured_default
                    if p.get("loaded_models"):
                        default_model = p["loaded_models"][0]
                    elif p.get("models"):
                        default_model = p["models"][0]
                    break
    except Exception:
        pass

    return {
        "providers": providers,
        "default_provider": default_provider,
        "default_model": default_model,
        "ollama_host": ollama_host,
        "ollama_models": ollama_models,
        "lmstudio_host": lmstudio_host,
        "lmstudio_models": lmstudio_models,
        "lmstudio_loaded": lmstudio_loaded
    }

# ==================== CHAT STORAGE ====================

def load_chats(user: Optional[str] = None, is_admin: bool = True) -> List[Dict[str, Any]]:
    if not os.path.exists(CHATS_FILE):
        return []
    try:
        with open(CHATS_FILE, "r") as f:
            data = json.load(f)
            all_chats = data.get("chats", [])
            if not is_admin:
                return [c for c in all_chats if c.get("user", "admin") == user]
            elif user and user != "all":
                return [c for c in all_chats if c.get("user", "admin") == user]
            return all_chats
    except Exception as e:
        logger.error(f"Failed to load genie chats: {e}")
        return []

def save_chats(chats: List[Dict[str, Any]]):
    try:
        with open(CHATS_FILE, "w") as f:
            json.dump({"chats": chats}, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save genie chats: {e}")

def get_chat(chat_id: str, user: Optional[str] = None, is_admin: bool = True) -> Optional[Dict[str, Any]]:
    chats = load_chats(user=None, is_admin=True)
    for c in chats:
        if c["id"] == chat_id:
            if not is_admin and user and c.get("user", "admin") != user:
                return None
            return c
    return None

def create_chat(title: str = "New Exploration", user: str = "admin") -> Dict[str, Any]:
    chats = load_chats(user=None, is_admin=True)
    new_chat = {
        "id": f"genie_{uuid.uuid4().hex[:8]}",
        "title": title,
        "user": user or "admin",
        "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "messages": []
    }
    chats.insert(0, new_chat)
    save_chats(chats)
    return new_chat

def delete_chat(chat_id: str, user: Optional[str] = None, is_admin: bool = True) -> bool:
    chats = load_chats(user=None, is_admin=True)
    orig_len = len(chats)
    if is_admin or not user:
        chats = [c for c in chats if c["id"] != chat_id]
    else:
        chats = [c for c in chats if not (c["id"] == chat_id and c.get("user", "admin") == user)]
    if len(chats) != orig_len:
        save_chats(chats)
        return True
    return False

# ==================== SCHEMA CONTEXT BUILDER ====================

def _tags_in_use() -> bool:
    try:
        from web.governance import tags
        return tags.has_any_tags()
    except Exception:
        return True          # if governance state cannot be read, assume tags exist (mask/omit samples)


def extract_schema_context(conn=None) -> Dict[str, Any]:
    """
    Extracts table schemas, column data types, and sample rows
    from the DuckDB warehouse and all attached catalogs to inject into LLM prompts.
    """
    from web.warehouses import sync_catalogs_with_duckrun
    if conn is None:
        import duckrun
        conn = duckrun.connect(WAREHOUSE_DIR, read_only=True)
        sync_catalogs_with_duckrun(conn)

    tables_info = []
    system_prompt_parts = []

    try:
        # Discover all tables across all attached catalogs in DuckDB
        raw_tables = conn.sql("SHOW ALL TABLES").fetchall()

        for row in raw_tables:
            cat_name = row[0]
            schema_name = row[1]
            tbl_name = row[2]
            col_names = row[3]
            col_types = row[4]

            # Skip internal or temp tables
            if tbl_name.startswith("__") or tbl_name.startswith("sqlite_"):
                continue

            full_ident = f"{cat_name}.{schema_name}.{tbl_name}" if cat_name != "warehouse" else tbl_name
            columns = [{"name": cname, "type": ctype, "nullable": True} for cname, ctype in zip(col_names, col_types)]
            col_defs = ", ".join([f"{c['name']} {c['type']}" for c in columns])

            sample_rows = []
            try:
                sample_sql = f'SELECT * FROM "{cat_name}"."{schema_name}"."{tbl_name}" LIMIT 2'
                if _tags_in_use():
                    # Samples go into LLM prompts: compute them as the LLM-context principal so tagged values are masked.
                    from web.governance import gateway
                    gateway.ensure_masks(conn)
                    gov = gateway.govern_sql(sample_sql, gateway.LLM_CONTEXT, con=conn.con.cursor(), trusted=True)
                    sample_sql = None if gov.blocked else gov.sql
                if sample_sql:
                    samples = conn.sql(sample_sql).fetchall()
                    sample_rows = [list(sr) for sr in samples]
            except Exception:
                pass

            tables_info.append({
                "table_name": full_ident,
                "catalog": cat_name,
                "schema": schema_name,
                "name": tbl_name,
                "columns": columns,
                "sample_rows": sample_rows
            })

            sample_text = ""
            if sample_rows:
                sample_text = f" -- Sample row: {sample_rows[0]}"
            system_prompt_parts.append(f"Table: {full_ident} ({col_defs}){sample_text}")

    except Exception as e:
        logger.error(f"Error extracting schema context: {e}")

    schema_summary = "\n".join(system_prompt_parts)
    return {
        "tables": tables_info,
        "schema_summary": schema_summary
    }

# ==================== LLM INFERENCE ====================

GENIE_SYSTEM_PROMPT = """You are Databricks Genie, an expert SQL data assistant for a local Delta Lakehouse powered by DuckDB.
Your task is to convert natural language questions into fast, accurate, DuckDB-compatible SQL queries.

Database Schema Information:
{schema_summary}

Rules & Guidelines:
1. Return ONLY valid DuckDB SQL. You can query any of the listed tables directly by name (e.g. `SELECT * FROM silver_employees`).
2. Use appropriate DuckDB SQL functions: `COUNT(*)`, `ROUND(val, 2)`, `AVG(val)`, `SUM(val)`, `DATE_TRUNC('month', dt)`, `CASE WHEN ... END`.
3. For aggregations and rankings, always include appropriate `GROUP BY` and `ORDER BY` clauses.
4. Default to read-only `SELECT` queries. Do NOT write DROP, DELETE, or destructive queries unless explicitly instructed.
5. If the user asks for a chart or visualization, specify the best visualization type: 'bar', 'line', 'pie', or 'table', along with the suggested x_axis and y_axis column names.
6. Provide a concise, friendly explanation of how the query works and what it computes.
7. Return your response strictly as a JSON object matching this schema:
{{
  "sql": "SELECT ...",
  "explanation": "...",
  "suggested_visualization": "bar" | "line" | "pie" | "table",
  "x_axis": "column_name",
  "y_axis": "column_name"
}}
"""

def extract_json_from_text(text: str) -> Dict[str, Any]:
    """Robustly extracts JSON object from LLM output (handles code fences, raw text)."""
    text = text.strip()
    
    # Try direct parse
    try:
        return json.loads(text)
    except Exception:
        pass

    # Try markdown json fence: ```json ... ```
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass

    # Try finding outermost { ... }
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start:end+1])
        except Exception:
            pass

    # Fallback: extract raw SQL if enclosed in ```sql ... ```
    sql_match = re.search(r"```(?:sql)?\s*(SELECT.*?;?)\s*```", text, re.IGNORECASE | re.DOTALL)
    if sql_match:
        sql = sql_match.group(1).strip()
        return {
            "sql": sql,
            "explanation": "Extracted SQL query from response.",
            "suggested_visualization": "table",
            "x_axis": "",
            "y_axis": ""
        }

    raise ValueError(f"Could not parse valid JSON or SQL from response: {text[:200]}")

def call_ollama(host: str, model: str, messages: List[Dict[str, str]]) -> Dict[str, Any]:
    url = f"{host.rstrip('/')}/api/chat"
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_predict": 512
        }
    }
    res = requests.post(url, json=payload, timeout=60)
    if res.status_code != 200:
        raise RuntimeError(f"Ollama error {res.status_code}: {res.text}")
    data = res.json()
    content = data.get("message", {}).get("content", "")
    return extract_json_from_text(content)

def call_lmstudio(host: str, model: str, messages: List[Dict[str, str]]) -> Dict[str, Any]:
    url = f"{host.rstrip('/')}/v1/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 512
    }
    res = requests.post(url, json=payload, timeout=60)
    if res.status_code != 200:
        raise RuntimeError(f"LM Studio error {res.status_code}: {res.text}")
    data = res.json()
    choices = data.get("choices", [])
    if not choices:
        raise RuntimeError(f"LM Studio returned empty choices: {res.text}")
    content = choices[0].get("message", {}).get("content", "")
    return extract_json_from_text(content)

def call_openai(model: str, messages: List[Dict[str, str]]) -> Dict[str, Any]:
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {
        "model": model or "gpt-4o-mini",
        "messages": messages,
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }
    res = requests.post(url, json=payload, headers=headers, timeout=30)
    if res.status_code != 200:
        raise RuntimeError(f"OpenAI error {res.status_code}: {res.text}")
    content = res.json()["choices"][0]["message"]["content"]
    return extract_json_from_text(content)

def call_gemini(model: str, messages: List[Dict[str, str]]) -> Dict[str, Any]:
    key = os.environ.get("GEMINI_API_KEY")
    if not key:
        raise ValueError("GEMINI_API_KEY environment variable not set")
    model_name = model or "gemini-1.5-flash"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={key}"
    
    # Format messages for Gemini
    gemini_contents = []
    for m in messages:
        role = "user" if m["role"] == "user" else "model"
        if m["role"] == "system":
            role = "user"
        gemini_contents.append({"role": role, "parts": [{"text": m["content"]}]})

    payload = {
        "contents": gemini_contents,
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json"
        }
    }
    res = requests.post(url, json=payload, timeout=30)
    if res.status_code != 200:
        raise RuntimeError(f"Gemini error {res.status_code}: {res.text}")
    data = res.json()
    text = data["candidates"][0]["content"]["parts"][0]["text"]
    return extract_json_from_text(text)

def call_anthropic(model: str, messages: List[Dict[str, str]]) -> Dict[str, Any]:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    model_name = model or "claude-3-5-sonnet-20241022"
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    sys_msgs = [m["content"] for m in messages if m.get("role") == "system"]
    conv_msgs = [
        {"role": "user" if m.get("role") == "user" else "assistant", "content": m.get("content", "")}
        for m in messages if m.get("role") in ("user", "assistant")
    ]
    payload = {
        "model": model_name,
        "messages": conv_msgs,
        "max_tokens": 1024,
        "temperature": 0.1
    }
    if sys_msgs:
        payload["system"] = "\n\n".join(sys_msgs)
    res = requests.post(url, json=payload, headers=headers, timeout=30)
    if res.status_code != 200:
        raise RuntimeError(f"Anthropic error {res.status_code}: {res.text}")
    data = res.json()
    text = "".join([block.get("text", "") for block in data.get("content", [])])
    return extract_json_from_text(text)

def call_heuristic_fallback(user_prompt: str, schema_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    High-accuracy rule-based natural language SQL generator.
    Provides instant, zero-latency responses for common exploration patterns,
    ensuring 100% offline reliability.
    """
    p = user_prompt.lower()
    tables = [t["table_name"] for t in schema_info.get("tables", [])]

    # Pattern 1: Employees / Salary / Department
    if any(k in p for k in ["employee", "salary", "department", "earner", "earn", "paid"]):
        if any(k in p for k in ["top", "highest", "best paid", "max"]):
            limit_m = re.search(r"\b(\d+)\b", p)
            limit = int(limit_m.group(1)) if limit_m else 5
            return {
                "sql": f"SELECT name, department, salary, hire_date, rank FROM warehouse.dbo.silver_employees ORDER BY salary DESC LIMIT {limit};",
                "explanation": f"Retrieves the top {limit} highest paid employees sorted by salary descending.",
                "suggested_visualization": "bar",
                "x_axis": "name",
                "y_axis": "salary"
            }
        elif any(k in p for k in ["avg", "average", "by department", "department"]):
            return {
                "sql": "SELECT department, count(*) as employee_count, round(avg(salary), 2) as avg_salary, round(sum(salary), 2) as total_payroll FROM warehouse.dbo.silver_employees GROUP BY department ORDER BY avg_salary DESC;",
                "explanation": "Calculates the employee headcount, average salary, and total payroll grouped by department.",
                "suggested_visualization": "bar",
                "x_axis": "department",
                "y_axis": "avg_salary"
            }
        else:
            return {
                "sql": "SELECT department, count(*) as employee_count FROM warehouse.dbo.silver_employees GROUP BY department ORDER BY employee_count DESC;",
                "explanation": "Groups employees by department to show the distribution of team sizes.",
                "suggested_visualization": "pie",
                "x_axis": "department",
                "y_axis": "employee_count"
            }

    # Pattern 2: NYSE Tickers / Stock Market / Sector
    if any(k in p for k in ["nyse", "ticker", "stock", "market", "sector", "cap"]):
        if "sector" in p:
            return {
                "sql": "SELECT sector, count(*) as ticker_count, round(avg(market_cap_billions), 2) as avg_market_cap_b, round(sum(market_cap_billions), 2) as total_market_cap_b FROM nyse_tickers GROUP BY sector ORDER BY total_market_cap_b DESC;",
                "explanation": "Aggregates NYSE listed companies by sector, showing total and average market capitalization in billions.",
                "suggested_visualization": "bar",
                "x_axis": "sector",
                "y_axis": "total_market_cap_b"
            }
        else:
            return {
                "sql": "SELECT symbol, company_name, sector, price, market_cap_billions, pe_ratio FROM nyse_tickers ORDER BY market_cap_billions DESC LIMIT 10;",
                "explanation": "Lists the top 10 largest publicly traded companies on NYSE by market capitalization.",
                "suggested_visualization": "bar",
                "x_axis": "symbol",
                "y_axis": "market_cap_billions"
            }

    # Pattern 3: Products / Inventory / Valuation
    if any(k in p for k in ["product", "inventory", "stock", "price", "valuation"]):
        return {
            "sql": "SELECT category, count(*) as product_count, round(avg(price), 2) as avg_price, round(sum(price * stock_quantity), 2) as inventory_valuation FROM dim_products GROUP BY category ORDER BY inventory_valuation DESC;",
            "explanation": "Breaks down catalog products by category, calculating inventory valuation and average product price.",
            "suggested_visualization": "bar",
            "x_axis": "category",
            "y_axis": "inventory_valuation"
        }

    # Pattern 4: Customers / Sales / Revenue
    if any(k in p for k in ["customer", "spend", "revenue", "country", "churn"]):
        return {
            "sql": "SELECT country, count(*) as customer_count, round(avg(total_spend), 2) as avg_spend, round(sum(total_spend), 2) as total_revenue FROM dim_customers GROUP BY country ORDER BY total_revenue DESC LIMIT 10;",
            "explanation": "Summarizes customer metrics and total sales revenue across top countries.",
            "suggested_visualization": "pie",
            "x_axis": "country",
            "y_axis": "total_revenue"
        }

    # Pattern 5: Telemetry / IoT / Sensors / Anomaly
    if any(k in p for k in ["sensor", "temp", "telemetry", "iot", "anomaly", "alert"]):
        return {
            "sql": "SELECT sensor_type, count(*) as total_readings, round(avg(temp_c), 2) as avg_temp_c, round(max(temp_c), 2) as max_temp_c, sum(case when alert_status != 'NORMAL' then 1 else 0 end) as alert_count FROM silver_telemetry GROUP BY sensor_type ORDER BY alert_count DESC;",
            "explanation": "Analyzes sensor telemetry readings and highlights temperature anomalies and critical alert counts.",
            "suggested_visualization": "bar",
            "x_axis": "sensor_type",
            "y_axis": "alert_count"
        }

    # Generic Fallback: Pick the first available table
    target_table = tables[0] if tables else "warehouse.dbo.silver_employees"
    return {
        "sql": f"SELECT * FROM {target_table} LIMIT 20;",
        "explanation": f"Displays a 20-row sample from the '{target_table}' table for data exploration.",
        "suggested_visualization": "table",
        "x_axis": "",
        "y_axis": ""
    }

# ==================== QUERY EXECUTION & LOGGING ====================

def execute_genie_sql(sql: str, conn=None, principal=None) -> Dict[str, Any]:
    """
    Executes generated SQL in DuckDB and formats results for table & chart rendering.
    The SQL runs as `principal` (a user dict or Principal; None means least privilege): LLM-written SQL gets no extra rights.
    """
    if conn is None:
        import duckrun
        conn = duckrun.connect(WAREHOUSE_DIR, read_only=True)

    start_time = time.perf_counter()
    try:
        clean_sql = sql.strip().rstrip(";")
        from web.governance import gateway
        gateway.ensure_masks(conn)
        governed = gateway.govern_sql(clean_sql, principal, client="genie", con=conn.con.cursor())
        if governed.blocked:
            raise ValueError(f"Blocked by governance: {governed.blocked}")
        cursor = conn.sql(governed.sql)
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

        desc = cursor.description or []
        columns = [{"name": d[0], "type": str(d[1])} for d in desc]
        rows = cursor.fetchall()

        # Serialize rows
        serialized_rows = []
        for row in rows[:200]:  # Limit UI preview to 200 rows
            row_dict = {}
            for i, col in enumerate(columns):
                val = row[i]
                if isinstance(val, (datetime.date, datetime.datetime)):
                    val = str(val)
                row_dict[col["name"]] = val
            serialized_rows.append(row_dict)

        # Log into query audit history
        log_to_history(clean_sql, duration_ms, len(rows), "SUCCESS", client="GENIE")

        return {
            "success": True,
            "columns": columns,
            "rows": serialized_rows,
            "row_count": len(rows),
            "duration_ms": duration_ms,
            "error": None
        }

    except Exception as e:
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        err_msg = str(e)
        log_to_history(sql, duration_ms, 0, "FAILED", error_message=err_msg, client="GENIE")
        return {
            "success": False,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "duration_ms": duration_ms,
            "error": err_msg
        }

def log_to_history(query_text: str, duration_ms: float, rows: int, status: str, error_message: str = None, client: str = "GENIE"):
    """Helper to record query executions in history.db without importing web.app cycle."""
    try:
        import sqlite3
        with sqlite3.connect(DB_PATH) as sconn:
            qid = f"qry_{uuid.uuid4().hex[:8]}"
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sconn.execute("""
                INSERT INTO query_history (
                    query_id, query_text, executed_at, duration_ms, rows_produced,
                    status, error_message, client, is_mutation, user
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (qid, query_text, now, duration_ms, rows, status, error_message, client, 0, "genie"))
    except Exception as e:
        logger.debug(f"Could not log query to history: {e}")

# ==================== MAIN GENIE PIPELINE ====================

def ask_genie(
    chat_id: str,
    user_prompt: str,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    user: str = "admin",
    is_admin: bool = True,
    principal=None
) -> Dict[str, Any]:
    """
    Main conversational pipeline:
    1. Loads chat session history.
    2. Extracts Unity Catalog schema context.
    3. Calls LLM (or heuristic fallback) with schema context.
    4. Executes generated SQL query.
    5. Appends user & assistant messages to chat session and returns response.
    """
    chat = get_chat(chat_id, user=user, is_admin=is_admin)
    if not chat:
        chat = create_chat(title=user_prompt[:35] + ("..." if len(user_prompt) > 35 else ""), user=user)

    # Extract schema context
    schema_info = extract_schema_context()
    sys_prompt = GENIE_SYSTEM_PROMPT.format(schema_summary=schema_info["schema_summary"])

    # Determine provider and model
    config = get_available_providers()
    chosen_provider = provider or config["default_provider"]

    if provider:
        prov_obj = next((p for p in config.get("providers", []) if p["id"] == provider), None)
        if prov_obj and prov_obj.get("models"):
            if not model or model not in prov_obj["models"]:
                if prov_obj.get("loaded_models"):
                    chosen_model = prov_obj["loaded_models"][0]
                else:
                    chosen_model = prov_obj["models"][0]
            else:
                chosen_model = model
        else:
            chosen_model = model or config["default_model"]
    else:
        chosen_model = model or config["default_model"]

    # Build multi-turn messages
    llm_messages = [{"role": "system", "content": sys_prompt}]
    
    # Inject up to last 4 conversation turns for context
    for msg in chat.get("messages", [])[-4:]:
        if msg["role"] == "user":
            llm_messages.append({"role": "user", "content": msg["content"]})
        elif msg["role"] == "assistant" and msg.get("sql"):
            llm_messages.append({
                "role": "assistant",
                "content": json.dumps({
                    "sql": msg["sql"],
                    "explanation": msg.get("explanation", "")
                })
            })

    llm_messages.append({"role": "user", "content": user_prompt})

    # Call Model
    generated_plan = None
    generation_error = None
    gen_start = time.perf_counter()

    if chosen_provider == "ollama" and config.get("ollama_host"):
        try:
            generated_plan = call_ollama(config["ollama_host"], chosen_model, llm_messages)
        except Exception as e:
            logger.warning(f"Ollama call failed ({e}); falling back to heuristic engine.")
            generation_error = str(e)
            generated_plan = call_heuristic_fallback(user_prompt, schema_info)
            chosen_provider = "heuristic (fallback)"

    elif chosen_provider == "lmstudio" and config.get("lmstudio_host"):
        try:
            generated_plan = call_lmstudio(config["lmstudio_host"], chosen_model, llm_messages)
        except Exception as e:
            logger.warning(f"LM Studio call failed ({e}); falling back to heuristic engine.")
            generation_error = str(e)
            generated_plan = call_heuristic_fallback(user_prompt, schema_info)
            chosen_provider = "heuristic (fallback)"

    elif chosen_provider == "openai":
        try:
            generated_plan = call_openai(chosen_model, llm_messages)
        except Exception as e:
            logger.warning(f"OpenAI call failed ({e}); falling back to heuristic engine.")
            generation_error = str(e)
            generated_plan = call_heuristic_fallback(user_prompt, schema_info)
            chosen_provider = "heuristic (fallback)"

    elif chosen_provider == "gemini":
        try:
            generated_plan = call_gemini(chosen_model, llm_messages)
        except Exception as e:
            logger.warning(f"Gemini call failed ({e}); falling back to heuristic engine.")
            generation_error = str(e)
            generated_plan = call_heuristic_fallback(user_prompt, schema_info)
            chosen_provider = "heuristic (fallback)"

    elif chosen_provider == "anthropic":
        try:
            generated_plan = call_anthropic(chosen_model, llm_messages)
        except Exception as e:
            logger.warning(f"Anthropic call failed ({e}); falling back to heuristic engine.")
            generation_error = str(e)
            generated_plan = call_heuristic_fallback(user_prompt, schema_info)
            chosen_provider = "heuristic (fallback)"

    else:
        generated_plan = call_heuristic_fallback(user_prompt, schema_info)

    gen_duration_sec = round(time.perf_counter() - gen_start, 2)

    sql_query = generated_plan.get("sql", "").strip()
    explanation = generated_plan.get("explanation", "Query generated based on schema analysis.")
    vis_type = generated_plan.get("suggested_visualization", "table")
    x_col = generated_plan.get("x_axis", "")
    y_col = generated_plan.get("y_axis", "")

    # Execute query
    exec_result = execute_genie_sql(sql_query, principal=principal)

    # If first query fails and we were using an LLM, attempt 1 quick self-correction
    if not exec_result["success"] and chosen_provider in ["ollama", "lmstudio", "openai", "gemini", "anthropic"]:
        try:
            fix_messages = list(llm_messages)
            fix_messages.append({"role": "assistant", "content": json.dumps({"sql": sql_query})})
            fix_messages.append({
                "role": "user",
                "content": f"The query failed with DuckDB error: {exec_result['error']}. Please fix the SQL statement and return JSON."
            })
            if chosen_provider == "ollama":
                fixed_plan = call_ollama(config["ollama_host"], chosen_model, fix_messages)
            elif chosen_provider == "lmstudio":
                fixed_plan = call_lmstudio(config["lmstudio_host"], chosen_model, fix_messages)
            elif chosen_provider == "openai":
                fixed_plan = call_openai(chosen_model, fix_messages)
            elif chosen_provider == "gemini":
                fixed_plan = call_gemini(chosen_model, fix_messages)
            else:
                fixed_plan = call_anthropic(chosen_model, fix_messages)

            if fixed_plan.get("sql"):
                sql_query = fixed_plan["sql"]
                explanation = fixed_plan.get("explanation", explanation) + " (Auto-corrected)"
                exec_result = execute_genie_sql(sql_query, principal=principal)
        except Exception:
            pass

    # Auto-infer visualization if missing
    if vis_type not in ["bar", "line", "pie", "table"]:
        vis_type = "table"
    if exec_result["success"] and len(exec_result["columns"]) >= 2 and not x_col:
        x_col = exec_result["columns"][0]["name"]
        y_col = exec_result["columns"][1]["name"]

    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Record messages in chat thread
    user_msg = {
        "id": f"msg_{uuid.uuid4().hex[:6]}",
        "role": "user",
        "content": user_prompt,
        "created_at": now_str
    }

    assistant_msg = {
        "id": f"msg_{uuid.uuid4().hex[:6]}",
        "role": "assistant",
        "content": explanation,
        "sql": sql_query,
        "explanation": explanation,
        "provider": chosen_provider,
        "model": chosen_model,
        "gen_duration_sec": gen_duration_sec,
        "query_result": exec_result,
        "suggested_visualization": vis_type,
        "x_axis": x_col,
        "y_axis": y_col,
        "created_at": now_str
    }

    chat["messages"].append(user_msg)
    chat["messages"].append(assistant_msg)
    chat["updated_at"] = now_str
    
    # Update title if it was default
    if chat["title"] == "New Exploration" or chat["title"].startswith("New "):
        chat["title"] = user_prompt[:40] + ("..." if len(user_prompt) > 40 else "")

    chats = load_chats(user=None, is_admin=True)
    for i, c in enumerate(chats):
        if c["id"] == chat["id"]:
            chats[i] = chat
            break
    else:
        chats.insert(0, chat)
    save_chats(chats)

    return {
        "chat_id": chat["id"],
        "user_message": user_msg,
        "assistant_message": assistant_msg
    }
