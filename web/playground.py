"""
AI Prompt Playground Engine for Localspark Studio.
Provides interactive LLM prompt engineering, local Ollama & LM Studio model discovery,
hyperparameter tuning (temperature, system persona, top_p, token caps),
side-by-side concurrent model comparison, dynamic prompt variable substitution,
lakehouse schema injection, and SQLite template/history persistence.
"""

import os
import time
import json
import uuid
import sqlite3
import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple, Union
import httpx
from deltalake import DeltaTable

logger = logging.getLogger("localspark.playground")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
if not os.path.exists(WAREHOUSE_DIR):
    local_alt = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "warehouse"))
    if os.path.exists(local_alt):
        WAREHOUSE_DIR = local_alt

METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
DB_PATH = os.path.join(METADATA_DIR, "playground.db")

# Candidate gateway addresses to reach host services from container/host
OLLAMA_CANDIDATE_HOSTS = [
    os.environ.get("OLLAMA_HOST", ""),
    "http://10.0.2.2:11434",
    "http://host.docker.internal:11434",
    "http://172.17.0.1:11434",
    "http://localhost:11434",
    "http://127.0.0.1:11434"
]

LMSTUDIO_CANDIDATE_HOSTS = [
    os.environ.get("LMSTUDIO_HOST", ""),
    os.environ.get("LM_STUDIO_HOST", ""),
    "http://10.0.2.2:1234",
    "http://host.docker.internal:1234",
    "http://172.17.0.1:1234",
    "http://localhost:1234",
    "http://127.0.0.1:1234"
]


def get_playground_db() -> sqlite3.Connection:
    """Returns a SQLite connection to playground.db with WAL mode enabled."""
    os.makedirs(METADATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_playground_db():
    """Initializes tables for prompt templates and execution history, seeding starters if empty."""
    try:
        with get_playground_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS playground_templates (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    category TEXT NOT NULL DEFAULT 'General',
                    system_prompt TEXT,
                    user_prompt TEXT NOT NULL,
                    temperature REAL DEFAULT 0.7,
                    top_p REAL DEFAULT 0.9,
                    max_tokens INTEGER DEFAULT 1024,
                    variables TEXT DEFAULT '[]',
                    is_builtin INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tmpl_cat ON playground_templates(category);")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS playground_history (
                    id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL DEFAULT 'single',
                    model_a TEXT NOT NULL,
                    provider_a TEXT NOT NULL,
                    model_b TEXT,
                    provider_b TEXT,
                    system_prompt TEXT,
                    user_prompt TEXT NOT NULL,
                    prompt_rendered TEXT NOT NULL,
                    variables_json TEXT DEFAULT '{}',
                    config_a TEXT,
                    config_b TEXT,
                    response_a TEXT,
                    response_b TEXT,
                    latency_a_ms REAL DEFAULT 0.0,
                    latency_b_ms REAL DEFAULT 0.0,
                    tokens_a INTEGER DEFAULT 0,
                    tokens_b INTEGER DEFAULT 0,
                    tps_a REAL DEFAULT 0.0,
                    tps_b REAL DEFAULT 0.0,
                    error_a TEXT,
                    error_b TEXT,
                    user_id TEXT DEFAULT 'admin',
                    created_at TEXT NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_hist_created ON playground_history(created_at DESC);")

            try:
                conn.execute("ALTER TABLE playground_templates ADD COLUMN user_id TEXT DEFAULT 'admin';")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE playground_history ADD COLUMN user_id TEXT DEFAULT 'admin';")
            except sqlite3.OperationalError:
                pass

            # Check if templates need seeding
            cur = conn.execute("SELECT COUNT(*) FROM playground_templates;")
            count = cur.fetchone()[0]
            if count == 0:
                _seed_builtin_templates(conn)
    except Exception as e:
        logger.error(f"Failed to initialize playground.db: {e}")


def _seed_builtin_templates(conn: sqlite3.Connection):
    """Seeds the 5 Lakehouse starter prompt templates."""
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    starters = [
        (
            "tmpl_sql_specialist",
            "Text-to-SQL Specialist",
            "Translates natural language analytical questions into high-performance DuckDB and Spark SQL.",
            "Data Engineering",
            "You are an expert Data Engineer specializing in DuckDB and Spark SQL. Write standard ANSI/DuckDB SQL queries. Always optimize for performance using CTEs and window functions where appropriate. Include concise comments explaining key transformations.",
            "Generate an analytical SQL query for the following business request:\n\nBusiness Request: {{business_request}}\n\nTarget Tables & Schema:\n{{schema_context}}\n\nReturn the SQL query enclosed in ```sql ... ``` code block followed by a concise bulleted breakdown of how it works.",
            0.2,
            0.9,
            1024,
            json.dumps(["business_request", "schema_context"]),
            1,
            now,
            now
        ),
        (
            "tmpl_delta_advisor",
            "Delta Lake Optimization Advisor",
            "Analyzes table schemas, partition strategies, and data ingestion patterns to suggest OPTIMIZE, Z-ORDER, and VACUUM configurations.",
            "Lakehouse Architecture",
            "You are a Senior Lakehouse Architect. You analyze Delta Lake tables and provide actionable tuning advice covering file compaction, Z-ordering keys, partitioning strategy, and vacuum retention.",
            "Analyze this Delta table configuration and recommend optimization strategies:\n\nTable Name: {{table_name}}\nSchema & Columns: {{columns}}\nDaily Ingest Volume: {{daily_volume}}\nPrimary Query Filter Patterns: {{query_patterns}}\n\nProvide recommendations for:\n1. Partitioning Strategy (if needed, or why unpartitioned is better)\n2. Z-ORDER clustering columns\n3. Maintenance schedule (OPTIMIZE / VACUUM frequency)",
            0.4,
            0.9,
            1024,
            json.dumps(["table_name", "columns", "daily_volume", "query_patterns"]),
            1,
            now,
            now
        ),
        (
            "tmpl_data_quality",
            "Data Quality & Integrity Auditor",
            "Drafts comprehensive validation rules, Great Expectations assertions, and anomaly detection checks.",
            "Data Governance",
            "You are a Data Governance & Quality specialist. Generate comprehensive data quality validation suites including null checks, uniqueness, range boundaries, regex patterns, and cross-column reconciliation.",
            "Given the following table schema:\n{{schema_context}}\n\nIdentify the top 5 data quality risks for table '{{table_name}}' and write executable SQL assertion queries that should return 0 rows if the data is clean.",
            0.3,
            0.9,
            1024,
            json.dumps(["table_name", "schema_context"]),
            1,
            now,
            now
        ),
        (
            "tmpl_pyspark_converter",
            "PySpark to DuckDB SQL Converter",
            "Converts PySpark DataFrame operations (joins, window functions, UDFs) into clean native DuckDB SQL.",
            "Code Migration",
            "You are a Spark and DuckDB migration expert. Convert PySpark DataFrame transformation code into equivalent, elegant, high-speed DuckDB SQL queries.",
            "Convert the following PySpark code snippet into equivalent DuckDB SQL:\n\n```python\n{{pyspark_code}}\n```\n\nEnsure window functions, null-handling, and column renaming behave identically.",
            0.2,
            0.9,
            1024,
            json.dumps(["pyspark_code"]),
            1,
            now,
            now
        ),
        (
            "tmpl_catalog_doc",
            "Data Catalog & Documentation Generator",
            "Generates rich Markdown catalog documentation, business descriptions, column glossaries, and lineage notes.",
            "Catalog & Metadata",
            "You are a Technical Data Writer and Catalog Curator. Given table DDL and sample records, create executive-ready Markdown data documentation including business purpose, column glossary, data freshness expectations, and downstream use cases.",
            "Generate complete catalog documentation for table '{{table_name}}':\n\nSchema Information:\n{{schema_context}}\n\nTarget Audience: {{audience}}",
            0.5,
            0.9,
            1024,
            json.dumps(["table_name", "schema_context", "audience"]),
            1,
            now,
            now
        )
    ]
    conn.executemany("""
        INSERT OR REPLACE INTO playground_templates
        (id, title, description, category, system_prompt, user_prompt, temperature, top_p, max_tokens, variables, is_builtin, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, starters)


# ==================== PROVIDER & MODEL DISCOVERY ====================

async def detect_ollama() -> Tuple[Optional[str], List[Dict[str, Any]]]:
    """Probes candidate Ollama hosts and returns list of models."""
    async with httpx.AsyncClient(timeout=1.5) as client:
        for host in OLLAMA_CANDIDATE_HOSTS:
            if not host:
                continue
            host = host.rstrip("/")
            if not host.startswith("http"):
                host = f"http://{host}"
            try:
                res = await client.get(f"{host}/api/tags")
                if res.status_code == 200:
                    data = res.json()
                    models = []
                    for m in data.get("models", []):
                        name = m.get("name")
                        size_gb = round(m.get("size", 0) / (1024**3), 1) if m.get("size") else None
                        models.append({
                            "id": f"ollama::{name}",
                            "model_name": name,
                            "provider": "ollama",
                            "host": host,
                            "label": f"{name} ({size_gb}GB)" if size_gb else name,
                            "size_gb": size_gb,
                            "details": m.get("details", {})
                        })
                    return host, models
            except Exception:
                continue
    return None, []


async def detect_lmstudio() -> Tuple[Optional[str], List[Dict[str, Any]]]:
    """Probes candidate LM Studio hosts and returns list of models."""
    async with httpx.AsyncClient(timeout=1.5) as client:
        for host in LMSTUDIO_CANDIDATE_HOSTS:
            if not host:
                continue
            host = host.rstrip("/")
            if not host.startswith("http"):
                host = f"http://{host}"
            try:
                # Try native LM Studio /api/v0/models
                res = await client.get(f"{host}/api/v0/models")
                if res.status_code == 200:
                    data = res.json()
                    models = []
                    for m in data.get("data", []):
                        m_id = m.get("id")
                        state = m.get("state", "available")
                        models.append({
                            "id": f"lmstudio::{m_id}",
                            "model_name": m_id,
                            "provider": "lmstudio",
                            "host": host,
                            "label": f"{m_id} [loaded]" if state == "loaded" else m_id,
                            "loaded": state == "loaded"
                        })
                    return host, models

                # Fallback to OpenAI-compatible /v1/models
                res2 = await client.get(f"{host}/v1/models")
                if res2.status_code == 200:
                    data = res2.json()
                    models = [{
                        "id": f"lmstudio::{m.get('id')}",
                        "model_name": m.get("id"),
                        "provider": "lmstudio",
                        "host": host,
                        "label": m.get("id"),
                        "loaded": False
                    } for m in data.get("data", [])]
                    return host, models
            except Exception:
                continue
    return None, []


async def get_available_models() -> Dict[str, Any]:
    """Returns detected models across Ollama, LM Studio, and Cloud providers."""
    ollama_host, ollama_models = await detect_ollama()
    lmstudio_host, lmstudio_models = await detect_lmstudio()

    all_models: List[Dict[str, Any]] = []
    
    # 1. Ollama models
    if ollama_models:
        all_models.extend(ollama_models)

    # 2. LM Studio models
    if lmstudio_models:
        all_models.extend(lmstudio_models)

    # 3. Cloud Provider integrations if API keys present
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if openai_key:
        for m in ["gpt-4o-mini", "gpt-4o"]:
            all_models.append({
                "id": f"openai::{m}",
                "model_name": m,
                "provider": "openai",
                "host": "https://api.openai.com",
                "label": f"{m} (Cloud)",
                "loaded": True
            })

    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if gemini_key:
        for m in ["gemini-1.5-flash", "gemini-1.5-pro"]:
            all_models.append({
                "id": f"gemini::{m}",
                "model_name": m,
                "provider": "gemini",
                "host": "https://generativelanguage.googleapis.com",
                "label": f"{m} (Cloud)",
                "loaded": True
            })

    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if anthropic_key:
        for m in ["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307"]:
            all_models.append({
                "id": f"anthropic::{m}",
                "model_name": m,
                "provider": "anthropic",
                "host": "https://api.anthropic.com",
                "label": f"{m} (Cloud)",
                "loaded": True
            })

    # Default model determination
    default_model_a = None
    default_model_b = None

    if ollama_models:
        preferred = ["qwen2.5-coder:latest", "llama3.2:3b", "qwen3.6:latest", "deepseek-r1:latest"]
        found_pref = [m for p in preferred for m in ollama_models if m["model_name"] == p]
        default_model_a = found_pref[0]["id"] if found_pref else ollama_models[0]["id"]
        if len(ollama_models) > 1:
            candidates = [m["id"] for m in ollama_models if m["id"] != default_model_a]
            default_model_b = candidates[0] if candidates else ollama_models[0]["id"]
        elif lmstudio_models:
            default_model_b = lmstudio_models[0]["id"]
    elif lmstudio_models:
        default_model_a = lmstudio_models[0]["id"]
        if len(lmstudio_models) > 1:
            default_model_b = lmstudio_models[1]["id"]

    return {
        "models": all_models,
        "ollama": {
            "available": bool(ollama_host),
            "host": ollama_host,
            "count": len(ollama_models)
        },
        "lmstudio": {
            "available": bool(lmstudio_host),
            "host": lmstudio_host,
            "count": len(lmstudio_models)
        },
        "openai": {
            "available": bool(openai_key)
        },
        "gemini": {
            "available": bool(gemini_key)
        },
        "anthropic": {
            "available": bool(anthropic_key)
        },
        "default_model_a": default_model_a or (all_models[0]["id"] if all_models else None),
        "default_model_b": default_model_b or (all_models[1]["id"] if len(all_models) > 1 else default_model_a)
    }


# ==================== INFERENCE EXECUTION ====================

async def run_single_prompt(
    provider: str,
    model: str,
    prompt: str,
    system_prompt: str = "",
    temperature: float = 0.7,
    top_p: float = 0.9,
    max_tokens: int = 1024,
    host: Optional[str] = None
) -> Dict[str, Any]:
    """Executes a prompt against a specific LLM provider and measures benchmark metrics."""
    t0 = time.time()
    result = {
        "provider": provider,
        "model": model,
        "content": "",
        "latency_ms": 0.0,
        "tokens": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "tokens_per_sec": 0.0,
        "error": None
    }

    try:
        messages = []
        if system_prompt and system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt.strip()})
        messages.append({"role": "user", "content": prompt})

        if provider == "ollama":
            if not host:
                host, _ = await detect_ollama()
            if not host:
                raise RuntimeError("No active Ollama server detected.")

            url = f"{host.rstrip('/')}/api/chat"
            payload = {
                "model": model,
                "messages": messages,
                "stream": False,
                "options": {
                    "temperature": float(temperature),
                    "top_p": float(top_p),
                    "num_predict": int(max_tokens)
                }
            }
            async with httpx.AsyncClient(timeout=120.0) as client:
                res = await client.post(url, json=payload)
                if res.status_code != 200:
                    raise RuntimeError(f"Ollama error ({res.status_code}): {res.text}")
                data = res.json()
                content = data.get("message", {}).get("content", "")
                result["content"] = content
                
                eval_count = data.get("eval_count", 0)
                prompt_eval_count = data.get("prompt_eval_count", 0)
                eval_duration_ns = data.get("eval_duration", 0)

                result["completion_tokens"] = eval_count
                result["prompt_tokens"] = prompt_eval_count
                result["tokens"] = eval_count + prompt_eval_count
                
                t1 = time.time()
                latency_ms = round((t1 - t0) * 1000, 1)
                result["latency_ms"] = latency_ms

                if eval_duration_ns and eval_duration_ns > 0:
                    tps = round(eval_count / (eval_duration_ns / 1e9), 1)
                else:
                    tps = round(eval_count / (max(t1 - t0, 0.01)), 1)
                result["tokens_per_sec"] = tps

        elif provider == "lmstudio":
            if not host:
                host, _ = await detect_lmstudio()
            if not host:
                raise RuntimeError("No active LM Studio server detected.")

            url = f"{host.rstrip('/')}/v1/chat/completions"
            payload = {
                "model": model,
                "messages": messages,
                "temperature": float(temperature),
                "top_p": float(top_p),
                "max_tokens": int(max_tokens)
            }
            async with httpx.AsyncClient(timeout=120.0) as client:
                res = await client.post(url, json=payload)
                if res.status_code != 200:
                    raise RuntimeError(f"LM Studio error ({res.status_code}): {res.text}")
                data = res.json()
                choices = data.get("choices", [])
                if not choices:
                    raise RuntimeError("LM Studio returned empty completion choices.")
                content = choices[0].get("message", {}).get("content", "")
                result["content"] = content

                usage = data.get("usage", {})
                comp_tokens = usage.get("completion_tokens", 0)
                prompt_tokens = usage.get("prompt_tokens", 0)
                result["completion_tokens"] = comp_tokens
                result["prompt_tokens"] = prompt_tokens
                result["tokens"] = usage.get("total_tokens", comp_tokens + prompt_tokens)

                t1 = time.time()
                latency_ms = round((t1 - t0) * 1000, 1)
                result["latency_ms"] = latency_ms
                result["tokens_per_sec"] = round(comp_tokens / (max(t1 - t0, 0.01)), 1)

        elif provider == "openai":
            key = os.environ.get("OPENAI_API_KEY")
            if not key:
                raise RuntimeError("OPENAI_API_KEY environment variable not set")
            url = "https://api.openai.com/v1/chat/completions"
            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
            payload = {
                "model": model or "gpt-4o-mini",
                "messages": messages,
                "temperature": float(temperature),
                "top_p": float(top_p),
                "max_tokens": int(max_tokens)
            }
            async with httpx.AsyncClient(timeout=60.0) as client:
                res = await client.post(url, json=payload, headers=headers)
                if res.status_code != 200:
                    raise RuntimeError(f"OpenAI error ({res.status_code}): {res.text}")
                data = res.json()
                content = data["choices"][0]["message"]["content"]
                result["content"] = content
                usage = data.get("usage", {})
                result["completion_tokens"] = usage.get("completion_tokens", 0)
                result["prompt_tokens"] = usage.get("prompt_tokens", 0)
                result["tokens"] = usage.get("total_tokens", 0)
                t1 = time.time()
                result["latency_ms"] = round((t1 - t0) * 1000, 1)
                result["tokens_per_sec"] = round(result["completion_tokens"] / (max(t1 - t0, 0.01)), 1)

        elif provider == "gemini":
            key = os.environ.get("GEMINI_API_KEY")
            if not key:
                raise RuntimeError("GEMINI_API_KEY environment variable not set")
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
            gemini_contents = []
            for m in messages:
                role = "user" if m["role"] == "user" else "model"
                if m["role"] == "system":
                    role = "user"
                gemini_contents.append({"role": role, "parts": [{"text": m["content"]}]})
            payload = {
                "contents": gemini_contents,
                "generationConfig": {
                    "temperature": float(temperature),
                    "topP": float(top_p),
                    "maxOutputTokens": int(max_tokens)
                }
            }
            async with httpx.AsyncClient(timeout=60.0) as client:
                res = await client.post(url, json=payload)
                if res.status_code != 200:
                    raise RuntimeError(f"Gemini error ({res.status_code}): {res.text}")
                data = res.json()
                content = data["candidates"][0]["content"]["parts"][0]["text"]
                result["content"] = content
                usage = data.get("usageMetadata", {})
                result["completion_tokens"] = usage.get("candidatesTokenCount", 0)
                result["prompt_tokens"] = usage.get("promptTokenCount", 0)
                result["tokens"] = usage.get("totalTokenCount", 0)
                t1 = time.time()
                result["latency_ms"] = round((t1 - t0) * 1000, 1)
                result["tokens_per_sec"] = round(result["completion_tokens"] / (max(t1 - t0, 0.01)), 1)

        elif provider == "anthropic":
            key = os.environ.get("ANTHROPIC_API_KEY")
            if not key:
                raise RuntimeError("ANTHROPIC_API_KEY environment variable not set")
            url = "https://api.anthropic.com/v1/messages"
            headers = {
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            }
            sys_msg = "\n".join(m["content"] for m in messages if m.get("role") == "system")
            anthropic_msgs = [
                {"role": m["role"], "content": m["content"]}
                for m in messages if m.get("role") in ("user", "assistant")
            ]
            payload = {
                "model": model or "claude-3-5-sonnet-20241022",
                "messages": anthropic_msgs,
                "max_tokens": int(max_tokens) or 1024,
                "temperature": float(temperature)
            }
            if sys_msg:
                payload["system"] = sys_msg
            async with httpx.AsyncClient(timeout=60.0) as client:
                res = await client.post(url, json=payload, headers=headers)
                if res.status_code != 200:
                    raise RuntimeError(f"Anthropic error ({res.status_code}): {res.text}")
                data = res.json()
                content = "".join(b.get("text", "") for b in data.get("content", []))
                result["content"] = content
                usage = data.get("usage", {})
                result["completion_tokens"] = usage.get("output_tokens", 0)
                result["prompt_tokens"] = usage.get("input_tokens", 0)
                result["tokens"] = result["completion_tokens"] + result["prompt_tokens"]
                t1 = time.time()
                result["latency_ms"] = round((t1 - t0) * 1000, 1)
                result["tokens_per_sec"] = round(result["completion_tokens"] / (max(t1 - t0, 0.01)), 1)
        else:
            raise ValueError(f"Unsupported provider: {provider}")

    except Exception as e:
        result["error"] = str(e)
        result["latency_ms"] = round((time.time() - t0) * 1000, 1)
        logger.warning(f"Playground execution failed for {provider}/{model}: {e}")

    return result


async def run_comparison_prompts(
    config_a: Dict[str, Any],
    config_b: Dict[str, Any],
    rendered_prompt: str,
    raw_prompt: str,
    system_prompt: str = "",
    variables: Optional[Dict[str, str]] = None,
    user_id: str = "admin"
) -> Dict[str, Any]:
    """Executes side-by-side prompt runs concurrently using asyncio.gather and records history."""
    task_a = run_single_prompt(
        provider=config_a.get("provider", "ollama"),
        model=config_a.get("model", ""),
        prompt=rendered_prompt,
        system_prompt=system_prompt,
        temperature=config_a.get("temperature", 0.7),
        top_p=config_a.get("top_p", 0.9),
        max_tokens=config_a.get("max_tokens", 1024),
        host=config_a.get("host")
    )
    task_b = run_single_prompt(
        provider=config_b.get("provider", "ollama"),
        model=config_b.get("model", ""),
        prompt=rendered_prompt,
        system_prompt=system_prompt,
        temperature=config_b.get("temperature", 0.7),
        top_p=config_b.get("top_p", 0.9),
        max_tokens=config_b.get("max_tokens", 1024),
        host=config_b.get("host")
    )

    res_a, res_b = await asyncio.gather(task_a, task_b, return_exceptions=True)

    if isinstance(res_a, Exception):
        res_a = {"provider": config_a.get("provider"), "model": config_a.get("model"), "error": str(res_a), "content": "", "tokens": 0, "latency_ms": 0.0, "tokens_per_sec": 0.0}
    if isinstance(res_b, Exception):
        res_b = {"provider": config_b.get("provider"), "model": config_b.get("model"), "error": str(res_b), "content": "", "tokens": 0, "latency_ms": 0.0, "tokens_per_sec": 0.0}

    # Save to history
    hist_id = f"hist_{uuid.uuid4().hex[:12]}"
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    try:
        with get_playground_db() as conn:
            conn.execute("""
                INSERT INTO playground_history (
                    id, mode, model_a, provider_a, model_b, provider_b,
                    system_prompt, user_prompt, prompt_rendered, variables_json,
                    config_a, config_b, response_a, response_b,
                    latency_a_ms, latency_b_ms, tokens_a, tokens_b,
                    tps_a, tps_b, error_a, error_b, user_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                hist_id,
                "compare",
                config_a.get("model", ""),
                config_a.get("provider", ""),
                config_b.get("model", ""),
                config_b.get("provider", ""),
                system_prompt,
                raw_prompt,
                rendered_prompt,
                json.dumps(variables or {}),
                json.dumps(config_a),
                json.dumps(config_b),
                res_a.get("content", ""),
                res_b.get("content", ""),
                res_a.get("latency_ms", 0.0),
                res_b.get("latency_ms", 0.0),
                res_a.get("tokens", 0),
                res_b.get("tokens", 0),
                res_a.get("tokens_per_sec", 0.0),
                res_b.get("tokens_per_sec", 0.0),
                res_a.get("error"),
                res_b.get("error"),
                user_id or "admin",
                now
            ))
    except Exception as e:
        logger.error(f"Failed to record compare history: {e}")

    return {
        "history_id": hist_id,
        "prompt": rendered_prompt,
        "result_a": res_a,
        "result_b": res_b
    }


async def run_and_record_single(
    config: Dict[str, Any],
    rendered_prompt: str,
    raw_prompt: str,
    system_prompt: str = "",
    variables: Optional[Dict[str, str]] = None,
    user_id: str = "admin"
) -> Dict[str, Any]:
    """Executes a single prompt run and records it into history."""
    res = await run_single_prompt(
        provider=config.get("provider", "ollama"),
        model=config.get("model", ""),
        prompt=rendered_prompt,
        system_prompt=system_prompt,
        temperature=config.get("temperature", 0.7),
        top_p=config.get("top_p", 0.9),
        max_tokens=config.get("max_tokens", 1024),
        host=config.get("host")
    )

    hist_id = f"hist_{uuid.uuid4().hex[:12]}"
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    try:
        with get_playground_db() as conn:
            conn.execute("""
                INSERT INTO playground_history (
                    id, mode, model_a, provider_a,
                    system_prompt, user_prompt, prompt_rendered, variables_json,
                    config_a, response_a, latency_a_ms, tokens_a, tps_a, error_a, user_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                hist_id,
                "single",
                config.get("model", ""),
                config.get("provider", ""),
                system_prompt,
                raw_prompt,
                rendered_prompt,
                json.dumps(variables or {}),
                json.dumps(config),
                res.get("content", ""),
                res.get("latency_ms", 0.0),
                res.get("tokens", 0),
                res.get("tokens_per_sec", 0.0),
                res.get("error"),
                user_id or "admin",
                now
            ))
    except Exception as e:
        logger.error(f"Failed to record single history: {e}")

    # Automatically log MLflow GenAI Trace for Playground execution
    try:
        from web.experiments import mlflow_log_trace
        trace_req_id = f"tr_{uuid.uuid4().hex[:12]}"
        trace_sp_id = f"sp_{uuid.uuid4().hex[:10]}"
        now_ms = int(time.time() * 1000)
        dur_ms = float(res.get("latency_ms", 0.0))
        status_code = "ERROR" if res.get("error") else "OK"
        prompt_tokens = int(res.get("prompt_tokens", 0))
        comp_tokens = int(res.get("tokens", 0))
        tot_tokens = prompt_tokens + comp_tokens

        mlflow_log_trace({
            "request_id": trace_req_id,
            "experiment_id": "0",
            "name": f"playground_{config.get('model', 'prompt')}",
            "timestamp_ms": now_ms,
            "execution_time_ms": dur_ms,
            "status": status_code,
            "request": {"system_prompt": system_prompt, "prompt": rendered_prompt, "variables": variables or {}},
            "response": {"content": res.get("content", ""), "error": res.get("error")},
            "tags": {"source": "playground", "model": config.get("model", ""), "provider": config.get("provider", "")},
            "prompt_tokens": prompt_tokens,
            "completion_tokens": comp_tokens,
            "total_tokens": tot_tokens,
            "spans": [{
                "span_id": trace_sp_id,
                "request_id": trace_req_id,
                "parent_id": None,
                "name": f"{config.get('provider', 'llm')}.chat.completion",
                "span_type": "LLM",
                "start_time_ns": now_ms * 1_000_000,
                "end_time_ns": (now_ms * 1_000_000) + int(dur_ms * 1e6),
                "duration_ms": dur_ms,
                "status_code": status_code,
                "status_message": res.get("error", "") or "",
                "inputs": {"prompt": rendered_prompt, "system_prompt": system_prompt, "temperature": config.get("temperature")},
                "outputs": {"content": res.get("content", "")},
                "attributes": {
                    "model": config.get("model", ""),
                    "provider": config.get("provider", ""),
                    "usage.prompt_tokens": prompt_tokens,
                    "usage.completion_tokens": comp_tokens,
                    "usage.total_tokens": tot_tokens,
                    "tps": res.get("tokens_per_sec", 0.0)
                },
                "events": []
            }]
        })
    except Exception as trace_ex:
        logger.debug(f"Optional MLflow trace logging for playground skipped: {trace_ex}")

    return {
        "history_id": hist_id,
        "prompt": rendered_prompt,
        "result": res
    }


# ==================== TEMPLATES & HISTORY CRUD ====================

def get_templates(
    category: Optional[str] = None,
    user_id: Optional[str] = None,
    is_admin: bool = True
) -> List[Dict[str, Any]]:
    """Returns prompt templates, optionally filtered by category and user ownership."""
    with get_playground_db() as conn:
        conditions = []
        params = []
        if category and category != "All":
            conditions.append("category = ?")
            params.append(category)
        if not is_admin and user_id:
            conditions.append("(is_builtin = 1 OR user_id = ?)")
            params.append(user_id)
        
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT * FROM playground_templates {where} ORDER BY is_builtin DESC, title ASC;"
        cur = conn.execute(sql, tuple(params))
        rows = cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["variables"] = json.loads(d.get("variables") or "[]")
            except Exception:
                d["variables"] = []
            result.append(d)
        return result


def get_template_by_id(template_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves a single template by ID."""
    with get_playground_db() as conn:
        cur = conn.execute("SELECT * FROM playground_templates WHERE id = ?;", (template_id,))
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        try:
            d["variables"] = json.loads(d.get("variables") or "[]")
        except Exception:
            d["variables"] = []
        return d


def save_template(data: Dict[str, Any], user_id: str = "admin") -> Dict[str, Any]:
    """Creates or updates a prompt template."""
    tmpl_id = data.get("id") or f"tmpl_{uuid.uuid4().hex[:10]}"
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    vars_json = json.dumps(data.get("variables") or [])
    owner = data.get("user_id") or user_id or "admin"
    
    with get_playground_db() as conn:
        conn.execute("""
            INSERT INTO playground_templates (
                id, title, description, category, system_prompt, user_prompt,
                temperature, top_p, max_tokens, variables, is_builtin, user_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                description = excluded.description,
                category = excluded.category,
                system_prompt = excluded.system_prompt,
                user_prompt = excluded.user_prompt,
                temperature = excluded.temperature,
                top_p = excluded.top_p,
                max_tokens = excluded.max_tokens,
                variables = excluded.variables,
                updated_at = excluded.updated_at;
        """, (
            tmpl_id,
            data.get("title", "Untitled Prompt"),
            data.get("description", ""),
            data.get("category", "Custom"),
            data.get("system_prompt", ""),
            data.get("user_prompt", ""),
            float(data.get("temperature", 0.7)),
            float(data.get("top_p", 0.9)),
            int(data.get("max_tokens", 1024)),
            vars_json,
            int(data.get("is_builtin", 0)),
            owner,
            data.get("created_at", now),
            now
        ))
    return get_template_by_id(tmpl_id)


def delete_template(template_id: str, user_id: Optional[str] = None, is_admin: bool = True) -> bool:
    """Deletes a custom template (prevents deleting built-in starter templates, enforces user ownership)."""
    with get_playground_db() as conn:
        if is_admin or not user_id:
            cur = conn.execute("DELETE FROM playground_templates WHERE id = ? AND is_builtin = 0;", (template_id,))
        else:
            cur = conn.execute("DELETE FROM playground_templates WHERE id = ? AND is_builtin = 0 AND user_id = ?;", (template_id, user_id))
        return cur.rowcount > 0


def get_history(limit: int = 50, user_id: Optional[str] = None, is_admin: bool = True) -> List[Dict[str, Any]]:
    """Returns recent prompt execution benchmark runs, scoped by user for non-admins."""
    with get_playground_db() as conn:
        if not is_admin and user_id:
            cur = conn.execute(
                "SELECT * FROM playground_history WHERE user_id = ? ORDER BY created_at DESC LIMIT ?;",
                (user_id, limit)
            )
        elif is_admin and user_id and user_id != "all":
            cur = conn.execute(
                "SELECT * FROM playground_history WHERE user_id = ? ORDER BY created_at DESC LIMIT ?;",
                (user_id, limit)
            )
        else:
            cur = conn.execute("SELECT * FROM playground_history ORDER BY created_at DESC LIMIT ?;", (limit,))
        rows = cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            for k in ["variables_json", "config_a", "config_b"]:
                try:
                    d[k] = json.loads(d.get(k) or "{}")
                except Exception:
                    pass
            result.append(d)
        return result


def delete_history_item(hist_id: str, user_id: Optional[str] = None, is_admin: bool = True) -> bool:
    """Deletes a specific history record."""
    with get_playground_db() as conn:
        if is_admin or not user_id:
            cur = conn.execute("DELETE FROM playground_history WHERE id = ?;", (hist_id,))
        else:
            cur = conn.execute("DELETE FROM playground_history WHERE id = ? AND user_id = ?;", (hist_id, user_id))
        return cur.rowcount > 0


def clear_history(user_id: Optional[str] = None, is_admin: bool = True) -> bool:
    """Clears execution history, scoped by user for non-admins."""
    with get_playground_db() as conn:
        if is_admin and not user_id:
            conn.execute("DELETE FROM playground_history;")
        else:
            conn.execute("DELETE FROM playground_history WHERE user_id = ?;", (user_id,))
        return True


# ==================== LAKEHOUSE SCHEMA INJECTION ====================

def get_schema_tables_summary() -> List[Dict[str, Any]]:
    """
    Scans the local lakehouse delta tables and returns summary with
    column names and types for easy injection into prompt templates.
    """
    tables_summary = []
    if not os.path.exists(WAREHOUSE_DIR):
        return tables_summary

    try:
        for root, dirs, files in os.walk(WAREHOUSE_DIR):
            dirs[:] = [d for d in dirs if not d.startswith(".")]     # .metadata, .dbt (private dbt output), ...
            if "_delta_log" in dirs:
                rel = os.path.relpath(root, WAREHOUSE_DIR)
                parts = rel.split(os.sep)
                if len(parts) >= 2:
                    schema_name = parts[-2]
                    table_name = parts[-1]
                else:
                    schema_name = "dbo"
                    table_name = parts[0]

                try:
                    dt = DeltaTable(root)
                    fields = dt.schema().fields
                    # Skip deleted tombstone tables
                    if any(f.name == "__duckrun_deleted__" for f in fields):
                        continue
                    
                    columns = [{"name": f.name, "type": str(f.type)} for f in fields]
                    cols_str = ", ".join(f"{c['name']} ({c['type']})" for c in columns)
                    ddl_preview = f"CREATE TABLE {schema_name}.{table_name} (\n  " + ",\n  ".join(f"{c['name']} {c['type']}" for c in columns) + "\n);"

                    tables_summary.append({
                        "schema": schema_name,
                        "table": table_name,
                        "full_name": f"{schema_name}.{table_name}",
                        "columns": columns,
                        "columns_inline": cols_str,
                        "ddl": ddl_preview
                    })
                except Exception as e:
                    logger.debug(f"Skipping {root} in playground schema scan: {e}")
    except Exception as e:
        logger.error(f"Failed to scan schema tables for playground: {e}")

    return sorted(tables_summary, key=lambda x: x["full_name"])
