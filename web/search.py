import os
import time
import json
import sqlite3
import re
import logging
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger("search")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
NOTEBOOKS_DIR = os.getenv("NOTEBOOKS_DIR", "/workspace/notebooks")
DASHBOARDS_FILE = os.path.join(METADATA_DIR, "dashboards.json")
JOBS_FILE = os.path.join(METADATA_DIR, "jobs.json")
SQL_WAREHOUSES_FILE = os.path.join(METADATA_DIR, "sql_warehouses.json")
SAVED_QUERIES_FILE = os.path.join(METADATA_DIR, "saved_queries.json")
HISTORY_DB_PATH = os.path.join(METADATA_DIR, "history.db")

# In-memory cache for catalog metadata to ensure sub-5ms keystroke search
_CATALOG_CACHE: Dict[str, Any] = {"data": None, "cached_at": 0.0}
CACHE_TTL_SECONDS = 5.0


def score_text_match(query: str, text: str) -> float:
    """
    Computes a relevance score from 0.0 to 100.0 based on fuzzy and prefix matching.
    """
    if not query or not text:
        return 0.0
    q = query.lower().strip()
    t = text.lower().strip()

    if q == t:
        return 100.0
    if t.startswith(q):
        return 90.0
    
    # Word boundary / token match (e.g., searching 'emp' matches 'silver_employees')
    tokens = re.split(r"[_\s\.\-:/]+", t)
    for tok in tokens:
        if tok.startswith(q):
            return 85.0
        if tok == q:
            return 95.0

    # Substring match
    idx = t.find(q)
    if idx != -1:
        # Closer to start of string gets higher score
        pos_penalty = min(idx * 0.5, 20.0)
        return max(50.0, 75.0 - pos_penalty)

    # Subsequence / fuzzy match (all query characters appear in order)
    q_idx = 0
    q_len = len(q)
    for char in t:
        if char == q[q_idx]:
            q_idx += 1
            if q_idx == q_len:
                return 40.0

    return 0.0


def get_cached_catalogs() -> List[Dict[str, Any]]:
    """Fetches catalogs and tables using a short-lived cache."""
    now = time.time()
    if _CATALOG_CACHE["data"] is not None and (now - _CATALOG_CACHE["cached_at"]) < CACHE_TTL_SECONDS:
        return _CATALOG_CACHE["data"]

    try:
        from web.warehouses import scan_all_catalogs_and_tables
        res = scan_all_catalogs_and_tables()
        catalogs = res.get("catalogs", [])
        _CATALOG_CACHE["data"] = catalogs
        _CATALOG_CACHE["cached_at"] = now
        return catalogs
    except Exception as e:
        logger.warning(f"Error fetching catalog metadata for search: {e}")
        return _CATALOG_CACHE["data"] or []


def invalidate_search_cache():
    """Invalidates search metadata cache when catalog changes."""
    _CATALOG_CACHE["data"] = None
    _CATALOG_CACHE["cached_at"] = 0.0


def search_tables_and_catalogs(query: str, catalogs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results = []
    is_empty_q = not query.strip()

    for cat in catalogs:
        cat_id = cat.get("id", "warehouse")
        cat_name = cat.get("name", cat_id)

        # Catalog match
        if not is_empty_q:
            cat_score = max(score_text_match(query, cat_id), score_text_match(query, cat_name))
            if cat_score >= 50.0:
                results.append({
                    "id": f"cat_{cat_id}",
                    "category": "catalogs",
                    "title": cat_name,
                    "subtitle": f"Catalog '{cat_id}' • {len(cat.get('schemas', []))} schemas",
                    "badge": "CATALOG",
                    "badge_color": "amber",
                    "icon": "ph-database",
                    "score": cat_score + 5.0,
                    "meta": {
                        "catalog": cat_id,
                        "action_type": "view_catalog"
                    }
                })

        for s in cat.get("schemas", []):
            schema_name = s.get("name", "dbo")
            for t in s.get("tables", []):
                t_name = t.get("name", "")
                full_name = f"{cat_id}.{schema_name}.{t_name}" if cat_id != "warehouse" else f"{schema_name}.{t_name}"
                loc = t.get("location", "")

                if is_empty_q:
                    score = 70.0
                else:
                    name_score = score_text_match(query, t_name)
                    full_score = score_text_match(query, full_name)
                    schema_score = score_text_match(query, schema_name)
                    score = max(name_score, full_score, schema_score * 0.7)

                if score >= 40.0 or is_empty_q:
                    results.append({
                        "id": f"tbl_{cat_id}_{schema_name}_{t_name}",
                        "category": "tables",
                        "title": t_name,
                        "subtitle": full_name,
                        "badge": "DELTA",
                        "badge_color": "emerald",
                        "icon": "ph-table",
                        "score": score + 10.0,
                        "meta": {
                            "catalog": cat_id,
                            "schema_name": schema_name,
                            "table_name": t_name,
                            "full_name": full_name,
                            "location": loc,
                            "action_type": "view_table"
                        }
                    })

    return results


_TABLE_COLUMNS_CACHE: Dict[Tuple[str, int], List[Dict[str, str]]] = {}

def get_table_columns(path: str, version: int = 0) -> List[Dict[str, str]]:
    key = (path, version)
    if key in _TABLE_COLUMNS_CACHE:
        return _TABLE_COLUMNS_CACHE[key]
    if not path or not os.path.exists(path):
        return []
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(path)
        fields = [f.name for f in dt.schema().fields]
        if fields == ["__duckrun_deleted__"] or "__duckrun_deleted__" in fields:
            return []
        cols = []
        for f in dt.schema().fields:
            type_str = str(f.type)
            clean_type = type_str.replace('PrimitiveType("', '').replace('")', '').upper()
            cols.append({"name": f.name, "type": clean_type})
        _TABLE_COLUMNS_CACHE[key] = cols
        return cols
    except Exception:
        return []


def search_columns(query: str, catalogs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not query.strip():
        return []

    results = []
    seen_cols = set()

    for cat in catalogs:
        cat_id = cat.get("id", "warehouse")
        for s in cat.get("schemas", []):
            schema_name = s.get("name", "dbo")
            for t in s.get("tables", []):
                t_name = t.get("name", "")
                t_path = t.get("path", "")
                t_ver = t.get("version", 0)
                full_name = f"{cat_id}.{schema_name}.{t_name}" if cat_id != "warehouse" else f"{schema_name}.{t_name}"

                columns = get_table_columns(t_path, t_ver)
                for col in columns:
                    col_name = col.get("name", "")
                    col_type = col.get("type", "UNKNOWN")
                    col_key = f"{cat_id}.{schema_name}.{t_name}.{col_name}"
                    if col_key in seen_cols:
                        continue

                    col_score = score_text_match(query, col_name)
                    type_score = score_text_match(query, col_type)
                    score = max(col_score, type_score * 0.6)

                    if score >= 40.0:
                        seen_cols.add(col_key)
                        results.append({
                            "id": f"col_{col_key}",
                            "category": "columns",
                            "title": col_name,
                            "subtitle": f"{col_type} in {full_name}",
                            "badge": col_type.split("(")[0],
                            "badge_color": "sky",
                            "icon": "ph-columns",
                            "score": score + 5.0,
                            "meta": {
                                "catalog": cat_id,
                                "schema_name": schema_name,
                                "table_name": t_name,
                                "column_name": col_name,
                                "column_type": col_type,
                                "action_type": "view_column"
                            }
                        })

    return results


def search_notebooks(query: str) -> List[Dict[str, Any]]:
    results = []
    is_empty_q = not query.strip()
    nb_dir = NOTEBOOKS_DIR

    if not os.path.exists(nb_dir):
        alt = os.path.join(os.path.dirname(__file__), "..", "notebooks")
        if os.path.exists(alt):
            nb_dir = alt

    if not os.path.exists(nb_dir):
        return results

    try:
        for root, dirs, files in os.walk(nb_dir):
            dirs[:] = [d for d in dirs if not d.startswith(".") and d != "__pycache__"]
            for f in files:
                if f.endswith(".ipynb") and not f.startswith("."):
                    full_path = os.path.join(root, f)
                    rel_path = os.path.relpath(full_path, nb_dir)
                    title = f

                    if is_empty_q:
                        score = 65.0
                    else:
                        score = max(score_text_match(query, f), score_text_match(query, rel_path))

                    if score >= 40.0 or is_empty_q:
                        results.append({
                            "id": f"nb_{rel_path}",
                            "category": "notebooks",
                            "title": title,
                            "subtitle": f"notebooks/{rel_path}",
                            "badge": "JUPYTER",
                            "badge_color": "amber",
                            "icon": "ph-notebook",
                            "score": score + 8.0,
                            "meta": {
                                "filename": f,
                                "path": rel_path,
                                "action_type": "open_notebook"
                            }
                        })
    except Exception as e:
        logger.warning(f"Error reading notebooks directory: {e}")

    return results


def search_dashboards(query: str) -> List[Dict[str, Any]]:
    results = []
    if not os.path.exists(DASHBOARDS_FILE):
        return results

    is_empty_q = not query.strip()
    try:
        with open(DASHBOARDS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            dashboards = data.get("dashboards", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

        for d in dashboards:
            d_id = d.get("id", "")
            name = d.get("name", "Untitled Dashboard")
            desc = d.get("description", "")
            widgets = d.get("widgets", [])

            if is_empty_q:
                score = 60.0
            else:
                score = max(score_text_match(query, name), score_text_match(query, desc) * 0.8)
                for w in widgets:
                    w_title = w.get("title", "")
                    w_score = score_text_match(query, w_title)
                    if w_score > score:
                        score = w_score * 0.9

            if score >= 40.0 or is_empty_q:
                results.append({
                    "id": f"dash_{d_id}",
                    "category": "dashboards",
                    "title": name,
                    "subtitle": desc or f"{len(widgets)} widgets configured",
                    "badge": f"{len(widgets)} WIDGETS",
                    "badge_color": "indigo",
                    "icon": "ph-chart-line-up",
                    "score": score + 6.0,
                    "meta": {
                        "dashboard_id": d_id,
                        "dashboard_name": name,
                        "action_type": "open_dashboard"
                    }
                })
    except Exception as e:
        logger.warning(f"Error searching dashboards: {e}")

    return results


def search_jobs(query: str) -> List[Dict[str, Any]]:
    results = []
    if not os.path.exists(JOBS_FILE):
        return results

    is_empty_q = not query.strip()
    try:
        with open(JOBS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            jobs = data.get("jobs", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

        for j in jobs:
            j_id = j.get("id", "")
            name = j.get("name", "Untitled Job")
            desc = j.get("description", "")
            cron = j.get("schedule_cron", "")
            tasks = j.get("tasks", [])

            if is_empty_q:
                score = 60.0
            else:
                score = max(score_text_match(query, name), score_text_match(query, desc) * 0.8)
                for t in tasks:
                    t_id = t.get("id", "")
                    t_type = t.get("type", "")
                    t_score = max(score_text_match(query, t_id), score_text_match(query, t_type))
                    if t_score > score:
                        score = t_score * 0.85

            if score >= 40.0 or is_empty_q:
                results.append({
                    "id": f"job_{j_id}",
                    "category": "jobs",
                    "title": name,
                    "subtitle": f"{len(tasks)} tasks • {cron if cron else 'Manual execution'}",
                    "badge": "WORKFLOW",
                    "badge_color": "rose",
                    "icon": "ph-git-merge",
                    "score": score + 6.0,
                    "meta": {
                        "job_id": j_id,
                        "job_name": name,
                        "action_type": "open_job"
                    }
                })
    except Exception as e:
        logger.warning(f"Error searching jobs: {e}")

    return results


def search_sql_warehouses(query: str) -> List[Dict[str, Any]]:
    results = []
    if not os.path.exists(SQL_WAREHOUSES_FILE):
        return results

    is_empty_q = not query.strip()
    try:
        with open(SQL_WAREHOUSES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            warehouses = data.get("warehouses", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

        for wh in warehouses:
            wh_id = wh.get("id", "")
            name = wh.get("name", "")
            csize = wh.get("cluster_size", "Small")
            threads = wh.get("threads", 2)
            max_mem = wh.get("max_memory", "4GB")
            state = wh.get("state", "STOPPED")

            if is_empty_q:
                score = 55.0
            else:
                score = max(score_text_match(query, name), score_text_match(query, csize), score_text_match(query, wh_id))

            if score >= 40.0 or is_empty_q:
                results.append({
                    "id": f"wh_{wh_id}",
                    "category": "warehouses",
                    "title": name,
                    "subtitle": f"{csize} ({threads} vCPU, {max_mem}) • {state}",
                    "badge": state,
                    "badge_color": "emerald" if state == "RUNNING" else "slate",
                    "icon": "ph-hard-drives",
                    "score": score + 4.0,
                    "meta": {
                        "warehouse_id": wh_id,
                        "action_type": "view_warehouse"
                    }
                })
    except Exception as e:
        logger.warning(f"Error searching SQL warehouses: {e}")

    return results


def search_saved_queries(query: str) -> List[Dict[str, Any]]:
    results = []
    sq_file = SAVED_QUERIES_FILE
    if not os.path.exists(sq_file):
        alt = os.path.join(os.path.dirname(__file__), "..", "warehouse", ".metadata", "saved_queries.json")
        if os.path.exists(alt):
            sq_file = alt

    if not os.path.exists(sq_file):
        return results

    is_empty_q = not query.strip()
    try:
        with open(sq_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            queries = data.get("queries", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

        for q in queries:
            q_id = q.get("id", "")
            name = q.get("name", "Untitled Query")
            desc = q.get("description", "")
            q_text = q.get("query_text", "")
            tags = q.get("tags", [])
            tags_str = ", ".join(tags) if tags else ""
            wh_id = q.get("warehouse_id", "wh_starter")
            runs = q.get("run_count", 0)

            if is_empty_q:
                score = 75.0
            else:
                score = max(
                    score_text_match(query, name) * 1.1,
                    score_text_match(query, tags_str) * 1.0,
                    score_text_match(query, desc) * 0.8,
                    score_text_match(query, q_text) * 0.75
                )

            if score >= 38.0 or is_empty_q:
                results.append({
                    "id": f"saved_qry_{q_id}",
                    "category": "queries",
                    "title": name,
                    "subtitle": f"{tags_str + ' • ' if tags_str else ''}{wh_id} • {runs} runs",
                    "badge": "SAVED",
                    "badge_color": "amber",
                    "icon": "ph-scroll",
                    "score": score + 12.0,
                    "meta": {
                        "saved_query_id": q_id,
                        "name": name,
                        "description": desc,
                        "query_text": q_text,
                        "warehouse_id": wh_id,
                        "catalog": q.get("catalog", "warehouse"),
                        "schema_name": q.get("schema_name", "dbo"),
                        "tags": tags,
                        "action_type": "open_saved_query"
                    }
                })
    except Exception as e:
        logger.warning(f"Error searching saved queries: {e}")

    return results


def search_query_history(query: str, limit: int = 5) -> List[Dict[str, Any]]:
    results = []
    if not os.path.exists(HISTORY_DB_PATH):
        return results

    is_empty_q = not query.strip()
    try:
        conn = sqlite3.connect(HISTORY_DB_PATH)
        conn.row_factory = sqlite3.Row
        
        if is_empty_q:
            sql = "SELECT query_id, query_text, executed_at, duration_ms, status, client, warehouse_id FROM query_history ORDER BY executed_at DESC LIMIT ?"
            params = [limit]
        else:
            sql = """
                SELECT query_id, query_text, executed_at, duration_ms, status, client, warehouse_id 
                FROM query_history 
                WHERE query_text LIKE ? 
                ORDER BY executed_at DESC 
                LIMIT ?
            """
            params = [f"%{query.strip()}%", limit]

        rows = conn.execute(sql, params).fetchall()
        for r in rows:
            q_text = (r["query_text"] or "").strip()
            first_line = q_text.split("\n")[0][:90]
            if len(first_line) < len(q_text):
                first_line += "..."
            
            score = 65.0 if is_empty_q else (score_text_match(query, q_text) + 2.0)
            status = r["status"] or "SUCCESS"
            wh_id = r["warehouse_id"] or "wh_starter"

            results.append({
                "id": f"qry_{r['query_id']}",
                "category": "queries",
                "title": first_line,
                "subtitle": f"{status} • {r['duration_ms']}ms • {r['executed_at']} ({wh_id})",
                "badge": "HISTORY",
                "badge_color": "purple",
                "icon": "ph-clock-counter-clockwise",
                "score": score,
                "meta": {
                    "query_id": r["query_id"],
                    "query_text": q_text,
                    "action_type": "open_query"
                }
            })
        conn.close()
    except Exception as e:
        logger.warning(f"Error searching query history: {e}")

    return results


def search_experiments(query: str) -> List[Dict[str, Any]]:
    results = []
    try:
        from web.experiments import get_exp_db
        with get_exp_db() as conn:
            # Search experiments
            rows = conn.execute("SELECT experiment_id, name, created_at FROM experiments WHERE lifecycle_stage != 'deleted'").fetchall()
            for r in rows:
                score = score_text_match(query, r["name"])
                if score > 0:
                    results.append({
                        "id": f"exp_{r['experiment_id']}",
                        "category": "experiments",
                        "title": r["name"],
                        "subtitle": f"MLflow Experiment • Created {r['created_at']}",
                        "badge": "EXPERIMENT",
                        "badge_color": "purple",
                        "icon": "ph-flask",
                        "score": score + 5,
                        "meta": {
                            "experiment_id": r["experiment_id"],
                            "name": r["name"],
                            "action_type": "open_experiment"
                        }
                    })
            # Search runs
            run_rows = conn.execute("""
                SELECT r.run_id, r.run_name, r.status, r.experiment_id, e.name as experiment_name
                FROM runs r
                JOIN experiments e ON r.experiment_id = e.experiment_id
                WHERE r.lifecycle_stage != 'deleted'
                LIMIT 50
            """).fetchall()
            for r in run_rows:
                score = score_text_match(query, r["run_name"])
                if score > 0:
                    results.append({
                        "id": f"run_{r['run_id']}",
                        "category": "experiments",
                        "title": r["run_name"],
                        "subtitle": f"Run in {r['experiment_name']} • {r['status']}",
                        "badge": "ML_RUN",
                        "badge_color": "emerald" if r["status"] == "FINISHED" else "blue",
                        "icon": "ph-chart-line-up",
                        "score": score,
                        "meta": {
                            "run_id": r["run_id"],
                            "experiment_id": r["experiment_id"],
                            "action_type": "open_experiment"
                        }
                    })
    except Exception as e:
        logger.warning(f"Error searching experiments: {e}")
    return sorted(results, key=lambda x: x["score"], reverse=True)


DOCS_TOPICS = [
    {
        "id": "doc_arch",
        "title": "Architecture & Engine Overview",
        "subtitle": "Zero-JVM, in-process DuckDB, Ray distributed compute, Delta Lake transaction log",
        "badge": "DOCS",
        "badge_color": "amber",
        "icon": "ph-cpu",
        "url": "/docs/#overview-architecture",
        "keywords": ["architecture", "overview", "engine", "duckdb", "ray", "delta lake", "jvm", "parquet", "vectorized", "zero jvm"]
    },
    {
        "id": "doc_tutorial",
        "title": "Hands-on Tutorial: Bronze, Silver & Gold Medallion",
        "subtitle": "End-to-end lakehouse pipeline from raw ingestion to Delta KPI tables",
        "badge": "TUTORIAL",
        "badge_color": "emerald",
        "icon": "ph-graduation-cap",
        "url": "/docs/#hands-on-tutorial",
        "keywords": ["tutorial", "hands on", "guide", "walkthrough", "step by step", "bronze", "silver", "gold", "medallion", "etl"]
    },
    {
        "id": "doc_sql_bi",
        "title": "SQL Editor & BI Warehouses",
        "subtitle": "ACID transactional queries, time travel snapshots, and Lakeview dashboards",
        "badge": "DOCS",
        "badge_color": "blue",
        "icon": "ph-terminal-window",
        "url": "/docs/#sql-bi",
        "keywords": ["sql", "editor", "warehouse", "queries", "bi", "dashboards", "time travel", "duckdb sql", "analytics"]
    },
    {
        "id": "doc_de",
        "title": "Data Engineering & Pipeline Workflows",
        "subtitle": "dbt Core orchestration, multi-task cron DAGs, and lineage tracing",
        "badge": "DOCS",
        "badge_color": "teal",
        "icon": "ph-git-merge",
        "url": "/docs/#data-engineering",
        "keywords": ["data engineering", "pipeline", "jobs", "workflows", "dbt", "cron", "orchestration", "lineage", "dag"]
    },
    {
        "id": "doc_ml",
        "title": "Machine Learning & AI Spaces",
        "subtitle": "MLflow tracking parity, Unity Catalog model registry, Prompt Playground, Genie Space",
        "badge": "DOCS",
        "badge_color": "purple",
        "icon": "ph-flask",
        "url": "/docs/#machine-learning",
        "keywords": ["machine learning", "mlflow", "ai", "genie", "playground", "experiments", "models", "registry", "llm", "prompt"]
    },
    {
        "id": "doc_docker",
        "title": "Docker Local Installation & Setup",
        "subtitle": "Step-by-step instructions to run DataKilnWorks locally with Docker Compose",
        "badge": "INSTALL",
        "badge_color": "sky",
        "icon": "ph-cube",
        "url": "/docs/#docker-local-install",
        "keywords": ["docker", "install", "installation", "setup", "compose", "container", "local", "ports", "environment"]
    },
    {
        "id": "doc_k8s",
        "title": "Kubernetes & Production Deployment",
        "subtitle": "StatefulSets, persistent volume claims, Helm charts, and ingress routing",
        "badge": "DEPLOY",
        "badge_color": "indigo",
        "icon": "ph-cloud",
        "url": "/docs/#k8s-install",
        "keywords": ["kubernetes", "k8s", "helm", "production", "deploy", "deployment", "statefulset", "pvc", "cloud"]
    },
    {
        "id": "doc_api",
        "title": "REST API Reference & SDK",
        "subtitle": "FastAPI endpoints, token auth, SQL query execution, and compute workers",
        "badge": "API",
        "badge_color": "slate",
        "icon": "ph-code",
        "url": "/docs/#api-reference",
        "keywords": ["api", "rest", "endpoints", "sdk", "swagger", "openapi", "auth", "token", "curl"]
    },
    {
        "id": "doc_troubleshoot",
        "title": "Troubleshooting & FAQ",
        "subtitle": "Memory limits, Delta log concurrency, worker connectivity, and common questions",
        "badge": "FAQ",
        "badge_color": "rose",
        "icon": "ph-question",
        "url": "/docs/#troubleshooting",
        "keywords": ["troubleshooting", "faq", "error", "debug", "memory", "concurrency", "lock", "help", "manual"]
    }
]

def search_documentation(query: str) -> List[Dict[str, Any]]:
    if not query:
        # If empty query, return top guide topics
        return [
            {
                "id": doc["id"],
                "category": "docs",
                "title": doc["title"],
                "subtitle": doc["subtitle"],
                "badge": doc["badge"],
                "badge_color": doc["badge_color"],
                "icon": doc["icon"],
                "score": 50.0,
                "meta": {
                    "url": doc["url"],
                    "action_type": "open_doc",
                    "title": doc["title"]
                }
            }
            for doc in DOCS_TOPICS[:4]
        ]
    clean_q = query.lower().strip()
    results = []
    for doc in DOCS_TOPICS:
        best_score = score_text_match(clean_q, doc["title"])
        sub_score = score_text_match(clean_q, doc["subtitle"])
        best_score = max(best_score, sub_score * 0.8)
        for kw in doc.get("keywords", []):
            kw_score = score_text_match(clean_q, kw)
            if kw_score > best_score:
                best_score = kw_score
        if best_score > 40:
            results.append({
                "id": doc["id"],
                "category": "docs",
                "title": doc["title"],
                "subtitle": doc["subtitle"],
                "badge": doc["badge"],
                "badge_color": doc["badge_color"],
                "icon": doc["icon"],
                "score": best_score + 10,
                "meta": {
                    "url": doc["url"],
                    "action_type": "open_doc",
                    "title": doc["title"]
                }
            })
    return sorted(results, key=lambda x: x["score"], reverse=True)


def universal_search(query: str = "", category: str = "ALL", limit: int = 25) -> Dict[str, Any]:
    """
    Executes a high-speed unified fuzzy search across all lakehouse assets.
    """
    t0 = time.perf_counter()
    clean_q = query.strip()
    cat_upper = category.upper()

    catalogs = get_cached_catalogs()
    all_results: List[Dict[str, Any]] = []

    # Category counters
    counts = {
        "all": 0,
        "tables": 0,
        "columns": 0,
        "notebooks": 0,
        "queries": 0,
        "dashboards": 0,
        "jobs": 0,
        "warehouses": 0,
        "experiments": 0,
        "docs": 0
    }

    # 1. Search Tables & Catalogs
    if cat_upper in ("ALL", "TABLES"):
        tbl_results = search_tables_and_catalogs(clean_q, catalogs)
        counts["tables"] = len(tbl_results)
        all_results.extend(tbl_results)

    # 2. Search Columns
    if cat_upper in ("ALL", "COLUMNS"):
        col_results = search_columns(clean_q, catalogs)
        counts["columns"] = len(col_results)
        all_results.extend(col_results)

    # 3. Search Notebooks
    if cat_upper in ("ALL", "NOTEBOOKS"):
        nb_results = search_notebooks(clean_q)
        counts["notebooks"] = len(nb_results)
        all_results.extend(nb_results)

    # 4. Search Dashboards
    if cat_upper in ("ALL", "DASHBOARDS"):
        dash_results = search_dashboards(clean_q)
        counts["dashboards"] = len(dash_results)
        all_results.extend(dash_results)

    # 5. Search Jobs & Pipelines
    if cat_upper in ("ALL", "JOBS", "WORKFLOWS"):
        job_results = search_jobs(clean_q)
        counts["jobs"] = len(job_results)
        all_results.extend(job_results)

    # 6. Search SQL Warehouses
    if cat_upper in ("ALL", "WAREHOUSES", "COMPUTE"):
        wh_results = search_sql_warehouses(clean_q)
        counts["warehouses"] = len(wh_results)
        all_results.extend(wh_results)

    # 7. Search Queries (Saved Queries + Query History)
    if cat_upper in ("ALL", "QUERIES"):
        sq_results = search_saved_queries(clean_q)
        qry_limit = 10 if cat_upper == "QUERIES" else 5
        qry_results = search_query_history(clean_q, limit=qry_limit)
        combined_queries = sq_results + qry_results
        counts["queries"] = len(combined_queries)
        all_results.extend(combined_queries)

    # 8. Search Experiments & ML Runs
    if cat_upper in ("ALL", "EXPERIMENTS", "ML"):
        exp_results = search_experiments(clean_q)
        counts["experiments"] = len(exp_results)
        all_results.extend(exp_results)

    # 9. Search Documentation & Manual
    if cat_upper in ("ALL", "DOCS", "HELP", "MANUAL"):
        doc_results = search_documentation(clean_q)
        counts["docs"] = len(doc_results)
        all_results.extend(doc_results)

    # Sort all results by score descending
    all_results.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    counts["all"] = len(all_results)

    # Slice to requested limit
    ranked = all_results[:limit]
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)

    return {
        "query": clean_q,
        "category": cat_upper,
        "total_matches": counts["all"],
        "category_counts": counts,
        "results": ranked,
        "elapsed_ms": elapsed_ms
    }
