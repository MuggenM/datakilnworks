"""
Automated Data Lineage Engine for Localspark Unity Catalog.
Extracts end-to-end data pipelines and table dependencies using sqlglot AST parsing across
Query History, Workflow DAGs, Lakeview Dashboards, and Data Ingestion events.
Stores the lineage graph in an embedded SQLite repository (/workspace/warehouse/.metadata/lineage.db).
"""

import os
import re
import time
import json
import sqlite3
import logging
import datetime
from typing import Dict, Any, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp

logger = logging.getLogger("localspark.lineage")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
if not os.path.exists(WAREHOUSE_DIR):
    local_alt = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "warehouse"))
    if os.path.exists(local_alt):
        WAREHOUSE_DIR = local_alt

METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
DB_PATH = os.path.join(METADATA_DIR, "lineage.db")


def get_db_connection() -> sqlite3.Connection:
    """Returns a SQLite connection for lineage storage."""
    os.makedirs(METADATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_lineage_db():
    """Initializes the lineage graph tables if they do not exist."""
    try:
        with get_db_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lineage_nodes (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    node_type TEXT NOT NULL,
                    layer TEXT NOT NULL,
                    catalog TEXT DEFAULT 'warehouse',
                    schema_name TEXT DEFAULT 'dbo',
                    metadata TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lineage_edges (
                    id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    edge_type TEXT NOT NULL,
                    query_text TEXT,
                    job_id TEXT,
                    last_executed_at TEXT NOT NULL,
                    UNIQUE(source_id, target_id, edge_type)
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lineage_source ON lineage_edges(source_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lineage_target ON lineage_edges(target_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lineage_layer ON lineage_nodes(layer);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lineage_type ON lineage_nodes(node_type);")
    except Exception as e:
        logger.error(f"Failed to initialize lineage database: {e}")


# Initialize on module load
init_lineage_db()


def infer_medallion_layer(name: str, node_type: str = "TABLE") -> str:
    """Infers the Medallion architectural layer from entity naming conventions."""
    name_lower = name.lower()
    if node_type == "FILE":
        return "RAW_FILE"
    elif node_type == "DASHBOARD":
        return "PRESENTATION"
    elif node_type == "QUERY":
        return "ANALYTICS"
    elif node_type == "JOB":
        return "ORCHESTRATION"
    elif node_type == "MODEL":
        return "MODEL"

    if "gold" in name_lower:
        return "GOLD"
    elif "silver" in name_lower:
        return "SILVER"
    elif "bronze" in name_lower:
        return "BRONZE"
    elif any(k in name_lower for k in ["kpi", "metric", "summary", "agg", "report", "fact", "dim"]):
        return "GOLD"
    elif any(k in name_lower for k in ["clean", "enrich", "stg", "staging", "employee", "ticker"]):
        return "SILVER"
    elif any(k in name_lower for k in ["raw", "landing", "telemetry", "ingest"]):
        return "BRONZE"
    return "SILVER"


def make_table_id(catalog: str, schema_name: str, table_name: str) -> str:
    """Generates standard unique node ID for a table."""
    c = catalog or "warehouse"
    s = schema_name or "dbo"
    t = table_name.strip()
    return f"table:{c}.{s}.{t}"


def parse_sql_lineage(sql_text: str) -> Tuple[Optional[str], Set[str], Set[str]]:
    """
    Parses a SQL query using sqlglot to extract target table, source tables, and source files.
    Returns (target_table, source_tables, source_files).
    """
    if not sql_text or not sql_text.strip():
        return None, set(), set()

    clean_sql = sql_text.strip()
    target_table: Optional[str] = None
    source_tables: Set[str] = set()
    source_files: Set[str] = set()

    try:
        parsed = sqlglot.parse_one(clean_sql, read="duckdb")
    except Exception:
        try:
            parsed = sqlglot.parse_one(clean_sql)
        except Exception:
            # Fallback regex parsing if syntax is unconventional
            return fallback_regex_parse(clean_sql)

    # 1. Determine Target Table
    if isinstance(parsed, (exp.Create, exp.Insert, exp.Update)):
        if parsed.this:
            if isinstance(parsed.this, exp.Schema):
                target_table = parsed.this.this.sql()
            else:
                target_table = parsed.this.sql()
    elif isinstance(parsed, exp.Merge):
        if parsed.this:
            target_table = parsed.this.sql()

    # Clean target name
    if target_table:
        target_table = target_table.replace('"', '').replace("'", "").strip()

    # 2. Extract CTE names to exclude them from external source tables
    cte_names = set()
    for cte in parsed.find_all(exp.CTE):
        if cte.alias:
            cte_names.add(cte.alias.replace('"', '').strip().lower())

    # 3. Extract Source Tables
    for table_expr in parsed.find_all(exp.Table):
        name = table_expr.name
        db = table_expr.db
        if not name:
            continue
        clean_name = name.replace('"', '').replace("'", "").strip()
        if clean_name.lower() in cte_names:
            continue

        full_name = f"{db}.{clean_name}" if db else clean_name
        if target_table and (full_name == target_table or clean_name == target_table):
            continue
        source_tables.add(full_name)

    # 4. Extract Source Files (e.g. read_csv, read_parquet, range)
    for func in parsed.find_all(exp.Anonymous):
        fname = func.name.lower()
        if fname in ["read_csv", "read_parquet", "read_json", "read_csv_auto", "sniff_csv"]:
            for arg in func.expressions:
                arg_sql = arg.sql().replace("'", "").replace('"', '').strip()
                if arg_sql:
                    source_files.add(os.path.basename(arg_sql))
        elif fname == "range":
            source_files.add("generator:synthetic_range")

    return target_table, source_tables, source_files


def fallback_regex_parse(sql_text: str) -> Tuple[Optional[str], Set[str], Set[str]]:
    """Simple regex fallback when sqlglot encounters unknown dialect extensions."""
    target = None
    sources = set()
    files = set()

    create_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([a-zA-Z0-9_\.]+)', sql_text, re.IGNORECASE)
    if create_match:
        target = create_match.group(1)

    insert_match = re.search(r'INSERT\s+INTO\s+([a-zA-Z0-9_\.]+)', sql_text, re.IGNORECASE)
    if insert_match and not target:
        target = insert_match.group(1)

    from_matches = re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z0-9_\.]+)', sql_text, re.IGNORECASE)
    for m in from_matches:
        if m.upper() not in ["SELECT", "WHERE", "GROUP", "ORDER", "RANGE"]:
            if m != target:
                sources.add(m)

    file_matches = re.findall(r'read_(?:csv|parquet|json)\s*\(\s*[\'"]([^\'"]+)[\'"]', sql_text, re.IGNORECASE)
    for f in file_matches:
        files.add(os.path.basename(f))

    if "range(" in sql_text.lower():
        files.add("generator:synthetic_range")

    return target, sources, files


def upsert_node(
    node_id: str,
    name: str,
    node_type: str,
    layer: Optional[str] = None,
    catalog: str = "warehouse",
    schema_name: str = "dbo",
    metadata: Optional[Dict[str, Any]] = None
):
    """Inserts or updates a node in the lineage repository."""
    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    inferred_layer = layer or infer_medallion_layer(name, node_type)
    meta_json = json.dumps(metadata or {})

    with get_db_connection() as conn:
        conn.execute("""
            INSERT INTO lineage_nodes (id, name, node_type, layer, catalog, schema_name, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                node_type=excluded.node_type,
                layer=excluded.layer,
                metadata=excluded.metadata,
                updated_at=excluded.updated_at
        """, (node_id, name, node_type, inferred_layer, catalog, schema_name, meta_json, now_iso, now_iso))


def upsert_edge(
    source_id: str,
    target_id: str,
    edge_type: str = "TRANSFORMS_TO",
    query_text: Optional[str] = None,
    job_id: Optional[str] = None
):
    """Inserts or updates a directed edge in the lineage repository."""
    if source_id == target_id:
        return

    edge_id = f"{source_id}->{target_id}:{edge_type}"
    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    with get_db_connection() as conn:
        conn.execute("""
            INSERT INTO lineage_edges (id, source_id, target_id, edge_type, query_text, job_id, last_executed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id, target_id, edge_type) DO UPDATE SET
                query_text=COALESCE(excluded.query_text, lineage_edges.query_text),
                job_id=COALESCE(excluded.job_id, lineage_edges.job_id),
                last_executed_at=excluded.last_executed_at
        """, (edge_id, source_id, target_id, edge_type, query_text, job_id, now_iso))


def record_query_lineage(sql_text: str, client: str = "SQL_EDITOR", job_id: Optional[str] = None):
    """Parses a query dynamically executed in the studio and updates the lineage graph."""
    if not sql_text or not sql_text.strip():
        return

    target, sources, files = parse_sql_lineage(sql_text)

    # If this query creates/modifies a table
    if target:
        target_clean = target.split(".")[-1]
        target_schema = target.split(".")[-2] if "." in target else "dbo"
        target_id = make_table_id("warehouse", target_schema, target_clean)
        upsert_node(target_id, target_clean, "TABLE", catalog="warehouse", schema_name=target_schema)

        # Connect source files -> target
        for f in files:
            file_id = f"file:{f}"
            upsert_node(file_id, f, "FILE", layer="RAW_FILE")
            upsert_edge(file_id, target_id, edge_type="INGESTS_TO", query_text=sql_text, job_id=job_id)

        # Connect source tables -> target
        for s in sources:
            src_clean = s.split(".")[-1]
            src_schema = s.split(".")[-2] if "." in s else "dbo"
            src_id = make_table_id("warehouse", src_schema, src_clean)
            upsert_node(src_id, src_clean, "TABLE", catalog="warehouse", schema_name=src_schema)
            upsert_edge(src_id, target_id, edge_type="TRANSFORMS_TO", query_text=sql_text, job_id=job_id)


def scan_and_sync_all_assets():
    """Comprehensive asset scanner to build full Lakehouse lineage from all system metadata."""
    logger.info("Starting comprehensive Lakehouse data lineage scan...")

    with get_db_connection() as conn:
        conn.execute("DELETE FROM lineage_edges")
        conn.execute("DELETE FROM lineage_nodes")

    # 1. Scan Unity Catalog tables
    try:
        from web.warehouses import scan_all_catalogs_and_tables
        catalog_data = scan_all_catalogs_and_tables()
        catalogs = catalog_data.get("catalogs", []) if isinstance(catalog_data, dict) else catalog_data
        for cat in catalogs:
            cat_id = cat.get("id", "warehouse")
            for schema in cat.get("schemas", []):
                schema_name = schema.get("name", "dbo")
                for tbl in schema.get("tables", []):
                    tbl_name = tbl.get("name") if isinstance(tbl, dict) else str(tbl)
                    if tbl_name:
                        t_id = make_table_id(cat_id, schema_name, tbl_name)
                        upsert_node(
                            node_id=t_id,
                            name=tbl_name,
                            node_type="TABLE",
                            catalog=cat_id,
                            schema_name=schema_name,
                            metadata={"format": "delta"}
                        )
    except Exception as e:
        logger.warning(f"Error scanning catalog tables for lineage: {e}")

    # 2. Scan Workflow DAGs (Jobs & Pipelines)
    try:
        from web.workflow import load_jobs
        jobs = load_jobs()
        for job in jobs:
            job_id = f"job:{job['id']}"
            upsert_node(job_id, job["name"], "JOB", layer="ORCHESTRATION", metadata={"description": job.get("description", "")})
            for task in job.get("tasks", []):
                params = task.get("parameters", {})
                query = params.get("query", "")
                if query:
                    target, sources, files = parse_sql_lineage(query)
                    if target:
                        t_clean = target.split(".")[-1]
                        t_schema = target.split(".")[-2] if "." in target else "dbo"
                        t_id = make_table_id("warehouse", t_schema, t_clean)
                        upsert_node(t_id, t_clean, "TABLE", schema_name=t_schema)
                        upsert_edge(job_id, t_id, edge_type="ORCHESTRATES", job_id=job["id"])

                        for f in files:
                            f_id = f"file:{f}"
                            upsert_node(f_id, f, "FILE", layer="RAW_FILE")
                            upsert_edge(f_id, t_id, edge_type="INGESTS_TO", query_text=query, job_id=job["id"])

                        for s in sources:
                            s_clean = s.split(".")[-1]
                            s_schema = s.split(".")[-2] if "." in s else "dbo"
                            s_id = make_table_id("warehouse", s_schema, s_clean)
                            upsert_node(s_id, s_clean, "TABLE", schema_name=s_schema)
                            upsert_edge(s_id, t_id, edge_type="TRANSFORMS_TO", query_text=query, job_id=job["id"])

                elif task.get("type") == "optimize":
                    target_tbl = params.get("target_table", "")
                    if target_tbl:
                        t_clean = target_tbl.split(".")[-1]
                        t_schema = target_tbl.split(".")[-2] if "." in target_tbl else "dbo"
                        t_id = make_table_id("warehouse", t_schema, t_clean)
                        upsert_node(t_id, t_clean, "TABLE", schema_name=t_schema)
                        upsert_edge(job_id, t_id, edge_type="MAINTAINS", job_id=job["id"])
    except Exception as e:
        logger.warning(f"Error scanning jobs for lineage: {e}")

    # 3. Scan Lakeview Dashboards
    try:
        dashboards_file = os.path.join(METADATA_DIR, "dashboards.json")
        if os.path.exists(dashboards_file):
            with open(dashboards_file, "r") as f:
                dash_data = json.load(f)
            dashboards = dash_data.get("dashboards", []) if isinstance(dash_data, dict) else dash_data
            for d in dashboards:
                d_id = f"dashboard:{d['id']}"
                upsert_node(d_id, d["name"], "DASHBOARD", layer="PRESENTATION", metadata={"description": d.get("description", "")})
                for w in d.get("widgets", []):
                    query = w.get("query", "")
                    if query:
                        _, sources, _ = parse_sql_lineage(query)
                        for s in sources:
                            s_clean = s.split(".")[-1]
                            s_schema = s.split(".")[-2] if "." in s else "dbo"
                            s_id = make_table_id("warehouse", s_schema, s_clean)
                            upsert_node(s_id, s_clean, "TABLE", schema_name=s_schema)
                            upsert_edge(s_id, d_id, edge_type="READ_BY", query_text=query)
    except Exception as e:
        logger.warning(f"Error scanning dashboards for lineage: {e}")

    # 4. Scan Saved Queries
    try:
        from web.saved_queries import get_saved_queries
        queries = get_saved_queries()
        for q in queries:
            q_id = f"query:{q['id']}"
            upsert_node(q_id, q["name"], "QUERY", layer="ANALYTICS", metadata={"tags": q.get("tags", [])})
            _, sources, _ = parse_sql_lineage(q.get("query_text", ""))
            for s in sources:
                s_clean = s.split(".")[-1]
                s_schema = s.split(".")[-2] if "." in s else "dbo"
                s_id = make_table_id("warehouse", s_schema, s_clean)
                upsert_node(s_id, s_clean, "TABLE", schema_name=s_schema)
                upsert_edge(s_id, q_id, edge_type="READ_BY", query_text=q.get("query_text", ""))
    except Exception as e:
        logger.warning(f"Error scanning saved queries for lineage: {e}")

    # 5. Scan dbt Project Models and Dependencies
    try:
        from web.dbt_service import list_dbt_models
        dbt_data = list_dbt_models()
        models = dbt_data.get("models", [])
        model_id_by_name = {m["name"]: make_table_id("dbt_analytics", m.get("schema", "main"), m["name"]) for m in models}

        for m in models:
            m_name = m["name"]
            m_schema = m.get("schema", "main")
            m_mat = m.get("materialization", "table")
            layer = "GOLD" if (m_mat == "table" or "fct_" in m_name) else "SILVER"
            m_id = model_id_by_name[m_name]
            upsert_node(
                node_id=m_id,
                name=m_name,
                node_type="VIEW" if m_mat == "view" else "TABLE",
                layer=layer,
                catalog="dbt_analytics",
                schema_name=m_schema,
                metadata={
                    "materialization": m_mat,
                    "description": m.get("description", ""),
                    "tags": m.get("tags", []),
                    "dbt": True
                }
            )

            # Parse dependencies from dbt manifest depends_on nodes
            for dep in m.get("depends_on", []):
                if dep.startswith("source."):
                    parts = dep.split(".")
                    src_tbl = parts[-1]
                    s_id = make_table_id("warehouse", "dbo", src_tbl)
                    upsert_node(s_id, src_tbl, "TABLE", catalog="warehouse", schema_name="dbo")
                    upsert_edge(s_id, m_id, edge_type="TRANSFORMS_TO", query_text=f"dbt source: {src_tbl}")
                elif dep.startswith("model."):
                    parts = dep.split(".")
                    ref_name = parts[-1]
                    ref_id = model_id_by_name.get(ref_name, make_table_id("dbt_analytics", "main", ref_name))
                    upsert_edge(ref_id, m_id, edge_type="TRANSFORMS_TO", query_text=f"dbt ref: {ref_name}")
    except Exception as e:
        logger.warning(f"Error scanning dbt models for lineage: {e}")

    # 6. Scan MLflow Experiments & Models for Delta Lineage
    try:
        from web.delta_lineage import sync_ml_models_to_lineage_graph
        sync_ml_models_to_lineage_graph()
    except Exception as e:
        logger.warning(f"Error scanning ML models for lineage: {e}")

    logger.info("Lakehouse data lineage scan completed successfully.")


def get_global_lineage(
    layer: Optional[str] = None,
    schema: Optional[str] = None,
    search: Optional[str] = None,
    allowed_catalogs: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Returns the full Lakehouse lineage graph with optional filtering and catalog RBAC."""
    try:
        from web.delta_lineage import sync_ml_models_to_lineage_graph
        sync_ml_models_to_lineage_graph()
    except Exception:
        pass

    with get_db_connection() as conn:
        query_nodes = "SELECT * FROM lineage_nodes WHERE 1=1"
        params_nodes = []
        if layer and layer.upper() != "ALL":
            query_nodes += " AND layer = ?"
            params_nodes.append(layer.upper())
        if schema and schema.lower() != "all":
            query_nodes += " AND schema_name = ?"
            params_nodes.append(schema.lower())
        if search:
            query_nodes += " AND name LIKE ?"
            params_nodes.append(f"%{search}%")

        rows_nodes = conn.execute(query_nodes, params_nodes).fetchall()
        node_map = {r["id"]: dict(r) for r in rows_nodes}

        if allowed_catalogs is not None:
            node_map = {
                k: v for k, v in node_map.items()
                if not v.get("catalog") or v.get("catalog") in allowed_catalogs or v.get("node_type") in ("FILE", "QUERY", "JOB")
            }
        node_ids = set(node_map.keys())

        # Parse metadata JSON
        for n in node_map.values():
            if n.get("metadata"):
                try:
                    n["metadata"] = json.loads(n["metadata"])
                except Exception:
                    n["metadata"] = {}

        # Edges
        rows_edges = conn.execute("SELECT * FROM lineage_edges").fetchall()
        edges = []
        for e in rows_edges:
            # Include edge if both endpoints are in node_ids, or if searching, include connecting edges
            if e["source_id"] in node_ids or e["target_id"] in node_ids or not (layer or schema or search):
                edges.append(dict(e))

        # Ensure endpoints exist in node_map
        all_edge_nodes = set([e["source_id"] for e in edges] + [e["target_id"] for e in edges])
        missing_ids = all_edge_nodes - node_ids
        if missing_ids:
            placeholders = ",".join(["?"] * len(missing_ids))
            extra_nodes = conn.execute(f"SELECT * FROM lineage_nodes WHERE id IN ({placeholders})", list(missing_ids)).fetchall()
            for r in extra_nodes:
                d = dict(r)
                if d.get("metadata"):
                    try:
                        d["metadata"] = json.loads(d["metadata"])
                    except Exception:
                        d["metadata"] = {}
                node_map[d["id"]] = d

        # Layer summary statistics
        counts_by_layer = {}
        for n in node_map.values():
            lyr = n.get("layer", "UNKNOWN")
            counts_by_layer[lyr] = counts_by_layer.get(lyr, 0) + 1

        return {
            "nodes": list(node_map.values()),
            "edges": edges,
            "total_nodes": len(node_map),
            "total_edges": len(edges),
            "layers": counts_by_layer
        }


def get_table_lineage(schema_name: str, table_name: str, depth: int = 2) -> Dict[str, Any]:
    """
    Returns an upstream and downstream subgraph centered around a specific table.
    Provides immediate context for embedding in the Unity Catalog table details view.
    """
    table_id = make_table_id("warehouse", schema_name, table_name)
    # Also check if flat table ID exists
    with get_db_connection() as conn:
        target_node = conn.execute("SELECT * FROM lineage_nodes WHERE id = ?", (table_id,)).fetchone()
        if not target_node:
            # Try alternate matching
            alt = conn.execute("SELECT * FROM lineage_nodes WHERE name = ? AND schema_name = ?", (table_name, schema_name)).fetchone()
            if not alt:
                alt = conn.execute("SELECT * FROM lineage_nodes WHERE name = ? AND node_type = 'TABLE' LIMIT 1", (table_name,)).fetchone()
            if alt:
                table_id = alt["id"]
                target_node = alt

        if not target_node:
            # Upsert on the fly if table exists in catalog
            layer = infer_medallion_layer(table_name)
            upsert_node(table_id, table_name, "TABLE", layer=layer, schema_name=schema_name)
            target_node = conn.execute("SELECT * FROM lineage_nodes WHERE id = ?", (table_id,)).fetchone()

        all_edges = [dict(e) for e in conn.execute("SELECT * FROM lineage_edges").fetchall()]

    # Upstream Traversal (what feeds this table)
    upstream_node_ids = set()
    upstream_edges = []
    current_level = {table_id}
    for _ in range(depth):
        next_level = set()
        for e in all_edges:
            if e["target_id"] in current_level:
                upstream_node_ids.add(e["source_id"])
                upstream_edges.append(e)
                next_level.add(e["source_id"])
        current_level = next_level

    # Downstream Traversal (what reads from this table)
    downstream_node_ids = set()
    downstream_edges = []
    current_level = {table_id}
    for _ in range(depth):
        next_level = set()
        for e in all_edges:
            if e["source_id"] in current_level:
                downstream_node_ids.add(e["target_id"])
                downstream_edges.append(e)
                next_level.add(e["target_id"])
        current_level = next_level

    subgraph_node_ids = upstream_node_ids | downstream_node_ids | {table_id}
    subgraph_edges = upstream_edges + downstream_edges

    # Deduplicate edges
    seen_edge_ids = set()
    dedup_edges = []
    for e in subgraph_edges:
        if e["id"] not in seen_edge_ids:
            seen_edge_ids.add(e["id"])
            dedup_edges.append(e)

    # Fetch all node details in subgraph
    with get_db_connection() as conn:
        placeholders = ",".join(["?"] * len(subgraph_node_ids))
        nodes_rows = conn.execute(f"SELECT * FROM lineage_nodes WHERE id IN ({placeholders})", list(subgraph_node_ids)).fetchall()
        nodes = []
        for r in nodes_rows:
            d = dict(r)
            if d.get("metadata"):
                try:
                    d["metadata"] = json.loads(d["metadata"])
                except Exception:
                    d["metadata"] = {}
            nodes.append(d)

    return {
        "focus_table_id": table_id,
        "nodes": nodes,
        "edges": dedup_edges,
        "upstream_count": len(upstream_node_ids),
        "downstream_count": len(downstream_node_ids),
        "downstream_impact": {
            "tables": [n["name"] for n in nodes if n["id"] in downstream_node_ids and n["node_type"] == "TABLE"],
            "dashboards": [n["name"] for n in nodes if n["id"] in downstream_node_ids and n["node_type"] == "DASHBOARD"],
            "queries": [n["name"] for n in nodes if n["id"] in downstream_node_ids and n["node_type"] == "QUERY"],
            "jobs": [n["name"] for n in nodes if n["id"] in downstream_node_ids and n["node_type"] == "JOB"],
            "models": [n["name"] for n in nodes if n["id"] in downstream_node_ids and n["node_type"] == "MODEL"],
        }
    }


def get_node_impact_analysis(node_id: str) -> Dict[str, Any]:
    """Calculates downstream blast-radius / impact analysis if a node is modified or dropped."""
    with get_db_connection() as conn:
        all_edges = [dict(e) for e in conn.execute("SELECT * FROM lineage_edges").fetchall()]
        start_node = conn.execute("SELECT * FROM lineage_nodes WHERE id = ?", (node_id,)).fetchone()
        if not start_node:
            return {"error": "Node not found", "impacted_nodes": []}

    downstream_node_ids = set()
    current_level = {node_id}
    while current_level:
        next_level = set()
        for e in all_edges:
            if e["source_id"] in current_level and e["target_id"] not in downstream_node_ids:
                downstream_node_ids.add(e["target_id"])
                next_level.add(e["target_id"])
        current_level = next_level

    with get_db_connection() as conn:
        if downstream_node_ids:
            placeholders = ",".join(["?"] * len(downstream_node_ids))
            impact_nodes = [dict(r) for r in conn.execute(f"SELECT * FROM lineage_nodes WHERE id IN ({placeholders})", list(downstream_node_ids)).fetchall()]
        else:
            impact_nodes = []

    return {
        "source_node": dict(start_node),
        "total_impacted_count": len(impact_nodes),
        "impacted_nodes": impact_nodes,
        "summary": {
            "tables": len([n for n in impact_nodes if n["node_type"] == "TABLE"]),
            "dashboards": len([n for n in impact_nodes if n["node_type"] == "DASHBOARD"]),
            "queries": len([n for n in impact_nodes if n["node_type"] == "QUERY"]),
            "jobs": len([n for n in impact_nodes if n["node_type"] == "JOB"]),
            "models": len([n for n in impact_nodes if n["node_type"] == "MODEL"]),
        }
    }
