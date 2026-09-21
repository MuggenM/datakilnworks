import os
import json
import uuid
import datetime
import subprocess
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger("localspark.dbt")

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/workspace/dbt_project")
DBT_RUNS_LOG = os.path.join(DBT_PROJECT_DIR, "logs", "dbt_runs_history.json")
DBT_DB_PATH = os.path.join(os.getenv("WAREHOUSE_DIR", "/workspace/warehouse"), "dbt_analytics.duckdb")

# In-memory runs cache
_RUNS_HISTORY: List[Dict[str, Any]] = []

def _load_runs_history(user: Optional[str] = None, is_admin: bool = True) -> List[Dict[str, Any]]:
    global _RUNS_HISTORY
    all_runs = []
    if _RUNS_HISTORY:
        all_runs = _RUNS_HISTORY
    elif os.path.exists(DBT_RUNS_LOG):
        try:
            with open(DBT_RUNS_LOG, "r", encoding="utf-8") as f:
                _RUNS_HISTORY = json.load(f)
                all_runs = _RUNS_HISTORY
        except Exception as e:
            logger.warning(f"Failed to read dbt runs history: {e}")
            all_runs = []

    if not is_admin and user:
        return [r for r in all_runs if r.get("user") == user or r.get("user") is None]
    elif is_admin and user and user != "all":
        return [r for r in all_runs if r.get("user") == user]
    return all_runs

def _save_run_record(record: Dict[str, Any]) -> None:
    global _RUNS_HISTORY
    _RUNS_HISTORY.insert(0, record)
    _RUNS_HISTORY = _RUNS_HISTORY[:50] # keep last 50
    try:
        os.makedirs(os.path.dirname(DBT_RUNS_LOG), exist_ok=True)
        with open(DBT_RUNS_LOG, "w", encoding="utf-8") as f:
            json.dump(_RUNS_HISTORY, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Failed to persist dbt run record: {e}")

def get_dbt_status(user: Optional[str] = None, is_admin: bool = True) -> Dict[str, Any]:
    project_exists = os.path.exists(os.path.join(DBT_PROJECT_DIR, "dbt_project.yml"))
    profiles_exists = os.path.exists(os.path.join(DBT_PROJECT_DIR, "profiles.yml"))

    # Check dbt executable
    dbt_bin = "/usr/local/bin/dbt" if os.path.exists("/usr/local/bin/dbt") else "dbt"
    version_str = "1.12.4"
    
    models = list_dbt_models()
    runs = _load_runs_history(user=user, is_admin=is_admin)
    last_run = runs[0] if runs else None

    return {
        "installed": True,
        "dbt_version": version_str,
        "adapter": "duckdb (duckrun enabled)",
        "project_dir": DBT_PROJECT_DIR,
        "project_valid": project_exists and profiles_exists,
        "project_name": "localspark_lakehouse",
        "total_models": len(models.get("models", [])),
        "total_sources": len(models.get("sources", [])),
        "total_tests": len(models.get("tests", [])),
        "last_run": last_run
    }

def list_dbt_models() -> Dict[str, Any]:
    manifest_path = os.path.join(DBT_PROJECT_DIR, "target", "manifest.json")
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            
            nodes = manifest.get("nodes", {})
            sources = manifest.get("sources", {})
            
            model_list = []
            test_list = []
            
            for node_id, node in nodes.items():
                resource_type = node.get("resource_type")
                if resource_type == "model":
                    config = node.get("config", {})
                    model_list.append({
                        "name": node.get("name"),
                        "unique_id": node_id,
                        "path": node.get("original_file_path"),
                        "materialization": config.get("materialized", "view"),
                        "schema": node.get("schema", "main"),
                        "database": node.get("database", "dbt_analytics"),
                        "description": node.get("description", ""),
                        "tags": node.get("tags", []),
                        "depends_on": node.get("depends_on", {}).get("nodes", []),
                        "columns": [
                            {"name": c_name, "type": c_info.get("data_type", ""), "description": c_info.get("description", "")}
                            for c_name, c_info in node.get("columns", {}).items()
                        ],
                        "status": "compiled"
                    })
                elif resource_type == "test":
                    test_meta = node.get("test_metadata", {})
                    t_type = test_meta.get("name") if test_meta else ("singular_sql" if not node.get("column_name") else "test")
                    test_list.append({
                        "name": node.get("name"),
                        "unique_id": node_id,
                        "test_type": t_type,
                        "column_name": node.get("column_name"),
                        "attached_node": node.get("attached_node"),
                        "kwargs": test_meta.get("kwargs", {}) if test_meta else {}
                    })
            
            source_list = []
            for src_id, src in sources.items():
                col_objs = []
                for c_name, c_info in src.get("columns", {}).items():
                    col_objs.append({
                        "name": c_name,
                        "description": c_info.get("description", ""),
                        "type": c_info.get("data_type", "TEXT")
                    })
                source_list.append({
                    "name": src.get("name"),
                    "source_name": src.get("source_name", "lakehouse"),
                    "unique_id": src_id,
                    "description": src.get("description", ""),
                    "columns": col_objs if col_objs else [{"name": c, "description": "", "type": "TEXT"} for c in src.get("columns", [])],
                    "meta": src.get("meta", {}),
                    "database": src.get("database", "warehouse"),
                    "schema": src.get("schema", "dbo")
                })
            
            return {
                "models": sorted(model_list, key=lambda x: x["name"]),
                "sources": sorted(source_list, key=lambda x: x["name"]),
                "tests": test_list
            }
        except Exception as e:
            logger.error(f"Error parsing manifest.json: {e}")
    
    # Fallback to filesystem scan if manifest not built yet
    models_dir = os.path.join(DBT_PROJECT_DIR, "models")
    fallback_models = []
    if os.path.exists(models_dir):
        for root, _, files in os.walk(models_dir):
            for file in files:
                if file.endswith(".sql"):
                    rel_path = os.path.relpath(os.path.join(root, file), DBT_PROJECT_DIR)
                    name = file[:-4]
                    layer = "staging" if "staging" in rel_path else ("marts" if "marts" in rel_path else "model")
                    fallback_models.append({
                        "name": name,
                        "unique_id": f"model.localspark_lakehouse.{name}",
                        "path": rel_path,
                        "materialization": "view" if layer == "staging" else "table",
                        "schema": layer,
                        "database": "dbt_analytics",
                        "description": f"{layer.capitalize()} layer model",
                        "tags": [layer],
                        "depends_on": [],
                        "columns": [],
                        "status": "raw"
                    })
    return {"models": fallback_models, "sources": [], "tests": []}

def get_dbt_model_detail(model_name: str) -> Optional[Dict[str, Any]]:
    # Search for model SQL in models directory
    models_dir = os.path.join(DBT_PROJECT_DIR, "models")
    raw_sql = None
    file_path = None
    
    for root, _, files in os.walk(models_dir):
        for file in files:
            if file == f"{model_name}.sql":
                file_path = os.path.relpath(os.path.join(root, file), DBT_PROJECT_DIR)
                with open(os.path.join(root, file), "r", encoding="utf-8") as f:
                    raw_sql = f.read()
                break
        if raw_sql:
            break
            
    if not raw_sql:
        return None

    # Check compiled SQL
    compiled_sql = None
    compiled_path = os.path.join(DBT_PROJECT_DIR, "target", "compiled", "localspark_lakehouse", file_path) if file_path else None
    if compiled_path and os.path.exists(compiled_path):
        try:
            with open(compiled_path, "r", encoding="utf-8") as f:
                compiled_sql = f.read()
        except Exception:
            pass

    # If compiled_sql not found on disk, compile via quick subprocess to avoid in-process singleton file locks
    if not compiled_sql:
        try:
            subprocess.run(
                ["dbt", "compile", "--select", model_name, "--profiles-dir", "."],
                cwd=DBT_PROJECT_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30
            )
            if compiled_path and os.path.exists(compiled_path):
                with open(compiled_path, "r", encoding="utf-8") as f:
                    compiled_sql = f.read()
        except Exception as e:
            logger.debug(f"CLI compile fallback: {e}")

    # Inspect CTEs using regex on compiled or raw SQL
    import re
    sql_to_scan = compiled_sql or raw_sql
    cte_matches = re.findall(r'(\b[a-zA-Z0-9_]+)\s+as\s*\(', sql_to_scan, re.IGNORECASE)
    reserved = {'select', 'with', 'from', 'where', 'and', 'or', 'case', 'when', 'then', 'else', 'end', 'group', 'order', 'by'}
    ctes = [c for c in cte_matches if c.lower() not in reserved]

    # Get manifest metadata if available
    all_models = list_dbt_models()
    meta = next((m for m in all_models.get("models", []) if m["name"] == model_name), {})

    return {
        "name": model_name,
        "path": file_path,
        "raw_sql": raw_sql,
        "compiled_sql": compiled_sql or raw_sql,
        "ctes": ctes,
        "metadata": meta
    }

def run_dbt_cli(action: str = "run", select: Optional[str] = None, full_refresh: bool = False, target: str = "dev", user: str = "admin") -> Dict[str, Any]:
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    start_time = datetime.datetime.now()
    
    cmd = ["dbt", action, "--profiles-dir", "."]
    if select and select.strip():
        cmd.extend(["--select", select.strip()])
    if full_refresh:
        cmd.append("--full-refresh")
    if target and target.strip():
        cmd.extend(["--target", target.strip()])

    logger.info(f"Executing dbt command in {DBT_PROJECT_DIR}: {' '.join(cmd)}")
    
    try:
        proc = subprocess.run(
            cmd,
            cwd=DBT_PROJECT_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=180
        )
        output = proc.stdout
        exit_code = proc.returncode
        status = "SUCCESS" if exit_code == 0 else "FAILED"
    except subprocess.TimeoutExpired:
        output = "Command timed out after 180 seconds"
        exit_code = 124
        status = "TIMEOUT"
    except Exception as e:
        output = f"Execution error: {str(e)}"
        exit_code = 1
        status = "FAILED"

    end_time = datetime.datetime.now()
    duration_s = round((end_time - start_time).total_seconds(), 2)

    # Parse simple pass/warn/error summary
    pass_count = 0
    error_count = 0
    warn_count = 0
    for line in output.splitlines():
        if "PASS=" in line:
            import re
            m = re.search(r'PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)', line)
            if m:
                pass_count = int(m.group(1))
                warn_count = int(m.group(2))
                error_count = int(m.group(3))

    record = {
        "run_id": run_id,
        "command": " ".join(cmd),
        "action": action,
        "select": select,
        "status": status,
        "exit_code": exit_code,
        "output": output,
        "duration_seconds": duration_s,
        "user": user or "admin",
        "created_at": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "summary": {
            "pass": pass_count,
            "warn": warn_count,
            "error": error_count
        }
    }
    
    _save_run_record(record)
    return record

def preview_dbt_model_data(model_name: str, limit: int = 50) -> Dict[str, Any]:
    """Queries the materialized table/view in dbt_analytics.duckdb."""
    import duckdb
    if not os.path.exists(DBT_DB_PATH):
        return {"columns": [], "rows": [], "row_count": 0, "error": "Database dbt_analytics.duckdb does not exist yet. Run 'dbt run' first."}
    
    con = None
    try:
        con = duckdb.connect(DBT_DB_PATH, read_only=True)
        df = con.execute(f"SELECT * FROM {model_name} LIMIT {limit}").df()
        columns = list(df.columns)
        import numpy as np
        df_clean = df.replace({np.nan: None})
        rows = df_clean.to_dict(orient="records")
        total_count = con.execute(f"SELECT COUNT(*) FROM {model_name}").fetchone()[0]
        return {
            "columns": columns,
            "rows": rows,
            "preview_limit": limit,
            "row_count": total_count,
            "error": None
        }
    except Exception as e:
        return {"columns": [], "rows": [], "row_count": 0, "error": str(e)}
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass

def preview_cte_step(model_name: str, cte_name: str, limit: int = 50) -> Dict[str, Any]:
    """Uses duckrun or compiled query splicing to preview intermediate CTE dataset."""
    import duckdb
    detail = get_dbt_model_detail(model_name)
    if not detail or not detail.get("compiled_sql"):
        return {"columns": [], "rows": [], "row_count": 0, "error": "Model compiled SQL not found."}

    compiled = detail["compiled_sql"]
    
    con = None
    try:
        import re
        parts = re.split(r'\nselect\s+', compiled, flags=re.IGNORECASE)
        if len(parts) >= 2:
            cte_prefix = "\nselect ".join(parts[:-1])
            test_sql = f"{cte_prefix}\nSELECT * FROM {cte_name} LIMIT {limit}"
        else:
            test_sql = f"WITH cte AS ({compiled}) SELECT * FROM {cte_name} LIMIT {limit}"

        con = duckdb.connect(DBT_DB_PATH, read_only=True)
        con.execute("INSTALL delta; LOAD delta;")
        df = con.execute(test_sql).df()
        import numpy as np
        df_clean = df.replace({np.nan: None})
        return {
            "cte_name": cte_name,
            "columns": list(df.columns),
            "rows": df_clean.to_dict(orient="records"),
            "row_count": len(df),
            "error": None
        }
    except Exception as e:
        logger.debug(f"CTE preview error: {e}")
        return {"columns": [], "rows": [], "row_count": 0, "error": str(e)}
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass

def add_dbt_test(
    model_name: str,
    column_name: Optional[str] = None,
    test_type: str = "not_null",
    parameters: Optional[Dict[str, Any]] = None,
    sql_text: Optional[str] = None,
    test_name: Optional[str] = None
) -> Dict[str, Any]:
    """Adds a schema or singular test to the dbt project and recompiles the manifest."""
    import yaml

    parameters = parameters or {}
    models_dir = os.path.join(DBT_PROJECT_DIR, "models")

    # 1. Singular Custom SQL Test
    if test_type == "sql":
        if not test_name or not test_name.strip():
            test_name = f"assert_{model_name}_{int(time.time())}"
        test_name = re.sub(r'[^a-zA-Z0-9_]', '_', test_name.strip().lower())

        if not sql_text or not sql_text.strip():
            raise ValueError("SQL assertion query cannot be empty")

        tests_dir = os.path.join(DBT_PROJECT_DIR, "tests")
        os.makedirs(tests_dir, exist_ok=True)
        test_file = os.path.join(tests_dir, f"{test_name}.sql")

        with open(test_file, "w", encoding="utf-8") as f:
            f.write(f"-- Singular data test for {model_name}\n{sql_text.strip()}\n")

        logger.info(f"Created custom dbt test at {test_file}")
        run_res = run_dbt_cli("compile")
        return {
            "success": True,
            "test_type": "sql",
            "test_name": test_name,
            "file": os.path.relpath(test_file, DBT_PROJECT_DIR),
            "compile_status": run_res.get("status")
        }

    # 2. Column / Generic Schema Test
    if not column_name or not column_name.strip():
        raise ValueError("Column name is required for column schema tests")

    column_name = column_name.strip()

    target_file = None
    target_data = None

    for root, _, files in os.walk(models_dir):
        for f in files:
            if f.endswith(".yml") or f.endswith(".yaml"):
                p = os.path.join(root, f)
                try:
                    with open(p, "r", encoding="utf-8") as fp:
                        doc = yaml.safe_load(fp)
                        if doc and isinstance(doc, dict) and "models" in doc:
                            for m in doc["models"]:
                                if m.get("name") == model_name:
                                    target_file = p
                                    target_data = doc
                                    break
                except Exception as e:
                    logger.warning(f"Error reading {p}: {e}")
            if target_file:
                break
        if target_file:
            break

    if not target_file:
        model_subfolder = "marts"
        for root, _, files in os.walk(models_dir):
            if f"{model_name}.sql" in files:
                model_subfolder = os.path.relpath(root, models_dir)
                break
        target_dir = os.path.join(models_dir, model_subfolder)
        os.makedirs(target_dir, exist_ok=True)
        target_file = os.path.join(target_dir, "schema.yml")
        if os.path.exists(target_file):
            try:
                with open(target_file, "r", encoding="utf-8") as fp:
                    target_data = yaml.safe_load(fp) or {"version": 2, "models": []}
            except Exception:
                target_data = {"version": 2, "models": []}
        else:
            target_data = {"version": 2, "models": []}

    if "models" not in target_data or not isinstance(target_data["models"], list):
        target_data["models"] = []

    model_entry = next((m for m in target_data["models"] if m.get("name") == model_name), None)
    if not model_entry:
        model_entry = {"name": model_name, "columns": []}
        target_data["models"].append(model_entry)

    if "columns" not in model_entry or not isinstance(model_entry["columns"], list):
        model_entry["columns"] = []

    col_entry = next((c for c in model_entry["columns"] if c.get("name") == column_name), None)
    if not col_entry:
        col_entry = {"name": column_name, "tests": []}
        model_entry["columns"].append(col_entry)

    if "tests" not in col_entry or not isinstance(col_entry["tests"], list):
        col_entry["tests"] = []

    if test_type == "not_null":
        if "not_null" not in col_entry["tests"]:
            col_entry["tests"].append("not_null")
    elif test_type == "unique":
        if "unique" not in col_entry["tests"]:
            col_entry["tests"].append("unique")
    elif test_type == "accepted_values":
        vals = parameters.get("values", [])
        if isinstance(vals, str):
            vals = [v.strip() for v in vals.split(",") if v.strip()]
        col_entry["tests"] = [t for t in col_entry["tests"] if not (isinstance(t, dict) and "accepted_values" in t)]
        col_entry["tests"].append({"accepted_values": {"values": vals}})
    elif test_type == "relationships":
        to_model = parameters.get("to", "")
        field = parameters.get("field", "id")
        to_ref = to_model if ("ref(" in to_model or "source(" in to_model) else f"ref('{to_model}')"
        col_entry["tests"] = [t for t in col_entry["tests"] if not (isinstance(t, dict) and "relationships" in t)]
        col_entry["tests"].append({"relationships": {"to": to_ref, "field": field}})
    else:
        if test_type not in col_entry["tests"]:
            col_entry["tests"].append(test_type)

    with open(target_file, "w", encoding="utf-8") as f:
        yaml.dump(target_data, f, sort_keys=False, default_flow_style=False)

    logger.info(f"Updated schema test for {model_name}.{column_name} in {target_file}")
    run_res = run_dbt_cli("compile")
    return {
        "success": True,
        "test_type": test_type,
        "model": model_name,
        "column": column_name,
        "file": os.path.relpath(target_file, DBT_PROJECT_DIR),
        "compile_status": run_res.get("status")
    }

def delete_dbt_test(
    model_name: str,
    column_name: Optional[str] = None,
    test_type: Optional[str] = None,
    test_name: Optional[str] = None
) -> Dict[str, Any]:
    """Deletes a test from the dbt project and recompiles."""
    import yaml

    if test_name:
        tests_dir = os.path.join(DBT_PROJECT_DIR, "tests")
        for f in [f"{test_name}.sql", test_name]:
            test_file = os.path.join(tests_dir, f)
            if os.path.exists(test_file):
                os.remove(test_file)
                logger.info(f"Removed custom dbt test file {test_file}")
                run_res = run_dbt_cli("compile")
                return {"success": True, "deleted": test_name, "compile_status": run_res.get("status")}

    if not column_name or not test_type:
        raise ValueError("column_name and test_type are required to delete a column test")

    models_dir = os.path.join(DBT_PROJECT_DIR, "models")
    target_file = None
    target_data = None

    for root, _, files in os.walk(models_dir):
        for f in files:
            if f.endswith(".yml") or f.endswith(".yaml"):
                p = os.path.join(root, f)
                try:
                    with open(p, "r", encoding="utf-8") as fp:
                        doc = yaml.safe_load(fp)
                        if doc and isinstance(doc, dict) and "models" in doc:
                            for m in doc["models"]:
                                if m.get("name") == model_name:
                                    target_file = p
                                    target_data = doc
                                    break
                except Exception:
                    pass
            if target_file:
                break
        if target_file:
            break

    if not target_file or not target_data:
        return {"success": False, "error": f"No YAML configuration found for model '{model_name}'"}

    for m in target_data.get("models", []):
        if m.get("name") == model_name:
            for col in m.get("columns", []):
                if col.get("name") == column_name:
                    tests = col.get("tests", [])
                    new_tests = []
                    for t in tests:
                        if isinstance(t, str) and t == test_type:
                            continue
                        elif isinstance(t, dict) and test_type in t:
                            continue
                        new_tests.append(t)
                    col["tests"] = new_tests

    with open(target_file, "w", encoding="utf-8") as f:
        yaml.dump(target_data, f, sort_keys=False, default_flow_style=False)

    run_res = run_dbt_cli("compile")
    return {"success": True, "deleted": f"{test_type} on {model_name}.{column_name}", "compile_status": run_res.get("status")}

def list_available_delta_tables() -> List[str]:
    """Scans the Delta Lakehouse warehouse directory and lists available tables."""
    dbo_dir = os.path.join(os.getenv("WAREHOUSE_DIR", "/workspace/warehouse"), "dbo")
    if not os.path.exists(dbo_dir):
        dbo_dir = "/workspace/warehouse/dbo"
    tables = []
    if os.path.exists(dbo_dir):
        for entry in sorted(os.listdir(dbo_dir)):
            p = os.path.join(dbo_dir, entry)
            if os.path.isdir(p) and os.path.exists(os.path.join(p, "_delta_log")):
                tables.append(entry)
    return tables

def add_dbt_source(
    source_name: str = "lakehouse",
    table_name: str = "",
    description: Optional[str] = None,
    external_location: Optional[str] = None
) -> Dict[str, Any]:
    """Adds a new Delta table source to models/staging/sources.yml and recompiles."""
    import yaml
    import re
    if not table_name or not table_name.strip():
        raise ValueError("table_name is required")
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name.strip().lower())
    source_name = re.sub(r'[^a-zA-Z0-9_]', '_', (source_name or "lakehouse").strip().lower())

    sources_file = os.path.join(DBT_PROJECT_DIR, "models", "staging", "sources.yml")
    os.makedirs(os.path.dirname(sources_file), exist_ok=True)

    data = {"version": 2, "sources": []}
    if os.path.exists(sources_file):
        try:
            with open(sources_file, "r", encoding="utf-8") as fp:
                loaded = yaml.safe_load(fp)
                if loaded and isinstance(loaded, dict) and "sources" in loaded:
                    data = loaded
        except Exception as e:
            logger.warning(f"Error loading {sources_file}: {e}")

    if "sources" not in data or not isinstance(data["sources"], list):
        data["sources"] = []

    src_entry = next((s for s in data["sources"] if s.get("name") == source_name), None)
    if not src_entry:
        src_entry = {
            "name": source_name,
            "description": f"Primary Localspark Delta Lakehouse tables in {source_name}",
            "meta": {
                "external_location": external_location or "delta_scan('/workspace/warehouse/dbo/{name}')"
            },
            "tables": []
        }
        data["sources"].append(src_entry)

    if "tables" not in src_entry or not isinstance(src_entry["tables"], list):
        src_entry["tables"] = []

    existing_table = next((t for t in src_entry["tables"] if t.get("name") == table_name), None)
    if existing_table:
        if description:
            existing_table["description"] = description
    else:
        src_entry["tables"].append({
            "name": table_name,
            "description": description or f"Delta Lake table {table_name}"
        })

    with open(sources_file, "w", encoding="utf-8") as fp:
        yaml.dump(data, fp, sort_keys=False, default_flow_style=False)

    run_res = run_dbt_cli("compile")
    return {
        "success": True,
        "source": source_name,
        "table": table_name,
        "file": "models/staging/sources.yml",
        "compile_status": run_res.get("status")
    }

def update_dbt_source(
    source_name: str,
    table_name: str,
    new_table_name: Optional[str] = None,
    description: Optional[str] = None
) -> Dict[str, Any]:
    """Updates an existing source table definition in sources.yml."""
    import yaml
    sources_file = os.path.join(DBT_PROJECT_DIR, "models", "staging", "sources.yml")
    if not os.path.exists(sources_file):
        raise ValueError("sources.yml not found")

    with open(sources_file, "r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp) or {}

    updated = False
    for s in data.get("sources", []):
        if s.get("name") == source_name:
            for t in s.get("tables", []):
                if t.get("name") == table_name:
                    if new_table_name and new_table_name.strip():
                        t["name"] = new_table_name.strip().lower()
                    if description is not None:
                        t["description"] = description
                    updated = True
                    break

    if not updated:
        raise ValueError(f"Source table {source_name}.{table_name} not found")

    with open(sources_file, "w", encoding="utf-8") as fp:
        yaml.dump(data, fp, sort_keys=False, default_flow_style=False)

    run_res = run_dbt_cli("compile")
    return {"success": True, "source": source_name, "table": new_table_name or table_name, "compile_status": run_res.get("status")}

def delete_dbt_source(source_name: str, table_name: str) -> Dict[str, Any]:
    """Removes a source table from sources.yml and recompiles."""
    import yaml
    sources_file = os.path.join(DBT_PROJECT_DIR, "models", "staging", "sources.yml")
    if not os.path.exists(sources_file):
        raise ValueError("sources.yml not found")

    with open(sources_file, "r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp) or {}

    for s in data.get("sources", []):
        if s.get("name") == source_name:
            tables = s.get("tables", [])
            s["tables"] = [t for t in tables if t.get("name") != table_name]

    with open(sources_file, "w", encoding="utf-8") as fp:
        yaml.dump(data, fp, sort_keys=False, default_flow_style=False)

    run_res = run_dbt_cli("compile")
    return {"success": True, "deleted": f"{source_name}.{table_name}", "compile_status": run_res.get("status")}

def preview_dbt_source(source_name: str, table_name: str, limit: int = 50) -> Dict[str, Any]:
    """Previews records from a Delta source table using DuckDB delta_scan."""
    import duckdb
    con = None
    try:
        dbo_dir = os.path.join(os.getenv("WAREHOUSE_DIR", "/workspace/warehouse"), "dbo")
        table_path = os.path.join(dbo_dir, table_name)
        
        con = duckdb.connect()
        con.execute("INSTALL delta; LOAD delta;")
        query = f"SELECT * FROM delta_scan('{table_path}') LIMIT {limit}"
        df = con.execute(query).df()
        import numpy as np
        df_clean = df.replace({np.nan: None})
        return {
            "source_name": source_name,
            "table_name": table_name,
            "columns": list(df.columns),
            "rows": df_clean.to_dict(orient="records"),
            "row_count": len(df),
            "error": None
        }
    except Exception as e:
        logger.warning(f"Error previewing Delta source {source_name}.{table_name}: {e}")
        return {"source_name": source_name, "table_name": table_name, "columns": [], "rows": [], "row_count": 0, "error": str(e)}
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass

def create_dbt_model(
    name: str,
    layer: str = "staging",
    materialization: str = "view",
    sql_content: Optional[str] = None,
    description: Optional[str] = ""
) -> Dict[str, Any]:
    """Creates a new model SQL file, updates schema.yml, and recompiles."""
    import yaml
    import re

    name = re.sub(r'[^a-zA-Z0-9_]', '_', name.strip().lower())
    if not name:
        raise ValueError("Model name cannot be empty")

    layer = layer.strip().lower() if layer in ["staging", "marts"] else "staging"
    target_dir = os.path.join(DBT_PROJECT_DIR, "models", layer)
    os.makedirs(target_dir, exist_ok=True)
    sql_file = os.path.join(target_dir, f"{name}.sql")

    if os.path.exists(sql_file):
        raise ValueError(f"Model '{name}' already exists at {os.path.relpath(sql_file, DBT_PROJECT_DIR)}")

    if not sql_content or not sql_content.strip():
        sql_content = f"""{{{{ config(materialized='{materialization}') }}}}

select
    1 as id,
    current_timestamp as created_at
"""

    with open(sql_file, "w", encoding="utf-8") as fp:
        fp.write(sql_content.strip() + "\n")

    # Update or create schema.yml in target_dir
    schema_file = os.path.join(target_dir, "schema.yml")
    schema_data = {"version": 2, "models": []}
    if os.path.exists(schema_file):
        try:
            with open(schema_file, "r", encoding="utf-8") as fp:
                loaded = yaml.safe_load(fp)
                if loaded and isinstance(loaded, dict) and "models" in loaded:
                    schema_data = loaded
        except Exception:
            pass

    if "models" not in schema_data or not isinstance(schema_data["models"], list):
        schema_data["models"] = []

    model_entry = next((m for m in schema_data["models"] if m.get("name") == name), None)
    if not model_entry:
        schema_data["models"].append({
            "name": name,
            "description": description or f"{layer.capitalize()} model {name}",
            "columns": []
        })

    with open(schema_file, "w", encoding="utf-8") as fp:
        yaml.dump(schema_data, fp, sort_keys=False, default_flow_style=False)

    run_res = run_dbt_cli("compile")
    return {
        "success": True,
        "model": name,
        "path": os.path.relpath(sql_file, DBT_PROJECT_DIR),
        "compile_status": run_res.get("status")
    }

def update_dbt_model_code(model_name: str, sql_content: str, description: Optional[str] = None) -> Dict[str, Any]:
    """Updates the SQL source code of an existing model and recompiles."""
    models_dir = os.path.join(DBT_PROJECT_DIR, "models")
    target_file = None
    for root, _, files in os.walk(models_dir):
        if f"{model_name}.sql" in files:
            target_file = os.path.join(root, f"{model_name}.sql")
            break

    if not target_file:
        raise ValueError(f"Model '{model_name}' SQL file not found")

    with open(target_file, "w", encoding="utf-8") as fp:
        fp.write(sql_content.strip() + "\n")

    if description:
        import yaml
        schema_file = os.path.join(os.path.dirname(target_file), "schema.yml")
        if os.path.exists(schema_file):
            try:
                with open(schema_file, "r", encoding="utf-8") as fp:
                    data = yaml.safe_load(fp) or {}
                for m in data.get("models", []):
                    if m.get("name") == model_name:
                        m["description"] = description
                with open(schema_file, "w", encoding="utf-8") as fp:
                    yaml.dump(data, fp, sort_keys=False, default_flow_style=False)
            except Exception as e:
                logger.warning(f"Error updating model description: {e}")

    run_res = run_dbt_cli("compile")
    detail = get_dbt_model_detail(model_name)
    return {
        "success": True,
        "model": model_name,
        "compile_status": run_res.get("status"),
        "compiled_sql": detail.get("compiled_sql") if detail else None,
        "ctes": detail.get("ctes") if detail else []
    }

def delete_dbt_model(model_name: str) -> Dict[str, Any]:
    """Deletes a model SQL file, removes it from schema.yml, and recompiles."""
    import yaml
    models_dir = os.path.join(DBT_PROJECT_DIR, "models")
    target_file = None
    for root, _, files in os.walk(models_dir):
        if f"{model_name}.sql" in files:
            target_file = os.path.join(root, f"{model_name}.sql")
            break

    if target_file and os.path.exists(target_file):
        os.remove(target_file)
        schema_file = os.path.join(os.path.dirname(target_file), "schema.yml")
        if os.path.exists(schema_file):
            try:
                with open(schema_file, "r", encoding="utf-8") as fp:
                    data = yaml.safe_load(fp) or {}
                if "models" in data:
                    data["models"] = [m for m in data["models"] if m.get("name") != model_name]
                with open(schema_file, "w", encoding="utf-8") as fp:
                    yaml.dump(data, fp, sort_keys=False, default_flow_style=False)
            except Exception as e:
                logger.warning(f"Error removing model from schema.yml: {e}")

    run_res = run_dbt_cli("compile")
    return {"success": True, "deleted": model_name, "compile_status": run_res.get("status")}
