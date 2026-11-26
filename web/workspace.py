import os
import posixpath
import json
import shutil
import time
import datetime
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger("workspace")

NOTEBOOKS_DIR = os.getenv("NOTEBOOKS_DIR", "/workspace/notebooks")
# Fallback to local ./notebooks if running outside Docker container
if not os.path.exists(NOTEBOOKS_DIR):
    alt = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "notebooks"))
    if os.path.exists(alt):
        NOTEBOOKS_DIR = alt


def get_safe_path(rel_path: str) -> str:
    """Resolves relative path safely inside NOTEBOOKS_DIR preventing directory traversal."""
    clean = (rel_path or "").strip().lstrip("/")
    target = os.path.abspath(os.path.join(NOTEBOOKS_DIR, clean))
    try:
        common = os.path.commonpath([os.path.abspath(NOTEBOOKS_DIR), target])
        if common != os.path.abspath(NOTEBOOKS_DIR):
            raise ValueError("Path traversal attempt detected")
    except ValueError:
        raise ValueError("Invalid workspace path")
    return target


def init_user_workspace(username: str):
    """Ensures personal workspace directory Users/<username> exists with a starter notebook."""
    if not username:
        return
    clean_user = username.strip().lower()
    os.makedirs(NOTEBOOKS_DIR, exist_ok=True)
    users_dir = os.path.join(NOTEBOOKS_DIR, "Users")
    user_dir = os.path.join(users_dir, clean_user)
    os.makedirs(user_dir, exist_ok=True)

    sample_nb = os.path.join(user_dir, f"{clean_user}_scratchpad.ipynb")
    if not os.path.exists(sample_nb):
        create_blank_notebook(sample_nb, title=f"{clean_user.title()}'s Lakehouse Workspace")


def can_access_workspace_path(rel_path: str, current_user: Optional[Dict[str, Any]], write: bool = False) -> bool:
    """
    Enforces Databricks workspace access control:
    - Users have full access to Users/<their_username> and Shared/
    - Admin has full access to all paths
    - Regular users cannot read or modify other users' private directories (Users/<other_user>)
    """
    if not current_user:
        return True
    role = current_user.get("role", "user")
    if role == "admin":
        return True
    username = (current_user.get("username") or "").strip().lower()
    # Normalise first: `Users/me/../someone_else/x` must be judged by where it actually lands, not by its prefix.
    norm = posixpath.normpath("/" + (rel_path or "").strip().replace("\\", "/")).lstrip("/")
    if norm == ".." or norm.startswith("../"):
        return False
    
    if norm.startswith("Users/") or norm == "Users":
        parts = norm.split("/")
        if len(parts) > 1:
            target_user = parts[1].strip().lower()
            if target_user != username:
                return False
    return True


def init_workspace_directories():
    """Ensures standard Databricks directory hierarchy (Users/<users>, Shared) exists."""
    os.makedirs(NOTEBOOKS_DIR, exist_ok=True)
    users_dir = os.path.join(NOTEBOOKS_DIR, "Users")
    shared_dir = os.path.join(NOTEBOOKS_DIR, "Shared")

    os.makedirs(users_dir, exist_ok=True)
    os.makedirs(shared_dir, exist_ok=True)

    # Seed default user workspaces
    for u in ["admin", "lead_engineer", "analyst_bob"]:
        init_user_workspace(u)

    # Seed sample shared python script in Shared if empty
    sample_shared = os.path.join(shared_dir, "common_transforms.py")
    if not os.path.exists(sample_shared):
        try:
            with open(sample_shared, "w", encoding="utf-8") as f:
                f.write(
                    "# Shared Lakehouse Data Transformations\n"
                    "# Can be imported by notebooks or executed in workflows\n\n"
                    "def format_currency(val):\n"
                    "    return f'${val:,.2f}' if val is not None else '$0.00'\n\n"
                    "def categorize_salary(salary):\n"
                    "    if salary >= 100000:\n"
                    "        return 'Senior / Lead'\n"
                    "    elif salary >= 70000:\n"
                    "        return 'Mid-Level'\n"
                    "    return 'Associate'\n"
                )
        except Exception as e:
            logger.warning(f"Could not create sample shared file: {e}")


def create_blank_notebook(full_path: str, title: str = "New Lakehouse Notebook", template: str = "pyspark"):
    """Writes a valid minimal Jupyter Notebook (nbformat 4.5) file configured with either PySpark or Python templates."""
    clean_title = title.replace(".ipynb", "")
    t = (template or "pyspark").lower()

    if t in ("pyspark", "spark", "sqlframe"):
        md_text = [
            f"# {clean_title}\n",
            "PySpark notebook created in Databricks Local Studio.\n",
            "- **Engine**: SQLFrame + DuckDB (Databricks PySpark API without JVM overhead)\n",
            "- **Default Warehouse**: `/workspace/warehouse` (pre-initialized as `spark`)\n",
            "- **Preloaded Globals**: `spark`, `dbutils`, `display()`, `%sql` / `%%sql`"
        ]
        code_text = [
            "# PySpark API via SQLFrame transpiler\n",
            "from sqlframe.duckdb import functions as F\n",
            "from sqlframe.duckdb.window import Window\n",
            "\n",
            "# 'spark' is pre-initialized to the default warehouse ('/workspace/warehouse')\n",
            "df = spark.table(\"silver_employees\")\n",
            "display(df.groupBy(\"department\").agg(F.count(\"*\").alias(\"total_employees\")))\n"
        ]
    else:
        md_text = [
            f"# {clean_title}\n",
            "Python & DuckDB notebook created in Databricks Local Studio.\n",
            "- **Engine**: DuckDB + duckrun (Direct Delta Lake queries)\n",
            "- **Default Warehouse**: `/workspace/warehouse` (pre-initialized as `conn`)\n",
            "- **Preloaded Globals**: `conn`, `dbutils`, `display()`, `%sql` / `%%sql`"
        ]
        code_text = [
            "# Direct Delta Lake access via DuckDB & duckrun\n",
            "import duckdb\n",
            "import duckrun\n",
            "\n",
            "# 'conn' is pre-initialized to the default warehouse ('/workspace/warehouse')\n",
            "# To connect to a specific catalog: duckrun.connect('/workspace/warehouse/catalogs/<catalog_id>')\n",
            "df = conn.sql(\"\"\"\n",
            "    SELECT department, COUNT(*) AS total_employees \n",
            "    FROM silver_employees \n",
            "    GROUP BY department \n",
            "    ORDER BY total_employees DESC\n",
            "\"\"\").df()\n",
            "\n",
            "display(df)\n"
        ]

    nb_content = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": md_text
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": code_text
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.11"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(nb_content, f, indent=1)


def get_file_type(name: str, is_dir: bool) -> str:
    if is_dir:
        return "directory"
    lower = name.lower()
    if lower.endswith(".ipynb"):
        return "notebook"
    if lower.endswith(".py"):
        return "python"
    if lower.endswith(".sql"):
        return "sql"
    if lower.endswith(".md"):
        return "markdown"
    if lower.endswith((".csv", ".tsv", ".parquet", ".json")):
        return "data"
    return "file"


def get_workspace_tree(base_dir: Optional[str] = None, current_user: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Recursively scans the workspace directory returning a nested hierarchical tree.
    Filters private Users/<other_user> folders for non-admin users.
    """
    init_workspace_directories()
    current_username = (current_user.get("username") or "").strip().lower() if current_user else ""
    is_admin = (current_user.get("role") == "admin") if current_user else True

    if current_username:
        init_user_workspace(current_username)

    root = base_dir or NOTEBOOKS_DIR
    if not os.path.exists(root):
        return []

    def scan_dir(dir_path: str, rel_prefix: str = "") -> List[Dict[str, Any]]:
        entries = []
        try:
            items = sorted(os.listdir(dir_path))
        except Exception as e:
            logger.warning(f"Failed to list directory {dir_path}: {e}")
            return []

        dirs_list = []
        files_list = []

        for item in items:
            # Skip hidden and cache folders
            if item.startswith(".") or item in ("__pycache__", "venv"):
                continue

            full_path = os.path.join(dir_path, item)
            rel_path = os.path.join(rel_prefix, item).replace("\\", "/")
            is_dir = os.path.isdir(full_path)

            # Access control filtering for Users directory
            if not is_admin and current_username:
                if rel_prefix == "Users" and is_dir:
                    # In Users/, non-admin users can ONLY see their own directory
                    if item.strip().lower() != current_username:
                        continue

            try:
                stat = os.stat(full_path)
                mtime = stat.st_mtime
                size = stat.st_size
                mtime_iso = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
            except Exception:
                mtime = 0
                size = 0
                mtime_iso = ""

            ftype = get_file_type(item, is_dir)
            entry = {
                "id": rel_path.replace("/", "_").replace(".", "_"),
                "name": item,
                "rel_path": rel_path,
                "path": full_path,
                "type": ftype,
                "size_bytes": size,
                "modified_at": mtime,
                "modified_iso": mtime_iso,
                "is_user_home": rel_path.lower() == f"users/{current_username}" if current_username else False
            }

            if is_dir:
                entry["children"] = scan_dir(full_path, rel_path)
                dirs_list.append(entry)
            else:
                files_list.append(entry)

        # Sort: directories first, then files alphabetically
        dirs_list.sort(key=lambda x: x["name"].lower())
        files_list.sort(key=lambda x: x["name"].lower())
        return dirs_list + files_list

    return scan_dir(root)


def get_file_details(rel_path: str) -> Dict[str, Any]:
    """
    Returns metadata, preview content, or parsed notebook cells for a workspace file.
    """
    full_path = get_safe_path(rel_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Workspace file not found: {rel_path}")

    is_dir = os.path.isdir(full_path)
    ftype = get_file_type(os.path.basename(full_path), is_dir)
    stat = os.stat(full_path)
    size = stat.st_size
    mtime = datetime.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")

    res = {
        "name": os.path.basename(full_path),
        "rel_path": rel_path.replace("\\", "/"),
        "full_path": full_path,
        "type": ftype,
        "size_bytes": size,
        "modified_at": mtime,
        "jupyter_url_path": f"notebooks/{rel_path.replace(os.sep, '/')}"
    }

    if is_dir:
        children = os.listdir(full_path)
        res["child_count"] = len([c for c in children if not c.startswith(".")])
        return res

    # Handle Jupyter Notebook files (.ipynb)
    if ftype == "notebook":
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                nb_data = json.load(f)

            raw_cells = nb_data.get("cells", [])
            cells = []
            code_cell_count = 0
            md_cell_count = 0

            for idx, c in enumerate(raw_cells):
                ctype = c.get("cell_type", "code")
                if ctype == "code":
                    code_cell_count += 1
                else:
                    md_cell_count += 1

                source_lines = c.get("source", [])
                source_str = "".join(source_lines) if isinstance(source_lines, list) else str(source_lines)

                # Extract brief output snippet
                outputs_summary = []
                for out in c.get("outputs", []):
                    ot = out.get("output_type", "")
                    if ot == "stream":
                        text = "".join(out.get("text", []))
                        outputs_summary.append({"type": "text", "content": text[:300]})
                    elif ot in ("execute_result", "display_data"):
                        data = out.get("data", {})
                        if "text/plain" in data:
                            outputs_summary.append({"type": "text", "content": "".join(data["text/plain"])[:300]})
                        elif "text/html" in data:
                            outputs_summary.append({"type": "html", "content": "HTML table / Rich visual"})

                cells.append({
                    "index": idx + 1,
                    "type": ctype,
                    "source": source_str,
                    "execution_count": c.get("execution_count"),
                    "outputs": outputs_summary
                })

            res["cells"] = cells
            res["cell_count"] = len(cells)
            res["code_cell_count"] = code_cell_count
            res["md_cell_count"] = md_cell_count
            res["kernel"] = nb_data.get("metadata", {}).get("kernelspec", {}).get("display_name", "Python 3")
        except Exception as e:
            res["error"] = f"Failed to parse notebook: {str(e)}"
            res["cells"] = []

    # Handle text/code files
    elif ftype in ("python", "sql", "markdown", "data", "file"):
        try:
            # Read up to 256KB text
            with open(full_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read(256 * 1024)
            res["content"] = content
            res["lines"] = content.count("\n") + 1
            res["is_truncated"] = size > (256 * 1024)
        except Exception as e:
            res["error"] = f"Failed to read file: {str(e)}"
            res["content"] = ""

    return res


def create_workspace_item(target_dir: str, name: str, item_type: str = "notebook", notebook_template: str = "pyspark") -> Dict[str, Any]:
    """
    Creates a new notebook, directory, SQL script, or Python file in the target directory.
    """
    parent_path = get_safe_path(target_dir)
    os.makedirs(parent_path, exist_ok=True)

    clean_name = name.strip()
    if not clean_name:
        raise ValueError("Item name cannot be empty")

    itype = item_type.lower()
    if itype == "notebook":
        if not clean_name.lower().endswith(".ipynb"):
            clean_name += ".ipynb"
        full_path = os.path.join(parent_path, clean_name)
        if os.path.exists(full_path):
            raise FileExistsError(f"Notebook already exists: {clean_name}")
        create_blank_notebook(full_path, title=clean_name.replace(".ipynb", ""), template=notebook_template)

    elif itype in ("folder", "directory"):
        full_path = os.path.join(parent_path, clean_name)
        if os.path.exists(full_path):
            raise FileExistsError(f"Folder already exists: {clean_name}")
        os.makedirs(full_path, exist_ok=True)

    elif itype == "sql":
        if not clean_name.lower().endswith(".sql"):
            clean_name += ".sql"
        full_path = os.path.join(parent_path, clean_name)
        if os.path.exists(full_path):
            raise FileExistsError(f"SQL file already exists: {clean_name}")
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(
                f"-- {clean_name}\n"
                "-- Created in Databricks Local Studio\n\n"
                "SELECT department, COUNT(*) as total_employees\n"
                "FROM silver_employees\n"
                "GROUP BY department\n"
                "ORDER BY total_employees DESC;\n"
            )

    elif itype == "python":
        if not clean_name.lower().endswith(".py"):
            clean_name += ".py"
        full_path = os.path.join(parent_path, clean_name)
        if os.path.exists(full_path):
            raise FileExistsError(f"Python file already exists: {clean_name}")
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(
                f"# {clean_name}\n"
                "# Created in Databricks Local Studio\n\n"
                "import duckdb\n"
                "import duckrun\n\n"
                "conn = duckrun.connect('/workspace/warehouse')\n"
                "df = conn.sql('SELECT * FROM silver_employees LIMIT 5').df()\n"
                "print(df)\n"
            )

    else:
        full_path = os.path.join(parent_path, clean_name)
        if os.path.exists(full_path):
            raise FileExistsError(f"File already exists: {clean_name}")
        with open(full_path, "w", encoding="utf-8") as f:
            f.write("")

    rel_path = os.path.relpath(full_path, NOTEBOOKS_DIR).replace("\\", "/")
    return {
        "success": True,
        "name": clean_name,
        "rel_path": rel_path,
        "type": get_file_type(clean_name, itype in ("folder", "directory")),
        "message": f"Successfully created {itype} '{clean_name}'"
    }


def rename_workspace_item(old_rel_path: str, new_name: str) -> Dict[str, Any]:
    """Renames a file or folder inside the workspace."""
    old_full = get_safe_path(old_rel_path)
    if not os.path.exists(old_full):
        raise FileNotFoundError(f"Item not found: {old_rel_path}")

    parent_dir = os.path.dirname(old_full)
    clean_new_name = new_name.strip()
    if not clean_new_name:
        raise ValueError("New name cannot be empty")

    # If renaming notebook, preserve .ipynb extension if omitted
    if old_full.endswith(".ipynb") and not clean_new_name.lower().endswith(".ipynb"):
        clean_new_name += ".ipynb"

    new_full = os.path.join(parent_dir, clean_new_name)
    if os.path.exists(new_full) and old_full != new_full:
        raise FileExistsError(f"Destination already exists: {clean_new_name}")

    os.rename(old_full, new_full)
    new_rel = os.path.relpath(new_full, NOTEBOOKS_DIR).replace("\\", "/")
    return {
        "success": True,
        "old_rel_path": old_rel_path,
        "new_rel_path": new_rel,
        "new_name": clean_new_name
    }


def delete_workspace_item(rel_path: str) -> Dict[str, Any]:
    """Deletes a file or directory from the workspace."""
    full_path = get_safe_path(rel_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Item not found: {rel_path}")

    # Prevent deleting root directories
    if full_path == os.path.abspath(NOTEBOOKS_DIR):
        raise ValueError("Cannot delete workspace root")

    if os.path.isdir(full_path):
        shutil.rmtree(full_path)
    else:
        os.remove(full_path)

    return {
        "success": True,
        "deleted_path": rel_path
    }
