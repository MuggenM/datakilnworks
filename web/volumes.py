import os
import json
import time
import shutil
import hashlib
import logging
from typing import Dict, Any, List, Optional
import duckdb

logger = logging.getLogger("localspark.volumes")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
VOLUMES_ROOT = os.path.join(WAREHOUSE_DIR, "volumes")
VOLUMES_METADATA_FILE = os.path.join(METADATA_DIR, "volumes.json")

os.makedirs(VOLUMES_ROOT, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)


def load_volumes_metadata() -> List[Dict[str, Any]]:
    """Loads volume definitions from metadata file."""
    if not os.path.exists(VOLUMES_METADATA_FILE):
        return []
    try:
        with open(VOLUMES_METADATA_FILE, "r") as f:
            data = json.load(f)
            return data.get("volumes", [])
    except Exception as e:
        logger.error(f"Failed to load volumes metadata: {e}")
        return []


def save_volumes_metadata(volumes: List[Dict[str, Any]]):
    """Saves volume definitions to metadata file."""
    try:
        with open(VOLUMES_METADATA_FILE, "w") as f:
            json.dump({"volumes": volumes, "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")}, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save volumes metadata: {e}")


def get_volume_physical_path(catalog: str, schema: str, volume_name: str) -> str:
    """Returns the canonical on-disk storage directory for a managed volume."""
    cat_clean = catalog.strip().lower().replace("-", "_")
    sch_clean = schema.strip().lower().replace("-", "_")
    vol_clean = volume_name.strip().lower().replace("-", "_")
    return os.path.join(VOLUMES_ROOT, cat_clean, sch_clean, vol_clean)


def resolve_volume_posix_path(posix_path: str) -> str:
    """
    Translates a POSIX Unity Catalog Volume path (e.g. /Volumes/catalog/schema/vol/sub/file.csv)
    or standard relative path into the local physical disk path.
    """
    norm = (posix_path or "").strip().replace("\\", "/")
    if norm.lower().startswith("/volumes/"):
        parts = [p for p in norm[len("/volumes/"):].split("/") if p]
        if len(parts) >= 3:
            catalog, schema, volume_name = parts[0], parts[1], parts[2]
            subpath = "/".join(parts[3:]) if len(parts) > 3 else ""
            base = get_volume_physical_path(catalog, schema, volume_name)
            target = os.path.join(base, subpath) if subpath else base
            # Path traversal check
            if os.path.commonpath([os.path.abspath(base), os.path.abspath(target)]) != os.path.abspath(base):
                raise ValueError("Path traversal violation in volume path")
            return target
    
    # If absolute path already inside VOLUMES_ROOT or WAREHOUSE_DIR
    target = os.path.abspath(norm)
    if os.path.commonpath([os.path.abspath(WAREHOUSE_DIR), target]) == os.path.abspath(WAREHOUSE_DIR):
        return target

    raise ValueError(f"Invalid volume path: '{posix_path}'. Must start with /Volumes/<catalog>/<schema>/<volume_name>")


def list_volumes(catalog: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """Lists all registered Unity Catalog volumes, optionally filtered by catalog and schema."""
    all_vols = load_volumes_metadata()
    results = []
    
    for v in all_vols:
        if catalog and v.get("catalog", "").lower() != catalog.lower():
            continue
        if schema and v.get("schema", "").lower() != schema.lower():
            continue
        
        # Verify physical directory exists and calculate file count and size
        phys = v.get("storage_location") or get_volume_physical_path(v["catalog"], v["schema"], v["name"])
        os.makedirs(phys, exist_ok=True)
        
        file_count = 0
        total_bytes = 0
        try:
            for root, dirs, files in os.walk(phys):
                # Skip quarantine and hidden dirs in top-level count
                dirs[:] = [d for d in dirs if not d.startswith(".") and d != "_quarantine"]
                for f in files:
                    if not f.startswith("."):
                        file_count += 1
                        fp = os.path.join(root, f)
                        try:
                            total_bytes += os.path.getsize(fp)
                        except OSError:
                            pass
        except Exception:
            pass

        item = dict(v)
        item["file_count"] = file_count
        item["total_size_bytes"] = total_bytes
        item["posix_path"] = f"/Volumes/{v['catalog']}/{v['schema']}/{v['name']}"
        results.append(item)

    return sorted(results, key=lambda x: (x.get("catalog", ""), x.get("schema", ""), x.get("name", "")))


def create_volume(
    catalog: str,
    schema: str,
    name: str,
    description: str = "",
    volume_type: str = "MANAGED",
    external_location: Optional[str] = None,
    owner: str = "admin"
) -> Dict[str, Any]:
    """Creates a new Unity Catalog volume."""
    cat_clean = catalog.strip().lower().replace("-", "_")
    sch_clean = schema.strip().lower().replace("-", "_")
    vol_clean = name.strip().lower().replace("-", "_")
    
    if not cat_clean or not sch_clean or not vol_clean:
        raise ValueError("Catalog, schema, and volume name are required.")

    all_vols = load_volumes_metadata()
    for v in all_vols:
        if v.get("catalog") == cat_clean and v.get("schema") == sch_clean and v.get("name") == vol_clean:
            raise ValueError(f"Volume '{vol_clean}' already exists in {cat_clean}.{sch_clean}.")

    phys_path = external_location if (volume_type.upper() == "EXTERNAL" and external_location) else get_volume_physical_path(cat_clean, sch_clean, vol_clean)
    os.makedirs(phys_path, exist_ok=True)

    vol_record = {
        "id": f"vol_{hashlib.md5(f'{cat_clean}.{sch_clean}.{vol_clean}'.encode()).hexdigest()[:8]}",
        "name": vol_clean,
        "catalog": cat_clean,
        "schema": sch_clean,
        "volume_type": volume_type.upper(),
        "storage_location": phys_path,
        "posix_path": f"/Volumes/{cat_clean}/{sch_clean}/{vol_clean}",
        "description": description.strip(),
        "owner": owner,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    all_vols.append(vol_record)
    save_volumes_metadata(all_vols)
    logger.info(f"Created volume: {vol_record['posix_path']} -> {phys_path}")
    return vol_record


def delete_volume(catalog: str, schema: str, name: str) -> bool:
    """Deletes a volume record and optionally purges managed files."""
    cat_clean = catalog.strip().lower()
    sch_clean = schema.strip().lower()
    vol_clean = name.strip().lower()

    all_vols = load_volumes_metadata()
    target = None
    remaining = []
    for v in all_vols:
        if v.get("catalog") == cat_clean and v.get("schema") == sch_clean and v.get("name") == vol_clean:
            target = v
        else:
            remaining.append(v)

    if not target:
        return False

    save_volumes_metadata(remaining)
    # If managed volume, purge physical directory
    if target.get("volume_type") == "MANAGED":
        phys = target.get("storage_location") or get_volume_physical_path(cat_clean, sch_clean, vol_clean)
        if os.path.exists(phys):
            try:
                shutil.rmtree(phys)
            except Exception as e:
                logger.warning(f"Could not purge physical volume directory {phys}: {e}")

    logger.info(f"Deleted volume {cat_clean}.{sch_clean}.{vol_clean}")
    return True


def list_volume_files(catalog: str, schema: str, volume_name: str, subpath: str = "") -> List[Dict[str, Any]]:
    """Lists files and folders inside a specified volume or subfolder."""
    base_dir = get_volume_physical_path(catalog, schema, volume_name)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir, exist_ok=True)
        return []

    target_dir = os.path.join(base_dir, subpath.strip().lstrip("/"))
    if os.path.commonpath([os.path.abspath(base_dir), os.path.abspath(target_dir)]) != os.path.abspath(base_dir):
        raise ValueError("Invalid subpath: directory traversal detected.")

    if not os.path.exists(target_dir):
        return []

    items = []
    try:
        with os.scandir(target_dir) as entries:
            for entry in entries:
                if entry.name.startswith("."):
                    continue
                stat = entry.stat()
                rel_to_vol = os.path.relpath(entry.path, base_dir).replace("\\", "/")
                is_dir = entry.is_dir()
                _, ext = os.path.splitext(entry.name)
                
                items.append({
                    "name": entry.name,
                    "relative_path": rel_to_vol,
                    "posix_path": f"/Volumes/{catalog.lower()}/{schema.lower()}/{volume_name.lower()}/{rel_to_vol}".rstrip("/"),
                    "is_directory": is_dir,
                    "size_bytes": 0 if is_dir else stat.st_size,
                    "extension": ext.lower().lstrip("."),
                    "modified_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_mtime)),
                    "is_quarantine": entry.name == "_quarantine"
                })
    except Exception as e:
        logger.error(f"Error scanning volume directory {target_dir}: {e}")

    # Folders first, then alphabetical by name
    return sorted(items, key=lambda x: (not x["is_directory"], x["name"].lower()))


def upload_file_to_volume(catalog: str, schema: str, volume_name: str, filename: str, content_bytes: bytes, subpath: str = "") -> Dict[str, Any]:
    """Saves uploaded binary content into a volume directory."""
    base_dir = get_volume_physical_path(catalog, schema, volume_name)
    os.makedirs(base_dir, exist_ok=True)

    target_dir = os.path.join(base_dir, subpath.strip().lstrip("/"))
    if os.path.commonpath([os.path.abspath(base_dir), os.path.abspath(target_dir)]) != os.path.abspath(base_dir):
        raise ValueError("Invalid subpath: directory traversal detected.")
    os.makedirs(target_dir, exist_ok=True)

    clean_filename = os.path.basename(filename.strip())
    dest_path = os.path.join(target_dir, clean_filename)
    
    with open(dest_path, "wb") as f:
        f.write(content_bytes)

    stat = os.stat(dest_path)
    rel_path = os.path.relpath(dest_path, base_dir).replace("\\", "/")
    posix_path = f"/Volumes/{catalog.lower()}/{schema.lower()}/{volume_name.lower()}/{rel_path}"

    return {
        "success": True,
        "name": clean_filename,
        "relative_path": rel_path,
        "posix_path": posix_path,
        "physical_path": dest_path,
        "size_bytes": stat.st_size,
        "modified_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_mtime))
    }


def delete_file_from_volume(catalog: str, schema: str, volume_name: str, rel_path: str) -> bool:
    """Deletes a file or directory from inside a volume."""
    base_dir = get_volume_physical_path(catalog, schema, volume_name)
    target_path = os.path.join(base_dir, rel_path.strip().lstrip("/"))
    
    if os.path.commonpath([os.path.abspath(base_dir), os.path.abspath(target_path)]) != os.path.abspath(base_dir):
        raise ValueError("Invalid path: directory traversal detected.")

    if not os.path.exists(target_path):
        return False

    if os.path.isdir(target_path):
        shutil.rmtree(target_path)
    else:
        os.remove(target_path)
    return True


def preview_volume_file(catalog: str, schema: str, volume_name: str, rel_path: str, limit: int = 10) -> Dict[str, Any]:
    """
    Parses a CSV, Parquet, or JSON file stored in a volume using DuckDB
    and returns column metadata and preview rows.
    """
    base_dir = get_volume_physical_path(catalog, schema, volume_name)
    file_path = os.path.join(base_dir, rel_path.strip().lstrip("/"))
    
    if os.path.commonpath([os.path.abspath(base_dir), os.path.abspath(file_path)]) != os.path.abspath(base_dir):
        raise ValueError("Invalid file path: directory traversal detected.")

    if not os.path.exists(file_path) or os.path.isdir(file_path):
        raise FileNotFoundError(f"File not found: {rel_path}")

    _, ext = os.path.splitext(file_path)
    ext_lower = ext.lower().lstrip(".")
    
    conn = duckdb.connect(":memory:")
    try:
        if ext_lower in ("csv", "tsv", "txt"):
            source_query = f"SELECT * FROM read_csv_auto('{file_path}')"
        elif ext_lower == "parquet":
            source_query = f"SELECT * FROM read_parquet('{file_path}')"
        elif ext_lower in ("json", "jsonl", "ndjson"):
            source_query = f"SELECT * FROM read_json_auto('{file_path}')"
        else:
            raise ValueError(f"Preview is not supported for file extension '.{ext_lower}' (supports CSV, Parquet, JSON).")

        df = conn.sql(f"{source_query} LIMIT {max(1, min(limit, 100))}").df()
        total_count = int(conn.sql(f"SELECT COUNT(*) FROM ({source_query})").fetchone()[0])
        
        columns = [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
        rows = df.fillna("").to_dict(orient="records")

        return {
            "success": True,
            "filename": os.path.basename(file_path),
            "relative_path": rel_path,
            "posix_path": f"/Volumes/{catalog.lower()}/{schema.lower()}/{volume_name.lower()}/{rel_path}",
            "extension": ext_lower,
            "columns": columns,
            "total_rows": total_count,
            "sample_rows": rows,
            "preview_limit": limit
        }
    finally:
        conn.close()


def ensure_default_volumes():
    """Initializes default volumes if none exist."""
    existing = load_volumes_metadata()
    if not existing:
        create_volume(
            catalog="warehouse",
            schema="raw",
            name="iot_stream",
            description="Default volume for incoming IoT sensor and telemetry streams (CSV, Parquet, JSON).",
            owner="admin"
        )
