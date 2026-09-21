import os
import json
import logging
import datetime
import uuid
import re
from typing import Dict, Any, List, Optional
import duckdb
from fastapi import HTTPException

logger = logging.getLogger("localspark.mounts")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
MOUNTS_FILE = os.path.join(METADATA_DIR, "storage_mounts.json")


def sanitize_connection_error(err_str: str, config: Optional[Dict[str, Any]] = None, mount_type: Optional[str] = None) -> str:
    """Removes passwords, secrets, and credentials from error messages and formats friendly messages."""
    if not err_str:
        return ""

    sanitized = str(err_str)

    # 1. Explicitly redact known secret values from config if provided
    if config and isinstance(config, dict):
        for key in ("password", "secret", "token", "s3_secret", "pg_password"):
            val = str(config.get(key, "")).strip()
            if val and len(val) >= 2:
                sanitized = sanitized.replace(val, "********")

    # 2. Regex redaction for credentials in queries, connection strings, and URIs
    sanitized = re.sub(r"(password\s*=\s*['\"]?)([^'\";\s]+)(['\"]?)", r"\1********\3", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"(secret\s*=\s*['\"]?)([^'\";\s]+)(['\"]?)", r"\1********\3", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"(SECRET\s+')[^']+(')", r"\1********\2", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"(://[^:]+:)[^@]+(@)", r"\1********\2", sanitized)

    lowered = sanitized.lower()

    # Determine type if not explicitly provided
    m_type = (mount_type or "").lower()
    if not m_type and config:
        if "host" in config or "database" in config:
            m_type = "postgres"
        elif "bucket" in config or "endpoint" in config:
            m_type = "s3"
        elif "path" in config:
            m_type = "sqlite"

    if m_type == "postgres" or "postgres" in lowered:
        host = (config.get("host") if config else "") or "host"
        port = (config.get("port") if config else "") or "5432"
        user = (config.get("user") if config else "") or ""
        database = (config.get("database") if config else "") or ""

        if "password authentication failed" in lowered:
            user_info = f" for user '{user}'" if user else ""
            return f"PostgreSQL authentication failed: Password authentication failed{user_info}. Please verify your credentials."

        if "connection refused" in lowered or "could not connect to server" in lowered or "is the server running" in lowered:
            return (
                f"PostgreSQL connection refused: Could not reach PostgreSQL server at '{host}:{port}'. "
                "Note: Inside Docker containers, '127.0.0.1' points to this container itself. Use '10.0.2.2' (Docker host gateway) "
                "or the Docker container name (e.g. 'pgvector_container')."
            )

        if "database" in lowered and "does not exist" in lowered:
            db_name = database or "specified database"
            return f"PostgreSQL error: Database '{db_name}' does not exist on '{host}:{port}'."

        if "timeout was reached" in lowered or "connection timed out" in lowered:
            return f"PostgreSQL connection timed out connecting to '{host}:{port}'. Please check network connectivity and firewall rules."

    elif m_type == "s3" or "s3" in lowered or "httpfs" in lowered:
        endpoint = (config.get("endpoint") if config else "") or ""
        if "invalid signature" in lowered or "403 forbidden" in lowered or "accessdenied" in lowered:
            return f"Access denied (403): Invalid Key ID or Secret for endpoint '{endpoint}'. Please verify your credentials."
        if "timeout was reached" in lowered or "could not resolve host" in lowered or "connection refused" in lowered:
            return f"Network connection failed to '{endpoint}'. Ensure host and port are reachable from the studio container."

    return sanitized


def mask_mount_record(mount: Dict[str, Any]) -> Dict[str, Any]:
    """Returns a shallow copy of mount with passwords/secrets masked for API responses."""
    m_copy = dict(mount)
    if "config" in m_copy and isinstance(m_copy["config"], dict):
        cfg_copy = dict(m_copy["config"])
        for secret_key in ("password", "secret", "token", "pg_password", "s3_secret"):
            if secret_key in cfg_copy and cfg_copy[secret_key]:
                cfg_copy[secret_key] = "********"
        m_copy["config"] = cfg_copy
    return m_copy


def get_default_mounts() -> List[Dict[str, Any]]:
    return []


def load_mounts() -> List[Dict[str, Any]]:
    os.makedirs(METADATA_DIR, exist_ok=True)
    if not os.path.exists(MOUNTS_FILE):
        defaults = get_default_mounts()
        save_mounts(defaults)
        return defaults
    try:
        with open(MOUNTS_FILE, "r") as f:
            data = json.load(f)
            return data.get("mounts", [])
    except Exception as e:
        logger.error(f"Failed to load storage_mounts.json: {e}")
        return get_default_mounts()


def save_mounts(mounts: List[Dict[str, Any]]):
    os.makedirs(METADATA_DIR, exist_ok=True)
    try:
        with open(MOUNTS_FILE, "w") as f:
            json.dump({"mounts": mounts, "updated_at": datetime.datetime.now().isoformat()}, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save storage_mounts.json: {e}")


def get_mount(mount_id: str) -> Optional[Dict[str, Any]]:
    mounts = load_mounts()
    return next((m for m in mounts if m["id"] == mount_id), None)


def create_or_update_mount(mount_data: Dict[str, Any]) -> Dict[str, Any]:
    mounts = load_mounts()
    mount_id = mount_data.get("id") or f"mount_{uuid.uuid4().hex[:8]}"
    catalog_name = mount_data.get("catalog_name", "").strip().lower().replace("-", "_").replace(" ", "_")
    if not catalog_name:
        catalog_name = f"mount_{uuid.uuid4().hex[:6]}"

    existing_idx = next((i for i, m in enumerate(mounts) if m["id"] == mount_id), -1)

    mount_record = {
        "id": mount_id,
        "name": mount_data.get("name", "External Storage Mount").strip(),
        "type": mount_data.get("type", "postgres").lower().strip(),
        "catalog_name": catalog_name,
        "read_only": bool(mount_data.get("read_only", True)),
        "enabled": bool(mount_data.get("enabled", True)),
        "description": mount_data.get("description", "").strip(),
        "config": dict(mount_data.get("config", {})),
        "status": mount_data.get("status", "ACTIVE"),
        "created_at": mount_data.get("created_at") or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_synced_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    if existing_idx >= 0:
        old_cfg = mounts[existing_idx].get("config", {})
        for secret_key in ("password", "secret", "token", "pg_password", "s3_secret"):
            val = mount_record["config"].get(secret_key)
            if val == "********" or val is None or val == "":
                mount_record["config"][secret_key] = old_cfg.get(secret_key, "")
        mounts[existing_idx] = mount_record
    elif mount_data.get("clone_from"):
        source_mount = next((m for m in mounts if m["id"] == mount_data.get("clone_from")), None)
        if source_mount:
            src_cfg = source_mount.get("config", {})
            for secret_key in ("password", "secret", "token", "pg_password", "s3_secret"):
                val = mount_record["config"].get(secret_key)
                if val == "********" or val is None or val == "":
                    mount_record["config"][secret_key] = src_cfg.get(secret_key, "")
        mounts.append(mount_record)
    else:
        mounts.append(mount_record)

    save_mounts(mounts)
    return mount_record


def duplicate_mount(mount_id: str, new_name: Optional[str] = None, new_catalog_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Creates a full duplicate copy of an existing mount record with credentials preserved."""
    mounts = load_mounts()
    source = next((m for m in mounts if m["id"] == mount_id), None)
    if not source:
        return None

    existing_cats = {m.get("catalog_name", "").lower() for m in mounts}
    base_name = source.get("name", "Storage Mount")
    base_cat = source.get("catalog_name", "mount")

    if not new_catalog_name:
        candidate_cat = f"{base_cat}_copy"
        idx = 2
        while candidate_cat in existing_cats:
            candidate_cat = f"{base_cat}_copy_{idx}"
            idx += 1
        new_catalog_name = candidate_cat

    if not new_name:
        new_name = f"{base_name} (Copy)"

    new_record = {
        "id": f"mount_{uuid.uuid4().hex[:8]}",
        "name": new_name,
        "type": source.get("type", "postgres"),
        "catalog_name": new_catalog_name,
        "read_only": source.get("read_only", True),
        "enabled": source.get("enabled", True),
        "description": source.get("description", ""),
        "config": dict(source.get("config", {})),
        "owner": source.get("owner", "admin"),
        "status": "ACTIVE",
        "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_synced_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    mounts.append(new_record)
    save_mounts(mounts)
    return new_record


def delete_mount(mount_id: str) -> bool:
    mounts = load_mounts()
    initial_len = len(mounts)
    mounts = [m for m in mounts if m["id"] != mount_id]
    if len(mounts) < initial_len:
        save_mounts(mounts)
        return True
    return False


def get_s3_storage_options(config: Dict[str, Any]) -> Dict[str, str]:
    """Generates standard AWS/S3 storage options dictionary for deltalake.DeltaTable and write_deltalake."""
    endpoint = config.get("endpoint", "garage:3900").strip()
    use_ssl = bool(config.get("use_ssl", False))
    if endpoint.lower().startswith("https://"):
        use_ssl = True
        endpoint = endpoint[8:]
    elif endpoint.lower().startswith("http://"):
        use_ssl = False
        endpoint = endpoint[7:]
    endpoint = endpoint.rstrip("/")
    protocol = "https://" if use_ssl else "http://"
    full_endpoint = f"{protocol}{endpoint}"

    return {
        "AWS_ENDPOINT_URL": full_endpoint,
        "AWS_ACCESS_KEY_ID": config.get("key_id", ""),
        "AWS_SECRET_ACCESS_KEY": config.get("secret", ""),
        "AWS_REGION": config.get("region", "us-east-1"),
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_ALLOW_HTTP": "true"
    }


def test_mount_connection(mount_data: Dict[str, Any]) -> Dict[str, Any]:
    """Tests connection to an external storage mount using an ephemeral DuckDB connection."""
    m_type = mount_data.get("type", "").lower().strip()
    config = dict(mount_data.get("config", {}))
    raw_catalog = mount_data.get("catalog_name", "test_mount").strip().lower()
    catalog_name = "".join(c if (c.isalnum() or c == "_") else "_" for c in raw_catalog)
    if not catalog_name:
        catalog_name = "test_mount"

    # If testing an edited or cloned mount, fill in secrets from existing mount if blank/masked
    ref_id = mount_data.get("id") or mount_data.get("clone_from")
    if ref_id:
        mounts = load_mounts()
        ref_mount = next((m for m in mounts if m["id"] == ref_id), None)
        if ref_mount:
            ref_cfg = ref_mount.get("config", {})
            for secret_key in ("password", "secret", "token", "pg_password", "s3_secret"):
                val = config.get(secret_key)
                if val == "********" or val is None or val == "":
                    config[secret_key] = ref_cfg.get(secret_key, "")

    test_conn = duckdb.connect()
    try:
        if m_type == "postgres":
            host = config.get("host", "127.0.0.1")
            port = int(config.get("port", 5432))
            database = config.get("database", "postgres")
            user = config.get("user", "postgres")
            password = config.get("password", "")
            
            test_conn.execute("INSTALL postgres; LOAD postgres;")
            attach_sql = (
                f"ATTACH 'dbname={database} host={host} port={port} user={user} password={password} connect_timeout=5' "
                f"AS \"{catalog_name}\" (TYPE postgres, READ_ONLY true);"
            )
            test_conn.execute(attach_sql)
            
            # Fetch tables
            tables = test_conn.execute(
                f"SELECT table_schema, table_name FROM information_schema.tables "
                f"WHERE table_catalog = '{catalog_name}' AND table_schema NOT IN ('information_schema', 'pg_catalog') "
                f"LIMIT 25"
            ).fetchall()
            
            table_list = [f"{s}.{t}" for s, t in tables]
            return {
                "success": True,
                "type": "postgres",
                "message": f"Successfully connected to PostgreSQL database '{database}' on {host}:{port}.",
                "tables": table_list,
                "table_count": len(table_list)
            }

        elif m_type == "s3":
            bucket = config.get("bucket", "").strip()
            endpoint = config.get("endpoint", "127.0.0.1:9000").strip()
            url_style = config.get("url_style", "path").strip()
            use_ssl_bool = bool(config.get("use_ssl", False))
            if endpoint.lower().startswith("https://"):
                use_ssl_bool = True
                endpoint = endpoint[8:]
            elif endpoint.lower().startswith("http://"):
                use_ssl_bool = False
                endpoint = endpoint[7:]
            endpoint = endpoint.rstrip("/")
            use_ssl = "true" if use_ssl_bool else "false"
            region = config.get("region", "us-east-1").strip()
            key_id = config.get("key_id", "").strip()
            secret = config.get("secret", "").strip()

            test_conn.execute("INSTALL httpfs; LOAD httpfs;")
            test_conn.execute("SET http_timeout = 5; SET http_retries = 1; SET http_keep_alive = false;")
            secret_name = f"test_{uuid.uuid4().hex[:8]}"
            secret_sql = f"""
            CREATE SECRET "{secret_name}" (
                TYPE S3,
                KEY_ID '{key_id}',
                SECRET '{secret}',
                ENDPOINT '{endpoint}',
                URL_STYLE '{url_style}',
                USE_SSL {use_ssl},
                REGION '{region}'
            );
            """
            test_conn.execute(secret_sql)
            
            # Test globbing bucket
            glob_path = f"s3://{bucket}/**"
            files = test_conn.execute(f"SELECT * FROM glob('{glob_path}') LIMIT 15").fetchall()
            file_list = [row[0] for row in files]

            return {
                "success": True,
                "type": "s3",
                "message": f"Successfully connected to S3/Garage endpoint '{endpoint}' bucket '{bucket}'. ({len(file_list)} object(s) found)",
                "files": file_list,
                "file_count": len(file_list)
            }

        elif m_type == "sqlite":
            path = config.get("path", "").strip()
            if not os.path.exists(path):
                return {
                    "success": False,
                    "error": f"SQLite database file does not exist at path '{path}'"
                }
            test_conn.execute("INSTALL sqlite; LOAD sqlite;")
            test_conn.execute(f"ATTACH '{path}' AS \"{catalog_name}\" (TYPE sqlite, READ_ONLY true);")
            tables = test_conn.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_catalog = '{catalog_name}'"
            ).fetchall()
            table_list = [t[0] for t in tables]
            return {
                "success": True,
                "type": "sqlite",
                "message": f"Successfully connected to SQLite database at '{path}'.",
                "tables": table_list,
                "table_count": len(table_list)
            }
        else:
            return {"success": False, "error": f"Unsupported mount type: {m_type}"}

    except Exception as e:
        clean_err = sanitize_connection_error(str(e), config=config, mount_type=m_type)
        logger.warning(f"Test mount failed for {m_type}: {clean_err}")
        return {"success": False, "error": clean_err}
    finally:
        try:
            test_conn.close()
        except Exception:
            pass


def attach_mount_to_duckdb(conn, mount: Dict[str, Any], force_reconnect: bool = False) -> bool:
    """Attaches a single storage mount to a live DuckDB connection or duckrun DuckSession."""
    if not mount.get("enabled", True):
        return False

    raw_conn = getattr(conn, "con", conn)
    m_type = mount.get("type", "").lower().strip()
    config = mount.get("config", {})
    raw_catalog = mount.get("catalog_name", "").strip().lower()
    catalog_name = "".join(c if (c.isalnum() or c == "_") else "_" for c in raw_catalog)
    read_only = mount.get("read_only", True)
    ro_str = "true" if read_only else "false"

    if not catalog_name:
        return False

    try:
        # Check attached databases and their readonly state
        db_rows = raw_conn.execute("SELECT database_name, readonly FROM duckdb_databases()").fetchall()
        db_readonly_map = {row[0]: bool(row[1]) for row in db_rows}

        if m_type == "postgres":
            if catalog_name in db_readonly_map:
                if db_readonly_map[catalog_name] == read_only and not force_reconnect:
                    return True
                # Read-only mismatch or force_reconnect: detach first
                try:
                    raw_conn.execute(f'DETACH "{catalog_name}"')
                    logger.info(f"Detached PostgreSQL mount '{catalog_name}' to update read_only state (target: {ro_str}).")
                except Exception as e_det:
                    logger.debug(f"Notice detaching '{catalog_name}': {e_det}")

            raw_conn.execute("INSTALL postgres; LOAD postgres;")
            host = config.get("host", "127.0.0.1")
            port = int(config.get("port", 5432))
            database = config.get("database", "postgres")
            user = config.get("user", "postgres")
            password = config.get("password", "")
            
            attach_sql = (
                f"ATTACH 'dbname={database} host={host} port={port} user={user} password={password} connect_timeout=5' "
                f"AS \"{catalog_name}\" (TYPE postgres, READ_ONLY {ro_str});"
            )
            raw_conn.execute(attach_sql)
            logger.info(f"Successfully attached PostgreSQL mount '{catalog_name}' (READ_ONLY: {ro_str})")
            return True

        elif m_type == "s3":
            raw_conn.execute("INSTALL httpfs; LOAD httpfs;")
            raw_conn.execute("SET http_timeout = 5; SET http_retries = 1; SET http_keep_alive = false;")
            bucket = config.get("bucket", "").strip()
            endpoint = config.get("endpoint", "127.0.0.1:9000").strip()
            url_style = config.get("url_style", "path").strip()
            use_ssl_bool = bool(config.get("use_ssl", False))
            if endpoint.lower().startswith("https://"):
                use_ssl_bool = True
                endpoint = endpoint[8:]
            elif endpoint.lower().startswith("http://"):
                use_ssl_bool = False
                endpoint = endpoint[7:]
            endpoint = endpoint.rstrip("/")
            use_ssl = "true" if use_ssl_bool else "false"
            region = config.get("region", "us-east-1").strip()
            key_id = config.get("key_id", "").strip()
            secret = config.get("secret", "").strip()

            secret_sql = f"""
            CREATE SECRET IF NOT EXISTS "{catalog_name}" (
                TYPE S3,
                KEY_ID '{key_id}',
                SECRET '{secret}',
                ENDPOINT '{endpoint}',
                URL_STYLE '{url_style}',
                USE_SSL {use_ssl},
                REGION '{region}'
            );
            """
            try:
                raw_conn.execute(secret_sql)
            except Exception as e:
                logger.debug(f"Secret {catalog_name} notice: {e}")

            so = get_s3_storage_options(config)

            # 1. Attach S3 Lakehouse catalog in DuckSession if available
            if hasattr(conn, "_catalogs"):
                if catalog_name not in conn._catalogs:
                    try:
                        conn.attach(f"s3://{bucket}", name=catalog_name, storage_options=so, read_only=read_only)
                        logger.info(f"Attached S3 Lakehouse catalog '{catalog_name}' to duckrun session.")
                    except Exception as e_att:
                        logger.warning(f"Could not attach S3 via duckrun ({e_att}), falling back to memory catalog.")

            # 2. Ensure catalog exists in DuckDB databases
            dbs_now = [row[0] for row in raw_conn.execute("SELECT database_name FROM duckdb_databases()").fetchall()]
            if catalog_name not in dbs_now:
                try:
                    raw_conn.execute(f"ATTACH ':memory:' AS \"{catalog_name}\";")
                except Exception as e_mem:
                    logger.warning(f"Could not ATTACH ':memory:' AS {catalog_name}: {e_mem}")

            # 3. Create schemas inside the attached catalog
            try:
                raw_conn.execute(f"CREATE SCHEMA IF NOT EXISTS \"{catalog_name}\".dbo;")
                if bucket:
                    raw_conn.execute(f"CREATE SCHEMA IF NOT EXISTS \"{catalog_name}\".\"{bucket}\";")
            except Exception as e_sch:
                logger.debug(f"Could not create schemas in {catalog_name}: {e_sch}")

            # 4. Discover and register Delta Lake tables
            try:
                delta_logs = raw_conn.execute(f"SELECT file FROM glob('s3://{bucket}/**/_delta_log/0*.json')").fetchall()
                seen_delta = set()
                for d_row in delta_logs:
                    fpath = d_row[0]
                    table_path = fpath.split("/_delta_log/")[0]
                    if table_path in seen_delta:
                        continue
                    seen_delta.add(table_path)

                    rel = table_path.replace(f"s3://{bucket}/", "").strip("/")
                    parts = rel.split("/")
                    if len(parts) >= 2:
                        sch = parts[0]
                        tbl = parts[1]
                    else:
                        sch = "dbo"
                        tbl = parts[0]

                    try:
                        raw_conn.execute(f"CREATE SCHEMA IF NOT EXISTS \"{catalog_name}\".\"{sch}\";")
                        raw_conn.execute(f"CREATE OR REPLACE VIEW \"{catalog_name}\".\"{sch}\".\"{tbl}\" AS SELECT * FROM delta_scan('{table_path}');")
                        if sch != "dbo":
                            raw_conn.execute(f"CREATE OR REPLACE VIEW \"{catalog_name}\".dbo.\"{tbl}\" AS SELECT * FROM delta_scan('{table_path}');")
                        raw_conn.execute(f"CREATE OR REPLACE VIEW \"{catalog_name}_{sch}_{tbl}\" AS SELECT * FROM delta_scan('{table_path}');")
                        raw_conn.execute(f"CREATE OR REPLACE VIEW \"{catalog_name}_{tbl}\" AS SELECT * FROM delta_scan('{table_path}');")
                    except Exception as e_v:
                        logger.debug(f"View creation notice for Delta table {table_path}: {e_v}")
            except Exception as e_dt:
                logger.debug(f"Delta table discovery notice for {catalog_name}: {e_dt}")

            # 5. Discover and register standalone Parquet files
            try:
                files = raw_conn.execute(f"SELECT file FROM glob('s3://{bucket}/**.parquet') LIMIT 20").fetchall()
                for f_row in files:
                    file_path = f_row[0]
                    if "_delta_log" in file_path:
                        continue
                    base_name = os.path.basename(file_path).replace(".parquet", "").replace("-", "_").replace(".", "_")
                    exact_name = os.path.basename(file_path)
                    try:
                        raw_conn.execute(f"CREATE OR REPLACE VIEW \"{catalog_name}\".dbo.\"{base_name}\" AS SELECT * FROM read_parquet('{file_path}');")
                        raw_conn.execute(f"CREATE OR REPLACE VIEW \"{catalog_name}\".dbo.\"{exact_name}\" AS SELECT * FROM read_parquet('{file_path}');")
                        if bucket:
                            raw_conn.execute(f"CREATE OR REPLACE VIEW \"{catalog_name}\".\"{bucket}\".\"{base_name}\" AS SELECT * FROM read_parquet('{file_path}');")
                            raw_conn.execute(f"CREATE OR REPLACE VIEW \"{catalog_name}\".\"{bucket}\".\"{exact_name}\" AS SELECT * FROM read_parquet('{file_path}');")
                        raw_conn.execute(f"CREATE OR REPLACE VIEW \"{catalog_name}_{base_name}\" AS SELECT * FROM read_parquet('{file_path}');")
                    except Exception:
                        pass
            except Exception as e_pq:
                logger.debug(f"S3 glob / view registration notice for {catalog_name}: {e_pq}")

            logger.info(f"Successfully configured and attached S3 mount '{catalog_name}'")
            return True

        elif m_type == "sqlite":
            if catalog_name in existing_dbs:
                return True
            path = config.get("path", "").strip()
            if os.path.exists(path):
                raw_conn.execute("INSTALL sqlite; LOAD sqlite;")
                raw_conn.execute(f"ATTACH '{path}' AS {catalog_name} (TYPE sqlite, READ_ONLY {ro_str});")
                logger.info(f"Successfully attached SQLite mount '{catalog_name}'")
                return True
            else:
                logger.warning(f"SQLite path '{path}' does not exist.")
                return False

    except Exception as e:
        clean_err = sanitize_connection_error(str(e), config=config, mount_type=m_type)
        logger.warning(f"Could not attach storage mount '{catalog_name}' ({m_type}): {clean_err}")
        return False

    return False


def sync_all_mounts(conn):
    """Iterates through all configured storage mounts and attaches them to DuckDB."""
    mounts = load_mounts()
    raw_conn = getattr(conn, "con", conn)

    # Detach any removed or renamed mount catalogs from DuckDB
    try:
        active_mount_cats = {m["catalog_name"] for m in mounts if m.get("enabled", True)}
        existing_dbs = [row[0] for row in raw_conn.execute("SELECT database_name FROM duckdb_databases()").fetchall()]
        for db_name in existing_dbs:
            if db_name not in ("system", "temp", "memory", "warehouse") and db_name not in active_mount_cats:
                try:
                    from web.warehouses import load_catalogs
                    wh_cats = {c["id"] for c in load_catalogs()}
                    if db_name not in wh_cats:
                        raw_conn.execute(f'DETACH "{db_name}"')
                        logger.info(f"Detached inactive or renamed mount catalog '{db_name}' from DuckDB.")
                except Exception as e_det:
                    logger.debug(f"Could not detach inactive mount '{db_name}': {e_det}")
    except Exception as e_clean:
        logger.debug(f"Mount detach check notice: {e_clean}")

    for mount in mounts:
        try:
            attach_mount_to_duckdb(conn, mount)
        except Exception as e:
            clean_err = sanitize_connection_error(str(e), config=mount.get("config"), mount_type=mount.get("type"))
            logger.warning(f"Failed to sync mount {mount.get('catalog_name')}: {clean_err}")


def get_mount_catalogs_metadata(conn) -> List[Dict[str, Any]]:
    """Inspects attached external mounts in DuckDB and extracts schema and table metadata for Catalog Explorer."""
    mounts = load_mounts()
    raw_conn = getattr(conn, "con", conn)
    catalogs_meta = []

    for mount in mounts:
        if not mount.get("enabled", True):
            continue

        cid = mount["catalog_name"]
        m_type = mount["type"]
        name = mount["name"]
        desc = mount.get("description", "")
        schemas_map: Dict[str, List[Dict[str, Any]]] = {}

        try:
            if m_type == "postgres":
                # Discover tables and detect declarative partitioning status
                try:
                    meta_query = f"""
                    SELECT 
                        c.relname,
                        n.nspname,
                        c.relkind,
                        c.relispartition,
                        p.relname AS parent_name
                    FROM {cid}.pg_catalog.pg_class c
                    JOIN {cid}.pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
                    LEFT JOIN {cid}.pg_catalog.pg_inherits i ON (i.inhrelid = c.oid)
                    LEFT JOIN {cid}.pg_catalog.pg_class p ON (i.inhparent = p.oid)
                    WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
                      AND c.relkind IN ('r', 'p', 'v')
                    ORDER BY n.nspname, c.relname;
                    """
                    pg_tables = raw_conn.execute(meta_query).fetchall()
                except Exception as e_meta:
                    logger.debug(f"Falling back to information_schema for mount '{cid}': {e_meta}")
                    try:
                        fallback = raw_conn.execute(
                            f"SELECT table_name, table_schema, 'r', false, NULL FROM information_schema.tables "
                            f"WHERE table_catalog = '{cid}' AND table_schema NOT IN ('information_schema', 'pg_catalog')"
                        ).fetchall()
                        pg_tables = fallback
                    except Exception:
                        pg_tables = []

                # Group child partitions under parents
                parent_partitions_map: Dict[str, List[str]] = {}
                for t_name, s_name, rkind, is_part, p_name in pg_tables:
                    if is_part and p_name:
                        key = f"{s_name}.{p_name}"
                        parent_partitions_map.setdefault(key, []).append(t_name)

                for table_name, schema_name, rkind, is_part, parent_name in pg_tables:
                    if schema_name not in schemas_map:
                        schemas_map[schema_name] = []
                    
                    # Row count estimate if available (skip querying child partitions directly to keep it fast)
                    row_count = 0
                    if not is_part:
                        try:
                            row_count = raw_conn.execute(f"SELECT COUNT(*) FROM {cid}.{schema_name}.{table_name}").fetchone()[0]
                        except Exception:
                            pass

                    is_partitioned = (rkind == 'p')
                    child_parts = parent_partitions_map.get(f"{schema_name}.{table_name}", [])

                    schemas_map[schema_name].append({
                        "catalog": cid,
                        "schema": schema_name,
                        "name": table_name,
                        "full_name": f"{cid}.{schema_name}.{table_name}",
                        "canonical_name": f"{cid}.{schema_name}.{table_name}",
                        "mount_type": "postgres",
                        "row_count": row_count,
                        "size_bytes": 0,
                        "is_federated": True,
                        "is_partitioned": is_partitioned,
                        "is_child_partition": bool(is_part),
                        "parent_table": parent_name,
                        "child_partitions": child_parts,
                        "partition_count": len(child_parts)
                    })

            elif m_type == "sqlite":
                tables = raw_conn.execute(
                    f"SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = '{cid}'"
                ).fetchall()
                for schema_name, table_name in tables:
                    s_name = schema_name or "main"
                    if s_name not in schemas_map:
                        schemas_map[s_name] = []
                    
                    row_count = 0
                    try:
                        row_count = raw_conn.execute(f"SELECT COUNT(*) FROM {cid}.{s_name}.{table_name}").fetchone()[0]
                    except Exception:
                        pass

                    schemas_map[s_name].append({
                        "catalog": cid,
                        "schema": s_name,
                        "name": table_name,
                        "full_name": f"{cid}.{s_name}.{table_name}",
                        "canonical_name": f"{cid}.{s_name}.{table_name}",
                        "mount_type": "sqlite",
                        "row_count": row_count,
                        "size_bytes": 0,
                        "is_federated": True
                    })

            elif m_type == "s3":
                bucket = mount["config"].get("bucket", "localspark")
                schemas_map[bucket] = []
                delta_dirs = set()

                # 1. Discover Delta Lake tables in S3 (folders containing _delta_log/*.json)
                try:
                    delta_logs = raw_conn.execute(f"SELECT file FROM glob('s3://{bucket}/**/_delta_log/0*.json')").fetchall()
                    for (log_file,) in delta_logs:
                        dt_path = log_file.split('/_delta_log/')[0]
                        delta_dirs.add(dt_path)
                except Exception as e:
                    logger.debug(f"Delta S3 glob notice for {cid}: {e}")

                for dt_path in sorted(delta_dirs):
                    rel_parts = dt_path.replace(f"s3://{bucket}/", "").split("/")
                    if len(rel_parts) >= 2:
                        s_name = rel_parts[0]
                        t_name = "/".join(rel_parts[1:])
                    else:
                        s_name = "dbo"
                        t_name = rel_parts[0]

                    if s_name not in schemas_map:
                        schemas_map[s_name] = []

                    row_count = None
                    try:
                        row_count = raw_conn.execute(f"SELECT COUNT(*) FROM delta_scan('{dt_path}')").fetchone()[0]
                    except Exception:
                        pass

                    clean_id = f"{cid}_{s_name}_{t_name}".replace("-", "_").replace(".", "_").replace("/", "_")
                    schemas_map[s_name].append({
                        "catalog": cid,
                        "schema": s_name,
                        "name": t_name,
                        "full_name": f"delta_scan('{dt_path}')",
                        "canonical_name": f"{cid}.{s_name}.{t_name}",
                        "view_name": clean_id,
                        "mount_type": "s3",
                        "format": "delta",
                        "is_delta": True,
                        "row_count": row_count,
                        "size_bytes": 0,
                        "path": dt_path,
                        "is_federated": True
                    })

                # 2. Discover standalone Parquet files in S3
                try:
                    s3_files = raw_conn.execute(f"SELECT file FROM glob('s3://{bucket}/**.parquet')").fetchall()
                    for (fpath,) in s3_files:
                        # Exclude files inside Delta table directories
                        if any(dt_dir in fpath for dt_dir in delta_dirs):
                            continue

                        fname = os.path.basename(fpath)
                        base_name = fname.replace(".parquet", "").replace("-", "_").replace(".", "_")
                        schemas_map[bucket].append({
                            "catalog": cid,
                            "schema": bucket,
                            "name": fname,
                            "full_name": f"read_parquet('s3://{bucket}/{fname}')",
                            "canonical_name": f"{cid}.{bucket}.{base_name}",
                            "view_name": f"{cid}_{base_name}",
                            "mount_type": "s3",
                            "format": "parquet",
                            "is_delta": False,
                            "row_count": None,
                            "size_bytes": 0,
                            "path": fpath,
                            "is_federated": True
                        })
                except Exception as e:
                    logger.debug(f"Could not list S3 files for metadata: {e}")

        except Exception as e:
            logger.warning(f"Error reading catalog metadata for mount {cid}: {e}")

        schemas_list = [{"name": s_name, "tables": s_tables, "models": []} for s_name, s_tables in sorted(schemas_map.items())]
        
        catalogs_meta.append({
            "id": cid,
            "name": name,
            "description": desc,
            "mount_id": mount["id"],
            "mount_type": m_type,
            "is_mounted": True,
            "read_only": mount.get("read_only", True),
            "owner": mount.get("owner", "admin"),
            "schemas": schemas_list,
            "table_count": sum(len(s["tables"]) for s in schemas_list),
            "model_count": 0
        })

    return catalogs_meta


def duckdb_to_postgres_type(dtype_str: str) -> str:
    """Maps DuckDB data types to equivalent PostgreSQL declarative column types."""
    dt = dtype_str.strip().upper()
    if dt.startswith("DECIMAL") or dt.startswith("NUMERIC"):
        return dt
    if dt.startswith("VARCHAR") or dt.startswith("TEXT") or dt.startswith("STRING"):
        return "TEXT"
    if "INT8" in dt or "BIGINT" in dt or "HUGEINT" in dt:
        return "BIGINT"
    if "INT4" in dt or "INTEGER" in dt or dt == "INT":
        return "INTEGER"
    if "INT2" in dt or "SMALLINT" in dt or "TINYINT" in dt or "INT1" in dt:
        return "SMALLINT"
    if dt == "UBIGINT":
        return "NUMERIC(20,0)"
    if dt == "UINTEGER":
        return "BIGINT"
    if dt == "USMALLINT":
        return "INTEGER"
    if dt == "UTINYINT":
        return "SMALLINT"
    if "DOUBLE" in dt or "FLOAT8" in dt:
        return "DOUBLE PRECISION"
    if "FLOAT" in dt or "REAL" in dt or "FLOAT4" in dt:
        return "REAL"
    if "BOOL" in dt:
        return "BOOLEAN"
    if dt == "DATE":
        return "DATE"
    if "TIMESTAMP WITH TIME ZONE" in dt or "TIMESTAMPTZ" in dt:
        return "TIMESTAMPTZ"
    if "TIMESTAMP" in dt:
        return "TIMESTAMP"
    if "TIME" in dt:
        return "TIME"
    if "BLOB" in dt or "BYTEA" in dt:
        return "BYTEA"
    if "JSON" in dt:
        return "JSONB"
    if "UUID" in dt:
        return "UUID"
    return "TEXT"


def execute_postgres_ddl(conn, catalog_name: str, ddl: str):
    """Executes native PostgreSQL DDL on the attached catalog via DuckDB's postgres_execute."""
    raw_conn = getattr(conn, "con", conn)
    escaped_ddl = ddl.replace("'", "''")
    call_sql = f"CALL postgres_execute('{catalog_name}', '{escaped_ddl}');"
    raw_conn.execute(call_sql)


def generate_monthly_slices(min_val, max_val, max_slices: int = 50) -> List[tuple]:
    """Generates monthly range slices [start_str, end_str) from min_val to max_val, capped at max_slices."""
    def to_date(v):
        if isinstance(v, datetime.datetime):
            return v.date()
        if isinstance(v, datetime.date):
            return v
        s = str(v)[:10]
        return datetime.date.fromisoformat(s)

    try:
        start_d = to_date(min_val)
        end_d = to_date(max_val)
    except Exception:
        return []

    if end_d < start_d:
        start_d, end_d = end_d, start_d

    slices = []
    curr_y = start_d.year
    curr_m = start_d.month

    while True:
        if curr_m == 12:
            next_y = curr_y + 1
            next_m = 1
        else:
            next_y = curr_y
            next_m = curr_m + 1

        start_str = f"{curr_y:04d}-{curr_m:02d}-01"
        end_str = f"{next_y:04d}-{next_m:02d}-01"
        slices.append((curr_y, curr_m, start_str, end_str))

        if len(slices) >= max_slices:
            break

        next_month_first = datetime.date(next_y, next_m, 1)
        if next_month_first > end_d:
            break

        curr_y = next_y
        curr_m = next_m

    return slices


def ingest_into_postgres_mount(
    conn,
    mount: Dict[str, Any],
    schema_name: str,
    table_name: str,
    source_sql: str,
    mode: str = "overwrite",
    partition_columns: Optional[List[str]] = None,
    max_partitions: int = 50
) -> Dict[str, Any]:
    """
    Ingests tabular data into an attached PostgreSQL mount with automated declarative partitioning translation.
    Supports PARTITION BY LIST (for categorical/discrete columns) and PARTITION BY RANGE (for dates/timestamps).
    Enforces authorization, creates child partition tables (capped at max_partitions), and always attaches a DEFAULT partition.
    """
    if mount.get("read_only", True):
        raise HTTPException(
            status_code=400,
            detail=f"Mounted catalog '{mount.get('name', mount.get('catalog_name'))}' is configured as read-only. Ingestion is not permitted. Please enable Read-Write access in Mount Settings."
        )

    # Ensure mount is attached to DuckDB with write capability
    attach_mount_to_duckdb(conn, mount, force_reconnect=False)
    catalog_name = mount.get("catalog_name") or mount.get("id")
    raw_conn = getattr(conn, "con", conn)
    cfg = mount.get("config", {})
    pg_user = cfg.get("user", "postgres")

    try:
        # 1. Analyze incoming schema from DuckDB source
        desc_rows = raw_conn.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
        col_names = [r[0] for r in desc_rows]
        col_types = {r[0]: str(r[1]).upper() for r in desc_rows}
        col_defs = [f'"{c}" {duckdb_to_postgres_type(col_types[c])}' for c in col_names]
        col_defs_sql = ", ".join(col_defs)

        # 2. Ensure destination schema exists
        execute_postgres_ddl(conn, catalog_name, f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')

        # 3. Determine partitioning strategy
        valid_parts = [c for c in (partition_columns or []) if c in col_types]
        partition_strategy = None
        child_partitions = []
        ddl_summary = ""

        if not valid_parts:
            # Case A: Standard unpartitioned table
            if mode == "overwrite":
                execute_postgres_ddl(conn, catalog_name, f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE;')
                execute_postgres_ddl(conn, catalog_name, f'CREATE TABLE "{schema_name}"."{table_name}" ({col_defs_sql});')
                ddl_summary = f'CREATE TABLE "{schema_name}"."{table_name}" ({col_defs_sql});'
            else:
                exists = raw_conn.execute(
                    f"SELECT 1 FROM information_schema.tables WHERE table_catalog = '{catalog_name}' AND table_schema = '{schema_name}' AND table_name = '{table_name}'"
                ).fetchall()
                if not exists:
                    execute_postgres_ddl(conn, catalog_name, f'CREATE TABLE "{schema_name}"."{table_name}" ({col_defs_sql});')
                    ddl_summary = f'CREATE TABLE "{schema_name}"."{table_name}" ({col_defs_sql});'
        else:
            # Case B: Declarative Partitioning Translation
            part_col = valid_parts[0]
            part_duck_type = col_types[part_col]
            is_temporal = any(t in part_duck_type for t in ("DATE", "TIMESTAMP", "TIME"))

            if is_temporal:
                # Subcase B1: Temporal column -> PARTITION BY RANGE
                partition_strategy = "RANGE"
                min_max = raw_conn.execute(
                    f'SELECT MIN("{part_col}"), MAX("{part_col}") FROM {source_sql} WHERE "{part_col}" IS NOT NULL'
                ).fetchone()

                slices = []
                if min_max and min_max[0] is not None and min_max[1] is not None:
                    slices = generate_monthly_slices(min_max[0], min_max[1], max_slices=max_partitions)

                if mode == "overwrite":
                    execute_postgres_ddl(conn, catalog_name, f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE;')
                    execute_postgres_ddl(conn, catalog_name, f'CREATE TABLE "{schema_name}"."{table_name}" ({col_defs_sql}) PARTITION BY RANGE ("{part_col}");')
                    ddl_summary = f'CREATE TABLE "{schema_name}"."{table_name}" (...) PARTITION BY RANGE ("{part_col}");'
                else:
                    exists = raw_conn.execute(
                        f"SELECT 1 FROM information_schema.tables WHERE table_catalog = '{catalog_name}' AND table_schema = '{schema_name}' AND table_name = '{table_name}'"
                    ).fetchall()
                    if not exists:
                        execute_postgres_ddl(conn, catalog_name, f'CREATE TABLE "{schema_name}"."{table_name}" ({col_defs_sql}) PARTITION BY RANGE ("{part_col}");')

                # Create monthly slice child partition tables
                for y, m, s_str, e_str in slices:
                    child_table = f"{table_name}_y{y}m{m:02d}"
                    child_ddl = f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{child_table}" PARTITION OF "{schema_name}"."{table_name}" FOR VALUES FROM (\'{s_str}\') TO (\'{e_str}\');'
                    execute_postgres_ddl(conn, catalog_name, child_ddl)
                    child_partitions.append(child_table)

                # Always attach default catch-all partition
                default_table = f"{table_name}_default"
                execute_postgres_ddl(conn, catalog_name, f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{default_table}" PARTITION OF "{schema_name}"."{table_name}" DEFAULT;')
                child_partitions.append(default_table)

            else:
                # Subcase B2: Categorical / Discrete -> PARTITION BY LIST
                partition_strategy = "LIST"
                distinct_rows = raw_conn.execute(
                    f'SELECT DISTINCT "{part_col}" FROM {source_sql} WHERE "{part_col}" IS NOT NULL LIMIT {max_partitions + 1}'
                ).fetchall()
                distinct_vals = [r[0] for r in distinct_rows]
                # Cap to top 50 values
                capped_vals = distinct_vals[:max_partitions]

                if mode == "overwrite":
                    execute_postgres_ddl(conn, catalog_name, f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE;')
                    execute_postgres_ddl(conn, catalog_name, f'CREATE TABLE "{schema_name}"."{table_name}" ({col_defs_sql}) PARTITION BY LIST ("{part_col}");')
                    ddl_summary = f'CREATE TABLE "{schema_name}"."{table_name}" (...) PARTITION BY LIST ("{part_col}");'
                else:
                    exists = raw_conn.execute(
                        f"SELECT 1 FROM information_schema.tables WHERE table_catalog = '{catalog_name}' AND table_schema = '{schema_name}' AND table_name = '{table_name}'"
                    ).fetchall()
                    if not exists:
                        execute_postgres_ddl(conn, catalog_name, f'CREATE TABLE "{schema_name}"."{table_name}" ({col_defs_sql}) PARTITION BY LIST ("{part_col}");')

                # Create child partition tables for distinct values
                for val in capped_vals:
                    # Sanitize table suffix
                    safe_slug = re.sub(r'[^a-zA-Z0-9_]', '_', str(val).lower()).strip('_')[:30] or "val"
                    child_table = f"{table_name}_p_{safe_slug}"

                    # Format literal value for PostgreSQL DDL
                    if isinstance(val, bool):
                        val_lit = "true" if val else "false"
                    elif isinstance(val, (int, float)):
                        val_lit = str(val)
                    else:
                        val_str = str(val).replace("'", "''")
                        val_lit = f"'{val_str}'"

                    child_ddl = f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{child_table}" PARTITION OF "{schema_name}"."{table_name}" FOR VALUES IN ({val_lit});'
                    execute_postgres_ddl(conn, catalog_name, child_ddl)
                    child_partitions.append(child_table)

                # Always attach default catch-all partition
                default_table = f"{table_name}_default"
                execute_postgres_ddl(conn, catalog_name, f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{default_table}" PARTITION OF "{schema_name}"."{table_name}" DEFAULT;')
                child_partitions.append(default_table)

        # 4. Stream bulk data from DuckDB into the PostgreSQL target table
        insert_sql = f'INSERT INTO "{catalog_name}"."{schema_name}"."{table_name}" SELECT * FROM {source_sql};'
        raw_conn.execute(insert_sql)

        # 5. Row count verification
        row_count = raw_conn.execute(f'SELECT COUNT(*) FROM "{catalog_name}"."{schema_name}"."{table_name}"').fetchone()[0]

        # 6. Retrieve verified child partitions from pg_catalog
        verified_partitions = []
        try:
            inherits_q = f"""
            SELECT c.relname
            FROM {catalog_name}.pg_catalog.pg_inherits i
            JOIN {catalog_name}.pg_catalog.pg_class c ON (i.inhrelid = c.oid)
            JOIN {catalog_name}.pg_catalog.pg_class p ON (i.inhparent = p.oid)
            JOIN {catalog_name}.pg_catalog.pg_namespace n ON (p.relnamespace = n.oid)
            WHERE n.nspname = '{schema_name}' AND p.relname = '{table_name}'
            ORDER BY c.relname;
            """
            verified_partitions = [r[0] for r in raw_conn.execute(inherits_q).fetchall()]
        except Exception:
            verified_partitions = child_partitions

        return {
            "success": True,
            "catalog": catalog_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "full_name": f"{catalog_name}.{schema_name}.{table_name}",
            "rows_ingested": row_count,
            "partition_strategy": partition_strategy,
            "partition_columns": valid_parts,
            "child_partitions": verified_partitions,
            "child_partitions_count": len(verified_partitions),
            "capped": len(distinct_rows) > max_partitions if (partition_strategy == "LIST" and 'distinct_rows' in locals()) else False,
            "ddl_summary": ddl_summary
        }

    except HTTPException:
        raise
    except Exception as e:
        err_str = str(e)
        logger.error(f"PostgreSQL ingestion error for '{catalog_name}.{schema_name}.{table_name}': {err_str}")
        lowered = err_str.lower()
        if "permission denied for schema" in lowered:
            raise HTTPException(
                status_code=403,
                detail=f"PostgreSQL Permission Denied: Account '{pg_user}' does not have CREATE/USAGE privileges on schema '{schema_name}'. Please grant privileges in PostgreSQL: GRANT USAGE, CREATE ON SCHEMA \"{schema_name}\" TO \"{pg_user}\";"
            )
        elif "permission denied for table" in lowered or "must be owner of table" in lowered or "must be table owner" in lowered:
            raise HTTPException(
                status_code=403,
                detail=f"PostgreSQL Permission Denied: Account '{pg_user}' lacks required table privileges on '{schema_name}.{table_name}'. Please ensure '{pg_user}' owns the table or has INSERT/ALL grants."
            )
        elif "read-only transaction" in lowered or "cannot execute in a read-only transaction" in lowered:
            raise HTTPException(
                status_code=403,
                detail=f"PostgreSQL Transaction Error: Server transaction is read-only. Please verify PostgreSQL role permissions or database read-only status."
            )
        else:
            clean_err = sanitize_connection_error(err_str, config=cfg, mount_type="postgres")
            raise HTTPException(status_code=400, detail=f"PostgreSQL Ingestion Error: {clean_err}")
