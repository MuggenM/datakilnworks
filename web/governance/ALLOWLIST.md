# Governance coverage allowlist

Every place in `web/` that executes SQL on a DuckDB connection is listed here or is *governed*
(its enclosing function calls the gateway: `govern_sql`, `governed_sql_or_raise`, `_gov_or_403`, `masked_relation`,
`mask_arrow`, ...). `scratch/test_governance_coverage.py` fails when a new, unreviewed site appears, so new data-reading
code cannot silently bypass column masking. To add a site: make it call the gateway, or add a row below with the
reason it does not need to. `file.py::*` covers every function in a file.

| Site | Status | Why it does not read governed data |
| :--- | :--- | :--- |
| alerts.py::* | sqlite | SQLite alert store; the alert query itself runs in `execute_alert_check`, which is governed as the alert's owner |
| autoloader.py::* | sqlite | SQLite checkpoint store |
| permissions.py::* | sqlite | Catalog ACL store (auth.db, SQLite); the SQL fence itself only parses text with sqlglot and executes nothing |
| app.py::search_principals_endpoint | sqlite | Looks up users in the SQLite account store for the share dialog |
| autoloader_s3.py::configure_duckdb | system | Only installs httpfs and an S3 secret on the ingestion's private in-memory connection; runs no data query |
| autoloader.py::_open_source_reader | system | Reads volume files it was told to ingest; writes tables, never reads tagged tables |
| experiments.py::* | sqlite | MLflow tracking store |
| lineage.py::* | sqlite | Lineage graph store (`parse_sql_lineage` only renders SQL text with sqlglot) |
| playground.py::* | sqlite | Prompt-playground store (SQLite); Delta metadata checks only |
| search.py::* | sqlite | Searches SQLite metadata stores |
| app.py::_execute_sync | governed | Runs the SQL rewritten by the gateway in `execute_sql` (also dispatched to workers / Ray) |
| app.py::analyze_partition_suitability | uploaded-file | Analyses a file the user just uploaded |
| app.py::drop_table_api | ddl | `DROP TABLE`, no data returned |
| app.py::get_column_query_frequency | sqlite | Query-history statistics (SQLite) |
| app.py::get_delta_compatible_source_sql | uploaded-file | Inspects the uploaded file's schema |
| app.py::get_duckrun_conn | setup | Connection setup (`SET`, catalog sync, mask installation) |
| app.py::get_settings_endpoint | sqlite | Application settings (SQLite) |
| app.py::update_settings_endpoint | sqlite | Application settings (SQLite) |
| app.py::get_table_details | metadata | Returns schema, counts, partitions and Delta history only, never row values |
| app.py::ingest_create | uploaded-file | Reads only `upload_<id>.<ext>` files (id validated); writes the target table |
| app.py::ingest_preview | uploaded-file | Previews the file being uploaded |
| app.py::normalize_uploaded_file | uploaded-file | Repairs an uploaded parquet file |
| compute_worker.py::execute_query | executor | Executes SQL the studio already rewrote (worker requires the compute token) |
| compute_worker.py::get_worker_conn | setup | Worker connection setup |
| copilot.py::get_autocomplete_metadata | metadata | `SHOW ALL TABLES`: names and types only |
| dbt_service.py::preview_cte_step | guarded-at-endpoint | The endpoint refuses principals subject to masking |
| dbt_service.py::preview_dbt_model_data | guarded-at-endpoint | The endpoint refuses principals subject to masking |
| dbt_service.py::preview_dbt_source | guarded-at-endpoint | The endpoint refuses principals subject to masking |
| mounts.py::attach_mount_to_duckdb | admin-operation | ATTACH / secrets for storage mounts (admin only) |
| mounts.py::execute_postgres_ddl | admin-operation | Admin DDL on a mounted Postgres |
| mounts.py::get_mount_catalogs_metadata | metadata | Lists remote catalogs/tables |
| mounts.py::ingest_into_postgres_mount | admin-operation | Writes uploaded data into a mount (admin only) |
| mounts.py::sync_all_mounts | setup | Attaches mounts at startup |
| mounts.py::test_mount_connection | admin-operation | Connection test (admin only) |
| onelake.py::query_with_duckdb | guarded-at-endpoint | The endpoint authenticates and refuses principals subject to masking |
| profiler.py::execute_profiled_query | executor | Callers pass SQL already rewritten by the gateway (`profile_sql`, history profile) |
| ray_engine.py::__init__ | setup | Actor setup (`SET threads/memory`, mask installation) |
| ray_engine.py::execute_query | executor | Executes SQL the studio already rewrote |
| volumes.py::preview_volume_file | volume-files | Volume files are not tagged data |
| warehouses.py::apply_warehouse_compute | setup | `SET threads` / `SET max_memory` |
| warehouses.py::sync_catalogs_with_duckrun | setup | ATTACH / DETACH of catalogs |
| workflow.py::init_runs_db | sqlite | Job-run store (SQLite) |
