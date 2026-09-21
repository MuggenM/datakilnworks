# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Data Kiln Works: a local Databricks-style lakehouse (Delta Lake tables on disk, DuckDB as the engine, SQLFrame for the Spark DataFrame API, no JVM). A FastAPI backend plus a single-page Alpine.js UI, and JupyterLab with a Databricks-compat shim. `README.md` has the full feature catalogue; `FEATURE_COMPARISON.md` maps features to Databricks/Snowflake.

## Running it

Everything runs in Docker. The Python dependencies (`requirements.txt`) are installed in the image, not necessarily on the host.

```bash
docker compose up -d            # build + start everything
docker compose logs -f datakilnworks-studio
docker compose exec datakilnworks-studio python scratch/test_sql_native_inference.py   # run one script
```

| Service | Port | Entry point |
| --- | --- | --- |
| `lakehouse-notebook` | 8890 (Jupyter, token `datakilnworks`) | Dockerfile CMD |
| `datakilnworks-studio` | 8891 | `uvicorn web.app:app --reload` |
| `compute-node-01/02/03` | 8001-8003 | `uvicorn web.compute_worker:app` |

- `./web`, `./notebooks`, `./warehouse` and `./docs` are bind-mounted, and the studio uses `--reload`, so edits to `web/*.py` apply without a rebuild. Only `requirements.txt`/`Dockerfile`/`config/00_databricks_shim.py` changes need `docker compose build`. The shim is copied into the image, not mounted.
- Swagger UI is at `/api/docs`; the built-in manual is served from `docs/index.html` at `/docs/`.
- There is no lint config, no pytest setup, and no CI. The tests are standalone scripts in `scratch/` (`test_*.py`, `verify_*.py`). They add the repo root to `sys.path` and are run directly with `python`. Some `verify_*_ui.py` scripts drive the UI.

## Architecture

**Backend (`web/`)**
- `web/app.py` (about 7,300 lines) holds the core FastAPI app and most routes (catalog explorer, ingestion, SQL execution, dashboards, time travel). Feature modules (`warehouses`, `workflow`, `genie`, `copilot`, `lineage`, `onelake`, `mounts`, `volumes`, `autoloader`, `serving`, `mlflow_shim`, `ai_sql`, `dbt_service`, and others) hold logic that `app.py` imports. Some modules define their own routers or route groups, so grep for the route path before assuming where it lives.
- Startup (`startup_event` in `app.py`) initializes the SQLite DBs and launches the background asyncio loops: cron scheduler, alerts, lineage scan, scheduled exports and the Auto Loader daemon. A new background service belongs there.
- Every module resolves `WAREHOUSE_DIR` (env, default `/workspace/warehouse`) on its own. Metadata is stored under `$WAREHOUSE_DIR/.metadata/`, as per-feature SQLite files (`auth.db`, `history.db`, `experiments.db`, `lineage.db`, `autoloader.db`, ...) and JSON files (`sql_warehouses.json`, `catalogs.json`). There is no central config or ORM, so each module owns its schema and its `init_*_db()`.

**Storage / namespace**
- Three-level `catalog.schema.table`. The default catalog's tables live directly at `$WAREHOUSE_DIR/<schema>/<table>` (Delta directories with `_delta_log/`). Additional catalogs live at `$WAREHOUSE_DIR/catalogs/{catalog_id}/`. Table-path resolution is repeated inline in many `app.py` handlers, with a fallback of `WAREHOUSE_DIR/<table>` when there is no schema dir, so keep them consistent when changing it.
- `mounts.py` and `onelake.py` mount external storage (OneLake/Fabric via `azure-storage-file-datalake`, plus others) as catalogs. OneLake paths need the `.Lakehouse` suffix and the `Tables/` prefix. The last several commits were fixes to exactly this.
- Delta reads and writes use `deltalake` (delta-rs) and `duckrun`, and DuckDB attaches catalogs for cross-catalog joins.

**Compute**
- A "SQL warehouse" (`warehouses.py`) is a named T-shirt-sized profile (threads plus `max_memory`, applied to DuckDB as `SET threads` and `SET max_memory`). The three defaults, `wh_starter`, `wh_analytics_pro` and `wh_etl_batch`, map to the `compute-node-0N` containers through the `WH_*_ENDPOINT` env vars. Queries can be forwarded over HTTP to `web/compute_worker.py`, or run in-process.
- `ray_engine.py` optionally scales each warehouse with Ray `DuckDBWorkerActor` pools for scatter-gather scans. It degrades gracefully when Ray isn't installed (`RAY_INSTALLED`).

**Notebooks**
- `config/00_databricks_shim.py` is an IPython startup hook. It injects `spark` (a SQLFrame session sharing the duckrun DuckDB connection), `dbutils`, `display()`, and the `%sql` magic. It also patches `createOrReplaceTempView` so SQLFrame DataFrames are visible to SQL. Notebooks under `notebooks/{Users,Shared}/` are run headless by `notebook_runner.py` (Papermill) and the workflow DAG engine (`workflow.py`).

**Frontend**
- `web/templates/index.html` is a single roughly 27k-line Jinja/Alpine.js file containing every view (Chart.js for charts, Monaco for SQL). Expect large, targeted edits with grep, not whole-file reads. Note the recent fix commits for Alpine expression and scope errors, since inline expressions are brittle.
- The MLflow shim (`mlflow_shim.py`), model serving (`serving.py`) and SQL-native inference functions (`ai_sql.py`, exposing `predict`, `ai_query` and similar as DuckDB UDFs) emulate Databricks MLflow, serving and AI functions locally. LLM backends (LM Studio and Ollama) are configured in `llm_settings.py`.

## Conventions

- Commit messages follow Conventional Commits (`fix(onelake): ...`, `feat(ui): ...`). The working branch is `development`; `main` is the PR target.
- Loggers are named `localspark.*`, a leftover from the project's earlier name. The Docker image is still tagged `localspark-lakehouse-notebook`, and compose references it, so don't rename it casually.
