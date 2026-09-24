# Data Kiln Works - Local Lakehouse & Studio (SQLFrame + DuckDB + duckrun)

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)

A lightweight, local data lakehouse platform that provides full Lakehouse and Spark DataFrame developer parity without requiring a JVM or cloud cluster infrastructure.

Includes **Data Kiln Works**—a powerful data lakehouse web workbench with **Data Ingestion ("Add Data" Wizard)**, **Unity Catalog Explorer**, **Monaco SQL Editor**, and **Delta Time-Travel Inspector**.

---

## 🏗️ Architecture

```text
┌──────────────────────────────────────────────────────────────────────────────────┐
│  DATA KILN WORKS WEB UI (Port 8891)                                              │
│  - Data Ingestion Wizard (CSV, TSV, Parquet, JSON drag-and-drop across Catalogs) │
│  - Lakeview Dashboards (KPI tiles, Bar, Line, Pie/Donut charts, Data tables)     │
│  - AI Query Assistant (Conversational Text-to-SQL with LM Studio, Ollama & charts)│
│  - SQL Warehouses Manager (Compute cluster sizing, vCPU threads & RAM controls)  │
│  - Query History & Audit Logging (SQLite WAL log with Warehouse & Catalog tags)  │
│  - Jobs & Pipelines / Local Workflows (DAG engine, Delta compaction & Papermill) │
│  - Unity Catalog Explorer (3-Level Namespace: catalog.schema.table, ACID log)    │
│  - Monaco SQL Editor (Live compute warehouse selector, cross-catalog joins)      │
│  - Workspace (In-Studio notebook runner, per-user folders, per-user kernels)     │
│  - Compute Monitor (DuckDB engine stats & active cluster pools)                  │
└────────────────────────┬─────────────────────────────────────────────────────────┘
                         │ REST APIs (FastAPI)
┌────────────────────────▼─────────────────────────────────────────────────────────┐
│  Compute Layer: Dynamic SQL Warehouses & Ray Distributed Compute Fabric           │
│  - Named Compute Endpoints: Serverless Starter, Analytics Pro, ETL Batch, Custom │
│  - Ray Distributed Compute Fabric: Dynamic DuckDBWorkerActor pools per Warehouse  │
│  - Elastic Sub-Second Scaling ([-] N w [+]): Zero container rebuilds or restarts   │
│  - Distributed Map-Reduce / Scatter-Gather: Parallel Delta Lake Parquet scanning  │
│  - Zero-Copy Apache Arrow table merging via Plasma Object Store                   │
│  - Heterogeneous Kubernetes Portability: k3s/k8s/KubeRay across ARM64 & x86_64    │
│  - Sizing T-Shirt Profiles: 2X-Small (1T/1GB) to 2X-Large (16T/32GB)             │
│  - Vectorized C++ execution, zero JVM overhead, dynamic SET threads & max_memory │
│  - Local Workflow DAG Scheduler & Headless Papermill Notebook runner             │
└────────────────────────┬─────────────────────────────────────────────────────────┘
                         │ Arrow C-Stream & In-Memory Attach
┌────────────────────────▼─────────────────────────────────────────────────────────┐
│  Storage Layer: Unity Catalog Multi-Warehouse Storage (duckrun + delta-rs)       │
│  - 3-Level Namespace Hierarchy: catalog.schema.table                             │
│  - Multi-Lakehouse Catalog Isolation: ./warehouse/catalogs/{catalog_id}/         │
│  - Sub-millisecond ATTACH for instant zero-copy cross-catalog joins              │
│  - Delta Lake ACID transaction logs, OCC commits, and time-travel snapshots      │
│  - Automated OPTIMIZE compaction & VACUUM storage maintenance                    │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quickstart

### 1. Launch Stack
```bash
cd /home/martin/volumes/datakilnworks
docker compose up -d
```

### 2. Available Interfaces

| Interface | URL | Credentials / Notes |
| :--- | :--- | :--- |
| **Data Kiln Works** | [http://localhost:8891](http://localhost:8891) | **No password required.** Includes Data Ingestion wizard, Unity Catalog tree, Monaco SQL Workbench, Time Travel, and Compute stats. |

---

## 🌟 Features Implemented

### 1. 📥 Data Ingestion ("Add Data" Wizard)
* **Drag-and-Drop File Upload**: Upload `.csv`, `.tsv`, `.parquet`, or `.json` files directly from your browser.
* **Auto-Schema Inference**: DuckDB analyzes file headers and types in-process without reading into memory.
* **Interactive Preview**: Inspect inferred column data types, nullability, and the first 25 sample rows before writing.
* **Destination Configuration**: Choose target catalog, schema (`dbo`, `bronze`, `silver`), table name, and write strategy (`Overwrite` vs `Append`).
* **ACID Delta Lake Materialization**: Creates a true Delta table with Parquet columnar data and `_delta_log/` transaction versioning via `duckrun` & `delta-rs`.
* **One-Click Transitions**: Jump immediately from ingestion into either the **Catalog Explorer** or the **Monaco SQL Editor**.

### 2. 📊 Lakeview BI Dashboards
* **Interactive Data Visualizations**: Built-in Chart.js charting engine rendering Bar charts, Line charts, Pie / Donut charts, and live tabular grids.
* **KPI Number Cards**: Single-metric KPI tiles with currency / unit formatting, sub-millisecond query latency badges, and query hints.
* **Live In-Process Query Execution**: Dashboards execute SQL queries directly against underlying Delta Lake tables via DuckDB's vectorized C++ engine, returning sub-10ms results without cluster startup delays.
* **Dashboard Management**: Switch between multiple dashboards, create new dashboards, and delete dashboards.
* **Widget Designer Modal**:
  - Add widgets on the fly with custom SQL queries.
  - Interactive "Test Query" button with live schema / column inference and execution latency stats.
  - Auto-detected X-Axis and Y-Axis column dropdown pickers.
  - Flexible layout support (1/4 width, half width, full width cards).
  - Quick query presets for instant exploration (Department Salary, NYSE Sector Cap, Product Valuation, Top Customers).
* **Persistent Storage**: Dashboard definitions and widget layouts are persisted in `warehouse/.metadata/dashboards.json`.

### 3. ⏱️ Query History & Audit Logging
* **Universal Execution Tracking**: Automatically logs every SQL statement executed across the Monaco SQL Workbench, Lakeview Dashboards, Data Ingestion, and JupyterLab notebooks (`%%sql`, `spark.sql()`, `conn.sql()`).
* **Persistent SQLite WAL Backend**: High-performance SQLite engine (`warehouse/.metadata/history.db`) operating with Write-Ahead Logging (`WAL`) to prevent lock contention between concurrent JupyterLab and FastAPI web workers.
* **Granular Audit Metrics**:
  - `query_id`, `query_text`, `executed_at`, `duration_ms`, `rows_produced`, `status` (`SUCCESS` / `FAILED`), `error_message`, `client` (`SQL_EDITOR`, `NOTEBOOK`, `DASHBOARD`, `INGESTION`), and `user`.
* **Search & Multi-Dimensional Filtering**:
  - Filter by execution status (`SUCCESS` vs `FAILED`).
  - Filter by execution source (`SQL Editor`, `Notebook`, `Dashboard`, `Ingestion`).
  - Filter by latency threshold (e.g. `>5ms`, `>20ms`, `>100ms`).
  - Substring search across SQL queries and error tracebacks.
* **Interactive Query Inspection**: Modal inspection with full SQL statement, copy-to-clipboard, execution metadata, and one-click "Open in Monaco SQL Editor".

### 4. 🔄 Jobs & Pipelines (Local Workflows / DLT)
* **Workflows & DLT Orchestration**: Full DAG orchestration engine running in-process without requiring external workflow tools (no Apache Airflow, Celery, or Redis dependencies).
* **Multi-Type Task Pipeline**:
  - **SQL Transformations (`sql`)**: Vectorized DuckDB execution (`CREATE OR REPLACE TABLE ... AS SELECT ...`).
  - **Delta Compaction & Vacuum (`optimize`)**: Native `dt.optimize.compact()` bin-packing and `dt.vacuum()` storage cleanup on Delta tables.
  - **Headless Notebook Execution (`notebook`)**: Headless `.ipynb` notebook runs via `papermill` with parameter injection and output recording.
  - **File Ingestion (`ingest`)**: Automated CSV/Parquet/JSON file ingestion to target Delta tables.
* **Topological DAG Scheduling**: Automatic dependency resolution and cycle detection executing tasks in deterministic order.
* **Dual Execution Triggers**:
  - **Manual Run ("Run Now")**: Immediate on-demand pipeline execution directly from the studio UI.
  - **Standard Cron Schedules**: Autonomous background cron loop powered by `croniter` (e.g., hourly `0 * * * *`, nightly `0 2 * * *`).
* **Interactive Visual Workflow Workbench**:
  - Visual DAG flow representation displaying task execution sequence and dependency hierarchy.
  - Interactive Pipeline Editor with one-click templates (**Medallion ETL**, **Delta Maintenance**, **Papermill Notebook**).
  - Detailed Execution Audit Trail tracking run status (`SUCCESS`, `FAILED`), execution duration, and per-task logs/outputs.

### 5. ✨ AI Query Assistant (Conversational Text-to-SQL)
* **Natural Language Exploration**: Chat directly with local Delta Lakehouse tables in conversational plain English.
* **Automatic Schema Extraction & Value Sampling**: Backend extracts table definitions, column types, nullability, and actual sample rows from Unity Catalog metadata to ground LLM completions in real schema context.
* **Vectorized DuckDB SQL Generation**: Generates high-efficiency DuckDB-compatible SQL queries with automatic self-correction against syntax or column hallucinations.
* **Integrated Data & Chart Visualizations**:
  - Automatically recommends and renders visual charts (Bar, Line, Donut) or data tables based on query shape.
  - Interactive chart metric toggles and dynamic type switching directly in the chat stream.
* **Flexible Provider Support**:
  - **Local LM Studio**: Auto-detects local host LM Studio instances (e.g. `http://localhost:1234`), enumerates loaded VRAM models with `⚡ (Loaded)` prioritization, and generates zero-latency local queries via OpenAI-compatible endpoints with 100% data privacy.
  - **Local Ollama**: Auto-detects local host Ollama instances (`qwen2.5-coder:latest`, `llama3.2:3b`, `deepseek-r1:latest`, `qwen3.6:latest`, etc.) with zero cloud data egress.
  - **Cloud LLMs**: Optional API support for OpenAI (`gpt-4o-mini`), Google Gemini (`gemini-1.5-flash`), and Anthropic Claude.
  - **Instant Rule-Based Heuristics**: Built-in offline fallback parser ensuring zero-latency responses for common queries even when models are offline.
* **Persistent Multi-Turn Conversations**: Preserves chat threads and execution history across sessions in `warehouse/.metadata/genie_chats.json`.

### 6. 🗄️ Unity Catalog (Multi-Catalog Lakehouses)
* **3-Level Namespace (`catalog.schema.table`)**: Full Databricks Unity Catalog namespace support across multiple storage warehouses (e.g. `warehouse.dbo.silver_employees`, `dev_catalog.dbo.test_dev`, `finance_lake.invoicing.invoices`).
* **Multi-Catalog Storage Isolation**: Catalogs are stored as isolated Delta Lake directories in `./warehouse/catalogs/{catalog_id}/`.
* **Zero-Copy Cross-Catalog Joins**: Secondary lakehouses are mounted via DuckDB's in-memory `ATTACH` mechanism, allowing seamless cross-catalog queries, joins, and aggregations in a single SQL statement.
* **Catalog & Schema Management**:
  - Create new catalogs (`+ Catalog`) with custom descriptions and storage isolation.
  - Create new schemas (`+ Schema`) within any existing catalog.
  - **Collapsible Hierarchical Tree**: Interactive carets allow collapsing/expanding individual schemas (e.g., `dbo`) and entire catalogs to manage large namespaces, complete with a one-click "Expand / Collapse All" toggle in the header.
* **Schema & Metadata Explorer**: Column names, data types, nullability, and storage footprint.
* **Sample Data Preview**: Instant vectorized preview of Delta table rows with zero cold start.
* **Delta Lake History & Time Travel**: Inspect all immutable ACID transaction commits (`WRITE`, `UPDATE`, `MERGE`), commit timestamps, affected rows, and one-click "Query this Version (Time Travel)".

### 7. ⚡ SQL Warehouses (Dynamic Compute Sizing & Ray Elastic Pools)
* **Dedicated Compute Endpoints**: Multiple named SQL Warehouses (`Serverless Starter`, `Analytics Pro`, `ETL & Maintenance`, or custom).
* **Configurable T-Shirt Cluster Sizing**:
  - `2X-Small`: 1 vCPU, 1GB RAM (Light ad-hoc exploration)
  - `X-Small`: 2 vCPU, 2GB RAM (Standard queries)
  - `Small`: 2 vCPU, 4GB RAM (Default interactive warehouse)
  - `Medium`: 4 vCPU, 8GB RAM (ETL & aggregation jobs)
  - `Large`: 8 vCPU, 16GB RAM (Heavy joins & reporting)
  - `2X-Large`: 16 vCPU, 32GB RAM (Max local parallelism)
  - `Custom`: Set exact thread count (1–64) and RAM limits (`4GB`, `16GB`, etc.)
* **Dynamic In-Process Enforcement**: Applied on-the-fly to the DuckDB connection via vectorized `SET threads = N` and `SET max_memory = 'XGB'` with zero container rebuilds or restarts.
* **Elastic Ray Actor Pools (Scale-on-the-Fly)**: Seamlessly scale warehouse worker instances dynamically (`[-] N w [+]`) from 0 to 16+ Ray-orchestrated DuckDB worker actors in sub-20 milliseconds. See [Section 22](#22-⚡-ray-distributed-compute-engine--kubernetes-scaling-strategy) for full Kubernetes deployment details.
* **Lifecycle Management**:
  - Start, stop, edit, and delete warehouses from the dedicated **SQL Warehouses** workbench tab.
  - Configurable auto-stop timeouts (5m, 10m, 15m, 30m, 1h, Never) to release compute resources.
  - Real-time metrics overview: Total Warehouses, Active Running Clusters, Total Allocated Cores, and Processed Query Counter.
* **Query Attribution**: Every query records the executing SQL Warehouse ID in SQLite audit logs.

### 8. 💻 Monaco SQL Editor
* Powered by Monaco Editor (the engine behind VS Code and Databricks).
* **Live Warehouse Switcher**: Dropdown in the top toolbar to route queries through any configured SQL Warehouse with running/stopped status indicators.
* **Quick Cross-Catalog Snippets**: One-click snippets for table inspection, cross-catalog joins, and `SHOW TABLES`.
* Keyboard shortcut: <kbd>Ctrl+Enter</kbd> or <kbd>Cmd+Enter</kbd> to execute queries.
* Execution time metrics (latency in milliseconds, rows returned).
* Results table with sticky headers and **Export CSV**.
* **Saved Queries Integration**: Save queries directly with <kbd>Ctrl+S</kbd>, quick-switch between saved templates via toolbar dropdown, and track active query state with unsaved change badges.

### 9. 📜 Saved Queries Library
* **Centralized Query Catalog**: Persistent storage of named, documented, and tagged SQL statements stored in `warehouse/.metadata/saved_queries.json`.
* **Dedicated Workbench View (`SQL & BI > Queries`)**:
  - Filterable master table with query search and category tag pills (`All`, `HR`, `Finance`, `Ops`, `Dev`, `IoT`).
  - Metric summary cards: Total Library Queries, Total Executions, Unique Tags, and Most Active Query.
  - Quick action buttons: **Run in SQL Editor** (instant vectorized execution), **Open in Editor**, **Edit Metadata**, **Duplicate Query**, and **Delete**.
  - Collapsible SQL Preview Drawer with line numbers, metadata badges, execution timestamps, and one-click **Copy SQL**.
* **Monaco SQL Editor Integration**:
  - **Save (<kbd>Ctrl+S</kbd> / <kbd>Cmd+S</kbd>)**: Save updates in-place directly from Monaco editor or prompt for initial save.
  - **Save As...**: Clone and save the active SQL query as a new reusable library template.
  - **Active Query Indicator**: Dynamic pill showing current query name and unsaved modification flag (`*`) with detach capability.
  - **Saved Queries Quick-Selector**: Instant dropdown in the SQL Editor toolbar to switch queries without leaving the editor.
* **Execution Tracking**: Executing a saved query automatically increments its `run_count` and updates its `last_run_at` timestamp across all sessions.
* **Universal Search Priority**: Indexed with top relevance ranking in the global <kbd>Ctrl+P</kbd> search palette with amber `SAVED` badges.

### 10. 🔍 Universal Search (`Ctrl+P`) Command Palette
* **Spotlight Modal**: Global shortcut <kbd>Ctrl+P</kbd> or <kbd>Cmd+P</kbd> opens a centered search palette from any view.
* **Unified 8-Way Entity Search**:
  - **Saved Queries**: Top-priority library queries with direct execution in the SQL Editor.
  - **Tables & Catalogs**: Search across all lakehouse catalogs with direct navigation to table inspection.
  - **Columns & Data Types**: Instant fuzzy search across table schemas (e.g. `price`, `salary`, `market_cap_b`) with direct routing to the Schema tab.
  - **Notebooks**: Recursive discovery in `./notebooks/*.ipynb` opened in the in-Studio notebook runner.
  - **Lakeview Dashboards**: Match by dashboard names and widget titles.
  - **Jobs & Pipelines**: Match workflow DAGs and individual task names.
  - **Query History**: Search historical SQL executions from SQLite WAL logs with 1-click loading into the Monaco SQL Editor.
  - **SQL Warehouses**: Search compute profiles and active cluster endpoints.
* **Sub-Millisecond Response**: In-memory metadata caching delivering sub-millisecond search latencies (< 1ms).
* **Keyboard Navigation**: Full arrow key navigation (<kbd>↑</kbd> / <kbd>↓</kbd>), <kbd>Enter</kbd> to open, <kbd>Shift+Enter</kbd> to query table in editor, <kbd>Tab</kbd> to cycle categories, and <kbd>Esc</kbd> to close.

### 11. 📁 Workspace Browser & Native In-Studio Notebook Runner
* **Native In-Studio Notebook Execution (no separate Jupyter server)**:
  - **Direct In-Browser Cell Execution**: Execute any code cell directly inside the Data Kiln Works right-pane with the **▶ Run** button or <kbd>Shift+Enter</kbd> / <kbd>Ctrl+Enter</kbd>.
  - **Persistent Stateful Kernel Sessions**: Built-in `IPython` / `ipykernel` runner maintains live in-memory state across cell executions (variables, DataFrames, and imports persist from cell to cell).
  - **Pre-Loaded Databricks Globals**: Native access to `spark` (SQLFrame session), `conn` (duckrun Delta session), `dbutils`, `display()` (rich DataTables & HTML previews), and `%sql` / `%%sql` magics.
  - **Sequential "▶ Run All"**: Execute all cells in order with a single click, with live execution spinners and cumulative runtime tracking.
  - **Editable Cells**: Live in-browser code editor with auto-growing textarea, code syntax formatting, auto-saving, and quick cell management (`+ Code`, `+ Text`, `Delete Cell`, `Copy Source`).
  - **Rich Multi-MIME Outputs**: Live output rendering for stdout/stderr console streams, interactive HTML tables, Matplotlib/Seaborn charts (`image/png`), and formatted tracebacks with ANSI code stripping.
  - **Automatic Disk Persistence**: Updates `.ipynb` files on the host filesystem immediately with captured outputs, execution counters (`In [N]:`), and user edits.
  - **Kernel Management**: Live status indicator (`● Kernel: Idle`, `● Kernel: Busy`, `○ Kernel: Stopped`), one-click **Restart Kernel**, and **Clear Outputs**.
* **Standard Databricks Workspace Layout**:
  - Auto-scaffolds standard Databricks folder layout in `./notebooks/`: `Users/{user}/` (user scratchpads) and `Shared/` (shared team transforms and utilities).
* **Two-Pane Workspace Workbench**:
  - **Left Pane (Hierarchical Tree Browser)**:
    - Expandable and collapsible directory tree with persistent open/closed state.
    - Live file filter search box to quickly locate notebooks and scripts across nested directories.
    - Type-specific icons: Jupyter Notebooks (`.ipynb`, purple), Python scripts (`.py`, blue), SQL scripts (`.sql`, green), and Folders (amber).
    - Quick actions per item: New Notebook inside folder, Rename, and Delete.
  - **Right Pane (In-Studio Workbench)**:
    - Dedicated interactive notebook environment, text/script viewer with one-click "Query in Monaco SQL Editor", and folder overview cards.
* **Workspace Management Operations**:
  - `+ New Notebook`: Creates valid `.ipynb` notebooks with Python 3 kernel metadata directly in the chosen folder.
  - `+ New Folder`: Creates nested subdirectories on the local filesystem.
  - `Upload`: Upload notebooks or scripts from your desktop directly into any workspace folder.
  - In-place renaming and deletion with confirmation safeguards and path traversal protection.
* **Per-user access**: every notebook, file and kernel endpoint requires a valid session and only serves `Users/<you>/` and `Shared/` (admins see everything). Kernels are per user, so two people opening the same Shared notebook never share variables. There is deliberately no JupyterLab server: one shared server saw every user's folder plus the warehouse files.
* **Running notebooks and column masking**: notebook kernels can read the warehouse files directly, so masking cannot cover them. Users that a masking policy applies to therefore never get such a kernel: by default (`GOVERNANCE_NOTEBOOK_EXECUTION=sandbox`) their notebooks run in the **notebook sandbox**, a separate container with no warehouse mount (see *Notebook sandbox* below). Everyone else runs in the Studio's own kernels. If the sandbox is not running, masked users can open and edit notebooks but not run them.
* **Universal Search (`Ctrl+P`) Deep-Linking**: Selecting any notebook result in Universal Search opens it immediately in the interactive in-studio notebook runner.

### 12. 🎨 Data Kiln Works UI/UX & Themes
* **Workspace Default Landing & Session Persistence**: Page reloads now land directly on the **Workspace Browser** instead of Catalog Explorer. Active view state is stored in `localStorage` (`dbx_current_view`), ensuring browser refreshes never lose your active tab.
* **Dark / Light Mode Theme Switcher**: Top navigation header features an instant theme toggle (<i class="ph ph-sun"></i> / <i class="ph ph-moon"></i>) with FOUC prevention, persistent preference storage in `localStorage` (`dbx_theme`), and live Monaco SQL editor theme switching (`vs-dark` vs `vs`).

### 13. ⚙️ Compute & Engine Monitor
* Active DuckDB engine version, duckrun adapter version, total registered catalogs, and active tables.

### 14. 🏛️ Data Governance & Interactive Lakehouse Lineage (Unity Catalog Parity)
* **Automated AST Pipeline Parsing**: `sqlglot` DuckDB dialect parser continuously extracts lineage edges from SQL `CREATE TABLE AS SELECT`, `INSERT INTO`, `MERGE INTO`, `SELECT ... FROM read_csv/read_parquet`, jobs, and Lakeview Dashboards into an embedded SQLite graph store.
* **5-Stage Medallion Dependency DAG**: Visualizes end-to-end data lifecycle across `1. Sources & Files` $\rightarrow$ `2. Bronze Layer (Raw)` $\rightarrow$ `3. Silver Layer (Cleaned)` $\rightarrow$ `4. Gold Layer (Business Aggregates)` $\rightarrow$ `5. Dashboards & BI`.
* **Global Lineage Explorer & Search**: Dedicated navigation link (`/lineage`), stage column filtering, real-time node search, and Slide-over Node Inspector displaying full schema metadata, upstream sources, downstream consumers, and direct navigation links.
* **In-Catalog Table Lineage View**: Unity Catalog Table detail view includes a dedicated **Lineage Graph** tab highlighting the active focus table with connected upstream inputs and downstream targets.
* **Automated Blast Radius (Impact Analysis)**: One-click downstream impact analysis calculates every dependent table, pipeline, and BI dashboard that would be affected by schema changes or upstream disruptions.

### 15. ⏱️ Delta Lake Time-Travel Visual Diff & 1-Click ACID Restore
* **Visual Diff Engine**: Instant sub-second comparison between any two historical Delta Lake transaction log commits (`vA` $\rightarrow$ `vB`).
  - **KPI Metrics**: Total historical rows, target rows, net row diff, added rows count, deleted rows count.
  - **Row Mutations**: Interactive paginated tables displaying exact added rows (green `+`) and removed rows (red `-`).
  - **Schema Evolution Detection**: Automated detection of added columns, removed columns, and mutated data types between Delta versions.
* **1-Click ACID Rollback**: Roll back any table to a historical version with zero data copying or destructive file deletion. Powered by `deltalake.DeltaTable.restore()`, this appends an immutable, audited `RESTORE` transaction commit into the Delta Lake transaction log (`_delta_log/`).
* **Delta History Actions**: One-click **Diff** inspection, snapshot **Query** generation (`SELECT * FROM delta_scan(..., version => N)`), and instant **Restore** confirmation modal directly within Unity Catalog table history.

* **Delta Shallow Clone** (`web/table_clone.py`): `CREATE [OR REPLACE] TABLE [IF NOT EXISTS] t SHALLOW CLONE src [VERSION AS OF n | TIMESTAMP AS OF '...']` from the SQL editor, a **Clone** button in the Catalog explorer, or `POST /api/table/{schema}/{table}/clone`. The clone is a new Delta table with its own log (version 0 is an audited `CLONE` commit) that shares the source's Parquet files, so it copies no data and costs no storage until it diverges.
  - **Hard links rather than path references**: neither delta-rs nor DuckDB follows an absolute path into another table, and a reference-based clone breaks when the source is vacuumed. Hard-linking the immutable data files keeps the clone an ordinary table for every reader and keeps it valid if the source is vacuumed, rewritten or dropped. Requires local catalogs on one filesystem; tables with deletion vectors or row tracking are refused; file statistics are preserved exactly; works on partitioned tables, at any version still covered by the log, and for a clone of a clone.
  - **Governed**: a user a masking or row-filter policy applies to cannot clone (it would hand them the raw files). Everyone else's clone receives the source's effective tags, including schema/catalog-inherited ones, before it exists, so policies keep applying to it.

### 16. ⚡ Visual Query Execution Profiler (DuckDB Vectorized Plan Inspector)
* **Vectorized Execution Tree**: Inspect DuckDB physical operator trees (Delta Scans, Hash Joins, Hash Group-By Aggregations, Filters, Order-By Sorts, and Projections) rendered with hierarchical visual indentation and category color-coding.
* **Automated Bottleneck Detection**: Automatically detects and highlights the slowest operator consuming the highest percentage of query latency, accompanied by a prominent warning banner and actionable performance diagnostics.
* **Rich Operator Telemetry**:
  - Cardinality & Selectivity: Input rows vs. output rows and filter selectivity ratios.
  - Granular Timings: Execution time per operator (in $\mu$s, ms, or s) and cumulative query time percentage.
  - Delta Lake Pruning Metrics: File pruning statistics (e.g. `Scanning Files: 1/4`) showing vectorized file skipping.
  - Expandable Node Details: Deep inspection of internal DuckDB expressions, hash keys, aggregate functions, and projection lists.
* **Dual Integration Points**:
  - **Monaco SQL Editor**: Dedicated **⚡ Profile** toolbar button, "Table" vs. "Query Profile" results tabs, and instant latency pill.
  - **Query History & Audit Log**: **Profile** action button on every historical query and interactive plan tree in the Query Execution Details modal.

### 17. 📊 Interactive Dashboard Parameters & Cross-Filtering (Lakeview Parity)
* **Global Parameters Toolbar**: Top filter ribbon on Lakeview Dashboards featuring dynamic category dropdowns (`department`, `sector`, `category`), quick date range presets (`All Time`, `Last 7D`, `Last 30D`, `YTD`), and instant filter reset.
* **Dynamic Warehouse Discovery**: Dropdowns automatically query distinct dimension values from underlying Delta Lake warehouse tables without manual configuration.
* **Interactive Chart Cross-Filtering**: Direct click-to-filter on Chart.js visualizations (bar charts, pie charts, donut charts). Clicking any segment automatically applies the clicked dimension as an active filter across all widgets on the dashboard.
* **Active Filter Pills & Reset**: Visual badge displaying active cross-filtered dimensions with 1-click dismissal (`×`) and a global **Reset Filters** action.
* **Safe Parameter Substitution**: Backend SQL parameter compiler supports `:param` and `{{param}}` syntax with parameterized sanitization and null-safe execution (`(:param IS NULL OR col = :param)`).

### 18. 🧠 Monaco Context-Aware Autocomplete (Unity Catalog Parity)
* **Live Catalog Metadata Ingestion**: Dynamically indexes all catalogs, schemas, tables, and column schemas with data types (`BIGINT`, `VARCHAR`, `TIMESTAMP`, etc.) from DuckDB and Unity Catalog via fast in-memory caching.
* **Clause-Aware Completion**:
  - Automatically suggests tables and views (with row count and column previews) after `FROM`, `JOIN`, `INTO`, or `TABLE`.
  - Prioritizes table columns (with data types) after `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `HAVING`, and `ON`.
  - **Rich DuckDB Function Snippets**: Autocompletes analytical, mathematical, date, and Delta Lake time-travel functions (`ROUND`, `DATE_TRUNC`, `COUNT(*)`, `delta_scan`, etc.) with multi-stop tab navigation.
  - Scoped column completion on table or alias dot-access (e.g. `silver_employees.` or `emp.`).

### 19. ✨ Inline Copilot (<kbd>Ctrl+I</kbd>)
* **Keyboard-First In-Editor Assistant**: Invoked via <kbd>Ctrl+I</kbd> / <kbd>Cmd+I</kbd> or the **✨ Copilot** button in the SQL Editor toolbar.
* **Schema-Injected NL-to-SQL Synthesis**: Translates natural language requests into production-grade DuckDB SQL using active Lakehouse table definitions, column types, and editor context.
* **Multi-Model Provider Negotiation**: Compatible with local LLMs (Ollama `qwen2.5-coder`, `llama3.2`, LM Studio), remote cloud APIs (OpenAI, Gemini, Claude), and an intelligent offline heuristic rule engine.
* **Interactive Action Workflow**:
  - Monospace SQL preview block with syntax styling and plain English logic explanations.
  - One-click insertion options: **Insert at Caret** (<kbd>Enter</kbd>), **Replace Query** (<kbd>Shift+Enter</kbd>), and **Run Immediately** (<kbd>Ctrl+Enter</kbd>).

### 20. 🌐 External Storage & DB Mounts (Zero-Copy Lakehouse Federation)
* **Zero-Copy Cross-Database Federation**: Query external operational databases and object storage buckets directly inside DuckDB as attached catalogs without ETL pipelines or data duplication.
* **Native Connector Support**:
  - **PostgreSQL**: Vectorized federation via DuckDB's native `postgres` extension (`ATTACH 'dbname=... host=... user=...' AS pg_local (TYPE postgres)`). Allows instant zero-copy queries against live relational tables and pgvector stores.
  - **S3 / MinIO / Garage S3**: Direct object storage queries via DuckDB's native `httpfs` extension and S3 secrets configuration (`CREATE SECRET (TYPE s3, KEY_ID '...', SECRET '...', ENDPOINT '...', URL_STYLE 'path')`). Exposes remote Parquet/Delta buckets as first-class schemas.
  - **SQLite**: Local relational database attachment (`ATTACH '/path/to/db.sqlite' AS sqlite_db (TYPE sqlite)`).
* **Unity Catalog 3-Level Namespace Integration**: Mounted external databases automatically populate the Unity Catalog hierarchy (`pg_local.public.demo_data`, `garage_s3.localspark.demo_metrics.parquet`) alongside local Delta Lakehouses, complete with catalog type badges (`POSTGRES`, `GARAGE S3`, `SQLITE`).
* **Interactive Mount Manager**:
  - Dedicated **Mount** modal in the Unity Catalog toolbar with multi-database tab configuration.
  - Live connection testing (`POST /api/mounts/test`) validating credentials and discovering remote tables/files before mounting.
  - Active mount registry management (`warehouse/.metadata/mounts.json`) with auto-attachment across all SQL warehouse sessions.
* **Monaco SQL & Copilot Autocomplete**: Tables and columns from external mounts are automatically indexed into Monaco Editor context-aware autocomplete and Copilot prompts for seamless cross-database joins (`SELECT * FROM warehouse.dbo.silver_employees e JOIN pg_local.public.demo_data p ON e.id = p.id`).

### 21. 🤖 MLflow Model Registry & Low-Latency Local Serving
* **Enterprise Model Governance & Versioning**:
  - Register machine learning models (`employee_turnover_predictor`, `equipment_failure_forecaster`) with multi-version lifecycle tracking (`v1`, `v2`, `v3`).
  - Model metadata tracking: Model flavors (`xgboost`, `lightgbm`, `sklearn`, `python_function`), training run IDs, registration timestamps, and evaluation metrics (`accuracy`, `auc_roc`, `f1_score`).
  - Stage governance: Transition model versions between stages (`None`, `Staging`, `Production`, `Archived`) with automated live routing to the active `Production` version.
* **In-Process HTTP Scoring Endpoint**:
  - Real-time scoring endpoint (`POST /api/models/{name}/score`) running locally inside the web runtime.
  - Ultra-low latency inference (< 2ms) with zero heavy container deployment overhead.
  - Flexible payload formats: Pandas-compatible DataFrame records (`{"dataframe_records": [...]}`), split format (`{"columns": [...], "data": [...]}`), or input vectors (`{"inputs": [...]}`).
  - Serving lifecycle toggle: One-click Start/Pause endpoint controls with live request counters and moving average latency gauges.
* **Interactive In-Studio Scoring Playground**:
  - Test inference queries interactively without leaving Data Kiln Works.
  - 1-click test scenario presets (e.g. *High Risk Attrition*, *Low Risk / Retained*, *Imminent Failure*, *Healthy Machine*).
  - Live JSON payload editor with real-time model evaluation cards and raw JSON prediction trees.
  - One-click **Copy cURL Command** for seamless terminal or API integration testing.

### 22. ⚡ Ray Distributed Compute Engine & Kubernetes Scaling Strategy
* **Ray-Native Dynamic Compute Orchestration**:
  - Eliminates privileged Docker socket mounts and static worker containers.
  - Stateful [`DuckDBWorkerActor`](file:///home/martin/volumes/datakilnworks/web/ray_engine.py#L40-L130) pools mapped per SQL Warehouse with isolated memory caps (`max_memory`), thread allocations (`threads`), and in-memory catalog mounting.
  - Managed by [`RayClusterManager`](file:///home/martin/volumes/datakilnworks/web/ray_engine.py#L139-L382) singleton with round-robin query dispatch and cluster telemetry tracking.
* **Sub-Second Horizontal Scaling on the Fly**:
  - Scale compute workers up, down, or to zero directly from the UI or API (`[-] N w [+]`) in **< 20 milliseconds** without restarting containers or interrupting active sessions.
  - Persistent state synchronization in `warehouse/.metadata/sql_warehouses.json`.
* **Distributed Map-Reduce / Scatter-Gather over Delta Lake**:
  - Parallel partition scanning via [`execute_distributed_delta_scan()`](file:///home/martin/volumes/datakilnworks/web/ray_engine.py#L297-L379).
  - Inspects Delta transaction logs (`DeltaTable.file_uris()`), slices Parquet files into balanced chunks, executes vectorized DuckDB scans across workers in parallel, and merges Apache Arrow tables with zero copy (`pyarrow.concat_tables`).
  - Tested execution latency: **6.77 ms** for distributed partition scan.
* **REST APIs & Studio Dashboard Controls**:
  - `GET /api/compute/ray/status`: Cluster health, active nodes, CPU cores, memory RSS, and Plasma object store stats.
  - `POST /api/compute/ray/start` & `POST /api/compute/ray/stop`: Lifecycle controls for Ray cluster and worker actor pools.
  - `POST /api/compute/warehouses/{id}/scale`: Dynamically scales target worker count (`{"target_workers": N}`).
  - `POST /api/compute/warehouses/{id}/distributed-query`: Parallel scatter-gather execution across Delta Parquet files.
  - Dedicated Ray Dashboard monitor in SQL Warehouses view with dynamic steppers, telemetry metrics, and test scan trigger.

#### 🌐 Kubernetes Cluster Deployment & Sizing Strategy (Generic Guide)

When deploying Data Kiln Works on a Kubernetes cluster (e.g. **k3s**, **microk8s**, **vanilla Kubernetes**, **EKS**, or **KubeRay**), the resource configuration is **strictly dependent on the physical resources available across your Kubernetes worker nodes**.

##### 1. The Heterogeneous Cluster Reality
Real-world Kubernetes clusters (especially edge, on-premise, or homelab environments) rarely consist of identical machines. They frequently feature a **heterogeneous mix**:
* **Mixed CPU Architectures**: ARM64 / ARMv8 (e.g. Raspberry Pi 4/5, Ampere Altra, Apple Silicon) alongside x86_64 (Intel Xeon, AMD EPYC, Intel NUCs).
* **Varying Memory Capacities**: Nodes with 8 GB, 16 GB, 32 GB, or 64 GB+ RAM.
* **Varying CPU Cores**: Nodes with 4, 6, 8, or 16+ cores.

##### 2. The "Lowest Common Denominator" (LCD) Compute Pod Sizing Model
Rather than configuring a single monolithic Ray worker pod sized to fill an entire physical machine (which would fail to schedule on smaller nodes in a heterogeneous cluster), the recommended strategy is to define a modular, atomic **Lowest Common Denominator (LCD) Ray Worker Pod**:

> **Recommended LCD Unit:** **`1 Ray Worker Pod = 2 vCPU, 4 GB RAM`**  
> *(or `2 vCPU, 3 GB RAM` if running tight 8 GB nodes)*

By sizing worker pods to the lowest common denominator, **Kubernetes' default scheduler automatically bin-packs the optimal number of Ray workers onto each node according to its capacity**:

| Kubernetes Node Spec | Hardware Example | Ray Worker Pods Scheduled | Total Node Compute Allocated | Headroom Left (OS, k3s, Flannel, Kubelet) |
| :--- | :--- | :---: | :--- | :--- |
| **4 Cores, 8 GB RAM** | Raspberry Pi 4/5, Thin Client | **1 Pod** | 2 vCPU, 4 GB RAM | 2 vCPU, 4 GB RAM |
| **6 Cores, 16 GB RAM** | Hexa-Core AMD Ryzen / NUC | **2 Pods** | 4 vCPU, 8 GB RAM | 2 vCPU, 8 GB RAM |
| **8 Cores, 32 GB RAM** | 8-Core Intel Xeon / Apple Silicon | **3–4 Pods** | 6–8 vCPU, 12–16 GB RAM | 2 vCPU, 16 GB RAM |
| **16 Cores, 64 GB RAM**| Dual Xeon / High-Density Server | **6–7 Pods** | 12–14 vCPU, 24–28 GB RAM | 2–4 vCPU, 36 GB RAM |

##### Why LCD Sizing is Superior for Heterogeneous Compute:
1. **Zero Node-Affinity Complexity**: You do not need custom node-selector rules or separate deployments per machine type. A single `Deployment` or `KubeRay` manifest scales seamlessly across all nodes.
2. **Superior Memory Isolation & Fault Tolerance**: If an intense analytical query exhausts memory in one DuckDB actor, only that single 4 GB pod restarts, without impacting the rest of the node.
3. **Vectorized Thread Efficiency**: Multiple smaller DuckDB instances (each with 2 dedicated threads) process morsels with lower thread synchronization overhead than a single monolithic 16-thread DuckDB process.
4. **Plasma Object Store Scalability**: Ray pools the Plasma memory of all distributed pods into one unified, shared-memory Arrow object store across the cluster.

##### 3. Kubernetes Configuration & Manifests
To deploy Data Kiln Works with Ray on Kubernetes:

* **Environment Variables**:
  - `RAY_ADDRESS`: Set to `ray://<ray-head-service>:10001` when connecting to a remote KubeRay cluster, or omit for single-pod embedded mode (`local://embedded`).
  - `WAREHOUSE_DIR`: Point to a shared PersistentVolumeClaim (NFS, Ceph, Longhorn, or MinIO S3 bucket) accessible by all workers.

* **Sample Ray Worker Kubernetes Deployment (LCD Sizing)**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ray-worker-lcd
  namespace: datakilnworks
spec:
  replicas: 6  # Adjust based on total cluster LCD capacity
  selector:
    matchLabels:
      app: ray-worker
  template:
    metadata:
      labels:
        app: ray-worker
    spec:
      containers:
      - name: ray-worker
        image: localspark-lakehouse-notebook:latest
        command: ["ray", "start", "--address=ray-head:6379", "--block"]
        resources:
          requests:
            cpu: "2"
            memory: "3.5Gi"
          limits:
            cpu: "2"
            memory: "4Gi"
        volumeMounts:
        - name: warehouse-storage
          mountPath: /workspace/warehouse
        - name: dshm
          mountPath: /dev/shm
      volumes:
      - name: warehouse-storage
        persistentVolumeClaim:
          claimName: lakehouse-shared-pvc
      - name: dshm
        emptyDir:
          medium: Memory
          sizeLimit: 1Gi  # Fast Plasma object store buffer
```

### 23. 🔐 Enterprise Authentication Frameworks (LDAP, OIDC, SAML 2.0 & RBAC)
* **Centralized Identity & Access Management (IAM)**:
  - Configure corporate identity providers under **Platform Settings > Authentication**.
  - Passwords hashed with salted `PBKDF2-HMAC-SHA256` with JWT cookie sessions (`dbx_session`).
  - **Env-var-driven bootstrap, not hardcoded credentials**: a brand-new, empty warehouse volume seeds no accounts on its own. It reads a single admin account from `INIT_ADMIN_USERNAME` / `INIT_ADMIN_PASSWORD_HASH` / `INIT_ADMIN_DISPLAY_NAME` (the hash is generated with `python -m web.auth hash-password`, so a plaintext password never needs to touch `.env` or a config file), and refuses to start if none is configured — there would be no way to log in. This only ever runs once, against a genuinely empty `users` table; an already-seeded warehouse (including every existing deployment) is completely unaffected regardless of what these variables are set to. The bootstrap admin (and any account whose password an admin resets on their behalf) is forced to change that password on first login — enforced server-side, not just hidden in the UI, so a direct API call can't skip it.
* **LDAP & Active Directory Integration** (`web/ldap_auth.py`; real bind/search/sync, not a stub):
  - Login for an unknown or LDAP-provisioned username does a real service-account bind, searches for the user (username filter-escaped against LDAP injection), then re-binds *as that user's DN* with the password given — the actual credential check, so a directory-side password change or account lock takes effect on the next login, never a cached hash.
  - Group membership (reverse search, `member`/`uniqueMember`, configurable) maps to a role via `admin_group` / `power_user_group` / `default_role`; the local account is created or updated with `auth_source='ldap'` and a random password hash nobody knows, so it can only ever authenticate through LDAP. An existing **local** username is always refused here first, so an LDAP login attempt can never take over a local account.
  - **Sync group & role groups**: set `sync_group` (a group DN, e.g. `cn=datakilnworks,...`) and only its members can log in, get discovered by sync, or stay active — login is refused for other directory users, bulk sync skips them, and a re-sync deactivates an account that left the group. Empty means everyone under the user search base. `admin_group` / `power_user_group` / `user_group` map directory groups to the local roles (first match wins); a member of `sync_group` in none of them gets `default_role`, never a silent skip.
  - **Sync**: `POST /api/auth/frameworks/ldap/sync` (or the *Sync LDAP Users Now* button) provisions every directory user under the user search base who has never logged in yet, re-resolves every already-known LDAP account's role against the directory, and deactivates one locally if its directory entry is gone. An existing local (or already-provisioned LDAP) username is always left untouched, never overwritten.
  - **Test Bind & Search** (beyond the existing TCP/TLS-only *Test Connection*) does the real service bind and a bounded search, for a config that is actually usable, not just reachable.
* **TOTP Two-Factor Authentication** (`web/mfa.py`, RFC 6238; any authenticator app):
  - Self-service from the user menu → *Two-factor authentication*: scan a QR code (rendered in the browser; the secret never goes to a third party), confirm with a first code, and receive 10 single-use backup codes, shown once.
  - Applies to password sign-ins (local and LDAP accounts). Login becomes two steps: the password returns only a 5-minute token that is *not* a session; `POST /api/auth/login/mfa` with a code (or backup code) issues the session. OIDC accounts are excluded, since their identity provider owns the second factor.
  - Codes are single-use (replay-protected), accepted ±1 step for clock drift, and 5 wrong codes lock the second factor for 5 minutes. The secret is encrypted at rest (key in `warehouse/.metadata/mfa.key`, mode 0600), backup codes are stored as HMACs, and no user endpoint returns either.
  - Turning MFA off or regenerating backup codes needs a current code (plus the password for local accounts), so a stolen session alone can't remove it. An administrator can switch off another user's MFA from Users & IAM when a device is lost.
  - Not implemented: an organisation-wide policy that *requires* MFA, or a grace period; enrolment is opt-in per user.
* **OIDC & OAuth 2.0 Providers** (`web/oidc_auth.py`; a real authorization-code flow, not a stub):
  - Federate logins with Azure AD, Okta, Keycloak, or Google Identity. A **Sign in with <provider>** button appears on the login screen once OIDC is enabled with an issuer and client ID; register `redirect_uri` (default `http://localhost:8891/api/auth/oidc/callback`, editable) with the provider.
  - Auto-discovery via `.well-known/openid-configuration` (the document's `issuer` must equal the configured one). PKCE (S256) always, plus `state` and `nonce`; the attempt is remembered only in a short-lived signed HttpOnly cookie. Public clients (no secret) work.
  - The ID token is verified against the provider's JWKS with an asymmetric-algorithm allow-list (never `none`/HS256), and `iss`, `aud`/`azp`, `exp` and `nonce` are checked. Username comes from `username_claim` (default `preferred_username`, else a *verified* email); role from `admin_claim` with `admin_value` / `power_user_value` (userinfo is consulted if the claim isn't in the ID token), else `default_role`.
  - Accounts are created with `auth_source='oidc'` and no usable password. An existing local or LDAP username, a deleted account, or one an administrator deactivated is always refused: an OIDC login can never take over or reactivate an account. Roles are refreshed on each login (there is no background sync for OIDC).
* **SAML 2.0 Enterprise Federation**:
  - Corporate SSO support for PingIdentity, Shibboleth, and enterprise IdPs.
* **Granular Role-Based Access Control (RBAC)**:
  - Pre-defined roles: `admin`, `power_user`, and `user`.
  - Zero-trust Catalog permissions (`READ`, `WRITE`, `ADMIN`) with query-level AST authorization checks.

### 24. 🛠️ Platform Settings & Lakehouse Governance
* **Workspace Branding & Whitelabeling**:
  - Customize workspace title, logo icon, primary brand accent color, and custom login welcome banners.
  - Live CSS overrides with real-time preview.
* **Audit Log & Telemetry Retention**:
  - Configure automatic SQLite WAL history retention limits (7 days, 30 days, 90 days, 1 year).
  - Background WAL checkpointing and database compaction.
* **Catalog Access Control Lists (ACLs)**:
  - User-to-catalog permissions matrix with instant grant/revoke toggles.

### 25. 🧱 dbt Core Workbench & Interactive CTE Stepper
* **Native dbt-core Integration**:
  - Full dbt project management inside Data Kiln Works (`./dbt_project`).
  - Compiles and materializes Jinja SQL models to local Delta Lake tables.
* **Interactive CTE Step Debugger**:
  - Inspect intermediate Common Table Expressions (`WITH cte AS (...)`) step-by-step.
  - View row count, column schemas, and live tabular output for each CTE before compiling the final model.
* **Model Runner & Console**:
  - One-click `dbt run`, `dbt test`, and `dbt compile` with real-time log output drawer.

### 26. 🔔 Multi-Channel Alerting & Incident Notification
* **Automated SQL Metric Monitors**:
  - Trigger alerts based on query execution thresholds (`latency > N ms`, `failure_count > 0`, `row_count == 0`).
* **Slack Webhooks Integration**:
  - Format and dispatch structured alert cards to Slack channels with direct links back to query profiles.
* **Generic REST Webhooks**:
  - Webhook payloads for PagerDuty, Discord, or automated orchestration pipelines.

### 27. 📂 Unity Catalog Volumes & Volume Auto-Loader (Snowpipe / Databricks Auto Loader equivalent)
* **Volumes** (`/Volumes/<catalog>/<schema>/<volume>/`): create, browse, upload, preview and delete files; paths are traversal-guarded and stored under `warehouse/volumes/`.
* **Auto-Loader pipelines**: a background daemon polls a volume folder (`*.csv`, `*.tsv`, `*.json`, `*.jsonl`, `*.parquet`) every 5s or more, or on a 5-field cron schedule (UTC; missed ticks run once on catch-up), and loads new files into a Delta table.
  - **S3 and S3-compatible sources** (`web/autoloader_s3.py`, needs `boto3`): use `s3://bucket/prefix/` as the source. Each poll lists the prefix (`ListObjectsV2`, paginated), skips objects already checkpointed and streams each new one from S3 through DuckDB into Delta, so nothing is downloaded. Endpoint and credentials come from an S3 storage mount (choose one in the form, or it defaults to a mount for that bucket, else the first S3 mount). An object's identity is key + size + ETag: identical bytes re-uploaded are not reloaded, a replaced object is. Same filters as local volumes (hidden segments, `_quarantine/`, `.tmp`/`.part`, pattern); csv/json/parquet, append/merge and schema policies unchanged. A malformed object is copied under `<prefix>/_quarantine/` and removed if the key may delete it (otherwise it stays and is recorded, never retried); a network or access error is a retryable FAILED, never a quarantine. S3 sources are polled (interval or cron): filesystem events do not exist for object stores, so *File events* is refused for them. Bucket-notification-driven triggering (SQS/webhooks) is not implemented.
  - **File-watch triggering** (`web/autoloader_watch.py`): choose *File events* instead of a poll interval and the pipeline starts the moment a file lands, using Linux inotify (no dependency), so latency is about the 1 s debounce rather than a poll tick. Only "the writer is finished" events count (`IN_CLOSE_WRITE`, `IN_MOVED_TO`), so a half-written file is never loaded; `.part`/`.tmp`/hidden files, non-matching patterns and `_quarantine/` never trigger; sub-folders (also ones renamed in later) are watched recursively; bursts coalesce into a few cycles; a pipeline never runs two cycles at once. Ingestion is unchanged (same exactly-once checkpoints, schema policies and quarantine).
  - **Safety net**: events can be missed (network/FUSE filesystems, queue overflow, downtime), so a low-frequency rescan (default 5 min, min 30 s) still runs and the watcher start triggers an immediate catch-up cycle. If inotify is unavailable (non-Linux, the per-user instance limit `fs.inotify.max_user_instances`) the pipeline shows *watch unavailable* and keeps polling at its interval, retrying the watcher every few seconds. File watching and a cron schedule are alternatives.
  - **Exactly-once**: each file is fingerprinted (size + mtime + first 64KB) in a SQLite checkpoint (`.metadata/autoloader.db`) and committed as one Delta transaction.
  - **Streaming reads**: files stream through DuckDB into delta-rs in `AUTOLOADER_BATCH_ROWS` (default 100,000) batches, so memory does not scale with file size.
  - **Load modes**: `append`, `merge` (upsert on `merge_keys`, which must be columns of the incoming file) and `overwrite`.
  - **Schema evolution policies**: `addNewColumns`, `failOnNewColumns` and `rescue` (unknown columns go to a JSON `_rescued_data` column).
  - **Quarantine**: unreadable or corrupt files move to `_quarantine/` while the pipeline continues; Delta write and schema-policy failures are logged as `FAILED` and retried on the next cycle.
  - **Observability**: per-file history (rows, latency, error), KPI cards in the UI, and lineage `VOLUME → TABLE` (shown in the Raw Files column, created with the pipeline).
* **API**: `/api/volumes/...` and `/api/autoloader/pipelines/...` (create, update, delete, `run` / `run-now`, `reset`, `history`, `stats`).
* **Verification**: `python scratch/test_autoloader.py` (backend, throwaway warehouse) and `AUTOLOADER_UI_URL=<throwaway studio> python3 scratch/verify_autoloader_ui.py` (Playwright; it creates pipelines, so never point it at real data).

### 28. 🛡️ Data Governance: Tags, Tag-Driven Column Masking & Row-Level Security
* **Tags** on catalogs, schemas, tables and columns (`pii=email`, `sensitivity=confidential`), stored in `warehouse/.metadata/governance.db`. Tags **inherit downward** (catalog → schema → table → column, most specific wins), so new columns from schema evolution or the Auto-Loader are covered automatically. Optional allowed-value lists, an audit trail, orphan detection when a column is dropped, and a name-based **classifier** that suggests tags without reading any data.
* **Masking policies** point at a tag, not at objects: *"mask everything tagged `pii` for everyone except admins"* protects every current and future column carrying that tag. Mask types: `redact`, `hash` (stable keyed pseudonym, joins still work), `partial`, `email`, `null`, `generalize` and validated `custom` expressions. Masks are type-aware and always cast back to the column's type; a mask that does not fit a type yields `NULL`, so a policy can never leak because it did not apply. Exemptions by role and by user, priorities, and a per-type filter.
* **Row filter policies** (`web/governance/row_filters.py`) point at a tag on a catalog/schema/table (never a column, since a row filter restricts the whole table) and keep only the rows a column matches for the caller: **owner** (the column equals the caller's username), **attribute** (the column must be one of the caller's assigned values for a key in the built-in `principal_attributes` membership table — nothing assigned means nothing visible, never everything) or a validated **custom** boolean expression (`{col}`/`{user}`/`{role}` placeholders). Multiple applicable policies on one table combine with `AND`.
* **Enforced at query time by rewriting the SQL** (`web/governance/enforce.py`): each scan of a table with masked columns and/or a row filter becomes `(SELECT * [REPLACE (mask AS col)] FROM table [WHERE predicate AND ...])`, so filters, joins, aggregates and `ORDER BY` only ever see masked values and filtered rows (no `WHERE ssn = '…'` oracle, no `WHERE region = 'other'` oracle either). Views are inlined recursively, path scans (`delta_scan('…')`, `read_parquet('…')`) map back to their table, and the rewritten SQL is what workers, Ray and the local engine execute. Masked or row-filtered principals get a default-deny statement allowlist; anything that cannot be verified is refused, never run unmasked or unfiltered.
* **Every data-egress path is governed**: SQL editor, exports, profiles, previews, version diffs (Arrow-level masking and row filtering), dashboards (with a result cache keyed by the mask *and* row-filter predicate set — two users under one policy can resolve to different predicates and never share a cached result), Genie (LLM prompt samples are *always* masked, whoever asks), alerts, scheduled exports and jobs (run as their owner). Features that read data outside the governed catalogs (dbt, direct OneLake queries, distributed scans, notebook kernels) are refused for masked or row-filtered users. `scratch/test_governance_coverage.py` fails when a new unreviewed DuckDB execution site appears.
* **UI**: a *Governance* view (tags; masking policies with a live mask tester; row filter policies and a user-attributes manager; suggestions; *preview as user*; audit and coverage), tag chips and lock badges in the Catalog Explorer, and masked-column indicators in SQL results. Admins are exempt by default (`except_roles`), and their reads of tagged columns or row-filtered tables are audited (`EXEMPT_READ` / `ROW_FILTER_EXEMPT_READ`).
* **Lifecycle**: when an exempt user (an admin, say) materialises tagged columns with `CREATE TABLE AS` / `INSERT ... SELECT` (SQL editor or a job), the new table is tagged like its sources (`source=propagated`); computed columns are tagged `sensitivity=unclassified` for review, and a copy of a row-filtered table is tagged with the same table-level tag so it isn't a full unfiltered leak of the original. Dropping a table removes its tags, tags of dropped columns are flagged as orphans (never silently deleted), and the Auto-Loader's `_rescued_data` column is flagged `unclassified`.
* **Cost**: secret-free masks are SQL macros and run at native speed (about 50-70 M rows/s on one thread in our benchmark); the keyed `hash` mask is a vectorised Python UDF (about 1 M rows/s), so prefer `partial`/`email`/`generalize` on very large scans and reserve `hash` for join keys. Rewriting a query takes about 5-9 ms; installs without tags skip catalog lookups entirely.
* **Rollout**: `GOVERNANCE_ENFORCEMENT=audit` computes and logs what would be masked without changing results; `enforce` (default) applies it; `off` disables the gateway.
* **Verification**: `scratch/test_governance_phase{0..6}.py`, `scratch/test_governance_coverage.py`, and `scratch/verify_governance_ui.py` (Playwright, throwaway instance only).

---

## 🔐 Security Settings & Governance Trust Boundary

| Setting | Default | Effect |
| :--- | :--- | :--- |
| `GOVERNANCE_REQUIRE_AUTH` | `false` | `false` keeps the single-user local mode (requests **without credentials** run as the local admin). `true` makes them the least-privilege `anonymous` user and ignores the credential-less `X-User` header. Invalid or expired credentials are **never** admin in either mode. |
| `GOVERNANCE_NOTEBOOK_EXECUTION` | `sandbox` | Where notebook code runs. `sandbox`: users no masking policy applies to use the Studio's kernels, users a policy applies to use the notebook sandbox. `exempt`: only users no policy applies to may run notebooks (others can open and edit them). `all`: everyone uses the Studio's kernels (masking is then not enforced for notebook code). |
| `SANDBOX_URL` / `SANDBOX_TOKEN` / `SANDBOX_GATEWAY_URL` | `http://notebook-sandbox:8000` / generated / `http://datakilnworks-studio:8000` | Where the studio finds the sandbox worker, its token (generated by the worker on a volume the studio mounts read-only; set `SANDBOX_TOKEN` to override), and where kernels reach the studio. |
| `SANDBOX_TOKEN_TTL` / `SANDBOX_MAX_ROWS` / `SANDBOX_MAX_KERNELS` | `900` / `1000000` / `32` | Lifetime in seconds of the per-user token a kernel presents (renewed on every cell run), rows a sandboxed kernel may pull in one query, and concurrent sandbox kernels. |
| `JWT_SECRET_KEY` | per-install random | Session signing key. If unset, a random key is created in `warehouse/.metadata/jwt_secret` (existing sessions are signed out once after upgrading). |
| `COMPUTE_TOKEN` | per-install random | Shared secret (`X-Compute-Token`) the studio sends to compute workers, which reject requests without it. Stored in `warehouse/.metadata/compute_token` when unset. |
| `GOVERNANCE_ENFORCEMENT` | `enforce` | `enforce` applies masking and statement gating; `audit` computes and logs what would be masked but never changes or blocks a query; `off` disables the gateway. |
| `GOVERNANCE_ALLOWED_PATHS` | empty | Extra directories (`:`-separated) non-admins may read with file functions, besides warehouse tables, volumes, exports and `/tmp/uploads`. |

**Non-admin file access.** File functions (`read_csv`, `read_parquet`, `delta_scan`, `read_text`, …) accept only literal paths inside warehouse tables, volumes and exports for non-admin roles, regardless of masking policies: reading `.metadata` (session signing key, compute token, auth database) would otherwise let anyone forge an admin session. `query()`/`query_table()` and redefining the `gov_*` mask functions are refused for non-admins.

**Notebook sandbox.** The `notebook-sandbox` service runs the kernels of users a masking policy applies to. From the outside in:

* *Container:* no warehouse, metadata or notebooks mounted; read-only root; `cap_drop: ALL` plus only what switching users needs; PID and memory limits; it is alone with the studio on an `internal` network (no internet, no compute nodes, no Ray).
* *OS user:* every Studio user gets a separate uid and each kernel drops to it before any user code runs, so kernels of different users cannot read each other's files, memory or connection keys, and cannot regain root.
* *Data:* kernels read data only through `/api/sandbox/sql`, with a short-lived token bound to the user (readable only by that uid). The endpoint applies the same permission check, masking rewrite, statement gating and audit as the SQL editor, and re-resolves the user on every call, so a changed policy or disabled account applies immediately. The studio refuses every other route to the sandbox's address, so the credential-less "local admin" of single-user mode is not reachable from a kernel.
* *Inside the kernel:* `spark`, `conn` and `%sql` work on a private in-memory DuckDB. A query that only reads warehouse tables is answered by the studio in one round trip (joins, filters and aggregates run there); a query that also touches local DataFrames pulls the tables it names (already masked, capped at `SANDBOX_MAX_ROWS`) and runs locally. `dbutils`, warehouse files and MLflow are not available in the sandbox, and it is read-only towards the warehouse.

**What column masking will and will not cover.** Studio queries (SQL editor, dashboards, previews, exports, alerts, Genie, jobs) are governed. **Notebook kernels and anything that can read `warehouse/` directly are outside that boundary**, because a Python process can open the files itself. Notebook code of users a masking policy applies to therefore runs in the notebook sandbox instead (`GOVERNANCE_NOTEBOOK_EXECUTION`). Compute workers (`compute-node-01..03`) are no longer published on the host; they are reachable only on the compose network and require the compute token.

---

## 🧪 Interactive Notebook Verification (Port 8890)

Open [`notebooks/sample_lakehouse_pipeline.ipynb`](notebooks/sample_lakehouse_pipeline.ipynb) in the Studio Workspace:
1. **PySpark DataFrame Transformations**: Create PySpark DataFrames, apply window functions (`dense_rank`), and view rich tables with `display()`.
2. **Delta Lake Materialization**: Materialize DataFrames to Delta tables via `conn.sql("CREATE OR REPLACE TABLE silver_employees AS SELECT * FROM transformed_df")`.
3. **Inspect Delta Logs**: Run `dbutils.fs.ls("dbfs:/silver_employees")` to inspect Parquet data and `_delta_log/` transaction files.
4. **Interactive SQL**: Run SQL analytics with `%%sql`.
5. **ACID Transactions & Time Travel**: Execute `UPDATE` statements and query historical snapshots using `delta_scan('...', version => 0)`.

---

## 📄 License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**. See the [`LICENSE`](LICENSE) file for the full license text.
