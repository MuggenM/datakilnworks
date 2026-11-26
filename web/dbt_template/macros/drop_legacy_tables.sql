{#
  Upgrade path from the plain dbt-duckdb adapter to the duckrun adapter.

  dbt-duckdb kept every `table` model as a real table inside dbt_analytics.duckdb. duckrun writes them as Delta tables
  (under `root_path`) and only exposes a *view* over the Delta location in that file, and DuckDB refuses to replace an
  existing table with a view ("Existing object ... is of type Table, trying to replace with type View"). So a project
  that already ran once on the old adapter would fail every table model after the switch.

  This runs before each `dbt run`/`build`: it drops only relations that are still legacy *tables* (never a Delta-backed
  view, never a relation dbt does not manage), so it is a no-op once migrated. The data is rebuilt by the run itself.
#}
{% macro drop_legacy_duckdb_tables() %}
  {%- if execute and target.type == 'duckrun' -%}
    {%- for node in graph.nodes.values() if node.resource_type == 'model' and node.config.materialized in ('table', 'incremental', 'delta') -%}
      {#- duckdb_tables() lists only real tables. (dbt's own relation type cannot be used: it reports the Delta-backed views
          of already migrated models as tables too, which would drop them on every run.) -#}
      {%- set found = run_query("select 1 from duckdb_tables() where database_name = '" ~ (node.database | replace("'", "''")) ~ "' and schema_name = '" ~ (node.schema | replace("'", "''")) ~ "' and table_name = '" ~ (node.alias | replace("'", "''")) ~ "'") -%}
      {%- if found.rows | length > 0 -%}
        {%- set legacy = api.Relation.create(database=node.database, schema=node.schema, identifier=node.alias, type='table') -%}
        {%- do log('duckrun: dropping legacy dbt-duckdb table ' ~ legacy ~ ' (it is rebuilt as a Delta table)', info=True) -%}
        {%- do adapter.drop_relation(legacy) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
