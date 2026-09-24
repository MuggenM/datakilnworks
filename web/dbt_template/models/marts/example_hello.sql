{{ config(materialized='table') }}

-- A smoke-test model so a brand-new project has something to run (`dbt run`); replace or delete it.
-- With the default profile it is written to <warehouse>/dbt/example_hello and is closed to users until an admin opens it.
select 1 as id, current_timestamp as built_at
