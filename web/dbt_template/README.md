# dbt project (Data Kiln Works)

This directory is the dbt project the studio runs (`DBT_PROJECT_DIR`, mounted at /workspace/dbt_project).
In production keep it in **its own git repository** (your analytics/application repo), not in the platform's, and point
`DBT_PROJECT_HOST_DIR` in the studio's `.env` at a checkout of it. The studio never commits: edits made under
*Transformations > Project files* change the working tree, so commit them from your own workflow. Config history and the
audit trail are application data (`warehouse/.metadata`), not part of this repository.
