"""
Data governance: tags on catalog objects and (from phase 2) tag-driven masking policies enforced at query time.

Modules
  store        governance.db access, versioning (cache invalidation) and the audit trail
  tags         tag definitions, assignments, inheritance and effective-tag resolution
  catalog_meta existence checks and column listings against the live DuckDB catalogs
  classify     column-name heuristics that suggest tags
  routes       FastAPI router mounted under /api/governance
"""
