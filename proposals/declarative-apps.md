# Proposal: declarative apps (no author code) built on the dashboard engine

## 1. Problem

Embedded HTML apps (`web/apps.py`) run author-supplied JS. The sandboxed iframe and the token-only query endpoint contain
the damage, but the trust model is still "a publisher ships code to other users' browsers", which is why upload is limited
to admin/power_user. We want a tier where **no author code exists at all**, so that ordinary users can build and share
interactive data apps safely.

## 2. Idea

An app is a JSON **spec** (pages, components, parameters, SQL per component) interpreted by a fixed renderer in the DKW
frontend. The spec contains data, never code. Nothing the author types is ever executed as JS or injected as HTML.

This is an extension of the existing dashboard engine, not a new subsystem.

## 3. What dashboards already give us (checked in code)

- Storage: JSON store (`load_dashboards_store` / `save_dashboards_store` in `web/dashboards.py`), plus versions, folders,
  templates, themes, share links, schedules, email, embed (`/embed/dashboard/{id}`).
- Grants: `dashboard_permissions` (owner / user / group / public) and the generic Share dialog.
- Widgets: ~25 types (`WidgetPayload.type`: kpi, bar, line, pie, table, pivot_table, heatmap, world_map, ...), each with a
  `query`, `x_col`, `y_col`, `width`.
- Filters: dashboard-level `filters` with dropdown options computed by a query (`get_dashboard_filter_options`).
- Parameters: `:name` and `{{name}}` placeholders in widget SQL (`resolve_query_parameters`); missing/`ALL` becomes NULL.
- Execution: `execute_widget_query` runs `gateway.govern_sql` as the viewer; the result cache is keyed by
  `gateway.fingerprint` (masks and row filters), so users never share a cache entry across different policies.

## 4. Gaps between a dashboard and an app

| Need | Dashboard today | Work |
| --- | --- | --- |
| Multiple pages / navigation | one flat widget list | `pages: [{id, title, widgets}]`; renderer tabs/sidebar |
| Layout | `width` class only | grid (row/col spans) per page |
| Inputs | filters bound to a column dropdown | typed parameters: text, number, date, select (static or SQL options), multiselect |
| Parameter in widget click | none | "click a bar sets parameter X" (cross-filtering) |
| Static content | none | `text` / markdown component (sanitized) |
| Write actions | none | out of scope v1 (read-only) |
| Parameter safety | string substitution with quote escaping | **bind parameters** (see 5) |

## 5. Security requirements

1. **Bind, don't substitute.** `resolve_query_parameters` rewrites the SQL text and escapes `'`. That is adequate for a
   quoted string literal but is not true binding: it also treats any `:word` in the query (including inside string
   literals or casts such as `x::int` neighbours and time literals) as a parameter, and every value becomes a string.
   For apps, declare parameters with a type and let the engine bind them (DuckDB prepared parameters, or typed literals
   produced from a validated value: int/float/date parsed and re-rendered, strings escaped). Reject undeclared names.
   Reuse the validation idea of `workflow._check_param_value` (pattern / allowed list).
2. **Option-list parameters** (`select`) validate the submitted value against the declared options server-side.
3. **Rendering**: all text via `textContent`/Alpine `x-text`; markdown through a sanitizer with an allow-list (no raw
   HTML, no `javascript:` URLs, no remote images unless enabled). Chart configs are built by the renderer, never taken
   from the spec verbatim (a spec must not be able to set Chart.js callbacks).
4. **Governance identical to the SQL editor.** Confirm that the dashboard path also applies catalog/table ACLs
   (`enforce_sql_permissions`). `execute_widget_query` calls `govern_sql` (masks, row filters) but the check should be
   verified before apps rely on it; if it is missing, that is an existing dashboard gap to fix first.
5. **Viewer identity**: run as the viewer, never the author (a shared app must not widen access). Public/share-link
   viewers run as the least-privilege anonymous principal, as the share-link route does today.
6. **Limits**: max components per page, max rows per result, per-query timeout, rate limit per viewer. Cache key
   includes parameter values and the mask/filter fingerprint.
7. **Spec validation on save** (like `workflow.validate_job`): schema check, unknown fields refused, SQL parsed with
   sqlglot (single SELECT only, no DDL/DML), parameter references must all be declared.

## 6. Proposed spec (sketch)

```json
{
  "id": "app_x", "name": "Sales explorer", "version": 1,
  "parameters": [
    {"id": "region", "type": "select", "label": "Region",
     "options_query": "select distinct region from main.sales.orders", "default": "ALL"},
    {"id": "from", "type": "date", "label": "From", "default": "2026-01-01"}
  ],
  "pages": [{
    "id": "overview", "title": "Overview",
    "layout": [
      {"component": "kpi",   "title": "Revenue", "query": "select sum(amount) v from main.sales.orders where (region = :region or :region is null) and order_date >= :from", "span": 3},
      {"component": "bar",   "title": "By month", "query": "...", "x": "month", "y": "revenue", "span": 9,
       "on_click": {"set_parameter": "month", "from_column": "month"}},
      {"component": "table", "title": "Orders", "query": "...", "span": 12},
      {"component": "text",  "markdown": "Data refreshed nightly.", "span": 12}
    ]
  }]
}
```

Components map onto the existing widget types, so the chart code is reused.

## 7. Delivery options

- **A. Extend dashboards** (recommended first step): add typed parameters, pages, layout and `on_click` to the dashboard
  model; "app" is a dashboard with `pages` and a chrome-less viewer (`/apps/{id}` style, also embeddable). One store, one
  permissions system, one set of tests. Existing dashboards keep working (single implicit page).
- **B. New `kind: "spec"` app type beside `kind: "zip"`** in `web/apps.py`: reuses the Apps nav, `RESOURCE_TYPES["app"]`
  grants and Share dialog, but renders with the dashboard component code. Better if you want one "Apps" catalogue.
  Can be layered on A: A is the engine, B is the listing.

## 8. Designer

Form-based, not drag-and-drop, in v1: left pane = page/component tree, centre = live preview (the real renderer), right
pane = properties (title, query with the existing SQL editor and a Run button, x/y columns, span, click action).
Parameter editor as a table. Raw JSON view for admins. Drag-to-reorder and resize can come later; `web/static/dag.js`
shows the project can do canvas work without dependencies if wanted.

## 9. What stays as is

The zip/HTML tier remains for things a spec cannot express (custom visualisations). It stays admin/power_user only,
sandboxed, with the token-only query endpoint. Optional later: an approval step before a zip app becomes visible.

## 10. Phases

1. Parameter binding module with typed params and tests (also fixes the substitution weaknesses for existing dashboards).
2. Spec model + `validate_app_spec` (sqlglot, limits) + pages/layout in the dashboard store; viewer renderer.
3. Designer UI (form-based) with live preview.
4. Cross-filtering (`on_click`), markdown component, static-text sanitizer.
5. Listing in Apps nav (option B), sharing, versions (reuse `dashboard_versions`).

## 11. Testing

- `scratch/test_declarative_apps.py`: spec validation refusals (undeclared param, DDL, multi-statement, unknown fields,
  oversize), parameter typing/binding with hostile values (`'; drop`, `:other`, quotes, unicode, huge strings), viewer
  identity (a masked viewer sees masked values even when the author is exempt), cache isolation by fingerprint,
  catalog ACL parity with the SQL editor, public/share-link least privilege.
- `scratch/verify_declarative_apps_ui.py` (Playwright, throwaway studio): build an app in the designer, set a
  parameter, cross-filter by clicking a bar, verify an XSS string in a title/markdown/cell renders as text.

## 12. Decisions and open questions

Decided:
- The Apps catalogue is "dashboards with pages" (option A). Zip/HTML apps remain listed beside them.
- Viewers can export data from an app (CSV/Excel/Parquet/JSON, with the current parameter values), under the viewer's own
  governance.
- The catalogue offers a few clonable sample apps as starting points.
- Existing gap fixed on the way: the dashboard list/read/query/export/widget endpoints did not check dashboard
  permissions at all. Apps make dashboards shareable by non-admins, so they are enforced now.

Open:
- Write-back (forms inserting rows) is deliberately left out for now: read-only. It needs its own proposal (governed
  MODIFY, audit, CSRF-like protections, idempotency).
