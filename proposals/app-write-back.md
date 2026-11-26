# Proposal: write-back for declarative apps (forms and editable tables)

Follow-up to `proposals/declarative-apps.md` (section 12, "Open"): apps are read-only today. This proposal covers letting
a viewer of an app insert, update or delete rows in a Delta table through a declared form or editable table, without any
author code.

## 1. Problem

Typical use cases that a read-only app cannot serve:

- A data-entry form (new customer, a budget line, a manual adjustment).
- Review queues: an analyst marks rows approved/rejected, adds a comment.
- Small reference tables maintained by the business (mappings, targets, exclusion lists).

Today these need a notebook or an external tool. Write-back is the feature that makes apps more than a report, and also the
one that can do real damage, so it is designed around constraints first and conveniences second.

## 2. Principles

1. **No author SQL runs with write effect.** An app author does not write `INSERT`/`UPDATE`. They declare an *action*
   (target table, mode, fields). The server builds the statement from that declaration and the validated field values.
   Arbitrary DML in a spec stays refused (the read-only SELECT-only rule of `validate_app_spec` is unchanged).
2. **Writes run as the viewer, never the author.** The viewer needs MODIFY on the target (catalog WRITE, or a
   table/schema MODIFY grant, via `table_access.can_access_table(..., "WRITE")`). An app cannot widen anyone's access. If
   the viewer lacks it the form is rendered disabled with the reason; the server enforces it regardless.
3. **Declared, bounded, typed.** Only the declared columns can be written; each field has a type and constraints; row and
   payload sizes are capped.
4. **Everything is audited**, including refusals.
5. **Fail closed.** Anything not understood (unknown field, type mismatch, masked column, row filter that cannot be
   proven) is refused, not coerced.

## 3. Action model (spec additions)

An app spec gains `actions` (page-level components of type `form` or `editable_table` reference them by id):

```json
{
  "actions": [{
    "id": "add_budget",
    "target": "main.finance.budget_adjustments",
    "mode": "insert",
    "fields": [
      {"column": "cost_center", "type": "select", "options_query": "select distinct cc from main.finance.cost_centers", "required": true},
      {"column": "amount",      "type": "number", "min": -1000000, "max": 1000000, "required": true},
      {"column": "reason",      "type": "text",   "max_length": 500}
    ],
    "fixed": {"submitted_by": "$user", "submitted_at": "$now"},
    "max_rows": 1
  }]
}
```

Modes:

| Mode | Meaning | Key requirement |
| --- | --- | --- |
| `insert` | append one row (form) or several (editable table "add rows") | none |
| `update` | change declared columns of existing rows | `key` columns declared; update by key only |
| `delete` | delete rows by key | `key` declared; off unless the author opts in; admin-confirm option |
| `upsert` | MERGE on key | `key` declared |

Notes:

- `fixed` columns are set by the server from `$user`, `$now`, `$app`, `$action`, or a literal. They are not editable and
  not sent by the client. This gives provenance columns without trusting the browser.
- `update`/`delete` never accept a free predicate. The statement is `... WHERE key1 = ? AND key2 = ?` from the declared
  key columns, with values from the row the viewer selected. Zero or more than `max_rows` matching rows refuses the
  write (no partial effect).
- Optional `where_scope`: a declared extra condition (e.g. `owner = $user`) that is ANDed into update/delete, so a user
  can only edit their own rows.
- Field types: text, number, integer, date, datetime, boolean, select (static or SQL options, validated server-side
  against the options), multiselect (stored as JSON/array only if the column type is). Values are parsed and re-rendered
  into typed values and **bound**, using the same typed-parameter module phase 1 of the apps proposal introduces. No
  string substitution.
- Spec validation on save: target exists (local catalog; mounts trusted as elsewhere), declared columns exist in the
  Delta schema, types are compatible, `key` columns exist and are non-null, `fixed` references are known, the author must
  themselves hold MODIFY on the target (so a spec cannot be saved pointing at a table the author could never write; this
  does not grant the viewers anything, it only stops pointing apps at forbidden places).

## 4. Execution path

New endpoint `POST /api/apps/{id}/actions/{action_id}` (session or app data token, same identity resolution as the app
query endpoint). Server steps, in order:

1. Resolve principal (`resolve_principal`), refuse if unauthenticated/anonymous. **Public and share-link viewers can never
   write.**
2. `gateway.deny_if_subject`-style check: decide whether subjects (masked/row-filtered viewers) may write at all, see 5.
3. Load the action from the *saved, validated* spec of the current app version (the client sends only `action_id`,
   values and, for update/delete, the key values; never SQL, table names or column names).
4. Permission: `table_access.can_access_table(user, catalog, schema, table, "WRITE")`.
5. Validate and type every value; enforce `required`, ranges, lengths, option membership; reject unknown fields.
6. Idempotency: the request carries a client-generated `request_id` (UUID). The server records
   `(app, action, user, request_id)` with the outcome in a small SQLite table and returns the stored outcome on a repeat.
   Prevents double submits and retry duplicates. Retention: 7 days.
7. Write with delta-rs (`deltalake`) as one commit (insert = append; update/delete/upsert = `DeltaTable.update`/`delete`/
   `merge` with the bound predicate), with commit properties carrying `{app, action, user, request_id}` so the Delta
   history itself shows who wrote what through which app. Optimistic-concurrency conflict from delta-rs is surfaced as a
   retryable "someone else changed this, reload" error, not retried blindly.
8. Audit row (section 7), cache invalidation for queries reading that table, return `{rows_affected, version}`.

The write is performed by server code, not by `execute_sql`, so it does not go through `govern_sql`. That is why the
masking/row-filter rules in section 5 must be explicit, and why `scratch/test_governance_coverage.py` needs an entry for
this module in `web/governance/ALLOWLIST.md` (executor that builds statements from a validated declaration; never returns
data other than counts).

## 5. Interaction with governance

Reads are masked and row-filtered; writes bypass the query rewriter, so they need their own rules, strict by default:

- **Masked columns**: a field for a column that a masking policy would mask for this viewer is refused (viewer would write
  blind or overwrite a value they cannot see). Applies to update of that column as well.
- **Row filters**: for `update`/`delete`/`upsert`, a viewer who is subject to a row filter on the target may only touch
  rows the filter allows. Implementation: resolve the viewer's filter predicate (`enforce` machinery) and AND it into the
  write predicate; if the predicate cannot be resolved to a literal WHERE, refuse. For `insert` by a filtered viewer the
  new row must satisfy the filter (check the typed row against the predicate in DuckDB before commit), otherwise a user
  could insert rows they can then not see, or rows into another tenant's slice.
- **Tags**: writes that create data in a table with tagged columns do not change tags; no propagation needed. Tag
  propagation (`gateway.propagate_tags`) applies to statements that create new tables, which actions never do.
- **Sandboxed (subject) users**: allowed to write through this endpoint only under the rules above. An admin can set
  the app-wide `allow_subject_writes: false` (default) to turn this off entirely until the rules are proven for their
  policies.

## 6. Abuse and safety controls

- CSRF: the endpoint accepts the cookie session only with a custom header the browser will not send cross-site
  (`X-DKW-Action: <app id>`) plus the SameSite=Lax cookie; the app-token path is for the sandboxed iframe case.
  Content type must be `application/json`.
- Rate limit per user and action (default 30/min) and per-action `max_rows`; request body cap (256 KB).
- Daily write quota per action and user (optional, default off).
- Dry-run/preview: `?dry_run=1` validates and reports what would change (rows matched, diff) without committing; the
  editable table uses it for the confirmation step.
- Destructive modes (`delete`, `upsert` over many rows) require an explicit author opt-in per action and a confirm dialog
  in the viewer.
- Optional **approval workflow** (v2): an action with `approval: true` stores the change as a pending request in
  `app_write_requests`; a named approver (user/group) accepts it in the UI and only then it is applied, by the same
  server code as if the requester had run it (re-checking the requester's permission at apply time).

## 7. Audit and undo

- `governance_audit` actions `APP_WRITE` (and `APP_WRITE_REFUSED`) with `{app, action, mode, table, rows, version,
  request_id}`; never row contents (values may be sensitive), but the Delta commit properties plus time travel hold the
  data change itself.
- A viewer-facing "history of changes" view for an editable table is a query over the Delta history/CDF of the target,
  governed like any other read.
- Undo: the admin Apps page shows recent writes per action with a link to the table's existing restore/time-travel
  function. No bespoke undo.

## 8. UI

- Component `form`: renders fields from the action; submit button; inline validation messages from the server's typed
  errors; success toast with the new version.
- Component `editable_table`: shows a SELECT (the component's own read query) and, per row, an edit action that opens the
  declared form pre-filled with the key. Not spreadsheet-style free editing in v1.
- After success the renderer refreshes the components that read the affected table (same parameter/refresh mechanism as
  cross-filtering).
- Designer: an "Actions" tree entry, form for target/mode/fields/key/fixed, a "Test as me" button that runs dry-run.
- All labels, placeholders and messages are rendered as text (same XSS rules as the rest of the renderer).

## 9. Scope and non-goals (v1)

In: insert, update-by-key, delete-by-key (opt-in), upsert; local Delta catalogs and S3-mounted Delta (the same writer as
the Auto-Loader target, `storage_options` from the mount); idempotency; audit; governance rules above.

Out: free-form SQL actions; multi-table transactions; file uploads into a table (the Auto-Loader covers that); writing
to read-only mounts (refused); non-Delta targets; spreadsheet-style bulk editing; approvals (v2).

## 10. Phases

1. `app_writes.py`: typed field parsing/binding (shared with the parameter module), action validation, writer for
   insert/update/delete/upsert with commit properties, idempotency table, audit. Tests first.
2. Governance rules (masked columns, row-filter predicate AND, filtered insert check), ALLOWLIST entry.
3. Endpoint, rate limits, CSRF header, dry-run.
4. Renderer: `form`, then `editable_table`; designer "Actions".
5. Approval workflow and quotas (optional).

## 11. Testing

`scratch/test_app_writes.py` (throwaway warehouse, TestClient):
- Spec validation: unknown column, missing key for update/delete, type mismatch, free predicate attempt, target the author
  cannot write, `fixed` with unknown variable.
- Hostile values: `'; drop table`, `:other`, quotes, unicode, huge strings, NaN/inf, wrong types, extra fields,
  select value not in options. Confirm bound, and confirm the stored value is exactly the input.
- Authorisation: viewer without MODIFY refused; table-level MODIFY grant allows; group grant allows; SELECT-only refused;
  public/share-link and anonymous refused; author's rights irrelevant.
- Governance: masked column field refused; row-filtered viewer cannot update/delete outside their slice or insert outside
  it; exempt viewer can; `allow_subject_writes: false` blocks subjects.
- Integrity: update matching 0 or >`max_rows` rows refused with no change; two concurrent updates produce one success and
  one clean conflict; idempotent repeat returns the same outcome with one commit; `fixed` columns cannot be overridden by
  a client value.
- Audit and provenance: audit rows exist for success and refusal, without values; Delta commit properties carry app,
  action, user, request id.
- Abuse: rate limit, body cap, missing CSRF header, wrong content type.

`scratch/verify_app_writes_ui.py` (Playwright, throwaway studio): build an app with a form, submit, see the new row in a
table component; submit twice quickly (one row); a viewer without MODIFY sees a disabled form; an XSS string entered into a
field is stored and rendered back as text.

## 12. Decisions and open questions

Open (recommendation first):

- **Who may author an action?** Recommend: anyone who can create an app AND holds MODIFY on the target. Alternative:
  admin/power_user only until the feature has been used in anger.
- **Subjects writing** (viewers under masking/row filters): recommend default off (`allow_subject_writes: false`) in v1,
  turn on per app by an admin.
- **Delete**: recommend shipping it behind the per-action opt-in, or soft-delete only (a declared `deleted_at` column) in
  v1 and real delete later.
- **Approval workflow** in v1 or v2: recommend v2; idempotency, audit and commit properties cover most of the need.
- **Conflict handling** for editable rows: recommend optimistic (compare the row's version/`updated_at` read time with
  commit time via an optional declared `version_column`) over locking.
