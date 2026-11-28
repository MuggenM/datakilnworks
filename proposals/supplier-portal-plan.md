# Supplier Portal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Outside suppliers sign in with a passkey to a separate portal process and see, and download, only materialized
copies of datasets that an admin prepared and a second person approved for their organisation, with an append-only audit
trail.

**Architecture:** A second ASGI app (`web/portal.py`, own compose service) mounts only portal routes and shares accounts,
sessions and SQLite files with the studio. The studio builds the copies (a `materialize` job task running the source SQL
through the governance gateway as the audience), runs the release state machine and sends signpost notifications; the
portal only reads copies (DuckDB read-only) and writes its own audit rows.

**Tech Stack:** FastAPI/Starlette, SQLite, delta-rs (`deltalake`), DuckDB, sqlglot, py_webauthn, plain JS + Chart.js
(portal page), Playwright for the UI test.

**Spec:** `proposals/supplier-portal.md` (sections referenced as §N). This plan lives in `proposals/`, not `docs/`:
`/docs` is served without sign-in.

## Global Constraints

- Do not weaken: default-deny `/api`, `GOVERNANCE_REQUIRE_AUTH`, server-side sessions, CSRF gate, CSP, login throttle.
  A gate wrong for a legitimate case gets the narrowest possible exception.
- Every SQL run on behalf of a person or an audience goes through `gateway.govern_sql`; new DuckDB execution sites are
  added to `web/governance/ALLOWLIST.md` with a reason (`scratch/test_governance_coverage.py` enforces it).
- No blocking network I/O or DuckDB work in an `async def` route: use `asyncio.to_thread`. A worker thread never uses the
  shared `get_duckrun_conn()`.
- Tests are standalone scripts `scratch/test_portal_*.py` / `scratch/verify_portal_ui.py` with a `check()` helper,
  ending with `os._exit(0|1)`; each new test goes into `ci/plan.json` (insert the entry by hand; do not re-sort the file).
- Run a test locally: `CI_IMAGE=localspark-lakehouse-notebook CI_MOUNT_SOURCES=1 DOCKER_NETWORK=datakilnworks_default ci/run_test.sh <name>`.
- Throwaway warehouses only (`WAREHOUSE_DIR=tempfile.mkdtemp()`); set `INIT_ADMIN_*` before importing `web.auth`;
  mint sessions with `auth.create_access_token(user)`; dummy endpoints are IP addresses (`http://127.0.0.1:9`), never
  made-up hostnames.
- Conventional Commits, branch `development`, push after each task; end messages with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Update CLAUDE.md in the same commit as the feature.
- Values from the spec: idle timeout 30 min for `external`; account expiry 12 months; GDPR reference required, max 200
  chars; reminder after 3 days pending; widget rows 5,000; export rows 1,000,000; query timeout 30 s; 60 queries/min and
  20 exports/hour per user; schedule default daily; formula-escape cells starting with `=`, `+`, `-`, `@`, tab or CR by
  prefixing `'`.
- Notifications carry only: event, release name, audience name, preparer, link. Never a profile, column name, data or
  GDPR reference (§8).

## Calls made in this plan (beyond the spec's own)

- Portal session cookie is `dkw_portal_session`, not the studio's cookie: cookies ignore ports, so on one host the studio
  cookie would otherwise reach the portal.
- `auth.session_user(token, surface="studio")`: the default surface refuses `external`, so every existing caller is
  closed to external accounts without being touched.
- For role `external` (and the audience principal) the default `warehouse` catalog is **not** implicitly open: the admin
  grants the audience group SELECT on exactly the source tables/schemas. Otherwise every warehouse table would be
  readable by every audience.
- A role switch to or from `external` is refused (an internal account with grants must never become an outside one).
- The release state lives on the dataset row (one release per dataset); history is the audit trail.
- The copy is always vacuumed with zero retention after a write (an overwrite replaces every file, so this is the
  "physical revocation" of §6 without a special case).
- Build order: compute → profile → compare with the approved hash → only then write. A drifted build writes nothing.
- Each dataset owns one managed job `portal_<dataset id>` (owner `system`, `managed_by: "portal"`); the jobs UI/API
  refuses to edit or delete it.
- `portal.db` lives in `$WAREHOUSE_DIR/.metadata/`; the portal writes its audit rows there, so the warehouse is not
  mounted read-only for the portal (the spec's "read-only where the chart allows" does not apply).
- `portal_audit` is protected by SQLite triggers that abort UPDATE and DELETE.
- Tags are propagated onto a copy by tracing a synthetic `CREATE TABLE portal.<s>.<t> AS <source_sql>` with
  `Principal.system()` through `gateway.propagate_tags`; nothing is executed by that call.

## Review Focus

1. A source table in the default `warehouse` catalog with no grant for the audience: the build must fail closed with a
   message naming the table, not copy it (Task 2 test `audience_without_grant_cannot_read_warehouse`).
2. Grants of the audience group change (a table grant revoked) without any policy change: the governance version does
   not move, so the copy must still turn stale and be blocked (Task 8 test `grant_revoke_blocks_copy`).
3. A widget query that tries to reach outside its datasets (`portal.other_audience.t`, `warehouse.dbo.t`,
   `read_parquet(...)`, `delta_scan(...)`, a CTE named `ds`): refused at app validation AND at query time (Task 12 test
   `foreign_table_refused`).
4. An external user's account expires while a session is open: the next request is 401, not served until the idle
   timeout (Task 5 test `expiry_ends_open_session`).
5. Two approvers click approve at the same moment, or the preparer approves through the API directly: one 200, one 409;
   the preparer gets 403 (Task 9 tests `concurrent_decisions_one_409`, `preparer_cannot_approve`).

---

## Phase 1: Separation (§4, §12.1)

### Task 1: Shared HTTP security module

**Files:**
- Create: `web/http_security.py`
- Modify: `web/app.py:224-520` (the gate definitions), `web/csp.py:62` (`policy_for`)
- Test: existing `scratch/test_csrf.py`, `scratch/test_ip_allowlist.py`, `scratch/test_network_policy.py`,
  `scratch/test_login_hardening.py`, `scratch/test_api_auth_gate.py`, `scratch/verify_csp_ui.py`

**Interfaces:**
- Produces: `install(app, surface: str, *, cookie_name: str) -> None` (surface `"studio"` | `"portal"`);
  `cookie_secure(request) -> bool`; `set_session_cookie(response, token: str, request, *, cookie_name: str) -> None`;
  `csp.policy_for(path: str, nonce: str, surface: str = "studio") -> str`.

- [ ] **Step 1:** Move the bodies of `_network_policy_gate`, `_csrf_origin_gate`, `_ip_allowlist_gate`,
  `_security_headers` and `_cookie_secure` into `web/http_security.py` unchanged in behaviour. `install()` registers them
  with `app.middleware("http")` in that order (so `_security_headers` stays outermost, then the IP allowlist, then CSRF),
  reading the session cookie by `cookie_name`. In `app.py`, register the studio-only gates (`_api_auth_gate`,
  `_must_change_password_gate`, `_mfa_policy_gate`, `_volume_upload_size_gate`) first, then call
  `http_security.install(app, "studio", cookie_name=<existing name>)`. Keep `app._cookie_secure` etc. as aliases so
  existing imports in tests keep working.
- [ ] **Step 2:** `csp.policy_for(..., surface="portal")` returns `script-src 'self'` (no nonce, no `'unsafe-eval'`, no
  CDN), `style-src 'self'`, `img-src 'self' data:`, `connect-src 'self'`, `frame-ancestors 'none'` for every portal path.
- [ ] **Step 3:** Run the six tests listed above. Expected: all pass unchanged (this is a pure move).
- [ ] **Step 4:** Commit `refactor(security): factor the HTTP security middlewares into web/http_security.py`.

### Task 2: Roles `external` and `release_approver`, and surfaces

**Files:**
- Modify: `web/auth.py:80-110` (users CHECK), `:287` (`session_user`), `:412`, `:450` (role checks); `web/app.py:1360`
  (roles list); `web/permissions.py:116` (`can_user_access_catalog`)
- Test: `scratch/test_portal_roles.py` (new)

**Interfaces:**
- Produces: `auth.ROLES = ("admin", "power_user", "user", "release_approver", "external")`;
  `auth.INTERNAL_ROLES` (all but `external`); `auth.session_user(token, surface: str = "studio") -> Optional[dict]`.

- [ ] **Step 1: Write the failing test** `scratch/test_portal_roles.py`:
  - `old_db_migrates`: an auth.db created with the old CHECK and every current column, holding 3 users → after
    `init_auth_db()` all 3 rows and every column value are intact and `create_user(..., role="external")` works.
  - `studio_refuses_external`: an external user's `create_access_token` → `session_user(tok)` is None,
    `session_user(tok, surface="portal")` is the user; TestClient on `web.app` with that cookie gets 401 on
    `GET /api/catalogs` and `GET /api/auth/me` answers `authenticated: false`.
  - `portal_refuses_internal`: admin token → `session_user(tok, surface="portal")` is None.
  - `role_switch_refused`: `update_user(id, role="external")` on a `user` raises ValueError, and the reverse too.
  - `audience_without_grant_cannot_read_warehouse`:
    `permissions.can_user_access_catalog({"role": "external", ...}, "warehouse")` is False; for `role="user"` it is
    still True.
- [ ] **Step 2:** Run it; expect FAIL (CHECK constraint / missing parameter).
- [ ] **Step 3:** Implement. `_migrate_role_check(conn)`: read the `users` DDL from `sqlite_master`; if it lacks
  `'external'`, inside one transaction create `users_new` from that DDL with the CHECK list replaced, `INSERT INTO
  users_new SELECT * FROM users`, drop, rename, recreate the table's indexes from `sqlite_master`, then
  `PRAGMA integrity_check`. `session_user` returns None when `surface == "studio"` and the role is `external`, or when
  `surface == "portal"` and the role is not `external`. In `can_user_access_catalog`, the `warehouse` shortcut applies
  only to `admin`, `power_user` and `user`.
- [ ] **Step 4:** Run the new test plus `test_sessions.py`, `test_api_auth_gate.py`, `test_webauthn.py`, `test_scim.py`;
  expect PASS.
- [ ] **Step 5:** Add to `ci/plan.json`, CLAUDE.md (Authentication: the two roles, surfaces); commit
  `feat(auth): external and release_approver roles; studio refuses external sessions`.

### Task 3: Portal app skeleton with passkey sign-in, compose service

**Files:**
- Create: `web/portal.py`, `web/static/portal/index.html`, `web/static/portal/portal.js`, `web/static/portal/portal.css`
- Modify: `docker-compose.yml` (service `datakilnworks-portal`, Traefik router under the `proxy` profile),
  `deploy/traefik/dynamic/routers.yml`, `web/init.py` (nothing yet; just confirm the portal needs no step of its own)
- Test: `scratch/test_portal_surface.py` (new)

**Interfaces:**
- Consumes: `http_security.install`, `auth.session_user(..., surface="portal")`, `webauthn_auth.authentication_options`,
  `finish_authentication`, `enrollment_user`, `registration_options`, `finish_registration`, `consume_enrollment`.
- Produces: `portal: FastAPI` (`docs_url=None, redoc_url=None, openapi_url=None`); `PORTAL_COOKIE = "dkw_portal_session"`;
  `portal_user(request) -> dict` (401 otherwise); `PORTAL_ROUTES: set[tuple[str, str]]`, the exact allow-list the
  surface test compares against.

- [ ] **Step 1: Write the failing test** `scratch/test_portal_surface.py`:
  - `exact_route_list`: `{(m, r.path) for r in portal.routes for m in r.methods}` minus HEAD equals `PORTAL_ROUTES`,
    which for this phase is: POST `/portal/api/auth/passkey/options`, `/portal/api/auth/passkey/login`,
    `/portal/api/auth/enroll/options`, `/portal/api/auth/enroll/verify`, `/portal/api/auth/logout`; GET
    `/portal/api/me`, `/`, and the static mount `/static/portal`.
  - `no_studio_route`: for every operation in `web.app.app.openapi()["paths"]`, the portal TestClient answers 404 (or 405
    for a mounted path with another method).
  - `no_web_app_import`: in a fresh subprocess, `import web.portal` leaves `"web.app"` out of `sys.modules`.
  - `external_refused_everywhere_in_studio`: with an external session cookie, every studio `/api` operation (walk
    `openapi()` like `test_api_auth_gate.py`, path parameters filled with `x`) answers 401, except `GET /api/auth/me`
    (200, `authenticated: false`) and the public sign-in list.
  - `internal_refused_on_portal`: an admin session (cookie `dkw_portal_session`) gets 401 on `GET /portal/api/me`.
  - `headers`: `GET /` on the portal carries the portal CSP (`script-src 'self'`, no `unsafe-eval`), nosniff and
    `X-Frame-Options`.
  - `body_cap`: a POST to any portal route with `Content-Length` above 64 KB answers 413 before the route runs.
  - `portal_off`: with `PORTAL=off` every portal path answers 503 and nothing is served.
- [ ] **Step 2:** Run; expect FAIL (module missing).
- [ ] **Step 3:** Implement `web/portal.py`: create the app, `http_security.install(portal, "portal",
  cookie_name=PORTAL_COOKIE)`, a `_portal_gate` middleware (every `/portal/api/*` except the four sign-in steps needs
  `portal_user`), the sign-in routes as thin wrappers over `webauthn_auth` (same shapes as `/api/auth/passkey/*` and
  `/api/auth/enroll/*` in `app.py:1192-1270`; passkey login additionally requires role `external`), and logout through
  `auth.end_session`. `GET /` serves `web/static/portal/index.html` as a file (no Jinja). Import nothing from `web.app`.
  A `_portal_body_cap` middleware refuses `Content-Length` > 64 KB (413); `PORTAL=off` (read per request) answers 503
  on every path. The page: a sign-in screen (passkey button, `?enroll=` handling like the studio's `initApp`), then "Signed in as …".
  No inline script or style.
- [ ] **Step 4:** Compose: service `datakilnworks-portal`, same image, `command: uvicorn web.portal:portal --host
  0.0.0.0 --port 8000 --no-proxy-headers`, port `${PORTAL_PORT:-8892}:8000`, the studio's volumes
  (warehouse, `dkw-secrets`), env `DKW_SECRETS_DIR`, `WEBAUTHN_RP_ID=${PORTAL_RP_ID:-localhost}`,
  `WEBAUTHN_ORIGINS=${PORTAL_PUBLIC_URL:-http://localhost:8892}`, `PORTAL_PUBLIC_URL`, umask 002, `depends_on` the init
  container. No Docker socket, not on `sandbox-net` or `control-net`. Check with `docker compose config -q` and with
  the profile variant.
- [ ] **Step 5:** Run the surface test, `test_api_auth_gate.py`, `test_webauthn.py`; expect PASS.
- [ ] **Step 6:** CI plan, CLAUDE.md (new "Supplier portal" bullet: process split, cookie, route list); commit
  `feat(portal): separate portal process with passkey sign-in only`.

## Phase 2: Identities (§5, §12.2)

### Task 4: Audiences, approver team, invitations

**Files:**
- Create: `web/portal_admin.py` (studio router, included in `app.py` next to `dashboard_apps_module.router`)
- Modify: `web/groups.py:72` (column `kind`), `:222` (`add_members`)
- Test: `scratch/test_portal_identities.py` (new)

**Interfaces:**
- Consumes: `auth.create_passwordless_user`, `webauthn_auth.issue_enrollment`.
- Produces: `groups.set_kind(group_id, kind: str, actor) -> dict` (kind `""|"audience"|"approver_team"`);
  `groups.audience_of(user: dict) -> Optional[str]`; `groups.approver_team_id() -> Optional[str]`;
  `portal_admin.router`; routes (admin only): `POST/GET /api/portal/audiences`,
  `POST /api/portal/audiences/{gid}/invite` `{username, display_name}` → `{user, enroll_url}`,
  `POST /api/portal/users/{uid}/link`, `POST /api/portal/users/{uid}/renew`, `PUT /api/portal/approver-team`
  `{group_id}`.

- [ ] **Step 1: Write the failing test** (part 1 of `test_portal_identities.py`):
  - `invite_creates_external`: invite → user role `external`, `passwordless`, member of exactly that audience;
    `enroll_url` starts with `PORTAL_PUBLIC_URL + "/?enroll="`.
  - `one_audience_only`: adding that user to a second audience raises ValueError (API: 409); adding them to a plain
    (`kind=""`) group is also refused (an external user's access is exactly their audience).
  - `internal_not_in_audience`: adding a `user`-role account to an audience is refused.
  - `one_approver_team`: setting a second group as approver team replaces the first (v1 has one).
  - `non_admin_refused`: every `/api/portal/*` admin route answers 403 for a power user.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement: `ALTER TABLE user_groups ADD COLUMN kind TEXT NOT NULL DEFAULT ''` in `_conn()`. Audience
  creation also creates schema `portal.<slug>` (Task 6 adds the catalog; until then the route creates the group only and
  Task 6 adds the schema step). Every action is audited through `portal_store.audit` once Task 6 exists; until then
  `groups._audit`.
- [ ] **Step 4:** Run; expect PASS. Commit `feat(portal): audiences, approver team and passkey invitations for suppliers`.

### Task 5: Idle timeout, expiry, approver gate

**Files:**
- Modify: `web/sessions.py:47` (column `idle_limit`), `:79` (`create`), `:108` (`validate`); `web/auth.py:246`
  (`create_access_token`), `:287` (`session_user`), users column `expires_at`, `expiry_noted_at`; `web/app.py`
  (`_approver_gate`, `startup_event`: `portal_lifecycle_loop`); `web/templates/index.html` (approver sees only the
  Releases view)
- Test: `scratch/test_portal_identities.py` (part 2)

**Interfaces:**
- Produces: `sessions.create(..., idle_limit: int = 0)`; `auth.account_expired(user) -> bool`;
  `portal_admin.renew(user_id, actor) -> int` (new `expires_at`); `_APPROVER_ALLOWED` in `app.py`.

- [ ] **Step 1: Write the failing tests:**
  - `external_idle_30_min`: an external session with `last_seen_at` set 31 min back is refused (and revoked); an admin
    session the same age is still live (`SESSION_IDLE_MINUTES=0`).
  - `expiry_ends_open_session`: set `expires_at = now - 1` on an external user with a live session → the next
    `session_user(tok, "portal")` is None; renew → a new sign-in works and `expires_at` is about now + 365 days.
  - `lifecycle_loop_audits_once`: two ticks of `portal_lifecycle_tick()` on an expired user → their sessions are revoked
    and exactly one `account_expired` audit row exists.
  - `approver_confined`: a `release_approver` session gets 200 on `/api/auth/me` and on `GET /api/portal/review`, and 403
    on `GET /api/catalogs`, `POST /api/sql/execute` and `GET /api/users`.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement. `validate` uses the stricter non-zero value of `idle_seconds()` and the row's `idle_limit`;
  `create_access_token` passes `int(os.getenv("PORTAL_IDLE_MINUTES", "30")) * 60` for role `external`. The invite sets
  `expires_at = now + PORTAL_ACCOUNT_MONTHS (default 12) * 30.44 days`. `_approver_gate` (registered with the other
  studio gates) lets a `release_approver` reach only exact paths and prefixes: `/api/auth/me`, `/api/auth/logout`,
  `/api/auth/sessions`, `/api/auth/webauthn/`, `/api/auth/mfa/`, `/api/auth/change-password`, `/api/portal/review`,
  `/api/portal/reports`; 403 otherwise. The SPA shell `/` stays reachable; in `index.html` `onUserChanged` routes a
  `release_approver` to the Releases view and hides the nav.
- [ ] **Step 4:** Run part 1 + part 2, `test_sessions.py`, `test_mfa_policy.py`; expect PASS.
- [ ] **Step 5:** CI plan, CLAUDE.md; commit `feat(portal): idle timeout and expiry for supplier accounts, approver confinement`.

## Phase 3: Datasets and copies (§6, §12.3)

### Task 6: Portal store, catalog and grant guard

**Files:**
- Create: `web/portal_store.py`
- Modify: `web/init.py` (`_database_steps`: `portal_store.init_portal_db`, `portal_store.ensure_portal_catalog`),
  `web/groups.py:393` (`grant`), `web/permissions.py` (`grant_catalog_permission`), `web/portal_admin.py`
  (dataset CRUD routes, audience schema)
- Test: `scratch/test_portal_datasets.py` (new, part 1)

**Interfaces:**
- Produces: `init_portal_db()`; `ensure_portal_catalog()`; `audit(actor: dict|str, event: str, *, audience=None,
  dataset=None, profile_hash=None, gdpr_ref=None, outcome="ok", detail=None) -> None` (writes `portal_audit` and one
  `governance_audit` mirror row via `store.write_audit`); `validate_dataset(raw: dict, audience_gid: str) -> dict`;
  `get_dataset(id)`, `list_datasets(audience=None)`, `create_dataset(raw, actor)`, `update_dataset(id, raw, actor)`
  (any edit sets state `draft`); `copy_location(dataset) -> str` (`<portal catalog path>/<audience slug>/<name>`).
  Tables: `portal_datasets(id, name, audience_group_id, source_sql, columns JSON, row_cap, schedule, downloadable,
  state, submitted_by, submitted_at, decided_by, decided_at, gdpr_ref, approved_profile JSON, approved_hash,
  pending_profile JSON, pending_hash, created_by, created_at)`, `portal_builds(id, dataset_id, started_at, finished_at,
  status, rows, delta_version, gov_version, state_hash, profile_hash, error)`, `portal_audit(id, ts, actor_id,
  actor_name, event, audience, dataset, profile_hash, gdpr_ref, outcome, detail JSON)`, `portal_channels` (Task 10).

- [ ] **Step 1: Write the failing tests:**
  - `audit_append_only`: `UPDATE portal_audit ...` and `DELETE FROM portal_audit` both raise `sqlite3.IntegrityError`
    (trigger `RAISE(ABORT, 'portal_audit is append-only')`); a mirror row exists in `governance_audit`.
  - `dataset_validation`: refused, each with a message: `SELECT *`, `SELECT t.*`, two statements, an INSERT, an output
    name list different from `columns`, `row_cap` 0 or above 1,000,000, a bad cron, a name outside `^[a-z][a-z0-9_]{0,62}$`.
    Default `row_cap` 100,000, default schedule `0 6 * * *`.
  - `portal_grants_guarded`: `groups.grant("schema", "portal.acme", "group:<other audience>", ...)` raises ValueError;
    the same for a user principal; for the matching audience it succeeds. `grant_catalog_permission` on catalog `portal`
    for any principal raises.
  - `portal_catalog_exists_read_only`: after init, catalog `portal` exists with `read_only` true; SQL `INSERT INTO
    portal.acme.x ...` through `execute_sql` is refused.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement. Audience creation now also creates `portal.<slug>` and grants the audience group schema
  SELECT on it (`groups.grant("schema", f"portal.{slug}", f"group:{gid}", "SELECT", actor)`).
- [ ] **Step 4:** Run; expect PASS; also `test_init.py` (database count goes up by one) and `test_groups.py`.
- [ ] **Step 5:** Commit `feat(portal): portal store, append-only audit and the portal catalog`.

### Task 7: Building a copy as the audience (`materialize`)

**Files:**
- Create: `web/portal_build.py`
- Modify: `web/workflow.py:218` (`execute_task` branch), `:475` (`TASK_TYPES`), `_task_write_targets`; `web/groups.py:272`
  (`group_ids_for`); `web/app.py:6919`, `:6944` (refuse `managed_by` jobs); `web/governance/ALLOWLIST.md`
- Test: `scratch/test_portal_datasets.py` (part 2)

**Interfaces:**
- Consumes: `gateway.govern_sql`, `gateway.propagate_tags`, `permissions.enforce_sql_permissions`, `store.get_version`.
- Produces: `audience_principal(gid) -> Principal` (`username="\0audience:" + gid`, `role="external"`,
  `groups=frozenset({gid})`); `audience_user(gid) -> dict` (`{"id": "", "username": ..., "role": "external",
  "_audience_group": gid}`, honoured by `groups.group_ids_for`); `@dataclass BuildResult(table: pa.Table|None, rows: int,
  profile: dict, profile_hash: str, gov_version: int, state_hash: str, error: str|None)`;
  `compute(dataset, conn) -> BuildResult` (nothing written); `write_copy(dataset, result) -> int` (Delta version);
  `profile_of(dataset, table: pa.Table, rewrite: RewriteResult, tags: dict) -> dict`;
  `profile_hash(profile) -> str` (sha256 of canonical JSON without `rows`); `audience_state_hash(gid) -> str`;
  `run_task(dataset_id, conn) -> dict` (the job task); `portal_jobs.sync_job(dataset)` in `portal_build.py`.

- [ ] **Step 1: Write the failing tests** (throwaway warehouse with table `warehouse.dbo.orders(id, supplier, amount,
  contact_email)`; `contact_email` tagged `pii=email` with a masking policy `hash` exempting admin; a row filter policy
  in `attribute` mode on `supplier` with the audience group's attribute value `acme`):
  - `copy_equals_governed_result`: after a build, the copy holds exactly the rows/values that `govern_sql` returns for
    `audience_principal(gid)` (only `supplier = 'acme'`, `contact_email` masked), and the copy's `contact_email`
    column carries the `pii` tag.
  - `admin_rights_not_used`: the job's owner is an admin; the copy is still filtered and masked.
  - `row_cap`: `row_cap=2` with 3 matching rows → build status `failed`, error mentions the row cap, no Delta version
    written.
  - `audience_without_grant_cannot_read_warehouse`: without a table grant for the audience on `dbo.orders` the build
    fails with the table named; with `GRANT SELECT ON TABLE warehouse.dbo.orders TO <group>` it succeeds.
  - `old_versions_gone`: build twice; after the second, `DeltaTable(copy).load_as_version(0)` has no readable data file
    (the vacuum removed it) and only the current files exist on disk.
  - `managed_job_locked`: the dataset's job `portal_<id>` exists with the dataset's cron; `PUT`/`DELETE` of it through
    the jobs API answers 409.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement `compute`: `enforce_sql_permissions(sql, audience_user(gid))`, `govern_sql(sql,
  audience_principal(gid))`, run `SELECT * FROM (<rewritten>) LIMIT <row_cap + 1>` on the given connection to Arrow,
  refuse above the cap, build the profile (columns and types, tags per column via tags of the copy as they WOULD be
  propagated, `masked_columns_payload`, `row_filter_payload`, rows, downloadable, audience, schedule, sha256 of
  `source_sql`). `write_copy`: `write_deltalake(loc, table, mode="overwrite", schema_mode="overwrite")`, then
  `DeltaTable(loc).vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)`, then tag propagation
  (see "Calls made"). `run_task` decides by state (Task 9 fills in the drift branch; here: state `approved` or
  `published` and hash equal → write; else record `blocked`) and writes a `portal_builds` row plus an audit row.
  `materialize` tasks need `dataset_id`; `_task_write_targets` returns `portal.<slug>.<name>`. Add the build's DuckDB
  execution to `ALLOWLIST.md` ("governed as the audience").
- [ ] **Step 4:** Run; expect PASS; also `test_workflow_orchestration.py`, `test_governance_coverage.py`,
  `test_governance_groups.py`.
- [ ] **Step 5:** Commit `feat(portal): materialize task builds copies as the audience`.

### Task 8: Staleness, blocking, withdrawal

**Files:**
- Modify: `web/portal_build.py`, `web/app.py` (`startup_event`: `portal_watch_loop`), `web/portal_admin.py`
  (audience delete)
- Test: `scratch/test_portal_datasets.py` (part 3)

**Interfaces:**
- Produces: `copy_state(dataset) -> str` (`"ok" | "stale" | "missing"`); `drop_copy(dataset, actor, reason) -> None`;
  `portal_watch_tick() -> list[str]` (dataset ids it queued); `delete_audience(gid, actor)`.

- [ ] **Step 1: Write the failing tests:**
  - `policy_change_blocks_copy`: after a masking policy edit, `copy_state` is `stale`; `portal_watch_tick()` queues the
    dataset's job; after the rebuild it is `ok`.
  - `grant_revoke_blocks_copy`: revoking the audience's table grant (no policy change) makes the copy `stale`, and the
    rebuild fails closed (the copy stays blocked, never served).
  - `attribute_change_blocks_copy`: changing the group's principal attribute value makes it `stale`.
  - `withdraw_drops_copy`: `drop_copy` removes the table directory; `copy_state` is `missing`; audit row `withdrawn`
    with the reason.
  - `audience_delete`: deleting an audience drops every copy, removes the schema grant, revokes its members' sessions and
    deactivates them (accounts kept for the audit trail).
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement. `audience_state_hash(gid)` = sha256 over the sorted rows of `resource_grants` and
  `catalog_permissions` naming `group:<gid>`, plus `principal_attributes` with `principal_type='group'` and value gid.
  `copy_state` compares the last successful build's `gov_version` with `store.get_version()`, its `state_hash` with
  `audience_state_hash`, and its source hash with the dataset's. `portal_watch_loop` runs `portal_watch_tick` every 15 s
  in a thread (`asyncio.to_thread`) and starts `workflow.start_run_in_background("portal_<id>", trigger="PORTAL_STALE")`
  once per stale dataset (no new start while a run is RUNNING).
- [ ] **Step 4:** Run; expect PASS.
- [ ] **Step 5:** CI plan, CLAUDE.md; commit `feat(portal): rule changes block copies until rebuilt; withdrawal drops them`.

## Phase 4: Release flow (§7, §10, §12.4)

### Task 9: State machine, profile diff, four-eyes approval

**Files:**
- Create: `web/portal_release.py`
- Modify: `web/portal_build.py` (`run_task` drift branch), `web/portal_admin.py` (routes)
- Test: `scratch/test_portal_releases.py` (new)

**Interfaces:**
- Consumes: `portal_build.compute`, `write_copy`, `profile_hash`; `groups.approver_team_id`.
- Produces: `class ReleaseConflict(Exception)` (409), `class ReleaseForbidden(Exception)` (403);
  `submit(dataset_id, actor) -> dict`, `approve(dataset_id, actor, gdpr_ref: str) -> dict`,
  `reject(dataset_id, actor, reason: str) -> dict`, `withdraw(dataset_id, actor, reason: str) -> dict`,
  `profile_diff(old: dict, new: dict) -> list[dict]` (`{kind: "column_added"|"column_removed"|"type_changed"|"tag_added"|
  "tag_removed"|"mask_removed"|"mask_added"|"filter_changed"|"setting_changed", column, before, after}`).
  Routes: admin `POST /api/portal/datasets/{id}/submit`; approver team `GET /api/portal/review`,
  `GET /api/portal/review/{id}` (profile, diff against the approved one), `POST /api/portal/review/{id}/approve`
  `{gdpr_ref}`, `POST .../reject` `{reason}`; admin or team `POST /api/portal/datasets/{id}/withdraw` `{reason}`.

- [ ] **Step 1: Write the failing tests:**
  - `happy_path`: draft → submit (state `pending_review`, `pending_hash` set) → approve by a team member with
    `gdpr_ref="GDPR-2026-014"` → first build → `published`; audit rows `submitted`, `approved` (with the GDPR ref and
    profile hash), `build_ok`, `published`, in that order.
  - `preparer_cannot_approve`: an admin who is in the team and submitted gets 403; the state is unchanged.
  - `non_member_cannot_approve`: an admin outside the team gets 403; a member removed from the team between submit and
    decision gets 403.
  - `concurrent_decisions_one_409`: two threads approve at once → exactly one success, one `ReleaseConflict`.
  - `gdpr_ref_rules`: empty or 201 chars → 400; a rejection needs a reason.
  - `edit_returns_to_draft`: editing a `pending_review` dataset sets `draft` and clears `pending_*`.
  - `drift_needs_reapproval`: for each of add a column, tag a column `pii`, delete the mask: the rebuild writes no new
    Delta version, the state is `needs_reapproval`, the copy is blocked, `GET /api/portal/review/{id}` shows the
    matching diff entry; approving publishes the new profile.
  - `row_count_not_drift`: adding source rows changes `rows` but not the hash; the rebuild publishes.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement every transition as `UPDATE portal_datasets SET ... WHERE id = ? AND state = ?`; rowcount 0 →
  `ReleaseConflict`. Team membership is checked inside the decision call. `submit` runs `compute` (dry) in a worker
  thread and freezes `pending_profile`/`pending_hash`; `approve` moves pending to approved and starts the dataset's job.
  `withdraw` calls `drop_copy`.
- [ ] **Step 4:** Run; expect PASS.
- [ ] **Step 5:** CI plan, CLAUDE.md; commit `feat(portal): release flow with frozen profile and four-eyes approval`.

### Task 10: Studio screens for preparers and approvers

**Files:**
- Modify: `web/templates/index.html` (nav item "Portal" for admins: audiences, members, invite, datasets, submit,
  withdraw; "Releases" for the approver team: list, profile, diff, approve/reject dialog)
- Test: covered by `scratch/verify_portal_ui.py` (Task 13); run `scratch/verify_csp_ui.py` here

- [ ] **Step 1:** Build the views with the existing Alpine patterns (one state object per view; loads note `authEpoch`
  before fetching and drop the answer if it changed). The invite dialog shows the enrolment link once with a copy button (delegated
  `[data-copy-code]` listener, no inline handler). The approve dialog requires the GDPR reference (max 200) before the
  button enables. Every profile value is rendered with `x-text`, never `x-html`.
- [ ] **Step 2:** Run `verify_csp_ui.py`; expect zero violations.
- [ ] **Step 3:** Commit `feat(ui): portal administration and release review screens`.

## Phase 5: Notifications (§8, §12.5)

### Task 11: Signpost notifications and reminders

**Files:**
- Create: `web/portal_notify.py`
- Modify: `web/portal_store.py` (`portal_channels(team_group_id, kind, target, enabled)`), `web/portal_release.py`
  (call sites), `web/portal_build.py` (`portal_watch_tick`: reminders), `web/portal_admin.py`
  (`GET/PUT /api/portal/channel`)
- Test: `scratch/test_portal_notifications.py` (new)

**Interfaces:**
- Consumes: `email_reports.send_email(recipients, subject, body_html, body_text)`,
  `slack_integration.send_notification(message, webhook_id, title)`, `webhook_alerts.send_webhook(webhook_id, title,
  message)` (covers kinds `teams` and `webhook`).
- Produces: `signpost(event: str, dataset: dict, actor_name: str) -> dict` with exactly the keys `event`,
  `release`, `audience`, `preparer`, `link` (`<studio url>/?portal_review=<id>`); `notify(event, dataset, actor) ->
  dict` (outcome); events `submitted`, `reminder`, `approved`, `rejected`, `needs_reapproval`, `withdrawn`.

- [ ] **Step 1: Write the failing tests** (the three send functions monkeypatched to record their arguments):
  - `payload_allow_list`: for every event and every channel kind, the recorded subject/body/message contains the release
    and audience names and the link, and contains none of: any column name, the GDPR reference, the profile hash, any
    cell value of the copy.
  - `failure_audited`: a send function raising → one audit row `notification_failed` with the error class only (no
    message text); the release state is unchanged.
  - `reminder_after_3_days`: `submitted_at` 3 days + 1 min back → one `reminder`; a second tick the same day sends none.
  - `decision_to_preparer`: approve/reject notify the preparer's own email when the channel kind is email, the team
    channel otherwise.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement; sends run in a worker thread; the Teams message uses the existing `teams` webhook type
  unchanged (§8).
- [ ] **Step 4:** Run; expect PASS.
- [ ] **Step 5:** CI plan, CLAUDE.md; commit `feat(portal): signpost notifications to the approver team`.

## Phase 6: The viewer (§9, §12.6)

### Task 12: Portal apps, queries, limits, downloads

**Files:**
- Create: `web/portal_apps.py`
- Modify: `web/portal.py` (routes `GET /portal/api/apps`, `GET /portal/api/apps/{id}`,
  `POST /portal/api/apps/{id}/widgets/{wid}/query`, `GET /portal/api/apps/{id}/widgets/{wid}/export`; add them to
  `PORTAL_ROUTES`), `web/dashboard_apps.py:515` (`validate_spec` calls the portal rule when `portal` is set),
  `web/governance/ALLOWLIST.md`
- Test: `scratch/test_portal_viewer.py` (new); `scratch/test_portal_surface.py` (route list update)

**Interfaces:**
- Consumes: `dashboard_apps.resolve_values`, `bind_sql`, `hydrate_widget`; `portal_build.copy_state`; `groups.audience_of`.
- Produces: dashboard record field `portal: {"audiences": [gid, ...]}`; `validate_portal_app(dash) -> None`
  (ValueError); `rewrite_ds(sql: str, audience_slug: str) -> str`; `visible_apps(user) -> list[dict]` (spec without any
  `query` field); `run_widget(user, app_id, widget_id, params) -> dict`; `export(user, app_id, widget_id, params, fmt)
  -> (bytes, media_type, filename)`; `escape_cell(v) -> Any`; `RateLimiter(per_minute=60)` / exports 20 per hour.

- [ ] **Step 1: Write the failing tests:**
  - `apps_per_audience`: two audiences, app A released to `acme` only → an acme user lists A, a `beta` user lists
    nothing; the listing and `GET /portal/api/apps/{id}` contain no `query` or SQL text anywhere (search the JSON).
  - `app_hidden_until_published`: an app whose dataset is `pending_review` or `needs_reapproval` is not listed.
  - `foreign_table_refused`: validation refuses widget SQL naming `portal.beta.t`, `warehouse.dbo.orders`, `orders`,
    `read_parquet('x')`, `delta_scan('x')`, `VERSION AS OF`, a CTE named `ds`, and `ds.unknown`; a query built at run
    time can only be the saved one (the request body has no SQL field: extra fields are ignored).
  - `rewrite`: `rewrite_ds("SELECT a FROM ds.orders o JOIN ds.items i ON ...", "acme")` names
    `portal.acme.orders` and `portal.acme.items` and nothing else.
  - `stale_answers_503`: a stale copy → the widget query answers 503 "being updated", nothing from the old copy.
  - `limits`: a widget returning 6,000 rows is cut to 5,000 with `truncated: true`; the 61st query in a minute → 429; the
    21st export in an hour → 429; a 40 s query is interrupted at 30 s (use a `range()` cross join; skip if the runner is
    too fast to reach 30 s, never sleep in SQL).
  - `downloads`: a non-`downloadable` dataset → 403; CSV and XLSX cells `=1+1`, `+x`, `-x`, `@x`, `\tx`, `\rx` come out
    prefixed with `'`; Parquet is untouched; one audit row `export` with dataset, format and row count.
  - `cache_key`: a result cached for copy version N is not served after a rebuild to version N+1.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement. The viewer's principal is the external user (`Principal.from_user`), so the portal grant
  (schema SELECT on its own `portal.<slug>`) and any propagated policy apply again on read (defence in depth). DuckDB is
  opened `read_only=True` on a fresh connection per request in a worker thread; `interrupt()` from a `threading.Timer`
  enforces the timeout. Cache key: `(gateway.fingerprint(rewrite), delta_version, app_id, widget_id, literals)`. Widget
  views and exports are audited (views aggregated per session per widget).
- [ ] **Step 4:** Run the viewer and surface tests; expect PASS.
- [ ] **Step 5:** Commit `feat(portal): released apps, governed widget queries, limits and escaped downloads`.

### Task 13: Viewer page and the end-to-end UI test

**Files:**
- Modify: `web/static/portal/index.html`, `portal.js`, `portal.css`
- Create: `scratch/verify_portal_ui.py`
- Modify: `.github/workflows/ci.yml` / `ci/plan.json` (UI list)

- [ ] **Step 1:** Page: app list → app with pages, typed parameters (select/date/number/text), widgets KPI, table, bar,
  line (Chart.js from `/static/chart.min.js`), text (port of the studio's escaped `renderTextBlock` into `portal.js`);
  every value through `textContent`; each widget shows "as of <build time>"; the header shows the organisation and the
  signed-in person; a download button only where allowed.
- [ ] **Step 2: Write `verify_portal_ui.py`** (throwaway studio + portal containers on a throwaway network,
  `DOCKER_NETWORK` honoured; Chromium virtual authenticator via CDP, like `verify_passkeys_ui.py`):
  admin creates audience, dataset, app, invites a supplier; an approver (second account) approves in the studio with a
  GDPR reference; the supplier enrols through the link on the portal, sees only the released app, changes a parameter,
  sees the as-of time, downloads CSV and the file has the `'=` escape; a title and a cell containing
  `<img src=x onerror=alert(1)>` render as text (no dialog, no CSP report); the admin withdraws, and after reload the app
  is gone and its query answers 404/503; the supplier's session on the studio URL gets the sign-in page and every `/api`
  call 401.
- [ ] **Step 3:** Run it with `UI_SLOW=3`; expect ALL PASS. Commit `feat(portal): viewer page and end-to-end UI test`.

## Phase 7: Reports and Helm (§10, §11, §12.7)

### Task 14: Reports

**Files:**
- Modify: `web/portal_admin.py` (routes), `web/templates/index.html` (Reports tab for admins and the approver team)
- Test: `scratch/test_portal_releases.py` (reports section)

**Interfaces:**
- Produces: `GET /api/portal/reports/audiences` (per audience: datasets it currently receives, since when, approved by,
  GDPR reference), `GET /api/portal/reports/datasets/{id}` (full history with profile diffs), `GET
  /api/portal/reports/downloads?from=&to=&audience=`; each with `?format=csv` (formula-escaped like downloads).

- [ ] **Step 1: Write the failing tests:** after the happy path and one drift + reapproval, the audience report shows the
  dataset with the second approver and GDPR ref; the dataset report lists both approvals with the diff between them; the
  downloads report counts the exports of Task 12's fixture; a plain `user` gets 403, an approver 200; the CSV export of a
  report whose dataset name starts with `=` is escaped.
- [ ] **Step 2:** Run; expect FAIL. **Step 3:** Implement from `portal_audit` and `portal_datasets` only.
  **Step 4:** Run; expect PASS. **Step 5:** Commit `feat(portal): audit reports for audiences, datasets and downloads`.

### Task 15: Helm

**Files:**
- Create: `deploy/helm/datakilnworks/templates/portal.yaml` (Deployment + Service)
- Modify: `values.yaml` (`portal.enabled: false`, `portal.publicUrl`, `portal.rpId`, resources), `ingress.yaml` (a portal
  host when enabled), `networkpolicy.yaml` (portal pod: ingress from the ingress controller only, egress to nothing but
  DNS), `.github/workflows/ci.yml` (one more `helm template` + kubeconform rendering with `portal.enabled=true`)

- [ ] **Step 1:** The portal pod mounts the warehouse PVC (rw, see "Calls made") and the `keys` PVC, with the same
  podAffinity as the compute nodes (RWO), command as in compose, no service account token.
- [ ] **Step 2:** Verify: `docker run --rm -v $PWD/deploy/helm:/h alpine/helm lint /h/datakilnworks --set portal.enabled=true`
  and `helm template` piped to kubeconform; expect no errors in both the default and the enabled rendering.
- [ ] **Step 3:** CLAUDE.md (Helm bullet), README; commit `feat(helm): optional portal deployment`.

---

## Notes for the implementer

- A value that a mask changed at build time is masked again on read when the propagated tag has a policy that applies to
  the external user (e.g. a hash of a hash). It is consistent across widgets and joins; the test in Task 12 compares the
  portal's answer with `govern_sql` for the external user on the copy, not with the copy's raw bytes.
- `web/app.py` is ~7,300 lines and `index.html` ~27k: grep, do not read whole files.
- `scratch/` is not bind-mounted into a running studio: `ci/run_test.sh` with `CI_MOUNT_SOURCES=1` mounts it.
