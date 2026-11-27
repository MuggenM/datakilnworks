# Design: supplier portal (outside viewers of released, materialized datasets)

Status: design, approved approach B (2026-11-28). Not built. Related: `proposals/declarative-apps.md` (the app engine),
`proposals/app-write-back.md` (a later step: suppliers sending data in).

Decisions made with the product owner are written plainly. Calls made by the designer are marked *(designer's call)* and
can be corrected.

## 1. Intent

External data deliverers (suppliers) need a secure place to look at, and download, data prepared for them, without ever
seeing the rest of the studio. Several people work for one supplier and all of them see the same data. Making data
available to an outside organisation is a GDPR-relevant act, so it has to be deliberate, approved by a second person,
and reportable afterwards.

Later steps, not in this design: suppliers uploading files and entering rows (write-back), self-service refresh, power
users preparing releases, the supplier's own SSO.

## 2. Goals, non-goals, anti-goals

Goals
- Outside users sign in to a separate portal and see only the apps and datasets released to their organisation.
- What they see is a materialized copy that contains only what their organisation may see, built through the existing
  governance (grants, row filters, masks).
- Every release is prepared by an admin and approved by a different person from an approver team, against a frozen data
  profile and a GDPR reference.
- A reportable, append-only audit trail: who made which data available to which organisation, with which profile, on
  whose authority, and who looked at or downloaded it.

Non-goals (v1)
- Uploads and write-back. Live queries against source tables. Self-service refresh. SSO for suppliers. Power users as
  preparers. One approver team per organisation (one team for all, room left for more).

Anti-goals (a failure even if it works)
- Any studio route reachable from the internet through the portal.
- Revoked data still readable: via time travel, an old copy, a cached result, or an account that should have expired.
- Personal data leaving through a notification (email, Slack, Teams are third-party services).
- One person able to both prepare and approve a release.
- A dataset whose shape changed (new column, new `pii` tag, a mask gone) still published under an approval that was
  given for a different profile.

## 3. Approaches considered

- **A. Inside the studio process**, an `external` role confined by a route gate, plus a small viewer page. Least work,
  one deployment; but the studio (600+ routes) must face the internet and one gate mistake exposes all of it.
- **B. A separate portal process from the same code (chosen).** A second ASGI app that mounts only the portal routes,
  its own compose service, Traefik route and hostname. Accounts, sessions and the databases are shared with the studio.
  The studio refuses `external` sessions and can stay internal. What faces the internet contains only portal routes.
  Cost: a second service (compose, Helm), the security middleware applied in both apps, passkeys bound to the portal's
  hostname.
- C. The studio page in a "portal mode": dropped; outsiders would load the whole studio and hidden screens do not stop
  route calls.

## 4. Architecture

```
internet ──TLS──> Traefik ──> datakilnworks-portal  (web.portal:app, portal routes only)
                                   │ reads: auth.db (accounts, sessions), portal catalog (Delta copies, read-only)
internal  ──────> Traefik ──> datakilnworks-studio  (web.app:app, refuses role=external)
                                   │ writes: releases, copies (materialize jobs), audit, notifications
```

- `web/portal.py` (new): `portal = FastAPI(...)`, mounting only the routes in section 9. Started as its own uvicorn
  process (`uvicorn web.portal:portal --no-proxy-headers`), own compose service `datakilnworks-portal`, own Helm
  Deployment. It reuses the security middlewares of `app.py` (`_security_headers` with CSP, `_csrf_origin_gate`,
  `_ip_allowlist_gate`, `_network_policy_gate`, the login throttle, `_mfa_policy_gate` where relevant) by factoring them
  into a shared module both apps register *(designer's call: factor, do not copy)*.
- The portal never runs materialization, never writes Delta, and opens DuckDB read-only. All writes (copies, releases,
  approvals) happen in the studio process.
- Shared state is the existing SQLite files (`auth.db` for accounts/sessions, a new `portal.db` for releases). Two
  processes write `auth.db` (sessions, `last_seen_at`): SQLite locking handles it; writes stay short. The login throttle
  is in memory per process (acceptable: each process throttles its own sign-in endpoint).
- WebAuthn: the portal sets its own RP id / origin (`WEBAUTHN_RP_ID`, `WEBAUTHN_ORIGINS` for the portal service); a
  supplier's passkey belongs to the portal hostname.
- The studio refuses every session whose user has role `external` (in `auth.session_user` callers via the gates, 401),
  and the portal refuses every non-`external` session. Defence in depth: each process only ever serves its own kind of
  account.

## 5. Identities

| Who | Account | Where | Confinement |
| --- | --- | --- | --- |
| Supplier user | role `external`, passkey-only (enrolment link), member of exactly one audience group | portal | portal routes only; studio refuses the session |
| Approver | internal account, role `release_approver` *(designer's call: a role, not a flag, so the existing role checks keep refusing it everywhere)*, member of the approver team group | studio | a route allow-list gate in the studio (review page, release history, reports, sign-in/session/MFA routes) |
| Preparer | admin | studio | normal admin |

- **Audience** = an IAM group marked `audience` (new column on `user_groups`), one per supplier organisation, e.g.
  `supplier_acme`. An `external` user belongs to exactly one audience; adding them to a second is refused
  *(designer's call: one org per person keeps "what does this person see" unambiguous)*.
- **Onboarding**: existing passkey-only accounts and enrolment links (`POST /api/users/passkey-only`), extended to create
  role `external` with an audience. Recovery = a new link. No password ever exists.
- **Approver team** = an IAM group marked `approver_team`. One team in v1; the data model allows a team per audience or
  data domain later. A member of the team may be an admin too; the four-eyes rule (section 7) still applies.
- **Lifecycle** (agreed): idle timeout 30 minutes for `external` sessions (`SESSION_IDLE_MINUTES` per role); an
  `external` account expires 12 months after creation or its last renewal unless an admin renews it (expiry = sessions
  revoked and sign-in refused, account kept for the audit trail). Optional network policy per audience role/user
  (existing per-user / per-role network policies).

## 6. Datasets and copies

- A **dataset** (table `portal_datasets` in `portal.db`): `id`, `name`, `audience_group_id`, `source_sql` (one read-only
  SELECT over source tables, validated with sqlglot like dashboard specs), `columns` (explicit list; `SELECT *` refused,
  data minimisation), `row_cap`, `schedule` (cron, default daily), `downloadable` (bool), `state` (section 7).
- **Location**: catalog `portal`, schema per audience (`portal.<audience>`), table per dataset. The audience group gets a
  schema-level SELECT grant on its own schema only (existing `resource_grants` type `schema`). No other grant may name
  `portal.*` *(enforced on grant)*.
- **Built as the audience**: a new workflow task type `materialize` runs `source_sql` through `gateway.govern_sql` with a
  synthetic principal for the audience (`role='external'`, `groups=[audience id]`, no user exemptions), so grants, row
  filters (incl. `attribute` mode with group values) and masks decide the content. It writes the result as a new Delta
  table version (overwrite), refusing if `row_cap` is exceeded. It is never exempt and never runs as the job owner.
  `gateway.propagate_tags` copies the source tags onto the copy (classification stays visible in the catalog and in the
  profile).
- **Schedule** (agreed): the jobs engine's cron scheduler, default once a day; an admin changes the interval per dataset
  on an organisation's request after evaluation. No self-service refresh in v1.
- **Rule changes win over the schedule** (agreed): each copy records the governance version it was built under
  (`policies` version counter) plus the grant/membership state of its audience. A copy built under an older version is
  **blocked** (portal answers "being updated") and a rebuild is queued at once. A rebuild whose new profile differs from
  the approved one goes to `needs_reapproval` instead of publishing (section 7).
- **Revocation is physical** (agreed): no time travel for the `portal` catalog (portal routes never accept a version);
  after any rebuild that removes rows or columns, `VACUUM` with zero retention on that copy (the copies are never read
  concurrently by long-running writers, so the usual retention safety does not apply). Withdrawing a release, removing
  an audience, or a user's expiry/deactivation: the copy is dropped (directory deleted after the Delta table is removed)
  or the user's sessions revoked, immediately.
- **Caches**: portal query results are keyed by `gateway.fingerprint` and the copy's Delta version, so a rebuilt copy
  never serves an old cached result.

## 7. Releases, profile and four-eyes approval

States of a dataset release:

```
draft ──submit──> pending_review ──approve──> approved ──first successful build──> published
  ^                    │ reject (reason)                                          │ profile drift on rebuild
  └──────── edit ──────┘                                                          v
                                                                          needs_reapproval ──approve──> published
published / approved / needs_reapproval ──withdraw (reason)──> withdrawn (copy dropped)
```

- **Data profile** (frozen at submit, stored as JSON with a hash): columns and types, governance tags per column, which
  masks and row filters applied to the build, row count, `downloadable`, the audience, the schedule, `source_sql` hash.
  It is computed from a dry build as the audience (same code path as `materialize`, result discarded).
- **Approval** (agreed: in the studio, four-eyes): any member of the approver team **except the preparer** may approve
  or reject. The server enforces it (`approver != submitted_by`, membership checked at decision time). The approver
  enters the GDPR reference (required, free text, max 200 chars) and, for a rejection, a reason. The first decision wins
  (a conditional update on `state = 'pending_review'`; a second concurrent decision gets 409).
- **What an approval covers**: exactly the stored profile hash. Any edit of the definition returns the release to
  `draft`. A rebuild whose profile hash differs (a new column, a tag added, a mask gone, a column type change)
  publishes nothing new: the previous copy stays blocked and the release goes to `needs_reapproval` with a diff of
  the two profiles. Row count changes do not count as drift *(designer's call; a large relative change could be added
  as a warning)*.
- **Withdrawal**: by an admin or an approver team member, with a reason; immediate (section 6).

## 8. Notifications to the approver team

- One channel per approver team, configured by an admin: a shared mailbox (existing SMTP config, `web/email_reports.py`),
  a Slack incoming webhook (`web/slack_integration.py`), a Microsoft Teams Workflows webhook, or a generic webhook
  (`web/webhook_alerts.py`).
- **Teams**: the existing `teams` format is the legacy Office 365 connector MessageCard, which Microsoft is retiring; add
  an Adaptive Card payload for Teams Workflows webhooks. Verify the payload against Microsoft's current documentation
  and a real Teams channel before relying on it.
- **Content rule**: a signpost only: event, release name, audience name, preparer, and a link to the review page. No
  profile, no column names, no data, no GDPR reference. The link grants nothing by itself: the approver signs in to the
  studio (passkey or the internal SSO) and sees the profile there. No approve/reject action from a message.
- Events: submitted for review; reminder after N days pending (default 3, configurable); decision (to the preparer:
  approved / rejected with reason); needs reapproval (to the team); withdrawn (to the team and the preparer).
- Delivery outcomes (sent / failed + error class) go to the audit trail. The approval in the studio is the record;
  a lost message never changes a state.

## 9. The portal

Routes (the complete list; anything else is not mounted):
- Sign-in: passkey options/login, enrolment options/verify, `GET /portal/api/me`, logout. No password login exists on
  the portal.
- `GET /` (the viewer page), `/static/portal/*`.
- `GET /portal/api/apps` (apps released to the viewer's audience), `GET /portal/api/apps/{id}` (spec, without SQL).
- `POST /portal/api/apps/{id}/widgets/{wid}/query` (run that widget's saved query with bound parameters; the client
  never sends SQL), `GET .../export?format=csv|xlsx|parquet` (only when the dataset is `downloadable`).

Viewer page *(designer's call: no Alpine, no `unsafe-eval`)*: a small standalone page in plain JS, everything rendered
with `textContent`, Chart.js from `/static`, CSP `script-src 'self'` (no inline, no eval). Widget subset: KPI, table,
bar, line, text (the existing escaped text renderer), with pages and typed parameters (select/date/number/text). Each
widget shows the copy's "as of" time. Page shows the organisation name and the signed-in person.

Apps for the portal reuse the dashboard-app spec (`web/dashboard_apps.py`) with an extra flag `portal: true` and a
validation rule: every table a widget query references must be a dataset released to that app's audience. Widget SQL
names datasets as `ds.<dataset name>`; the server rewrites that to `portal.<audience>.<table>` with sqlglot before
governing *(designer's call: one app definition can then be released to several audiences)*. The app itself is part of
the release: an app is visible to an audience only while all its datasets are `published` for it.

Limits (missing today for dashboards as well, required here): rows per widget result (default 5,000; export 1,000,000),
query timeout (30 s, via `interrupt`), rate limit per user (60 queries/min, 20 exports/hour), request body cap.

Downloads (agreed): only `downloadable` datasets; CSV and Excel cells starting with `=`, `+`, `-`, `@`, tab or CR are
prefixed with `'` (formula injection); each download is audited.

## 10. Audit trail and reports

- Append-only table `portal_audit` in `portal.db` (no UPDATE/DELETE code path; admin cannot clear it), plus a mirror line
  in `governance_audit` for the existing audit views. Events: dataset created/edited, submitted, approved, rejected,
  needs_reapproval, withdrawn, build ok/failed/blocked, audience created/changed, member added/removed, account invited/
  enrolled/renewed/expired, notification sent/failed, app view, widget query (aggregated per session per widget),
  export (dataset, format, rows). Each row: time, actor (user id + name at the time), audience, dataset, release, profile
  hash, GDPR reference (approval events), outcome.
- Reports in the studio (admins and approver team): per audience, what it currently receives, since when, approved by
  whom under which GDPR reference; per dataset, its full history with profile diffs; downloads per audience and period.
  CSV export of each report.
- Retention: kept indefinitely by default *(designer's call; the GDPR process may set a period, then a scheduled purge
  of entries older than it, itself audited)*.

## 11. Deployment

- Compose: service `datakilnworks-portal` (same image), `depends_on` the init container, Traefik router for its own
  hostname under the `proxy` profile; the studio's Traefik router can be removed for an internal-only studio.
- Helm: a `portal` Deployment + Service + optional Ingress, sharing the warehouse PVC (read-only mount for the portal
  where the chart allows) and the keys PVC (sessions are signed with the shared JWT secret).
- Env: `PORTAL_PUBLIC_URL`, `WEBAUTHN_RP_ID` / `WEBAUTHN_ORIGINS` for the portal service, `PORTAL=off` to disable.

## 12. Phases

1. Shared security middleware module; `web/portal.py` skeleton with sign-in only; studio refuses `external`; the
   route-surface test (below). Compose service.
2. Audiences, `external` and `release_approver` roles, enrolment for externals, expiry and idle timeout, approver gate.
3. Datasets, `materialize` task, `portal` catalog, governance-version blocking, vacuum on revoke.
4. Release flow, profile and diff, four-eyes approval, audit trail.
5. Notifications (email, Slack, Teams Adaptive Card, webhook).
6. Portal viewer, app validation and `ds.` rewrite, limits, downloads.
7. Reports; Helm.

## 13. Testing

- `scratch/test_portal_surface.py`: walk the portal's OpenAPI and its mounted routes: exactly the list in section 9;
  every studio `/api` operation answers 404 on the portal; every studio route refuses an `external` session; every
  portal route refuses a non-`external` session; the approver role reaches only its allow-list in the studio.
- `scratch/test_portal_datasets.py`: a copy built as the audience equals the governed result for that audience (row
  filter by group attribute, masks); `SELECT *` refused; row cap; governance version bump blocks the copy and rebuilds;
  revoke removes rows physically (no older Delta version readable, files gone after vacuum); withdraw drops the copy.
- `scratch/test_portal_releases.py`: state machine; preparer cannot approve; non-member cannot approve; concurrent
  decisions give one 409; profile drift (new column, tag added, mask removed) goes to `needs_reapproval` and publishes
  nothing; audit rows for every transition; reports.
- `scratch/test_portal_notifications.py`: payloads contain only the allowed fields; failures are audited; Teams card
  shape.
- `scratch/verify_portal_ui.py` (Playwright, throwaway studio + portal): enrol a supplier with a virtual authenticator,
  see only the released app, parameters, as-of time, a download with formula escaping, a withdrawn release disappears;
  an approver approves in the studio; an XSS string in a title or cell renders as text.
- All new tests in `ci/plan.json`.

## 14. Left to the builder

Table and column names, the exact middleware factoring, how the approver gate shares code with the MFA/password gates,
the profile JSON layout, how reminders are scheduled (the jobs scheduler or the alerts loop), the portal page's styling
(follow the studio's look and the brand settings).

## 15. Later

Write-back and file uploads into per-audience inboxes (`proposals/app-write-back.md`); on-request refresh with a quota per
organisation; power users as preparers; an approver team per audience or data domain; the supplier's own SSO.
