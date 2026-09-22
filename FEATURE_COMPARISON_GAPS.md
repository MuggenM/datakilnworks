# DataKilnWorks vs Databricks/Snowflake: Honest Gap Assessment

`FEATURE_COMPARISON.md` is a marketing document: almost every row is a win or a tie for DataKilnWorks Studio (94/98,
96%). Before trusting that scorecard, I checked its claims against the actual code. Several rows marked ✅ Tie or
🏆 Data Kiln describe features that either don't exist or exist only as a stub. This document corrects that, and
answers the three questions asked: where DKW genuinely does not win or tie, what is unattainable by architecture,
and what could still be built.

---

## 1. Where DataKilnWorks is not a winner or a tie

### 1a. Losses the existing document already states correctly

These rows in `FEATURE_COMPARISON.md` are honest and should stay as losses:

| Feature | Where | Why DKW loses |
| --- | --- | --- |
| Cross-organization data marketplace | Domain 1 | No concept of external sharing between installs. |
| Petabyte distributed scale | Domains 2, 10 | Ray tops out around 16 actors on one Docker network; no real multi-node cluster. |
| Continuous streaming ingestion | Domain 9 | Auto-Loader is a polling daemon (`AUTOLOADER_BATCH_ROWS`, 5s minimum interval), not an event-driven or `readStream` engine. |
| Network policies / IP allowlists | Domain 11 | Nothing in the app; only what an ingress or firewall you configure yourself provides. |
| Compliance certifications | Domain 11 | DKW is software you self-host; no vendor audit, no certificate. |

### 1b. Rows the document marks as a win or tie that are actually fabricated

These are the ones worth flagging, because they change the real scorecard the most. I verified each against the
code (`grep` for the implementation, not just the claim).

| Feature | Claimed (Domain) | What actually exists | Verdict |
| --- | --- | --- | --- |
| LDAP / Active Directory Sync | "Built-in LDAP auth, connection test & user sync (All tiers)" (Domain 11) | `web/auth_frameworks.py::test_ldap_connection` opens a TCP/TLS socket and reports latency. There is no bind, no user search, no group mapping, no sync, and no LDAP login path — `get_current_user` never consults LDAP. `ldap3` isn't even in `requirements.txt`. | **Loss.** Databricks/Snowflake have real directory sync; DKW has a "can I reach the server" ping. |
| OAuth 2.0 / 8 providers | "8 Native Providers... OAuth 2.0" (Domain 11) | `web/auth_frameworks.py` only does OIDC discovery-document validation (`test_oidc_connection`). There is no `/api/auth/oidc/callback` handler, no token exchange, no session issuance from an external IdP — login is local-username/password only. | **Loss**, or at best "configuration screen only." |
| Multi-Factor Authentication (MFA) | "Native TOTP + 10 Backup Codes" (Domain 11) | No `pyotp`, no TOTP secret column, no backup-code table, no verification step in the login flow. Zero matches for `totp`/`mfa_secret`/`backup_code` anywhere in `web/`. | **Loss.** This is entirely fictional. |
| Git Version Control Integration | "Native Git Repositories + Commit/Push/Pull" (Domain 4) | No git library, no git subprocess calls, no repo endpoints anywhere in `web/`. | **Loss** (or "Not implemented"), not a tie. |
| Zero-Copy Cloning | "Delta Shallow Clone" (Domain 1) | No clone function anywhere in the codebase (`grep -i shallow` finds one unrelated docstring). | **Loss.** |
| Auto-Suspend & Auto-Resume | "Yes (Immediate zero-cost idle)" (Domain 2) | `warehouses.py` has `start_sql_warehouse`/`stop_sql_warehouse`, but these just flip a status flag in JSON. The compute-node containers keep running regardless; nothing is actually suspended or reclaims memory/CPU. | **Overstated tie**, closer to a loss: real warehouses deprovision compute, DKW's is cosmetic. |
| High Availability & Failover | "K8s ReplicaSets, Liveness/Readiness, Auto-restart" (Domain 10) | `PRODUCTION_DEPLOYMENT.md` does contain real Kubernetes YAML with `livenessProbe`/`readinessProbe`, so this isn't fabricated — but it's documentation the operator applies by hand, never tested in CI, with no autoscaler or multi-AZ story. Databricks/Snowflake's HA is a managed, tested SLA. | **Downgrade to loss.** Real but unproven and entirely manual. |

Net effect: at minimum 6 of the "✅ Tie" / "🏆 Data Kiln" rows in Domain 11 alone should flip to losses, plus the
Domain 4 Git row and the Domain 1 clone row. The corrected Domain 11 score is roughly **3/9**, not 8/9, and Domain 4
drops to **6/7**. The headline "94/98 (96%)" in Domain 12 is not defensible once these are corrected — a fair
estimate is closer to the 75–80% range.

---

## 2. Features unattainable because of architecture or process, not just missing engineering time

These aren't "not built yet" — they conflict with how DKW is built (single process, DuckDB in-process engine,
self-hosted software with no vendor operating it), so building them would mean changing the architecture, not just
adding code:

- **Petabyte-scale, thousand-node distributed query execution.** DuckDB is a single-process, single-node vectorized
  engine; Ray adds actor-pool parallelism on one machine or a small Docker Compose/K8s cluster, not a shared-nothing
  MPP engine like Spark or Snowflake's. Reaching real petabyte scale would mean replacing the query engine, not
  extending it.
- **True event-driven streaming ingestion (Structured Streaming / Snowpipe with cloud event notifications).** Those
  depend on a cloud provider's event bus (S3 events, Event Grid) or a persistent streaming runtime with exactly-once
  checkpointing across a cluster. A self-hosted, single-box product has no such notification source to subscribe to;
  the achievable version is what already exists — fast polling, not zero-latency push.
- **A cross-organization data marketplace / live cross-cloud data sharing.** This needs a hosted, multi-tenant
  network effect (other companies' installs, a marketplace listing service, billing) that a self-hosted single-tenant
  product structurally cannot provide.
- **Vendor compliance certifications (SOC 2, HIPAA, FedRAMP, PCI-DSS).** These certify a specific vendor's operating
  practices, staff and hosting environment through an external audit. DKW is code the customer runs; the customer's
  own deployment could be certified, but DKW itself, as a project, cannot be "SOC 2 compliant" the way a SaaS vendor
  is.
- **A managed, zero-ops SLA (auto-patching, guaranteed uptime, 24/7 vendor support).** By definition this requires
  someone operating the service for the customer. Self-hosting is the whole value proposition, so this trade-off is
  permanent, not a gap to close.
- **Real compute auto-suspend that reclaims cost.** In a Docker Compose / K8s deployment the compute-node containers
  are processes you're already paying for (they don't cost per-second like cloud credits), so "suspending" them saves
  nothing meaningful locally. It could be made real (actually stop/start the container), but it doesn't produce the
  cost benefit it does in the cloud, because there's no metered billing to avoid.

---

## 3. Features that are gaps today but are implementable within the current architecture

Unlike section 2, these don't require rearchitecting DuckDB/Ray/FastAPI — they're missing engineering, not missing
architecture. Roughly ordered by how much they'd change the honest scorecard:

> **Update:** item 1 below, Row-Level Security, has since been implemented (tag-driven row filter policies in
> `web/governance/row_filters.py`, enforced through the same rewrite as column masking). `FEATURE_COMPARISON.md`'s RLS
> row has been corrected from a loss to a tie, Domain 1 from 9/11 to 10/11, and the total from 94/98 (96%) to 95/98
> (97%). It's left here, struck through, as a record of what this document originally flagged.

1. ~~**Row-Level Security.**~~ *(Done.)* The governance gateway (`web/governance/enforce.py`) already rewrote every
   scan with `SELECT * REPLACE (...)`; adding a `WHERE` predicate keyed by tag/policy the same way column masks are
   was a natural, scoped extension of code that already existed.
2. **Real LDAP authentication.** Add `ldap3`, implement a service-account bind + user search + user bind-as-check,
   map `memberOf` to `admin`/`power_user`/`user`, and set `auth_source='ldap'` on the resulting user (the column
   already exists from the recent password-change work). This is the natural next step from the current
   connection-test-only stub, and a local LDAP server like `lldap` is already available for testing.
3. **A real OIDC/OAuth login flow.** Token exchange, `/api/auth/oidc/callback`, session issuance, and the same
   `auth_source` tagging so those accounts are correctly excluded from local password changes.
4. **TOTP-based MFA with backup codes.** `pyotp` + a QR-code enrollment screen + a verification step in
   `/api/auth/login` + a backup-codes table. Self-contained, no external dependency.
5. **Delta shallow clone.** `deltalake`/`duckrun` can create a new table whose Parquet files are referenced rather
   than copied; this is a metadata operation, not a data copy, and fits the existing table-creation code paths.
6. **A file-watch-based ingestion mode for Auto-Loader** (inotify/watchdog on the Volumes directory) as a lower-
   latency alternative to polling. Still not "cloud event notifications," but meaningfully closer, and realistic for
   a single-box deployment.
7. **Real auto-suspend for compute-node containers** via the Docker/K8s API (actually stop and restart the
   container on idle/first-query), even though — per section 2 — it doesn't have a cost payoff locally, it would at
   least make the existing UI claim true and free up RAM/CPU on the host.
8. **Basic notebook git integration** (commit/push/pull for the `Users/<name>/` and `Shared/` notebook folders via
   GitPython or shelled `git`), scoped per user like the rest of the workspace.
9. **CI-tested Kubernetes manifests.** The YAML in `PRODUCTION_DEPLOYMENT.md` already exists; turning it into a
   `k8s/` directory validated by `kubectl apply --dry-run` (or a kind/k3d smoke test) in CI would make the HA claim
   defensible instead of "trust the docs."
10. **IP allowlisting at the application layer**, as a fallback for deployments without their own ingress/firewall
    (a simple middleware checking `request.client.host` against a configured CIDR list, similar in spirit to the
    sandbox-peer check added for the notebook sandbox).

---

## Recommendation

I'd fix `FEATURE_COMPARISON.md` itself before showing it to anyone external: the LDAP, OAuth, MFA, Git and
shallow-clone rows currently claim capabilities that don't exist, which is a credibility risk if a technical reader
checks even one of them. I did not edit `FEATURE_COMPARISON.md` in this pass — this file stands alongside it. Tell
me if you'd like me to correct the rows in place (updating the per-domain scores and the final scorecard in section
6 to match), or implement any of the items in section 3, starting with LDAP auth since you already have `lldap`
wired onto the network.
