# DataKilnWorks vs Databricks/Snowflake: Honest Gap Assessment

`FEATURE_COMPARISON.md` is a marketing-style scorecard. This document is its counterweight: what DataKilnWorks (DKW) still
does **not** do, what cannot be done because of how it is built, and what could still be built. It was first written when
several rows of the scorecard described features that did not exist (MFA, OIDC login, Git, shallow clone, real auto-suspend,
IP allowlists, RLS, LDAP). All of those have since been built and tested; this revision (September 2026) replaces the
original text, records what closed, and lists what is genuinely left.

Every statement below was checked against the code and the test scripts in `scratch/`, not against the marketing rows. Where
something is implemented but only partly verified, that is said in section 1b.

---

## 0. What the first assessment flagged, and where it stands now

| Originally flagged as missing or fake | Now | Evidence |
| --- | --- | --- |
| OIDC / OAuth login (config screen only) | **Built**: OIDC (auth code + PKCE), SAML 2.0 SSO, SCIM 2.0 provisioning, OAuth 2.0 client credentials for REST connections | `oidc_auth.py`, `saml_auth.py`, `scim.py`, `oauth_client.py`; tests against a mock IdP, a real Keycloak (SAML) and mock authorization servers |
| MFA (fictional) | **Built**: TOTP + backup codes, org-wide MFA policy with grace period and stats, **WebAuthn passkeys / security keys** (second factor, passwordless sign-in, autofill sign-in, attestation policy, passkey-only accounts) | `mfa.py`, `mfa_policy.py`, `webauthn_auth.py`; a software authenticator and Chromium's virtual authenticator |
| Git integration (none) | **Built**: dbt project and shared notebooks, pull-request mode (Gitea, GitHub, GitLab APIs), diff view, conflict resolution | `git_sync.py`, `git_review.py`; real Gitea in tests |
| Zero-copy clone (none) | **Built**: shallow clone with hard-linked data files, governance-aware | `table_clone.py` |
| Auto-suspend was cosmetic | **Built**: idle warehouses really stop or pause their compute-node container, resume on the next query, per-warehouse warm start | `warehouse_lifecycle.py`, `container_control.py`, controller service; real containers in tests |
| Row-level security, LDAP | **Built** (already struck through in the original) | `governance/row_filters.py`, `ldap_auth.py` |
| IP allowlists (nothing) | **Built**: global allowlist with trusted proxies, per-recipient rules for Delta Sharing | `ip_allowlist.py` |
| File-watch ingestion | **Built**: inotify triggering, S3 bucket-event webhooks, S3/Azure Blob/GCS/HTTP/SFTP/REST sources, previews | `autoloader_watch.py`, `s3_events.py`, `autoloader_s3.py`, `autoloader_azure.py`, `autoloader_gcs.py`, `autoloader_conn.py` |
| Streaming ingestion (polling only) | **Partly built**: Kafka / Redpanda streams with exactly-once micro-batches, Avro / Protobuf / JSON Schema registries, rewind and move | `streaming.py`, `stream_ops.py`; real Redpanda in tests |
| No cross-organisation sharing | **Partly built**: a Delta Sharing server (shares, recipients, signed links, change data feed, hints, governance-gated). No marketplace | `delta_sharing.py`; the real `delta-sharing` client in tests |
| HA was "documentation only" | **Partly built**: Helm chart, init container, health probes, optional Traefik TLS proxy. Not exercised on a cluster, see 1b | `deploy/helm/`, `web/init.py`, `deploy/traefik/` |
| Workflows had no orchestration | **Built**: retries, timeouts, run conditions, parameters, event triggers, cancel, repair, notifications, a drag-to-edit task graph, live run page, workflows table | `workflow.py`, `web/static/dag.js` |

The original headline ("94/98, 96%") should not be repeated: it was computed before these rows were corrected, and it has
not been recomputed since (see 1b, last row).

---

## 1. Where DataKilnWorks is still not a winner or a tie

### 1a. Real, stated losses

| Feature | Why DKW loses |
| --- | --- |
| Petabyte-scale distributed execution | DuckDB is a single-node engine and Ray parallelises on one machine or a small cluster. See section 2. |
| Marketplace and data monetisation | Delta Sharing covers sharing with named recipients. There is no listing, discovery, request workflow or billing service. |
| Spark-class continuous processing | Kafka streams are exactly-once micro-batches into Delta tables. There is no Structured Streaming, no streaming SQL / materialised streaming tables, no windowed aggregation over unbounded streams. |
| Cloud event queues as Auto-Loader triggers | Bucket events arrive through webhooks (MinIO, Garage, anything that can POST S3 event JSON). SQS, SNS, EventBridge, Event Grid and Pub/Sub are not consumed; only S3 has an instant-webhook trigger, Azure Blob and GCS sources are polled or cron only. |
| Parallel execution of workflow tasks | The tasks of one run execute one after another (deliberately deferred). |
| Compliance certifications and a managed SLA | Not attainable for self-hosted software, see section 2. |

### 1b. Implemented, but with a caveat a technical reader should know

| Area | Caveat |
| --- | --- |
| High availability | The Helm chart passes `helm lint`, `helm template` and kubeconform, and a GitHub Actions job installs it on a kind cluster and passed there (sign-in, SQL on the compute node, pod restart with data kept, upgrade with the compute token kept, a broken-config install failing in the init container, uninstall keeping the volumes). That is one replica on a single-node cluster: the studio is one replica (SQLite metadata and a ReadWriteOnce volume), and there is no autoscaler, no multi-node scheduling test and no multi-zone story. Deployable and smoke-tested, not a tested HA design. |
| TLS with Let's Encrypt (Traefik profile) | Configured, but not exercised: it needs a public host name. Self-signed and bring-your-own certificates are tested. |
| GitHub and GitLab pull requests | Verified against in-process mock servers of their APIs; only Gitea was run for real. |
| Delta Sharing | Works with the Python client in the Parquet format (snapshots, time travel, change feed), for both local tables and tables in an S3 mount (pre-signed URLs straight to the mount's object store; verified end to end against a real throwaway Garage server). The client's Rust reader (Delta response format) only recognizes S3/Azure/GCS files by hostname, so a non-cloud-storage host is misread as a local path; that format is built to the specification but unverified end to end. Tables with deletion vectors or column mapping cannot be shared. The change data feed is local-tables-only (it reads the Delta log off local disk); it is refused for a table in an S3 mount. History, time travel and the change feed are opt-in per table because they can expose deleted rows. |
| Passkeys | Attestation is verified only against trust roots you paste in: no bundled vendor roots (a FIDO Metadata Service cache adds model names, certification status and revocation, but that is a separate check from the trust-root one and does not bundle vendor roots either). Passkey-only accounts work for local and LDAP accounts, never OIDC/SAML. The browser autofill dropdown could not be driven by automation; the request and the sign-in behind it are tested. |
| SAML | Solicited SP-initiated sign-in with strict validation, signed AuthnRequests, encrypted assertions and Single Logout (SP- and IdP-initiated, both directions signed) all built and tested. A real encrypt+decrypt round trip is not covered end to end: building a valid `<xenc:EncryptedData>` fixture via the low-level xmlsec Python bindings proved impractical in the time available, so only the rejection-of-unencrypted path is tested, and the decrypt path is verified by reading python3-saml's own implementation instead. |
| SQL `GRANT` / `REVOKE` | Catalog, schema, table and column-level grants, `WITH GRANT OPTION` / `GRANT OPTION FOR` on tables and schemas. Column-level grants only support SELECT (no column-level MODIFY) and never carry a grant option; `WITH GRANT OPTION` never applies to a catalog (an admin or the catalog's power-user owner still manages that directly) and only delegates GRANT, never REVOKE. Role principals and grants on all tables of a catalog are still refused with a clear message. |
| Network policy | One global allowlist plus per-recipient rules for Delta Sharing, and now per-user / per-role network policies on top (a specific account, or every account of a role, restricted to chosen address ranges, independent of the global allowlist's own mode). Only ever applies to an authenticated session: the login page itself, SSO callbacks and `/docs` have no identity yet to check it against. |
| Governance on shared data | A table with a masking policy or row filter cannot be shared through Delta Sharing (recipients receive raw files). The fix is to share a de-identified copy. |
| Azure Blob and GCS Auto-Loader sources | GCS goes through its S3-compatible interoperability API (HMAC keys), not the native Google Cloud SDK or OAuth service accounts; real GCS was not available to test against, so `scratch/test_autoloader_gcs.py` runs against a throwaway Garage container standing in for GCS's endpoint instead. Azure Blob was tested against a throwaway Azurite emulator, not a real Azure account. Neither is a Delta write target (source only, like every non-S3 mount); neither has an instant bucket-notification trigger (S3 events has no Azure/GCS equivalent here); ADLS Gen2-specific features (hierarchical namespace ACLs) are not used. |
| `FEATURE_COMPARISON.md` scorecard | Its per-domain scores and totals were last recomputed before most of the work above, so the totals are stale in both directions. Treat the individual rows, not the sum, as the reference until it is recomputed. |

---

## 2. Unattainable because of architecture or process

These conflict with how DKW is built (a single-process engine, self-hosted software with no vendor operating it), so
building them would mean changing the architecture, not adding code:

- **Petabyte-scale, thousand-node query execution.** Reaching it means replacing the query engine, not extending it.
- **A managed marketplace or live cross-cloud sharing network.** It needs a hosted multi-tenant service with other
  companies on it. What a self-hosted product can offer, and DKW now does, is the open protocol between installations.
- **Vendor compliance certifications** (SOC 2, HIPAA, FedRAMP, PCI-DSS). They certify a vendor's operating practices. A
  customer can certify their own deployment; DKW as a project cannot be "SOC 2 compliant" the way a SaaS vendor is.
- **A zero-ops SLA** (patching, guaranteed uptime, 24/7 support). It requires someone operating the service for you, which
  is the opposite of self-hosting.
- **Cloud-metered cost benefits of suspend / resume.** Suspending is real now (containers stop or pause), but on your own
  hardware it frees memory and CPU, not a bill.

Removed from this list since the first version: true event-driven ingestion is no longer "unattainable". File watching,
bucket-event webhooks and Kafka streams give event-driven ingestion within a single-box design. What remains out of reach is
the cluster-wide, cloud-provider-integrated variant.

---

## 3. Gaps that could be closed within the current architecture

Ordered by how much they would change the honest picture. None needs a new engine.

1. ~~**Prove the deployment claims.**~~ *(Done: `.github/workflows/ci.yml`, `ci/`.)* CI runs 38 test scripts in the built image, 7 browser tests, the static checks and a kind smoke test of the chart. Still to do: the integration tier (tests that need Gitea, Redpanda, MinIO, lldap, Keycloak) as a scheduled job.
2. **Parallel workflow tasks.** Run independent branches of a DAG concurrently (bounded by a per-workflow limit). The graph
   and run page already show branches; the engine is the missing part.
3. ~~**More Auto-Loader sources.**~~ *(Done: `web/autoloader_azure.py`, `web/autoloader_gcs.py`.)* Azure Blob (`azure://`) and GCS
   (`gcs://`, through its S3-compatible interoperability API) as file-arrival sources, mirroring the S3 path: listing, exactly-once
   checkpoints, quarantine, preview, credentials from a storage mount. Both are sources only, not targets (see 1b). Still open:
   SQS / Event Grid consumers (this was always the optional half of the item).
4. ~~**SAML completeness.**~~ *(Done: signed AuthnRequests, encrypted assertions, Single Logout. See 1b for the one caveat.)*
5. ~~**Column-level grants** and `WITH GRANT OPTION`~~ *(Done: `web/column_grants.py` built on the existing masking machinery; `WITH GRANT OPTION` / `GRANT OPTION FOR` on table and schema grants. See 1b for the scope kept deliberately narrow.)*
6. ~~**Delta Sharing reach.**~~ *(Done: tables in an S3 mount can now be shared, handing out a pre-signed URL straight to the
   mount's own object store instead of proxying through this server, verified end to end against a real throwaway Garage
   server with the standard `delta-sharing` client; the change data feed is refused for these tables since it reads the
   Delta log off local disk. Still open: tables with deletion vectors or column mapping through the Delta format, and a
   verified end-to-end run of the Delta response format against a client that can fetch our URLs — the Python client's
   Rust kernel only recognizes S3/Azure/GCS files by hostname, so this needs real cloud storage or a Spark connector,
   neither available here.)*
7. ~~**Passkey depth.**~~ *(Done: `web/fido_mds.py` caches the FIDO Alliance's own registry by AAGUID (model name, certification
   status, revocation -- a real, from-scratch JWS + X.509 chain verification against an administrator-pasted trust root, no
   vendor roots bundled); a revoked model is refused at registration unconditionally. Passkey-only accounts now work for LDAP
   accounts too (`set_passwordless`/the enrolment endpoints), not just local ones. Per-role authenticator requirements
   (`webauthn_policy.hardware_roles`, e.g. hardware keys for administrators) force attestation for the chosen roles even while
   the organisation-wide mode stays lenient. See 1b for the one caveat kept from before.)*
8. ~~**Per-user / per-role network policies**~~ *(Done: `web/ip_allowlist.py`'s `network_policies` table restricts a specific user, or
   every user of a role, to chosen address ranges, independently of the global allowlist's own mode; a user policy takes precedence
   over that user's role policy. Can only ever apply once a session exists (there is no identity yet for the login page, SSO
   callbacks or `/docs`), and always allow-lists `/api/auth/me`/`/api/auth/logout` so a blocked account can still see why and sign
   out. See 1b.)*
9. **A multi-replica studio.** The metadata lives in SQLite files; moving the shared pieces (sessions, challenges, run
   state) to a shared store would allow replicas. This is the one item here that is a real re-design, not a feature.
10. **Recompute `FEATURE_COMPARISON.md`.** Refresh the scores from the current code, and add rows for the newer features
    (Delta Sharing, passkeys, streaming, the workflow UI) so the scorecard and this document agree.

---

## Recommendation

`FEATURE_COMPARISON.md` no longer contains the fabricated rows this document originally warned about, but its totals are
stale and it does not yet reflect the caveats in 1b. Items 1, 3, 4, 5, 6, 7 and 8 are now done; item 10 (recompute the scorecard) would
make both documents defensible for an external reader. Tell me which items to build next; I would start with parallel
workflow tasks or the scorecard recompute.
