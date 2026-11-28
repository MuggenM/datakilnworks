# CI Backend Services Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** CI starts the outside services the service-dependent tests need (LDAP, Gitea, Redpanda, Azurite, Garage, SFTP,
Keycloak, the Entra/Fabric emulators, Mailpit) and runs those tests on every push, exactly as they run locally.

**Architecture:** One compose file `ci/services/docker-compose.yml` with a profile per service and one-shot seed
services that write generated values to `ci/services/out/<profile>.env`. `ci/services/up.sh` / `down.sh` wrap it under
project `dkwci`; `ci/run_test.sh` gains `CI_SERVICES` to start profiles, join their network and pass their env files.
`ci/plan.json` entries name the profiles they need.

**Tech Stack:** Docker Compose v2 (profiles, `--wait`, health checks), POSIX sh seed scripts, GitHub Actions, the
existing standalone test scripts.

**Spec:** `proposals/ci-services.md` (sections referenced as §N).

## Global Constraints

- Never touch the product owner's containers, networks or volumes: `../backendservices`, `dev-lldap`, `dev-kafka`, the
  live studio and compute nodes. Compose project is always `dkwci` (`-p dkwci`); no `ports:` anywhere in the services file.
- Use Garage, not MinIO (MinIO stays only in `verify_s3_events_ui.py`).
- Images pinned by an exact tag (no `latest`); record each tag's choice in the compose file comment.
- Tracked files hold no personal paths and no real credentials; test credentials are fixed dummy values or generated at
  seed time into the gitignored `ci/services/out/`.
- Dummy endpoints in tests are IP addresses or the compose service names of this stack, never made-up hostnames.
- In CI a missing service is a failure: `REQUIRE_SERVICES=1` turns every "unreachable: skipping" path into `[FAIL]`.
- Insert `ci/plan.json` entries by hand (do not re-sort the file). Conventional Commits; push to `development` after
  each task; end messages with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; update CLAUDE.md (CI bullet
  and the bullet of each feature whose test moves into CI) in the same commit.
- Locally, run with `DOCKER_NETWORK` unset when `CI_SERVICES` is set (the test joins `dkwci_default`, which has
  internet through compose's bridge; verify that in Task 1, Step 4).

## Calls made in this plan (beyond the design's own)

- Service names are fixed and short (`lldap`, `gitea`, `redpanda`, `redpanda2`, `redpanda-sasl`, `azurite`, `garage`,
  `sftp`, `keycloak`, `entra`, `fabric`, `onelake-proxy`, `mailpit`). Each test reads its host from an env var whose
  default is the old throwaway name, so the docstring commands keep working.
- `up.sh` waits with `docker compose up -d --wait` (health checks on every long-running service) and then waits for the
  seed services to exit 0; a seed failure fails `up.sh` with that service's log.
- The two gitignored LDAP tests become tracked once they read every login from env vars (defaults = the seeded dummy
  values of this stack); `.gitignore` drops them.
- `test_ldap_tls.py` keeps starting its own lldap (it needs a throwaway CA and LDAPS); it gets a `"host": true` plan
  entry and a host job.
- `test_git_forges` keeps its in-process GitHub/GitLab mocks; only its Gitea part uses the stack.

## Review Focus

1. A seed that half-ran on a previous local run (volume left over): `up.sh` must start from empty volumes every time
   (`down -v` first), or a second local run sees stale users/tokens (Task 1 test `up_twice_is_clean`).
2. Two local runs at once (two terminals): the second `up.sh` must refuse with "dkwci is already running", not tear the
   first one down (Task 1 test `second_up_refuses`).
3. A test that still skips silently: the `REQUIRE_SERVICES=1` path must cover every `skip` in the moved tests (Task 2
   check `no_silent_skips`, a grep over the moved tests for skip paths not guarded by it).
4. A service whose image tag no longer exists or is slow to become healthy on a shared runner: `up.sh` gives up after a
   bounded wait (default 180 s, `CI_SERVICES_TIMEOUT`) and prints the service's last 100 log lines, never hangs the job
   until the 25-minute job timeout (Task 1 test `unhealthy_times_out`).
5. A seeded value leaking into the job log (the Gitea token, the SFTP private key): `up.sh` never prints the env files'
   contents; `ci/annotate.py` output for a failing service test contains no line from `ci/services/out/` (Task 3 check).

---

### Task 1: Services file, `up.sh` / `down.sh`, `run_test.sh` support (profile `ldap` as the first service)

**Files:**
- Create: `ci/services/docker-compose.yml`, `ci/services/up.sh`, `ci/services/down.sh`, `ci/services/seed/lldap.py`,
  `ci/services/test_services_harness.sh`
- Modify: `ci/run_test.sh`, `.gitignore` (`ci/services/out/`), `ci/README.md`
- Test: `ci/services/test_services_harness.sh` (host shell test)

**Interfaces:**
- Produces: `ci/services/up.sh <profile>...` (exit 0 when every requested service is healthy and every seed exited 0;
  writes `ci/services/out/<profile>.env`), `ci/services/down.sh` (`down -v --remove-orphans`, idempotent), network
  `dkwci_default`; `ci/run_test.sh` reads `CI_SERVICES="<profile> ..."`: runs `up.sh` unless `CI_SERVICES_UP=0`
  (already up), adds `--network dkwci_default`, one `--env-file` per profile file, `-e REQUIRE_SERVICES=1`, and runs
  `down.sh` on exit unless `CI_SERVICES_KEEP=1`. Profile `ldap` env file: `LLDAP_TEST_HOST=lldap`,
  `LLDAP_BIND_DN`, `LLDAP_BIND_PASSWORD`, `LLDAP_BASE_DN=dc=example,dc=com`, `LLDAP_USER_PASSWORD`.

- [ ] **Step 1: Write the harness test** `ci/services/test_services_harness.sh` (prints `[PASS]/[FAIL]` lines):
  - `up_and_seeded`: `up.sh ldap` exits 0; `out/ldap.env` exists; an `ldapsearch` (from a throwaway
    `alpine` + `openldap-clients` container on `dkwci_default`) for `uid=jdoe` with the seeded bind finds one entry
    in group `dkw_engineers`.
  - `up_twice_is_clean`: delete jdoe through the lldap GraphQL API, run `up.sh ldap` again: jdoe exists again.
  - `second_up_refuses`: with the stack up, a second `up.sh ldap` from another shell (`CI_SERVICES_UP` unset, a lock
    file `ci/services/out/.lock` held) exits 1 with "dkwci is already running".
  - `unhealthy_times_out`: `CI_SERVICES_TIMEOUT=5 up.sh ldap` with a compose override whose health check always fails
    exits non-zero within 30 s and prints log lines of `lldap`.
  - `nothing_of_the_owner_touched`: the output of `docker ps -a --format '{{.Names}}'` and `docker network ls` minus
    the `dkwci` entries is identical before and after the harness.
  - `down_clean`: after `down.sh`, no container, network or volume named `dkwci*` remains.
- [ ] **Step 2:** Run `sh ci/services/test_services_harness.sh`; expect FAIL (files missing).
- [ ] **Step 3: Implement.** Compose: top-level `name: dkwci`; service `lldap` (profile `ldap`, health check on
  `:17170/health`, dummy `LLDAP_JWT_SECRET` / `LLDAP_LDAP_USER_PASS` from the file), two seed services: `lldap-seed` (profile `ldap`, the studio image, `depends_on: lldap: condition: service_healthy`,
  a Python script `seed/lldap.py` calling lldap's GraphQL API after `/auth/simple/login`: creates the three users and
  groups and the memberships) and `lldap-pw` (the lldap image, `depends_on: lldap-seed: condition:
  service_completed_successfully`, runs the image's `/app/lldap_set_password` for each user with `Password123!`;
  lldap only accepts passwords through that OPAQUE client). `lldap-seed` writes `out/ldap.env` through a bind mount of `ci/services/out`. `up.sh`: lock file,
  `down -v`, `up -d --wait --wait-timeout $CI_SERVICES_TIMEOUT` for the listed profiles, then
  `docker compose wait <seed services>`; on failure print `logs --tail 100` of the failed services and exit 1.
- [ ] **Step 4:** Run the harness; expect all PASS. Also check that a test container on `dkwci_default` can download
  DuckDB's delta extension: `CI_SERVICES=ldap ci/run_test.sh autoloader` passes (it does not use LDAP; this only proves
  the network has internet).
- [ ] **Step 5:** Commit `ci(services): compose stack for test services, lldap first`.

### Task 2: LDAP tests on the stack

**Files:**
- Modify: `scratch/test_ldap_auth.py`, `scratch/test_group_sync_ldap.py` (env-based logins), `.gitignore` (drop both),
  `ci/plan.json` (two entries with `"services": "ldap"`), `.github/workflows/ci.yml` (tests job: `CI_SERVICES` from
  `matrix.test.services`; on failure `docker compose -p dkwci logs --tail 200`)
- Create: `scratch/_services.py` (shared helper)

**Interfaces:**
- Produces: `scratch/_services.py`: `require_or_skip(name: str, host: str, port: int, timeout: float = 2.0) -> bool`
  (True when reachable; when not, prints the skip line and returns False, or, with `REQUIRE_SERVICES=1`, prints
  `[FAIL] <name> not reachable at host:port` and calls `os._exit(1)`).

- [ ] **Step 1:** Replace the two tests' reachability checks with `require_or_skip("lldap", LLDAP_HOST, 3890)`; every
  login, bind DN, base DN and user password comes from the `LLDAP_*` env vars with the stack's dummy values as defaults.
  The docstrings name `ci/services/up.sh ldap` instead of the dev lldap and contain no path of the product owner's.
- [ ] **Step 2:** Add the check `no_silent_skips` to `scratch/test_ci_services_wiring.py` (new, in `ci/plan.json`, no
  services): for every plan entry with `services`, its script contains no `skipping`/`skipped` print that is not inside
  `_services.require_or_skip`. (Grep-based; lists offenders.)
- [ ] **Step 3:** Run `CI_SERVICES=ldap ci/run_test.sh ldap_auth` and `... group_sync_ldap`; expect PASS. Run
  `REQUIRE_SERVICES=1 ci/run_test.sh ldap_auth` with no stack: expect `[FAIL] lldap not reachable`.
- [ ] **Step 4:** `git check-ignore scratch/test_ldap_auth.py` prints nothing; `git grep -n "/home/" scratch/` finds
  nothing new.
- [ ] **Step 5:** Commit `ci: run the LDAP tests against the services stack`.

### Task 3: Git, Kafka, Azure, S3 and SFTP profiles and their tests

**Files:**
- Modify: `ci/services/docker-compose.yml` (profiles `git`, `kafka`, `azure`, `s3`, `sftp`), create
  `ci/services/seed/{gitea,garage,sftp}.sh`; modify `scratch/test_git_sync.py`, `test_git_pr_mode.py`,
  `test_git_review.py`, `test_git_forges.py` (host `GIT_HOST`, default `gtest_gitea:3000`, token `TOK` from the env
  file), `test_streaming.py`, `test_stream_ops.py` (`require_or_skip` on `KAFKA`, `KAFKA2`, `SASL_KAFKA`),
  `test_autoloader_azure.py` (`AZURITE_HOST`), `test_autoloader_gcs.py`, `test_autoloader_preview.py`,
  `test_autoloader_target.py`, `test_delta_sharing.py` (S3 part; read `TEST_S3_*` / `S3_*` from the `s3` env file),
  `test_connections.py` (`SFTP_HOST`, `SFTP_KEY`); `ci/plan.json`
- Test: the moved tests themselves; `scratch/test_ci_services_wiring.py`

**Interfaces:**
- Produces env files: `git.env` (`GIT_HOST=gitea:3000`, `TOK`, repos `ga/dbt-rv`, `ga/nb-rv`, `ga/dbt-fg`,
  `ga/dbt-project`, `ga/notebooks`: the union of what the four git tests create or expect); `kafka.env`
  (`KAFKA=redpanda:9092`, `KAFKA2=redpanda2:9092`, `REGISTRY=http://redpanda:8081`, `SASL_KAFKA=redpanda-sasl:9092`,
  `SASL_USER`, `SASL_PASSWORD`); `azure.env` (`AZURITE_HOST=azurite`); `s3.env` (`GARAGE_ENDPOINT=garage:3900`,
  `TEST_S3_ENDPOINT`, `TEST_S3_ACCESS_KEY`, `TEST_S3_SECRET_KEY`, and the `S3_*` aliases `test_autoloader_preview.py`
  reads); `sftp.env` (`SFTP_HOST=sftp`, `SFTP_KEY=/services/sftp/id`: the seed writes the private key into
  `ci/services/out/sftp/`, which `run_test.sh` mounts read-only at `/services`).

- [ ] **Step 1:** Add the profiles. Gitea: the settings of `scratch/gitea_up.sh` (rootless image, install lock, root URL
  `http://gitea:3000`), seed = user `ga`, a token with all scopes, the repos above. Redpanda: the three brokers with the
  flags from the two tests' docstrings; the SASL broker with the `--set redpanda.kafka_api=...`, `enable_sasl`,
  `superusers` flags from CLAUDE.md and a seed running `rpk security user create`. Azurite with
  `azurite-blob --skipApiVersionCheck --blobHost 0.0.0.0`. Garage: the config and key/bucket steps of
  `scratch/garage_up.sh`. SFTP: `atmoz/sftp` with a key pair generated by the seed (`ssh-keygen -t ed25519`, public key
  mounted into the server's `.ssh/keys`).
- [ ] **Step 2:** Adapt the tests (host and credentials from env; old defaults kept; `require_or_skip` for every
  service). Add their plan entries: `git_sync`, `git_pr_mode`, `git_review`, `git_forges` (`"services": "git"`),
  `streaming` (`"kafka"`, extras `confluent-kafka fastavro 'grpcio-tools>=1.73,<1.77'`), `stream_ops` (`"kafka"`,
  extras `confluent-kafka`), `autoloader_azure` (`"azure"`), `autoloader_gcs`, `autoloader_preview`,
  `autoloader_target` (`"s3"`), `connections` (`"sftp"`), and give the existing `delta_sharing` entry `"services": "s3"`.
- [ ] **Step 3:** Run each locally with `CI_SERVICES=<profile> ci/run_test.sh <name> "<extras>"`; expect PASS for all
  twelve. For `streaming`, confirm in its output that the SASL and Avro/Protobuf/JSON Schema sections ran (not skipped).
- [ ] **Step 4:** Leak check: run `CI_SERVICES=git GIT_HOST=127.0.0.1:9 ci/run_test.sh git_sync` (fails on purpose),
  pipe the output through `python3 ci/annotate.py`, and assert that none of the values in `ci/services/out/*.env`
  (token, keys, passwords) appears in either. Add this as `secrets_not_in_output` to `ci/services/test_services_harness.sh`.
- [ ] **Step 5:** Commit `ci: git, kafka, azure, s3 and sftp services; their tests run in CI`.

### Task 4: Keycloak (SAML + OIDC)

**Files:**
- Modify: `ci/services/docker-compose.yml` (profile `idp`), create `ci/services/seed/keycloak-realm.json`
- Create: `scratch/test_oidc_keycloak.py`
- Modify: `scratch/verify_saml_keycloak.py` (start its own throwaway studio on `dkwci_default` like the other UI
  scripts, Keycloak URL from env), `ci/plan.json` (test + UI entry, `"services": "idp"`), `.github/workflows/ci.yml`
  (ui job honours `matrix.ui.services`)

**Interfaces:**
- Produces `idp.env`: `KC_URL=http://keycloak:8080`, `KC_REALM=dkw`, `KC_DESCRIPTOR=http://keycloak:8080/realms/dkw/protocol/saml/descriptor`,
  `KC_OIDC_CLIENT_ID=dkw-oidc`, `KC_OIDC_CLIENT_SECRET` (fixed dummy in the realm file), users `alice`/`alicepw`
  (group `LakehouseAdmins`), `bob`/`bobpw`.
- Realm file: the SAML client of `verify_saml_keycloak.py`'s docstring (`urn:dkw:keycloak-test`, signed assertions,
  mappers `username`, `groups`) and an OIDC confidential client `dkw-oidc` (standard flow, PKCE S256 required, redirect
  `http://*/api/auth/oidc/callback`, a `groups` claim mapper). Keycloak runs `start-dev --import-realm` with
  `KC_HOSTNAME=http://keycloak:8080` (one name for browser and back channel, since the UI test's browser runs on the
  host it reaches Keycloak through a published-on-demand port: see Step 3).

- [ ] **Step 1: Write `scratch/test_oidc_keycloak.py`** (in the image, on `dkwci_default`; `requests` drives Keycloak's
  login form like a browser): configure `oidc_auth` with discovery `KC_URL/realms/dkw/.well-known/openid-configuration`;
  `GET /api/auth/oidc/login` through a TestClient gives a redirect to Keycloak with `code_challenge_method=S256`;
  follow it with a `requests.Session`, post the login form for alice, catch the redirect to the callback, pass it to
  the TestClient: checks `alice_signed_in` (session cookie, role from the LakehouseAdmins mapping), `groups_synced`,
  `bob_default_role`, `state_tampered_refused` (callback with a changed `state` → refused), `code_replay_refused`
  (same code twice → second refused).
- [ ] **Step 2:** Run with `CI_SERVICES=idp ci/run_test.sh oidc_keycloak`; expect PASS.
- [ ] **Step 3:** UI: `verify_saml_keycloak.py` needs the browser (host) to reach Keycloak and the studio. Add to
  `up.sh` an option `CI_SERVICES_PUBLISH="keycloak:8180:8080"` that writes a temporary compose override with that one
  `ports:` entry bound to `127.0.0.1` (the only published port in this plan, opt-in, loopback only), and set
  `KC_HOSTNAME=http://localhost:8180` for this profile when it is published. The script starts its studio on
  `dkwci_default` with a published `127.0.0.1:8117`. Run it; expect ALL PASS.
- [ ] **Step 4:** Commit `ci: Keycloak for SAML and OIDC; real-IdP tests in CI`.

### Task 5: Entra + Fabric emulators (OneLake)

**Files:**
- Modify: `ci/services/docker-compose.yml` (profile `fabric`), create `ci/services/seed/fabric.sh`
- Create: `scratch/test_onelake_emulator.py`
- Modify: `ci/plan.json`

**Interfaces:**
- Produces `fabric.env`: `ENTRA_AUTHORITY=https://entra.localhost:8443/<tenant>`, `FABRIC_TENANT_ID`,
  `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_WORKSPACE`, `FABRIC_LAKEHOUSE`, `FABRIC_TABLE=orders`,
  `FABRIC_CA_PEM_FILE=/services/fabric/ca.pem` (the emulator's certificate, extracted by the seed after start: it is
  regenerated on every restart, CLAUDE.md "Testing this against a fake OneLake").
- Network aliases as in CLAUDE.md: `entra.localhost` on the entra service; `onelake.dfs.fabric.microsoft.com` and
  `onelake.blob.fabric.microsoft.com` on `onelake-proxy` (TCP 443 → fabric:9443). Aliases replace the per-container
  `/etc/hosts` lines because every container on `dkwci_default` resolves them, and only on this network.

- [ ] **Step 1: Spike the seed** (throwaway, not committed): with the profile up, find the emulator's calls that create
  a workspace, a Lakehouse item and a client credential, and write one Delta table under `Tables/orders` with
  `deltalake.write_deltalake("abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/orders", ...)`
  and a bearer token from the Entra emulator. Record the working calls; if writing through `abfss://` does not work
  against the emulator, write the table through the emulator's DFS API file by file (the `_delta_log` JSON and one
  Parquet file built locally with delta-rs). Throw the spike away.
- [ ] **Step 2:** Write `ci/services/seed/fabric.sh` (or a Python seed run in the studio image, since it needs
  `deltalake`) from the spike; it writes `fabric.env` and the CA file.
- [ ] **Step 3: Write `scratch/test_onelake_emulator.py`**: `mount_onelake_catalog(..., authority=ENTRA_AUTHORITY,
  ca_cert_pem=<file>)` succeeds; `list_tables` lists `orders`; `get_table_metadata` has its columns; `read_table`
  returns the seeded rows (by table name and by GUID path); `unmount_onelake_catalog` removes the trusted CA again
  (`trust_ca_cert` key gone); a mount with a wrong CA fails with a TLS error, not a hang.
- [ ] **Step 4:** Run with `CI_SERVICES=fabric ci/run_test.sh onelake_emulator`; expect PASS. Add the plan entry.
- [ ] **Step 5:** CLAUDE.md OneLake bullet: point at this test instead of `../backendservices`; commit
  `ci: Entra and Fabric emulators; OneLake mounts tested in CI`.

### Task 6: Mail and webhook delivery

**Files:**
- Modify: `ci/services/docker-compose.yml` (profile `mail`)
- Create: `scratch/test_notifications_delivery.py`
- Modify: `ci/plan.json`

**Interfaces:**
- Produces `mail.env`: `SMTP_HOST=mailpit`, `SMTP_PORT=1025`, `MAILPIT_API=http://mailpit:8025/api/v1`.

- [ ] **Step 1: Write the test:** `email_reports.update_email_config(...)` to mailpit; `send_email` with an attachment →
  the message appears in `MAILPIT_API/messages` with subject, recipient and attachment name; a wrong port gives a clean
  error result, not an exception. Webhooks: an in-process `http.server` thread on `127.0.0.1:<free port>`;
  `slack_integration.send_notification`, `webhook_alerts.send_webhook` for types `teams` and generic → the receiver
  got one POST each, JSON, the Teams one in the MessageCard shape the module builds, no session cookie or token header.
- [ ] **Step 2:** Run with `CI_SERVICES=mail ci/run_test.sh notifications_delivery`; expect PASS. Add the plan entry.
- [ ] **Step 3:** Commit `ci: mail and webhook delivery tested against Mailpit and a local receiver`.

### Task 7: Remaining UI scripts, host test, Dependabot, docs

**Files:**
- Modify: `scratch/verify_git_sync_ui.py`, `verify_git_pr_ui.py`, `verify_notebook_git_ui.py`,
  `verify_group_sync_ui.py` (use the stack's Gitea / lldap instead of their own when `CI_SERVICES` env files are
  present; studio on `dkwci_default`), `ci/plan.json` (UI entries with `services`; `ldap_tls` with `"host": true`),
  `.github/workflows/ci.yml` (a `host` job matrix for `"host": true` entries: `/usr/bin/python3 scratch/test_<name>.py`),
  `.github/dependabot.yml` (`docker-compose` ecosystem, directory `/ci/services`, weekly, target `development`),
  `ci/README.md`, CLAUDE.md (CI bullet: the stack, profiles, `CI_SERVICES`, `REQUIRE_SERVICES`, counts)

- [ ] **Step 1:** Adapt the four UI scripts and run each with `UI_SLOW=3` after `ci/services/up.sh <profile>`; expect
  ALL PASS.
- [ ] **Step 2:** Run `test_ldap_tls.py` the way the host job will; expect PASS.
- [ ] **Step 3:** Push; wait for CI (`waitci.sh <sha>`); expect every job green, including each new service job. Note
  the slowest service start time per profile in `ci/README.md`.
- [ ] **Step 4:** Commit `ci: UI scripts and host test on the services stack; Dependabot watches its images`.

---

## Notes for the implementer

- Locally the plain Docker bridge has no internet on this machine; a compose network does. If Task 1 Step 4 shows the
  opposite, `run_test.sh` connects the test container to `DOCKER_NETWORK` as a second network instead.
- The Gitea, Garage and lldap seeds already exist as `scratch/gitea_up.sh`, `scratch/garage_up.sh` and the LDAP group
  sync test's GraphQL calls: port them, do not reinvent them, and leave the old scripts in place for manual use.
