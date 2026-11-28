# Design: backend services for the tests in CI

Status: design, approach A chosen by the product owner (2026-11-28). Plan: `proposals/ci-services-plan.md`.
Calls made by the designer are marked *(designer's call)*.

## 1. Intent

About twenty tests and UI scripts are kept out of CI because they need a real outside service (LDAP, Git forge, Kafka,
object storage, SFTP, an IdP, OneLake). They only run when someone starts the right throwaway container by hand, so
regressions in those features reach `development` unseen. The goal is that CI starts the services these tests need and
runs them on every push, the same way locally.

The product owner keeps using their own `../backendservices` for day-to-day work; this stack is for tests only and never
touches it, the dev lldap or the dev Redpanda (`dev-kafka`).

## 2. Goals, non-goals, anti-goals

Goals
- One compose file in this repository, `ci/services/docker-compose.yml`, with every service the tests need, seeded with
  fixed test data, started per CI job for exactly the services that job's test needs.
- The same command locally (`ci/services/up.sh <profile...>`), under compose project `dkwci`, without published ports.
- Every test that today needs a hand-started service runs in CI.

Non-goals
- Ollama / LM Studio (excluded by the product owner).
- Key Vault and ARM emulators: Data Kiln Works does not use them *(designer's call)*.
- Tests that need a studio already running on :8891 (`user_switch_isolation`, `tracking_api_parity`, the `:8891` UI
  scripts) and the Docker-socket test (`container_autosuspend`): separate work.
- Replacing `backendservices`.

Anti-goals
- A test that SKIPS in CI because its service is missing: it would look green. In CI a missing service is a failure.
- A secret of the product owner's in a tracked file (the two gitignored LDAP tests carry the dev lldap login today).
- The stack reaching or colliding with the product owner's own containers, networks, volumes or ports.
- Floating images (`latest`): a service release must not break CI unannounced.

## 3. Approaches

- **A. A services compose file in this repository (chosen).** One reviewable file with compose profiles per service;
  CI and local runs use it identically; it costs a second definition of services that `backendservices` also defines.
- B. The tests' own start scripts (`garage_up.sh`, `gitea_up.sh`, ...): least code, but each is a one-off and Keycloak,
  lldap, Redpanda and the Fabric emulators have none.
- C. A self-hosted runner on the product owner's machine using `backendservices`: rejected, CI would run pull-request code
  inside the home network.

## 4. Services

| Profile | Service(s) | Image (pinned) | Seeded with | Used by |
| --- | --- | --- | --- | --- |
| `ldap` | `lldap` | `lldap/lldap` | users jdoe/asmith/bjones, groups dkw_engineers/dkw_admins/dkw_analysts | `test_ldap_auth`, `test_group_sync_ldap`, `verify_group_sync_ui` |
| `git` | `gitea` | `gitea/gitea:<x>-rootless` | user `ga`, token, repos | `test_git_sync`, `test_git_pr_mode`, `test_git_review`, `test_git_forges`, the git UI scripts |
| `kafka` | `redpanda`, `redpanda2`, `redpanda-sasl` | `redpandadata/redpanda` | SCRAM user on the SASL broker; built-in schema registry | `test_streaming`, `test_stream_ops` |
| `azure` | `azurite` | `mcr.microsoft.com/azure-storage/azurite` | none (tests create containers) | `test_autoloader_azure` |
| `s3` | `garage` | `dxflrs/garage` | key + bucket `test` | `test_autoloader_gcs`, `test_autoloader_preview`, `test_autoloader_target`, S3 part of `test_delta_sharing` |
| `sftp` | `sftp` | `atmoz/sftp` | user + generated key pair | SFTP part of `test_connections` |
| `idp` | `keycloak` | `quay.io/keycloak/keycloak` | realm `dkw` (SAML client + OIDC client, alice/bob, group LakehouseAdmins) | `verify_saml_keycloak`, new `test_oidc_keycloak` |
| `fabric` | `entra-emulator`, `fabric-emulator`, `onelake-proxy` | `ghcr.io/calvinchengx/...` | a workspace, a Lakehouse, one Delta table | new `test_onelake_emulator` |
| `mail` | `mailpit` | `axllent/mailpit` | none | new `test_email_delivery` |

- Webhook delivery (Slack / Teams / generic webhook) is tested with an in-process HTTP receiver inside a new test, not a
  container: a receiver container adds nothing a test thread cannot do *(designer's call)*.
- MinIO stays where it is: `verify_s3_events_ui.py` already starts its own and runs in CI *(designer's call)*.

## 5. How a test finds its service

- Fixed service names on the compose network `dkwci_default` (`lldap`, `gitea`, `redpanda`, `garage`, ...) *(designer's call:
  tests take the host from an environment variable whose default is the old throwaway name, so manual runs keep working)*.
- Seed jobs (one-shot compose services) write generated values (Gitea token, Garage key, SFTP key, Keycloak realm
  details, Fabric ids) to `ci/services/out/<profile>.env` (gitignored); `ci/run_test.sh` passes those files to the test
  container with `--env-file` when `CI_SERVICES` is set, and puts the container on `dkwci_default`.
- `REQUIRE_SERVICES=1` (set by `ci/run_test.sh` whenever `CI_SERVICES` is set) turns every "service unreachable: skipping"
  path in a test into a failure.

## 6. CI

- `ci/plan.json` entries gain an optional `"services": "<profile> [<profile>...]"`; the test job starts them with
  `ci/services/up.sh` before the test and dumps their logs on failure. UI entries likewise.
- Host tests that start their own containers (`test_ldap_tls`) get a `"host": true` entry run with `/usr/bin/python3`.
- Dependabot's `docker-compose` ecosystem watches `ci/services/docker-compose.yml`; images are pinned by tag.

## 7. Left to the builder

The exact image tags (latest stable at implementation time), seed script details, health checks, how the Fabric emulator
is seeded (its own REST API; see the plan's spike step).
