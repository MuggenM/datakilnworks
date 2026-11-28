# Continuous integration

`.github/workflows/ci.yml` runs on every push to `development` / `main` and on every pull request. Everything it does can be reproduced locally.

| Job | What it checks | Locally |
| --- | --- | --- |
| **Static checks** | Python compiles; every place that runs SQL is governed (`scratch/test_governance_coverage.py`); `shellcheck` on `ci/*.sh` and `ci/services/` scripts; both Compose files are valid (default and `proxy` profile); `helm lint` and `helm template` (default values, the smoke-test values, ingress + network policy); the rendered manifests validate against Kubernetes 1.30 schemas (kubeconform) | `python scratch/test_governance_coverage.py`, `docker compose config -q`, `helm lint deploy/helm/datakilnworks --set-string secrets.initAdminPasswordHash=x` |
| **test_\*** (one job each) | A self-contained test script in the **freshly built image** (`scratch/test_<name>.py`), in a throwaway container with a throwaway warehouse | `docker build -t localspark-lakehouse-notebook . && ci/run_test.sh <name>` (add `CI_MOUNT_SOURCES=1` to use your working tree instead of the image's copy) |
| **browser \<name\>** | A Playwright script that starts its own throwaway studio (and MinIO / Traefik where needed) and drives the UI with Chromium | `pip install playwright && playwright install chromium && python scratch/verify_<name>.py` |
| **kind** | The Helm chart on a real Kubernetes cluster, see below | `ci/kind-smoke.sh` |
| **Supply chain** (own workflow, `supply-chain.yml`; also weekly with a cache-less rebuild) | Known vulnerabilities in the built image: Python packages with pip-audit, OS and other packages with Trivy (HIGH/CRITICAL that have a fix). Fails on anything not listed, with a reason, in `ci/pip-audit-ignore.txt` / `ci/trivyignore`. Dependabot (`.github/dependabot.yml`) proposes the updates weekly | `ci/pip_audit.sh`, `ci/trivy_scan.sh` (env `IMAGE`, `DOCKER_NETWORK`, `DOCKER_SOCKET_PATH` for rootless Docker) |

The lists live in `ci/plan.json` (`tests` with optional extra pip packages, `ui`). Add a test there and it runs in its own job. A test script must exit
non-zero on failure and must not need anything outside the container, except services of the stack below (named in the entry's
`"services"`). Tests that need Keycloak or the Docker socket stay out of CI for now; each says how to run it in its docstring.

## Backend services for tests (`ci/services/`)

Tests that need a real outside service get it from one compose stack, `ci/services/docker-compose.yml` (design:
`proposals/ci-services.md`), with one profile per service. Only test data lives there: every login is a dummy value;
generated values (tokens, keys) go to the gitignored `ci/services/out/<profile>.env`.

```bash
ci/services/up.sh ldap            # start profiles from empty volumes, wait for health + seeds; refuses while already up
CI_SERVICES=ldap ci/run_test.sh ldap_auth   # starts them, runs the test on network dkwci_default with their env files, stops them
ci/services/down.sh               # remove everything of project dkwci (idempotent)
bash ci/services/test_services_harness.sh   # tests the stack itself
```

- Project `dkwci`, no published ports: it never touches other containers, networks or ports on the machine.
- `CI_SERVICES` sets `REQUIRE_SERVICES=1` in the test: a service the test cannot reach is a failure, not a skip. Tests
  check reachability with `scratch/_services.require_or_skip` and have no default host for a service they modify.
- A `ci/plan.json` entry names its profiles in `"services"`; CI starts them, and `test_ci_services_wiring` checks the wiring.
- Every service and every test container on the stack runs with **no DNS search domain** (`dns_search: .`,
  `--dns-search .`): Docker copies the host's, and where that domain resolves unknown names (this dev machine's does, to
  an outside host), curl tried `lldap.<domain>` first and connected there instead of to the stack's lldap.
- `ci/services/out` is mounted read-only at `/services` in the test (the sftp key pair). `CI_TEST_ENV="NAME=value ..."`
  overrides a value of the env files for one run (the harness points `GIT_HOST` at a dead address that way).

| Profile | Services (host names on `dkwci_default`) | Env file | Tests |
| --- | --- | --- | --- |
| `ldap` | `lldap` `v0.6.2-alpine`: users jdoe / asmith / bjones in dkw_engineers / dkw_admins / dkw_analysts, u1 in dkw_analysts, u2 and u3 in no group, group dkw_ops, password `Password123!`, bind `uid=admin,ou=people,dc=example,dc=com` / `adminpassword` | `LLDAP_*` | `ldap_auth`, `group_sync_ldap` |
| `git` | `gitea` 1.22 (rootless): user `ga`, an all-scopes token, empty repos `ga/{dbt-project,notebooks,dbt-pr,nb-pr,dbt-rv,nb-rv,dbt-fg}` | `GIT_HOST`, `TOK` | `git_sync`, `git_pr_mode`, `git_review`, `git_forges` |
| `kafka` | `redpanda`, `redpanda2` (a second cluster), `redpanda-sasl` (SCRAM-SHA-256 user `dkwci`); Schema Registry on `redpanda:8081` | `KAFKA`, `KAFKA2`, `REGISTRY`, `SASL_*` | `streaming`, `stream_ops` |
| `azure` | `azurite` 3.35 (blob, `--skipApiVersionCheck`), the well-known dev account | `AZURITE_HOST` | `autoloader_azure` |
| `s3` | `garage` (Deuxfleurs Garage v1.0.1, region `garage`): key `test`, may create buckets; stands in for GCS too | `TEST_S3_*`, `S3_*`, `GARAGE_ENDPOINT` | `autoloader_gcs`, `autoloader_preview`, `autoloader_target`, `delta_sharing` |
| `sftp` | `sftp` (atmoz/sftp): `user`/`pass`, folder `/upload`, an ed25519 key pair made at every start | `SFTP_HOST`, `SFTP_KEY=/services/sftp/id` | `connections` |

## The cluster smoke test (`ci/kind-smoke.sh`)

Creates a kind cluster, loads the image, installs `deploy/helm/datakilnworks` with `ci/kind-values.yaml` (one compute node, small volumes), and checks:

1. the init container, the studio and the compute node become ready and the three volumes are bound;
2. through the Service: sign in as the bootstrap admin, the forced password change (other API calls are refused until then), SQL on the studio and on the
   **compute node pod**, a table written to the warehouse volume (`ci/smoke_api.py first`, plain HTTP);
3. deleting the studio pod: the **changed password and the table survive** (`ci/smoke_api.py again`);
4. `helm upgrade` with a changed setting: still healthy, the generated compute token is kept;
5. a second release with a broken bootstrap admin stops in the init container (`Init:Error`) with a log that names `INIT_ADMIN_PASSWORD_HASH`, and the studio
   container never starts;
6. `helm uninstall` keeps the data volumes (`helm.sh/resource-policy: keep`).

On failure it prints the pods, events and logs. Needs docker, kind, kubectl, helm and python3; `IMAGE=<tag>` tests another image, `KEEP_CLUSTER=1` keeps the
cluster, `USE_CURRENT_CONTEXT=1 IMAGE_LOADER=<cmd>` uses another cluster (k3s, minikube, ...).

**Status of this test:** it passed on a real kind cluster on GitHub Actions on its first run (September 2026), so the chart has been installed, signed in to, queried, restarted, upgraded and uninstalled on a real cluster there. It cannot be run on the machine it was written on (rootless Docker without cgroup delegation, exhausted inotify limit), so it was never run locally; the HTTP part (`ci/smoke_api.py`) was also run against real containers laid out like the chart. It covers one replica on a single-node kind cluster, not a multi-node or multi-zone setup.


**A real S3 server in tests:** `scratch/garage_up.sh` boots a throwaway single-node Deuxfleurs Garage and prints its endpoint and a generated access key, used
by `scratch/test_autoloader_preview.py`, `scratch/test_autoloader_target.py` and `scratch/verify_autoloader_preview_ui.py` (none of these run in CI; see
their docstrings). `scratch/verify_s3_events_ui.py` is the one exception and still uses MinIO (`cgr.dev/chainguard/minio`, env `MINIO_IMAGE` overrides;
MinIO no longer publishes images on Docker Hub or quay.io), because it tests MinIO's bucket-notification/webhook feature and Garage has no S3
notification API at all.
