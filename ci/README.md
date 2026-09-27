# Continuous integration

`.github/workflows/ci.yml` runs on every push to `development` / `main` and on every pull request. Everything it does can be reproduced locally.

| Job | What it checks | Locally |
| --- | --- | --- |
| **Static checks** | Python compiles; every place that runs SQL is governed (`scratch/test_governance_coverage.py`); `shellcheck` on `ci/*.sh`; both Compose files are valid (default and `proxy` profile); `helm lint` and `helm template` (default values, the smoke-test values, ingress + network policy); the rendered manifests validate against Kubernetes 1.30 schemas (kubeconform) | `python scratch/test_governance_coverage.py`, `docker compose config -q`, `helm lint deploy/helm/datakilnworks --set-string secrets.initAdminPasswordHash=x` |
| **test_\*** (one job each) | A self-contained test script in the **freshly built image** (`scratch/test_<name>.py`), in a throwaway container with a throwaway warehouse | `docker build -t localspark-lakehouse-notebook . && ci/run_test.sh <name>` (add `CI_MOUNT_SOURCES=1` to use your working tree instead of the image's copy) |
| **browser \<name\>** | A Playwright script that starts its own throwaway studio (and MinIO / Traefik where needed) and drives the UI with Chromium | `pip install playwright && playwright install chromium && python scratch/verify_<name>.py` |
| **kind** | The Helm chart on a real Kubernetes cluster, see below | `ci/kind-smoke.sh` |

The lists live in `ci/plan.json` (`tests` with optional extra pip packages, `ui`). Add a test there and it runs in its own job. A test script must exit
non-zero on failure and must not need anything outside the container: tests that need real services (Gitea, Redpanda, a real S3 server, lldap, Keycloak,
SFTP, the Docker socket) stay out of CI; each says how to run it in its docstring.

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
