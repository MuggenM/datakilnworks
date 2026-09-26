# Data Kiln Works on Kubernetes (Helm chart)

`deploy/helm/datakilnworks` runs the studio and its SQL compute nodes on Kubernetes, with the one-shot initialisation (`python -m web.init`) as an
**init container**. It was checked with `helm lint` and `helm template` (structure, values, the rendered objects); it has **not** been installed on a
live cluster from here, so render it and review it for your cluster before installing.

## What it creates

| Object | Notes |
| --- | --- |
| Deployment `<release>-studio` | **one replica, strategy `Recreate`**: the studio owns SQLite metadata files, one Git working tree behind an in-process lock and one Kafka runner per stream. Scale it up (memory, CPU), not out. |
| initContainer `init` | same image, `python -m web.init`: validates the bootstrap admin, creates / migrates every database, seeds the dbt project, checks the volumes, optionally waits for other services. A misconfiguration shows as `Init:Error` with a readable log instead of a crash loop. |
| Probes | startup + liveness on `/healthz`, readiness on `/readyz` (no login, no data, exempt from the IP allowlist). |
| Deployments + Services `compute-node-*` | the SQL warehouse workers (`computeNodes` in the values). Token-protected (`COMPUTE_TOKEN`), reachable only inside the cluster; with a ReadWriteOnce warehouse volume they are scheduled on the studio's node. |
| PVCs | `warehouse` (Delta tables, metadata), `notebooks`, `dbtProject`; annotated `helm.sh/resource-policy: keep`. |
| Secret (optional) | or use `secrets.existingSecret`; its keys become environment variables. The `COMPUTE_TOKEN` is generated once and kept across upgrades. |
| Ingress, NetworkPolicy (optional) | the policy lets the studio be reached from the ingress namespace only, and the compute nodes from the studio only. |

## Install

```bash
docker build -t registry.example.org/dkw:<tag> . && docker push registry.example.org/dkw:<tag>      # the image contains web/ and docs/

kubectl create namespace dkw
kubectl -n dkw create secret generic dkw-secrets \
    --from-literal=INIT_ADMIN_PASSWORD_HASH="$(docker run --rm -it registry.example.org/dkw:<tag> python -m web.auth hash-password)"
helm upgrade --install dkw deploy/helm/datakilnworks -n dkw \
    --set image.repository=registry.example.org/dkw --set image.tag=<tag> --set secrets.existingSecret=dkw-secrets

kubectl -n dkw logs deploy/dkw-datakilnworks-studio -c init        # what the initialisation did
kubectl -n dkw port-forward svc/dkw-datakilnworks 8891:80          # http://localhost:8891
```

Put other settings (Git, `TRUSTED_PROXIES`, `GIT_FORGE`, ...) under `studio.env`, and tokens (`GIT_TOKEN`, `NOTEBOOKS_GIT_TOKEN`) into the Secret.

## Things to know

* **The dbt project** volume should hold a checkout of *your own* dbt project repository in production; an empty volume is seeded from the template.
* **Behind an ingress** the studio sees the ingress controller's address: declare it as a trusted proxy (`studio.env.TRUSTED_PROXIES`) before using the IP allowlist.
* **Not included:** the notebook sandbox (kernels of users under a masking policy; without it their notebooks are refused, never run unmasked) and the container
  controller (Docker-socket auto-suspend of compute nodes; disabled here through an empty `CONTROLLER_URL`, warehouses keep the flag-only `auto_stop` behaviour).
* **Upgrades** are `helm upgrade`: the `Recreate` strategy stops the old pod, the init container migrates, the new pod starts. Back up the warehouse volume first.
* **The same init in Docker Compose:** the `datakilnworks-init` service (the studio waits for it to succeed).
