# Gitea for Data Kiln Works (optional)

The studio's git integration speaks the plain git protocol, so any Gitea (or GitLab, GitHub, ...) works. This directory only
helps if you want to run one yourself.

## What the studio needs from Gitea

| Need | Detail |
| :--- | :--- |
| Reachable URL | `http://gitea:3000` (compose network) or `http://gitea-http.<ns>.svc:3000` (Kubernetes), or your HTTPS ingress. TCP **3000** (or 443). |
| SSH | **Not needed** for the studio (HTTPS + token). Only publish SSH (2222) if people push over SSH. |
| Internet egress | None. |
| Account | A dedicated bot user (e.g. `dkw-bot`) with **write** access to the dbt project repository. |
| Token | A personal access token for that bot with scope `write:repository` (plus `write:issue` if pull requests are enabled later). Give it to the studio as a secret, never in a file in the repo. |
| Branch protection | Optional, recommended in production: protect `main`, let the bot push branches and open pull requests. |

## Do I need to expose a port on 0.0.0.0?

Not for the studio. Between containers or pods use the internal service name; nothing has to be published to the host.
Publish ports only for **people**:

* Web UI: `127.0.0.1:3000:3000` (only this machine) is enough for a single admin; use a TLS reverse proxy or ingress if others
  need it. Bind `0.0.0.0` only behind a firewall or a proxy, never plain-HTTP on an open network (tokens and passwords cross it).
* SSH: `2222:22` only if people push over SSH.

## Development with Docker Compose

`docker-compose.yml` in this directory runs a rootless Gitea with SQLite that joins the studio's compose network:

```bash
docker compose up -d                                   # in the repo root: creates the studio network (datakilnworks_default)
docker compose -f deploy/gitea/docker-compose.yml up -d
docker exec gitea gitea admin user create --admin --username gitea-admin --password '<strong password>' \
    --email admin@example.internal --must-change-password=false
```

The studio then uses `http://gitea:3000/<owner>/<repo>.git` (no port needs publishing for that). The web UI is published on
`127.0.0.1:3000` only; the header of the file lists the settings (bind address, port, root URL, optional SSH). The file was
verified end to end: healthy, sign-in required, an access token creates a repository, and a second container on the network
cloned and pushed over HTTP with the token.

## Kubernetes

`values.yaml` (values for the official chart) and `networkpolicy.yaml` are provided as a starting point; see the comments
in them. Untested here (no cluster available): run `helm template` first.
