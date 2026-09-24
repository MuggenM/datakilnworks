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

## Development with Docker Compose (you run it yourself)

```yaml
services:
  gitea:
    image: gitea/gitea:1.22-rootless
    restart: unless-stopped
    environment:
      - GITEA__server__ROOT_URL=http://localhost:3000/     # what browsers use; the studio does not care
      - GITEA__service__DISABLE_REGISTRATION=true
    volumes:
      - gitea-data:/var/lib/gitea
      - gitea-config:/etc/gitea
    ports:
      - "127.0.0.1:3000:3000"
    networks: [default]          # put it on the same network as the studio (or use the studio's project network)
volumes: {gitea-data: {}, gitea-config: {}}
```

The studio then uses `http://gitea:3000/<owner>/<repo>.git` when both are on the same Docker network.

## Kubernetes

`values.yaml` (values for the official chart) and `networkpolicy.yaml` are provided as a starting point; see the comments
in them. Untested here (no cluster available): run `helm template` first.
