# Traefik in front of the studio (optional `proxy` profile)

```bash
# .env (see .env.example): the public address, then
docker compose --profile proxy up -d
```

* **What it does**: terminates TLS and forwards to the studio (`http://datakilnworks-studio:8000`). No Docker socket is given to Traefik (file
  provider only, dashboard off), the studio is not changed. Port 80 redirects to https.
* **Ports**: `TRAEFIK_HTTPS_PORT` (default 8443) and `TRAEFIK_HTTP_PORT` (default 8080); use 443 / 80 on a server where you may bind them
  (rootless Docker cannot bind below 1024 unless configured).
* **Certificate**: by default Traefik's own self-signed certificate. Your own: copy `certs.yml.example` to `dynamic/certs.yml` and put
  `fullchain.pem` / `privkey.pem` in `deploy/traefik/certs/`. Let's Encrypt: set `TRAEFIK_CERT_RESOLVER=le`, `TRAEFIK_ACME_EMAIL`, `TRAEFIK_DOMAIN`, and
  `TRAEFIK_HTTP_PORT=80` / `TRAEFIK_HTTPS_PORT=443` on a host that is reachable from the internet under that name (HTTP-01 challenge; the certificate is
  kept in the `traefik-acme` volume). The Let's Encrypt path is configured but was not exercised in the tests (it needs a public name).
* **Delta Sharing**: set `DELTA_SHARING_ENDPOINT=https://<your domain>[:port]/delta-sharing` so profiles and file links carry the public https address.
* **Client addresses**: without further setup the studio sees Traefik's address for every request, which makes the IP allowlist and per-recipient address
  rules useless. Tell it that Traefik is a trusted proxy: `docker network inspect <project>_proxy-net -f '{{(index .IPAM.Config 0).Subnet}}'` (project =
  the folder name, usually `datakilnworks`) and put that subnet into `TRUSTED_PROXIES` in `.env`. The studio then believes `X-Forwarded-For` only from that
  network (which holds nothing but the studio and Traefik) and sees the real client address. Check it under Users & IAM > Network access ("your address").
