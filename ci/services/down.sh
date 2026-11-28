#!/usr/bin/env bash
# Stops and removes the whole test services stack (every profile), its network and volumes, and the lock. Idempotent.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
profiles=(); for p in $(docker compose -f "$here/docker-compose.yml" config --profiles 2>/dev/null); do profiles+=(--profile "$p"); done
docker compose -p dkwci -f "$here/docker-compose.yml" "${profiles[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf "$here/out/.lock" "$here/out/"*.env "$here/out/sftp" "$here/out/publish.yml" "$here/out/fabric"
exit 0
