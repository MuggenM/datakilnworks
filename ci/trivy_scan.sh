#!/usr/bin/env bash
# Known HIGH / CRITICAL vulnerabilities with an available fix in the studio image: OS packages and anything else Trivy recognises.
#   ci/trivy_scan.sh                 IMAGE (default localspark-lakehouse-notebook), DOCKER_SOCKET_PATH (default /var/run/docker.sock;
#                                    rootless Docker: /run/user/<uid>/docker.sock), DOCKER_NETWORK (optional), TRIVY_CACHE (optional dir)
# The scanner is the official image pinned by version AND digest (no third-party GitHub Action in the path). Accepted findings go in
# ci/trivyignore, one id per line with a reason after '#'.
set -euo pipefail
cd "$(dirname "$0")/.."
image="${IMAGE:-localspark-lakehouse-notebook}"
sock="${DOCKER_SOCKET_PATH:-/var/run/docker.sock}"
trivy="aquasec/trivy:0.58.1@sha256:ab70a02200597efa04748f210f793936eb647cbcdb0ea69cc30b226d6f5a22c7"
args=(--rm -v "$sock:/var/run/docker.sock" -v "$PWD/ci/trivyignore:/trivyignore:ro")
[ -n "${DOCKER_NETWORK:-}" ] && args+=(--network "$DOCKER_NETWORK")
[ -n "${TRIVY_CACHE:-}" ] && args+=(-v "$TRIVY_CACHE:/root/.cache")
echo "== trivy on ${image}"
docker run "${args[@]}" "$trivy" image --quiet --scanners vuln --severity HIGH,CRITICAL --ignore-unfixed \
  --ignorefile /trivyignore --exit-code 1 --format table "$image"
