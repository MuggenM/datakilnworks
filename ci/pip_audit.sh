#!/usr/bin/env bash
# Known vulnerabilities in the Python packages installed in the studio image (pip-audit against the image's own site-packages).
# pip-audit runs from a separate venv inside a throwaway container, so its own dependencies are not part of what is audited.
#   ci/pip_audit.sh                  IMAGE (default localspark-lakehouse-notebook), DOCKER_NETWORK (optional, e.g. a network with internet)
# Accepted findings go in ci/pip-audit-ignore.txt, one id per line with a reason after '#'. Fails on any other finding.
set -euo pipefail
cd "$(dirname "$0")/.."
image="${IMAGE:-localspark-lakehouse-notebook}"
ignores=""
while read -r id _; do
  [ -n "$id" ] && ignores="$ignores --ignore-vuln $id"
done < <(sed -e 's/#.*//' ci/pip-audit-ignore.txt | grep -v '^[[:space:]]*$' || true)
net=()
[ -n "${DOCKER_NETWORK:-}" ] && net=(--network "$DOCKER_NETWORK")
echo "== pip-audit on ${image}"
docker run --rm "${net[@]}" --entrypoint sh "$image" -c "
  python -m venv /tmp/pa >/dev/null && /tmp/pa/bin/pip install -q 'pip-audit>=2.7,<3' >/dev/null &&
  site=\$(python -c 'import sysconfig; print(sysconfig.get_paths()[\"purelib\"])') &&
  /tmp/pa/bin/pip-audit --progress-spinner off --desc --path \"\$site\"${ignores}"
