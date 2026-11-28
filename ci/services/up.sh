#!/usr/bin/env bash
# Starts the named profiles of the test services stack from EMPTY volumes and waits until every service is healthy and
# every seed has finished:   ci/services/up.sh ldap [git kafka ...]
# Env: CI_SERVICES_TIMEOUT (seconds, default 180), CI_SERVICES_EXTRA_COMPOSE (an extra compose file, e.g. an override),
# CI_SERVICES_PUBLISH (below).
# Refuses while the stack is already up (lock file out/.lock; ci/services/down.sh removes it). Never prints out/*.env.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
[ $# -gt 0 ] || { echo "usage: ci/services/up.sh <profile>..." >&2; exit 2; }
timeout="${CI_SERVICES_TIMEOUT:-180}"
files=(-f "$here/docker-compose.yml")
[ -n "${CI_SERVICES_EXTRA_COMPOSE:-}" ] && files+=(-f "$CI_SERVICES_EXTRA_COMPOSE")
profiles=(); for p in "$@"; do profiles+=(--profile "$p"); done
# CI_SERVICES_PUBLISH="<service>:<host port>:<container port> ...": publish those ports on 127.0.0.1 ONLY (opt-in, for a browser
# on the host, e.g. keycloak:8180:8080). Each also exports DKWCI_PUBLISHED_<SERVICE>=http://localhost:<host port>, which a
# service's configuration may use for the URLs it hands out (Keycloak's KC_HOSTNAME). Checked before anything is touched.
publish=""
for spec in ${CI_SERVICES_PUBLISH:-}; do
  [[ "$spec" =~ ^([a-z0-9-]+):([0-9]{2,5}):([0-9]{2,5})$ ]] || { echo "bad CI_SERVICES_PUBLISH entry: $spec" >&2; exit 2; }
  publish+="$(printf '  %s:\n    ports: ["127.0.0.1:%s:%s"]' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}")"$'\n'
  export "DKWCI_PUBLISHED_$(echo "${BASH_REMATCH[1]}" | tr 'a-z-' 'A-Z_')=http://localhost:${BASH_REMATCH[2]}"
done

mkdir -p "$here/out"
if ! (set -o noclobber; echo "$$" > "$here/out/.lock") 2>/dev/null; then
  echo "dkwci is already running (lock $here/out/.lock); stop it with ci/services/down.sh" >&2
  exit 1
fi
rm -f "$here/out/publish.yml"
if [ -n "$publish" ]; then
  printf 'services:\n%s' "$publish" > "$here/out/publish.yml"
  files+=(-f "$here/out/publish.yml")
fi
dc() { docker compose -p dkwci "${files[@]}" "${profiles[@]}" "$@"; }


dc down -v --remove-orphans >/dev/null 2>&1 || true        # never reuse a volume a crashed run left behind
seeds=($(dc config --services | grep -- '-seed$' || true))
for s in "${seeds[@]}"; do rm -f "$here/out/${s%-seed}.env"; done

fail() {
  echo "services did not come up: $1" >&2
  for s in $(dc config --services); do
    echo "---- $s (last 100 lines)" >&2
    dc logs --no-color --tail 100 "$s" >&2 2>/dev/null || true
  done
  exit 1
}
dc up -d --wait --wait-timeout "$timeout" $(dc config --services | grep -v -- '-seed$') >/dev/null 2>&1 \
  || fail "a service is not healthy after ${timeout} s"
for s in "${seeds[@]}"; do
  dc up -d "$s" >/dev/null 2>&1 || fail "seed $s could not start"
  cid="$(dc ps -a -q "$s")"; [ -n "$cid" ] || fail "seed $s has no container"
  # `docker wait`, not `docker compose wait`: older Compose releases wait only for RUNNING containers and fail when a quick
  # seed has already exited (seen on GitHub's runners with the one-line azure seed)
  st=0; code="$(timeout "$timeout" docker wait "$cid" 2>&1)" || st=$?
  [ "$st" -ne 124 ] || fail "seed $s did not finish in ${timeout} s"
  [ "$st" -eq 0 ] || fail "could not wait for seed $s: $code"
  [ "$code" = 0 ] || fail "seed $s exited with $code ($(docker inspect -f '{{.State.Status}} {{.State.Error}}' "$cid" 2>&1))"
done
echo "dkwci up: $*"
