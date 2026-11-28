#!/usr/bin/env bash
# Runs one self-contained test script (scratch/test_<name>.py) inside the studio image, like `docker compose exec` would, but in a throwaway container.
#   ci/run_test.sh <name> [extra pip packages]       e.g.  ci/run_test.sh delta_sharing "delta-sharing"
# CI_IMAGE            image to run (default localspark-lakehouse-notebook, built by `docker compose build` or `docker build -t ... .`)
# CI_MOUNT_SOURCES=1  bind-mount ./web and ./docs over the ones baked into the image (fast local iteration; CI tests the image as built)
# DOCKER_NETWORK      run on that network (locally a plain `docker run` may have no internet, and DuckDB then cannot fetch its delta extension)
# CI_SERVICES         profiles of the test services stack the test needs (ci/services, e.g. "ldap git"): started before the test (unless
#                     CI_SERVICES_UP=0: already up), the test joins their network with their env files and REQUIRE_SERVICES=1 (a missing
#                     service is a failure, not a skip), and the stack is stopped afterwards (unless CI_SERVICES_KEEP=1). Replaces DOCKER_NETWORK.
#                     ci/services/out is mounted read-only at /services.
# CI_TEST_ENV         space-separated NAME=value pairs passed to the test (they override the services' env files).
set -euo pipefail
cd "$(dirname "$0")/.."
name="${1:?usage: ci/run_test.sh <test name> [pip packages]}"
extras="${2:-}"
image="${CI_IMAGE:-localspark-lakehouse-notebook}"
[ -f "scratch/test_${name}.py" ] || { echo "no such test: scratch/test_${name}.py" >&2; exit 2; }
work="$(mktemp -d)"
services_started=""
cleanup() { rm -rf "$work"; if [ -n "$services_started" ] && [ "${CI_SERVICES_KEEP:-}" != 1 ]; then ci/services/down.sh; fi; }
trap cleanup EXIT
mkdir -p "$work/dbt_project" && cp -a dbt_project/. "$work/dbt_project/" 2>/dev/null || true      # the dbt tests write into a project directory: give them a copy
args=(--rm -v "$PWD/scratch:/workspace/scratch" -v "$PWD/docker-compose.yml:/workspace/docker-compose.yml:ro" -v "$PWD/requirements.txt:/workspace/requirements.txt:ro" -v "$PWD/Dockerfile:/workspace/Dockerfile:ro" -v "$PWD/ci:/workspace/ci:ro" -v "$work/dbt_project:/workspace/dbt_project" -e PYTHONUNBUFFERED=1)
if [ -n "${CI_SERVICES:-}" ]; then
  if [ "${CI_SERVICES_UP:-}" != 0 ]; then ci/services/up.sh $CI_SERVICES; services_started=1; fi    # a refused up (stack already running) must not stop that stack
  args+=(--network dkwci_default --dns-search . -e REQUIRE_SERVICES=1)     # no search domain: see ci/services/docker-compose.yml
  for p in $CI_SERVICES; do [ -f "ci/services/out/$p.env" ] && args+=(--env-file "ci/services/out/$p.env"); done
  args+=(-v "$PWD/ci/services/out:/services:ro")                         # generated files a seed made (the sftp key pair)
elif [ -n "${DOCKER_NETWORK:-}" ]; then
  args+=(--network "$DOCKER_NETWORK")
fi
for kv in ${CI_TEST_ENV:-}; do args+=(-e "$kv"); done                     # NAME=value overrides, after the env files
[ -n "${CI_MOUNT_SOURCES:-}" ] && args+=(-v "$PWD/web:/workspace/web" -v "$PWD/docs:/workspace/docs")
cmd="python /workspace/scratch/test_${name}.py"
[ -n "$extras" ] && cmd="pip install -q ${extras} >/dev/null 2>&1; ${cmd}"
echo "== test_${name} (image ${image})"
set +e
docker run "${args[@]}" "$image" sh -c "$cmd" > "$work/out.txt" 2>&1
status=$?
set -e
# librdkafka's own log lines (`%3|<time>|FAIL|...`, one per reconnect attempt) would push the test's lines out of the tail:
# counted, not shown (the last 5 stay)
grep -v "^INFO\|^WARNING\|httpx\|^Connected to\|^%[0-7]|" "$work/out.txt" | tail -n 300 || true
nrd="$(grep -c "^%[0-7]|" "$work/out.txt" || true)"
if [ "${nrd:-0}" -gt 0 ]; then echo "($nrd librdkafka log lines not shown; the last 5:)"; grep "^%[0-7]|" "$work/out.txt" | tail -n 5 | cut -c1-300; fi
# a test script prints [FAIL] lines / ALL PASS; its exit code is the verdict, but a crash before the summary must fail the job too
if grep -q "\[FAIL\]" "$work/out.txt" || [ "$status" -ne 0 ]; then echo "test_${name} FAILED (exit ${status})"; exit 1; fi
echo "test_${name} passed"
