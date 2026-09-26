#!/usr/bin/env bash
# Runs one self-contained test script (scratch/test_<name>.py) inside the studio image, like `docker compose exec` would, but in a throwaway container.
#   ci/run_test.sh <name> [extra pip packages]       e.g.  ci/run_test.sh delta_sharing "delta-sharing"
# CI_IMAGE            image to run (default localspark-lakehouse-notebook, built by `docker compose build` or `docker build -t ... .`)
# CI_MOUNT_SOURCES=1  bind-mount ./web and ./docs over the ones baked into the image (fast local iteration; CI tests the image as built)
set -euo pipefail
cd "$(dirname "$0")/.."
name="${1:?usage: ci/run_test.sh <test name> [pip packages]}"
extras="${2:-}"
image="${CI_IMAGE:-localspark-lakehouse-notebook}"
[ -f "scratch/test_${name}.py" ] || { echo "no such test: scratch/test_${name}.py" >&2; exit 2; }
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
mkdir -p "$work/dbt_project" && cp -a dbt_project/. "$work/dbt_project/" 2>/dev/null || true      # the dbt tests write into a project directory: give them a copy
args=(--rm -v "$PWD/scratch:/workspace/scratch" -v "$PWD/docker-compose.yml:/workspace/docker-compose.yml:ro" -v "$PWD/requirements.txt:/workspace/requirements.txt:ro" -v "$PWD/Dockerfile:/workspace/Dockerfile:ro" -v "$work/dbt_project:/workspace/dbt_project" -e PYTHONUNBUFFERED=1)
[ -n "${CI_MOUNT_SOURCES:-}" ] && args+=(-v "$PWD/web:/workspace/web" -v "$PWD/docs:/workspace/docs")
cmd="python /workspace/scratch/test_${name}.py"
[ -n "$extras" ] && cmd="pip install -q ${extras} >/dev/null 2>&1; ${cmd}"
echo "== test_${name} (image ${image})"
set +e
docker run "${args[@]}" "$image" sh -c "$cmd" > "$work/out.txt" 2>&1
status=$?
set -e
grep -v "^INFO\|^WARNING\|httpx\|^Connected to" "$work/out.txt" | tail -n 300 || true
# a test script prints [FAIL] lines / ALL PASS; its exit code is the verdict, but a crash before the summary must fail the job too
if grep -q "\[FAIL\]" "$work/out.txt" || [ "$status" -ne 0 ]; then echo "test_${name} FAILED (exit ${status})"; exit 1; fi
echo "test_${name} passed"
