#!/usr/bin/env bash
# Prints the versions that tests depend on most, at the start of a CI job, so a version drift is visible in the log:
#   ci/print_versions.sh            the studio image's packages (sqlglot, duckdb, deltalake, ...)
#   ci/print_versions.sh --browser  also Playwright and the Chromium it drives (host Python: $PYTHON, default python3)
# IMAGE / CI_IMAGE: the image to inspect (default localspark-lakehouse-notebook). Never fails the job.
set -u
cd "$(dirname "$0")/.."
image="${CI_IMAGE:-${IMAGE:-localspark-lakehouse-notebook}}"
echo "== versions (image ${image})"
docker run --rm "$image" python -c '
import importlib.metadata as m, sys
print("  python", sys.version.split()[0])
for p in ("sqlglot", "sqlframe", "duckdb", "deltalake", "duckrun", "dbt-core", "pyarrow", "fastapi", "starlette", "webauthn"):
    try: print(f"  {p} {m.version(p)}")
    except m.PackageNotFoundError: print(f"  {p} (not installed)")
' 2>&1 || echo "  (could not inspect the image)"
if [ "${1:-}" = "--browser" ]; then
  "${PYTHON:-python3}" - <<'EOF' 2>&1 || echo "  (could not start the browser)"
import importlib.metadata as m
from playwright.sync_api import sync_playwright
print(f"  playwright {m.version('playwright')}")
with sync_playwright() as p:
    b = p.chromium.launch(); print(f"  chromium {b.version}"); b.close()
EOF
fi
exit 0
