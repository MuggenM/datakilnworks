#!/usr/bin/env python3
"""Turns the tail of a job's output into a GitHub Actions error annotation, so the reason for a failure is visible in the checks UI and API without opening the
log:   ci/annotate.py <output file> "<title>"        (used by the workflow's `if: failure()` steps)"""
import sys

path, title = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "CI failure")
try:
    lines = open(path, errors="replace").read().splitlines()
except OSError:
    lines = ["(no output was captured)"]
keep = [l for l in lines if l.strip() and not l.startswith(("INFO", "WARNING", "DEBUG"))]
# the test's own [FAIL] lines first: diagnostics dumped after a failure (kubectl "Error from server ...") must not crowd them out.
# "[FAIL] |" lines are a test's diagnostic dump (timelines, open requests): they get annotations of their own below, so a long
# dump neither pushes the failing checks out of the first annotation nor gets cut off after 12 lines itself.
diag = [l for l in keep if "[FAIL] |" in l]
fails = [l for l in keep if "[FAIL]" in l and "[FAIL] |" not in l] + [l for l in keep if "[FAIL]" not in l and ("Traceback" in l or "Error" in l or "FAILED" in l)]
tail = (fails[:12] + ["..."] + [l for l in keep[-25:] if "[FAIL] |" not in l]) if fails else [l for l in keep[-40:] if "[FAIL] |" not in l]


def emit(t, body_lines):
    msg = "\n".join(l[:400] for l in body_lines)[:3800]
    msg = msg.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    t = t.replace("%", "%25").replace(",", "%2C").replace(":", "%3A")
    print(f"::error title={t}::{msg}")


emit(title, tail)
chunk, size, parts = [], 0, 0
for line in diag:                                  # at most 3 more annotations of diagnostics (a step may carry 10)
    if size + len(line[:400]) + 1 > 3800 and chunk:
        parts += 1
        emit(f"{title} (diagnostics {parts})", chunk)
        chunk, size = [], 0
        if parts == 3:
            break
    chunk.append(line); size += len(line[:400]) + 1
else:
    if chunk:
        emit(f"{title} (diagnostics {parts + 1})", chunk)
