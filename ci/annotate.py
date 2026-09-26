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
fails = [l for l in keep if "[FAIL]" in l or "Traceback" in l or "Error" in l or "FAILED" in l]
tail = (fails[:12] + ["..."] + keep[-25:]) if fails else keep[-40:]
msg = "\n".join(l[:400] for l in tail)[:3800]
msg = msg.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
title = title.replace("%", "%25").replace(",", "%2C").replace(":", "%3A")
print(f"::error title={title}::{msg}")
