#!/usr/bin/env python3
"""
Coverage lint for tag-based masking: no place that runs SQL on a DuckDB connection may bypass the governance gateway.

Scans web/*.py with the AST for `.sql(...)` calls and `.execute(...)` on connection-like receivers. A site is fine when
its enclosing function chain calls the gateway ("governed") or it is listed in web/governance/ALLOWLIST.md with a
reason. The test fails on (a) unreviewed sites and (b) allowlist rows that no longer match any code (stale).
It also checks a few structural guarantees that the runtime tests rely on.
"""

import ast
import os
import re
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
WEB = os.path.join(BASE_DIR, "web")
ALLOWLIST = os.path.join(WEB, "governance", "ALLOWLIST.md")

GOVERNED = re.compile(r"govern_sql|governed_sql_or_raise|_gov_or_403|masked_relation|mask_arrow|enforce\.govern|gov_gateway|"
                      r"deny_if_subject|_masked_df|_masked\(")
RECEIVER = re.compile(r"^(raw_conn|duck_conn|_?duckrun_conn|_?worker_conn|test_conn|target_conn|conn|con|cur|cursor|c|self\.con|self\.conn|_conn)$")
STATUSES = {"governed", "sqlite", "metadata", "setup", "ddl", "uploaded-file", "executor", "admin-operation",
            "guarded-at-endpoint", "system", "volume-files"}
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def scan():
    """{(file, function): {"governed": bool, "calls": n}} for every DuckDB-ish execution site outside web/governance."""
    sites = {}
    for fn in sorted(os.listdir(WEB)):
        if not fn.endswith(".py"):
            continue
        src = open(os.path.join(WEB, fn)).read()
        if not re.search(r"duckdb|duckrun", src):
            continue
        tree = ast.parse(src)

        def walk(node, stack):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    walk(child, stack + [child])
                    continue
                if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute) and child.func.attr in ("sql", "execute", "executemany"):
                    recv = ast.unparse(child.func.value)
                    if child.func.attr == "sql" or RECEIVER.match(recv) or ".cursor()" in recv or recv.endswith(".con"):
                        name = stack[-1].name if stack else "<module>"
                        governed = any(GOVERNED.search(ast.unparse(f)) for f in stack)
                        entry = sites.setdefault((fn, name), {"governed": governed, "calls": 0})
                        entry["calls"] += 1
                walk(child, stack)
        walk(tree, [])
    return sites


def load_allowlist():
    rows = {}
    for line in open(ALLOWLIST):
        m = re.match(r"^\|\s*([\w.]+::[\w*<>]+)\s*\|\s*([\w-]+)\s*\|\s*(.+?)\s*\|\s*$", line)
        if m:
            rows[m.group(1)] = (m.group(2), m.group(3))
    return rows


def main():
    print("\n1. Every DuckDB execution site is governed or reviewed")
    sites = scan()
    allow = load_allowlist()
    check("allowlist parsed", len(allow) > 20, len(allow))
    check("allowlist statuses are valid", all(v[0] in STATUSES for v in allow.values()), {k: v for k, v in allow.items() if v[0] not in STATUSES})
    check("every allowlist row has a reason", all(len(v[1]) > 8 for v in allow.values()))

    unreviewed = []
    used = set()
    for (fn, func), info in sorted(sites.items()):
        exact, wild = f"{fn}::{func}", f"{fn}::*"
        if exact in allow:
            used.add(exact)
        elif wild in allow:
            used.add(wild)
        elif not info["governed"]:
            unreviewed.append(exact)
    check("no unreviewed ungoverned execution site", not unreviewed, unreviewed)
    print(f"      ({len(sites)} sites scanned: {sum(1 for s in sites.values() if s['governed'])} governed, {len(used)} allowlisted)")
    stale = [k for k in allow if k not in used and not k.endswith("::*")]
    check("no stale allowlist rows (each row still matches code)", not stale, stale)
    stale_wild = [k for k in allow if k.endswith("::*") and k not in used]
    check("no stale wildcard rows", not stale_wild, stale_wild)

    print("\n2. Structural guarantees")
    app_src = open(os.path.join(WEB, "app.py")).read()
    check("the SQL editor route runs the gateway before dispatching", "gov_gateway.govern_sql, query, current_user" in app_src
          and app_src.index("gov_gateway.govern_sql, query, current_user") < app_src.index('"query": run_sql'))
    check("dispatch paths use the rewritten SQL, not the raw text",
          "cur.sql(run_sql)" in app_src and "conn.sql(run_sql)" in app_src and 'ray_manager.execute_query(wh["id"], run_sql)' in app_src)
    check("the raw query is never sent to a worker", re.search(r'json=\{\s*"query": run_sql,', app_src) is not None
          and re.search(r'json=\{\s*"query": query,', app_src) is None)
    dash = open(os.path.join(WEB, "dashboards.py")).read()
    check("dashboard result cache is keyed by mask fingerprint", "mask_fingerprint" in dash and "gateway.fingerprint(gov)" in dash)
    check("dashboard queries default to least privilege", "principal=None" in dash)
    wf = open(os.path.join(WEB, "workflow.py")).read()
    check("jobs run as their owner", "def job_principal" in wf and "principal = principal or job_principal(job)" in wf)
    check("no endpoint defaults to admin on auth errors", not re.search(r'username = "admin"\n\s*is_admin = True\n\s*try:', app_src))
    gw = open(os.path.join(WEB, "governance", "gateway.py")).read()
    check("LLM context principal matches no exemption role", 'role="llm-context"' in gw)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("Governance coverage lint passed.")


if __name__ == "__main__":
    main()
