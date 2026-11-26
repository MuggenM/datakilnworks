#!/usr/bin/env python3
"""
dbt with the duckrun adapter (run inside the studio container; everything happens in a temp dir, never the real warehouse).
Tests: requirements.txt lists dbt-core, jinja2 and duckrun; the shipped profile uses `type: duckrun`; dbt debug/run/test
work and dbt reports the duckrun adapter; `table` models are real Delta tables under root_path (and not visible as a
catalog schema); the studio's dbt status and previews work; upgrading a project that already ran on the plain dbt-duckdb
adapter migrates cleanly (and is a no-op afterwards); a fresh install works; the studio's own run_dbt_cli works.
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:400]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def dbt(cmd, cwd, extra_env=None):
    r = subprocess.run(["dbt", *cmd, "--profiles-dir", "."], cwd=cwd, capture_output=True, text=True, env={**os.environ, **(extra_env or {})})
    return r.returncode, r.stdout + r.stderr


def main():
    tmp = tempfile.mkdtemp(prefix="dbt_duckrun_")
    try:
        wh, proj = os.path.join(tmp, "warehouse"), os.path.join(tmp, "dbt_project")
        os.makedirs(os.path.join(wh, "dbo"))
        for d in ("models", "macros", "seeds", "tests"):
            if os.path.isdir(os.path.join(REPO, "dbt_project", d)):
                shutil.copytree(os.path.join(REPO, "dbt_project", d), os.path.join(proj, d))
        for f in ("dbt_project.yml", "profiles.yml"):
            shutil.copy(os.path.join(REPO, "dbt_project", f), proj)
        # point the project's hard-coded warehouse paths at the temp warehouse
        for base, _, files in os.walk(proj):
            for f in files:
                if f.endswith((".yml", ".sql")):
                    p = os.path.join(base, f)
                    txt = open(p).read()
                    if "/workspace/warehouse" in txt:
                        open(p, "w").write(txt.replace("/workspace/warehouse", wh))

        print("1. Requirements and profile")
        reqs = open(os.path.join(REPO, "requirements.txt")).read().lower()
        for pkg in ("dbt-core", "jinja2", "duckrun"):
            check(f"requirements.txt lists {pkg}", re.search(rf"^{pkg}\b", reqs, re.M) is not None)
        profile = open(os.path.join(proj, "profiles.yml")).read()
        check("the shipped profile uses the duckrun adapter", re.search(r"^\s*type:\s*duckrun\s*$", profile, re.M) is not None and "type: duckdb" not in profile)
        check("dbt-core, jinja2 and duckrun are importable in this image", all(subprocess.run([sys.executable, "-c", f"import {m}"]).returncode == 0 for m in ("dbt.adapters.duckrun", "jinja2", "dbt.cli.main")))

        import pyarrow as pa
        from deltalake import DeltaTable, write_deltalake
        import datetime
        src = lambda table, **cols: write_deltalake(os.path.join(wh, "dbo", table), pa.table(cols))
        src("silver_employees", name=["a", "b", "c", "d"], department=["eng", "eng", "ops", "ops"], salary=[100.0, 120.0, 80.0, 90.0],
            hire_date=[datetime.date(2020, 1, 1)] * 4, bonus_estimate=[1.0, 2.0, 3.0, 4.0])
        src("dim_products", product_id=[1, 2, 3], product_name=["x", "y", "z"], category=["c1", "c1", "c2"], price=[10.0, 20.0, 30.0], stock_qty=[10, 40, 100])
        src("nyse_tickers", ticker=["A", "B"], cap=[1, 2])
        src("gold_telemetry_kpis", device=["d1"], v=[1.0])

        print("\n2. dbt on the duckrun adapter")
        rc, out = dbt(["debug"], proj)
        check("dbt debug passes and registers the duckrun adapter", rc == 0 and "Registered adapter: duckrun" in out, out[-400:])
        rc, out = dbt(["run"], proj)
        check("dbt run succeeds", rc == 0 and "ERROR=0" in out and "Completed successfully" in out, out[-500:])
        for model in ("fct_department_payroll", "fct_inventory_health"):
            path = os.path.join(wh, "dbt", model)
            ok = os.path.isdir(os.path.join(path, "_delta_log"))
            check(f"{model} is a Delta table under root_path", ok and DeltaTable(path).to_pyarrow_table().num_rows == 2, path)
        dept = DeltaTable(os.path.join(wh, "dbt", "fct_department_payroll")).to_pyarrow_table().to_pylist()
        check("the Delta table holds the right numbers", {r["department"]: r["total_payroll"] for r in dept} == {"eng": 220.0, "ops": 170.0}, dept)
        check("dbt writes into the lakehouse: <warehouse>/<schema>/<model> with schema `dbt`", os.path.isdir(os.path.join(wh, "dbt", "fct_department_payroll", "_delta_log")))
        rc, out = dbt(["test"], proj)
        check("dbt test passes on the Delta-backed models", rc == 0 and "ERROR=0" in out, out[-400:])
        rc, out = dbt(["run"], proj)
        check("a second run works and migrates nothing", rc == 0 and "dropping legacy" not in out and "ERROR=0" in out, out[-400:])

        probe_scan = ("import sys,os;os.environ['WAREHOUSE_DIR']=%r;os.environ['INIT_ADMIN_PASSWORD_HASH']='pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d'\n"
                      "sys.path.insert(0,%r)\nfrom web import app\nprint('SCAN', sorted(t['full_name'] for t in app.scan_delta_tables()))" % (wh, REPO))
        out = subprocess.run([sys.executable, "-c", probe_scan], capture_output=True, text=True).stdout
        scan = [l for l in out.splitlines() if l.startswith("SCAN")]
        check("the catalog scan lists the source tables and the dbt output tables", bool(scan) and "dbo.silver_employees" in scan[0] and "dbt.fct_department_payroll" in scan[0], scan)

        print("\n3. The studio's dbt service")
        env = {"DBT_PROJECT_DIR": proj, "WAREHOUSE_DIR": wh}
        probe = ("import sys,json;sys.path.insert(0,%r)\nfrom web import dbt_service as d\n"
                 "s=d.get_dbt_status();p=d.preview_dbt_model_data('fct_department_payroll',5);v=d.preview_dbt_model_data('stg_employees',5)\n"
                 "r=d.run_dbt_cli('run');print(json.dumps({'s':s,'p':[p['row_count'],p['error']],'v':[v['row_count'],v['error']],'r':[r['status'],r['summary']]},default=str))" % REPO)
        r = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, env={**os.environ, **env})
        import json
        try:
            got = json.loads(r.stdout.strip().splitlines()[-1])
        except Exception:
            got = {}
            print(r.stderr[-600:])
        st = got.get("s", {})
        check("status reports the duckrun adapter and the real dbt version", st.get("uses_duckrun") is True and "duckrun" in st.get("adapter", "") and re.match(r"^\d+\.\d+", st.get("dbt_version", "")), st)
        check("a table model previews (through the view over its Delta table)", got.get("p", [0, "x"])[0] == 2 and got["p"][1] is None, got.get("p"))
        check("a view model previews", got.get("v", [0, "x"])[0] == 4 and got["v"][1] is None, got.get("v"))
        check("run_dbt_cli (the studio and workflow tasks) succeeds", got.get("r", ["", {}])[0] == "SUCCESS", got.get("r"))

        print("\n4. Upgrading a project that already ran on the plain dbt-duckdb adapter")
        shutil.rmtree(os.path.join(wh, "dbt"), ignore_errors=True)
        os.remove(os.path.join(wh, "dbt_analytics.duckdb"))
        old_profile = ("localspark_dbt:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: '%s/dbt_analytics.duckdb'\n"
                       "      extensions:\n        - httpfs\n        - delta\n      threads: 2\n" % wh)
        new_profile = open(os.path.join(proj, "profiles.yml")).read()
        open(os.path.join(proj, "profiles.yml"), "w").write(old_profile)
        rc, out = dbt(["run"], proj)
        check("(setup) the plain duckdb adapter builds real tables (schema main) in the DuckDB file", rc == 0 and "Registered adapter: duckdb" in out)
        open(os.path.join(proj, "profiles.yml"), "w").write(new_profile)
        rc, out = dbt(["run"], proj)
        check("switching to duckrun (schema dbt) works next to the leftover tables", rc == 0 and "ERROR=0" in out and os.path.isdir(os.path.join(wh, "dbt", "fct_department_payroll", "_delta_log")), out[-500:])
        # a profile that keeps dbt's default schema `main` collides with the legacy tables: the on-run-start macro migrates them
        shutil.rmtree(os.path.join(wh, "dbt"), ignore_errors=True)
        os.remove(os.path.join(wh, "dbt_analytics.duckdb"))
        open(os.path.join(proj, "profiles.yml"), "w").write(old_profile)
        dbt(["run"], proj)
        open(os.path.join(proj, "profiles.yml"), "w").write(new_profile.replace("      schema: dbt\n", ""))
        rc, out = dbt(["run"], proj)
        check("with schema `main` the legacy tables are migrated by the macro, once", rc == 0 and out.count("dropping legacy") == 2 and "ERROR=0" in out, out[-600:])
        rc, out = dbt(["run"], proj)
        check("...and the next run has nothing left to migrate", rc == 0 and "dropping legacy" not in out and "ERROR=0" in out, out[-300:])
        open(os.path.join(proj, "profiles.yml"), "w").write(new_profile)

        print("\n5. A fresh install")
        shutil.rmtree(os.path.join(wh, "dbt"), ignore_errors=True)
        shutil.rmtree(os.path.join(wh, "main"), ignore_errors=True)
        os.remove(os.path.join(wh, "dbt_analytics.duckdb"))
        rc, out = dbt(["run"], proj)
        check("no DuckDB file and no Delta output yet: dbt run builds everything", rc == 0 and "ERROR=0" in out and os.path.isdir(os.path.join(wh, "dbt", "fct_inventory_health", "_delta_log")), out[-400:])
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All dbt / duckrun checks passed.")


if __name__ == "__main__":
    main()
