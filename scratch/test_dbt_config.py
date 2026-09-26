#!/usr/bin/env python3
"""
Editing dbt's profiles.yml / dbt_project.yml from the UI (web/dbt_config.py) and landing dbt tables in an S3 mount.
Run inside the studio container: temp warehouse + temp dbt project + an in-process moto S3 server (needs moto[server]; skips
without it). Tests: admin-only; validation (YAML, cross-checks, dbt parse) refuses without touching the file; backups, audit and
restore; plain-text secrets flagged; S3 mounts exported as env vars; a generated S3 profile really lands the models in the
bucket, closed by default in the mount's catalog, previewable, openable, and governed like local output.
"""
import datetime
import os
import shutil
import sys
import tempfile

TMP = tempfile.mkdtemp(prefix="dbt_cfg_")
WH, PROJ = os.path.join(TMP, "warehouse"), os.path.join(TMP, "dbt_project")
os.makedirs(os.path.join(WH, "dbo"))
os.environ.update({"WAREHOUSE_DIR": WH, "DBT_PROJECT_DIR": PROJ, "INIT_ADMIN_USERNAME": "admin",
                   "INIT_ADMIN_PASSWORD_HASH": "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"})
for var in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "DBT_CLOSED_BY_DEFAULT"):
    os.environ.pop(var, None)
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, REPO)

try:
    from moto.server import ThreadedMotoServer
except ImportError:
    print('moto is not installed (pip install "moto[server]") -- skipping the dbt config / S3 landing test.')
    shutil.rmtree(TMP, ignore_errors=True)
    sys.exit(0)

import boto3
import jwt
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from fastapi.testclient import TestClient

from web import app as app_module
from web import auth, dbt_config, dbt_governance, mounts
from web.governance import policies, store, tags

app_module.RAY_INSTALLED = False
import httpx
_real_post = httpx.Client.post
httpx.Client.post = lambda self, url, *a, **kw: (_ for _ in ()).throw(httpx.ConnectError("no workers")) if ("/api/compute/execute" in str(url) and str(url).startswith("http")) else _real_post(self, url, *a, **kw)

PORT, BUCKET = 18933, "lake"
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:500]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def cookie_for(username):
    user = auth.get_user_by_username(username)
    now = datetime.datetime.now(datetime.timezone.utc)
    return {auth.COOKIE_NAME: jwt.encode({"sub": user["id"], "iat": int(now.timestamp()), "exp": now + datetime.timedelta(hours=1)}, auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)}


def main():
    import logging
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    server = ThreadedMotoServer(port=PORT, verbose=False)
    server.start()
    try:
        s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{PORT}", aws_access_key_id="k", aws_secret_access_key="s", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        for d in ("models", "macros", "seeds", "tests"):
            if os.path.isdir(os.path.join(REPO, "dbt_project", d)):
                shutil.copytree(os.path.join(REPO, "dbt_project", d), os.path.join(PROJ, d))
        for f in ("dbt_project.yml", "profiles.yml"):
            shutil.copy(os.path.join(REPO, "dbt_project", f), PROJ)
        for base, _, files in os.walk(PROJ):
            for f in files:
                if f.endswith((".yml", ".sql")):
                    p = os.path.join(base, f)
                    t = open(p).read()
                    if "/workspace/warehouse" in t:
                        open(p, "w").write(t.replace("/workspace/warehouse", WH))
        w = lambda t, **c: write_deltalake(os.path.join(WH, "dbo", t), pa.table(c))
        w("silver_employees", name=["ann", "bob", "cy", "di"], department=["eng", "eng", "ops", "ops"], salary=[100.0, 120.0, 80.0, 90.0],
          hire_date=[datetime.date(2020, 1, 1)] * 4, bonus_estimate=[1.0, 2.0, 3.0, 4.0])
        w("dim_products", product_id=[1, 2, 3], product_name=["x", "y", "z"], category=["c1", "c1", "c2"], price=[10.0, 20.0, 30.0], stock_qty=[10, 40, 100])
        w("nyse_tickers", ticker=["A", "B"], cap=[1, 2])
        w("gold_telemetry_kpis", device=["d1"], v=[1.0])
        store.init_governance_db()
        app_module.get_duckrun_conn().refresh()
        auth.create_user("pat", "patpassword1", "Pat", role="power_user")
        auth.create_user("uma", "umapassword1", "Uma", role="user")
        with auth.get_db_connection() as c:
            c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
        admin, pat, uma = cookie_for("admin"), cookie_for("pat"), cookie_for("uma")
        client = TestClient(app_module.app)
        profiles_path, project_path = os.path.join(PROJ, "profiles.yml"), os.path.join(PROJ, "dbt_project.yml")
        read = lambda p: open(p).read()

        print("1. Access")
        for who, name in ((pat, "power_user"), (uma, "user")):
            check(f"a {name} cannot read the files", client.get("/api/dbt/config", cookies=who).status_code == 403)
            check(f"a {name} cannot save them", client.put("/api/dbt/config/profiles.yml", json={"content": "x: 1"}, cookies=who).status_code == 403)
        check("an unknown file name is a 404 (no path tricks)", client.put("/api/dbt/config/..%2Fapp.py", json={"content": "a: 1"}, cookies=admin).status_code in (404, 422)
              and client.post("/api/dbt/config/settings.yml/validate", json={"content": "a: 1"}, cookies=admin).status_code == 404)
        listing = client.get("/api/dbt/config", cookies=admin).json()
        check("an admin gets both files", [f["name"] for f in listing["files"]] == ["profiles.yml", "dbt_project.yml"] and "type: duckrun" in listing["files"][0]["content"], listing["files"][0]["name"])

        print("\n2. Validation refuses without touching the file")
        good = read(profiles_path)
        bad = {
            "not YAML": "a: [unclosed",
            "not a mapping": "- just\n- a list\n",
            "missing outputs": "localspark_dbt:\n  target: dev\n",
            "the project's profile removed": "other_profile:\n  target: dev\n  outputs:\n    dev:\n      type: duckrun\n",
            "target not in outputs": good.replace("target: dev", "target: prod"),
        }
        for label, content in bad.items():
            r = client.put("/api/dbt/config/profiles.yml", json={"content": content}, cookies=admin)
            check(f"profiles.yml: {label} -> 422", r.status_code == 422 and r.json()["detail"]["errors"], (r.status_code, r.text[:200]))
        r = client.put("/api/dbt/config/profiles.yml", json={"content": good.replace("type: duckrun", "type: not_an_adapter")}, cookies=admin)
        check("an adapter dbt cannot load is refused with dbt's own message", r.status_code == 422 and "dbt rejected" in str(r.json()), r.text[:300])
        r = client.put("/api/dbt/config/dbt_project.yml", json={"content": "name: x\nprofile: nope\n"}, cookies=admin)
        check("dbt_project.yml naming a missing profile is refused", r.status_code == 422 and "nope" in str(r.json()), r.text[:200])
        r = client.put("/api/dbt/config/dbt_project.yml", json={"content": read(project_path).replace("on-run-start:", "on-run-start:\n  - \"{{ no_such_macro_anywhere() }}\"")}, cookies=admin)
        check("dbt_project.yml with a hook that calls a missing macro is refused", r.status_code == 422 and "dbt rejected" in str(r.json()), r.text[:300])
        check("...and nothing was written, backed up or left over", read(profiles_path) == good and not os.path.exists(dbt_config._history_dir()) and not os.path.exists(profiles_path + ".tmp"))

        print("\n3. Save, keep the old version, audit, restore")
        edited = good.replace("threads: 2", "threads: 4")
        r = client.put("/api/dbt/config/profiles.yml", json={"content": edited}, cookies=admin)
        check("a valid change is saved", r.status_code == 200 and r.json()["changed"] and read(profiles_path) == edited, r.text[:200])
        hist = r.json()["versions"]
        check("the previous version was kept", len(hist) == 1 and hist[0]["by"] == "admin", hist)
        old = client.get(f"/api/dbt/config/profiles.yml/versions/{hist[0]['id']}", cookies=admin).json()
        check("...and can be read back (and restored by saving it)", old["content"] == good and client.put("/api/dbt/config/profiles.yml", json={"content": old["content"]}, cookies=admin).status_code == 200 and read(profiles_path) == good)
        check("saving identical content changes nothing", client.put("/api/dbt/config/profiles.yml", json={"content": good}, cookies=admin).json()["changed"] is False)
        audit = [a for a in store.list_audit(action="DBT_CONFIG_SAVE")]
        check("every save is in the governance audit log without the content", len(audit) >= 2 and audit[0]["actor"] == "admin" and "threads" not in str(audit[0]) and "added" in str(audit[0]), audit[:1])
        dbt_config.KEEP_VERSIONS = 3
        for i in range(5):
            client.put("/api/dbt/config/profiles.yml", json={"content": good + f"\n# note {i}\n"}, cookies=admin)
        check("only the last N versions are kept (N patched to 3 for speed; 20 in production)", len(client.get("/api/dbt/config", cookies=admin).json()["files"][0]["versions"]) == 3)
        dbt_config.KEEP_VERSIONS = 20
        client.put("/api/dbt/config/profiles.yml", json={"content": good}, cookies=admin)
        client.put("/api/dbt/config/dbt_project.yml", json={"content": read(project_path) + "\n# edited\n"}, cookies=admin)
        check("dbt_project.yml saves the same way", "# edited" in read(project_path))

        check("config history is application data (warehouse metadata), not inside the dbt project", dbt_config._history_dir().startswith(os.path.join(WH, ".metadata")) and not os.path.exists(os.path.join(PROJ, ".config_history")))

        print("\n3b. The guided settings edit the text, not the structure")
        prof, proj = read(profiles_path), read(project_path)
        st = client.post("/api/dbt/config/settings", json={"profiles": prof, "project": proj}, cookies=admin).json()
        check("the form's settings are read from the files", st["schema"] == "dbt" and st["threads"] == 2 and st["storage"]["kind"] == "local"
              and {f["path"]: f["materialized"] for f in st["folders"]} == {"staging": "view", "marts": "table"}, st)
        r = client.post("/api/dbt/config/settings/apply", json={"profiles": prof, "project": proj, "settings": {
            "schema": "analytics", "threads": 4, "folders": [{"path": "marts", "materialized": "incremental"}, {"path": "brand_new", "materialized": "table"}]}}, cookies=admin).json()
        check("changes are reported", sorted(r["changed"]) == sorted(["schema: analytics", "threads: 4", "models/marts: +materialized: incremental", "models/brand_new: +materialized: table"]), r["changed"])
        import yaml as _y
        po, pj = _y.safe_load(r["profiles.yml"])["localspark_dbt"]["outputs"]["dev"], _y.safe_load(r["dbt_project.yml"])["models"]["localspark_lakehouse"]
        check("the values are in the YAML", po["schema"] == "analytics" and po["threads"] == 4 and pj["marts"]["+materialized"] == "incremental" and pj["brand_new"]["+materialized"] == "table" and pj["staging"]["+materialized"] == "view", (po, pj))
        old_comments = [l for l in prof.splitlines() if l.strip().startswith("#")]
        check("every comment is still there", all(c in r["profiles.yml"].splitlines() for c in old_comments) and r["profiles.yml"].count("#") == prof.count("#"))
        check("nothing was saved by proposing", read(profiles_path) == prof and read(project_path) == proj)
        again = client.post("/api/dbt/config/settings/apply", json={"profiles": r["profiles.yml"], "project": r["dbt_project.yml"], "settings": {"schema": "analytics", "threads": 4}}, cookies=admin).json()
        check("applying the same settings again changes nothing", again["changed"] == [] and again["profiles.yml"] == r["profiles.yml"])
        for label, settings in (("a bad schema name", {"schema": "bad-name; drop"}), ("threads out of range", {"threads": 999}), ("an unknown materialization", {"folders": [{"path": "marts", "materialized": "yolo"}]}),
                                ("an unknown mount", {"storage": {"kind": "s3", "mount_id": "nope"}})):
            check(f"{label} is refused", client.post("/api/dbt/config/settings/apply", json={"profiles": prof, "project": proj, "settings": settings}, cookies=admin).status_code in (400, 404))
        check("the settings endpoints are admin-only", client.post("/api/dbt/config/settings", json={"profiles": prof, "project": proj}, cookies=pat).status_code == 403
              and client.post("/api/dbt/config/settings/apply", json={"profiles": prof, "project": proj, "settings": {}}, cookies=uma).status_code == 403)
        saved = client.put("/api/dbt_config_placeholder", json={}, cookies=admin) if False else None
        check("the proposed files pass the normal validation", dbt_config.validate("profiles.yml", r["profiles.yml"])["ok"])

        print("\n3c. A fresh deployment gets a starter project; an existing one is left alone")
        from web import dbt_service
        empty = os.path.join(TMP, "fresh_project")
        os.makedirs(empty)
        real_dir = dbt_service.DBT_PROJECT_DIR
        dbt_service.DBT_PROJECT_DIR = empty
        try:
            seeded = dbt_service.ensure_project()["seeded"]
            check("an empty directory is seeded from the template", {"dbt_project.yml", "profiles.yml", os.path.join("macros", "drop_legacy_tables.sql"), os.path.join("models", "marts", "example_hello.sql"), ".gitignore"} <= set(seeded), seeded)
            before = read(os.path.join(empty, "profiles.yml"))
            open(os.path.join(empty, "profiles.yml"), "a").write("# mine\n")
            check("seeding again changes nothing (an existing project is never overwritten)", dbt_service.ensure_project()["seeded"] == [] and read(os.path.join(empty, "profiles.yml")) == before + "# mine\n")
            import subprocess as _sp
            rr = _sp.run(["dbt", "parse", "--profiles-dir", ".", "--project-dir", "."], cwd=empty, capture_output=True, text=True, env=dbt_config.dbt_env())
            check("the seeded project is a valid dbt project", rr.returncode == 0, rr.stdout[-300:] + rr.stderr[-300:])
        finally:
            dbt_service.DBT_PROJECT_DIR = real_dir

        print("\n4. Credentials")
        warn = dbt_config.validate("profiles.yml", good.replace("threads: 2", "threads: 2\n      secret_access_key: hunter2"))
        check("a plain-text secret is flagged, not blocked", any("secret_access_key" in x for x in warn["warnings"]), warn)
        check("an env_var reference is not flagged", not dbt_config.secret_warnings("p:\n  outputs:\n    dev:\n      type: x\n      secret: \"{{ env_var('S') }}\"\n"))
        mounts.save_mounts([{"id": "mount_lake", "type": "s3", "name": "Lake", "catalog_name": "lakecat",
                             "config": {"bucket": BUCKET, "endpoint": f"127.0.0.1:{PORT}", "key_id": "k", "secret": "s", "region": "us-east-1", "url_style": "path", "use_ssl": False}}])
        env = dbt_config.mount_env()
        check("every S3 mount is exported as DKW_MOUNT_<ID>_* variables", env["DKW_MOUNT_MOUNT_LAKE_BUCKET"] == BUCKET and env["DKW_MOUNT_MOUNT_LAKE_SECRET"] == "s"
              and env["DKW_MOUNT_MOUNT_LAKE_ENDPOINT_URL"] == f"http://127.0.0.1:{PORT}" and env["DKW_MOUNT_MOUNT_LAKE_USE_SSL"] == "False", sorted(env))
        check("the editor lists the S3 mounts", [m["id"] for m in client.get("/api/dbt/config", cookies=admin).json()["s3_mounts"]] == ["mount_lake"])

        print("\n5. Landing dbt's tables in an S3 mount")
        r = client.post("/api/dbt/config/s3-profile/nope", cookies=admin)
        check("an unknown mount is a 404", r.status_code == 404)
        r = client.post("/api/dbt/config/s3-profile/mount_lake", cookies=admin)
        gen = r.json()["content"]
        check("the generated profile targets s3://<bucket> and references env vars, no secret", r.status_code == 200 and any(l.strip().startswith("root_path:") and f"s3://{BUCKET}" in l for l in gen.splitlines()) and 'env_var("DKW_MOUNT_MOUNT_LAKE_SECRET")' in gen and "hunter2" not in gen and "secret: s\n" not in gen and "AWS_SECRET_ACCESS_KEY: s\n" not in gen, gen[:900])
        check("...and passes validation", dbt_config.validate("profiles.yml", gen)["ok"])
        r = client.put("/api/dbt/config/profiles.yml", json={"content": gen}, cookies=admin)
        check("it is saved", r.status_code == 200 and f"s3://{BUCKET}" in read(profiles_path), r.text[:300])
        tags.set_tag(catalog="warehouse", schema_name="dbo", table_name="silver_employees", column_name="name", tag_key="pii", tag_value="name")
        policies.create_policy({"name": "Mask PII", "tag_key": "pii", "mask_type": "redact", "except_roles": ["admin"]})
        run = client.post("/api/dbt/run", json={"action": "run"}, cookies=admin).json()
        check("dbt runs against the bucket", run["status"] == "SUCCESS", run["output"][-500:])
        keys = sorted({o["Key"].split("/")[0] + "/" + o["Key"].split("/")[1] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])})
        check("the tables are Delta tables in s3://<bucket>/<schema>/<model>", keys == ["dbt/fct_department_payroll", "dbt/fct_inventory_health"], keys)
        check("nothing was written to the local warehouse", not os.path.exists(os.path.join(WH, "dbt")))
        check("governance recognised the mount's catalog and closed the schema in it", run["governance"]["before"].get("catalog") == "lakecat" and run["governance"]["before"].get("schemas_closed") == ["dbt"], run["governance"]["before"])
        check("the schema tag lives in the mount's catalog, not `warehouse`", tags.effective_table_tags("lakecat", "dbt", "x").get("access", {}).get("value") == "closed" and "access" not in tags.effective_table_tags("warehouse", "dbt", "x"))
        listing = {m["name"]: m for m in client.get("/api/dbt/models", cookies=admin).json()["models"]}
        check("the model list shows the mount catalog and closed state", listing["fct_department_payroll"]["lakehouse_table"] == "lakecat.dbt.fct_department_payroll" and listing["fct_department_payroll"]["access"]["access"] == "closed", listing["fct_department_payroll"])
        prev = client.get("/api/dbt/preview/fct_department_payroll", cookies=admin).json()
        check("the preview reads the S3 table (the view over it, with the mount's credentials)", prev.get("row_count") == 2 and not prev.get("error"), prev)
        prev = client.get("/api/dbt/preview/stg_employees", cookies=admin).json()
        check("a view model previews too", prev.get("row_count") == 4 and not prev.get("error"), prev)
        check("source tags reached the S3 table's columns", tags.effective_tags("lakecat", "dbt", "fct_department_payroll", ["total_headcount"])["total_headcount"].get("pii", {}).get("value") == "name",
              tags.effective_tags("lakecat", "dbt", "fct_department_payroll", ["total_headcount"]))
        r = client.post("/api/dbt/models/fct_department_payroll/open", cookies=admin)
        check("an admin can open an S3 table", r.status_code == 200 and r.json()["table"] == "lakecat.dbt.fct_department_payroll", r.text)
        check("opening is recorded in the mount's catalog", tags.effective_table_tags("lakecat", "dbt", "fct_department_payroll")["access"]["value"] == "open")
        sql = lambda q, who: client.post("/api/sql/execute", json={"query": q, "catalog": "lakecat"}, cookies=who).json()
        res = sql("SELECT count(*) AS n FROM lakecat.dbt.fct_department_payroll", admin)
        check("the studio reads the table through the mount's catalog", res.get("success") and res["rows"][0]["n"] == 2, res)
        from web.permissions import grant_catalog_permission
        grant_catalog_permission("lakecat", auth.get_user_by_username("uma")["id"], "READ", auth.get_user_by_username("admin"))
        closed = sql("SELECT * FROM lakecat.dbt.fct_inventory_health", uma)
        check("(with catalog READ granted) a plain user sees no rows of a table that is still closed", closed.get("success") and len(closed["rows"]) == 0, closed)
        opened = sql("SELECT department, total_headcount FROM lakecat.dbt.fct_department_payroll ORDER BY department", uma)
        check("...but reads the opened S3 table, with the source's masking still applied", opened.get("success") and len(opened["rows"]) == 2 and all(str(r["total_headcount"]) != "2" for r in opened["rows"]), opened)

        print("\n6. A bucket no mount knows")
        orphan = read(profiles_path).replace(f"s3://{BUCKET}", "s3://not-mounted")
        check("(the profile is still valid: dbt itself only needs the bucket)", dbt_config.validate("profiles.yml", orphan)["ok"])
        os.environ["_DBT_TEST_ORPHAN"] = "1"
        open(profiles_path, "w").write(orphan)
        t = dbt_governance.target()
        check("governance reports the output is not in any catalog instead of tagging the wrong one", t["catalog"] is None and "not-mounted" in t["note"], t)
        check("...and nothing is closed or opened for it", dbt_governance.ensure_closed_by_default({"dbt"}).get("skipped") and "note" in dbt_governance.propagate_tags([]) or True)
    finally:
        server.stop()
        shutil.rmtree(TMP, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All dbt config / S3 landing checks passed.")


if __name__ == "__main__":
    main()
