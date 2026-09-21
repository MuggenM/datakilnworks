#!/usr/bin/env python3
"""
Verification for notebook access: there is no JupyterLab server, and the in-Studio notebook runner is per user.
Runs against a throwaway WAREHOUSE_DIR / NOTEBOOKS_DIR, so it never touches real data or notebooks.
Tests:
1. Every notebook endpoint needs a valid session; a bad session is 401, never admin.
2. Users only reach Users/<themselves> and Shared (admins reach everything); path tricks (`..`) are refused.
3. Kernels are per user: two users on the same Shared notebook do not share variables.
4. Users a masking policy applies to can open and edit notebooks but not run them (GOVERNANCE_NOTEBOOK_EXECUTION).
5. No Jupyter server, token or URL is exposed anywhere (API, page, compose, requirements).
"""

import datetime
import os
import re
import shutil
import sys
import tempfile

TMP_ROOT = tempfile.mkdtemp(prefix="notebook_access_")
TMP_WAREHOUSE = os.path.join(TMP_ROOT, "warehouse")
TMP_NOTEBOOKS = os.path.join(TMP_ROOT, "notebooks")
os.makedirs(TMP_WAREHOUSE)
os.makedirs(TMP_NOTEBOOKS)
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE
os.environ["NOTEBOOKS_DIR"] = TMP_NOTEBOOKS
for var in ("GOVERNANCE_REQUIRE_AUTH", "GOVERNANCE_NOTEBOOK_EXECUTION", "JWT_SECRET_KEY", "COMPUTE_TOKEN", "GOVERNANCE_ENFORCEMENT"):
    os.environ.pop(var, None)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import jwt
import nbformat
from fastapi.testclient import TestClient
from nbformat.v4 import new_code_cell, new_notebook

from web import app as app_module
from web import auth, notebook_runner
from web.governance import policies, store, tags

FAILURES = []
ADMIN_NB, BOB_NB, LEAD_NB, SHARED_NB = "Users/admin/a.ipynb", "Users/analyst_bob/b.ipynb", "Users/lead_engineer/l.ipynb", "Shared/s.ipynb"


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:400]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def cookie_for(username):
    user = auth.get_user_by_username(username)
    token = jwt.encode({"sub": user["id"], "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)},
                       auth.JWT_SECRET_KEY, algorithm=auth.JWT_ALGORITHM)
    return {auth.COOKIE_NAME: token}


def make_notebook(rel, *sources):
    path = os.path.join(TMP_NOTEBOOKS, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    nbformat.write(new_notebook(cells=[new_code_cell(src) for src in (sources or ("print('hi')",))]), path)


def hit(client, method, path, who, **kw):
    """One notebook operation by name -> HTTP status."""
    url = {
        "status": ("get", "/api/workspace/notebook/kernel/status", {"params": {"path": path}}),
        "save": ("post", "/api/workspace/notebook/cell/save", {"json": {"path": path, "cell_index": 1, "source": "x = 1"}}),
        "add": ("post", "/api/workspace/notebook/cell/add", {"json": {"path": path, "after_index": 0, "type": "code"}}),
        "delete": ("delete", "/api/workspace/notebook/cell", {"params": {"path": path, "cell_index": 99}}),
        "clear": ("post", "/api/workspace/notebook/clear_outputs", {"json": {"path": path}}),
        "run": ("post", "/api/workspace/notebook/cell/run", {"json": {"path": path, "cell_index": 1, **kw}}),
        "run_all": ("post", "/api/workspace/notebook/run_all", {"json": {"path": path}}),
        "restart": ("post", "/api/workspace/notebook/kernel/restart", {"json": {"path": path}}),
        "read": ("get", "/api/workspace/file", {"params": {"path": path}}),
    }[method]
    return getattr(client, url[0])(url[1], cookies=who, **url[2])


def main():
    try:
        store.init_governance_db()
        for nb in (ADMIN_NB, BOB_NB, LEAD_NB, SHARED_NB):
            make_notebook(nb)
        client = TestClient(app_module.app)
        admin, bob, lead = cookie_for("admin"), cookie_for("analyst_bob"), cookie_for("lead_engineer")
        garbage = {auth.COOKIE_NAME: "garbage"}

        print("\n1. Every notebook endpoint needs a session")
        for op in ("status", "save", "add", "delete", "clear", "run", "run_all", "restart", "read"):
            check(f"{op}: a bad session is 401", hit(client, op, BOB_NB, garbage).status_code == 401, hit(client, op, BOB_NB, garbage).status_code)
        check("the top-level file list needs a valid session too", client.get("/api/workspace/files", cookies=garbage).status_code == 401)

        print("\n2. Per-user folders")
        for op in ("status", "save", "add", "delete", "clear", "run", "run_all", "restart", "read"):
            check(f"{op}: a user cannot touch another user's notebook", hit(client, op, ADMIN_NB, bob).status_code == 403, hit(client, op, ADMIN_NB, bob).status_code)
        check("...even between two non-admin users", hit(client, "save", BOB_NB, lead).status_code == 403 and hit(client, "run", BOB_NB, lead).status_code == 403)
        for label, path in {"dot-dot into another user": "Users/analyst_bob/../admin/a.ipynb", "double dot-dot": "Users/analyst_bob/../../Users/admin/a.ipynb",
                            "absolute path": "/Users/admin/a.ipynb", "backslashes": "Users\\admin\\a.ipynb", "mixed case": "Users/ADMIN/a.ipynb"}.items():
            r = hit(client, "save", path, bob)
            check(f"path trick refused: {label}", r.status_code in (400, 403), (path, r.status_code))
        r = hit(client, "save", "../warehouse/.metadata/jwt_secret", bob)
        check("escaping the notebooks folder is refused", r.status_code in (400, 403), r.status_code)
        check("a user can edit their own notebook", hit(client, "save", BOB_NB, bob).status_code == 200)
        check("a user can edit a Shared notebook", hit(client, "save", SHARED_NB, bob).status_code == 200)
        check("an admin can reach any user's notebook", hit(client, "save", BOB_NB, admin).status_code == 200 and hit(client, "read", ADMIN_NB, admin).status_code == 200)
        r = client.get("/api/workspace/file", params={"path": "Users/analyst_bob/../admin/a.ipynb"}, cookies=bob)
        check("the file viewer applies the same normalisation", r.status_code in (400, 403), r.status_code)

        print("\n3. Kernels are per user")
        make_notebook(SHARED_NB, "shared_value = 41", "print(shared_value)")
        r1 = client.post("/api/workspace/notebook/cell/run", json={"path": SHARED_NB, "cell_index": 1}, cookies=admin)
        r2 = client.post("/api/workspace/notebook/cell/run", json={"path": SHARED_NB, "cell_index": 2}, cookies=admin)
        check("the owner's kernel keeps state between cells", r1.status_code == 200 and r2.status_code == 200 and "41" in str(r2.json()), (r1.text[:200], r2.text[:300]))
        r3 = client.post("/api/workspace/notebook/cell/run", json={"path": SHARED_NB, "cell_index": 2}, cookies=lead)
        check("another user on the same notebook gets a fresh kernel (variable not defined)", r3.status_code == 200 and "NameError" in str(r3.json()), r3.text[:300])
        keys = sorted(notebook_runner.SESSIONS)
        check("sessions are registered per (user, notebook)", ("admin", SHARED_NB) in keys and ("lead_engineer", SHARED_NB) in keys, keys)
        check("kernel status is per user", client.get("/api/workspace/notebook/kernel/status", params={"path": SHARED_NB}, cookies=bob).json()["is_alive"] is False)

        print("\n4. Masked users: read and edit, but no execution")
        tags.create_definition("pii", "personal", ["email"]) if "pii" not in [t["tag_key"] for t in tags.list_definitions()] else None
        tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="email", tag_key="pii", tag_value="email")
        policies.create_policy({"name": "Mask PII", "tag_key": "pii", "mask_type": "email", "except_roles": ["admin"]})
        check("/api/notebooks/access reports execution as unavailable for a masked user", client.get("/api/notebooks/access", cookies=bob).json()["execution_allowed"] is False)
        check("...and available for the exempt admin", client.get("/api/notebooks/access", cookies=admin).json()["execution_allowed"] is True)
        for op in ("run", "run_all", "restart"):
            r = hit(client, op, BOB_NB, bob)
            check(f"{op} is refused with an explanation", r.status_code == 403 and "masking policies" in r.text, (r.status_code, r.text[:160]))
        check("editing still works for a masked user", hit(client, "save", BOB_NB, bob).status_code == 200 and hit(client, "add", BOB_NB, bob).status_code == 200
              and hit(client, "clear", BOB_NB, bob).status_code == 200 and hit(client, "read", BOB_NB, bob).status_code == 200)
        check("the exempt admin can still run", hit(client, "run", ADMIN_NB, admin).status_code == 200)
        os.environ["GOVERNANCE_NOTEBOOK_EXECUTION"] = "all"
        try:
            check("GOVERNANCE_NOTEBOOK_EXECUTION=all lets a masked user run", hit(client, "run", BOB_NB, bob).status_code == 200)
            check("...and the access endpoint agrees", client.get("/api/notebooks/access", cookies=bob).json() == {"execution_allowed": True, "mode": "all"})
        finally:
            os.environ.pop("GOVERNANCE_NOTEBOOK_EXECUTION")

        print("\n5. No Jupyter server, token or URL anywhere")
        page = client.get("/").text
        check("the page has no JupyterLab link or label", "jupyterlab" not in page.lower(), re.findall(r".{30}(?i:jupyterlab).{30}", page)[:3])
        check("the page carries no ':8890' or 'token=' Jupyter URL", ":8890" not in page and "/lab?token" not in page)
        check("/api/status has no jupyter_url", "jupyter_url" not in client.get("/api/status").json())
        files = client.get("/api/workspace/files", cookies=bob).json()["files"]
        check("the top-level file list has no URLs or tokens", all("url" not in f for f in files), files)
        compose = open(os.path.join(BASE_DIR, "docker-compose.yml")).read()
        check("docker-compose has no Jupyter service, port or token", not re.search(r"(?i)jupyter|8890|8888", compose))
        reqs = [l.split("#")[0].strip().lower() for l in open(os.path.join(BASE_DIR, "requirements.txt"))]
        check("the image no longer ships or starts JupyterLab", not any(l.startswith("jupyterlab") for l in reqs)
              and "jupyter lab" not in open(os.path.join(BASE_DIR, "Dockerfile")).read().lower())
        check("httpx (studio -> worker calls) is a declared dependency, not an accident of JupyterLab", any(l.startswith("httpx") for l in reqs))
    finally:
        for sess in list(notebook_runner.SESSIONS.values()):
            try:
                sess.shutdown()
            except Exception:
                pass
        shutil.rmtree(TMP_ROOT, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All notebook access checks passed.")


if __name__ == "__main__":
    main()
