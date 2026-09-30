#!/usr/bin/env python3
"""
Embedded HTML apps (web/apps.py). Throwaway WAREHOUSE_DIR, no compose services needed.
Run in the studio image: docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_apps.py

Covers: zip-slip / symlink / no-index.html / oversize refusal, the CRUD store and its grant-based access ladder
(admin/power_user/owner always manage; a grant or 'public' visibility gives VIEW; nothing gives nothing), the HTTP
layer's publish-is-admin/power_user-only rule, the served page's security headers and data-token injection, the
Sec-Fetch-Dest top-level-navigation redirect, path traversal refusal, CORS preflight, and -- the core security claim
of the feature -- that the data-gateway endpoint (POST /api/apps/{id}/query) really does run the SAME masking and
catalog-ACL enforcement as the SQL editor, not a bypass of it.
"""
import io
import os
import shutil
import sys
import tempfile
import zipfile

TMP_ROOT = tempfile.mkdtemp(prefix="apps_test_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP_ROOT, "warehouse")
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
os.makedirs(os.environ["WAREHOUSE_DIR"])
for var in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY"):
    os.environ.pop(var, None)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

from fastapi.testclient import TestClient

from web import apps
from web import auth, groups

FAIL = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAIL.append(name)


def raises(exc, fn, *a, **k):
    try:
        fn(*a, **k)
        return False
    except exc:
        return True


def zip_bytes(files, bad_entry=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
        if bad_entry:
            zf.writestr(bad_entry, "x")
    return buf.getvalue()


def symlink_zip():
    import stat as stat_mod
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.html", "<html></html>")
        info = zipfile.ZipInfo("evil_link")
        info.external_attr = (stat_mod.S_IFLNK | 0o777) << 16
        zf.writestr(info, "/etc/passwd")
    return buf.getvalue()


GOOD_ZIP = zip_bytes({"index.html": "<html><head></head><body>hi</body></html>", "assets/style.css": "body{color:red}"})

print("=== zip safety (create_app) ===")
check("no index.html refused", raises(apps.AppError, apps.create_app, "No Index", zip_bytes({"foo.html": "x"}), "admin"))
check("path traversal (..) refused", raises(apps.AppError, apps.create_app, "Traversal",
      zip_bytes({"index.html": "<html></html>"}, bad_entry="../../etc/passwd"), "admin"))
check("absolute path entry refused", raises(apps.AppError, apps.create_app, "Absolute",
      zip_bytes({"index.html": "<html></html>"}, bad_entry="/etc/passwd"), "admin"))
check("symlink entry refused", raises(apps.AppError, apps.create_app, "Symlink", symlink_zip(), "admin"))
check("not a zip refused", raises(apps.AppError, apps.create_app, "Not A Zip", b"not a zip file", "admin"))
check("bad name refused", raises(apps.AppError, apps.create_app, "  ", GOOD_ZIP, "admin"))
check("bad visibility refused", raises(apps.AppError, apps.create_app, "Bad Vis", GOOD_ZIP, "admin", "hidden"))
_orig_max = apps.MAX_ZIP_BYTES
apps.MAX_ZIP_BYTES = 10
check("oversize zip refused", raises(apps.AppError, apps.create_app, "Too Big", GOOD_ZIP, "admin"))
apps.MAX_ZIP_BYTES = _orig_max
check("a rejected upload leaves nothing on disk", not os.path.isdir(apps.APPS_DIR) or not os.listdir(apps.APPS_DIR))

print("=== CRUD ===")
app = apps.create_app("Demo App", GOOD_ZIP, "admin", "private")
check("create returns the record", app["name"] == "Demo App" and app["id"].startswith("app_") and app["visibility"] == "private")
check("files actually landed on disk", os.path.isfile(os.path.join(apps._dir_for(app["id"]), "index.html")))
check("get_app", apps.get_app(app["id"])["id"] == app["id"])
check("list_apps includes it", any(a["id"] == app["id"] for a in apps.list_apps()))
apps.update_app_meta(app["id"], "Renamed App", "shared", "admin")
check("rename + visibility change", apps.get_app(app["id"])["name"] == "Renamed App" and apps.get_app(app["id"])["visibility"] == "shared")
clash = apps.create_app("Other App", GOOD_ZIP, "admin")
check("duplicate name on rename refused", raises(apps.AppError, apps.update_app_meta, clash["id"], "Renamed App", None, "admin"))
apps.update_app_files(app["id"], zip_bytes({"index.html": "<html>v2</html>"}), "admin")
with open(os.path.join(apps._dir_for(app["id"]), "index.html")) as f:
    check("redeploy replaces files", "v2" in f.read())
check("redeploy of a missing app refused", raises(LookupError, apps.update_app_files, "app_nope", GOOD_ZIP, "admin"))
appdir = apps._dir_for(app["id"])
apps.delete_app(app["id"], "admin")
check("delete removes the db row", apps.get_app(app["id"]) is None)
check("delete removes the files", not os.path.isdir(appdir))

print("=== access ladder ===")
alice = auth.create_user("alice", "alicepass123", "Alice (power user)", role="power_user")
bob = auth.create_user("bob", "bobpass1234", "Bob (plain user)", role="user")
carol = auth.create_user("carol", "carolpass123", "Carol (plain user)", role="user")
U = lambda u: {"id": u["id"], "username": u["username"], "role": u["role"]}
admin_u = auth.get_user_by_username("admin")

owned = apps.create_app("Bobs App", GOOD_ZIP, "bob", "private")
check("owner manages their own app", apps.app_access(U(bob), owned) == "manage")
check("admin manages every app", apps.app_access(U(admin_u), owned) == "manage")
check("power_user manages every app", apps.app_access(U(alice), owned) == "manage")
check("a stranger gets nothing on a private app", apps.app_access(U(carol), owned) is None)
groups.grant("app", owned["id"], f"user:{carol['id']}", "VIEW", "bob")
check("a VIEW grant lets a stranger view, not manage", apps.app_access(U(carol), owned) == "view")
groups.grant("app", owned["id"], f"user:{carol['id']}", "MANAGE", "bob")
check("a MANAGE grant lets a stranger manage", apps.app_access(U(carol), owned) == "manage")
groups.revoke("app", owned["id"], f"user:{carol['id']}", "bob")
check("revoking removes access again", apps.app_access(U(carol), owned) is None)

public_app = apps.create_app("Public App", GOOD_ZIP, "bob", "public")
check("'public' visibility gives every authenticated user VIEW with no grant", apps.app_access(U(carol), public_app) == "view")
check("...but never MANAGE", apps.app_access(U(carol), public_app) != "manage")

g = groups.create_group("App Viewers", "", "admin")
groups.add_members(g["id"], [carol["id"]], "admin")
groups.grant("app", owned["id"], f"group:{g['id']}", "VIEW", "admin")
check("a group VIEW grant applies to its members", apps.app_access(U(carol), owned) == "view")
groups.delete_group(g["id"], "admin")
check("deleting the group removes the access it granted", apps.app_access(U(carol), owned) is None)

print("=== resolve_file (path traversal at read time) ===")
check("root resolves to index.html", apps.resolve_file(owned["id"], "").endswith("index.html"))
check("a real asset resolves", apps.resolve_file(owned["id"], "assets/style.css") is not None)
check("traversal out of the app dir is refused", apps.resolve_file(owned["id"], "../../../etc/passwd") is None)
check("a nonexistent file is refused", apps.resolve_file(owned["id"], "nope.html") is None)

print("=== the data token ===")
tok = apps.mint_data_token(owned["id"], "bob")
check("a fresh token verifies for its own app", apps.verify_data_token(tok, owned["id"]) == "bob")
check("...but not for a different app", apps.verify_data_token(tok, public_app["id"]) is None)
check("garbage is refused", apps.verify_data_token("not-a-token", owned["id"]) is None)
check("a normal session token (no purpose claim) is not an app-data token",
      apps.verify_data_token(auth.create_access_token(admin_u), owned["id"]) is None)

print("=== HTTP layer ===")
from web import app as app_module  # noqa: E402  (imported after env vars are set; also wires the governance connection provider)

# The studio's duckrun connection is a lazy singleton (web.app.get_duckrun_conn()) that scans the warehouse once,
# on first use, and is cached for the life of the process. Every table this test needs must exist on disk BEFORE
# the very first request touches it (below), or the cached connection simply never sees it.
from web.governance import tags as gov_tags, policies as gov_policies
from web import warehouses
from deltalake import write_deltalake
import pandas as pd

write_deltalake(os.path.join(os.environ["WAREHOUSE_DIR"], "hr", "employees"),
                pd.DataFrame({"name": ["Ada", "Grace"], "email": ["ada@example.com", "grace@example.com"]}), mode="overwrite")
gov_tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="email", tag_key="pii", tag_value="email", actor="admin")
gov_policies.create_policy({"name": "mask emails for apps test", "tag_key": "pii", "tag_value": "email",
                            "mask_type": "null", "except_roles": ["admin"]}, actor="admin")
warehouses.create_catalog("Finance", "finance", "", None, False, "admin")

client = TestClient(app_module.app)


def login(username, password):
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    return r.cookies


admin_cookies = {"X-User": "admin"}  # GOVERNANCE_REQUIRE_AUTH is off in this test, so the dev X-User header works
bob_cookies = {"X-User": "bob"}
carol_cookies = {"X-User": "carol"}

r = client.post("/api/apps", data={"name": "HTTP Test App", "visibility": "private"},
                files={"file": ("a.zip", GOOD_ZIP, "application/zip")}, headers=carol_cookies)
check("a plain user cannot publish an app", r.status_code == 403, r.text)

r = client.post("/api/apps", data={"name": "HTTP Test App", "visibility": "private"},
                files={"file": ("a.zip", GOOD_ZIP, "application/zip")}, headers=admin_cookies)
check("admin can publish", r.status_code == 200, r.text)
http_app = r.json()

r = client.get(f"/apps/{http_app['id']}/", headers={**admin_cookies, "Sec-Fetch-Dest": "iframe"})
check("served page: 200", r.status_code == 200)
check("served page: frame-ancestors CSP", r.headers.get("content-security-policy") == "frame-ancestors 'self'")
check("served page: X-Frame-Options", r.headers.get("x-frame-options") == "SAMEORIGIN")
check("served page: nosniff", r.headers.get("x-content-type-options") == "nosniff")
check("served page: token injected", "DKW_APP_TOKEN" in r.text)

r2 = client.get(f"/apps/{http_app['id']}/", headers={**admin_cookies, "Sec-Fetch-Dest": "document"}, follow_redirects=False)
check("a top-level navigation (Sec-Fetch-Dest=document) is redirected to the shell, not served",
      r2.status_code in (302, 307) and r2.headers.get("location", "").startswith(f"/?open_app={http_app['id']}"), (r2.status_code, r2.headers.get("location")))

r3 = client.get(f"/apps/{http_app['id']}/assets/style.css", headers={**admin_cookies, "Sec-Fetch-Dest": "iframe"})
check("a real asset is served with the right content type", r3.status_code == 200 and "text/css" in r3.headers.get("content-type", ""))

r4 = client.get(f"/apps/{http_app['id']}/../../../../etc/passwd", headers={**admin_cookies, "Sec-Fetch-Dest": "iframe"})
check("path traversal through the URL is refused", r4.status_code == 404)

r5 = client.get(f"/apps/{http_app['id']}/", headers={**bob_cookies, "Sec-Fetch-Dest": "iframe"})
check("a user with no access gets 404, not the app", r5.status_code == 404)

pre = client.options(f"/api/apps/{http_app['id']}/query")
check("CORS preflight answers with a wildcard origin (no cookie is ever involved, see the module docstring)",
      pre.status_code == 204 and pre.headers.get("access-control-allow-origin") == "*")

import re
m = re.search(r"DKW_APP_TOKEN=(['\"])(.*?)\1", r.text)
token = m.group(2) if m else None
check("a token was extracted from the page", bool(token))

rq = client.post(f"/api/apps/{http_app['id']}/query", json={"sql": "SELECT 1 AS x"}, headers={"Authorization": f"Bearer {token}"})
check("a governed query with the page's own token succeeds", rq.status_code == 200 and rq.json()["rows"] == [[1]], rq.text)

rq2 = client.post(f"/api/apps/{http_app['id']}/query", json={"sql": "SELECT 1"})
check("a query with no token is refused", rq2.status_code == 401)

rq3 = client.post(f"/api/apps/{http_app['id']}/query", json={"sql": "SELECT 1"}, headers={"Authorization": "Bearer garbage"})
check("a query with a bad token is refused", rq3.status_code == 401)

other_app = client.post("/api/apps", data={"name": "Other HTTP App", "visibility": "private"},
                        files={"file": ("a.zip", GOOD_ZIP, "application/zip")}, headers=admin_cookies).json()
rq4 = client.post(f"/api/apps/{other_app['id']}/query", json={"sql": "SELECT 1"}, headers={"Authorization": f"Bearer {token}"})
check("a token minted for one app cannot query through another app's endpoint", rq4.status_code == 401)

print("=== the core claim: the data gateway runs the SAME governance as the SQL editor ===")
masked_app = apps.create_app("Masked Data App", GOOD_ZIP, "bob", "private")
bob_token = apps.mint_data_token(masked_app["id"], "bob")
admin_token = apps.mint_data_token(masked_app["id"], "admin")

r_bob = client.post(f"/api/apps/{masked_app['id']}/query", json={"sql": "SELECT email FROM warehouse.hr.employees ORDER BY email"},
                    headers={"Authorization": f"Bearer {bob_token}"})
check("a non-exempt user's app query comes back masked, exactly like the SQL editor would",
      r_bob.status_code == 200 and all(row == [None] for row in r_bob.json()["rows"]), r_bob.text)

r_admin = client.post(f"/api/apps/{masked_app['id']}/query", json={"sql": "SELECT email FROM warehouse.hr.employees ORDER BY email"},
                      headers={"Authorization": f"Bearer {admin_token}"})
check("an exempt (admin) user's app query sees the real values",
      r_admin.status_code == 200 and r_admin.json()["rows"] == [["ada@example.com"], ["grace@example.com"]], r_admin.text)

fin_token = apps.mint_data_token(masked_app["id"], "carol")
r_fin = client.post(f"/api/apps/{masked_app['id']}/query", json={"sql": "SELECT * FROM finance.main.nope"},
                    headers={"Authorization": f"Bearer {fin_token}"})
check("a user with no catalog access is refused by the same catalog ACL the SQL editor uses (not a bypass)",
      r_fin.status_code in (400, 403), r_fin.text)

print(f"\n{'ALL PASSED' if not FAIL else f'{len(FAIL)} FAILED: ' + ', '.join(FAIL)}")
shutil.rmtree(TMP_ROOT, ignore_errors=True)
sys.exit(1 if FAIL else 0)
