"""Volume routes: access checks and path safety (web/volumes.py, the /api/volumes routes in web/app.py).

Every hole found on 2026-11-28 is a check here, each one failing on the old code:
  - no access check: any signed-in user listed, uploaded and previewed in every volume of every catalog;
  - catalog / schema / volume names went into the path unchecked: /api/volumes/%2e%2e/%2e%2e/<dir>/upload wrote
    anywhere the studio process could write (outside the warehouse, into .metadata);
  - abspath checks: a symbolic link inside a volume (notebook kernels can create them) let preview read and upload
    write outside the volume;
  - preview pasted the path into SQL: a quote in a file or folder name ended the string literal;
  - an upload read the whole file into memory, had no size limit and silently replaced an existing file.
Plus the cases that must keep working (the default catalog stays open to every user, as for tables).

Throwaway warehouse, TestClient; nothing outside this process is contacted. Run in the image:
  ci/run_test.sh volume_routes
"""
import os
import sys
import tempfile

WH = tempfile.mkdtemp(prefix="dkw_vol_")
os.environ["WAREHOUSE_DIR"] = WH
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = ("pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$"
                                          "e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d")
os.environ["CONTROLLER_URL"] = ""
for _k in ("WH_STARTER_ENDPOINT", "WH_ANALYTICS_ENDPOINT", "WH_ETL_ENDPOINT"):
    os.environ[_k] = "http://127.0.0.1:9"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

FAILS = []


def check(label, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f"  ({str(detail)[:300]})" if detail and not ok else ""), flush=True)
    if not ok:
        FAILS.append(label)


from fastapi.testclient import TestClient
from web import app as app_module, auth, permissions, warehouses, volumes

with auth.get_db_connection() as c:
    c.execute("UPDATE users SET must_change_password = 0")
admin = auth.get_user_by_username("admin")
for name in ("plain", "reader", "writer", "power"):
    auth.create_user(name, f"{name}password123", name.title(), role="power_user" if name == "power" else "user")
with auth.get_db_connection() as c:
    c.execute("UPDATE users SET must_change_password = 0")
U = {n: auth.get_user_by_username(n) for n in ("admin", "plain", "reader", "writer", "power")}
H = {n: {"Authorization": f"Bearer {auth.create_access_token(u)}"} for n, u in U.items()}

warehouses.create_catalog("Sales", "sales", "", None, False, "admin")
permissions.grant_catalog_permission("sales", U["reader"]["id"], "READ", admin)
permissions.grant_catalog_permission("sales", U["writer"]["id"], "WRITE", admin)
meta = os.path.join(WH, ".metadata")
open(os.path.join(meta, "probe_secret.txt"), "w").write("col\nTOP-SECRET-VALUE\n")
outside = tempfile.mkdtemp(prefix="dkw_outside_")
open(os.path.join(outside, "secret.csv"), "w").write("k\nOUTSIDE-SECRET\n")

with TestClient(app_module.app) as cl:
    def up(who, cat, sch, vol, name, data, **form):
        return cl.post(f"/api/volumes/{cat}/{sch}/{vol}/upload", headers=H[who], files={"file": (name, data)}, data=form)

    print("setup")
    r = cl.post("/api/volumes", headers=H["admin"], json={"catalog": "sales", "schema": "landing", "name": "drops"})
    check("admin creates a volume in the sales catalog", r.status_code == 200, r.text)
    check("... and the answer does not show the server path", "storage_location" not in r.json(), r.text)
    iot = os.path.join(WH, "volumes", "warehouse", "raw", "iot_stream")
    drops = os.path.join(WH, "volumes", "sales", "landing", "drops")
    os.makedirs(iot, exist_ok=True)

    print("1. access follows the catalog rights")
    names = lambda who: {(v["catalog"], v["name"]) for v in cl.get("/api/volumes", headers=H[who]).json().get("volumes", [])}
    check("a user without rights on sales does not see its volume", ("sales", "drops") not in names("plain"), names("plain"))
    check("... but sees the default catalog's volume (warehouse is open to every user)", ("warehouse", "iot_stream") in names("plain"))
    check("a user with READ on sales sees it", ("sales", "drops") in names("reader"))
    check("the volume list does not show server paths",
          all("storage_location" not in v for v in cl.get("/api/volumes", headers=H["admin"]).json()["volumes"]))
    r = cl.get("/api/volumes/sales/landing/drops/files", headers=H["plain"])
    check("listing files without rights: 404 (cannot even probe)", r.status_code == 404, r.status_code)
    r = up("plain", "sales", "landing", "drops", "x.csv", b"a\n1\n")
    check("uploading without rights: 404, nothing written", r.status_code == 404 and not os.path.exists(os.path.join(drops, "x.csv")), r.status_code)
    r = up("reader", "sales", "landing", "drops", "x.csv", b"a\n1\n")
    check("READ is not enough to upload: 403", r.status_code == 403 and not os.path.exists(os.path.join(drops, "x.csv")), r.status_code)
    r = up("writer", "sales", "landing", "drops", "x.csv", b"a\n1\n2\n")
    check("WRITE uploads", r.status_code == 200 and os.path.exists(os.path.join(drops, "x.csv")), r.text)
    check("... and the answer does not show the server path", "physical_path" not in r.json(), r.text)
    r = cl.get("/api/volumes/sales/landing/drops/preview", headers=H["reader"], params={"path": "x.csv"})
    check("READ previews", r.status_code == 200 and r.json().get("total_rows") == 2, r.text)
    r = cl.get("/api/volumes/sales/landing/drops/preview", headers=H["plain"], params={"path": "x.csv"})
    check("previewing without rights: 404", r.status_code == 404, r.status_code)
    r = up("plain", "warehouse", "raw", "iot_stream", "mine.csv", b"a\n1\n")
    check("every user may still upload to the default catalog's volume", r.status_code == 200, r.text)
    r = cl.request("DELETE", "/api/volumes/sales/landing/drops/files", headers=H["reader"], params={"path": "x.csv"})
    check("deleting a file still needs admin or power user", r.status_code == 403, r.status_code)
    r = cl.post("/api/volumes", headers=H["power"], json={"catalog": "sales", "schema": "landing", "name": "other"})
    check("a power user without WRITE on sales cannot create a volume there", r.status_code == 403, r.text)
    r = cl.get("/api/volumes/sales/landing/nosuch/files", headers=H["admin"])
    check("an unregistered volume is 404, even for an admin", r.status_code == 404, r.status_code)

    print("2. names cannot leave the volumes folder")
    tgt, whname = os.path.basename(outside), os.path.basename(WH)
    r = cl.get(f"/api/volumes/%2e%2e/%2e%2e/{tgt}/files", headers=H["admin"])
    check("listing %2e%2e/%2e%2e/<dir> is refused", r.status_code == 400, f"{r.status_code} {r.text}")
    r = cl.post(f"/api/volumes/%2e%2e/%2e%2e/{tgt}/upload", headers=H["admin"], files={"file": ("planted.txt", b"x")})
    check("uploading there is refused and writes nothing", r.status_code == 400 and not os.path.exists(os.path.join(outside, "planted.txt")), r.status_code)
    r = cl.post(f"/api/volumes/%2e%2e/%2e%2e/{whname}/upload", headers=H["admin"], files={"file": ("planted.txt", b"x")}, data={"subpath": ".metadata"})
    check("uploading into <warehouse>/.metadata is refused", r.status_code == 400 and not os.path.exists(os.path.join(meta, "planted.txt")), r.status_code)
    r = cl.get(f"/api/volumes/%2e%2e/%2e%2e/{whname}/preview", headers=H["admin"], params={"path": ".metadata/probe_secret.txt"})
    check("previewing a .metadata file through the names is refused", r.status_code == 400 and "TOP-SECRET" not in r.text, r.status_code)
    for sub in ("..", "../..", "a/../../..", ".hidden", "a/.hidden"):
        r = up("admin", "warehouse", "raw", "iot_stream", "s.csv", b"a\n1\n", subpath=sub)
        check(f"subpath {sub!r} is refused", r.status_code == 400, f"{r.status_code} {r.text}")
    for fname in (".hidden.csv", "..", "a/b.csv", "a\\b.csv"):
        r = up("admin", "warehouse", "raw", "iot_stream", fname, b"a\n1\n")
        check(f"file name {fname!r} is refused", r.status_code in (400, 422), f"{r.status_code} {r.text}")
    for path in ("../../../.metadata/probe_secret.txt", "../drops/x.csv"):
        r = cl.get("/api/volumes/warehouse/raw/iot_stream/preview", headers=H["admin"], params={"path": path})
        check(f"preview path {path!r} is refused", r.status_code == 400 and "SECRET" not in r.text, r.status_code)
    r = cl.post("/api/volumes", headers=H["admin"], json={"catalog": "..", "schema": "..", "name": "x"})
    check("creating a volume named '..' is refused", r.status_code == 400, r.text)
    r = cl.post("/api/volumes", headers=H["admin"], json={"catalog": "warehouse", "schema": "raw", "name": "ext",
                                                          "volume_type": "EXTERNAL", "external_location": outside})
    check("an external location outside the warehouse is refused", r.status_code == 400, r.text)
    r = cl.post("/api/volumes", headers=H["admin"], json={"catalog": "warehouse", "schema": "raw", "name": "ext2",
                                                          "volume_type": "EXTERNAL", "external_location": meta})
    check("an external location in .metadata is refused", r.status_code == 400, r.text)

    print("3. symbolic links are never followed")
    os.symlink(outside, os.path.join(iot, "lnk"))
    os.symlink(os.path.join(outside, "secret.csv"), os.path.join(iot, "file_link.csv"))
    r = cl.get("/api/volumes/warehouse/raw/iot_stream/preview", headers=H["admin"], params={"path": "lnk/secret.csv"})
    check("preview through a linked folder is refused", r.status_code == 400 and "OUTSIDE-SECRET" not in r.text, f"{r.status_code} {r.text[:120]}")
    r = cl.get("/api/volumes/warehouse/raw/iot_stream/preview", headers=H["admin"], params={"path": "file_link.csv"})
    check("preview of a linked file is refused", r.status_code == 400 and "OUTSIDE-SECRET" not in r.text, f"{r.status_code} {r.text[:120]}")
    r = up("admin", "warehouse", "raw", "iot_stream", "dropped.txt", b"x", subpath="lnk")
    check("upload into a linked folder is refused and writes nothing outside",
          r.status_code == 400 and not os.path.exists(os.path.join(outside, "dropped.txt")), r.status_code)
    r = up("admin", "warehouse", "raw", "iot_stream", "file_link.csv", b"k\nREPLACED\n", overwrite="true")
    check("overwriting a linked file's name is refused, the link target stays intact",
          r.status_code == 409 and open(os.path.join(outside, "secret.csv")).read() == "k\nOUTSIDE-SECRET\n", f"{r.status_code} {r.text}")
    r = cl.get("/api/volumes/warehouse/raw/iot_stream/files", headers=H["admin"])
    listed = [f["name"] for f in r.json().get("files", [])]
    check("links are not listed", "lnk" not in listed and "file_link.csv" not in listed, listed)
    r = cl.get("/api/volumes/warehouse/raw/iot_stream/files", headers=H["admin"], params={"subpath": "lnk"})
    check("listing a linked folder is refused", r.status_code == 400, r.status_code)
    r = cl.request("DELETE", "/api/volumes/warehouse/raw/iot_stream/files", headers=H["admin"], params={"path": "lnk/secret.csv"})
    check("deleting through a linked folder is refused, the target file stays",
          r.status_code == 400 and os.path.exists(os.path.join(outside, "secret.csv")), r.status_code)
    r = cl.request("DELETE", "/api/volumes/warehouse/raw/iot_stream/files", headers=H["admin"], params={"path": "lnk"})
    check("deleting a link removes the link only", r.status_code == 200 and not os.path.lexists(os.path.join(iot, "lnk"))
          and os.path.exists(os.path.join(outside, "secret.csv")), r.status_code)

    print("4. a quote in a name is data, not SQL")
    up("admin", "warehouse", "raw", "iot_stream", "x.csv", b"a\n1\n")
    # the target has no hidden folder in its path (hidden names are refused anyway), so only the quote is tested here
    inj = "x.csv') UNION ALL SELECT content FROM read_text('" + os.path.join(outside, "secret.csv") + "') --"
    sub, name = os.path.dirname(inj), os.path.basename(inj) + ".csv"
    r = up("admin", "warehouse", "raw", "iot_stream", name, b"col\nharmless\n", subpath=sub)
    check("a crafted name can still be uploaded (it is a legal name)", r.status_code == 200, r.text)
    r = cl.get("/api/volumes/warehouse/raw/iot_stream/preview", headers=H["admin"], params={"path": f"{sub}/{name}"})
    check("its preview shows the file's own content and no secret",
          r.status_code == 200 and "OUTSIDE-SECRET" not in r.text and r.json().get("sample_rows") == [{"col": "harmless"}], r.text[:200])

    print("5. uploads: no silent overwrite, a size limit, nothing half-written")
    up("admin", "warehouse", "raw", "iot_stream", "same.csv", b"a\nfirst\n")
    r = up("admin", "warehouse", "raw", "iot_stream", "same.csv", b"a\nsecond\n")
    check("a second upload of the same name is 409 and keeps the first",
          r.status_code == 409 and open(os.path.join(iot, "same.csv")).read() == "a\nfirst\n", r.status_code)
    r = up("admin", "warehouse", "raw", "iot_stream", "same.csv", b"a\nsecond\n", overwrite="true")
    check("with overwrite it replaces the file", r.status_code == 200 and r.json().get("replaced") is True
          and open(os.path.join(iot, "same.csv")).read() == "a\nsecond\n", r.text)
    os.environ["VOLUME_UPLOAD_MAX_MB"] = "1"
    r = up("admin", "warehouse", "raw", "iot_stream", "big.csv", b"x" * (3 * 1024 * 1024))
    check("an upload over VOLUME_UPLOAD_MAX_MB is 413 (limit read at call time)", r.status_code == 413, r.status_code)
    r = cl.post("/api/volumes/warehouse/raw/iot_stream/upload", headers=H["admin"], files={"file": ("big2.csv", b"x" * (1536 * 1024))})
    check("one just over the limit (passes the Content-Length gate) is refused while streaming", r.status_code == 413, r.status_code)
    leftovers = [n for n in os.listdir(iot) if n.startswith(".upload-") or n.startswith("big")]
    check("... and leaves no file or temp file behind", leftovers == [], leftovers)
    os.environ["VOLUME_UPLOAD_MAX_MB"] = "1024"
    r = up("admin", "warehouse", "raw", "iot_stream", "nested.csv", b"a\n1\n", subpath="2026/11")
    check("an upload into new sub-folders creates them", r.status_code == 200 and os.path.isfile(os.path.join(iot, "2026", "11", "nested.csv")), r.text)
    r = cl.get("/api/volumes/warehouse/raw/iot_stream/files", headers=H["admin"], params={"subpath": "2026/11"})
    check("... and lists them with the right relative path",
          [f["relative_path"] for f in r.json().get("files", [])] == ["2026/11/nested.csv"], r.text[:200])

    print("6. the module directly (Auto Loader's demo seed and source paths use it)")
    try:
        volumes.resolve_volume_posix_path("/Volumes/../../etc")
        check("a /Volumes path with '..' names is refused", False)
    except ValueError:
        check("a /Volumes path with '..' names is refused", True)
    res = volumes.upload_file_to_volume("warehouse", "raw", "iot_stream", "seeded.csv", b"a\n1\n")
    check("upload with content_bytes still works (the demo seed)", res.get("success") and os.path.isfile(os.path.join(iot, "seeded.csv")), res)
    r = cl.request("DELETE", "/api/volumes/sales/landing/drops", headers=H["admin"])
    check("deleting a volume removes its folder", r.status_code == 200 and not os.path.exists(drops), r.text)

print("ALL PASS" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}", flush=True)
sys.stdout.flush()
os._exit(1 if FAILS else 0)
