#!/usr/bin/env python3
"""
Auto Loader must never read through a symbolic link (web/autoloader.py, web/autoloader_preview.py, web/volumes.py).

The warehouse is group-writable, so a notebook kernel can plant a link in a volume. The studio runs as root: a link to a
file it can read (e.g. under .metadata, which kernels themselves cannot reach) used to be listed, fingerprinted and LOADED
into the target table, where the kernel's user could then query it. Linked folders were already skipped by os.walk, but a
linked source folder, a linked volume folder and a linked `_quarantine` folder were followed too.

Runs against a throwaway WAREHOUSE_DIR, no services.
"""

import os
import sys
import tempfile

TMP_WAREHOUSE = tempfile.mkdtemp(prefix="autoloader_links_")
os.environ["WAREHOUSE_DIR"] = TMP_WAREHOUSE

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from deltalake import DeltaTable
from web import volumes, autoloader, autoloader_preview

FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)
    return path


# A "secret" the studio can read but a kernel cannot (stand-in for .metadata/<key>), and one outside the warehouse.
SECRET = write(os.path.join(TMP_WAREHOUSE, ".metadata", "secret.csv"), "device_id,temperature\nSECRET_META,1\n")
OUTSIDE_DIR = tempfile.mkdtemp(prefix="autoloader_links_outside_")
OUTSIDE = write(os.path.join(OUTSIDE_DIR, "outside.csv"), "device_id,temperature\nSECRET_OUTSIDE,2\n")


def pipeline(vol, table, sub=""):
    volumes.create_volume("warehouse", "raw", vol)
    vol_dir = volumes.resolve_volume_posix_path(f"/Volumes/warehouse/raw/{vol}")
    src = f"/Volumes/warehouse/raw/{vol}" + (f"/{sub}" if sub else "")
    pipe = autoloader.create_pipeline({"name": f"links_{vol}", "source_volume_path": src, "file_pattern": "*",
                                       "target_table": table, "ingest_mode": "append", "schema_evolution": "addNewColumns"})
    return pipe, vol_dir, src


def table_ids(table):
    path = os.path.join(TMP_WAREHOUSE, "dbo", table)
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        return []
    return sorted(DeltaTable(path).to_pyarrow_table().column("device_id").to_pylist())


def test_file_links():
    print("\n1. A link to a file is not loaded")
    pipe, vol_dir, _ = pipeline("flinks", "flinks_t")
    write(os.path.join(vol_dir, "good.csv"), "device_id,temperature\nGOOD,3\n")
    os.symlink(SECRET, os.path.join(vol_dir, "meta.csv"))
    os.symlink(OUTSIDE, os.path.join(vol_dir, "outside.csv"))
    write(os.path.join(vol_dir, "sub", "nested.csv"), "device_id,temperature\nNESTED,4\n")
    os.symlink(SECRET, os.path.join(vol_dir, "sub", "meta2.csv"))
    os.symlink(os.path.join(vol_dir, "good.csv"), os.path.join(vol_dir, "inside_link.csv"))   # even to a file of the volume
    os.symlink("/nonexistent/x.csv", os.path.join(vol_dir, "dangling.csv"))
    res = autoloader.run_pipeline_cycle(pipe["id"])
    ids = table_ids("flinks_t")
    check("the regular files are loaded", ids == ["GOOD", "NESTED"], f"{ids} / {res}")
    check("no secret row reached the table", not any(i.startswith("SECRET") for i in ids), str(ids))
    check("links are not even counted as found", res.get("files_found") == 2, str(res.get("files_found")))
    check("the cycle did not error", "error" not in res, str(res))
    check("the link targets are untouched", os.path.exists(SECRET) and os.path.exists(OUTSIDE))


def test_linked_folders():
    print("\n2. Linked folders: inside the source, as the source, as the volume")
    pipe, vol_dir, _ = pipeline("dlinks", "dlinks_t")
    write(os.path.join(vol_dir, "ok.csv"), "device_id,temperature\nOK,5\n")
    os.symlink(OUTSIDE_DIR, os.path.join(vol_dir, "linked_dir"))
    os.symlink(os.path.join(TMP_WAREHOUSE, ".metadata"), os.path.join(vol_dir, "meta_dir"))
    autoloader.run_pipeline_cycle(pipe["id"])
    check("a linked folder inside the source is not walked", table_ids("dlinks_t") == ["OK"], str(table_ids("dlinks_t")))

    # The source folder of the pipeline is a link (the pipeline names /Volumes/.../<vol>/landing).
    volumes.create_volume("warehouse", "raw", "srclink")
    sl_dir = volumes.resolve_volume_posix_path("/Volumes/warehouse/raw/srclink")
    pipe2 = autoloader.create_pipeline({"name": "links_srclink", "source_volume_path": "/Volumes/warehouse/raw/srclink/landing",
                                        "file_pattern": "*", "target_table": "srclink_t", "ingest_mode": "append",
                                        "schema_evolution": "addNewColumns"})
    os.symlink(OUTSIDE_DIR, os.path.join(sl_dir, "landing"))
    res2 = autoloader.run_pipeline_cycle(pipe2["id"])
    check("a linked source folder is not read", table_ids("srclink_t") == [], f"{table_ids('srclink_t')} / {res2}")
    check("... and the cycle says why", "link" in str(res2.get("error", "")).lower(), str(res2))

    # The volume folder itself is replaced by a link after the pipeline was created.
    pipe3, v3_dir, _ = pipeline("vollink", "vollink_t")
    os.rmdir(v3_dir)
    os.symlink(OUTSIDE_DIR, v3_dir)
    res3 = autoloader.run_pipeline_cycle(pipe3["id"])
    check("a linked volume folder is not read", table_ids("vollink_t") == [], f"{table_ids('vollink_t')} / {res3}")
    check("... and nothing was created through the link", sorted(os.listdir(OUTSIDE_DIR)) == ["outside.csv"],
          str(os.listdir(OUTSIDE_DIR)))

    # A missing source folder is still created on the first cycle (old behaviour), inside the volume.
    pipe4 = autoloader.create_pipeline({"name": "links_newsub", "source_volume_path": "/Volumes/warehouse/raw/srclink/fresh/deep",
                                        "file_pattern": "*", "target_table": "newsub_t", "ingest_mode": "append",
                                        "schema_evolution": "addNewColumns"})
    res4 = autoloader.run_pipeline_cycle(pipe4["id"])
    check("a missing source folder is created", os.path.isdir(os.path.join(sl_dir, "fresh", "deep")) and res4.get("files_found") == 0, str(res4))


def test_quarantine_link():
    print("\n3. A linked _quarantine folder does not receive the bad file")
    pipe, vol_dir, _ = pipeline("qlink", "qlink_t")
    qtarget = tempfile.mkdtemp(prefix="autoloader_links_q_")
    os.symlink(qtarget, os.path.join(vol_dir, "_quarantine"))
    write(os.path.join(vol_dir, "bad.parquet"), "this is not parquet")
    res = autoloader.run_pipeline_cycle(pipe["id"])
    check("nothing was moved through the link", os.listdir(qtarget) == [], str(os.listdir(qtarget)))
    check("the bad file is still recorded as quarantined",
          any(d.get("status") == "QUARANTINED" for d in res.get("details", [])), str(res))
    check("the bad file did not leave the volume", os.path.exists(os.path.join(vol_dir, "bad.parquet")))

    # A normal (real) _quarantine folder keeps working.
    pipe2, v2, _ = pipeline("qreal", "qreal_t")
    write(os.path.join(v2, "bad.parquet"), "this is not parquet")
    autoloader.run_pipeline_cycle(pipe2["id"])
    q = os.path.join(v2, "_quarantine")
    check("a real _quarantine folder still gets the bad file",
          os.path.isdir(q) and not os.path.islink(q) and any(n.startswith("bad.parquet.") for n in os.listdir(q)),
          str(os.listdir(v2)))


def test_swap_after_discovery():
    print("\n4. A file swapped for a link between discovery and reading")
    pipe, vol_dir, _ = pipeline("swap", "swap_t")
    target = write(os.path.join(vol_dir, "race.csv"), "device_id,temperature\nRACE,6\n")
    real_process = autoloader.process_single_file

    def swapping(p, file_path, base, *a, **kw):
        if os.path.basename(file_path) == "race.csv" and not os.path.islink(target):
            os.remove(target)
            os.symlink(SECRET, target)
        return real_process(p, file_path, base, *a, **kw)

    autoloader.process_single_file = swapping
    try:
        res = autoloader.run_pipeline_cycle(pipe["id"])
    finally:
        autoloader.process_single_file = real_process
    ids = table_ids("swap_t")
    check("the secret was not read through the swapped-in link", not any(i.startswith("SECRET") for i in ids), f"{ids} / {res}")


def test_preview():
    print("\n5. Preview of a local source")
    _, vol_dir, src = pipeline("pv", "pv_t")
    write(os.path.join(vol_dir, "a.csv"), "device_id,temperature\nPV,7\n")
    os.symlink(SECRET, os.path.join(vol_dir, "b.csv"))
    os.symlink(OUTSIDE_DIR, os.path.join(vol_dir, "d"))
    out = autoloader_preview.preview(src)
    paths = [f["path"] for f in out["files"]]
    check("preview lists only the regular file", paths == ["a.csv"], str(paths))
    try:
        r = autoloader_preview.preview(src, sample_file="b.csv")
        check("preview refuses a link as the sample", False, str(r.get("rows")))
    except autoloader_preview.PreviewError as exc:
        check("preview refuses a link as the sample", "not one of the files" in str(exc), str(exc))
    rows = out.get("rows", [])
    check("preview rows come from the regular file", rows and rows[0][0] == "PV", str(rows))

    volumes.create_volume("warehouse", "raw", "pvl")
    pvl = volumes.resolve_volume_posix_path("/Volumes/warehouse/raw/pvl")
    os.symlink(OUTSIDE_DIR, os.path.join(pvl, "landing"))
    try:
        autoloader_preview.preview("/Volumes/warehouse/raw/pvl/landing")
        check("preview refuses a linked source folder", False)
    except autoloader_preview.PreviewError as exc:
        check("preview refuses a linked source folder", "link" in str(exc).lower(), str(exc))


def main():
    autoloader.init_autoloader_db()
    test_file_links()
    test_linked_folders()
    test_quarantine_link()
    test_swap_after_discovery()
    test_preview()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: " + "; ".join(FAILURES))
        os._exit(1)
    print("ALL PASS")
    os._exit(0)


if __name__ == "__main__":
    main()
