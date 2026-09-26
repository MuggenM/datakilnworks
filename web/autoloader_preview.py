"""Preview of the first rows of an Auto-Loader source that is a local volume folder or an `s3://` location, BEFORE a pipeline exists
(connection sources have their own, `autoloader_conn.preview`).

It answers "what would this pipeline read?" with the pipeline's own rules: the same discovery filters (hidden files and folders, `_quarantine`, `.tmp` /
`.part`, the file pattern), the same reader (`autoloader._open_source_reader`, DuckDB streaming with a LIMIT) and, for S3, the same mount and DuckDB
`httpfs` settings, so objects are read in place (a Parquet file costs a few range requests, nothing is downloaded). Nothing is created, checkpointed
or loaded: a missing folder is reported, not created (the pipeline itself creates it on its first run).

The sample is the oldest file, like the pipeline's first cycle, unless a specific one is asked for (`sample_file`, which must be one of the
discovered files). Local paths are the volumes / warehouse area only, and never a hidden folder (`.metadata` holds credentials).
"""
import fnmatch
import os
from typing import Any, Dict, List, Optional

from web import autoloader_s3

PREVIEW_ROWS = 10
MAX_LOCAL_SCAN = 5000
MAX_S3_SCAN = 3000                      # keys looked at (3 pages); a preview must not list a bucket with millions of objects
LISTED_FILES = 30
READABLE = ("csv", "tsv", "txt", "parquet", "json", "jsonl", "ndjson")


class PreviewError(Exception):
    """A problem the person can fix; the message is safe to show."""


def _ext(name: str) -> str:
    return os.path.splitext(name)[1].lower().lstrip(".")


def _render(duck, target: str, ext: str, limit: int) -> Dict[str, Any]:
    from web import autoloader
    from web.autoloader_conn import _jsonable
    if ext not in READABLE:
        raise PreviewError(f"The loader reads {', '.join('.' + e for e in READABLE)} files, not '.{ext}'.")
    try:
        table = autoloader._open_source_reader(duck, target, ext, limit=limit + 1).read_all()
    except Exception as exc:
        first = str(exc).strip().splitlines()[0][:200] if str(exc).strip() else type(exc).__name__
        if autoloader._looks_like_remote_io_error(exc):
            raise PreviewError(f"The storage could not be read ({first}).")
        raise PreviewError(f"The file could not be read as {ext.upper()} ({first}).")
    more = table.num_rows > limit
    table = table.slice(0, limit)
    return {"truncated": more, "columns": [{"name": f.name, "type": str(f.type)} for f in table.schema],
            "rows": [[_jsonable(v) for v in row] for row in zip(*[c.to_pylist() for c in table.columns])] if table.num_columns else []}


def _size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n} B"


def _pick(cands: List[Dict[str, Any]], sample_file: Optional[str]) -> Dict[str, Any]:
    if sample_file:
        hit = next((c for c in cands if c["path"] == sample_file), None)
        if hit is None:
            raise PreviewError("That file is not one of the files this pipeline would read.")
        return hit
    readable = [c for c in cands if _ext(c["path"]) in READABLE]
    if not readable:
        raise PreviewError("Files match, but none has a format the loader reads (" + ", ".join("." + e for e in READABLE) + ").")
    return readable[0]


# ---------------------------------------------------------------- local volume folder

def _local(path: str, pattern: str, sample_file: Optional[str], limit: int) -> Dict[str, Any]:
    from web.volumes import resolve_volume_posix_path, WAREHOUSE_DIR
    try:
        source_dir = resolve_volume_posix_path(path)
    except Exception as exc:
        raise PreviewError(str(exc))
    rel = os.path.relpath(os.path.realpath(source_dir), os.path.realpath(WAREHOUSE_DIR))
    if rel.startswith("..") or any(p.startswith(".") for p in rel.split(os.sep) if p not in (".", "")):
        raise PreviewError("Hidden folders (such as .metadata) cannot be a source.")
    if not os.path.isdir(source_dir):
        raise PreviewError("That folder does not exist yet, so there is nothing to read. (The pipeline creates it on its first run; upload files to the volume first.)")
    pattern = pattern or "*"
    found: List[Dict[str, Any]] = []
    scanned = 0
    truncated = False
    for root, dirs, files in os.walk(source_dir):
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "_quarantine"]
        for f in files:
            if f.startswith(".") or f.endswith(".tmp") or f.endswith(".part"):
                continue
            if not (pattern == "*" or fnmatch.fnmatch(f.lower(), pattern.lower())):
                continue
            full = os.path.join(root, f)
            try:
                st = os.stat(full)
            except OSError:
                continue
            found.append({"path": os.path.relpath(full, source_dir).replace(os.sep, "/"), "full": full, "size": st.st_size, "mtime": st.st_mtime})
            scanned += 1
            if scanned >= MAX_LOCAL_SCAN:
                truncated = True
                break
        if truncated:
            break
    if not found:
        raise PreviewError(f"No file in that folder matches '{pattern}'. Files that are hidden, end in .tmp / .part, or sit in _quarantine are ignored.")
    found.sort(key=lambda c: (c["mtime"], c["path"]))                    # the pipeline ingests older batches first
    chosen = _pick(found, sample_file)
    import duckdb
    duck = duckdb.connect(":memory:")
    try:
        body = _render(duck, chosen["full"], _ext(chosen["path"]), limit)
    finally:
        duck.close()
    skipped = sum(1 for c in found if _ext(c["path"]) not in READABLE)
    notes = [f"{len(found)}{'+' if truncated else ''} matching file(s). Showing {chosen['path']} ({_size(chosen['size'])}); "
             + ("the one you chose." if sample_file else "the oldest, which the pipeline loads first.")]
    if skipped:
        notes.append(f"{skipped} matching file(s) have a format the loader does not read; they would fail (and be quarantined).")
    return {"ok": True, "source": "local", "sample": chosen["path"], "file_count": len(found), "truncated_listing": truncated, "notes": notes,
            "files": [{"path": c["path"], "size": c["size"], "readable": _ext(c["path"]) in READABLE} for c in found[:LISTED_FILES]], **body}


# ---------------------------------------------------------------- s3://

def _s3(path: str, pattern: str, mount_id: Optional[str], sample_file: Optional[str], limit: int) -> Dict[str, Any]:
    S3Err = autoloader_s3.S3SourceError
    try:
        bucket, prefix = autoloader_s3.parse_s3_path(path)
        pipe = {"source_volume_path": path, "source_mount_id": mount_id or ""}
        conn = autoloader_s3.resolve_connection(pipe)
        client = autoloader_s3.make_client(conn)
    except S3Err as exc:
        raise PreviewError(str(exc))
    pattern = pattern or "*"
    found: List[Dict[str, Any]] = []
    scanned = 0
    truncated = False
    try:
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000})
        for page in pages:
            for o in page.get("Contents", []):
                scanned += 1
                rel = o["Key"][len(prefix):]
                if autoloader_s3.is_ignored_key(rel, pattern):
                    continue
                found.append({"path": rel, "key": o["Key"], "size": int(o["Size"]), "mtime": o["LastModified"].timestamp()})
            if scanned >= MAX_S3_SCAN:
                truncated = True
                break
    except S3Err as exc:
        raise PreviewError(str(exc))
    except Exception as exc:
        raise PreviewError(f"Could not list s3://{bucket}/{prefix}: {autoloader_s3._describe(exc)}")
    if not found:
        raise PreviewError(f"No object under s3://{bucket}/{prefix} matches '{pattern}'"
                           + (f" among the first {scanned} keys." if truncated else ". Hidden names, .tmp / .part files and _quarantine/ are ignored."))
    found.sort(key=lambda c: (c["mtime"], c["path"]))
    chosen = _pick(found, sample_file)
    import duckdb
    duck = duckdb.connect(":memory:")
    try:
        try:
            autoloader_s3.configure_duckdb(duck, conn)
        except Exception as exc:
            raise PreviewError(f"The storage could not be set up for reading ({str(exc).splitlines()[0][:160]}).")
        body = _render(duck, f"s3://{bucket}/{chosen['key']}", _ext(chosen["path"]), limit)
    finally:
        duck.close()
    skipped = sum(1 for c in found if _ext(c["path"]) not in READABLE)
    notes = [f"{len(found)}{'+' if truncated else ''} matching object(s)" + (f" (only the first {scanned} keys were looked at)" if truncated else "")
             + f". Showing {chosen['path']} ({_size(chosen['size'])}), read in place; " + ("the one you chose." if sample_file else "the oldest, which the pipeline loads first.")]
    if skipped:
        notes.append(f"{skipped} matching object(s) have a format the loader does not read.")
    return {"ok": True, "source": "s3", "sample": chosen["path"], "file_count": len(found), "truncated_listing": truncated, "notes": notes,
            "files": [{"path": c["path"], "size": c["size"], "readable": _ext(c["path"]) in READABLE} for c in found[:LISTED_FILES]], **body}


def preview(path: str, file_pattern: str = "*", source_mount_id: Optional[str] = None, sample_file: Optional[str] = None, limit: int = PREVIEW_ROWS) -> Dict[str, Any]:
    path = (path or "").strip()
    if not path:
        raise PreviewError("Enter the source path first.")
    limit = max(1, min(int(limit or PREVIEW_ROWS), 50))
    if autoloader_s3.is_s3_path(path):
        return _s3(path, file_pattern, source_mount_id, sample_file, limit)
    if source_mount_id:
        raise PreviewError("A storage mount only applies to s3:// sources.")
    return _local(path, file_pattern, sample_file, limit)
