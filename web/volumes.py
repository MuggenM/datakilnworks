import os
import json
import time
import shutil
import hashlib
import logging
import re
import stat as stat_mod
import threading
import uuid
from typing import Dict, Any, List, Optional, BinaryIO
import duckdb

logger = logging.getLogger("localspark.volumes")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
VOLUMES_ROOT = os.path.join(WAREHOUSE_DIR, "volumes")
VOLUMES_METADATA_FILE = os.path.join(METADATA_DIR, "volumes.json")

os.makedirs(VOLUMES_ROOT, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)


# ---------------------------------------------------------------------------------------------------------------- path safety
# Every file operation walks from WAREHOUSE_DIR down to the target one component at a time through directory file descriptors
# opened with O_NOFOLLOW: a symbolic link anywhere below the warehouse (notebook kernels can create them: the warehouse is
# group-writable) is refused, never followed, and a directory swapped for a link between a check and the use cannot redirect
# the operation (there is no separate check). Names are validated first: catalog / schema / volume segments used to be joined
# into the path unchecked, so `/api/volumes/%2e%2e/%2e%2e/<dir>/upload` wrote anywhere the studio process could write.

class VolumePathError(ValueError):
    """A name or path that is not allowed (400)."""


class VolumeFileExists(VolumePathError):
    """The target name exists and overwriting was not asked for (409)."""


class VolumeFileTooLarge(VolumePathError):
    """The upload is larger than VOLUME_UPLOAD_MAX_MB (413)."""


IDENT_RE = re.compile(r"^[a-z0-9_]{1,128}$")
_CHUNK = 1024 * 1024
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
_name_locks: Dict[str, threading.Lock] = {}
_name_locks_guard = threading.Lock()


def max_upload_bytes() -> int:
    """Read on every call (a default argument would freeze it at import)."""
    try:
        mb = float(os.environ.get("VOLUME_UPLOAD_MAX_MB", "1024"))
    except ValueError:
        mb = 1024.0
    return int(max(0.0, mb) * 1024 * 1024)


def clean_ident(value: str, what: str = "name") -> str:
    """catalog / schema / volume name: lower case, '-' becomes '_', then only [a-z0-9_]."""
    v = (value or "").strip().lower().replace("-", "_")
    if not IDENT_RE.match(v):
        raise VolumePathError(f"Invalid volume {what}: use letters, digits and underscores only.")
    return v


def clean_component(part: str) -> str:
    """One file or folder name inside a volume. Hidden names (Auto Loader ignores them, and they include
    upload temp files) and anything that could move up or sideways are refused."""
    if (not part or part in (".", "..") or part.startswith(".") or "/" in part or "\\" in part or "\x00" in part
            or len(part.encode("utf-8", "surrogateescape")) > 255 or any(ord(ch) < 32 for ch in part)):
        raise VolumePathError(f"Invalid file or folder name: {part[:80]!r}")
    return part


def split_path(rel: str) -> List[str]:
    return [clean_component(p) for p in (rel or "").replace("\\", "/").split("/") if p]


def _open_dir(parent_fd: int, name: str, create: bool) -> int:
    try:
        return os.open(name, _DIR_FLAGS, dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise
        try:
            os.mkdir(name, 0o777, dir_fd=parent_fd)        # umask (002 in the studio) and the setgid parent decide the mode
        except FileExistsError:
            pass
        return os.open(name, _DIR_FLAGS, dir_fd=parent_fd)
    except OSError as exc:                                  # ELOOP: a symbolic link; ENOTDIR: a file where a folder should be
        raise VolumePathError(f"'{name}' is not a folder (symbolic links are not followed).") from exc


def open_volume_dir(catalog: str, schema: str, volume_name: str, parts: List[str], create: bool = False) -> int:
    """A directory fd for <volume>/<parts...>; the caller closes it. FileNotFoundError when missing and not create."""
    chain = ["volumes", clean_ident(catalog, "catalog"), clean_ident(schema, "schema"), clean_ident(volume_name)] + list(parts)
    fd = os.open(WAREHOUSE_DIR, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0))
    try:
        for name in chain:
            nxt = _open_dir(fd, name, create)
            os.close(fd)
            fd = nxt
        return fd
    except BaseException:
        os.close(fd)
        raise


def warehouse_parts(path: str) -> List[str]:
    """The components of `path` below WAREHOUSE_DIR (lexical: the fd walk below never follows a link, so the lexical path IS
    the path that gets opened). Outside the warehouse or a hidden component is refused."""
    rel = os.path.relpath(os.path.abspath(path), os.path.abspath(WAREHOUSE_DIR))
    if rel == ".":
        return []
    parts = rel.split(os.sep)
    if parts[0] == "..":
        raise VolumePathError("The folder is outside the warehouse.")
    if any(p.startswith(".") for p in parts):
        raise VolumePathError("Hidden folders (such as .metadata) cannot be a source.")
    return [clean_component(p) for p in parts]


def open_warehouse_dir(path: str, create: bool = False) -> int:
    """A directory fd for a folder below WAREHOUSE_DIR, every component opened with O_NOFOLLOW; the caller closes it.
    FileNotFoundError when missing and not `create`, VolumePathError for a link or a non-folder on the way."""
    fd = os.open(WAREHOUSE_DIR, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0))
    try:
        for name in warehouse_parts(path):
            nxt = _open_dir(fd, name, create)
            os.close(fd)
            fd = nxt
        return fd
    except BaseException:
        os.close(fd)
        raise


def regular_files_below(dir_fd: int, accept=None, skip_dirs=("_quarantine",), max_files: Optional[int] = None):
    """Walks a directory fd and returns ([(relative path, stat)], truncated) for REGULAR files only: symbolic links (to files
    or folders, dangling or not), hidden names, devices, FIFOs and sockets are left out, and `skip_dirs` are not entered.
    `accept(name)` filters files; `max_files` caps the accepted ones."""
    found = []
    stack = [(os.dup(dir_fd), "")]
    try:
        while stack:
            fd, prefix = stack.pop()
            try:
                with os.scandir(fd) as entries:
                    for entry in entries:
                        name = entry.name
                        if name.startswith("."):
                            continue
                        try:
                            st = entry.stat(follow_symlinks=False)
                        except OSError:
                            continue
                        if stat_mod.S_ISDIR(st.st_mode):
                            if name in skip_dirs:
                                continue
                            try:
                                stack.append((os.open(name, _DIR_FLAGS, dir_fd=fd), f"{prefix}{name}/"))
                            except OSError:
                                continue                # swapped for a link since the scan: not followed
                        elif stat_mod.S_ISREG(st.st_mode) and (accept is None or accept(name)):
                            found.append((prefix + name, st))
                            if max_files is not None and len(found) >= max_files:
                                return found, True
            finally:
                os.close(fd)
    finally:
        for fd, _ in stack:
            os.close(fd)
    return found, False


class OpenedFile:
    """A regular file opened below a directory fd without following any link. `path` is what DuckDB (or anything else that
    wants a name) should open: /proc/self/fd/N reopens exactly this file, never whatever sits at the name by then."""

    def __init__(self, root_fd: int, parent_fd: int, fd: int, name: str, st: os.stat_result):
        self.root_fd, self.parent_fd, self.fd, self.name, self.stat = root_fd, parent_fd, fd, name, st

    @property
    def path(self) -> str:
        return f"/proc/self/fd/{self.fd}"

    def close(self):
        for fd in (self.fd, self.parent_fd):
            try:
                os.close(fd)
            except OSError:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def open_regular_file(root_fd: int, rel: str) -> OpenedFile:
    """Opens <root>/<rel> (a path from regular_files_below) with O_NOFOLLOW at every step. A link, a non-regular file or a
    missing one raises OSError / VolumePathError. O_NONBLOCK: a FIFO swapped in cannot hang the open."""
    parts = (rel or "").split("/")
    if not parts or any(p in ("", ".", "..") for p in parts):
        raise VolumePathError(f"Invalid path: {rel[:80]!r}")
    parent = os.dup(root_fd)
    try:
        for name in parts[:-1]:
            nxt = _open_dir(parent, name, False)
            os.close(parent)
            parent = nxt
        try:
            fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0), dir_fd=parent)
        except FileNotFoundError:
            raise
        except OSError as exc:
            raise VolumePathError(f"'{parts[-1]}' is not a regular file (symbolic links are not followed).") from exc
        st = os.fstat(fd)
        if not stat_mod.S_ISREG(st.st_mode):
            os.close(fd)
            raise VolumePathError(f"'{parts[-1]}' is not a regular file.")
        return OpenedFile(root_fd, parent, fd, parts[-1], st)
    except BaseException:
        os.close(parent)
        raise


def _name_lock(key: str) -> threading.Lock:
    with _name_locks_guard:
        return _name_locks.setdefault(key, threading.Lock())


def load_volumes_metadata() -> List[Dict[str, Any]]:
    """Loads volume definitions from metadata file."""
    if not os.path.exists(VOLUMES_METADATA_FILE):
        return []
    try:
        with open(VOLUMES_METADATA_FILE, "r") as f:
            data = json.load(f)
            return data.get("volumes", [])
    except Exception as e:
        logger.error(f"Failed to load volumes metadata: {e}")
        return []


def save_volumes_metadata(volumes: List[Dict[str, Any]]):
    """Saves volume definitions to metadata file."""
    try:
        with open(VOLUMES_METADATA_FILE, "w") as f:
            json.dump({"volumes": volumes, "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")}, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save volumes metadata: {e}")


def get_volume_physical_path(catalog: str, schema: str, volume_name: str) -> str:
    """Returns the canonical on-disk storage directory for a managed volume."""
    return os.path.join(VOLUMES_ROOT, clean_ident(catalog, "catalog"), clean_ident(schema, "schema"), clean_ident(volume_name))


def resolve_volume_posix_path(posix_path: str) -> str:
    """
    Translates a POSIX Unity Catalog Volume path (e.g. /Volumes/catalog/schema/vol/sub/file.csv)
    or standard relative path into the local physical disk path.
    """
    norm = (posix_path or "").strip().replace("\\", "/")
    if norm.lower().startswith("/volumes/"):
        parts = [p for p in norm[len("/volumes/"):].split("/") if p]
        if len(parts) >= 3:
            catalog, schema, volume_name = parts[0], parts[1], parts[2]
            subpath = "/".join(parts[3:]) if len(parts) > 3 else ""
            base = get_volume_physical_path(catalog, schema, volume_name)
            target = os.path.join(base, subpath) if subpath else base
            # Path traversal check
            if os.path.commonpath([os.path.abspath(base), os.path.abspath(target)]) != os.path.abspath(base):
                raise ValueError("Path traversal violation in volume path")
            return target
    
    # If absolute path already inside VOLUMES_ROOT or WAREHOUSE_DIR
    target = os.path.abspath(norm)
    if os.path.commonpath([os.path.abspath(WAREHOUSE_DIR), target]) == os.path.abspath(WAREHOUSE_DIR):
        return target

    raise ValueError(f"Invalid volume path: '{posix_path}'. Must start with /Volumes/<catalog>/<schema>/<volume_name>")


def list_volumes(catalog: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """Lists all registered Unity Catalog volumes, optionally filtered by catalog and schema."""
    all_vols = load_volumes_metadata()
    results = []
    
    for v in all_vols:
        if catalog and v.get("catalog", "").lower() != catalog.lower():
            continue
        if schema and v.get("schema", "").lower() != schema.lower():
            continue
        
        # Verify physical directory exists and calculate file count and size
        try:
            phys = get_volume_physical_path(v["catalog"], v["schema"], v["name"])
        except VolumePathError:
            continue
        
        file_count = 0
        total_bytes = 0
        try:
            for root, dirs, files in os.walk(phys):            # os.walk does not follow directory links
                # Skip quarantine and hidden dirs in top-level count
                dirs[:] = [d for d in dirs if not d.startswith(".") and d != "_quarantine"]
                for f in files:
                    fp = os.path.join(root, f)
                    if not f.startswith(".") and not os.path.islink(fp):
                        file_count += 1
                        try:
                            total_bytes += os.path.getsize(fp)
                        except OSError:
                            pass
        except Exception:
            pass

        item = dict(v)
        item["file_count"] = file_count
        item["total_size_bytes"] = total_bytes
        item["posix_path"] = f"/Volumes/{v['catalog']}/{v['schema']}/{v['name']}"
        results.append(item)

    return sorted(results, key=lambda x: (x.get("catalog", ""), x.get("schema", ""), x.get("name", "")))


def create_volume(
    catalog: str,
    schema: str,
    name: str,
    description: str = "",
    volume_type: str = "MANAGED",
    external_location: Optional[str] = None,
    owner: str = "admin"
) -> Dict[str, Any]:
    """Creates a new Unity Catalog volume."""
    cat_clean = clean_ident(catalog, "catalog")
    sch_clean = clean_ident(schema, "schema")
    vol_clean = clean_ident(name)
    if volume_type.upper() == "EXTERNAL" and external_location:
        # The file routes always use the managed path; an external location is recorded only, and must not point the
        # volume listing (which walks it) at an arbitrary directory of the studio's file system.
        loc = os.path.realpath(external_location)
        rel = os.path.relpath(loc, os.path.realpath(WAREHOUSE_DIR))
        if rel.startswith("..") or any(p.startswith(".") for p in rel.split(os.sep) if p and p != "."):
            raise VolumePathError("An external location must be a non-hidden folder inside the warehouse.")

    all_vols = load_volumes_metadata()
    for v in all_vols:
        if v.get("catalog") == cat_clean and v.get("schema") == sch_clean and v.get("name") == vol_clean:
            raise ValueError(f"Volume '{vol_clean}' already exists in {cat_clean}.{sch_clean}.")

    phys_path = external_location if (volume_type.upper() == "EXTERNAL" and external_location) else get_volume_physical_path(cat_clean, sch_clean, vol_clean)
    os.makedirs(phys_path, exist_ok=True)

    vol_record = {
        "id": f"vol_{hashlib.md5(f'{cat_clean}.{sch_clean}.{vol_clean}'.encode()).hexdigest()[:8]}",
        "name": vol_clean,
        "catalog": cat_clean,
        "schema": sch_clean,
        "volume_type": volume_type.upper(),
        "storage_location": phys_path,
        "posix_path": f"/Volumes/{cat_clean}/{sch_clean}/{vol_clean}",
        "description": description.strip(),
        "owner": owner,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    all_vols.append(vol_record)
    save_volumes_metadata(all_vols)
    logger.info(f"Created volume: {vol_record['posix_path']} -> {phys_path}")
    return vol_record


def delete_volume(catalog: str, schema: str, name: str) -> bool:
    """Deletes a volume record and optionally purges managed files."""
    cat_clean = clean_ident(catalog, "catalog")
    sch_clean = clean_ident(schema, "schema")
    vol_clean = clean_ident(name)

    all_vols = load_volumes_metadata()
    target = None
    remaining = []
    for v in all_vols:
        if v.get("catalog") == cat_clean and v.get("schema") == sch_clean and v.get("name") == vol_clean:
            target = v
        else:
            remaining.append(v)

    if not target:
        return False

    save_volumes_metadata(remaining)
    # If managed volume, purge physical directory
    if target.get("volume_type") == "MANAGED":
        try:
            # fd-based all the way down: a link planted for volumes/<catalog>/<schema> cannot redirect the removal
            fd = os.open(WAREHOUSE_DIR, os.O_RDONLY | os.O_DIRECTORY)
            try:
                for part in ("volumes", cat_clean, sch_clean):
                    nxt = os.open(part, _DIR_FLAGS, dir_fd=fd)
                    os.close(fd)
                    fd = nxt
                st = os.stat(vol_clean, dir_fd=fd, follow_symlinks=False)
                if stat_mod.S_ISDIR(st.st_mode):
                    shutil.rmtree(vol_clean, dir_fd=fd)
                else:
                    os.unlink(vol_clean, dir_fd=fd)
            finally:
                os.close(fd)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Could not purge physical volume directory of {cat_clean}.{sch_clean}.{vol_clean}: {e}")

    logger.info(f"Deleted volume {cat_clean}.{sch_clean}.{vol_clean}")
    return True


def list_volume_files(catalog: str, schema: str, volume_name: str, subpath: str = "") -> List[Dict[str, Any]]:
    """Lists files and folders inside a volume or one of its folders. Symbolic links and hidden names are not listed."""
    parts = split_path(subpath)
    try:
        fd = open_volume_dir(catalog, schema, volume_name, parts, create=not parts)
    except FileNotFoundError:
        return []
    cat, sch, vol = clean_ident(catalog, "catalog"), clean_ident(schema, "schema"), clean_ident(volume_name)
    prefix = "/".join(parts)
    items = []
    try:
        with os.scandir(fd) as entries:
            for entry in entries:
                if entry.name.startswith(".") or entry.is_symlink():
                    continue
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                is_dir = stat_mod.S_ISDIR(st.st_mode)
                if not is_dir and not stat_mod.S_ISREG(st.st_mode):
                    continue
                rel_to_vol = f"{prefix}/{entry.name}" if prefix else entry.name
                _, ext = os.path.splitext(entry.name)
                items.append({
                    "name": entry.name,
                    "relative_path": rel_to_vol,
                    "posix_path": f"/Volumes/{cat}/{sch}/{vol}/{rel_to_vol}",
                    "is_directory": is_dir,
                    "size_bytes": 0 if is_dir else st.st_size,
                    "extension": ext.lower().lstrip("."),
                    "modified_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(st.st_mtime)),
                    "is_quarantine": entry.name == "_quarantine"
                })
    finally:
        os.close(fd)
    # Folders first, then alphabetical by name
    return sorted(items, key=lambda x: (not x["is_directory"], x["name"].lower()))


def upload_file_to_volume(catalog: str, schema: str, volume_name: str, filename: str, content_bytes: Optional[bytes] = None,
                          subpath: str = "", stream: Optional[BinaryIO] = None, overwrite: bool = False) -> Dict[str, Any]:
    """Stores an upload in a volume: written to a hidden temp file first (Auto Loader ignores hidden names) and renamed
    into place when complete, so a half-written file is never ingested and the file watcher sees one IN_MOVED_TO.
    An existing name is refused unless overwrite (VolumeFileExists); more than VOLUME_UPLOAD_MAX_MB is refused
    (VolumeFileTooLarge) and leaves nothing behind."""
    name = clean_component((filename or "").strip())
    parts = split_path(subpath)
    cat, sch, vol = clean_ident(catalog, "catalog"), clean_ident(schema, "schema"), clean_ident(volume_name)
    limit = max_upload_bytes()
    dir_fd = open_volume_dir(cat, sch, vol, parts, create=True)
    tmp = f".upload-{uuid.uuid4().hex}.part"
    size = 0
    try:
        out = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0), 0o666, dir_fd=dir_fd)
        try:
            with os.fdopen(out, "wb", closefd=True) as f:
                if content_bytes is not None:
                    chunks = (content_bytes[i:i + _CHUNK] for i in range(0, len(content_bytes), _CHUNK))
                else:
                    chunks = iter(lambda: stream.read(_CHUNK), b"")
                for chunk in chunks:
                    size += len(chunk)
                    if size > limit:
                        raise VolumeFileTooLarge(f"The file is larger than the upload limit of {limit // (1024 * 1024)} MB.")
                    f.write(chunk)
            with _name_lock(f"{cat}/{sch}/{vol}/{'/'.join(parts)}/{name}"):
                try:
                    existing = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
                except FileNotFoundError:
                    existing = None
                if existing is not None:
                    if not overwrite:
                        raise VolumeFileExists(f"'{name}' already exists in this folder.")
                    if not stat_mod.S_ISREG(existing.st_mode):
                        raise VolumeFileExists(f"'{name}' exists and is not a file; it cannot be replaced by an upload.")
                os.replace(tmp, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)   # replaces a file, never writes through a link
            tmp = None
        finally:
            if tmp is not None:
                try:
                    os.unlink(tmp, dir_fd=dir_fd)
                except FileNotFoundError:
                    pass
        st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    finally:
        os.close(dir_fd)
    rel_path = "/".join(parts + [name])
    return {
        "success": True,
        "name": name,
        "relative_path": rel_path,
        "posix_path": f"/Volumes/{cat}/{sch}/{vol}/{rel_path}",
        "size_bytes": st.st_size,
        "replaced": existing is not None,
    }


def delete_file_from_volume(catalog: str, schema: str, volume_name: str, rel_path: str) -> bool:
    """Deletes a file or folder inside a volume (never the volume root). A symbolic link is removed itself, never followed."""
    parts = split_path(rel_path)
    if not parts:
        raise VolumePathError("Choose a file or folder inside the volume.")
    try:
        dir_fd = open_volume_dir(catalog, schema, volume_name, parts[:-1])
    except FileNotFoundError:
        return False
    try:
        try:
            st = os.stat(parts[-1], dir_fd=dir_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        if stat_mod.S_ISDIR(st.st_mode):
            shutil.rmtree(parts[-1], dir_fd=dir_fd)         # fd-based: does not follow links inside the tree
        else:
            os.unlink(parts[-1], dir_fd=dir_fd)
        return True
    finally:
        os.close(dir_fd)


def preview_volume_file(catalog: str, schema: str, volume_name: str, rel_path: str, limit: int = 10) -> Dict[str, Any]:
    """
    Parses a CSV, Parquet, or JSON file stored in a volume using DuckDB and returns column metadata and preview rows.
    The file is opened here (O_NOFOLLOW, through the checked directory chain) and DuckDB reads that open file through
    /proc/self/fd, with the path as a bound parameter: no path string is ever pasted into SQL (a quote in a file or
    folder name used to end the literal and run the rest as SQL), and no link can be swapped in after the check.
    """
    parts = split_path(rel_path)
    if not parts:
        raise FileNotFoundError(f"File not found: {rel_path}")
    _, ext = os.path.splitext(parts[-1])
    ext_lower = ext.lower().lstrip(".")
    readers = {"csv": "read_csv_auto", "tsv": "read_csv_auto", "txt": "read_csv_auto", "parquet": "read_parquet",
               "json": "read_json_auto", "jsonl": "read_json_auto", "ndjson": "read_json_auto"}
    if ext_lower not in readers:
        raise ValueError(f"Preview is not supported for file extension '.{ext_lower}' (supports CSV, Parquet, JSON).")
    try:
        dir_fd = open_volume_dir(catalog, schema, volume_name, parts[:-1])
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {rel_path}")
    try:
        try:
            file_fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0), dir_fd=dir_fd)
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {rel_path}")
        except OSError as exc:
            raise VolumePathError("Symbolic links are not followed.") from exc
    finally:
        os.close(dir_fd)
    conn = duckdb.connect(":memory:")
    try:
        if not stat_mod.S_ISREG(os.fstat(file_fd).st_mode):
            raise FileNotFoundError(f"File not found: {rel_path}")
        source = f"/proc/self/fd/{file_fd}"
        reader = readers[ext_lower]
        n = max(1, min(int(limit), 100))
        df = conn.execute(f"SELECT * FROM {reader}(?) LIMIT ?", [source, n]).df()
        total_count = int(conn.execute(f"SELECT COUNT(*) FROM {reader}(?)", [source]).fetchone()[0])

        columns = [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
        rows = df.fillna("").to_dict(orient="records")
        rel = "/".join(parts)
        return {
            "success": True,
            "filename": parts[-1],
            "relative_path": rel,
            "posix_path": f"/Volumes/{clean_ident(catalog, 'catalog')}/{clean_ident(schema, 'schema')}/{clean_ident(volume_name)}/{rel}",
            "extension": ext_lower,
            "columns": columns,
            "total_rows": total_count,
            "sample_rows": rows,
            "preview_limit": n
        }
    finally:
        conn.close()
        os.close(file_fd)


def ensure_default_volumes():
    """Initializes default volumes if none exist."""
    existing = load_volumes_metadata()
    if not existing:
        create_volume(
            catalog="warehouse",
            schema="raw",
            name="iot_stream",
            description="Default volume for incoming IoT sensor and telemetry streams (CSV, Parquet, JSON).",
            owner="admin"
        )
