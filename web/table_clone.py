"""
Delta shallow clone: `CREATE [OR REPLACE] TABLE t SHALLOW CLONE src [VERSION AS OF n | TIMESTAMP AS OF '...']`.

A clone is a new, independent Delta table whose version 0 already lists the source's data files at the cloned
version, without copying a byte of data. delta-rs and DuckDB's Delta reader both resolve an `add.path` relative to
the table root (neither follows an absolute URI to another table -- verified), so the clone *hard-links* the
source's immutable Parquet files into its own directory instead. That keeps every reader working and makes this
strictly safer than Databricks' reference-based shallow clone: the clone stays valid if the source is later
VACUUMed, rewritten or dropped, because the data survives as long as either name for it exists. Delta data files
are never modified in place, so sharing an inode is safe. Storage is shared until the clone's own writes diverge.

Limits: source and target must be on the same filesystem (a hard link cannot cross one) and local (no S3 or
mounted catalogs); tables using deletion vectors or row tracking are refused; a source whose log needs a v2
checkpoint or has had its earlier commits cleaned up cannot be cloned at that version (fails closed).
"""

import datetime
import json
import logging
import os
import re
import shutil
import time
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable

logger = logging.getLogger("localspark.table_clone")

REFUSED_FEATURES = ("deletionvectors", "rowtracking")

_IDENT = r'(?:"[^"]+"|`[^`]+`|[A-Za-z_][\w]*)(?:\.(?:"[^"]+"|`[^`]+`|[A-Za-z_][\w]*)){0,2}'
_CLONE_RE = re.compile(
    rf"^\s*CREATE\s+(?P<replace>OR\s+REPLACE\s+)?TABLE\s+(?P<ine>IF\s+NOT\s+EXISTS\s+)?(?P<target>{_IDENT})\s+"
    rf"SHALLOW\s+CLONE\s+(?P<source>{_IDENT})"
    r"(?:\s+VERSION\s+AS\s+OF\s+(?P<version>\d+)|\s+TIMESTAMP\s+AS\s+OF\s+'(?P<ts>[^']+)')?\s*;?\s*$",
    re.IGNORECASE | re.DOTALL)


class CloneError(Exception):
    """A clone that cannot be made; the message is safe to show the user."""


def parse_clone_sql(sql: str) -> Optional[Dict[str, Any]]:
    """The parts of a SHALLOW CLONE statement, or None when `sql` is not one."""
    m = _CLONE_RE.match(sql or "")
    if not m:
        return None

    def parts(ident: str) -> List[str]:
        return [p.strip('"`') for p in re.findall(r'"[^"]+"|`[^`]+`|[^.]+', ident)]

    return {"target": parts(m.group("target")), "source": parts(m.group("source")),
            "replace": bool(m.group("replace")), "if_not_exists": bool(m.group("ine")),
            "version": int(m.group("version")) if m.group("version") else None, "timestamp": m.group("ts")}


def _parse_timestamp(value: str) -> datetime.datetime:
    try:
        ts = datetime.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise CloneError(f"'{value}' is not a valid timestamp (use e.g. '2026-09-24 10:30:00').") from exc
    return ts if ts.tzinfo else ts.replace(tzinfo=datetime.timezone.utc)


# ---------------------------------------------------------------- reading the source log (raw actions, exact strings)

_CKPT_RE = re.compile(r"^(\d{20})\.checkpoint(?:\.(\d{10})\.(\d{10}))?\.parquet$")


def _checkpoint_at_or_before(log_dir: str, version: int):
    """(version, [part files]) of the newest classic checkpoint with version <= `version`, or None."""
    found: Dict[int, List[tuple]] = {}
    for name in os.listdir(log_dir):
        m = _CKPT_RE.match(name)
        if m and int(m.group(1)) <= version:
            found.setdefault(int(m.group(1)), []).append((int(m.group(2) or 1), int(m.group(3) or 1), name))
    for v in sorted(found, reverse=True):
        parts = found[v]
        if len({p[1] for p in parts}) == 1 and len(parts) == parts[0][1]:          # every part present
            return v, [os.path.join(log_dir, p[2]) for p in sorted(parts)]
    return None


def _live_adds(table_dir: str, version: int) -> Dict[str, Dict[str, Any]]:
    """The exact `add` actions live at `version`, by replaying checkpoint + JSON commits. Raises CloneError if the
    log cannot be replayed faithfully."""
    log_dir = os.path.join(table_dir, "_delta_log")
    state: Dict[str, Dict[str, Any]] = {}
    start = 0
    ckpt = _checkpoint_at_or_before(log_dir, version)
    if ckpt:
        start = ckpt[0] + 1
        for part in ckpt[1]:
            tbl = pq.read_table(part, columns=["add"])
            for row in tbl.column("add").to_pylist():
                if row:
                    pv = row.get("partitionValues")
                    row["partitionValues"] = dict(pv) if isinstance(pv, list) else (pv or {})
                    state[row["path"]] = row
    for v in range(start, version + 1):
        commit = os.path.join(log_dir, f"{v:020d}.json")
        if not os.path.exists(commit):
            raise CloneError(f"Version {version} cannot be cloned: log entry {v} no longer exists (history was cleaned up). "
                             "Clone a later version.")
        with open(commit) as f:
            for line in f:
                if not line.strip():
                    continue
                action = json.loads(line)
                if "add" in action:
                    state[action["add"]["path"]] = action["add"]
                elif "remove" in action:
                    state.pop(action["remove"]["path"], None)
    return state


# ---------------------------------------------------------------- the clone

def _open_source(src_dir: str, version: Optional[int], timestamp: Optional[str]) -> DeltaTable:
    if not os.path.isdir(os.path.join(src_dir, "_delta_log")):
        raise CloneError("The source table does not exist.")
    try:
        dt = DeltaTable(src_dir)
        if version is not None:
            dt.load_as_version(version)
        elif timestamp:
            dt.load_as_version(_parse_timestamp(timestamp))
        return dt
    except CloneError:
        raise
    except Exception as exc:
        raise CloneError(f"Could not open the source at that version: {exc}") from exc


def shallow_clone(src_dir: str, dst_dir: str, *, version: Optional[int] = None, timestamp: Optional[str] = None,
                  replace: bool = False, if_not_exists: bool = False, source_label: str = "", actor: str = "system") -> Dict[str, Any]:
    """Creates `dst_dir` as a shallow clone of `src_dir`. Returns a summary. Raises CloneError."""
    src_dir, dst_dir = os.path.abspath(src_dir), os.path.abspath(dst_dir)
    if src_dir == dst_dir:
        raise CloneError("A table cannot be cloned onto itself.")
    if os.path.exists(dst_dir):
        if if_not_exists:
            return {"created": False, "message": "Target already exists; nothing to do (IF NOT EXISTS)."}
        if not replace:
            raise CloneError("The target table already exists. Use CREATE OR REPLACE TABLE to replace it.")
        if not os.path.isdir(os.path.join(dst_dir, "_delta_log")):
            raise CloneError("The target path exists but is not a Delta table; refusing to replace it.")
    dt = _open_source(src_dir, version, timestamp)
    cloned_version = dt.version()

    proto = dt.protocol()
    features = {str(f).lower().replace("_", "") for f in list(proto.reader_features or []) + list(proto.writer_features or [])}
    bad = sorted(f for f in features if f in REFUSED_FEATURES)
    if bad:
        raise CloneError(f"This table uses Delta features a clone cannot share safely ({', '.join(bad)}).")

    adds = _live_adds(src_dir, cloned_version)
    expected = {a["path"] for a in pa.table(dt.get_add_actions(flatten=True)).to_pylist()}
    if set(adds) != expected:
        raise CloneError("The source's transaction log could not be replayed consistently; refusing to make a wrong clone.")
    for path, add in adds.items():
        if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", path) or path.startswith("/"):
            raise CloneError("The source references files by absolute path (an external shallow clone); clone it from its origin.")
        if add.get("deletionVector"):
            raise CloneError("The source has rows removed through deletion vectors, which a clone cannot share safely.")

    parent = os.path.dirname(dst_dir)
    os.makedirs(parent, exist_ok=True)
    tmp = os.path.join(parent, f".{os.path.basename(dst_dir)}.clone-{uuid.uuid4().hex[:8]}")
    linked = 0
    try:
        for path in adds:
            rel = unquote(path)
            src_file, dst_file = os.path.join(src_dir, rel), os.path.join(tmp, rel)
            if not os.path.commonpath([os.path.abspath(src_file), src_dir]) == src_dir:
                raise CloneError("The source log references a file outside the table; refusing.")
            os.makedirs(os.path.dirname(dst_file), exist_ok=True)
            try:
                os.link(src_file, dst_file)
            except FileNotFoundError:
                raise CloneError(f"A data file of the source is missing on disk ({rel}); the source is damaged or was vacuumed "
                                 "past this version.")
            except OSError as exc:
                raise CloneError("The clone could not hard-link the source's data files (source and target must be on the same "
                                 f"local filesystem): {exc}")
            linked += 1

        now_ms = int(time.time() * 1000)
        meta = dt.metadata()
        protocol = {"minReaderVersion": proto.min_reader_version, "minWriterVersion": proto.min_writer_version}
        if proto.reader_features is not None:
            protocol["readerFeatures"] = [str(f) for f in proto.reader_features]
        if proto.writer_features is not None:
            protocol["writerFeatures"] = [str(f) for f in proto.writer_features]
        actions: List[Dict[str, Any]] = [
            {"commitInfo": {"timestamp": now_ms, "operation": "CLONE", "engineInfo": "data-kiln-works",
                            "operationParameters": {"source": source_label or src_dir, "sourceVersion": cloned_version, "isShallow": True,
                                                    "user": actor},
                            "operationMetrics": {"numCopiedFiles": 0, "sourceNumOfFiles": linked}}},
            {"protocol": protocol},
            {"metaData": {"id": str(uuid.uuid4()), "name": None, "description": meta.description,
                          "format": {"provider": "parquet", "options": {}}, "schemaString": dt.schema().to_json(),
                          "partitionColumns": list(meta.partition_columns), "createdTime": now_ms,
                          "configuration": dict(meta.configuration or {})}},
        ]
        size = rows = 0
        for path, add in adds.items():
            new_add = {"path": path, "partitionValues": add.get("partitionValues") or {}, "size": add["size"],
                       "modificationTime": add["modificationTime"], "dataChange": True, "stats": add.get("stats")}
            if add.get("tags"):
                new_add["tags"] = add["tags"]
            actions.append({"add": new_add})
            size += int(add["size"])
            if add.get("stats"):
                try:
                    rows += int(json.loads(add["stats"]).get("numRecords") or 0)
                except (ValueError, TypeError):
                    pass
        log_dir = os.path.join(tmp, "_delta_log")
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, f"{0:020d}.json"), "w") as f:
            f.write("\n".join(json.dumps(a) for a in actions) + "\n")

        check = DeltaTable(tmp)                                   # must open, at version 0, with the same files
        if check.version() != 0 or check.schema().to_json() != dt.schema().to_json() \
                or len(check.file_uris()) != len(adds):
            raise CloneError("The clone failed its own consistency check and was discarded.")

        old = None
        if os.path.exists(dst_dir):                               # CREATE OR REPLACE: swap, then drop the old table
            old = os.path.join(parent, f".{os.path.basename(dst_dir)}.old-{uuid.uuid4().hex[:8]}")
            os.rename(dst_dir, old)
        try:
            os.rename(tmp, dst_dir)
        except Exception:
            if old:
                os.rename(old, dst_dir)
            raise
        if old:
            shutil.rmtree(old, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return {"created": True, "source_version": cloned_version, "files": linked, "size_bytes": size, "rows": rows or None,
            "message": f"Shallow clone created from version {cloned_version} ({linked} data file(s) shared, none copied)."}
