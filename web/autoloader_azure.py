"""
Azure Blob Storage (and ADLS Gen2 accounts addressed through their Blob endpoint) as an Auto-Loader source, mirroring
web/autoloader_s3.py.

A pipeline whose `source_volume_path` is `azure://container/prefix/` is *polled*: each cycle lists the prefix with the
`azure-storage-blob` SDK, skips blobs already checkpointed, and streams each new blob straight from Azure through
DuckDB (the `azure` extension) into Delta, so nothing is downloaded to disk. Everything downstream (schema policies,
merge / append, exactly-once checkpoints, per-file history) is the same code as for local volumes and S3.

Differences from S3, all deliberate:
  * Identity of a blob is its name + size + ETag (a replaced blob has a new ETag, like a changed mtime locally).
  * Discovery filters mirror the local scan and the S3 one: hidden path segments, `_quarantine/`, `.tmp`/`.part`
    names and blobs not matching the pattern are skipped.
  * Quarantine copies the blob under `<prefix>/_quarantine/` with a server-side copy (same storage account, no SAS
    needed) and then deletes the original if the credentials allow; if they do not, the blob stays put and is
    recorded as QUARANTINED so it is never retried in a loop.
  * inotify has nothing to watch here, so file-event triggering is refused for Azure sources; use an interval or
    cron. S3 bucket-event webhooks have no Azure equivalent in this build (Event Grid is not consumed).

Credentials come from a configured Azure storage mount (Platform > Storage Mounts, type "azure"): either a full
`connection_string`, or `account_name` + `account_key` (+ an optional `account_url` override, for Azurite in tests).
The pipeline's `source_mount_id` if set, else an Azure mount whose container matches, else the first Azure mount.
"""

import fnmatch
import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger("localspark.autoloader.azure")

QUARANTINE_DIR = "_quarantine"


class AzureSourceError(Exception):
    """A problem with the Azure Blob source (bad path, no mount, unreachable, access denied); the message is user-safe."""


SOURCE_ERROR = AzureSourceError


@dataclass
class RemoteObject:
    bucket: str          # the container, named `bucket` so it lines up with autoloader_s3.RemoteObject / process_single_file
    key: str
    rel_key: str
    size: int
    etag: str
    last_modified: datetime

    @property
    def url(self) -> str:
        return f"azure://{self.bucket}/{self.key}"


def is_azure_path(path: Optional[str]) -> bool:
    return bool(path) and path.strip().lower().startswith("azure://")


def parse_azure_path(path: str) -> Tuple[str, str]:
    """('container', 'prefix/') from azure://container/prefix (prefix is '' or ends with '/')."""
    u = urlparse((path or "").strip())
    if u.scheme.lower() != "azure" or not u.netloc:
        raise AzureSourceError("An Azure Blob source looks like azure://container/optional/prefix/")
    prefix = u.path.lstrip("/")
    if ".." in prefix.split("/"):
        raise AzureSourceError("The Azure prefix must not contain '..'.")
    return u.netloc, (prefix if not prefix or prefix.endswith("/") else prefix + "/")


def normalize_path(path: str) -> str:
    container, prefix = parse_azure_path(path)
    return f"azure://{container}/{prefix}"


def _connection_string(cfg: Dict[str, Any]) -> str:
    cs = (cfg.get("connection_string") or "").strip()
    if cs:
        return cs
    account = (cfg.get("account_name") or "").strip()
    key = (cfg.get("account_key") or "").strip()
    if not account or not key:
        raise AzureSourceError("Set either a connection string, or an account name and key, for this mount.")
    url = (cfg.get("account_url") or "").strip()
    if url:
        proto = "https" if url.lower().startswith("https://") else "http"
        return f"DefaultEndpointsProtocol={proto};AccountName={account};AccountKey={key};BlobEndpoint={url};"
    return f"DefaultEndpointsProtocol=https;AccountName={account};AccountKey={key};EndpointSuffix=core.windows.net;"


def resolve_connection(pipeline: Dict[str, Any]) -> Dict[str, Any]:
    from web.mounts import load_mounts
    container, _ = parse_azure_path(pipeline["source_volume_path"])
    az_mounts = [m for m in load_mounts() if (m.get("type") or "").lower() == "azure"]
    wanted = (pipeline.get("source_mount_id") or "").strip()
    if wanted:
        mount = next((m for m in az_mounts if wanted in (m.get("id"), m.get("catalog_name"), m.get("name"))), None)
        if not mount:
            raise AzureSourceError(f"The storage mount '{wanted}' does not exist or is not an Azure mount.")
    else:
        mount = next((m for m in az_mounts if (m.get("config") or {}).get("container") == container), None) or (az_mounts[0] if az_mounts else None)
        if not mount:
            raise AzureSourceError("No Azure storage mount is configured. Add one under Storage Mounts first.")
    cfg = dict(mount.get("config") or {})
    return {"connection_string": _connection_string(cfg), "mount": mount.get("id")}


def make_client(conn: Dict[str, Any]):
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise AzureSourceError("azure-storage-blob is not installed in this image; rebuild it (docker compose build) to use Azure sources.") from exc
    try:
        return BlobServiceClient.from_connection_string(conn["connection_string"])
    except Exception as exc:
        raise AzureSourceError(f"Could not build an Azure client from this mount's connection string: {_describe(exc)}") from exc


def _describe(exc: Exception) -> str:
    code = getattr(exc, "error_code", None)
    if code in ("ContainerNotFound",):
        return "the container does not exist"
    if code in ("AuthenticationFailed", "AuthorizationFailure", "InvalidAuthenticationInfo"):
        return f"access denied ({code}); check the mount's account name/key and its permissions on this container"
    status = getattr(exc, "status_code", None)
    return f"{code or type(exc).__name__}{f' ({status})' if status else ''}: {exc}"[:200]


def is_ignored_key(rel_key: str, pattern: str) -> bool:
    """Mirror of the local scan's filters, applied to a blob name relative to the prefix."""
    if not rel_key or rel_key.endswith("/"):
        return True
    parts = rel_key.split("/")
    if any(p.startswith(".") for p in parts) or QUARANTINE_DIR in parts[:-1]:
        return True
    name = parts[-1]
    if name.endswith(".tmp") or name.endswith(".part"):
        return True
    return not (pattern == "*" or fnmatch.fnmatch(name.lower(), pattern.lower()))


def list_objects(pipeline: Dict[str, Any], client=None) -> List[RemoteObject]:
    """Every blob under the pipeline's prefix that the scan would consider, oldest first."""
    container, prefix = parse_azure_path(pipeline["source_volume_path"])
    pattern = pipeline.get("file_pattern") or "*"
    client = client or make_client(resolve_connection(pipeline))
    found: List[RemoteObject] = []
    try:
        cc = client.get_container_client(container)
        for b in cc.list_blobs(name_starts_with=prefix):
            rel = b.name[len(prefix):]
            if is_ignored_key(rel, pattern):
                continue
            lm = b.last_modified
            if lm is not None and lm.tzinfo is None:
                lm = lm.replace(tzinfo=timezone.utc)
            found.append(RemoteObject(container, b.name, rel, int(b.size or 0), (b.etag or "").strip('"'), lm or datetime.now(timezone.utc)))
    except AzureSourceError:
        raise
    except Exception as exc:
        raise AzureSourceError(f"Could not list azure://{container}/{prefix}: {_describe(exc)}") from exc
    found.sort(key=lambda o: (o.last_modified, o.key))
    return found


def fingerprint(obj: RemoteObject) -> str:
    return hashlib.sha256(f"azure|{obj.bucket}|{obj.key}|{obj.size}|{obj.etag}".encode()).hexdigest()


def configure_duckdb(duck_conn, conn: Dict[str, Any], bucket: Optional[str] = None):
    """Points a DuckDB connection's `azure` extension at this mount so `azure://` URLs read straight from the container."""
    def q(v: str) -> str:
        return (v or "").replace("'", "''")
    duck_conn.execute("INSTALL azure; LOAD azure;")
    duck_conn.execute(f"CREATE OR REPLACE SECRET dkw_autoloader (TYPE azure, CONNECTION_STRING '{q(conn['connection_string'])}')")


def quarantine(pipeline: Dict[str, Any], obj: RemoteObject, client=None) -> str:
    """Copies a malformed blob under `<prefix>/_quarantine/` (server-side, same account, no SAS needed) and removes the
    original if allowed. Returns 'moved', or 'copied' (original could not be deleted) or 'left' (nothing could be written)."""
    client = client or make_client(resolve_connection(pipeline))
    _, prefix = parse_azure_path(pipeline["source_volume_path"])
    dest_name = f"{prefix}{QUARANTINE_DIR}/{obj.key.rsplit('/', 1)[-1]}.{int(time.time())}.bad"
    cc = client.get_container_client(obj.bucket)
    src = cc.get_blob_client(obj.key)
    dest = cc.get_blob_client(dest_name)
    try:
        copy = dest.start_copy_from_url(src.url)
        deadline = time.time() + 30
        while dest.get_blob_properties().copy.status == "pending" and time.time() < deadline:
            time.sleep(0.5)
    except Exception as exc:
        logger.warning(f"Could not copy {obj.url} to quarantine: {_describe(exc)}")
        return "left"
    try:
        src.delete_blob()
        logger.warning(f"Quarantined corrupt blob {obj.url} -> azure://{obj.bucket}/{dest_name}")
        return "moved"
    except Exception as exc:
        logger.warning(f"Copied {obj.url} to quarantine but could not delete the original: {_describe(exc)}")
        return "copied"
