"""
S3 (and S3-compatible: MinIO, Garage, Ceph, R2, ...) sources for Auto-Loader pipelines.

A pipeline whose `source_volume_path` is `s3://bucket/prefix/` is *polled*: each cycle lists the prefix with
`ListObjectsV2` (paginated), skips objects already checkpointed, and streams each new object straight from S3 through
DuckDB (httpfs) into Delta, so nothing is downloaded to disk. Everything downstream (schema policies, merge / append,
exactly-once checkpoints, per-file history) is the same code as for local volumes.

Differences from a local volume, all deliberate:
  * Identity of an object is its key + size + ETag (a replaced object with new content has a new ETag and is loaded
    again, like a changed mtime locally). There are no half-written files: S3 publishes an object atomically.
  * Discovery filters mirror the local scan: hidden path segments, `_quarantine/`, `.tmp`/`.part` names, directory
    marker keys and files not matching the pattern are skipped.
  * Quarantine copies the object under `<prefix>/_quarantine/` and then deletes the original if the credentials allow;
    if they do not, the object stays put and is recorded as QUARANTINED so it is never retried in a loop.
  * inotify has nothing to watch here, so file-event triggering is refused for S3 sources; use an interval or cron.
    Each poll costs one list request per 1000 objects under the prefix, so keep the interval sensible.

Credentials and endpoint come from a configured S3 storage mount (Platform > Storage Mounts): the pipeline's
`source_mount_id` if set, else an S3 mount whose bucket matches, else the first S3 mount.
"""

import fnmatch
import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger("localspark.autoloader.s3")

QUARANTINE_DIR = "_quarantine"


class S3SourceError(Exception):
    """A problem with the S3 source (bad path, no mount, unreachable, access denied); the message is user-safe."""


@dataclass
class RemoteObject:
    bucket: str
    key: str
    rel_key: str            # key relative to the pipeline's prefix (what history shows)
    size: int
    etag: str
    last_modified: datetime

    @property
    def url(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


def is_s3_path(path: Optional[str]) -> bool:
    return bool(path) and path.strip().lower().startswith("s3://")


def parse_s3_path(path: str) -> Tuple[str, str]:
    """('bucket', 'prefix/') from s3://bucket/prefix (prefix is '' or ends with '/')."""
    u = urlparse((path or "").strip())
    if u.scheme.lower() != "s3" or not u.netloc:
        raise S3SourceError("An S3 source looks like s3://bucket/optional/prefix/")
    prefix = u.path.lstrip("/")
    if ".." in prefix.split("/"):
        raise S3SourceError("The S3 prefix must not contain '..'.")
    return u.netloc, (prefix if not prefix or prefix.endswith("/") else prefix + "/")


def normalize_path(path: str) -> str:
    bucket, prefix = parse_s3_path(path)
    return f"s3://{bucket}/{prefix}"


def resolve_connection(pipeline: Dict[str, Any]) -> Dict[str, Any]:
    from web.mounts import load_mounts
    bucket, _ = parse_s3_path(pipeline["source_volume_path"])
    s3_mounts = [m for m in load_mounts() if (m.get("type") or "").lower() == "s3"]
    wanted = (pipeline.get("source_mount_id") or "").strip()
    if wanted:
        mount = next((m for m in s3_mounts if wanted in (m.get("id"), m.get("catalog_name"), m.get("name"))), None)
        if not mount:
            raise S3SourceError(f"The storage mount '{wanted}' does not exist or is not an S3 mount.")
    else:
        mount = next((m for m in s3_mounts if (m.get("config") or {}).get("bucket") == bucket), None) or (s3_mounts[0] if s3_mounts else None)
        if not mount:
            raise S3SourceError("No S3 storage mount is configured. Add one under Storage Mounts first.")
    cfg = dict(mount.get("config") or {})
    endpoint = (cfg.get("endpoint") or "").strip()
    use_ssl = bool(cfg.get("use_ssl", False))
    if endpoint.lower().startswith("https://"):
        use_ssl, endpoint = True, endpoint[8:]
    elif endpoint.lower().startswith("http://"):
        use_ssl, endpoint = False, endpoint[7:]
    return {"endpoint": endpoint.rstrip("/"), "use_ssl": use_ssl, "region": (cfg.get("region") or "us-east-1").strip(),
            "key_id": (cfg.get("key_id") or "").strip(), "secret": (cfg.get("secret") or "").strip(),
            "url_style": (cfg.get("url_style") or "path").strip(), "mount": mount.get("id")}


def make_client(conn: Dict[str, Any]):
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise S3SourceError("boto3 is not installed in this image; rebuild it (docker compose build) to use S3 sources.") from exc
    return boto3.client(
        "s3", endpoint_url=(f"{'https' if conn['use_ssl'] else 'http'}://{conn['endpoint']}" if conn["endpoint"] else None),
        aws_access_key_id=conn["key_id"] or None, aws_secret_access_key=conn["secret"] or None, region_name=conn["region"],
        config=Config(s3={"addressing_style": "path" if conn["url_style"] == "path" else "virtual"},
                      retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=5, read_timeout=30))


def _describe(exc: Exception) -> str:
    code = getattr(exc, "response", {}).get("Error", {}).get("Code") if hasattr(exc, "response") else None
    if code in ("NoSuchBucket",):
        return "the bucket does not exist"
    if code in ("AccessDenied", "403", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
        return f"access denied ({code}); check the mount's key and its permissions on this bucket"
    return f"{code or type(exc).__name__}: {exc}"[:200]


def is_ignored_key(rel_key: str, pattern: str) -> bool:
    """Mirror of the local scan's filters, applied to a key relative to the prefix."""
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
    """Every object under the pipeline's prefix that the scan would consider, oldest first."""
    bucket, prefix = parse_s3_path(pipeline["source_volume_path"])
    pattern = pipeline.get("file_pattern") or "*"
    client = client or make_client(resolve_connection(pipeline))
    found: List[RemoteObject] = []
    try:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                rel = o["Key"][len(prefix):]
                if is_ignored_key(rel, pattern):
                    continue
                found.append(RemoteObject(bucket, o["Key"], rel, int(o["Size"]), (o.get("ETag") or "").strip('"'), o["LastModified"]))
    except S3SourceError:
        raise
    except Exception as exc:
        raise S3SourceError(f"Could not list s3://{bucket}/{prefix}: {_describe(exc)}") from exc
    found.sort(key=lambda o: (o.last_modified, o.key))
    return found


def fingerprint(obj: RemoteObject) -> str:
    return hashlib.sha256(f"s3|{obj.bucket}|{obj.key}|{obj.size}|{obj.etag}".encode()).hexdigest()


def configure_duckdb(duck_conn, conn: Dict[str, Any]):
    """Point a DuckDB connection's httpfs at this mount so `s3://` URLs read straight from the bucket."""
    def q(v: str) -> str:
        return (v or "").replace("'", "''")
    duck_conn.execute("INSTALL httpfs; LOAD httpfs;")
    duck_conn.execute("SET http_timeout = 30000; SET http_retries = 3;")
    duck_conn.execute(f"""
        CREATE OR REPLACE SECRET dkw_autoloader (
            TYPE S3, KEY_ID '{q(conn['key_id'])}', SECRET '{q(conn['secret'])}', ENDPOINT '{q(conn['endpoint'])}',
            URL_STYLE '{q(conn['url_style'])}', USE_SSL {'true' if conn['use_ssl'] else 'false'}, REGION '{q(conn['region'])}')
    """)


def quarantine(pipeline: Dict[str, Any], obj: RemoteObject, client=None) -> str:
    """Copies a malformed object under `<prefix>/_quarantine/` and removes the original if allowed.
    Returns 'moved', or 'copied' (original could not be deleted) or 'left' (nothing could be written)."""
    client = client or make_client(resolve_connection(pipeline))
    _, prefix = parse_s3_path(pipeline["source_volume_path"])
    dest = f"{prefix}{QUARANTINE_DIR}/{obj.key.rsplit('/', 1)[-1]}.{int(time.time())}.bad"
    try:
        client.copy_object(Bucket=obj.bucket, Key=dest, CopySource={"Bucket": obj.bucket, "Key": obj.key})
    except Exception as exc:
        logger.warning(f"Could not copy {obj.url} to quarantine: {_describe(exc)}")
        return "left"
    try:
        client.delete_object(Bucket=obj.bucket, Key=obj.key)
        logger.warning(f"Quarantined corrupt object {obj.url} -> s3://{obj.bucket}/{dest}")
        return "moved"
    except Exception as exc:
        logger.warning(f"Copied {obj.url} to quarantine but could not delete the original: {_describe(exc)}")
        return "copied"
