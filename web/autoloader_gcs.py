"""
Google Cloud Storage as an Auto-Loader source, mirroring web/autoloader_s3.py — through GCS's S3-compatible
*interoperability* (XML) API, not the native Google Cloud SDK. GCS's HMAC keys (Cloud Storage > Settings >
Interoperability) authenticate exactly like an S3 access key/secret, so a "gcs" storage mount holds a bucket plus an
HMAC key_id/secret (and, for tests, an endpoint override standing in for `storage.googleapis.com`), and this module
reuses web/autoloader_s3.py's boto3 client and quarantine code unchanged. Only URL parsing (`gcs://bucket/prefix`,
DuckDB's own name for this backend, not the more common `gs://`) and mount resolution are GCS-specific.

DuckDB's `TYPE gcs` secret has a real quirk worth keeping in mind: when its `SCOPE` does not name a bucket, DuckDB
silently ignores the secret's `ENDPOINT` and talks to the real `storage.googleapis.com` instead (verified directly:
`SCOPE 'gcs://'` fell back to Google even with a working local ENDPOINT override, while `SCOPE 'gcs://<bucket>'`
honoured it). `configure_duckdb` below therefore always scopes the secret to the exact bucket being read.

Real GCS was not available to verify this against; scratch/test_autoloader_gcs.py runs it against a throwaway
Deuxfleurs Garage container instead (a real S3-compatible server standing in for GCS's own S3-compatible endpoint),
so Google-specific quirks of the interoperability API beyond the one above are unverified.
"""

import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from web import autoloader_s3

logger = logging.getLogger("localspark.autoloader.gcs")

DEFAULT_ENDPOINT = "storage.googleapis.com"
QUARANTINE_DIR = autoloader_s3.QUARANTINE_DIR
is_ignored_key = autoloader_s3.is_ignored_key
_describe = autoloader_s3._describe


class GcsSourceError(Exception):
    """A problem with the GCS source (bad path, no mount, unreachable, access denied); the message is user-safe."""


SOURCE_ERROR = GcsSourceError


@dataclass
class RemoteObject:
    bucket: str
    key: str
    rel_key: str
    size: int
    etag: str
    last_modified: datetime

    @property
    def url(self) -> str:
        return f"gcs://{self.bucket}/{self.key}"


def is_gcs_path(path: Optional[str]) -> bool:
    return bool(path) and path.strip().lower().startswith("gcs://")


def parse_gcs_path(path: str) -> Tuple[str, str]:
    """('bucket', 'prefix/') from gcs://bucket/prefix (prefix is '' or ends with '/')."""
    u = urlparse((path or "").strip())
    if u.scheme.lower() != "gcs" or not u.netloc:
        raise GcsSourceError("A GCS source looks like gcs://bucket/optional/prefix/")
    prefix = u.path.lstrip("/")
    if ".." in prefix.split("/"):
        raise GcsSourceError("The GCS prefix must not contain '..'.")
    return u.netloc, (prefix if not prefix or prefix.endswith("/") else prefix + "/")


def normalize_path(path: str) -> str:
    bucket, prefix = parse_gcs_path(path)
    return f"gcs://{bucket}/{prefix}"


def resolve_connection(pipeline: Dict[str, Any]) -> Dict[str, Any]:
    """Same shape as autoloader_s3.resolve_connection, resolved against 'gcs' mounts and defaulted to Google's endpoint."""
    from web.mounts import load_mounts
    bucket, _ = parse_gcs_path(pipeline["source_volume_path"])
    gcs_mounts = [m for m in load_mounts() if (m.get("type") or "").lower() == "gcs"]
    wanted = (pipeline.get("source_mount_id") or "").strip()
    if wanted:
        mount = next((m for m in gcs_mounts if wanted in (m.get("id"), m.get("catalog_name"), m.get("name"))), None)
        if not mount:
            raise GcsSourceError(f"The storage mount '{wanted}' does not exist or is not a GCS mount.")
    else:
        mount = next((m for m in gcs_mounts if (m.get("config") or {}).get("bucket") == bucket), None) or (gcs_mounts[0] if gcs_mounts else None)
        if not mount:
            raise GcsSourceError("No GCS storage mount is configured. Add one under Storage Mounts first.")
    cfg = dict(mount.get("config") or {})
    endpoint = (cfg.get("endpoint") or DEFAULT_ENDPOINT).strip() or DEFAULT_ENDPOINT
    use_ssl = bool(cfg.get("use_ssl", endpoint == DEFAULT_ENDPOINT))
    if endpoint.lower().startswith("https://"):
        use_ssl, endpoint = True, endpoint[8:]
    elif endpoint.lower().startswith("http://"):
        use_ssl, endpoint = False, endpoint[7:]
    return {"endpoint": endpoint.rstrip("/"), "use_ssl": use_ssl, "region": (cfg.get("region") or "auto").strip(),
            "key_id": (cfg.get("key_id") or "").strip(), "secret": (cfg.get("secret") or "").strip(),
            "url_style": (cfg.get("url_style") or "path").strip(), "mount": mount.get("id")}


make_client = autoloader_s3.make_client


def list_objects(pipeline: Dict[str, Any], client=None) -> List[RemoteObject]:
    bucket, prefix = parse_gcs_path(pipeline["source_volume_path"])
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
    except GcsSourceError:
        raise
    except Exception as exc:
        raise GcsSourceError(f"Could not list gcs://{bucket}/{prefix}: {_describe(exc)}") from exc
    found.sort(key=lambda o: (o.last_modified, o.key))
    return found


def fingerprint(obj: RemoteObject) -> str:
    return hashlib.sha256(f"gcs|{obj.bucket}|{obj.key}|{obj.size}|{obj.etag}".encode()).hexdigest()


def configure_duckdb(duck_conn, conn: Dict[str, Any], bucket: str):
    """Points a DuckDB connection's httpfs at this GCS bucket. The secret is scoped to the exact bucket: see the module
    docstring for why an unscoped 'gcs' secret silently ignores ENDPOINT and talks to real Google instead."""
    def q(v: str) -> str:
        return (v or "").replace("'", "''")
    duck_conn.execute("INSTALL httpfs; LOAD httpfs;")
    duck_conn.execute("SET http_timeout = 30000; SET http_retries = 3;")
    duck_conn.execute(f"""
        CREATE OR REPLACE SECRET dkw_autoloader (
            TYPE GCS, KEY_ID '{q(conn['key_id'])}', SECRET '{q(conn['secret'])}', ENDPOINT '{q(conn['endpoint'])}',
            URL_STYLE '{q(conn['url_style'])}', USE_SSL {'true' if conn['use_ssl'] else 'false'}, REGION '{q(conn['region'])}',
            SCOPE 'gcs://{q(bucket)}')
    """)


def quarantine(pipeline: Dict[str, Any], obj: RemoteObject, client=None) -> str:
    """Copies a malformed object under `<prefix>/_quarantine/` and removes the original if allowed.
    Returns 'moved', or 'copied' (original could not be deleted) or 'left' (nothing could be written)."""
    client = client or make_client(resolve_connection(pipeline))
    _, prefix = parse_gcs_path(pipeline["source_volume_path"])
    dest = f"{prefix}{QUARANTINE_DIR}/{obj.key.rsplit('/', 1)[-1]}.{int(time.time())}.bad"
    try:
        client.copy_object(Bucket=obj.bucket, Key=dest, CopySource={"Bucket": obj.bucket, "Key": obj.key})
    except Exception as exc:
        logger.warning(f"Could not copy {obj.url} to quarantine: {_describe(exc)}")
        return "left"
    try:
        client.delete_object(Bucket=obj.bucket, Key=obj.key)
        logger.warning(f"Quarantined corrupt object {obj.url} -> gcs://{obj.bucket}/{dest}")
        return "moved"
    except Exception as exc:
        logger.warning(f"Copied {obj.url} to quarantine but could not delete the original: {_describe(exc)}")
        return "copied"
