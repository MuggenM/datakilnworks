"""
OneLake Lakehouse Integration
Mount Microsoft Fabric OneLake lakehouses as read-only external catalogs.
"""

import hashlib
import os
import logging
import subprocess
import threading
from typing import Dict, Any, List, Optional
import duckdb
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import DeltaTable
import pandas as pd

logger = logging.getLogger("datakilnworks.onelake")

# Global registry of mounted OneLake catalogs
ONELAKE_CATALOGS: Dict[str, Dict[str, Any]] = {}

_CA_CERT_DIR = "/usr/local/share/ca-certificates"
_SYSTEM_CA_BUNDLE = "/etc/ssl/certs/ca-certificates.crt"
_ca_cert_lock = threading.Lock()


def _cert_file(catalog_id: str) -> str:
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in catalog_id)
    return os.path.join(_CA_CERT_DIR, f"onelake_{safe}.crt")


def _ensure_requests_trust_system_store() -> None:
    """`requests` (and MSAL/azure-identity, which use it internally) ignores the OS trust store by
    default -- it verifies against certifi's own bundled list unless told otherwise via the
    REQUESTS_CA_BUNDLE env var (verified directly: SSL_CERT_FILE alone, which delta-rs's own Rust TLS
    stack DOES honor automatically with no env var at all, is not enough for `requests`). Unlike a
    process-start env var, this one can be set at runtime with no restart: `requests` re-reads
    os.environ on every call (Session.merge_environment_settings), so setting it here, once, makes
    every subsequent MSAL/azure-identity call in this process trust whatever update-ca-certificates
    just added to the system bundle -- the same trust store delta-rs already reads. Only sets it when
    unset, so an operator's own REQUESTS_CA_BUNDLE is never overridden."""
    if not os.environ.get("REQUESTS_CA_BUNDLE"):
        os.environ["REQUESTS_CA_BUNDLE"] = _SYSTEM_CA_BUNDLE


def trust_ca_cert(catalog_id: str, pem: str) -> None:
    """Trusts `pem` for THIS CONTAINER, not just this one catalog's connections: there is no per-request
    CA trust knob in either the Azure SDKs (ClientSecretCredential, DataLakeServiceClient) or delta-rs's
    own Rust TLS stack (verified directly -- neither honors a storage_options key). Adding it to the
    system trust store (update-ca-certificates) plus pointing REQUESTS_CA_BUNDLE at that same store
    (see _ensure_requests_trust_system_store) is therefore the only combination that reaches both
    delta-rs and the Azure SDKs' MSAL-based auth, on every connection, with no restart -- at the cost of
    trusting it container-wide for as long as it stays installed. Meant for a local dev/test identity
    provider or OneLake-shaped emulator (see CLAUDE.md); pasting a real, sensitive CA here would be a
    mistake for the same reason. Idempotent: re-mounting the same catalog with the same cert is a no-op
    after the first call. Root-only (the studio runs as root in this image); logs and gives up quietly
    otherwise, since a real tenant's cert is already trusted and never needs this."""
    if not pem or not pem.strip():
        return
    path = _cert_file(catalog_id)
    with _ca_cert_lock:
        _ensure_requests_trust_system_store()
        existing = None
        if os.path.exists(path):
            with open(path) as f:
                existing = f.read()
        if existing == pem:
            return
        try:
            os.makedirs(_CA_CERT_DIR, exist_ok=True)
            with open(path, "w") as f:
                f.write(pem)
            subprocess.run(["update-ca-certificates"], check=True, capture_output=True, timeout=30)
            logger.warning(f"Trusted a pasted CA certificate for OneLake catalog {catalog_id} -- container-wide, dev/test only")
        except Exception as e:
            logger.error(f"Could not trust the pasted CA certificate for {catalog_id}: {e}")


def untrust_ca_cert(catalog_id: str) -> None:
    path = _cert_file(catalog_id)
    with _ca_cert_lock:
        if not os.path.exists(path):
            return
        try:
            os.remove(path)
            subprocess.run(["update-ca-certificates", "--fresh"], check=True, capture_output=True, timeout=30)
        except Exception as e:
            logger.error(f"Could not remove the trusted CA certificate for {catalog_id}: {e}")


class OneLakeCredentials:
    """Manage OneLake authentication credentials."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str, authority: Optional[str] = None):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        # A non-default AAD authority (e.g. a local identity-provider emulator's own endpoint). None (the
        # default, and the only sane thing for a real tenant) means real Azure AD, exactly as before.
        self.authority = (authority or "").strip() or None
        self._credential = None

    @property
    def credential(self):
        """Get Azure credential object."""
        if not self._credential:
            kwargs = {}
            if self.authority:
                kwargs["authority"] = self.authority
            self._credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
                # MSAL's "is this authority actually Microsoft" lookup refuses any authority it does not
                # already know about -- which is every authority but the real login.microsoftonline.com,
                # including a local identity-provider emulator's. Harmless against a real tenant: it only
                # skips that preliminary discovery call, never changes which token gets issued or what
                # gets validated.
                disable_instance_discovery=True,
                **kwargs
            )
        return self._credential

    def get_bearer_token(self) -> str:
        """Get bearer token for Azure Storage."""
        token = self.credential.get_token("https://storage.azure.com/.default")
        return token.token

    def to_dict(self) -> dict:
        """Export credentials (for storage - should be encrypted)."""
        return {
            'tenant_id': self.tenant_id,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'authority': self.authority
        }


class OneLakeCatalog:
    """Read-only OneLake lakehouse catalog."""

    def __init__(
        self,
        catalog_id: str,
        workspace: str,
        lakehouse: str,
        credentials: OneLakeCredentials
    ):
        self.catalog_id = catalog_id
        self.workspace = workspace
        # Add .Lakehouse suffix if not already present (Fabric OneLake requirement)
        self.lakehouse = lakehouse if lakehouse.endswith('.Lakehouse') else f"{lakehouse}.Lakehouse"
        self.lakehouse_display = lakehouse  # Keep original name for display
        self.credentials = credentials
        self.base_url = f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{self.lakehouse}"
        self.tables_cache: Optional[List[str]] = None

        logger.info(f"Initialized OneLake catalog: {catalog_id} (workspace={workspace}, lakehouse={self.lakehouse})")

    def _get_service_client(self) -> DataLakeServiceClient:
        """Get Azure Data Lake service client.

        `connection_verify` is passed explicitly (rather than left at its `True` default) because
        azure-core's `RequestsTransport` disables environment-based settings on its own `requests`
        session (`trust_env=False`), so unlike MSAL's own use of `requests` it never honors
        REQUESTS_CA_BUNDLE -- only `True`/`False`/an explicit path (verified directly: the same system
        bundle that already makes MSAL and delta-rs trust a pasted `ca_cert_pem`, above, left this SDK's
        own transport still rejecting it until passed here explicitly). Harmless for a real tenant: it is
        the OS's normal default root store, a superset of what `verify=True` would otherwise use."""
        return DataLakeServiceClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            credential=self.credentials.credential,
            connection_verify=_SYSTEM_CA_BUNDLE
        )

    def test_connection(self) -> bool:
        """Test if OneLake lakehouse is accessible."""
        try:
            service_client = self._get_service_client()
            # The ADLS Gen2 filesystem (container) is the WORKSPACE alone -- the lakehouse is a directory
            # within it, not part of the filesystem name (verified directly against a real OneLake-shaped
            # data plane: a filesystem name containing '/' is simply not a thing this API has).
            filesystem = service_client.get_file_system_client(self.workspace)
            # Managed tables are directly under Tables/, no Files/ nesting (verified directly against a
            # real OneLake-shaped data plane: a freshly-written managed table lands at
            # `<lakehouse>/Tables/<table>`, confirmed both by GUID- and display-name-based path). "Files/"
            # is Fabric's SEPARATE top-level area for unmanaged/raw data, unrelated to managed tables.
            # recursive=False: see list_tables for why (a recursive listing never reports a directory as one).
            list(filesystem.get_paths(path=f"{self.lakehouse}/Tables", recursive=False, max_results=1))
            logger.info(f"OneLake connection test successful: {self.catalog_id}")
            return True
        except Exception as e:
            logger.error(f"OneLake connection test failed: {e}")
            return False

    def _is_skippable_name(self, name: str) -> bool:
        return name.startswith('_') or 'year=' in name or 'month=' in name

    def _is_table_dir(self, filesystem, dir_path: str) -> bool:
        """A Delta table's own directory always has a _delta_log child; a category folder (one level of
        grouping between Tables/ and the tables themselves) does not."""
        try:
            next(iter(filesystem.get_paths(path=f"{dir_path}/_delta_log", recursive=False, max_results=1)))
            return True
        except StopIteration:
            return False
        except Exception:
            return False

    def list_tables(self, force_refresh: bool = False) -> List[str]:
        """List all Delta tables in OneLake lakehouse."""
        if self.tables_cache and not force_refresh:
            return self.tables_cache

        try:
            service_client = self._get_service_client()
            filesystem = service_client.get_file_system_client(self.workspace)

            # Managed tables are directly under Tables/ (not Tables/Files/ -- verified directly, see
            # test_connection). recursive=False is essential here: a recursive listing returns only leaf
            # FILES (never a directory entry for the table folder itself, so `path.is_directory` would
            # never be true), while a non-recursive listing at a given path returns exactly its immediate
            # children, correctly flagged as files or directories -- which is what distinguishing a table
            # folder from a plain file needs.
            top = list(filesystem.get_paths(path=f"{self.lakehouse}/Tables", recursive=False))

            tables: List[str] = []
            for entry in top:
                name = entry.name.rsplit('/', 1)[-1]
                if not entry.is_directory or self._is_skippable_name(name):
                    continue
                if self._is_table_dir(filesystem, entry.name):
                    # Direct child: {lakehouse}/Tables/table_name
                    tables.append(name)
                else:
                    # A category folder, one level deep: {lakehouse}/Tables/category/table_name
                    for nested in filesystem.get_paths(path=entry.name, recursive=False):
                        nested_name = nested.name.rsplit('/', 1)[-1]
                        if (nested.is_directory and not self._is_skippable_name(nested_name)
                                and self._is_table_dir(filesystem, nested.name)):
                            tables.append(nested_name)

            # Remove duplicates and sort
            tables = sorted(set(tables))

            self.tables_cache = tables
            logger.info(f"Found {len(tables)} tables in OneLake catalog {self.catalog_id}")

            return tables

        except Exception as e:
            logger.error(f"Failed to list OneLake tables: {e}")
            raise

    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Get metadata for a specific OneLake Delta table."""
        try:
            # OneLake tables are directly under Tables/ (verified directly, see test_connection)
            table_url = f"abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse}/Tables/{table_name}"

            storage_options = {
                'bearer_token': self.credentials.get_bearer_token(),
                'use_fabric_endpoint': 'true'
            }

            dt = DeltaTable(table_url, storage_options=storage_options)

            schema = dt.schema().to_arrow()  # to_pyarrow() was renamed to_arrow() in the pinned deltalake version

            metadata = {
                'name': table_name,
                'catalog': self.catalog_id,
                'source': 'onelake',
                'read_only': True,
                'delta_version': dt.version(),
                'num_files': len(dt.file_uris()),  # files() was renamed file_uris() in the pinned deltalake version
                'columns': [
                    {
                        'name': field.name,
                        'type': str(field.type),
                        'nullable': field.nullable
                    }
                    for field in schema
                ],
                'num_columns': len(schema),
                'location': table_url,
                'workspace': self.workspace,
                'lakehouse': self.lakehouse
            }

            return metadata

        except Exception as e:
            logger.error(f"Failed to get table metadata for {table_name}: {e}")
            raise

    def read_table(
        self,
        table_name: str,
        limit: Optional[int] = None,
        filters: Optional[List] = None
    ) -> pd.DataFrame:
        """Read OneLake Delta table as pandas DataFrame."""
        try:
            # OneLake tables are directly under Tables/ (verified directly, see test_connection)
            table_url = f"abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse}/Tables/{table_name}"

            storage_options = {
                'bearer_token': self.credentials.get_bearer_token(),
                'use_fabric_endpoint': 'true'
            }

            dt = DeltaTable(table_url, storage_options=storage_options)

            if filters:
                df = dt.to_pandas(filters=filters)
            else:
                df = dt.to_pandas()

            if limit:
                df = df.head(limit)

            logger.info(f"Read {len(df)} rows from OneLake table {table_name}")

            return df

        except Exception as e:
            logger.error(f"Failed to read OneLake table {table_name}: {e}")
            raise

    def query_with_duckdb(self, sql: str) -> pd.DataFrame:
        """
        Execute SQL query against OneLake tables using DuckDB.
        Replaces table names with OneLake URLs in the query.
        """
        try:
            # For each table in the catalog, register it with DuckDB
            conn = duckdb.connect()

            # Install and load Azure extension
            try:
                conn.execute("INSTALL azure;")
            except:
                pass  # Already installed
            conn.execute("LOAD azure;")

            # Create Azure secret for authentication
            secret_name = f"onelake_{self.catalog_id}"
            conn.execute(f"""
                CREATE OR REPLACE SECRET {secret_name} (
                    TYPE AZURE,
                    PROVIDER SERVICE_PRINCIPAL,
                    TENANT_ID '{self.credentials.tenant_id}',
                    CLIENT_ID '{self.credentials.client_id}',
                    CLIENT_SECRET '{self.credentials.client_secret}',
                    ACCOUNT_NAME 'onelake'
                );
            """)

            # Replace table references with delta_scan
            modified_sql = sql
            for table_name in self.list_tables():
                # OneLake tables are directly under Tables/ (verified directly, see test_connection)
                table_url = f"abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse}/Tables/{table_name}"
                # Replace table name with delta_scan
                modified_sql = modified_sql.replace(
                    f"{self.catalog_id}.{table_name}",
                    f"delta_scan('{table_url}')"
                )
                modified_sql = modified_sql.replace(
                    f"{table_name}",
                    f"delta_scan('{table_url}')"
                )

            result = conn.execute(modified_sql).df()

            logger.info(f"OneLake query returned {len(result)} rows")

            return result

        except Exception as e:
            logger.error(f"Failed to execute OneLake query: {e}")
            raise

    def to_dict(self) -> dict:
        """Export catalog metadata."""
        return {
            'catalog_id': self.catalog_id,
            'workspace': self.workspace,
            'lakehouse': self.lakehouse,
            'type': 'onelake',
            'read_only': True,
            'table_count': len(self.tables_cache) if self.tables_cache else 0,
            'base_url': self.base_url
        }


def mount_onelake_catalog(
    workspace: str,
    lakehouse: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    catalog_id: Optional[str] = None,
    authority: Optional[str] = None,
    ca_cert_pem: Optional[str] = None
) -> OneLakeCatalog:
    """
    Mount OneLake lakehouse as read-only external catalog.

    Args:
        workspace: Fabric workspace name
        lakehouse: Lakehouse name
        tenant_id: Azure AD tenant ID
        client_id: Service principal client ID
        client_secret: Service principal secret
        catalog_id: Optional custom catalog ID (defaults to onelake_{lakehouse})
        authority: Optional non-default AAD authority (a local identity-provider emulator's own endpoint,
            e.g. for testing against a OneLake-shaped emulator instead of real Fabric; see CLAUDE.md).
            Leave unset for a real tenant -- this is never needed there.
        ca_cert_pem: Optional CA certificate (PEM) to trust for that authority/data-plane's self-signed
            TLS, if it has one. Trusted for the whole container, not just this catalog (see
            trust_ca_cert): a real tenant's certificate is already trusted and never needs this.

    Returns:
        OneLakeCatalog instance
    """
    if not catalog_id:
        catalog_id = f"onelake_{lakehouse}"

    if ca_cert_pem:
        trust_ca_cert(catalog_id, ca_cert_pem)

    # Create credentials
    credentials = OneLakeCredentials(tenant_id, client_id, client_secret, authority=authority)

    # Create catalog
    catalog = OneLakeCatalog(catalog_id, workspace, lakehouse, credentials)

    # Test connection
    if not catalog.test_connection():
        raise ConnectionError(f"Failed to connect to OneLake: {workspace}/{lakehouse}")

    # Discover tables
    tables = catalog.list_tables()

    # Register in global registry
    ONELAKE_CATALOGS[catalog_id] = catalog

    logger.info(f"Mounted OneLake catalog {catalog_id} with {len(tables)} tables")

    return catalog


def unmount_onelake_catalog(catalog_id: str) -> bool:
    """Unmount OneLake catalog."""
    untrust_ca_cert(catalog_id)
    if catalog_id in ONELAKE_CATALOGS:
        del ONELAKE_CATALOGS[catalog_id]
        logger.info(f"Unmounted OneLake catalog: {catalog_id}")
        return True
    return False


def get_onelake_catalog(catalog_id: str) -> Optional[OneLakeCatalog]:
    """Get mounted OneLake catalog by ID."""
    return ONELAKE_CATALOGS.get(catalog_id)


def list_onelake_catalogs() -> List[Dict[str, Any]]:
    """List all mounted OneLake catalogs."""
    return [catalog.to_dict() for catalog in ONELAKE_CATALOGS.values()]
