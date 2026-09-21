"""
OneLake Lakehouse Integration
Mount Microsoft Fabric OneLake lakehouses as read-only external catalogs.
"""

import os
import logging
from typing import Dict, Any, List, Optional
import duckdb
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import DeltaTable
import pandas as pd

logger = logging.getLogger("datakilnworks.onelake")

# Global registry of mounted OneLake catalogs
ONELAKE_CATALOGS: Dict[str, Dict[str, Any]] = {}


class OneLakeCredentials:
    """Manage OneLake authentication credentials."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._credential = None

    @property
    def credential(self):
        """Get Azure credential object."""
        if not self._credential:
            self._credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret
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
            'client_secret': self.client_secret
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
        """Get Azure Data Lake service client."""
        return DataLakeServiceClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            credential=self.credentials.credential
        )

    def test_connection(self) -> bool:
        """Test if OneLake lakehouse is accessible."""
        try:
            service_client = self._get_service_client()
            filesystem = service_client.get_file_system_client(
                f"{self.workspace}/{self.lakehouse}"
            )
            # Try to list Tables/Files directory (OneLake lakehouse structure)
            list(filesystem.get_paths(path="Tables/Files", max_results=1))
            logger.info(f"OneLake connection test successful: {self.catalog_id}")
            return True
        except Exception as e:
            logger.error(f"OneLake connection test failed: {e}")
            return False

    def list_tables(self, force_refresh: bool = False) -> List[str]:
        """List all Delta tables in OneLake lakehouse."""
        if self.tables_cache and not force_refresh:
            return self.tables_cache

        try:
            service_client = self._get_service_client()
            filesystem = service_client.get_file_system_client(
                f"{self.workspace}/{self.lakehouse}"
            )

            # Managed tables are directly under Tables/ (not Files/Tables/)
            paths = filesystem.get_paths(path="Tables/Files")

            tables = []
            for path in paths:
                if path.is_directory and not path.name.startswith('_') and not 'year=' in path.name and not 'month=' in path.name:
                    # Extract table name from path (Tables/Files/table_name or Tables/Files/category/table_name)
                    parts = path.name.split('/')
                    if len(parts) >= 3:
                        # Direct child: Tables/Files/table_name
                        if len(parts) == 3:
                            table_name = parts[2]
                            tables.append(table_name)
                        # One level deep: Tables/Files/API/table_name
                        elif len(parts) == 4:
                            table_name = parts[3]
                            tables.append(table_name)

            # Remove duplicates and sort
            tables = sorted(list(set(tables)))

            self.tables_cache = tables
            logger.info(f"Found {len(tables)} tables in OneLake catalog {self.catalog_id}")

            return tables

        except Exception as e:
            logger.error(f"Failed to list OneLake tables: {e}")
            raise

    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Get metadata for a specific OneLake Delta table."""
        try:
            # OneLake tables are under Tables/Files/ path structure
            table_url = f"abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse}/Tables/Files/{table_name}"

            storage_options = {
                'bearer_token': self.credentials.get_bearer_token(),
                'use_fabric_endpoint': 'true'
            }

            dt = DeltaTable(table_url, storage_options=storage_options)

            schema = dt.schema().to_pyarrow()

            metadata = {
                'name': table_name,
                'catalog': self.catalog_id,
                'source': 'onelake',
                'read_only': True,
                'delta_version': dt.version(),
                'num_files': len(dt.files()),
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
            # OneLake tables are under Tables/Files/ path structure
            table_url = f"abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse}/Tables/Files/{table_name}"

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
                # OneLake tables are under Tables/Files/ path structure
                table_url = f"abfss://{self.workspace}@onelake.dfs.fabric.microsoft.com/{self.lakehouse}/Tables/Files/{table_name}"
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
    catalog_id: Optional[str] = None
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

    Returns:
        OneLakeCatalog instance
    """
    if not catalog_id:
        catalog_id = f"onelake_{lakehouse}"

    # Create credentials
    credentials = OneLakeCredentials(tenant_id, client_id, client_secret)

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
