from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.azure.database.azure_database_utils import (
    build_azure_database_cost_metadata,
    marker_asset_key_for_job,
)

# Import base PostgreSQL/MySQL implementations
try:
    from dagster_insights.postgresql.insights_postgresql_resource import (
        InsightsPostgreSQLResource,
    )
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False

try:
    from dagster_insights.mysql.insights_mysql_resource import (
        InsightsMySQLResource,
    )
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

if TYPE_CHECKING:
    try:
        from azure.mgmt.rdbms import postgresql, mysql
    except ImportError:
        postgresql = None
        mysql = None

OUTPUT_NON_ASSET_SIGIL = "__azure_database_query_metadata_"


@beta
class InsightsAzureDatabaseResource:
    """A wrapper around Azure Database for PostgreSQL/MySQL which automatically collects metadata about
    Azure Database costs which can be attributed to Dagster jobs and assets.

    This resource extends PostgreSQL/MySQL implementations with Azure-specific cost tracking:
    - Compute units (vCores)
    - Storage (GB)
    - Query execution time

    Supports both PostgreSQL and MySQL Azure Database instances.

    A simple example of using Azure Database with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.azure.database import InsightsAzureDatabaseResource

            @op
            def run_query(azure_db: InsightsAzureDatabaseResource):
                with azure_db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM my_table")

            @job
            def my_azure_db_job():
                run_query()

            my_azure_db_job.execute_in_process(
                resources={
                    "azure_db": InsightsAzureDatabaseResource(
                        engine="postgresql",  # or "mysql"
                        server_name=EnvVar("AZURE_DB_SERVER"),
                        host=EnvVar("AZURE_DB_HOST"),
                        port=5432,
                        database=EnvVar("AZURE_DB_DATABASE"),
                        user=EnvVar("AZURE_DB_USER"),
                        password=EnvVar("AZURE_DB_PASSWORD"),
                        subscription_id=EnvVar("AZURE_SUBSCRIPTION_ID"),
                        resource_group=EnvVar("AZURE_RESOURCE_GROUP")
                    )
                }
            )
    """

    def __init__(
        self,
        engine: str,  # "postgresql" or "mysql"
        server_name: str,
        host: str,
        port: int = 5432,
        database: str = "",
        user: str = "",
        password: str = "",
        subscription_id: Optional[str] = None,
        resource_group: Optional[str] = None,
        **kwargs,
    ):
        """Initialize the Azure Database resource.

        Args:
            engine: Database engine ("postgresql" or "mysql")
            server_name: Azure Database server name
            host: Database hostname
            port: Database port (5432 for PostgreSQL, 3306 for MySQL)
            database: Database name
            user: Database user
            password: Database password
            subscription_id: Azure subscription ID
            resource_group: Azure resource group name
            **kwargs: Additional connection parameters
        """
        self.engine = engine.lower()
        self.server_name = server_name
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        
        # Create base resource based on engine
        if self.engine == "postgresql":
            if not POSTGRESQL_AVAILABLE:
                raise ImportError(
                    "PostgreSQL support is required. Install it with: pip install psycopg2-binary"
                )
            self._base_resource = InsightsPostgreSQLResource(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                **kwargs,
            )
        elif self.engine == "mysql":
            if not MYSQL_AVAILABLE:
                raise ImportError(
                    "MySQL support is required. Install it with: pip install pymysql"
                )
            self._base_resource = InsightsMySQLResource(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                **kwargs,
            )
        else:
            raise ValueError(f"Unsupported engine: {engine}. Must be 'postgresql' or 'mysql'")

    @contextmanager
    def get_connection(
        self, asset_key: Optional[AssetKey] = None
    ) -> Iterator:
        """Get a database connection with Azure Database-specific cost tracking."""
        context, inferred_asset_key = get_current_context_and_asset_key()

        # Track metrics before connection closes
        execution_time_ms = 0
        rows_processed = 0
        query_ids = []

        # Get base connection
        with self._base_resource.get_connection(asset_key=asset_key) as conn:
            # Capture metrics before connection closes
            if hasattr(conn, "_execution_times_ms"):
                execution_time_ms = sum(conn._execution_times_ms)
            if hasattr(conn, "_rows_processed"):
                rows_processed = sum(conn._rows_processed)
            if hasattr(conn, "_query_ids"):
                query_ids = conn._query_ids
            
            yield conn

        # After connection closes, query Azure Database-specific costs
        if asset_key or inferred_asset_key:
            associated_asset_key = asset_key or inferred_asset_key
            if not associated_asset_key:
                associated_asset_key = marker_asset_key_for_job(context.job_def)

            try:
                from azure.identity import DefaultAzureCredential
                from azure.mgmt.rdbms import postgresql, mysql

                credential = DefaultAzureCredential()
                
                if self.engine == "postgresql":
                    client = postgresql.PostgreSQLManagementClient(
                        credential, self.subscription_id
                    )
                    server = client.servers.get(
                        resource_group_name=self.resource_group,
                        server_name=self.server_name,
                    )
                else:  # mysql
                    client = mysql.MySQLManagementClient(
                        credential, self.subscription_id
                    )
                    server = client.servers.get(
                        resource_group_name=self.resource_group,
                        server_name=self.server_name,
                    )

                # Get compute units (vCores)
                compute_units = 0.0
                if hasattr(server, "sku") and hasattr(server.sku, "capacity"):
                    compute_units = float(server.sku.capacity or 0)
                
                # Get storage
                storage_gb = 0.0
                if hasattr(server, "storage_profile") and hasattr(server.storage_profile, "storage_mb"):
                    storage_gb = float(server.storage_profile.storage_mb or 0) / 1024

                # Use captured metrics

                if execution_time_ms > 0 or compute_units > 0:
                    context.log_event(
                        AssetObservation(
                            asset_key=associated_asset_key,
                            metadata=build_azure_database_cost_metadata(
                                query_ids,
                                execution_time_ms,
                                compute_units,
                                storage_gb,
                                rows_processed,
                            ),
                        )
                    )
            except Exception:
                # If we can't get Azure Database details, just use base connection metrics
                pass

    @contextmanager
    def get_connection_for_asset(
        self, asset_key: AssetKey
    ) -> Iterator:
        """Get a connection specifically for a given asset."""
        yield from self.get_connection(asset_key=asset_key)

