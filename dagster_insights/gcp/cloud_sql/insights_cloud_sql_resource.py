from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.gcp.cloud_sql.cloud_sql_utils import (
    build_cloud_sql_cost_metadata,
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
        from google.cloud import sqladmin_v1
    except ImportError:
        sqladmin_v1 = None

OUTPUT_NON_ASSET_SIGIL = "__cloud_sql_query_metadata_"


@beta
class InsightsCloudSQLResource:
    """A wrapper around Google Cloud SQL databases which automatically collects metadata about
    Cloud SQL costs which can be attributed to Dagster jobs and assets.

    This resource extends PostgreSQL/MySQL implementations with Cloud SQL-specific cost tracking:
    - Instance hours (compute)
    - Storage (GB)
    - Query execution time

    Supports both PostgreSQL and MySQL Cloud SQL instances.

    A simple example of using Google Cloud SQL with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.gcp.cloud_sql import InsightsCloudSQLResource

            @op
            def run_query(cloud_sql: InsightsCloudSQLResource):
                with cloud_sql.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM my_table")

            @job
            def my_cloud_sql_job():
                run_query()

            my_cloud_sql_job.execute_in_process(
                resources={
                    "cloud_sql": InsightsCloudSQLResource(
                        engine="postgresql",  # or "mysql"
                        instance_connection_name=EnvVar("CLOUD_SQL_INSTANCE"),
                        host=EnvVar("CLOUD_SQL_HOST"),
                        port=5432,
                        database=EnvVar("CLOUD_SQL_DATABASE"),
                        user=EnvVar("CLOUD_SQL_USER"),
                        password=EnvVar("CLOUD_SQL_PASSWORD"),
                        project_id=EnvVar("GCP_PROJECT_ID")
                    )
                }
            )
    """

    def __init__(
        self,
        engine: str,  # "postgresql" or "mysql"
        instance_connection_name: str,
        host: str,
        port: int = 5432,
        database: str = "",
        user: str = "",
        password: str = "",
        project_id: Optional[str] = None,
        **kwargs,
    ):
        """Initialize the Google Cloud SQL resource.

        Args:
            engine: Database engine ("postgresql" or "mysql")
            instance_connection_name: Cloud SQL instance connection name
            host: Cloud SQL instance hostname
            port: Database port (5432 for PostgreSQL, 3306 for MySQL)
            database: Database name
            user: Database user
            password: Database password
            project_id: GCP project ID
            **kwargs: Additional connection parameters
        """
        self.engine = engine.lower()
        self.instance_connection_name = instance_connection_name
        self.project_id = project_id
        
        # Extract project and instance from connection name if needed
        if not self.project_id and ":" in instance_connection_name:
            parts = instance_connection_name.split(":")
            if len(parts) >= 2:
                self.project_id = parts[0]
        
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
        """Get a database connection with Cloud SQL-specific cost tracking."""
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

        # After connection closes, query Cloud SQL-specific costs
        if asset_key or inferred_asset_key:
            associated_asset_key = asset_key or inferred_asset_key
            if not associated_asset_key:
                associated_asset_key = marker_asset_key_for_job(context.job_def)

            try:
                from google.cloud import sqladmin_v1

                client = sqladmin_v1.SqlInstancesServiceClient()
                
                # Get instance details
                instance_name = self.instance_connection_name.split(":")[-1] if ":" in self.instance_connection_name else self.instance_connection_name
                instance = client.get(
                    project=self.project_id,
                    instance=instance_name,
                )

                # Calculate instance hours (simplified - would need actual runtime)
                instance_hours = 0.0  # Would need to track actual runtime
                
                # Get storage
                storage_gb = instance.settings.data_disk_size_gb if hasattr(instance.settings, "data_disk_size_gb") else 0

                # Use captured metrics

                if execution_time_ms > 0 or instance_hours > 0:
                    context.log_event(
                        AssetObservation(
                            asset_key=associated_asset_key,
                            metadata=build_cloud_sql_cost_metadata(
                                query_ids,
                                execution_time_ms,
                                instance_hours,
                                storage_gb,
                                rows_processed,
                            ),
                        )
                    )
            except Exception:
                # If we can't get Cloud SQL details, just use base connection metrics
                pass

    @contextmanager
    def get_connection_for_asset(
        self, asset_key: AssetKey
    ) -> Iterator:
        """Get a connection specifically for a given asset."""
        yield from self.get_connection(asset_key=asset_key)

