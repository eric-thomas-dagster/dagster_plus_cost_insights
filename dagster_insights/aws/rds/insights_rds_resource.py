from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.aws.rds.rds_utils import (
    build_rds_cost_metadata,
    marker_asset_key_for_job,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

# Import base PostgreSQL/MySQL implementations
try:
    from dagster_insights.postgresql.insights_postgresql_resource import (
        InsightsPostgreSQLResource,
        WrappedPostgreSQLConnection,
    )
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False

try:
    from dagster_insights.mysql.insights_mysql_resource import (
        InsightsMySQLResource,
        WrappedMySQLConnection,
    )
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

if TYPE_CHECKING:
    try:
        import boto3
        from mypy_boto3_rds import RDSClient
    except ImportError:
        RDSClient = None

OUTPUT_NON_ASSET_SIGIL = "__rds_query_metadata_"


@beta
class InsightsRDSResource:
    """A wrapper around AWS RDS databases which automatically collects metadata about
    RDS costs which can be attributed to Dagster jobs and assets.

    This resource extends PostgreSQL/MySQL implementations with RDS-specific cost tracking:
    - Instance hours (compute)
    - Storage (GB)
    - IOPS (I/O operations)
    - Query execution time

    Supports both PostgreSQL and MySQL RDS instances.

    A simple example of using AWS RDS with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.aws.rds import InsightsRDSResource

            @op
            def run_query(rds: InsightsRDSResource):
                with rds.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM my_table")

            @job
            def my_rds_job():
                run_query()

            my_rds_job.execute_in_process(
                resources={
                    "rds": InsightsRDSResource(
                        engine="postgresql",  # or "mysql"
                        db_instance_identifier=EnvVar("RDS_INSTANCE_ID"),
                        host=EnvVar("RDS_HOST"),
                        port=5432,
                        database=EnvVar("RDS_DATABASE"),
                        user=EnvVar("RDS_USER"),
                        password=EnvVar("RDS_PASSWORD"),
                        region_name=EnvVar("AWS_REGION")
                    )
                }
            )
    """

    def __init__(
        self,
        engine: str,  # "postgresql" or "mysql"
        db_instance_identifier: str,
        host: str,
        port: int = 5432,
        database: str = "",
        user: str = "",
        password: str = "",
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        **kwargs,
    ):
        """Initialize the AWS RDS resource.

        Args:
            engine: Database engine ("postgresql" or "mysql")
            db_instance_identifier: RDS instance identifier
            host: RDS endpoint hostname
            port: Database port (5432 for PostgreSQL, 3306 for MySQL)
            database: Database name
            user: Database user
            password: Database password
            region_name: AWS region name
            aws_access_key_id: AWS access key ID (optional)
            aws_secret_access_key: AWS secret access key (optional)
            **kwargs: Additional connection parameters
        """
        self.engine = engine.lower()
        self.db_instance_identifier = db_instance_identifier
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        
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
        """Get a database connection with RDS-specific cost tracking."""
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

        # After connection closes, query RDS-specific costs
        if asset_key or inferred_asset_key:
            associated_asset_key = asset_key or inferred_asset_key
            if not associated_asset_key:
                associated_asset_key = marker_asset_key_for_job(context.job_def)

            try:
                import boto3

                rds_client = boto3.client(
                    "rds",
                    region_name=self.region_name,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                )

                # Get RDS instance details
                instance = rds_client.describe_db_instances(
                    DBInstanceIdentifier=self.db_instance_identifier
                )["DBInstances"][0]

                # Calculate instance hours (simplified - would need actual runtime)
                instance_hours = 0.0  # Would need to track actual runtime
                
                # Get storage
                storage_gb = instance.get("AllocatedStorage", 0)
                
                # Get IOPS (if provisioned)
                iops = instance.get("Iops", 0)

                # Use captured metrics

                if execution_time_ms > 0 or instance_hours > 0:
                    context.log_event(
                        AssetObservation(
                            asset_key=associated_asset_key,
                            metadata=build_rds_cost_metadata(
                                query_ids,
                                execution_time_ms,
                                instance_hours,
                                storage_gb,
                                iops,
                                rows_processed,
                            ),
                        )
                    )
            except Exception:
                # If we can't get RDS details, just use base connection metrics
                pass

    @contextmanager
    def get_connection_for_asset(
        self, asset_key: AssetKey
    ) -> Iterator:
        """Get a connection specifically for a given asset."""
        yield from self.get_connection(asset_key=asset_key)

