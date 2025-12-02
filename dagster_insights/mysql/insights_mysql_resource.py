from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.mysql.mysql_utils import (
    build_mysql_cost_metadata,
    marker_asset_key_for_job,
    meter_mysql_query,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

if TYPE_CHECKING:
    try:
        import pymysql
    except ImportError:
        pymysql = None

OUTPUT_NON_ASSET_SIGIL = "__mysql_query_metadata_"


class WrappedMySQLConnection:
    """Wrapper around MySQL connection to track query costs."""

    def __init__(self, connection, context, asset_key: Optional[AssetKey] = None) -> None:
        self._connection = connection
        self._context = context
        self._asset_key = asset_key
        self._execution_times_ms = []
        self._rows_processed = []
        self._query_ids = []

    def cursor(self, *args, **kwargs):
        """Get a cursor that tracks query execution."""
        cursor = self._connection.cursor(*args, **kwargs)
        
        # Wrap the cursor's execute method
        original_execute = cursor.execute
        
        def tracked_execute(query, *args, **kwargs):
            context, inferred_asset_key = get_current_context_and_asset_key()
            if not context:
                return original_execute(query, *args, **kwargs)

            associated_asset_key = self._asset_key or inferred_asset_key
            modified_query = meter_mysql_query(
                context, query, associated_asset_key=associated_asset_key
            )
            
            import time
            start_time = time.time()
            result = original_execute(modified_query, *args, **kwargs)
            execution_time_ms = int((time.time() - start_time) * 1000)
            
            self._execution_times_ms.append(execution_time_ms)
            
            return result
        
        cursor.execute = tracked_execute
        return cursor

    def commit(self):
        """Commit the transaction."""
        return self._connection.commit()

    def close(self):
        """Close the connection."""
        return self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Emit cost observations after all queries
        if self._execution_times_ms:
            context, inferred_asset_key = get_current_context_and_asset_key()
            if not context:
                return

            asset_key = self._asset_key or inferred_asset_key
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)

            total_execution_time_ms = sum(self._execution_times_ms)
            total_rows_processed = sum(self._rows_processed) if self._rows_processed else 0

            if total_execution_time_ms > 0:
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata=build_mysql_cost_metadata(
                            self._query_ids,
                            total_execution_time_ms,
                            total_rows_processed,
                        ),
                    )
                )

        self.close()

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying connection."""
        return getattr(self._connection, name)


@beta
class InsightsMySQLResource:
    """A wrapper around MySQL connections which automatically collects metadata about
    MySQL costs which can be attributed to Dagster jobs and assets.

    This resource tracks:
    - Query execution time
    - Rows processed
    - Query IDs for cost attribution

    A simple example of using MySQL with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.mysql import InsightsMySQLResource

            @op
            def run_query(mysql: InsightsMySQLResource):
                with mysql.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM my_table")

            @job
            def my_mysql_job():
                run_query()

            my_mysql_job.execute_in_process(
                resources={
                    "mysql": InsightsMySQLResource(
                        host=EnvVar("MYSQL_HOST"),
                        port=3306,
                        database=EnvVar("MYSQL_DATABASE"),
                        user=EnvVar("MYSQL_USER"),
                        password=EnvVar("MYSQL_PASSWORD")
                    )
                }
            )
    """

    def __init__(
        self,
        host: str,
        port: int = 3306,
        database: str = "",
        user: str = "",
        password: str = "",
        **kwargs,
    ):
        """Initialize the MySQL resource.

        Args:
            host: The MySQL hostname
            port: The MySQL port (default: 3306)
            database: The database name
            user: The database user
            password: The database password
            **kwargs: Additional connection parameters
        """
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            **kwargs,
        }

    @contextmanager
    def get_connection(
        self, asset_key: Optional[AssetKey] = None
    ) -> Iterator[WrappedMySQLConnection]:
        """Get a MySQL connection with cost tracking.

        Args:
            asset_key: Optional asset key to associate queries with
        """
        try:
            import pymysql
        except ImportError:
            raise ImportError(
                "pymysql is required for MySQL. Install it with: pip install pymysql"
            )

        context, inferred_asset_key = get_current_context_and_asset_key()

        associated_asset_key = asset_key or inferred_asset_key

        connection = pymysql.connect(**self.connection_params)

        wrapped_conn = WrappedMySQLConnection(connection, context, associated_asset_key)

        try:
            yield wrapped_conn
            wrapped_conn.commit()
        finally:
            wrapped_conn.close()

    @contextmanager
    def get_connection_for_asset(
        self, asset_key: AssetKey
    ) -> Iterator[WrappedMySQLConnection]:
        """Get a connection specifically for a given asset."""
        yield from self.get_connection(asset_key=asset_key)


