from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.trino.trino_utils import (
    build_trino_cost_metadata,
    marker_asset_key_for_job,
    meter_trino_query,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

if TYPE_CHECKING:
    try:
        from trino.dbapi import Connection
    except ImportError:
        Connection = None

OUTPUT_NON_ASSET_SIGIL = "__trino_query_metadata_"


class WrappedTrinoConnection:
    """Wrapper around Trino connection to track query costs."""

    def __init__(
        self, connection: "Connection", context, asset_key: Optional[AssetKey] = None
    ) -> None:
        self._connection = connection
        self._context = context
        self._asset_key = asset_key
        self._execution_times_ms = []
        self._rows_processed = []
        self._bytes_read = []
        self._query_ids = []

    def cursor(self, *args, **kwargs):
        """Get a cursor that tracks query execution."""
        cursor = self._connection.cursor(*args, **kwargs)

        # Wrap the cursor's execute method
        original_execute = cursor.execute

        def tracked_execute(query, *args, **kwargs):
            # Tag the query
            modified_query = meter_trino_query(
                self._context, query, associated_asset_key=self._asset_key
            )

            # Execute and track time
            import time

            start_time = time.time()
            result = original_execute(modified_query, *args, **kwargs)
            execution_time_ms = int((time.time() - start_time) * 1000)

            self._execution_times_ms.append(execution_time_ms)
            # Note: rows_processed and bytes_read would need to be extracted
            # from query stats if available

            return result

        cursor.execute = tracked_execute
        return cursor

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
            total_bytes_read = sum(self._bytes_read) if self._bytes_read else 0

            if total_execution_time_ms > 0:
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata=build_trino_cost_metadata(
                            self._query_ids,
                            total_execution_time_ms,
                            total_rows_processed,
                            total_bytes_read,
                        ),
                    )
                )

        self.close()

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying connection."""
        return getattr(self._connection, name)


@beta
class InsightsTrinoResource:
    """A wrapper around Trino connections which automatically collects metadata about
    Trino query costs which can be attributed to Dagster jobs and assets.

    This resource tracks:
    - Query execution time
    - Rows processed
    - Bytes read from connectors
    - Query IDs for cost attribution

    A simple example of using Trino with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.trino import InsightsTrinoResource

            @op
            def run_query(trino: InsightsTrinoResource):
                with trino.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM my_catalog.my_schema.my_table")

            @job
            def my_trino_job():
                run_query()

            my_trino_job.execute_in_process(
                resources={
                    "trino": InsightsTrinoResource(
                        host=EnvVar("TRINO_HOST"),
                        port=8080,
                        user=EnvVar("TRINO_USER"),
                        catalog=EnvVar("TRINO_CATALOG"),
                        schema=EnvVar("TRINO_SCHEMA")
                    )
                }
            )
    """

    def __init__(
        self,
        host: str,
        port: int = 8080,
        user: str = "dagster",
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        **kwargs,
    ):
        """Initialize the Trino resource.

        Args:
            host: The Trino coordinator hostname
            port: The Trino port (default: 8080)
            user: The Trino user
            catalog: The Trino catalog
            schema: The Trino schema
            **kwargs: Additional connection parameters
        """
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self._kwargs = kwargs

    @contextmanager
    def get_connection(
        self, asset_key: Optional[AssetKey] = None
    ) -> Iterator[WrappedTrinoConnection]:
        """Get a Trino connection with cost tracking.

        Args:
            asset_key: Optional asset key to associate queries with
        """
        try:
            from trino.dbapi import connect
        except ImportError:
            raise ImportError(
                "trino is required. Install it with: pip install trino"
            )

        context, inferred_asset_key = get_current_context_and_asset_key()

        associated_asset_key = asset_key or inferred_asset_key

        connection = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            **self._kwargs,
        )

        wrapped_conn = WrappedTrinoConnection(connection, context, associated_asset_key)

        try:
            yield wrapped_conn
        finally:
            wrapped_conn.close()

    @contextmanager
    def get_connection_for_asset(
        self, asset_key: AssetKey
    ) -> Iterator[WrappedTrinoConnection]:
        """Get a connection specifically for a given asset."""
        yield from self.get_connection(asset_key=asset_key)


