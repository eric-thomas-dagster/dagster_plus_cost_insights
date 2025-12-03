from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

import psycopg2
from dagster import AssetKey, AssetObservation
from dagster._annotations import beta
from psycopg2.extensions import connection as PgConnection

from dagster_insights.postgresql.postgresql_utils import (
    build_postgresql_cost_metadata,
    marker_asset_key_for_job,
    meter_postgresql_query,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

OUTPUT_NON_ASSET_SIGIL = "__postgresql_query_metadata_"


class WrappedPostgreSQLCursor:
    """Wrapper around PostgreSQL cursor to track query execution metrics."""

    def __init__(self, cursor, context, asset_key: Optional[AssetKey], connection_wrapper) -> None:
        self._cursor = cursor
        self._context = context
        self._asset_key = asset_key
        self._connection_wrapper = connection_wrapper

    def execute(self, query: str, *args, **kwargs):
        """Execute a query and track metrics."""
        import time

        # Tag the query with opaque ID
        modified_query = meter_postgresql_query(
            self._context, query, associated_asset_key=self._asset_key
        )

        # Execute and track execution time
        start_time = time.time()
        result = self._cursor.execute(modified_query, *args, **kwargs)
        execution_time_ms = int((time.time() - start_time) * 1000)

        # Store metrics in connection wrapper
        self._connection_wrapper._execution_times_ms.append(execution_time_ms)
        # Note: rows_processed would need to be extracted from cursor.rowcount if available
        if self._cursor.rowcount >= 0:
            self._connection_wrapper._rows_processed.append(self._cursor.rowcount)

        return result

    def fetchall(self):
        """Fetch all results."""
        return self._cursor.fetchall()

    def fetchone(self):
        """Fetch one result."""
        return self._cursor.fetchone()

    def fetchmany(self, size=None):
        """Fetch many results."""
        if size is None:
            return self._cursor.fetchmany()
        return self._cursor.fetchmany(size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cursor.close()

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying cursor."""
        return getattr(self._cursor, name)


class WrappedPostgreSQLConnection:
    """Wrapper around PostgreSQL connection to track query costs."""

    def __init__(self, connection: PgConnection, context, asset_key: Optional[AssetKey] = None) -> None:
        self._connection = connection
        self._context = context
        self._asset_key = asset_key
        self._execution_times_ms = []
        self._rows_processed = []
        self._query_ids = []

    def cursor(self, *args, **kwargs):
        """Get a cursor that tracks query execution."""
        cursor = self._connection.cursor(*args, **kwargs)
        return WrappedPostgreSQLCursor(cursor, self._context, self._asset_key, self)

    def commit(self):
        """Commit the transaction."""
        return self._connection.commit()

    def rollback(self):
        """Rollback the transaction."""
        return self._connection.rollback()

    def close(self):
        """Close the connection."""
        return self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        self.close()

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying connection."""
        return getattr(self._connection, name)


@beta
class InsightsPostgreSQLResource:
    """A wrapper around PostgreSQL connections which automatically collects metadata about
    PostgreSQL costs which can be attributed to Dagster jobs and assets.

    This resource tracks:
    - Query execution time
    - Rows processed
    - Query IDs for cost attribution

    A simple example of using PostgreSQL with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.postgresql import InsightsPostgreSQLResource

            @op
            def run_query(postgresql: InsightsPostgreSQLResource):
                with postgresql.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM my_table")

            @job
            def my_postgresql_job():
                run_query()

            my_postgresql_job.execute_in_process(
                resources={
                    "postgresql": InsightsPostgreSQLResource(
                        host=EnvVar("POSTGRES_HOST"),
                        port=5432,
                        database=EnvVar("POSTGRES_DATABASE"),
                        user=EnvVar("POSTGRES_USER"),
                        password=EnvVar("POSTGRES_PASSWORD")
                    )
                }
            )
    """

    def __init__(
        self,
        host: str,
        port: int = 5432,
        database: str = "",
        user: str = "",
        password: str = "",
        **kwargs,
    ):
        """Initialize the PostgreSQL resource.

        Args:
            host: The PostgreSQL hostname
            port: The PostgreSQL port (default: 5432)
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
    ) -> Iterator[WrappedPostgreSQLConnection]:
        """Get a PostgreSQL connection with cost tracking.

        Args:
            asset_key: Optional asset key to associate queries with
        """
        context, inferred_asset_key = get_current_context_and_asset_key()

        associated_asset_key = asset_key or inferred_asset_key

        # Create actual psycopg2 connection
        pg_conn = psycopg2.connect(**self.connection_params)

        # Wrap it for cost tracking
        conn = WrappedPostgreSQLConnection(
            connection=pg_conn,
            context=context,
            asset_key=associated_asset_key
        )

        try:
            yield conn
            conn.commit()
            
            # Emit cost observations after all queries
            if conn._execution_times_ms:
                if not associated_asset_key:
                    associated_asset_key = marker_asset_key_for_job(context.job_def)
                
                total_execution_time_ms = sum(conn._execution_times_ms)
                total_rows_processed = sum(conn._rows_processed) if conn._rows_processed else 0
                
                if total_execution_time_ms > 0:
                    context.log_event(
                        AssetObservation(
                            asset_key=associated_asset_key,
                            metadata=build_postgresql_cost_metadata(
                                conn._query_ids,
                                total_execution_time_ms,
                                total_rows_processed,
                            ),
                        )
                    )
        finally:
            conn.close()

    @contextmanager
    def get_connection_for_asset(
        self, asset_key: AssetKey
    ) -> Iterator[WrappedPostgreSQLConnection]:
        """Get a connection specifically for a given asset."""
        yield from self.get_connection(asset_key=asset_key)


