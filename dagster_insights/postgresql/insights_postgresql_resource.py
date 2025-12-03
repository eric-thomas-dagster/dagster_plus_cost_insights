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
    extract_opaque_id_from_query,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

OUTPUT_NON_ASSET_SIGIL = "__postgresql_query_metadata_"

# SQL to create the tracking table
CREATE_TRACKING_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dagster_insights_query_tracking (
    id SERIAL PRIMARY KEY,
    opaque_id TEXT NOT NULL,
    execution_time_ms INTEGER NOT NULL,
    query_id TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    query_text TEXT,
    rows_processed INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tracking_start_time
    ON dagster_insights_query_tracking(start_time);
CREATE INDEX IF NOT EXISTS idx_tracking_opaque_id
    ON dagster_insights_query_tracking(opaque_id);
"""


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
        from datetime import datetime, timezone
        from uuid import uuid4

        # Tag the query with opaque ID and get the modified query
        modified_query, opaque_id = meter_postgresql_query(
            self._context, query, associated_asset_key=self._asset_key, return_opaque_id=True
        )

        # Generate a unique query ID
        query_id = str(uuid4())

        # Execute and track execution time
        start_time_dt = datetime.now(timezone.utc)
        start_time = time.time()
        result = self._cursor.execute(modified_query, *args, **kwargs)
        execution_time_ms = int((time.time() - start_time) * 1000)
        end_time_dt = datetime.now(timezone.utc)

        rows_processed = self._cursor.rowcount if self._cursor.rowcount >= 0 else 0

        # Store metrics in connection wrapper
        self._connection_wrapper._execution_times_ms.append(execution_time_ms)
        self._connection_wrapper._rows_processed.append(rows_processed)
        self._connection_wrapper._query_ids.append(query_id)

        # Store tracking data for later insertion
        if self._connection_wrapper._enable_tracking and opaque_id:
            self._connection_wrapper._tracking_data.append({
                "opaque_id": opaque_id,
                "execution_time_ms": execution_time_ms,
                "query_id": query_id,
                "start_time": start_time_dt,
                "end_time": end_time_dt,
                "query_text": query[:1000],  # Truncate to first 1000 chars
                "rows_processed": rows_processed,
            })

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

    def __init__(
        self,
        connection: PgConnection,
        context,
        asset_key: Optional[AssetKey] = None,
        enable_tracking: bool = False,
    ) -> None:
        self._connection = connection
        self._context = context
        self._asset_key = asset_key
        self._execution_times_ms = []
        self._rows_processed = []
        self._query_ids = []
        self._enable_tracking = enable_tracking
        self._tracking_data = []

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

    def _insert_tracking_data(self):
        """Insert accumulated tracking data into the tracking table."""
        if not self._tracking_data:
            return

        # Use a separate cursor to avoid interfering with user queries
        cursor = self._connection.cursor()
        try:
            insert_sql = """
                INSERT INTO dagster_insights_query_tracking
                    (opaque_id, execution_time_ms, query_id, start_time, end_time, query_text, rows_processed)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            for data in self._tracking_data:
                cursor.execute(
                    insert_sql,
                    (
                        data["opaque_id"],
                        data["execution_time_ms"],
                        data["query_id"],
                        data["start_time"],
                        data["end_time"],
                        data["query_text"],
                        data["rows_processed"],
                    ),
                )
            self._connection.commit()
        except Exception as e:
            # Log error but don't fail the user's transaction
            if self._context:
                self._context.log.warning(
                    f"Failed to insert tracking data: {e}. Cost insights may be incomplete."
                )
        finally:
            cursor.close()

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
        enable_cost_tracking: bool = False,
        **kwargs,
    ):
        """Initialize the PostgreSQL resource.

        Args:
            host: The PostgreSQL hostname
            port: The PostgreSQL port (default: 5432)
            database: The database name
            user: The database user
            password: The database password
            enable_cost_tracking: Enable writing query data to tracking table for cost insights.
                Set this to True if you plan to use create_postgresql_insights_asset_and_schedule().
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
        self.enable_cost_tracking = enable_cost_tracking
        self._tracking_table_initialized = False

    def _ensure_tracking_table(self, conn: PgConnection) -> None:
        """Ensure the tracking table exists."""
        if self._tracking_table_initialized:
            return

        cursor = conn.cursor()
        try:
            cursor.execute(CREATE_TRACKING_TABLE_SQL)
            conn.commit()
            self._tracking_table_initialized = True
        except Exception as e:
            # If table creation fails, log warning but continue
            # This allows the resource to work even if user doesn't have table creation permissions
            conn.rollback()
            import warnings
            warnings.warn(
                f"Failed to create tracking table: {e}. "
                "Cost insights tracking will be disabled. "
                "Please create the table manually or grant CREATE TABLE permissions."
            )
        finally:
            cursor.close()

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

        # Ensure tracking table exists if tracking is enabled
        if self.enable_cost_tracking:
            self._ensure_tracking_table(pg_conn)

        # Wrap it for cost tracking
        conn = WrappedPostgreSQLConnection(
            connection=pg_conn,
            context=context,
            asset_key=associated_asset_key,
            enable_tracking=self.enable_cost_tracking,
        )

        try:
            yield conn
            conn.commit()

            # Insert tracking data if enabled
            if self.enable_cost_tracking:
                conn._insert_tracking_data()

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


