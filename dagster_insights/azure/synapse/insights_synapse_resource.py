from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.azure.synapse.synapse_utils import (
    build_synapse_cost_metadata,
    marker_asset_key_for_job,
    meter_synapse_query,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

if TYPE_CHECKING:
    try:
        import pyodbc
    except ImportError:
        pyodbc = None

OUTPUT_NON_ASSET_SIGIL = "__synapse_query_metadata_"


try:
    import pyodbc
    _PYODBC_AVAILABLE = True
except ImportError:
    _PYODBC_AVAILABLE = False
    pyodbc = None


class WrappedSynapseConnection:
    """Wrapper around Azure Synapse connection to track query costs.
    
    Wraps pyodbc.Connection to track query execution and costs.
    Note: pyodbc.Connection is not easily subclassable, so we use composition.
    """

    def __init__(self, connection: "pyodbc.Connection", context, asset_key: Optional[AssetKey] = None) -> None:
        if not _PYODBC_AVAILABLE:
            raise ImportError("pyodbc is required. Install it with: pip install pyodbc")
        
        self._connection = connection
        self._context = context
        self._asset_key = asset_key
        self._query_ids = []
        self._execution_times_ms = []
        self._dwu_seconds = []
        self._rows_processed = []

    def cursor(self):
        """Get a cursor that tracks query execution."""
        cursor = WrappedSynapseCursor(
            self._connection.cursor(), self._context, self._asset_key, self
        )
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
        if self._query_ids and (self._dwu_seconds or self._execution_times_ms):
            context, inferred_asset_key = get_current_context_and_asset_key()
            if not context:
                return

            asset_key = self._asset_key or inferred_asset_key
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)

            total_dwu_seconds = sum(self._dwu_seconds) if self._dwu_seconds else 0.0
            total_execution_time_ms = sum(self._execution_times_ms) if self._execution_times_ms else 0
            total_rows_processed = sum(self._rows_processed) if self._rows_processed else 0

            if total_dwu_seconds > 0 or total_execution_time_ms > 0:
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata=build_synapse_cost_metadata(
                            self._query_ids,
                            total_dwu_seconds,
                            total_execution_time_ms,
                            total_rows_processed,
                        ),
                    )
                )

        self.close()

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying connection."""
        return getattr(self._connection, name)


class WrappedSynapseCursor:
    """Wrapper around Azure Synapse cursor to track query execution metrics."""

    def __init__(
        self, cursor, context, asset_key: Optional[AssetKey], connection_wrapper
    ) -> None:
        self._cursor = cursor
        self._context = context
        self._asset_key = asset_key
        self._connection_wrapper = connection_wrapper

    def execute(self, query: str, *args, **kwargs):
        """Execute a query and track metrics."""
        import time

        # Tag the query with opaque ID
        modified_query = meter_synapse_query(
            self._context, query, associated_asset_key=self._asset_key
        )

        # Execute and track execution time
        start_time = time.time()
        result = self._cursor.execute(modified_query, *args, **kwargs)
        execution_time_ms = int((time.time() - start_time) * 1000)

        # Store metrics in connection wrapper
        self._connection_wrapper._execution_times_ms.append(execution_time_ms)
        # Note: We can't easily get DWU consumption or query ID from pyodbc directly
        # These would need to be queried from sys.dm_pdw_exec_requests after execution
        # For now, we track execution time as a proxy for cost
        # DWU seconds can be calculated based on execution time and service level

        return result

    def fetchall(self):
        """Fetch all results."""
        return self._cursor.fetchall()

    def fetchone(self):
        """Fetch one result."""
        return self._cursor.fetchone()

    def fetchmany(self, size=None):
        """Fetch many results."""
        return self._cursor.fetchmany(size)

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying cursor."""
        return getattr(self._cursor, name)


@beta
class InsightsSynapseResource:
    """A wrapper around Azure Synapse Analytics connections which automatically collects metadata about
    Synapse costs which can be attributed to Dagster jobs and assets.

    This resource tracks:
    - Query execution time
    - DWU (Data Warehouse Units) consumption
    - Rows processed
    - Query IDs for cost attribution

    A simple example of using Azure Synapse with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.azure.synapse import InsightsSynapseResource

            @op
            def run_query(synapse: InsightsSynapseResource):
                with synapse.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM my_table")

            @job
            def my_synapse_job():
                run_query()

            my_synapse_job.execute_in_process(
                resources={
                    "synapse": InsightsSynapseResource(
                        server=EnvVar("SYNAPSE_SERVER"),
                        database=EnvVar("SYNAPSE_DATABASE"),
                        user=EnvVar("SYNAPSE_USER"),
                        password=EnvVar("SYNAPSE_PASSWORD")
                    )
                }
            )
    """

    def __init__(
        self,
        server: str,
        database: str,
        user: str,
        password: str,
        driver: str = "{ODBC Driver 17 for SQL Server}",
        **kwargs,
    ):
        """Initialize the Azure Synapse resource.

        Args:
            server: The Synapse workspace server name (e.g., mysynapse.sql.azuresynapse.net)
            database: The database name
            user: The database user
            password: The database password
            driver: The ODBC driver to use. Defaults to ODBC Driver 17 for SQL Server.
            **kwargs: Additional connection parameters
        """
        self.connection_string = (
            f"Driver={driver};"
            f"Server={server};"
            f"Database={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        # Add any additional connection string parameters
        for key, value in kwargs.items():
            self.connection_string += f"{key}={value};"

    @contextmanager
    def get_connection(
        self, asset_key: Optional[AssetKey] = None
    ) -> Iterator[WrappedSynapseConnection]:
        """Get an Azure Synapse connection with cost tracking.

        Args:
            asset_key: Optional asset key to associate queries with
        """
        try:
            import pyodbc
        except ImportError:
            raise ImportError(
                "pyodbc is required for Azure Synapse. Install it with: pip install pyodbc"
            )

        context, inferred_asset_key = get_current_context_and_asset_key()

        associated_asset_key = asset_key or inferred_asset_key

        # Create the underlying pyodbc connection
        connection = pyodbc.connect(self.connection_string)
        
        # Wrap it
        wrapped_conn = WrappedSynapseConnection(connection, context, associated_asset_key)

        try:
            yield wrapped_conn
            wrapped_conn.commit()
        finally:
            wrapped_conn.close()

    @contextmanager
    def get_connection_for_asset(
        self, asset_key: AssetKey
    ) -> Iterator[WrappedSynapseConnection]:
        """Get a connection specifically for a given asset."""
        yield from self.get_connection(asset_key=asset_key)

