from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.databricks.databricks_utils import (
    build_databricks_cost_metadata,
    marker_asset_key_for_job,
)
from dagster_insights.databricks.workspace_client_wrapper import (
    WrappedDatabricksWorkspaceClient,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

if TYPE_CHECKING:
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sql import Connection
    except ImportError:
        WorkspaceClient = None
        Connection = None

OUTPUT_NON_ASSET_SIGIL = "__databricks_query_metadata_"


class WrappedDatabricksConnection:
    """Wrapper around Databricks SQL Connection to track query costs.
    
    Note: databricks.sql.Connection doesn't appear to be easily subclassable,
    so we use composition and delegate to the underlying connection.
    """

    def __init__(self, connection: "Connection", context, asset_key: Optional[AssetKey] = None) -> None:
        self._connection = connection
        self._context = context
        self._asset_key = asset_key
        self._query_job_ids = []
        self._query_dbu = []
        self._query_compute_time_ms = []
        self._cluster_ids = []

    def cursor(self, *args, **kwargs):
        """Get a cursor that tracks query execution."""
        from dagster_insights.databricks.databricks_utils import meter_databricks_query
        import time

        cursor = self._connection.cursor(*args, **kwargs)
        
        # Wrap the cursor's execute method
        original_execute = cursor.execute
        
        def tracked_execute(query, *args, **kwargs):
            # Tag the query
            modified_query = meter_databricks_query(
                self._context, query, associated_asset_key=self._asset_key
            )
            
            # Execute and track time
            start_time = time.time()
            result = original_execute(modified_query, *args, **kwargs)
            execution_time_ms = int((time.time() - start_time) * 1000)
            
            self._query_compute_time_ms.append(execution_time_ms)
            # Note: DBU calculation would require querying Databricks API
            # For now, we track execution time as a proxy
            
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
        if self._query_compute_time_ms:
            context, inferred_asset_key = get_current_context_and_asset_key()
            if not context:
                return

            asset_key = self._asset_key or inferred_asset_key
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)

            total_compute_time_ms = sum(self._query_compute_time_ms)
            # Estimate DBU based on execution time (simplified)
            # Actual DBU would need to query Databricks API
            estimated_dbu = total_compute_time_ms / (1000 * 60 * 60) * 0.15  # Rough estimate

            if total_compute_time_ms > 0:
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata=build_databricks_cost_metadata(
                            self._query_job_ids,
                            estimated_dbu,
                            total_compute_time_ms,
                        ),
                    )
                )

        self.close()

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying connection."""
        return getattr(self._connection, name)


@beta
class InsightsDatabricksResource:
    """A wrapper around Databricks resources which automatically collects metadata about
    Databricks costs which can be attributed to Dagster jobs and assets.

    This resource tracks:
    - Job runs and their DBU consumption
    - SQL query execution costs
    - Cluster compute time

    A simple example of using Databricks with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op
            from dagster_insights.databricks import InsightsDatabricksResource

            @op
            def run_query(databricks: InsightsDatabricksResource):
                with databricks.get_sql_client() as client:
                    client.execute("SELECT * FROM my_table")

            @job
            def my_databricks_job():
                run_query()

            my_databricks_job.execute_in_process(
                resources={
                    "databricks": InsightsDatabricksResource(
                        workspace_url=EnvVar("DATABRICKS_WORKSPACE_URL"),
                        access_token=EnvVar("DATABRICKS_ACCESS_TOKEN")
                    )
                }
            )
    """

    def __init__(
        self,
        workspace_url: str,
        access_token: str,
        **kwargs,
    ):
        """Initialize the Databricks resource.

        Args:
            workspace_url: The Databricks workspace URL
            access_token: The Databricks access token
            **kwargs: Additional arguments passed to the underlying Databricks client
        """
        self.workspace_url = workspace_url.rstrip("/")
        self.access_token = access_token
        self._kwargs = kwargs

    @contextmanager
    def get_workspace_client(self) -> Iterator["WorkspaceClient"]:
        """Get a Databricks WorkspaceClient for API operations.
        
        This client can be used to:
        - Submit and monitor job runs (notebooks, Python, JAR, etc.)
        - Work with Asset bundles
        - Manage LakeFlow pipelines
        - Work with Delta Live Tables
        
        Costs are tracked by tagging job runs with opaque IDs.
        """
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            raise ImportError(
                "databricks-sdk is required. Install it with `pip install databricks-sdk`"
            )

        context, asset_key = get_current_context_and_asset_key()

        client = WorkspaceClient(
            host=self.workspace_url,
            token=self.access_token,
            **self._kwargs,
        )

        # Wrap the client to track job submissions
        wrapped_client = WrappedDatabricksWorkspaceClient(client, context, asset_key)

        yield wrapped_client

        # After operations, emit cost observations if available
        if wrapped_client._tracked_runs:
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)

            # Query job run costs from Databricks API
            total_dbu = 0.0
            total_compute_time_ms = 0
            job_ids = []

            for run_id, opaque_id in wrapped_client._tracked_runs:
                try:
                    # Get run details to extract cost information
                    run = client.jobs.get_run(run_id=run_id)
                    if hasattr(run, "execution_duration") and run.execution_duration:
                        execution_time_ms = run.execution_duration
                        total_compute_time_ms += execution_time_ms
                        # Estimate DBU (would need cluster config for accurate calculation)
                        estimated_dbu = execution_time_ms / (1000 * 60 * 60) * 0.15
                        total_dbu += estimated_dbu
                        job_ids.append(str(run_id))
                except Exception:
                    # If we can't get run details, skip it
                    # Note: For more accurate costs, you can query system.billing.usage
                    # or system.operational_data.jobs_cost tables
                    pass

            if total_dbu > 0 or total_compute_time_ms > 0:
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata=build_databricks_cost_metadata(
                            job_ids,
                            total_dbu,
                            total_compute_time_ms,
                        ),
                    )
                )

    @contextmanager
    def get_sql_client(self) -> Iterator["Connection"]:
        """Get a Databricks SQL client for query execution."""
        try:
            from databricks import sql
        except ImportError:
            raise ImportError(
                "databricks-sql-connector is required. Install it with `pip install databricks-sql-connector`"
            )

        context, asset_key = get_current_context_and_asset_key()

        connection = sql.connect(
            server_hostname=self.workspace_url.replace("https://", ""),
            http_path=self._kwargs.get("http_path", ""),
            access_token=self.access_token,
        )

        wrapped_connection = WrappedDatabricksConnection(connection, context, asset_key)

        try:
            yield wrapped_connection
        finally:
            wrapped_connection.close()

