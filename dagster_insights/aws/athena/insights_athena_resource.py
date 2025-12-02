from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.aws.athena.athena_utils import (
    build_athena_cost_metadata,
    build_opaque_id_metadata,
    marker_asset_key_for_job,
    OPAQUE_ID_SQL_SIGIL,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

if TYPE_CHECKING:
    try:
        import boto3
        from mypy_boto3_athena import AthenaClient
    except ImportError:
        AthenaClient = None

OUTPUT_NON_ASSET_SIGIL = "__athena_query_metadata_"


class WrappedAthenaClient:
    """Wrapper around AWS Athena client to track query costs."""

    def __init__(self, client, context, asset_key: Optional[AssetKey] = None) -> None:
        self._client = client
        self._context = context
        self._asset_key = asset_key
        self._query_execution_ids = []
        self._data_scanned_bytes = []
        self._execution_times_ms = []

    def start_query_execution(self, QueryString: str, **kwargs):
        """Start an Athena query execution with automatic cost tracking."""
        from uuid import uuid4

        # Get or create asset key
        inferred_context, inferred_asset_key = get_current_context_and_asset_key()
        associated_asset_key = self._asset_key or inferred_asset_key
        if not associated_asset_key and inferred_context:
            associated_asset_key = marker_asset_key_for_job(inferred_context.job_def)

        # Generate opaque ID
        opaque_id = str(uuid4())

        # Emit observation
        if inferred_context and associated_asset_key:
            inferred_context.log_event(
                AssetObservation(
                    asset_key=associated_asset_key,
                    metadata=build_opaque_id_metadata(opaque_id),
                )
            )

        # Tag the query with opaque ID
        # Athena supports query comments
        tagged_query = QueryString + f"\n-- {OPAQUE_ID_SQL_SIGIL}[[[{opaque_id}]]]\n"

        # Add workgroup tags if available
        work_group = kwargs.get("WorkGroup", "primary")
        query_execution_context = kwargs.get("QueryExecutionContext", {})
        
        # Start the query
        result = self._client.start_query_execution(
            QueryString=tagged_query,
            WorkGroup=work_group,
            QueryExecutionContext=query_execution_context,
            **{k: v for k, v in kwargs.items() if k not in ["QueryString", "WorkGroup", "QueryExecutionContext"]},
        )

        # Track the query execution
        if result and "QueryExecutionId" in result:
            self._query_execution_ids.append((result["QueryExecutionId"], opaque_id))

        return result

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying Athena client."""
        return getattr(self._client, name)


@beta
class InsightsAthenaResource:
    """A wrapper around AWS Athena client which automatically collects metadata about
    Athena query costs which can be attributed to Dagster jobs and assets.

    This resource tracks:
    - Data scanned (charged per TB)
    - Query execution time
    - Query execution IDs

    A simple example of using AWS Athena with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.aws.athena import InsightsAthenaResource

            @op
            def run_query(athena: InsightsAthenaResource):
                with athena.get_client() as client:
                    result = client.start_query_execution(
                        QueryString="SELECT * FROM my_table",
                        QueryExecutionContext={"Database": "my_database"},
                        ResultConfiguration={"OutputLocation": "s3://my-bucket/results/"}
                    )

            @job
            def my_athena_job():
                run_query()

            my_athena_job.execute_in_process(
                resources={
                    "athena": InsightsAthenaResource(
                        region_name=EnvVar("AWS_REGION")
                    )
                }
            )
    """

    def __init__(
        self,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        **kwargs,
    ):
        """Initialize the AWS Athena resource.

        Args:
            region_name: AWS region name
            aws_access_key_id: AWS access key ID (optional, can use IAM role)
            aws_secret_access_key: AWS secret access key (optional, can use IAM role)
            **kwargs: Additional boto3 client parameters
        """
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self._kwargs = kwargs

    @contextmanager
    def get_client(self) -> Iterator[WrappedAthenaClient]:
        """Get an AWS Athena client with cost tracking."""
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 is required. Install it with: pip install boto3")

        context, asset_key = get_current_context_and_asset_key()

        client = boto3.client(
            "athena",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            **self._kwargs,
        )

        wrapped_client = WrappedAthenaClient(client, context, asset_key)

        yield wrapped_client

        # After operations, query execution costs
        if wrapped_client._query_execution_ids:
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)

            total_data_scanned_gb = 0.0
            total_execution_time_ms = 0
            query_ids = []

            for query_execution_id, opaque_id in wrapped_client._query_execution_ids:
                try:
                    # Get query execution details
                    execution = client.get_query_execution(QueryExecutionId=query_execution_id)
                    if "QueryExecution" in execution:
                        query_exec = execution["QueryExecution"]
                        stats = query_exec.get("Statistics", {})
                        
                        # Get data scanned (in bytes)
                        data_scanned_bytes = stats.get("DataScannedInBytes", 0)
                        data_scanned_gb = data_scanned_bytes / (1024**3)
                        total_data_scanned_gb += data_scanned_gb
                        
                        # Get execution time
                        execution_time_ms = stats.get("TotalExecutionTimeInMillis", 0)
                        total_execution_time_ms += execution_time_ms
                        
                        query_ids.append(query_execution_id)
                except Exception:
                    # If we can't get execution details, skip it
                    pass

            if total_data_scanned_gb > 0 or total_execution_time_ms > 0:
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata=build_athena_cost_metadata(
                            query_ids,
                            total_data_scanned_gb,
                            total_execution_time_ms,
                        ),
                    )
                )


