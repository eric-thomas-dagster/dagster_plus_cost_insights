from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.aws.glue.glue_utils import (
    build_glue_cost_metadata,
    build_opaque_id_metadata,
    marker_asset_key_for_job,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

if TYPE_CHECKING:
    try:
        import boto3
        from mypy_boto3_glue import GlueClient
    except ImportError:
        GlueClient = None

OUTPUT_NON_ASSET_SIGIL = "__glue_job_metadata_"


class WrappedGlueClient:
    """Wrapper around AWS Glue client to track job runs and costs."""

    def __init__(self, client, context, asset_key: Optional[AssetKey] = None) -> None:
        self._client = client
        self._context = context
        self._asset_key = asset_key
        self._job_run_ids = []  # List of (job_run_id, opaque_id, job_name) tuples
        self._dpu_hours = []
        self._execution_times_ms = []

    def start_job_run(self, JobName: str, **kwargs):
        """Start a Glue job run with automatic cost tracking."""
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

        # Add tags to job run
        arguments = kwargs.get("Arguments", {})
        arguments["--dagster_opaque_id"] = opaque_id
        if inferred_context:
            arguments["--dagster_job_name"] = inferred_context.job_def.name
            arguments["--dagster_run_id"] = inferred_context.run_id
        kwargs["Arguments"] = arguments

        # Start the job run
        result = self._client.start_job_run(JobName=JobName, **kwargs)

        # Track the run
        if result and "JobRunId" in result:
            self._job_run_ids.append((result["JobRunId"], opaque_id, JobName))

        return result

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying Glue client."""
        return getattr(self._client, name)


@beta
class InsightsGlueResource:
    """A wrapper around AWS Glue client which automatically collects metadata about
    Glue job costs which can be attributed to Dagster jobs and assets.

    This resource tracks:
    - Job run execution time
    - DPU (Data Processing Units) consumption
    - Job run IDs for cost attribution

    A simple example of using AWS Glue with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.aws.glue import InsightsGlueResource

            @op
            def run_glue_job(glue: InsightsGlueResource):
                with glue.get_client() as client:
                    client.start_job_run(JobName="my_glue_job")

            @job
            def my_glue_job():
                run_glue_job()

            my_glue_job.execute_in_process(
                resources={
                    "glue": InsightsGlueResource(
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
        """Initialize the AWS Glue resource.

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
    def get_client(self) -> Iterator[WrappedGlueClient]:
        """Get an AWS Glue client with cost tracking."""
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 is required. Install it with: pip install boto3")

        context, asset_key = get_current_context_and_asset_key()

        client = boto3.client(
            "glue",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            **self._kwargs,
        )

        wrapped_client = WrappedGlueClient(client, context, asset_key)

        yield wrapped_client

        # After operations, query job run costs
        if wrapped_client._job_run_ids:
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)

            total_dpu_hours = 0.0
            total_execution_time_ms = 0
            job_ids = []

            for job_run_id, opaque_id, job_name in wrapped_client._job_run_ids:
                try:
                    # Get job run details
                    run = client.get_job_run(JobName=job_name, RunId=job_run_id)
                    if "CompletedOn" in run["JobRun"] and "StartedOn" in run["JobRun"]:
                        execution_time_ms = int(
                            (run["JobRun"]["CompletedOn"] - run["JobRun"]["StartedOn"]).total_seconds() * 1000
                        )
                        total_execution_time_ms += execution_time_ms
                        
                        # Get DPU hours from job run
                        # DPU hours = (execution_time * allocated_capacity) / 3600
                        allocated_capacity = run["JobRun"].get("AllocatedCapacity", 2)  # Default 2 DPU
                        dpu_hours = (execution_time_ms / 1000 / 3600) * allocated_capacity
                        total_dpu_hours += dpu_hours
                        job_ids.append(job_run_id)
                except Exception:
                    # If we can't get run details, skip it
                    pass

            if total_dpu_hours > 0 or total_execution_time_ms > 0:
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata=build_glue_cost_metadata(
                            job_ids,
                            total_dpu_hours,
                            total_execution_time_ms,
                        ),
                    )
                )

