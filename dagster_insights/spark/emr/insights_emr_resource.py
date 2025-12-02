from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.spark.emr.emr_utils import (
    build_emr_cost_metadata,
    build_opaque_id_metadata,
    marker_asset_key_for_job,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

if TYPE_CHECKING:
    try:
        import boto3
        from mypy_boto3_emr import EMRClient
    except ImportError:
        EMRClient = None

OUTPUT_NON_ASSET_SIGIL = "__emr_job_metadata_"


class WrappedEMRClient:
    """Wrapper around AWS EMR client to track job runs and costs."""

    def __init__(self, client, context, asset_key: Optional[AssetKey] = None) -> None:
        self._client = client
        self._context = context
        self._asset_key = asset_key
        self._job_flow_ids = []  # List of (job_flow_id, opaque_id) tuples
        self._step_ids = []  # List of (step_id, opaque_id, job_flow_id) tuples

    def run_job_flow(self, **kwargs):
        """Run an EMR job flow with automatic cost tracking."""
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

        # Add tags for tracking
        tags = kwargs.get("Tags", [])
        if not isinstance(tags, list):
            tags = []
        tags.append({"Key": "dagster_opaque_id", "Value": opaque_id})
        if inferred_context:
            tags.append({"Key": "dagster_job_name", "Value": inferred_context.job_def.name})
            tags.append({"Key": "dagster_run_id", "Value": inferred_context.run_id})
        kwargs["Tags"] = tags

        # Run the job flow
        result = self._client.run_job_flow(**kwargs)

        # Track the job flow
        if result and "JobFlowId" in result:
            self._job_flow_ids.append((result["JobFlowId"], opaque_id))

        return result

    def add_job_flow_steps(self, JobFlowId: str, **kwargs):
        """Add steps to an EMR job flow with automatic cost tracking."""
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

        # Add steps
        result = self._client.add_job_flow_steps(JobFlowId=JobFlowId, **kwargs)

        # Track the steps
        if result and "StepIds" in result:
            for step_id in result["StepIds"]:
                self._step_ids.append((step_id, opaque_id, JobFlowId))

        return result

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying EMR client."""
        return getattr(self._client, name)


@beta
class InsightsEMRResource:
    """A wrapper around AWS EMR client which automatically collects metadata about
    EMR cluster and job costs which can be attributed to Dagster jobs and assets.

    This resource tracks:
    - Cluster instance hours
    - Job execution time
    - Data processed

    A simple example of using AWS EMR with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.spark.emr import InsightsEMRResource

            @op
            def run_spark_job(emr: InsightsEMRResource):
                with emr.get_client() as client:
                    client.run_job_flow(
                        Name="my_spark_job",
                        ReleaseLabel="emr-6.15.0",
                        Instances={
                            "InstanceCount": 3,
                            "MasterInstanceType": "m5.xlarge",
                            "SlaveInstanceType": "m5.xlarge"
                        },
                        Applications=[{"Name": "Spark"}]
                    )

            @job
            def my_emr_job():
                run_spark_job()

            my_emr_job.execute_in_process(
                resources={
                    "emr": InsightsEMRResource(
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
        """Initialize the AWS EMR resource.

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
    def get_client(self) -> Iterator[WrappedEMRClient]:
        """Get an AWS EMR client with cost tracking."""
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 is required. Install it with: pip install boto3")

        context, asset_key = get_current_context_and_asset_key()

        client = boto3.client(
            "emr",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            **self._kwargs,
        )

        wrapped_client = WrappedEMRClient(client, context, asset_key)

        yield wrapped_client

        # After operations, query cluster and job costs
        if wrapped_client._job_flow_ids or wrapped_client._step_ids:
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)

            total_cluster_hours = 0.0
            total_execution_time_ms = 0
            total_data_processed_gb = 0.0
            job_ids = []

            # Calculate costs from job flows
            for job_flow_id, opaque_id in wrapped_client._job_flow_ids:
                try:
                    # Get cluster details
                    cluster = client.describe_cluster(ClusterId=job_flow_id)
                    if "Cluster" in cluster:
                        cluster_info = cluster["Cluster"]
                        if "Status" in cluster_info and "Timeline" in cluster_info["Status"]:
                            timeline = cluster_info["Status"]["Timeline"]
                            if "CreationDateTime" in timeline and "EndDateTime" in timeline:
                                start_time = timeline["CreationDateTime"]
                                end_time = timeline.get("EndDateTime")
                                if end_time:
                                    execution_time_ms = int(
                                        (end_time - start_time).total_seconds() * 1000
                                    )
                                    total_execution_time_ms += execution_time_ms
                                    
                                    # Calculate cluster hours
                                    # Count instances from cluster configuration
                                    instance_count = cluster_info.get("InstanceCollectionType", {}).get("InstanceCount", 1)
                                    cluster_hours = (execution_time_ms / 1000 / 3600) * instance_count
                                    total_cluster_hours += cluster_hours
                                    job_ids.append(job_flow_id)
                except Exception:
                    # If we can't get cluster details, skip it
                    pass

            if total_cluster_hours > 0 or total_execution_time_ms > 0:
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata=build_emr_cost_metadata(
                            job_ids,
                            total_cluster_hours,
                            total_execution_time_ms,
                            total_data_processed_gb,
                        ),
                    )
                )


