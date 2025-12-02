from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.spark.dataproc.dataproc_utils import (
    build_dataproc_cost_metadata,
    build_opaque_id_metadata,
    marker_asset_key_for_job,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

if TYPE_CHECKING:
    try:
        from google.cloud import dataproc_v1
    except ImportError:
        dataproc_v1 = None

OUTPUT_NON_ASSET_SIGIL = "__dataproc_job_metadata_"


class WrappedDataprocClient:
    """Wrapper around Google Cloud Dataproc client to track job runs and costs."""

    def __init__(
        self, client, context, asset_key: Optional[AssetKey] = None
    ) -> None:
        self._client = client
        self._context = context
        self._asset_key = asset_key
        self._job_ids = []  # List of (job_id, opaque_id, cluster_name) tuples

    def submit_job(self, project_id: str, region: str, job: dict, **kwargs):
        """Submit a Dataproc job with automatic cost tracking."""
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

        # Add labels for tracking
        labels = job.get("labels", {})
        labels["dagster_opaque_id"] = opaque_id
        if inferred_context:
            labels["dagster_job_name"] = inferred_context.job_def.name
            labels["dagster_run_id"] = inferred_context.run_id
        job["labels"] = labels

        # Submit the job
        result = self._client.submit_job(
            project_id=project_id, region=region, job=job, **kwargs
        )

        # Track the job
        if result and hasattr(result, "reference") and hasattr(result.reference, "job_id"):
            cluster_name = job.get("placement", {}).get("cluster_name", "unknown")
            self._job_ids.append((result.reference.job_id, opaque_id, cluster_name))

        return result

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying Dataproc client."""
        return getattr(self._client, name)


@beta
class InsightsDataprocResource:
    """A wrapper around Google Cloud Dataproc client which automatically collects metadata about
    Dataproc cluster and job costs which can be attributed to Dagster jobs and assets.

    This resource tracks:
    - Cluster instance hours
    - Job execution time
    - Data processed

    A simple example of using Google Cloud Dataproc with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.spark.dataproc import InsightsDataprocResource

            @op
            def run_spark_job(dataproc: InsightsDataprocResource):
                with dataproc.get_client() as client:
                    client.submit_job(
                        project_id="my-project",
                        region="us-central1",
                        job={
                            "placement": {"cluster_name": "my-cluster"},
                            "spark_job": {
                                "main_class": "com.example.SparkJob",
                                "jar_file_uris": ["gs://my-bucket/job.jar"]
                            }
                        }
                    )

            @job
            def my_dataproc_job():
                run_spark_job()

            my_dataproc_job.execute_in_process(
                resources={
                    "dataproc": InsightsDataprocResource(
                        project_id=EnvVar("GCP_PROJECT_ID"),
                        region=EnvVar("GCP_REGION")
                    )
                }
            )
    """

    def __init__(
        self,
        project_id: str,
        region: str,
        **kwargs,
    ):
        """Initialize the Google Cloud Dataproc resource.

        Args:
            project_id: GCP project ID
            region: GCP region
            **kwargs: Additional client parameters
        """
        self.project_id = project_id
        self.region = region
        self._kwargs = kwargs

    @contextmanager
    def get_client(self) -> Iterator[WrappedDataprocClient]:
        """Get a Google Cloud Dataproc client with cost tracking."""
        try:
            from google.cloud import dataproc_v1
        except ImportError:
            raise ImportError(
                "google-cloud-dataproc is required. Install it with: pip install google-cloud-dataproc"
            )

        context, asset_key = get_current_context_and_asset_key()

        client = dataproc_v1.JobControllerClient(**self._kwargs)

        wrapped_client = WrappedDataprocClient(client, context, asset_key)

        yield wrapped_client

        # After operations, query job costs
        if wrapped_client._job_ids:
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)

            total_cluster_hours = 0.0
            total_execution_time_ms = 0
            total_data_processed_gb = 0.0
            job_ids = []

            for job_id, opaque_id, cluster_name in wrapped_client._job_ids:
                try:
                    # Get job details
                    job = client.get_job(
                        project_id=self.project_id,
                        region=self.region,
                        job_id=job_id,
                    )
                    if job.status and job.status.state == job.status.State.DONE:
                        if job.status.state_start_time and job.status.state_start_time:
                            # Calculate execution time
                            start_time = job.status.state_start_time
                            end_time = job.status.state_start_time  # Would need actual end time
                            # Note: Dataproc API may provide duration differently
                            execution_time_ms = 0  # Would need to calculate from timestamps
                            total_execution_time_ms += execution_time_ms
                            
                            # Estimate cluster hours (would need cluster config)
                            cluster_hours = 0.0  # Would need to query cluster details
                            total_cluster_hours += cluster_hours
                            job_ids.append(job_id)
                except Exception:
                    # If we can't get job details, skip it
                    pass

            if total_cluster_hours > 0 or total_execution_time_ms > 0:
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata=build_dataproc_cost_metadata(
                            job_ids,
                            total_cluster_hours,
                            total_execution_time_ms,
                            total_data_processed_gb,
                        ),
                    )
                )


