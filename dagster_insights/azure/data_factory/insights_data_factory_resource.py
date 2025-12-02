from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from dagster import AssetKey, AssetObservation
from dagster._annotations import beta

from dagster_insights.azure.data_factory.data_factory_utils import (
    build_adf_cost_metadata,
    build_opaque_id_metadata,
    marker_asset_key_for_job,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

if TYPE_CHECKING:
    try:
        from azure.mgmt.datafactory import DataFactoryManagementClient
    except ImportError:
        DataFactoryManagementClient = None

OUTPUT_NON_ASSET_SIGIL = "__adf_pipeline_metadata_"


class WrappedDataFactoryClient:
    """Wrapper around Azure Data Factory client to track pipeline runs and costs."""

    def __init__(
        self, client, context, asset_key: Optional[AssetKey] = None
    ) -> None:
        self._client = client
        self._context = context
        self._asset_key = asset_key
        self._pipeline_run_ids = []
        self._execution_times_ms = []
        self._activity_runs = []

    def create_pipeline_run(self, pipeline_name: str, **kwargs):
        """Create a pipeline run with automatic cost tracking."""
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

        # Add parameters/tags for tracking
        parameters = kwargs.get("parameters", {})
        parameters["dagster_opaque_id"] = opaque_id
        if inferred_context:
            parameters["dagster_job_name"] = inferred_context.job_def.name
            parameters["dagster_run_id"] = inferred_context.run_id
        kwargs["parameters"] = parameters

        # Create the pipeline run
        result = self._client.pipelines.create_run(
            pipeline_name=pipeline_name, **kwargs
        )

        # Track the run
        if result and hasattr(result, "run_id"):
            self._pipeline_run_ids.append((result.run_id, opaque_id))

        return result

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying Data Factory client."""
        return getattr(self._client, name)


@beta
class InsightsDataFactoryResource:
    """A wrapper around Azure Data Factory client which automatically collects metadata about
    Data Factory pipeline costs which can be attributed to Dagster jobs and assets.

    This resource tracks:
    - Pipeline run execution time
    - Activity execution time
    - Pipeline run IDs for cost attribution

    A simple example of using Azure Data Factory with cost insights:

    Examples:
        .. code-block:: python

            from dagster import job, op, EnvVar
            from dagster_insights.azure.data_factory import InsightsDataFactoryResource

            @op
            def run_pipeline(adf: InsightsDataFactoryResource):
                with adf.get_client() as client:
                    client.create_pipeline_run(pipeline_name="my_pipeline")

            @job
            def my_adf_job():
                run_pipeline()

            my_adf_job.execute_in_process(
                resources={
                    "adf": InsightsDataFactoryResource(
                        subscription_id=EnvVar("AZURE_SUBSCRIPTION_ID"),
                        resource_group=EnvVar("AZURE_RESOURCE_GROUP"),
                        factory_name=EnvVar("ADF_FACTORY_NAME")
                    )
                }
            )
    """

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        factory_name: str,
        **kwargs,
    ):
        """Initialize the Azure Data Factory resource.

        Args:
            subscription_id: Azure subscription ID
            resource_group: Resource group name
            factory_name: Data Factory name
            **kwargs: Additional client parameters
        """
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        self._kwargs = kwargs

    @contextmanager
    def get_client(self) -> Iterator[WrappedDataFactoryClient]:
        """Get an Azure Data Factory client with cost tracking."""
        try:
            from azure.mgmt.datafactory import DataFactoryManagementClient
            from azure.identity import DefaultAzureCredential
        except ImportError:
            raise ImportError(
                "azure-mgmt-datafactory and azure-identity are required. "
                "Install them with: pip install azure-mgmt-datafactory azure-identity"
            )

        context, asset_key = get_current_context_and_asset_key()

        credential = DefaultAzureCredential()
        client = DataFactoryManagementClient(credential, self.subscription_id)

        wrapped_client = WrappedDataFactoryClient(client, context, asset_key)

        yield wrapped_client

        # After operations, query pipeline run costs
        if wrapped_client._pipeline_run_ids:
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)

            total_execution_time_ms = 0
            total_activity_runs = 0
            pipeline_run_ids = []

            for run_id, opaque_id in wrapped_client._pipeline_run_ids:
                try:
                    # Get pipeline run details
                    run = client.pipeline_runs.get(
                        self.resource_group, self.factory_name, run_id
                    )
                    if run.run_start and run.run_end:
                        execution_time_ms = int(
                            (run.run_end - run.run_start).total_seconds() * 1000
                        )
                        total_execution_time_ms += execution_time_ms
                        total_activity_runs += run.activities_queued or 0
                        pipeline_run_ids.append(run_id)
                except Exception:
                    # If we can't get run details, skip it
                    pass

            if total_execution_time_ms > 0:
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata=build_adf_cost_metadata(
                            pipeline_run_ids,
                            total_execution_time_ms,
                            total_activity_runs,
                        ),
                    )
                )


