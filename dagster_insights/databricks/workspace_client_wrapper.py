"""Wrapper for Databricks WorkspaceClient to track job runs and costs."""

from typing import TYPE_CHECKING, Optional

from dagster import AssetKey
from dagster_insights.databricks.databricks_utils import (
    OPAQUE_ID_METADATA_KEY_PREFIX,
    build_opaque_id_metadata,
    marker_asset_key_for_job,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


class WrappedDatabricksWorkspaceClient:
    """Wrapper around Databricks WorkspaceClient to track job runs and costs.
    
    This wrapper intercepts job submission methods to automatically tag
    job runs with opaque IDs for cost attribution.
    """

    def __init__(
        self,
        client: "WorkspaceClient",
        context,
        asset_key: Optional[AssetKey] = None,
    ) -> None:
        self._client = client
        self._context = context
        self._asset_key = asset_key
        self._tracked_runs = []  # List of (run_id, opaque_id) tuples

    def submit_job_run(
        self,
        job_id: Optional[int] = None,
        notebook_path: Optional[str] = None,
        python_file: Optional[str] = None,
        parameters: Optional[dict] = None,
        tags: Optional[dict] = None,
        **kwargs,
    ):
        """Submit a job run with automatic cost tracking.
        
        This method wraps the WorkspaceClient.jobs.submit() method and
        automatically adds tags for cost attribution.
        """
        from uuid import uuid4

        # Get or create asset key
        inferred_context, inferred_asset_key = get_current_context_and_asset_key()
        associated_asset_key = self._asset_key or inferred_asset_key
        if not associated_asset_key and inferred_context:
            associated_asset_key = marker_asset_key_for_job(inferred_context.job_def)

        # Generate opaque ID for this job run
        opaque_id = str(uuid4())

        # Emit observation with opaque ID
        if inferred_context and associated_asset_key:
            inferred_context.log_event(
                AssetObservation(
                    asset_key=associated_asset_key,
                    metadata=build_opaque_id_metadata(opaque_id),
                )
            )

        # Prepare tags for the job run
        job_tags = tags or {}
        job_tags[f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}"] = "true"
        if inferred_context:
            job_tags["dagster_job_name"] = inferred_context.job_def.name
            job_tags["dagster_run_id"] = inferred_context.run_id

        # Submit the job with tags
        if job_id:
            result = self._client.jobs.submit(
                job_id=job_id,
                notebook_params=parameters if notebook_path else None,
                python_params=parameters if python_file else None,
                tags=job_tags,
                **kwargs,
            )
        else:
            # Create a new job run
            result = self._client.jobs.submit(
                tasks=[{
                    "notebook_task": {"notebook_path": notebook_path} if notebook_path else None,
                    "python_wheel_task": {"entry_point": python_file} if python_file else None,
                }],
                tags=job_tags,
                **kwargs,
            )

        # Track the run
        if result and hasattr(result, "run_id"):
            self._tracked_runs.append((result.run_id, opaque_id))

        return result

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying WorkspaceClient."""
        attr = getattr(self._client, name)
        
        # Wrap job submission methods
        if name == "jobs":
            return WrappedJobsClient(self._client.jobs, self._context, self._asset_key, self._tracked_runs)
        
        return attr


class WrappedJobsClient:
    """Wrapper for Databricks JobsClient to track job submissions."""

    def __init__(self, jobs_client, context, asset_key: Optional[AssetKey], tracked_runs):
        self._jobs_client = jobs_client
        self._context = context
        self._asset_key = asset_key
        self._tracked_runs = tracked_runs

    def submit(self, *args, **kwargs):
        """Submit a job run with automatic cost tracking."""
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

        # Add tags to job submission
        tags = kwargs.get("tags", {})
        if not isinstance(tags, dict):
            tags = {}
        tags[f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}"] = "true"
        if inferred_context:
            tags["dagster_job_name"] = inferred_context.job_def.name
            tags["dagster_run_id"] = inferred_context.run_id
        kwargs["tags"] = tags

        # Submit the job
        result = self._jobs_client.submit(*args, **kwargs)

        # Track the run
        if result and hasattr(result, "run_id"):
            self._tracked_runs.append((result.run_id, opaque_id))

        return result

    def __getattr__(self, name):
        """Delegate all other attributes to the underlying JobsClient."""
        return getattr(self._jobs_client, name)


