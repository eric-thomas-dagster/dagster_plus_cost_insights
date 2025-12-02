"""Helper to track Databricks job runs and associate them with Dagster assets."""

from typing import Optional
from uuid import uuid4

from dagster import AssetKey, AssetObservation
from dagster_insights.databricks.databricks_utils import (
    OPAQUE_ID_METADATA_KEY_PREFIX,
    build_opaque_id_metadata,
    marker_asset_key_for_job,
)
from dagster_insights.insights_utils import get_current_context_and_asset_key


class DatabricksJobTracker:
    """Tracks Databricks job runs and associates them with Dagster assets.
    
    This helps track costs for:
    - Notebook jobs
    - Python jobs
    - JAR jobs
    - Asset bundles
    - LakeFlow pipelines
    - Delta Live Tables
    """

    def __init__(self):
        self._run_ids = []
        self._opaque_ids = []

    def submit_job_run(
        self,
        job_id: Optional[int] = None,
        notebook_path: Optional[str] = None,
        python_file: Optional[str] = None,
        parameters: Optional[dict] = None,
        tags: Optional[dict] = None,
    ) -> tuple[int, str]:
        """Submit a job run and return (run_id, opaque_id).
        
        The opaque_id can be used to match the job run to costs later.
        """
        context, asset_key = get_current_context_and_asset_key()
        if not context:
            # Can't track without context
            return None, None

        if not asset_key:
            asset_key = marker_asset_key_for_job(context.job_def)

        # Generate opaque ID for this job run
        opaque_id = str(uuid4())
        self._opaque_ids.append((opaque_id, asset_key))

        # Emit observation with opaque ID
        context.log_event(
            AssetObservation(
                asset_key=asset_key,
                metadata=build_opaque_id_metadata(opaque_id),
            )
        )

        # Prepare tags for the job run
        job_tags = tags or {}
        job_tags[f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}"] = "true"
        job_tags["dagster_job_name"] = context.job_def.name
        job_tags["dagster_run_id"] = context.run_id

        # Return tags to be used when submitting the job
        return opaque_id, job_tags

    def track_job_run(self, run_id: int, opaque_id: str):
        """Track a job run that was already submitted."""
        self._run_ids.append((run_id, opaque_id))


