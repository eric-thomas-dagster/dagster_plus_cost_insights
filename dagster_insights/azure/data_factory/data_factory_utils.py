from typing import Optional

from dagster import AssetKey, AssetObservation, JobDefinition

# Metadata key prefix used to tag Azure Data Factory pipeline runs with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_adf_opaque_id:"

OUTPUT_NON_ASSET_SIGIL = "__adf_pipeline_metadata_"

# Azure Data Factory cost metadata keys
ADF_METADATA_EXECUTION_TIME_MS = "__adf_execution_time_ms"
ADF_METADATA_ACTIVITY_RUNS = "__adf_activity_runs"
ADF_METADATA_PIPELINE_RUN_IDS = "__adf_pipeline_run_ids"


def build_opaque_id_metadata(opaque_id: str) -> dict:
    """Build metadata dictionary for opaque ID tracking."""
    return {f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True}


def marker_asset_key_for_job(
    job: JobDefinition,
) -> AssetKey:
    """Create a marker asset key for jobs that don't materialize assets."""
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def build_adf_cost_metadata(
    pipeline_run_ids: Optional[list[str]],
    execution_time_ms: int,
    activity_runs: int = 0,
) -> dict:
    """Build metadata dictionary for Azure Data Factory cost information.
    
    ADF costs are typically based on:
    - Pipeline execution time
    - Activity execution time
    - Data movement volume
    - Integration runtime usage
    """
    metadata: dict = {
        ADF_METADATA_EXECUTION_TIME_MS: execution_time_ms,
        ADF_METADATA_ACTIVITY_RUNS: activity_runs,
    }
    if pipeline_run_ids:
        metadata[ADF_METADATA_PIPELINE_RUN_IDS] = pipeline_run_ids
    return metadata


