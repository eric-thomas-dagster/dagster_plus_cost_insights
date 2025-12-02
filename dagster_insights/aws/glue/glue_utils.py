from typing import Optional

from dagster import AssetKey, AssetObservation, JobDefinition

# Metadata key prefix used to tag AWS Glue jobs with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_glue_opaque_id:"

OUTPUT_NON_ASSET_SIGIL = "__glue_job_metadata_"

# AWS Glue cost metadata keys
GLUE_METADATA_DPU_HOURS = "__glue_dpu_hours"
GLUE_METADATA_EXECUTION_TIME_MS = "__glue_execution_time_ms"
GLUE_METADATA_JOB_IDS = "__glue_job_ids"


def build_opaque_id_metadata(opaque_id: str) -> dict:
    """Build metadata dictionary for opaque ID tracking."""
    return {f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True}


def marker_asset_key_for_job(
    job: JobDefinition,
) -> AssetKey:
    """Create a marker asset key for jobs that don't materialize assets."""
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def build_glue_cost_metadata(
    job_ids: Optional[list[str]],
    dpu_hours: float,
    execution_time_ms: int,
) -> dict:
    """Build metadata dictionary for AWS Glue cost information.
    
    DPU (Data Processing Units) is the compute unit for AWS Glue.
    """
    metadata: dict = {
        GLUE_METADATA_DPU_HOURS: dpu_hours,
        GLUE_METADATA_EXECUTION_TIME_MS: execution_time_ms,
    }
    if job_ids:
        metadata[GLUE_METADATA_JOB_IDS] = job_ids
    return metadata


