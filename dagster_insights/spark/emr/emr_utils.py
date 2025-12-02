from typing import Optional

from dagster import AssetKey, AssetObservation, JobDefinition

# Metadata key prefix used to tag EMR job runs with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_emr_opaque_id:"

OUTPUT_NON_ASSET_SIGIL = "__emr_job_metadata_"

# EMR cost metadata keys
EMR_METADATA_CLUSTER_HOURS = "__emr_cluster_hours"
EMR_METADATA_EXECUTION_TIME_MS = "__emr_execution_time_ms"
EMR_METADATA_DATA_PROCESSED_GB = "__emr_data_processed_gb"
EMR_METADATA_JOB_IDS = "__emr_job_ids"


def build_opaque_id_metadata(opaque_id: str) -> dict:
    """Build metadata dictionary for opaque ID tracking."""
    return {f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True}


def marker_asset_key_for_job(
    job: JobDefinition,
) -> AssetKey:
    """Create a marker asset key for jobs that don't materialize assets."""
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def build_emr_cost_metadata(
    job_ids: Optional[list[str]],
    cluster_hours: float,
    execution_time_ms: int,
    data_processed_gb: float = 0.0,
) -> dict:
    """Build metadata dictionary for AWS EMR cost information.
    
    EMR costs are typically based on:
    - Cluster instance hours (EC2 instances)
    - EMR service charges
    - Data transfer
    """
    metadata: dict = {
        EMR_METADATA_CLUSTER_HOURS: cluster_hours,
        EMR_METADATA_EXECUTION_TIME_MS: execution_time_ms,
        EMR_METADATA_DATA_PROCESSED_GB: data_processed_gb,
    }
    if job_ids:
        metadata[EMR_METADATA_JOB_IDS] = job_ids
    return metadata


