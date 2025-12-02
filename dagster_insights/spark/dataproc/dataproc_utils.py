from typing import Optional

from dagster import AssetKey, AssetObservation, JobDefinition

# Metadata key prefix used to tag Dataproc job runs with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_dataproc_opaque_id:"

OUTPUT_NON_ASSET_SIGIL = "__dataproc_job_metadata_"

# Dataproc cost metadata keys
DATAPROC_METADATA_CLUSTER_HOURS = "__dataproc_cluster_hours"
DATAPROC_METADATA_EXECUTION_TIME_MS = "__dataproc_execution_time_ms"
DATAPROC_METADATA_DATA_PROCESSED_GB = "__dataproc_data_processed_gb"
DATAPROC_METADATA_JOB_IDS = "__dataproc_job_ids"


def build_opaque_id_metadata(opaque_id: str) -> dict:
    """Build metadata dictionary for opaque ID tracking."""
    return {f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True}


def marker_asset_key_for_job(
    job: JobDefinition,
) -> AssetKey:
    """Create a marker asset key for jobs that don't materialize assets."""
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def build_dataproc_cost_metadata(
    job_ids: Optional[list[str]],
    cluster_hours: float,
    execution_time_ms: int,
    data_processed_gb: float = 0.0,
) -> dict:
    """Build metadata dictionary for Google Cloud Dataproc cost information.
    
    Dataproc costs are typically based on:
    - Cluster instance hours (Compute Engine VMs)
    - Dataproc service charges
    - Data transfer
    """
    metadata: dict = {
        DATAPROC_METADATA_CLUSTER_HOURS: cluster_hours,
        DATAPROC_METADATA_EXECUTION_TIME_MS: execution_time_ms,
        DATAPROC_METADATA_DATA_PROCESSED_GB: data_processed_gb,
    }
    if job_ids:
        metadata[DATAPROC_METADATA_JOB_IDS] = job_ids
    return metadata


