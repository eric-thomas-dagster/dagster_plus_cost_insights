from typing import Optional

from dagster import AssetKey, AssetObservation, JobDefinition

# Metadata key prefix used to tag RDS queries with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_rds_opaque_id:"

OUTPUT_NON_ASSET_SIGIL = "__rds_query_metadata_"

# RDS cost metadata keys (extends PostgreSQL/MySQL with RDS-specific costs)
RDS_METADATA_EXECUTION_TIME_MS = "__rds_execution_time_ms"
RDS_METADATA_ROWS_PROCESSED = "__rds_rows_processed"
RDS_METADATA_INSTANCE_HOURS = "__rds_instance_hours"
RDS_METADATA_STORAGE_GB = "__rds_storage_gb"
RDS_METADATA_IOPS = "__rds_iops"
RDS_METADATA_QUERY_IDS = "__rds_query_ids"


def build_opaque_id_metadata(opaque_id: str) -> dict:
    """Build metadata dictionary for opaque ID tracking."""
    return {f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True}


def marker_asset_key_for_job(
    job: JobDefinition,
) -> AssetKey:
    """Create a marker asset key for jobs that don't materialize assets."""
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def build_rds_cost_metadata(
    query_ids: Optional[list[str]],
    execution_time_ms: int,
    instance_hours: float = 0.0,
    storage_gb: float = 0.0,
    iops: int = 0,
    rows_processed: int = 0,
) -> dict:
    """Build metadata dictionary for AWS RDS cost information.
    
    RDS costs include:
    - Instance hours (compute)
    - Storage (GB)
    - IOPS (I/O operations)
    - Data transfer
    """
    metadata: dict = {
        RDS_METADATA_EXECUTION_TIME_MS: execution_time_ms,
        RDS_METADATA_ROWS_PROCESSED: rows_processed,
        RDS_METADATA_INSTANCE_HOURS: instance_hours,
        RDS_METADATA_STORAGE_GB: storage_gb,
        RDS_METADATA_IOPS: iops,
    }
    if query_ids:
        metadata[RDS_METADATA_QUERY_IDS] = query_ids
    return metadata


