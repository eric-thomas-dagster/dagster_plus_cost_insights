from typing import Optional

from dagster import AssetKey, AssetObservation, JobDefinition

# Metadata key prefix used to tag Cloud SQL queries with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_cloud_sql_opaque_id:"

OUTPUT_NON_ASSET_SIGIL = "__cloud_sql_query_metadata_"

# Cloud SQL cost metadata keys (extends PostgreSQL/MySQL with GCP-specific costs)
CLOUD_SQL_METADATA_EXECUTION_TIME_MS = "__cloud_sql_execution_time_ms"
CLOUD_SQL_METADATA_ROWS_PROCESSED = "__cloud_sql_rows_processed"
CLOUD_SQL_METADATA_INSTANCE_HOURS = "__cloud_sql_instance_hours"
CLOUD_SQL_METADATA_STORAGE_GB = "__cloud_sql_storage_gb"
CLOUD_SQL_METADATA_QUERY_IDS = "__cloud_sql_query_ids"


def build_opaque_id_metadata(opaque_id: str) -> dict:
    """Build metadata dictionary for opaque ID tracking."""
    return {f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True}


def marker_asset_key_for_job(
    job: JobDefinition,
) -> AssetKey:
    """Create a marker asset key for jobs that don't materialize assets."""
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def build_cloud_sql_cost_metadata(
    query_ids: Optional[list[str]],
    execution_time_ms: int,
    instance_hours: float = 0.0,
    storage_gb: float = 0.0,
    rows_processed: int = 0,
) -> dict:
    """Build metadata dictionary for Google Cloud SQL cost information.
    
    Cloud SQL costs include:
    - Instance hours (compute)
    - Storage (GB)
    - Network egress
    """
    metadata: dict = {
        CLOUD_SQL_METADATA_EXECUTION_TIME_MS: execution_time_ms,
        CLOUD_SQL_METADATA_ROWS_PROCESSED: rows_processed,
        CLOUD_SQL_METADATA_INSTANCE_HOURS: instance_hours,
        CLOUD_SQL_METADATA_STORAGE_GB: storage_gb,
    }
    if query_ids:
        metadata[CLOUD_SQL_METADATA_QUERY_IDS] = query_ids
    return metadata


