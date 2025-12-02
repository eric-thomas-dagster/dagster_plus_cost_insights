from typing import Optional

from dagster import AssetKey, AssetObservation, JobDefinition

# Metadata key prefix used to tag Athena queries with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_athena_opaque_id:"
OPAQUE_ID_SQL_SIGIL = "athena_dagster_dbt_v1_opaque_id"

OUTPUT_NON_ASSET_SIGIL = "__athena_query_metadata_"

# Athena cost metadata keys
ATHENA_METADATA_DATA_SCANNED_GB = "__athena_data_scanned_gb"
ATHENA_METADATA_EXECUTION_TIME_MS = "__athena_execution_time_ms"
ATHENA_METADATA_QUERY_IDS = "__athena_query_ids"


def build_opaque_id_metadata(opaque_id: str) -> dict:
    """Build metadata dictionary for opaque ID tracking."""
    return {f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True}


def marker_asset_key_for_job(
    job: JobDefinition,
) -> AssetKey:
    """Create a marker asset key for jobs that don't materialize assets."""
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def build_athena_cost_metadata(
    query_ids: Optional[list[str]],
    data_scanned_gb: float,
    execution_time_ms: int,
) -> dict:
    """Build metadata dictionary for AWS Athena cost information.
    
    Athena costs are based on:
    - Data scanned (charged per TB scanned)
    - Query execution time (for serverless compute)
    """
    metadata: dict = {
        ATHENA_METADATA_DATA_SCANNED_GB: data_scanned_gb,
        ATHENA_METADATA_EXECUTION_TIME_MS: execution_time_ms,
    }
    if query_ids:
        metadata[ATHENA_METADATA_QUERY_IDS] = query_ids
    return metadata


