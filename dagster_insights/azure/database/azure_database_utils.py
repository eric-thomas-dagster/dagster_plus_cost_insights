from typing import Optional

from dagster import AssetKey, AssetObservation, JobDefinition

# Metadata key prefix used to tag Azure Database queries with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_azure_database_opaque_id:"

OUTPUT_NON_ASSET_SIGIL = "__azure_database_query_metadata_"

# Azure Database cost metadata keys (extends PostgreSQL/MySQL with Azure-specific costs)
AZURE_DATABASE_METADATA_EXECUTION_TIME_MS = "__azure_database_execution_time_ms"
AZURE_DATABASE_METADATA_ROWS_PROCESSED = "__azure_database_rows_processed"
AZURE_DATABASE_METADATA_COMPUTE_UNITS = "__azure_database_compute_units"
AZURE_DATABASE_METADATA_STORAGE_GB = "__azure_database_storage_gb"
AZURE_DATABASE_METADATA_QUERY_IDS = "__azure_database_query_ids"


def build_opaque_id_metadata(opaque_id: str) -> dict:
    """Build metadata dictionary for opaque ID tracking."""
    return {f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True}


def marker_asset_key_for_job(
    job: JobDefinition,
) -> AssetKey:
    """Create a marker asset key for jobs that don't materialize assets."""
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def build_azure_database_cost_metadata(
    query_ids: Optional[list[str]],
    execution_time_ms: int,
    compute_units: float = 0.0,
    storage_gb: float = 0.0,
    rows_processed: int = 0,
) -> dict:
    """Build metadata dictionary for Azure Database cost information.
    
    Azure Database costs include:
    - Compute units (vCores)
    - Storage (GB)
    - Backup storage
    """
    metadata: dict = {
        AZURE_DATABASE_METADATA_EXECUTION_TIME_MS: execution_time_ms,
        AZURE_DATABASE_METADATA_ROWS_PROCESSED: rows_processed,
        AZURE_DATABASE_METADATA_COMPUTE_UNITS: compute_units,
        AZURE_DATABASE_METADATA_STORAGE_GB: storage_gb,
    }
    if query_ids:
        metadata[AZURE_DATABASE_METADATA_QUERY_IDS] = query_ids
    return metadata


