"""Shared utilities for object storage cost insights."""

from typing import Optional

from dagster import AssetKey, JobDefinition

OUTPUT_NON_ASSET_SIGIL = "__storage_query_metadata_"


def marker_asset_key_for_job(
    job: JobDefinition,
) -> AssetKey:
    """Create a marker asset key for jobs that don't materialize assets."""
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def match_bucket_to_asset(bucket_name: str, asset_keys: list[AssetKey]) -> Optional[AssetKey]:
    """Match a bucket/container name to a Dagster asset key.

    This uses naming conventions to match storage buckets to assets.
    You may want to customize this logic based on your naming patterns.

    Args:
        bucket_name: Name of the S3 bucket, GCS bucket, or Azure container
        asset_keys: List of available asset keys

    Returns:
        Matching AssetKey if found, None otherwise
    """
    # Simple matching: check if bucket name appears in asset key path
    for asset_key in asset_keys:
        # Check if bucket name matches any part of the asset key
        if bucket_name in str(asset_key):
            return asset_key

        # Check if asset key path contains bucket-like patterns
        for path_component in asset_key.path:
            if bucket_name.lower() in path_component.lower():
                return asset_key

    return None


