from typing import Optional, Union
from uuid import uuid4 as uuid

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetObservation,
    JobDefinition,
    OpExecutionContext,
)

# Metadata key prefix used to tag Redshift queries with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_redshift_opaque_id:"
OPAQUE_ID_SQL_SIGIL = "redshift_dagster_dbt_v1_opaque_id"

OUTPUT_NON_ASSET_SIGIL = "__redshift_query_metadata_"

# Redshift cost metadata keys
REDSHIFT_METADATA_EXECUTION_TIME_MS = "__redshift_execution_time_ms"
REDSHIFT_METADATA_BYTES_SCANNED = "__redshift_bytes_scanned"
REDSHIFT_METADATA_ROWS_PROCESSED = "__redshift_rows_processed"
REDSHIFT_METADATA_QUERY_IDS = "__redshift_query_ids"


def build_opaque_id_metadata(opaque_id: str) -> dict:
    """Build metadata dictionary for opaque ID tracking."""
    return {f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True}


def meter_redshift_query(
    context: Union[OpExecutionContext, AssetExecutionContext],
    sql: str,
    comment_factory=lambda comment: f"\n-- {comment}\n",
    opaque_id=None,
    associated_asset_key: Optional[AssetKey] = None,
):
    """A utility function that takes a SQL query and returns a modified version of the query
    that includes a comment that will be used to identify the query when attributing cost.
    Logs an AssetObservation event to associate the query with the executing op/asset.

    .. code-block:: python

        from dagster import asset, AssetExecutionContext
        from dagster_aws import RedshiftResource
        from dagster_insights.redshift.redshift_utils import meter_redshift_query

        @asset
        def my_cool_asset(
            context: AssetExecutionContext, redshift: RedshiftResource
        ) -> Dict[str, Any]:
            with redshift.get_connection() as conn:
                res = conn.execute(meter_redshift_query(context, "SELECT * FROM my_big_table"))

            return res

    """
    if context.has_assets_def and len(context.assets_def.keys_by_output_name.keys()) == 1:
        inferred_asset_key = context.asset_key
        if associated_asset_key and associated_asset_key != inferred_asset_key:
            raise RuntimeError(
                f"Op materializes asset key {inferred_asset_key}, which does not match supplied"
                f" asset key {associated_asset_key}"
            )
        associated_asset_key = inferred_asset_key
    elif context.has_assets_def:
        asset_keys = context.assets_def.keys_by_output_name.values()

        if not associated_asset_key:
            context.log.warn(
                "Op materializes assets, but no asset key associated with Redshift query usage."
            )
        if associated_asset_key and associated_asset_key not in asset_keys:
            raise RuntimeError(
                f"Op materializes assets {asset_keys}, but supplied asset key"
                f" {associated_asset_key} not found"
            )
    elif not context.has_assets_def and associated_asset_key:
        raise RuntimeError(
            f"Op does not materialize assets, but supplied asset key {associated_asset_key} found"
        )

    associated_output_name = None
    if associated_asset_key:
        associated_output_name = context.assets_def.get_output_name_for_asset_key(
            associated_asset_key
        )

    if opaque_id is None:
        opaque_id = str(uuid())
    modified_sql = sql + comment_factory(f"{OPAQUE_ID_SQL_SIGIL}[[[{opaque_id}]]]")

    if associated_output_name and associated_asset_key:
        context.log_event(
            AssetObservation(
                asset_key=associated_asset_key,
                metadata=build_opaque_id_metadata(opaque_id),
            )
        )
    else:
        context.log_event(
            AssetObservation(
                asset_key=marker_asset_key_for_job(context.job_def),
                metadata=build_opaque_id_metadata(opaque_id),
            )
        )

    return modified_sql


def marker_asset_key_for_job(
    job: JobDefinition,
) -> AssetKey:
    """Create a marker asset key for jobs that don't materialize assets."""
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def build_redshift_cost_metadata(
    query_ids: Optional[list[str]],
    execution_time_ms: int,
    bytes_scanned: int = 0,
    rows_processed: int = 0,
) -> dict:
    """Build metadata dictionary for Redshift cost information."""
    metadata: dict = {
        REDSHIFT_METADATA_EXECUTION_TIME_MS: execution_time_ms,
        REDSHIFT_METADATA_BYTES_SCANNED: bytes_scanned,
        REDSHIFT_METADATA_ROWS_PROCESSED: rows_processed,
    }
    if query_ids:
        metadata[REDSHIFT_METADATA_QUERY_IDS] = query_ids
    return metadata


