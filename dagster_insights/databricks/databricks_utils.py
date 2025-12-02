from typing import Optional, Union
from uuid import uuid4 as uuid

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetObservation,
    JobDefinition,
    OpExecutionContext,
)

# Metadata key prefix used to tag Databricks queries with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_databricks_opaque_id:"
OPAQUE_ID_SQL_SIGIL = "databricks_dagster_dbt_v1_opaque_id"

OUTPUT_NON_ASSET_SIGIL = "__databricks_query_metadata_"

# Databricks cost metadata keys
DATABRICKS_METADATA_DBU = "__databricks_dbu"
DATABRICKS_METADATA_COMPUTE_TIME_MS = "__databricks_compute_time_ms"
DATABRICKS_METADATA_JOB_IDS = "__databricks_job_ids"
DATABRICKS_METADATA_CLUSTER_ID = "__databricks_cluster_id"


def build_opaque_id_metadata(opaque_id: str) -> dict:
    """Build metadata dictionary for opaque ID tracking."""
    return {f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True}


def meter_databricks_query(
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
        from dagster_databricks import DatabricksResource
        from dagster_insights.databricks.databricks_utils import meter_databricks_query

        @asset
        def my_cool_asset(
            context: AssetExecutionContext, databricks: DatabricksResource
        ) -> Dict[str, Any]:
            with databricks.get_client() as client:
                res = client.sql(meter_databricks_query(context, "SELECT * FROM my_big_table"))

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
                "Op materializes assets, but no asset key associated with Databricks query usage."
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


def build_databricks_cost_metadata(
    job_ids: Optional[list[str]],
    dbu: float,
    compute_time_ms: int,
    cluster_id: Optional[str] = None,
) -> dict:
    """Build metadata dictionary for Databricks cost information."""
    metadata: dict = {
        DATABRICKS_METADATA_DBU: dbu,
        DATABRICKS_METADATA_COMPUTE_TIME_MS: compute_time_ms,
    }
    if job_ids:
        metadata[DATABRICKS_METADATA_JOB_IDS] = job_ids
    if cluster_id:
        metadata[DATABRICKS_METADATA_CLUSTER_ID] = cluster_id
    return metadata


