from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Optional, Union

import yaml
from dagster import (
    AssetCheckEvaluation,
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    OpExecutionContext,
    Output,
)

from dagster_insights.mysql.mysql_utils import (
    OPAQUE_ID_SQL_SIGIL,
    build_opaque_id_metadata,
    marker_asset_key_for_job,
)
from dagster_insights.insights_utils import (
    extract_asset_info_from_event,
    handle_raise_on_error,
)

if TYPE_CHECKING:
    from dagster_dbt import DbtCliInvocation


@handle_raise_on_error("dbt_cli_invocation")
def dbt_with_mysql_insights(
    context: Union[OpExecutionContext, AssetExecutionContext],
    dbt_cli_invocation: "DbtCliInvocation",
    dagster_events: Optional[
        Iterable[
            Union[
                Output,
                AssetMaterialization,
                AssetObservation,
                AssetCheckResult,
                AssetCheckEvaluation,
            ]
        ]
    ] = None,
    skip_config_check=False,
    record_observation_usage: bool = True,
) -> Iterator[
    Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult, AssetCheckEvaluation]
]:
    """Wraps a dagster-dbt invocation to associate each MySQL query with the produced
    asset materializations. This allows the cost of each query to be associated with the asset
    materialization that it produced.

    Args:
        context: The context of the asset that is being materialized.
        dbt_cli_invocation: The invocation of the dbt CLI to wrap.
        dagster_events: The events that were produced by the dbt CLI invocation.
        skip_config_check: If true, skips the check that the dbt project config is set up correctly.
        record_observation_usage: If True, associates the usage associated with asset observations with that asset.

    Example:
        .. code-block:: python

            from dagster_dbt import DbtCliResource, dbt_assets
            from dagster_insights import dbt_with_mysql_insights

            @dbt_assets(manifest=DBT_MANIFEST_PATH)
            def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
                dbt_cli_invocation = dbt.cli(["build"], context=context)
                yield from dbt_with_mysql_insights(context, dbt_cli_invocation)
    """
    if not skip_config_check:
        adapter_type = dbt_cli_invocation.manifest["metadata"]["adapter_type"]
        if adapter_type != "mysql":
            raise RuntimeError(
                f"The 'mysql' adapter must be used but instead found '{adapter_type}'"
            )
        dbt_project_config = yaml.safe_load(
            (dbt_cli_invocation.project_dir / "dbt_project.yml").open("r")
        )
        query_comment = dbt_project_config.get("query-comment")
        if query_comment is None:
            raise RuntimeError("query-comment is required in dbt_project.yml but it was missing")
        comment = query_comment.get("comment")
        if comment is None:
            raise RuntimeError(
                "query-comment.comment is required in dbt_project.yml but it was missing"
            )
        if OPAQUE_ID_SQL_SIGIL not in comment:
            raise RuntimeError(
                "query-comment.comment in dbt_project.yml must contain the string"
                f" '{OPAQUE_ID_SQL_SIGIL}'. Read the Dagster Insights docs for more info."
            )

    if dagster_events is None:
        dagster_events = dbt_cli_invocation.stream()

    asset_and_partition_key_to_unique_id: list[tuple[AssetKey, Optional[str], Any]] = []
    for dagster_event in dagster_events:
        if isinstance(
            dagster_event,
            (
                AssetMaterialization,
                AssetObservation,
                Output,
                AssetCheckResult,
                AssetCheckEvaluation,
            ),
        ):
            unique_id = dagster_event.metadata["unique_id"].value
            asset_key, partition = extract_asset_info_from_event(
                context, dagster_event, record_observation_usage
            )
            if asset_key is not None:
                asset_and_partition_key_to_unique_id.append((asset_key, partition, unique_id))
        yield dagster_event

    # Emit opaque ID metadata for cost tracking
    for asset_key, partition, unique_id in asset_and_partition_key_to_unique_id:
        context.log_event(
            AssetObservation(
                asset_key=asset_key if asset_key else marker_asset_key_for_job(context.job_def),
                partition=partition,
                metadata=build_opaque_id_metadata([context.get_opaque_id()]),
            )
        )
