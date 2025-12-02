"""GCS cost insights asset and schedule definitions."""

import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Union

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    DailyPartitionsDefinition,
    HourlyPartitionsDefinition,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    TimeWindow,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
    fs_io_manager,
    schedule,
)
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)

try:
    from dagster_cloud.dagster_insights.metrics_utils import put_cost_information
except ImportError:
    def put_cost_information(*args, **kwargs):
        raise ImportError(
            "dagster-cloud is required for put_cost_information. "
            "Install it with: pip install dagster-cloud[insights]"
        )

from dagster_insights.storage.gcs.gcs_insights import get_cost_data_for_hour

GCS_BILLING_LATENCY_HRS = 24


@dataclass
class GCSInsightsDefinitions:
    assets: Sequence[AssetsDefinition]
    schedule: ScheduleDefinition


def _build_run_request_for_partition_key_range(
    job: UnresolvedAssetJobDefinition,
    asset_keys: Sequence[AssetKey],
    partition_range_start: str,
    partition_range_end: str,
) -> RunRequest:
    tags = {
        ASSET_PARTITION_RANGE_START_TAG: partition_range_start,
        ASSET_PARTITION_RANGE_END_TAG: partition_range_end,
    }
    partition_key = partition_range_start if partition_range_start == partition_range_end else None
    return RunRequest(
        job_name=job.name, asset_selection=asset_keys, partition_key=partition_key, tags=tags
    )


def create_gcs_insights_asset_and_schedule(
    start_date: Union[datetime, date, str],
    project_id: str,
    name: Optional[str] = None,
    group_name: Optional[str] = None,
    job_name: str = "gcs_insights_import",
    dry_run=False,
    allow_partial_partitions=True,
    gcp_resource_key: str = "gcp",
    billing_latency_hrs: int = GCS_BILLING_LATENCY_HRS,
    partition_end_offset_hrs: int = 24,
    schedule_batch_size_hrs: int = 24,
    use_daily_partitions: bool = True,
    submit_to_s3_only: bool = True,  # DEPRECATED
) -> GCSInsightsDefinitions:
    """Generates a pre-defined Dagster asset and schedule for importing GCS cost data.

    Args:
        start_date: The date to start the partitioned schedule on.
        project_id: GCP project ID.
        name: The name of the asset. Defaults to "gcs_cost_history".
        group_name: The name of the asset group.
        job_name: The name of the job. Defaults to "gcs_insights_import".
        dry_run: If true, prints cost data instead of submitting.
        allow_partial_partitions: If true, allows running even if data may not be available.
        gcp_resource_key: The name of the GCP resource key. Defaults to "gcp".
        billing_latency_hrs: Hours to wait before querying billing API. Defaults to 24.
        partition_end_offset_hrs: Hours to offset partition end. Defaults to 24.
        schedule_batch_size_hrs: Hours of data to process per run. Defaults to 24 (daily).
        use_daily_partitions: If true, uses daily partitions. Defaults to True.
        submit_to_s3_only: Deprecated.
    """
    if isinstance(start_date, date):
        start_date = start_date.strftime("%Y-%m-%d")

    if submit_to_s3_only is False:
        warnings.warn(
            "The `submit_to_s3_only` parameter is now deprecated. "
            "Insights cost data will now always be uploaded to Dagster Insights via S3."
        )

    partition_end_offset_hrs = -abs(partition_end_offset_hrs)

    if use_daily_partitions:
        partitions_def = DailyPartitionsDefinition(
            start_date=start_date, end_offset=partition_end_offset_hrs
        )
    else:
        partitions_def = HourlyPartitionsDefinition(
            start_date=start_date, end_offset=partition_end_offset_hrs
        )

    @asset(
        name=name or "gcs_cost_history",
        group_name=group_name,
        partitions_def=partitions_def,
        required_resource_keys={gcp_resource_key},
        io_manager_def=fs_io_manager,
    )
    def poll_gcs_cost_history(
        context: AssetExecutionContext,
    ) -> None:
        try:
            from google.cloud import billing_v1
        except ImportError:
            raise ImportError(
                "google-cloud-billing is required. Install it with: pip install google-cloud-billing"
            )

        gcp_resource = getattr(context.resources, gcp_resource_key)
        try:
            billing_client = gcp_resource.get_billing_client()
        except AttributeError:
            billing_client = billing_v1.CloudBillingClient()

        start_time = context.partition_time_window.start
        end_time = context.partition_time_window.end

        now = datetime.now().astimezone(timezone.utc)
        earliest_call_time = end_time + timedelta(hours=billing_latency_hrs)
        if now < earliest_call_time:
            err = (
                f"Attempted to gather GCS cost information before billing data may be "
                f"available. For period starting {start_time.isoformat()} you can call it "
                f"starting at {earliest_call_time.isoformat()} (it is currently {now.isoformat()})"
            )
            if allow_partial_partitions:
                context.log.error(err)
            else:
                raise RuntimeError(err)

        costs = get_cost_data_for_hour(
            billing_client,
            project_id,
            start_time,
            end_time,
        ) or []

        context.log.info(
            f"Fetched GCS cost information from {start_time.isoformat()} to {end_time.isoformat()}"
        )

        if dry_run:
            context.log.info(f"DRY RUN: Would submit {len(costs)} cost records")
            for opaque_id, cost, resource_id in costs[:10]:
                context.log.info(f"  {opaque_id}: ${cost:.4f} ({resource_id})")
        else:
            context.log.info(
                f"Submitting cost information for {len(costs)} GCS resources to Dagster Insights"
            )
            put_cost_information(
                context=context,
                metric_name="gcs_cost_usd",
                cost_information=costs,
                start=start_time.timestamp(),
                end=end_time.timestamp(),
            )

    insights_job = define_asset_job(
        job_name,
        AssetSelection.assets(poll_gcs_cost_history),
        partitions_def=partitions_def,
    )

    if schedule_batch_size_hrs == 24 or use_daily_partitions:
        insights_schedule = build_schedule_from_partitioned_job(
            job=insights_job,
            hour_of_day=0,
            minute_of_hour=0,
        )
    else:
        @schedule(
            job=insights_job,
            name=f"{job_name}_schedule_{schedule_batch_size_hrs}_hrs",
            cron_schedule=f"0 0/{schedule_batch_size_hrs} * * *",
        )
        def _insights_schedule(context: ScheduleEvaluationContext):
            timestamp = context.scheduled_execution_time.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(hours=partition_end_offset_hrs)
            n_hours_ago = timestamp - timedelta(hours=schedule_batch_size_hrs)
            window = TimeWindow(start=n_hours_ago, end=timestamp)

            partition_key_range = partitions_def.get_partition_key_range_for_time_window(window)

            yield _build_run_request_for_partition_key_range(
                insights_job,
                [poll_gcs_cost_history.key],
                partition_key_range.start,
                partition_key_range.end,
            )

        insights_schedule = _insights_schedule

    return GCSInsightsDefinitions(
        assets=[poll_gcs_cost_history],
        schedule=insights_schedule,  # type: ignore
    )


