"""Scheduled asset and definitions for PostgreSQL cost insights ingestion."""

import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional, Sequence, Union

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AssetSelection,
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

from dagster_cloud.dagster_insights.metrics_utils import put_cost_information

if TYPE_CHECKING:
    from dagster_insights.postgresql.insights_postgresql_resource import (
        InsightsPostgreSQLResource,
    )

# PostgreSQL queries are typically available immediately, but add small buffer
POSTGRESQL_QUERY_LATENCY_SLA_MINS = 5


@dataclass
class PostgreSQLInsightsDefinitions:
    """Container for PostgreSQL insights asset and schedule definitions."""

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


def get_cost_data_for_hour(
    postgres: "InsightsPostgreSQLResource",
    start_hour: datetime,
    end_hour: datetime,
    hourly_cost: float,
) -> list[tuple[str, float, str]]:
    """Query the PostgreSQL insights tracking table for queries executed during the time period.

    Returns a list of tuples: (opaque_id, cost, query_id)

    Cost is calculated as: (execution_time_ms / 3,600,000) * hourly_cost
    """
    sql = """
        SELECT
            opaque_id,
            execution_time_ms,
            query_id,
            start_time
        FROM dagster_insights_query_tracking
        WHERE start_time >= %s
          AND start_time < %s
        ORDER BY start_time
    """

    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (start_hour, end_hour))
            results = cursor.fetchall()

    costs: list[tuple[str, float, str]] = []

    if not results:
        return []

    for opaque_id, execution_time_ms, query_id, start_time in results:
        # Calculate cost: (execution_time_ms / 3,600,000 ms per hour) * cost per hour
        execution_hours = execution_time_ms / 3_600_000
        cost = execution_hours * hourly_cost
        costs.append((opaque_id, float(cost), query_id))

    return costs


def cleanup_old_tracking_data(
    postgres: "InsightsPostgreSQLResource",
    older_than_hours: int = 168,  # 7 days
) -> int:
    """Clean up old tracking data to prevent table bloat.

    Args:
        postgres: The PostgreSQL resource
        older_than_hours: Delete records older than this many hours (default: 168 = 7 days)

    Returns:
        Number of rows deleted
    """
    sql = """
        DELETE FROM dagster_insights_query_tracking
        WHERE start_time < NOW() - INTERVAL '%s hours'
    """

    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (older_than_hours,))
            deleted_count = cursor.rowcount
            conn.commit()

    return deleted_count


def create_postgresql_insights_asset_and_schedule(
    start_date: Union[datetime, str],
    hourly_cost: float,
    name: Optional[str] = None,
    group_name: Optional[str] = None,
    job_name: str = "postgresql_insights_import",
    dry_run: bool = False,
    allow_partial_partitions: bool = True,
    postgresql_resource_key: str = "postgres",
    postgresql_query_latency: int = POSTGRESQL_QUERY_LATENCY_SLA_MINS,
    partition_end_offset_hrs: int = 0,
    schedule_batch_size_hrs: int = 1,
    cleanup_after_days: int = 7,
) -> PostgreSQLInsightsDefinitions:
    """Generates a pre-defined Dagster asset and schedule for importing PostgreSQL cost data.

    The schedule will run hourly and query the dagster_insights_query_tracking table for all
    queries that ran in the previous hour. It will calculate costs based on execution time
    and your configured hourly instance cost, then submit the data to Dagster Insights.

    Args:
        start_date (Union[datetime, str]): The date to start the partitioned schedule. This should
            be the date you began tracking cost data.
        hourly_cost (float): The cost per hour of your PostgreSQL instance (e.g., 0.068 for
            AWS RDS db.t3.medium). This is used to calculate query costs based on execution time.
        name (Optional[str]): The name of the asset. Defaults to "postgresql_query_tracking".
        group_name (Optional[str]): The name of the asset group. Defaults to the default group.
        job_name (str): The name of the job for the schedule. Defaults to
            "postgresql_insights_import".
        dry_run (bool): If True, the schedule will print cost data instead of submitting to
            Dagster Insights. Defaults to False.
        postgresql_resource_key (str): The name of the PostgreSQL resource key. Defaults to
            "postgres".
        partition_end_offset_hrs (int): The number of additional hours to wait before processing
            data. Useful for ensuring all queries are captured. Defaults to 0 since PostgreSQL
            queries are available immediately.
        schedule_batch_size_hrs (int): The number of hours of data to process in each schedule
            run. Defaults to 1.
        cleanup_after_days (int): Number of days to retain tracking data before cleanup.
            Defaults to 7 days.

    Returns:
        PostgreSQLInsightsDefinitions: Container with the asset and schedule definitions.

    Example:
        .. code-block:: python

            from dagster import Definitions
            from dagster_insights import (
                InsightsPostgreSQLResource,
                create_postgresql_insights_asset_and_schedule,
            )

            # AWS RDS db.t3.medium costs ~$0.068/hour
            insights_defs = create_postgresql_insights_asset_and_schedule(
                start_date="2025-01-01",
                hourly_cost=0.068,
            )

            defs = Definitions(
                assets=insights_defs.assets,
                schedules=[insights_defs.schedule],
                resources={
                    "postgres": InsightsPostgreSQLResource(
                        host="my-rds-instance.amazonaws.com",
                        port=5432,
                        database="mydb",
                        user="user",
                        password="password",
                    )
                },
            )
    """
    # For backcompat, handle date objects
    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d-%H:%M")

    partition_end_offset_hrs = -abs(partition_end_offset_hrs)

    partitions_def = HourlyPartitionsDefinition(
        start_date=start_date, end_offset=partition_end_offset_hrs
    )

    @asset(
        name=name or "postgresql_query_tracking",
        group_name=group_name,
        partitions_def=partitions_def,
        required_resource_keys={postgresql_resource_key},
        io_manager_def=fs_io_manager,
    )
    def poll_postgresql_query_tracking_hour(
        context: AssetExecutionContext,
    ) -> None:
        """Asset that polls PostgreSQL query tracking table and submits costs to Insights."""
        postgres: "InsightsPostgreSQLResource" = getattr(context.resources, postgresql_resource_key)

        start_hour = context.partition_time_window.start
        end_hour = context.partition_time_window.end

        now = datetime.now().astimezone(timezone.utc)
        earliest_call_time = end_hour + timedelta(minutes=postgresql_query_latency)
        if now < earliest_call_time:
            err = (
                "Attempted to gather PostgreSQL usage information before queries may be fully "
                f"tracked. For hour starting {start_hour.isoformat()} you can call it "
                f"starting at {earliest_call_time.isoformat()} (it is currently "
                f"{now.isoformat()})"
            )
            if allow_partial_partitions:
                context.log.warning(err)
            else:
                raise RuntimeError(err)

        costs = get_cost_data_for_hour(postgres, start_hour, end_hour, hourly_cost) or []

        query_fetch_end_time = datetime.now().astimezone(timezone.utc)
        context.log.info(
            f"Fetched query tracking information from {start_hour.isoformat()} to "
            f"{end_hour.isoformat()} in {(query_fetch_end_time - now).total_seconds()} seconds. "
            f"Found {len(costs)} queries."
        )

        if dry_run:
            context.log.info(f"DRY RUN: Would submit {len(costs)} cost records to Insights")
            for opaque_id, cost, query_id in costs[:10]:  # Show first 10
                context.log.info(f"  {query_id}: ${cost:.6f} (opaque_id: {opaque_id[:8]}...)")
        else:
            if costs:
                context.log.info(
                    f"Submitting cost information for {len(costs)} queries to Dagster Insights"
                )
                put_cost_information(
                    context=context,
                    metric_name="snowflake_credits",  # TEMP: Testing if control plane recognizes this
                    cost_information=costs,
                    start=start_hour.timestamp(),
                    end=end_hour.timestamp(),
                )

        # Cleanup old tracking data
        deleted_count = cleanup_old_tracking_data(postgres, older_than_hours=cleanup_after_days * 24)
        if deleted_count > 0:
            context.log.info(
                f"Cleaned up {deleted_count} old tracking records "
                f"(older than {cleanup_after_days} days)"
            )

    insights_job = define_asset_job(
        job_name,
        AssetSelection.assets(poll_postgresql_query_tracking_hour),
        partitions_def=partitions_def,
    )

    if schedule_batch_size_hrs == 1:
        insights_schedule = build_schedule_from_partitioned_job(
            job=insights_job,
            minute_of_hour=5,  # Run 5 minutes past the hour
        )
    else:

        @schedule(
            job=insights_job,
            name=f"{job_name}_schedule_{schedule_batch_size_hrs}_hrs",
            cron_schedule=f"5 0/{schedule_batch_size_hrs} * * *",
        )
        def _insights_schedule(context: ScheduleEvaluationContext):
            timestamp = context.scheduled_execution_time.replace(
                minute=0, second=0, microsecond=0
            ) + timedelta(hours=partition_end_offset_hrs)
            n_hours_ago = timestamp - timedelta(hours=schedule_batch_size_hrs)
            window = TimeWindow(start=n_hours_ago, end=timestamp)

            partition_key_range = partitions_def.get_partition_key_range_for_time_window(window)

            yield _build_run_request_for_partition_key_range(
                insights_job,
                [poll_postgresql_query_tracking_hour.key],
                partition_key_range.start,
                partition_key_range.end,
            )

        insights_schedule = _insights_schedule

    return PostgreSQLInsightsDefinitions(
        assets=[poll_postgresql_query_tracking_hour],
        schedule=insights_schedule,  # type: ignore
    )
