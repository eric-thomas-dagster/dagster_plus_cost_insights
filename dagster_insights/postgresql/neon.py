"""Neon-specific cost tracking for Dagster+ Insights.

Neon (https://neon.com) is a serverless PostgreSQL platform with usage-based pricing.
Unlike traditional PostgreSQL instances, Neon charges based on Compute Units (CU) consumed.

Pricing Model (as of 2025):
- Launch Plan: $0.106 per CU-hour
- Scale Plan: $0.222 per CU-hour
- Storage: $0.35 per GB-month (tracked separately)
- Data Transfer: $0.10 per GB after 100 GB (tracked separately)

This module provides helper functions to create cost tracking configured for Neon's pricing.
"""

from datetime import datetime
from typing import Optional, Union, Literal

from dagster_insights.postgresql.definitions import (
    PostgreSQLInsightsDefinitions,
    create_postgresql_insights_asset_and_schedule,
)


# Neon pricing (as of 2025-01-01)
NEON_LAUNCH_PRICE_PER_CU_HOUR = 0.106
NEON_SCALE_PRICE_PER_CU_HOUR = 0.222

# Common Neon compute configurations
NEON_COMPUTE_CONFIGS = {
    "0.25": 0.25,  # Shared compute (autosuspend enabled)
    "0.5": 0.5,    # 0.5 CU
    "1": 1.0,      # 1 CU
    "2": 2.0,      # 2 CU
    "3": 3.0,      # 3 CU
    "4": 4.0,      # 4 CU
    "5": 5.0,      # 5 CU
    "6": 6.0,      # 6 CU
    "7": 7.0,      # 7 CU
    "8": 8.0,      # 8 CU
}


def create_neon_insights_asset_and_schedule(
    start_date: Union[datetime, str],
    compute_units: float,
    pricing_tier: Literal["launch", "scale"] = "launch",
    name: Optional[str] = None,
    group_name: Optional[str] = None,
    job_name: str = "neon_insights_import",
    dry_run: bool = False,
    postgresql_resource_key: str = "postgres",
    **kwargs,
) -> PostgreSQLInsightsDefinitions:
    """Create a cost tracking asset and schedule configured for Neon pricing.

    Neon charges based on Compute Units (CU) consumed per hour. This function calculates
    the effective hourly cost based on your Neon configuration and creates a scheduled
    asset that tracks query costs.

    Cost Formula:
        cost = (execution_time_ms / 3,600,000) * compute_units * price_per_CU

    Args:
        start_date: The date to start the partitioned schedule
        compute_units: The number of Compute Units (CU) for your Neon instance.
            Common values: 0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8
        pricing_tier: Neon pricing tier - "launch" ($0.106/CU-hr) or "scale" ($0.222/CU-hr)
        name: Asset name (defaults to "neon_query_tracking")
        group_name: Asset group name
        job_name: Job name (defaults to "neon_insights_import")
        dry_run: If True, prints costs instead of submitting to Dagster+
        postgresql_resource_key: Resource key for PostgreSQL resource (defaults to "postgres")
        **kwargs: Additional arguments passed to create_postgresql_insights_asset_and_schedule

    Returns:
        PostgreSQLInsightsDefinitions with assets and schedule

    Examples:
        .. code-block:: python

            from dagster import Definitions, EnvVar
            from dagster_insights import InsightsPostgreSQLResource
            from dagster_insights.postgresql.neon import create_neon_insights_asset_and_schedule

            # Neon Launch plan with 1 CU
            neon_insights = create_neon_insights_asset_and_schedule(
                start_date="2025-01-01",
                compute_units=1.0,
                pricing_tier="launch",  # $0.106/CU-hour
            )

            defs = Definitions(
                assets=[*your_assets, *neon_insights.assets],
                schedules=[neon_insights.schedule],
                resources={
                    "postgres": InsightsPostgreSQLResource(
                        host=EnvVar("NEON_HOST"),
                        database=EnvVar("NEON_DATABASE"),
                        user=EnvVar("NEON_USER"),
                        password=EnvVar("NEON_PASSWORD"),
                        enable_cost_tracking=True,
                    )
                },
            )

        .. code-block:: python

            # Neon Scale plan with 4 CU (higher performance, higher cost)
            neon_insights = create_neon_insights_asset_and_schedule(
                start_date="2025-01-01",
                compute_units=4.0,
                pricing_tier="scale",  # $0.222/CU-hour
            )
            # Effective hourly cost: 4.0 * $0.222 = $0.888/hour

        .. code-block:: python

            # Neon shared compute with autosuspend (0.25 CU)
            neon_insights = create_neon_insights_asset_and_schedule(
                start_date="2025-01-01",
                compute_units=0.25,
                pricing_tier="launch",
            )
            # Effective hourly cost: 0.25 * $0.106 = $0.0265/hour

    Note:
        This only tracks compute costs. Storage costs ($0.35/GB-month) and data transfer
        costs ($0.10/GB) are not tracked by this function and should be monitored via
        Neon's dashboard or API.
    """
    # Calculate effective hourly cost
    price_per_cu = (
        NEON_LAUNCH_PRICE_PER_CU_HOUR if pricing_tier == "launch" else NEON_SCALE_PRICE_PER_CU_HOUR
    )
    hourly_cost = compute_units * price_per_cu

    # Set defaults specific to Neon
    if name is None:
        name = "neon_query_tracking"

    # Create the insights definitions with calculated hourly cost
    return create_postgresql_insights_asset_and_schedule(
        start_date=start_date,
        hourly_cost=hourly_cost,
        name=name,
        group_name=group_name,
        job_name=job_name,
        dry_run=dry_run,
        postgresql_resource_key=postgresql_resource_key,
        **kwargs,
    )


def calculate_neon_hourly_cost(
    compute_units: float,
    pricing_tier: Literal["launch", "scale"] = "launch",
) -> float:
    """Calculate the effective hourly cost for a Neon instance.

    Args:
        compute_units: Number of Compute Units (CU)
        pricing_tier: "launch" or "scale"

    Returns:
        Hourly cost in USD

    Examples:
        >>> calculate_neon_hourly_cost(1.0, "launch")
        0.106
        >>> calculate_neon_hourly_cost(4.0, "scale")
        0.888
        >>> calculate_neon_hourly_cost(0.25, "launch")
        0.0265
    """
    price_per_cu = (
        NEON_LAUNCH_PRICE_PER_CU_HOUR if pricing_tier == "launch" else NEON_SCALE_PRICE_PER_CU_HOUR
    )
    return compute_units * price_per_cu


def print_neon_cost_breakdown(
    compute_units: float,
    pricing_tier: Literal["launch", "scale"] = "launch",
    monthly_query_hours: float = 100,
) -> None:
    """Print a cost breakdown for a Neon configuration.

    Args:
        compute_units: Number of Compute Units
        pricing_tier: "launch" or "scale"
        monthly_query_hours: Estimated hours of query execution per month

    Examples:
        >>> print_neon_cost_breakdown(1.0, "launch", monthly_query_hours=100)
        Neon Cost Breakdown
        ===================
        Configuration: 1.0 CU (Launch plan)
        Price per CU-hour: $0.106

        Hourly Cost: $0.106/hour
        Daily Cost (24h): $2.54/day
        Monthly Cost (730h): $77.38/month

        Estimated Query Cost:
        - 100 hours of queries/month: $10.60/month
        """
    price_per_cu = (
        NEON_LAUNCH_PRICE_PER_CU_HOUR if pricing_tier == "launch" else NEON_SCALE_PRICE_PER_CU_HOUR
    )
    hourly_cost = compute_units * price_per_cu

    print("Neon Cost Breakdown")
    print("===================")
    print(f"Configuration: {compute_units} CU ({pricing_tier.title()} plan)")
    print(f"Price per CU-hour: ${price_per_cu:.3f}")
    print()
    print(f"Hourly Cost: ${hourly_cost:.3f}/hour")
    print(f"Daily Cost (24h): ${hourly_cost * 24:.2f}/day")
    print(f"Monthly Cost (730h): ${hourly_cost * 730:.2f}/month")
    print()
    print("Estimated Query Cost:")
    print(f"- {monthly_query_hours} hours of queries/month: ${hourly_cost * monthly_query_hours:.2f}/month")
    print()
    print("Note: This only includes compute costs.")
    print(f"Add storage costs: $0.35/GB-month")
    print(f"Add data transfer: $0.10/GB (after 100 GB free)")
