"""Example: Neon PostgreSQL Cost Tracking with Dagster+ Insights

Neon (https://neon.com) is a serverless PostgreSQL platform with usage-based pricing.
This example shows how to track Neon costs in Dagster+ Insights.

Neon Pricing (as of 2025):
- Launch Plan: $0.106 per CU-hour
- Scale Plan: $0.222 per CU-hour
- Storage: $0.35 per GB-month
- Data Transfer: $0.10 per GB (after 100 GB free)

Cost Formula:
    query_cost = (execution_time_ms / 3,600,000) * compute_units * price_per_CU
"""

from dagster import (
    Definitions,
    EnvVar,
    asset,
    AssetExecutionContext,
)
from dagster_insights import (
    InsightsPostgreSQLResource,
    create_neon_insights_asset_and_schedule,
    calculate_neon_hourly_cost,
)


# Example 1: Basic Neon setup (Free tier with 0.25 CU)
@asset(required_resource_keys={"postgres"})
def analyze_user_activity(context: AssetExecutionContext):
    """Analyze user activity - costs tracked automatically."""
    postgres = context.resources.postgres

    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    DATE(created_at) as date,
                    COUNT(*) as active_users,
                    COUNT(DISTINCT user_id) as unique_users
                FROM user_events
                WHERE created_at >= NOW() - INTERVAL '30 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
            """)
            results = cur.fetchall()
            context.log.info(f"Analyzed {len(results)} days of user activity")


# Create Neon cost tracking for Free tier
free_tier_insights = create_neon_insights_asset_and_schedule(
    start_date="2025-01-01",
    compute_units=0.25,  # Neon Free tier: shared compute with autosuspend
    pricing_tier="launch",
    name="neon_free_tier_tracking",
)

# Effective cost: 0.25 CU × $0.106/CU-hour = $0.0265/hour
print(f"Free tier hourly cost: ${calculate_neon_hourly_cost(0.25, 'launch'):.4f}/hour")


# Example 2: Neon Launch plan with dedicated compute (1 CU)
launch_insights = create_neon_insights_asset_and_schedule(
    start_date="2025-01-01",
    compute_units=1.0,  # 1 CU dedicated compute
    pricing_tier="launch",
    name="neon_launch_tracking",
)

# Effective cost: 1.0 CU × $0.106/CU-hour = $0.106/hour
print(f"Launch 1 CU hourly cost: ${calculate_neon_hourly_cost(1.0, 'launch'):.4f}/hour")


# Example 3: Neon Scale plan with high performance (4 CU)
scale_insights = create_neon_insights_asset_and_schedule(
    start_date="2025-01-01",
    compute_units=4.0,  # 4 CU for high-performance workloads
    pricing_tier="scale",  # Scale plan has premium features
    name="neon_scale_tracking",
)

# Effective cost: 4.0 CU × $0.222/CU-hour = $0.888/hour
print(f"Scale 4 CU hourly cost: ${calculate_neon_hourly_cost(4.0, 'scale'):.4f}/hour")


# Example 4: Complete Neon setup with cost tracking
defs = Definitions(
    assets=[
        analyze_user_activity,
        *free_tier_insights.assets,
    ],
    schedules=[
        free_tier_insights.schedule,
    ],
    resources={
        "postgres": InsightsPostgreSQLResource(
            host=EnvVar("NEON_HOST"),  # e.g., ep-xyz-123.us-east-1.aws.neon.tech
            port=5432,
            database=EnvVar("NEON_DATABASE"),
            user=EnvVar("NEON_USER"),
            password=EnvVar("NEON_PASSWORD"),
            enable_cost_tracking=True,  # Required for Neon cost tracking!
        )
    },
)


# Example 5: Neon compute configurations and costs
"""
Common Neon Compute Configurations:

Free Tier (with autosuspend):
- 0.25 CU shared: $0.0265/hour → $19.35/month (730 hours)

Launch Plan:
- 0.25 CU: $0.0265/hour → $19.35/month
- 0.5 CU: $0.053/hour → $38.69/month
- 1 CU: $0.106/hour → $77.38/month
- 2 CU: $0.212/hour → $154.76/month
- 4 CU: $0.424/hour → $309.52/month

Scale Plan (premium features):
- 1 CU: $0.222/hour → $162.06/month
- 2 CU: $0.444/hour → $324.12/month
- 4 CU: $0.888/hour → $648.24/month
- 8 CU: $1.776/hour → $1,296.48/month

Note: With Neon's autosuspend, you only pay for active time!
If your database is active 25% of the time (182.5 hours/month):
- 1 CU Launch: $0.106/hour × 182.5 hours = $19.35/month
"""


# Example 6: Finding your Neon configuration
"""
To find your Neon compute configuration:

1. Go to your Neon Console: https://console.neon.tech
2. Select your project
3. Click on "Settings" → "Compute"
4. Look for "Compute size" or "vCPU"

Mapping vCPU to Compute Units:
- 0.25 vCPU = 0.25 CU (shared, with autosuspend)
- 1 vCPU = 1 CU
- 2 vCPU = 2 CU
- 4 vCPU = 4 CU
- etc.

Then use that value in create_neon_insights_asset_and_schedule().
"""


# Example 7: Understanding Neon costs vs traditional PostgreSQL
"""
Traditional PostgreSQL (AWS RDS, GCP Cloud SQL):
- Fixed hourly cost regardless of usage
- Pay 24/7 even when idle
- Example: db.t3.medium = $0.068/hour = $49.64/month

Neon PostgreSQL:
- Pay only for active compute time
- Autosuspend when idle (< 5 minutes of inactivity)
- Example: 1 CU @ 25% active = $0.106/hour × 182.5 hours = $19.35/month

Benefits of Neon for development/staging:
- Automatic cost savings from autosuspend
- Scale-to-zero when not in use
- Branch-based development (copy-on-write)
- Point-in-time restore without additional cost

When to use traditional RDS/Cloud SQL:
- Production workloads with consistent 24/7 traffic
- Very large databases (> 100 GB)
- Need for specific extensions not in Neon
- Multi-region requirements
"""


# Example 8: Monitoring Neon costs in Dagster+
"""
Once deployed, your Neon query costs will appear in Dagster+ Insights:

1. Deploy to Dagster+ (dg plus deploy)
2. Materialize your assets
3. Wait for the hourly cost ingestion schedule to run
4. View costs in Dagster+ UI under "Insights"

Cost Insights will show:
- Query-level costs attributed to specific assets
- Execution time breakdown
- Cost per run
- Cost trends over time

The scheduled asset runs at :05 past each hour (e.g., 1:05, 2:05, 3:05)
to allow time for queries to be tracked in the tracking table.
"""


# Example 9: Advanced: Multiple Neon databases
"""
If you have multiple Neon projects (e.g., dev, staging, prod):

dev_insights = create_neon_insights_asset_and_schedule(
    start_date="2025-01-01",
    compute_units=0.25,
    pricing_tier="launch",
    name="neon_dev_tracking",
    postgresql_resource_key="dev_postgres",
    job_name="neon_dev_insights_import",
)

staging_insights = create_neon_insights_asset_and_schedule(
    start_date="2025-01-01",
    compute_units=1.0,
    pricing_tier="launch",
    name="neon_staging_tracking",
    postgresql_resource_key="staging_postgres",
    job_name="neon_staging_insights_import",
)

prod_insights = create_neon_insights_asset_and_schedule(
    start_date="2025-01-01",
    compute_units=4.0,
    pricing_tier="scale",
    name="neon_prod_tracking",
    postgresql_resource_key="prod_postgres",
    job_name="neon_prod_insights_import",
)

defs = Definitions(
    assets=[
        *dev_insights.assets,
        *staging_insights.assets,
        *prod_insights.assets,
    ],
    schedules=[
        dev_insights.schedule,
        staging_insights.schedule,
        prod_insights.schedule,
    ],
    resources={
        "dev_postgres": InsightsPostgreSQLResource(..., enable_cost_tracking=True),
        "staging_postgres": InsightsPostgreSQLResource(..., enable_cost_tracking=True),
        "prod_postgres": InsightsPostgreSQLResource(..., enable_cost_tracking=True),
    },
)
"""
