"""Example: PostgreSQL Cost Tracking with Dagster+ Insights

This example demonstrates how to:
1. Enable cost tracking on the PostgreSQL resource
2. Create a scheduled asset that ingests cost data
3. Submit costs to Dagster+ Insights API

The cost calculation is based on:
    cost = (execution_time_ms / 3,600,000) * hourly_cost

Where hourly_cost is the cost per hour of your PostgreSQL instance.
"""

from dagster import (
    Definitions,
    EnvVar,
    asset,
    AssetExecutionContext,
)
from dagster_insights import (
    InsightsPostgreSQLResource,
    create_postgresql_insights_asset_and_schedule,
)


# Example 1: Basic asset using PostgreSQL with cost tracking
@asset(required_resource_keys={"postgres"})
def create_sales_table(context: AssetExecutionContext):
    """Create a sales table and populate it with sample data."""
    postgres = context.resources.postgres

    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # Create table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sales (
                    id SERIAL PRIMARY KEY,
                    product VARCHAR(100),
                    amount DECIMAL(10, 2),
                    sale_date DATE
                )
            """)

            # Insert sample data
            cur.execute("""
                INSERT INTO sales (product, amount, sale_date)
                SELECT
                    'Product ' || (random() * 100)::int,
                    (random() * 1000)::decimal(10,2),
                    CURRENT_DATE - (random() * 365)::int
                FROM generate_series(1, 1000)
            """)

            context.log.info(f"Created sales table with 1000 rows")


@asset(required_resource_keys={"postgres"})
def analyze_sales(context: AssetExecutionContext):
    """Run analytics queries on sales data."""
    postgres = context.resources.postgres

    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # Run an aggregation query
            cur.execute("""
                SELECT
                    product,
                    COUNT(*) as num_sales,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount
                FROM sales
                GROUP BY product
                ORDER BY total_amount DESC
                LIMIT 10
            """)

            results = cur.fetchall()
            context.log.info(f"Top 10 products by revenue: {len(results)} products")

            # Run a time series query
            cur.execute("""
                SELECT
                    DATE_TRUNC('month', sale_date) as month,
                    COUNT(*) as num_sales,
                    SUM(amount) as revenue
                FROM sales
                GROUP BY month
                ORDER BY month DESC
            """)

            time_series = cur.fetchall()
            context.log.info(f"Monthly revenue trends: {len(time_series)} months")


# Example 2: Create the cost tracking asset and schedule
# AWS RDS db.t3.medium costs approximately $0.068/hour
# You can find your instance cost at: https://aws.amazon.com/rds/pricing/
insights_defs = create_postgresql_insights_asset_and_schedule(
    start_date="2025-01-01",  # When you started tracking
    hourly_cost=0.068,  # Cost per hour of your PostgreSQL instance
    dry_run=False,  # Set to True to test without submitting to Dagster+
)


# Example 3: Define all assets, schedules, and resources
defs = Definitions(
    assets=[
        create_sales_table,
        analyze_sales,
        *insights_defs.assets,  # Include the cost tracking asset
    ],
    schedules=[
        insights_defs.schedule,  # Include the cost ingestion schedule
    ],
    resources={
        "postgres": InsightsPostgreSQLResource(
            host=EnvVar("POSTGRES_HOST"),
            port=int(EnvVar.int("POSTGRES_PORT")),
            database=EnvVar("POSTGRES_DB"),
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            enable_cost_tracking=True,  # IMPORTANT: Enable cost tracking!
        )
    },
)


# Example 4: Different PostgreSQL instance types and their hourly costs
"""
AWS RDS Pricing Examples (as of 2025):
- db.t3.micro: $0.017/hour
- db.t3.small: $0.034/hour
- db.t3.medium: $0.068/hour
- db.t3.large: $0.136/hour
- db.m5.large: $0.192/hour
- db.m5.xlarge: $0.384/hour

GCP Cloud SQL Pricing Examples:
- db-f1-micro: $0.0150/hour
- db-g1-small: $0.0500/hour
- db-n1-standard-1: $0.0960/hour
- db-n1-standard-2: $0.1920/hour

Azure Database for PostgreSQL Pricing Examples:
- B1ms (1 vCore, 2 GiB): $0.0208/hour
- B2s (2 vCore, 4 GiB): $0.0416/hour
- GP_Gen5_2 (2 vCore, 10 GiB): $0.228/hour
- GP_Gen5_4 (4 vCore, 20 GiB): $0.456/hour

For self-hosted PostgreSQL, calculate your hourly cost by dividing
your monthly infrastructure cost by 730 hours.
"""


# Example 5: Advanced usage with multiple databases
"""
If you have multiple PostgreSQL databases, you can create separate
resources and cost tracking assets for each:

# Production database
prod_postgres = InsightsPostgreSQLResource(
    host=EnvVar("PROD_POSTGRES_HOST"),
    database="prod_db",
    enable_cost_tracking=True,
)

prod_insights = create_postgresql_insights_asset_and_schedule(
    start_date="2025-01-01",
    hourly_cost=0.384,  # db.m5.xlarge production instance
    name="prod_postgresql_tracking",
    postgresql_resource_key="prod_postgres",
    job_name="prod_postgresql_insights_import",
)

# Analytics database
analytics_postgres = InsightsPostgreSQLResource(
    host=EnvVar("ANALYTICS_POSTGRES_HOST"),
    database="analytics_db",
    enable_cost_tracking=True,
)

analytics_insights = create_postgresql_insights_asset_and_schedule(
    start_date="2025-01-01",
    hourly_cost=0.192,  # db.m5.large analytics instance
    name="analytics_postgresql_tracking",
    postgresql_resource_key="analytics_postgres",
    job_name="analytics_postgresql_insights_import",
)

defs = Definitions(
    assets=[
        *prod_insights.assets,
        *analytics_insights.assets,
    ],
    schedules=[
        prod_insights.schedule,
        analytics_insights.schedule,
    ],
    resources={
        "prod_postgres": prod_postgres,
        "analytics_postgres": analytics_postgres,
    },
)
"""
