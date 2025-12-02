# Dagster Insights Extensions

This package provides cost insights integrations for additional data sources beyond the built-in Snowflake and BigQuery support in Dagster Cloud.

## Supported Sources

- **Databricks** - Track DBU consumption and compute costs
- **Redshift** - Track query execution time and bytes scanned

## Installation

Install the required dependencies for each source you want to use:

### Databricks
```bash
pip install databricks-sdk databricks-sql-connector
```

### Redshift
```bash
pip install psycopg2-binary
```

## Usage

### Databricks

```python
from dagster import job, op, EnvVar
from dagster_insights import InsightsDatabricksResource

@op
def run_databricks_query(databricks: InsightsDatabricksResource):
    with databricks.get_sql_client() as client:
        client.execute("SELECT * FROM my_table")

@job
def my_databricks_job():
    run_databricks_query()

my_databricks_job.execute_in_process(
    resources={
        "databricks": InsightsDatabricksResource(
            workspace_url=EnvVar("DATABRICKS_WORKSPACE_URL"),
            access_token=EnvVar("DATABRICKS_ACCESS_TOKEN")
        )
    }
)
```

### Redshift

```python
from dagster import job, op, EnvVar
from dagster_insights import InsightsRedshiftResource

@op
def run_redshift_query(redshift: InsightsRedshiftResource):
    with redshift.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM my_table")

@job
def my_redshift_job():
    run_redshift_query()

my_redshift_job.execute_in_process(
    resources={
        "redshift": InsightsRedshiftResource(
            host=EnvVar("REDSHIFT_HOST"),
            port=5439,
            database=EnvVar("REDSHIFT_DATABASE"),
            user=EnvVar("REDSHIFT_USER"),
            password=EnvVar("REDSHIFT_PASSWORD")
        )
    }
)
```

### dbt Integration

Both sources support dbt integration:

```python
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_insights import dbt_with_databricks_insights, dbt_with_redshift_insights

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    dbt_cli_invocation = dbt.cli(["build"], context=context)
    # For Databricks:
    yield from dbt_with_databricks_insights(context, dbt_cli_invocation)
    # Or for Redshift:
    # yield from dbt_with_redshift_insights(context, dbt_cli_invocation)
```

## Structure

```
dagster_insights/
├── __init__.py                 # Main exports
├── insights_utils.py           # Shared utilities
├── databricks/
│   ├── __init__.py
│   ├── databricks_utils.py     # Query tagging and metadata utilities
│   ├── insights_databricks_resource.py  # Resource wrapper
│   └── dbt_wrapper.py          # dbt integration
└── redshift/
    ├── __init__.py
    ├── redshift_utils.py       # Query tagging and metadata utilities
    ├── insights_redshift_resource.py    # Resource wrapper
    └── dbt_wrapper.py          # dbt integration
```

## How It Works

1. **Resource Wrappers**: The `Insights*Resource` classes wrap the underlying data source clients and automatically track query execution.

2. **Query Tagging**: Queries are tagged with opaque IDs that can be used to match queries to Dagster assets/jobs.

3. **Cost Metadata**: Cost information (DBU, execution time, bytes scanned, etc.) is collected and emitted as `AssetObservation` events.

4. **dbt Integration**: The dbt wrappers associate dbt model materializations with their query costs.

## Next Steps

To use these with Dagster Cloud Insights, you'll need to:

1. Ensure your Dagster Cloud instance can access the cost data
2. Use the `put_cost_information()` function from `dagster_cloud.dagster_insights.metrics_utils` to upload cost data
3. Create scheduled assets to periodically fetch and upload cost data (similar to the Snowflake pattern)

See the reference implementations in the `dagster-cloud` repository for examples of creating scheduled cost insights assets.


