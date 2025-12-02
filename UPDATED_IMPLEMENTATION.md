# Updated Implementation Summary

## Changes Made

Based on your feedback, I've removed scheduled asset definitions for Databricks and Redshift since they can track costs in real-time (like BigQuery). Scheduled assets are now only available for object storage sources (S3, GCS, Azure Data Lake) which require billing APIs.

## Current Structure

### Compute Sources (Real-Time Tracking Only)

**Databricks & Redshift** now only have:
- ✅ Resource wrappers for real-time cost tracking
- ✅ dbt integration
- ✅ Utilities for query tagging and metadata
- ❌ No scheduled assets (costs tracked in real-time)

### Object Storage Sources (Scheduled Assets)

**S3, GCS, Azure Data Lake** have:
- ✅ Scheduled asset definitions (billing APIs have latency)
- ✅ Cost extraction from billing APIs
- ✅ Daily partitions (billing APIs work better with daily granularity)

## File Structure (Updated)

```
dagster_insights/
├── __init__.py                          # Main exports (updated)
├── insights_utils.py                    # Shared utilities
│
├── databricks/
│   ├── __init__.py
│   ├── databricks_utils.py
│   ├── insights_databricks_resource.py  # Real-time tracking
│   └── dbt_wrapper.py
│
├── redshift/
│   ├── __init__.py
│   ├── redshift_utils.py
│   ├── insights_redshift_resource.py    # Real-time tracking
│   └── dbt_wrapper.py
│
└── storage/
    ├── storage_utils.py
    ├── s3/
    │   ├── s3_insights.py               # Cost extraction
    │   └── definitions.py               # Scheduled assets
    ├── gcs/
    │   ├── gcs_insights.py              # Cost extraction
    │   └── definitions.py               # Scheduled assets
    └── azure/
        ├── azure_insights.py            # Cost extraction
        └── definitions.py               # Scheduled assets
```

## Cost Tracking Approach

### Real-Time Sources (Databricks, Redshift, BigQuery)

These sources track costs **in real-time** via resource wrappers:
- Costs are collected during query execution
- Emitted as `AssetObservation` events immediately
- No scheduled assets needed
- No extrapolation - uses actual execution metrics

### Historical/Billing Sources (S3, GCS, Azure Data Lake)

These sources require **scheduled assets** because:
- Billing APIs have 24-hour latency
- Need to query historical cost data
- Daily partitions work better than hourly
- Uses actual costs from billing APIs (no extrapolation)

## Usage Examples

### Databricks (Real-Time)

```python
from dagster_insights import InsightsDatabricksResource

@op
def run_query(databricks: InsightsDatabricksResource):
    with databricks.get_sql_client() as client:
        client.execute("SELECT * FROM my_table")
    # Costs are automatically tracked and emitted in real-time
```

### Redshift (Real-Time)

```python
from dagster_insights import InsightsRedshiftResource

@op
def run_query(redshift: InsightsRedshiftResource):
    with redshift.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM my_table")
    # Costs are automatically tracked and emitted in real-time
```

### S3 (Scheduled)

```python
from dagster_insights.storage.s3 import create_s3_insights_asset_and_schedule

# Create scheduled asset to import S3 cost data from billing API
s3_insights = create_s3_insights_asset_and_schedule(
    start_date="2024-01-01",
    aws_resource_key="aws",
)

defs = Definitions(
    assets=[*s3_insights.assets],
    schedules=[s3_insights.schedule],
)
```

## Cost Calculation Notes

- **No Extrapolation**: All cost calculations use actual metrics from the time window
- **Databricks**: Tracks actual DBU consumption during execution
- **Redshift**: Tracks actual execution time and bytes scanned
- **Object Stores**: Uses actual costs from billing APIs for the time period

## Removed Files

- `databricks/definitions.py` - Removed (real-time tracking only)
- `databricks/dagster_databricks_insights.py` - Removed (not needed for real-time)
- `redshift/definitions.py` - Removed (real-time tracking only)
- `redshift/dagster_redshift_insights.py` - Removed (not needed for real-time)

These files were for historical cost extraction, which isn't needed since costs are tracked in real-time.


