# Databricks Cost Insights Coverage

## What We're Currently Capturing ✅

### 1. SQL Queries (Fully Supported)
- **Method**: Via `get_sql_client()` → `WrappedDatabricksConnection`
- **Coverage**: All SQL queries executed through SQL warehouses
- **Tracking**: Real-time via cursor wrapper
- **Costs Tracked**: Execution time, estimated DBU

### 2. Job Runs (Supported)
- **Method**: Via `get_workspace_client()` → `WrappedDatabricksWorkspaceClient`
- **Coverage**: 
  - ✅ Notebook jobs
  - ✅ Python jobs
  - ✅ JAR jobs
  - ✅ Spark jobs
  - ✅ Asset Bundles (when jobs are submitted through wrapped client)
- **Tracking**: Automatic tagging when jobs are submitted
- **Costs Tracked**: Execution duration, estimated DBU from job run details

### 3. Asset Bundles (Partially Supported)
- **Coverage**: Jobs created/updated by asset bundles are tracked when:
  - Jobs are submitted via `get_workspace_client()`
  - Jobs are tagged with opaque IDs
- **Limitation**: If bundles deploy jobs separately (outside Dagster), those jobs won't be automatically tracked
- **Solution**: Query `system.billing.usage` or `system.operational_data.jobs_cost` for comprehensive tracking

### 4. LakeFlow Pipelines (Partially Supported)
- **Coverage**: Pipeline runs are tracked when:
  - Submitted via `get_workspace_client()`
  - Pipeline jobs are tagged with opaque IDs
- **Limitation**: Direct LakeFlow API calls may not be automatically tracked
- **Solution**: Use wrapped WorkspaceClient or query system tables

### 5. Delta Live Tables (Partially Supported)
- **Coverage**: DLT pipeline runs are tracked when:
  - Managed via `get_workspace_client()`
  - Pipeline runs are tagged with opaque IDs
- **Limitation**: DLT pipelines managed outside Dagster won't be tracked
- **Solution**: Query system tables for comprehensive tracking

## What We're NOT Currently Capturing ❌

### 1. Interactive Notebook Sessions
- **Reason**: Interactive sessions don't go through job submission API
- **Solution**: Query `system.billing.usage` table (see `system_tables.py`)

### 2. Jobs Submitted Outside Dagster
- **Reason**: If jobs are submitted directly via Databricks UI or CLI without using wrapped client
- **Solution**: Query system tables to find jobs tagged with Dagster metadata

### 3. Direct API Calls
- **Reason**: If you bypass the wrapped clients and call Databricks APIs directly
- **Solution**: Always use `get_sql_client()` and `get_workspace_client()`

## How It Works

### Real-Time Tracking

1. **SQL Queries**:
   ```python
   with databricks.get_sql_client() as client:
       with client.cursor() as cur:
           cur.execute("SELECT * FROM table")
   # Costs tracked automatically
   ```

2. **Job Runs**:
   ```python
   with databricks.get_workspace_client() as client:
       run = client.jobs.submit(job_id=123)
   # Job automatically tagged, costs tracked after completion
   ```

### Historical Tracking (System Tables)

For comprehensive tracking, you can query Databricks system tables:

```python
from dagster_insights.databricks.system_tables import (
    get_cost_data_from_system_tables,
    get_job_costs_from_system_tables,
)

# Query all costs (including interactive sessions, etc.)
with databricks.get_sql_client() as conn:
    costs = get_cost_data_from_system_tables(conn, start_time, end_time)
```

## Tagging Strategy

All tracked executions are automatically tagged with:
- `dagster_snowflake_opaque_id:<opaque_id>` - For cost matching
- `dagster_job_name` - Dagster job name  
- `dagster_run_id` - Dagster run ID

These tags appear in:
- Job run metadata
- `system.billing.usage` table
- `system.operational_data.jobs_cost` table
- Cost analysis queries

## Recommendations

### For Maximum Coverage:

1. **Always Use Wrapped Clients**: Use `get_sql_client()` and `get_workspace_client()` for all Databricks operations

2. **Tag Resources**: Apply custom tags to Databricks resources (clusters, jobs, etc.) for better attribution

3. **Query System Tables**: For comprehensive historical analysis, periodically query:
   - `system.billing.usage` - All billable usage
   - `system.operational_data.jobs_cost` - Detailed job costs
   - `system.query.history` - SQL query history

4. **For Asset Bundles**: 
   - Ensure jobs created by bundles are submitted through wrapped client when possible
   - Or query system tables to find bundle-deployed jobs by tags

5. **For LakeFlow/DLT**: 
   - Use WorkspaceClient to manage pipelines when possible
   - Or query system tables for pipeline costs

## Complexity Assessment

**Is it too complex?** Not really! The current implementation covers:
- ✅ Most common use cases (SQL queries, job runs)
- ✅ Asset bundles (when jobs are submitted through Dagster)
- ⚠️ Advanced features (LakeFlow, DLT) require using wrapped clients or system table queries

**For comprehensive tracking**, you would need to:
1. Use wrapped clients for all operations (recommended)
2. Periodically query system tables for anything missed
3. Apply consistent tagging strategy

The system is designed to be **extensible** - you can add more tracking methods as needed without changing the core architecture.


