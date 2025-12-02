# Databricks Execution Methods and Cost Tracking

## Overview

Databricks supports multiple execution methods, each with different cost implications. This document explains how cost insights are tracked for each method.

## Supported Execution Methods

### 1. SQL Queries (✅ Fully Supported)

**Via `get_sql_client()`:**
- Direct SQL queries through SQL warehouses
- Tracked in real-time via `WrappedDatabricksConnection`
- Costs tracked: execution time, estimated DBU

**Usage:**
```python
with databricks.get_sql_client() as client:
    with client.cursor() as cur:
        cur.execute("SELECT * FROM my_table")
```

### 2. Job Runs (✅ Supported)

**Via `get_workspace_client()`:**
- Notebook jobs
- Python jobs
- JAR jobs
- Spark jobs
- **Asset Bundles** (when submitted as jobs)
- Tracked via `WrappedDatabricksWorkspaceClient`
- Job runs are automatically tagged with opaque IDs
- Costs tracked: execution duration, estimated DBU

**Usage:**
```python
with databricks.get_workspace_client() as client:
    # Submit a notebook job
    run = client.jobs.submit(
        job_id=123,
        notebook_params={"param1": "value1"},
    )
    
    # Or create a new job run
    run = client.jobs.submit(
        tasks=[{
            "notebook_task": {"notebook_path": "/path/to/notebook"}
        }]
    )
```

### 3. Asset Bundles (✅ Supported via Jobs)

**Asset Bundles** are deployed and run as Databricks jobs, so they're tracked when:
- The bundle is deployed and creates/updates jobs
- Jobs from the bundle are submitted via `get_workspace_client()`

**Usage:**
```python
# Asset bundles typically use the Databricks CLI or SDK
# When jobs are submitted through our wrapped client, costs are tracked
with databricks.get_workspace_client() as client:
    # Jobs created/updated by asset bundles will be tracked
    # when they're submitted through the wrapped client
    pass
```

### 4. LakeFlow Pipelines (⚠️ Partial Support)

**LakeFlow** pipelines run as Databricks jobs under the hood, so they're tracked when:
- Pipeline runs are submitted via `get_workspace_client()`
- Pipeline jobs are tagged with opaque IDs

**Note:** Direct LakeFlow API calls may not be automatically tracked. You may need to:
- Use the WorkspaceClient to submit pipeline runs
- Or manually tag pipeline runs with opaque IDs

**Usage:**
```python
with databricks.get_workspace_client() as client:
    # LakeFlow pipelines can be managed via the client
    # Pipeline runs will be tracked if submitted through the wrapped client
    pass
```

### 5. Delta Live Tables (⚠️ Partial Support)

**Delta Live Tables** also run as jobs, so they're tracked when:
- DLT pipelines are updated/run via `get_workspace_client()`
- Pipeline runs are tagged with opaque IDs

**Usage:**
```python
with databricks.get_workspace_client() as client:
    # DLT pipelines managed through the client will be tracked
    pass
```

### 6. Interactive Notebooks (❌ Not Currently Tracked)

**Interactive notebook sessions** (not job runs) are not currently tracked because:
- They don't go through the job submission API
- They run in interactive clusters
- Cost tracking would require querying system tables after execution

**Future Enhancement:** Could query `system.billing.usage` table to track interactive session costs.

## Cost Tracking Mechanisms

### Real-Time Tracking (Current Implementation)

1. **SQL Queries**: Tracked via wrapped connection cursor
2. **Job Runs**: Tracked via wrapped WorkspaceClient when jobs are submitted

### Historical Tracking (Future Enhancement)

For comprehensive cost tracking across all execution methods, you could:

1. **Query System Tables:**
   ```sql
   SELECT * FROM system.billing.usage
   WHERE tags['dagster_job_name'] IS NOT NULL
   ```

2. **Query Job Cost Tables:**
   ```sql
   SELECT * FROM system.operational_data.jobs_cost
   WHERE tags['dagster_run_id'] IS NOT NULL
   ```

3. **Use Databricks System Tables API:**
   - `system.billing.usage` - Detailed billing logs
   - `system.operational_data.jobs_cost` - Job cost analysis
   - `system.query.history` - SQL query history

## Tagging Strategy

All tracked executions are automatically tagged with:
- `dagster_snowflake_opaque_id:<opaque_id>` - For cost matching
- `dagster_job_name` - Dagster job name
- `dagster_run_id` - Dagster run ID

These tags propagate to:
- Job run metadata
- System billing tables
- Cost analysis queries

## Limitations

1. **Interactive Notebooks**: Not tracked in real-time (would need system table queries)
2. **Direct API Calls**: If you bypass the wrapped clients, costs won't be tracked
3. **DBU Estimation**: Current implementation uses execution time as a proxy; accurate DBU requires cluster configuration
4. **Asset Bundles**: Only tracked when jobs are submitted through wrapped client

## Recommendations

1. **Use Wrapped Clients**: Always use `get_sql_client()` and `get_workspace_client()` for automatic tracking
2. **Tag Resources**: Apply custom tags to Databricks resources for better cost attribution
3. **Query System Tables**: For comprehensive historical analysis, query `system.billing.usage`
4. **Monitor Job Costs**: Use `system.operational_data.jobs_cost` to identify expensive jobs

## Future Enhancements

Potential improvements:
1. Query `system.billing.usage` for comprehensive cost tracking
2. Support for interactive notebook session tracking
3. Direct integration with Asset Bundles deployment
4. LakeFlow pipeline cost tracking via LakeFlow API
5. Delta Live Tables pipeline cost tracking
6. More accurate DBU calculation using cluster configuration


