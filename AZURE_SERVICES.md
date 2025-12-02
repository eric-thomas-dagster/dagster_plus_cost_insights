# Azure Services Cost Insights

## Overview

Cost insights support for Azure compute services, including Azure Synapse Analytics and Azure SQL Database. These services track costs in real-time (like Databricks and Redshift), so no scheduled assets are needed.

## Supported Services

### Azure Synapse Analytics

Azure Synapse Analytics is a unified analytics platform that brings together data integration, enterprise data warehousing, and big data analytics.

**Features:**
- Real-time cost tracking via resource wrapper
- Query tagging with opaque IDs
- DWU (Data Warehouse Units) consumption tracking
- dbt integration support

**Dependencies:**
- `pyodbc` - For SQL Server connections

**Usage:**
```python
from dagster_insights import InsightsSynapseResource

@op
def run_synapse_query(synapse: InsightsSynapseResource):
    with synapse.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM my_table")
    # Costs are automatically tracked and emitted in real-time
```

### Azure SQL Database

Azure SQL Database is a fully managed relational database service.

**Features:**
- Real-time cost tracking via resource wrapper
- Query tagging with opaque IDs
- DTU (Database Transaction Units) consumption tracking

**Dependencies:**
- `pyodbc` - For SQL Server connections

**Usage:**
```python
from dagster_insights.azure.sql import InsightsAzureSQLResource

@op
def run_azuresql_query(azuresql: InsightsAzureSQLResource):
    with azuresql.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM my_table")
    # Costs are automatically tracked and emitted in real-time
```

## Cost Metrics

### Azure Synapse Analytics
- **DWU Seconds**: Data Warehouse Units consumed (based on execution time and service level)
- **Execution Time (ms)**: Actual query execution time
- **Rows Processed**: Number of rows processed by the query

### Azure SQL Database
- **DTU Seconds**: Database Transaction Units consumed (based on execution time and service tier)
- **Execution Time (ms)**: Actual query execution time
- **Rows Processed**: Number of rows processed by the query

## dbt Integration

Azure Synapse supports dbt integration:

```python
from dagster_insights import dbt_with_synapse_insights

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    dbt_cli_invocation = dbt.cli(["build"], context=context)
    yield from dbt_with_synapse_insights(context, dbt_cli_invocation)
```

## Connection Configuration

### Azure Synapse
```python
InsightsSynapseResource(
    server="mysynapse.sql.azuresynapse.net",
    database="mydatabase",
    user=EnvVar("SYNAPSE_USER"),
    password=EnvVar("SYNAPSE_PASSWORD"),
    driver="{ODBC Driver 17 for SQL Server}",  # Optional
)
```

### Azure SQL Database
```python
InsightsAzureSQLResource(
    server="myserver.database.windows.net",
    database="mydatabase",
    user=EnvVar("AZURESQL_USER"),
    password=EnvVar("AZURESQL_PASSWORD"),
    driver="{ODBC Driver 17 for SQL Server}",  # Optional
)
```

## Cost Calculation Notes

- **Execution Time**: Tracked in real-time during query execution
- **DWU/DTU Consumption**: Currently uses execution time as a proxy. For accurate DWU/DTU tracking, you would need to:
  1. Query Azure Monitor metrics for actual consumption
  2. Or query system views like `sys.dm_pdw_exec_requests` (Synapse) or `sys.dm_db_resource_stats` (SQL Database)
  3. Calculate based on service level/tier and actual resource usage

The current implementation provides a foundation that can be extended with more accurate cost tracking as needed.

## File Structure

```
dagster_insights/azure/
├── __init__.py
├── synapse/
│   ├── __init__.py
│   ├── synapse_utils.py              # Query tagging and metadata
│   ├── insights_synapse_resource.py  # Resource wrapper
│   └── dbt_wrapper.py                # dbt integration
└── sql/
    ├── __init__.py
    ├── sql_utils.py                   # Query tagging and metadata
    └── insights_azuresql_resource.py # Resource wrapper
```

## Future Enhancements

Potential additions for other Azure services:
- **Azure Data Factory**: Pipeline execution costs
- **Azure Databricks**: Already covered separately
- **Azure HDInsight**: Big data cluster costs
- **Azure Stream Analytics**: Streaming job costs

These would follow similar patterns based on whether they support real-time tracking or require scheduled assets.


