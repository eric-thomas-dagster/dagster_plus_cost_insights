# Extending Cost Insights to Additional Data Sources

## Overview

The Dagster Insights feature currently has built-in integrations for **Snowflake** and **BigQuery**. However, the architecture is designed to be extensible, allowing you to add cost insights for additional data sources.

## Current Implementation Pattern

Based on the Dagster Cloud reference implementation, cost insights typically follow this pattern:

1. **Resource Definitions**: Resources that connect to the data source's API or metadata tables
2. **Insights Assets**: Assets that query cost/usage data and emit metrics to Dagster Insights
3. **Query Matching**: Logic to match queries/operations to Dagster assets/jobs

## Adding New Data Sources

### Supported Sources You Can Add

- **Databricks** (especially important for you)
- **Amazon Redshift**
- **Azure Synapse Analytics / Azure SQL Data Warehouse**
- **AWS S3/Glacier**
- **Google Cloud Storage (GCS)**
- **Azure Data Lake Storage**

### Implementation Approach

For each new source, you'll need to:

1. **Identify Cost Metrics Source**:
   - Databricks: REST API (`/api/2.0/jobs/runs/list`, `/api/2.0/sql/history/queries`)
   - Redshift: System tables (`stl_query`, `svl_query_summary`, `stl_scan`)
   - Azure Synapse: `sys.dm_pdw_exec_requests`, `sys.dm_pdw_request_steps`
   - S3/Glacier: AWS Cost Explorer API, CloudWatch metrics
   - GCS: Google Cloud Billing API, Cloud Monitoring
   - Azure Data Lake: Azure Cost Management API, Azure Monitor

2. **Create Resource Definitions**: Similar to how Snowflake/BigQuery resources are defined

3. **Create Insights Assets**: Assets that fetch and emit cost metrics

4. **Implement Query Matching**: Logic to correlate operations with Dagster assets/jobs

## Key Questions to Answer

When examining the reference code, look for:

1. **Hardcoded Source Names**: Check if there are any hardcoded checks like `if source == "snowflake"` or `if source == "bigquery"` that would need to be made generic
2. **Resource Configuration**: How are credentials/connections configured? Is this extensible?
3. **Metrics Emission**: How are metrics emitted to Dagster Insights? Is this generic enough?
4. **Query Parsing**: How are queries parsed to match assets? Can this be abstracted?

## Next Steps

1. Clone/examine the reference repository structure
2. Identify the abstraction points
3. Create a plugin/extension architecture if needed
4. Implement integrations for each new source following the established patterns


