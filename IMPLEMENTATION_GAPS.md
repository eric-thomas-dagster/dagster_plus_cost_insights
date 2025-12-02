# Implementation Gaps Analysis

## Critical Gaps Found

### 1. Missing Scheduled Assets for Compute Sources

**Snowflake Pattern**: Has `create_snowflake_insights_asset_and_schedule()` that:
- Queries `query_history` system table
- Uses `get_cost_data_for_hour()` function
- Creates hourly partitioned assets
- Calls `put_cost_information()` to upload costs

**Missing For**:
- ❌ Redshift - Has system tables (`stl_query`, `svl_query_summary`)
- ❌ Databricks - Has system tables (`system.billing.usage`, `system.query.history`)
- ❌ Azure Synapse - Has DMVs (`sys.dm_pdw_exec_requests`)
- ❌ PostgreSQL - Could query `pg_stat_statements`
- ❌ MySQL - Could query `performance_schema`
- ❌ Trino - Has system tables
- ❌ Athena - Could query query history

### 2. Missing `get_cost_data_for_hour()` Functions

**Snowflake Has**: `get_cost_data_for_hour()` that queries system tables and returns `list[tuple[str, float, str]]` (opaque_id, cost, query_id)

**Missing For**:
- ❌ Redshift
- ❌ Databricks
- ❌ Azure Synapse
- ❌ PostgreSQL
- ❌ MySQL
- ❌ Trino
- ❌ Athena

### 3. dbt Integration Completeness

**BigQuery Pattern**: dbt wrapper queries `INFORMATION_SCHEMA.JOBS` after dbt runs to get actual costs

**Our dbt Wrappers**: 
- ✅ Have basic structure
- ⚠️ May not query system tables for comprehensive cost data
- ⚠️ Need to verify they match BigQuery's pattern

### 4. Metric Names Consistency

**Reference**:
- Snowflake: `"snowflake_credits"`
- S3: `"s3_cost_usd"`

**Need to Define**:
- Redshift metric name
- Databricks metric name
- Azure Synapse metric name
- PostgreSQL/MySQL metric names
- Trino metric name
- Athena metric name
- etc.

### 5. Real-Time vs Scheduled Tracking

**Current State**:
- ✅ Real-time tracking via resource wrappers (most sources)
- ✅ Scheduled assets for storage (S3, GCS, Azure Data Lake)
- ❌ Scheduled assets for compute sources (except storage)

**Best Practice** (from Snowflake):
- Real-time tracking for immediate attribution
- Scheduled assets for comprehensive historical coverage and system table queries

## Recommendations

### High Priority

1. **Add `get_cost_data_for_hour()` functions** for all compute sources with system tables
2. **Add scheduled asset definitions** for Redshift, Databricks, Azure Synapse
3. **Enhance dbt wrappers** to query system tables like BigQuery does
4. **Define consistent metric names** for all sources

### Medium Priority

5. Add scheduled assets for PostgreSQL/MySQL (if system tables are enabled)
6. Add scheduled assets for Trino/Athena

### Low Priority

7. Document metric names and cost units
8. Add validation/error handling improvements


