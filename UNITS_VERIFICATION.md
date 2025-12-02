# Units Verification - All Sources

## ✅ Units Are Properly Defined

All our implementations follow the BigQuery/Snowflake pattern where **units are embedded in the metadata key names or metric names**.

## Real-Time Tracking (AssetObservation Metadata)

Units are in the metadata key names, following BigQuery pattern:

### ✅ Databricks
- `__databricks_dbu` → **DBU** (Databricks Units)
- `__databricks_compute_time_ms` → **milliseconds**

### ✅ Redshift
- `__redshift_execution_time_ms` → **milliseconds**
- `__redshift_bytes_scanned` → **bytes**
- `__redshift_rows_processed` → **rows** (count)

### ✅ Azure Synapse
- `__synapse_dwu_seconds` → **seconds** (Data Warehouse Units)
- `__synapse_execution_time_ms` → **milliseconds**
- `__synapse_rows_processed` → **rows** (count)

### ✅ Azure SQL Database
- `__azuresql_dtu_seconds` → **seconds** (Database Transaction Units)
- `__azuresql_execution_time_ms` → **milliseconds**

### ✅ PostgreSQL
- `__postgresql_execution_time_ms` → **milliseconds**
- `__postgresql_rows_processed` → **rows** (count)

### ✅ MySQL
- `__mysql_execution_time_ms` → **milliseconds**
- `__mysql_rows_processed` → **rows** (count)

### ✅ Trino
- `__trino_execution_time_ms` → **milliseconds**
- `__trino_rows_processed` → **rows** (count)
- `__trino_bytes_read` → **bytes**

### ✅ Athena
- `__athena_data_scanned_gb` → **gigabytes**
- `__athena_execution_time_ms` → **milliseconds**

### ✅ AWS Glue
- `__glue_dpu_hours` → **hours** (Data Processing Units)
- `__glue_execution_time_ms` → **milliseconds**

### ✅ Azure Data Factory
- `__adf_execution_time_ms` → **milliseconds**
- `__adf_activity_runs` → **count**

### ✅ EMR
- `__emr_cluster_hours` → **hours**
- `__emr_execution_time_ms` → **milliseconds**
- `__emr_data_processed_gb` → **gigabytes**

### ✅ Dataproc
- `__dataproc_cluster_hours` → **hours**
- `__dataproc_execution_time_ms` → **milliseconds**
- `__dataproc_data_processed_gb` → **gigabytes**

### ✅ RDS
- `__rds_execution_time_ms` → **milliseconds**
- `__rds_instance_hours` → **hours**
- `__rds_storage_gb` → **gigabytes**
- `__rds_iops` → **IOPS** (count)
- `__rds_rows_processed` → **rows** (count)

### ✅ Cloud SQL
- `__cloud_sql_execution_time_ms` → **milliseconds**
- `__cloud_sql_instance_hours` → **hours**
- `__cloud_sql_storage_gb` → **gigabytes**
- `__cloud_sql_rows_processed` → **rows** (count)

### ✅ Azure Database
- `__azure_database_execution_time_ms` → **milliseconds**
- `__azure_database_compute_units` → **vCores** (count)
- `__azure_database_storage_gb` → **gigabytes**
- `__azure_database_rows_processed` → **rows** (count)

## Scheduled Assets (put_cost_information)

Units are in the metric_name, following Snowflake pattern:

### ✅ S3
- `"s3_cost_usd"` → **USD**

### ✅ GCS
- `"gcs_cost_usd"` → **USD**

### ✅ Azure Data Lake Storage
- `"azure_storage_cost_usd"` → **USD**

## Pattern Compliance

✅ **All metadata keys include unit suffixes** (`_ms`, `_gb`, `_hours`, `_dbu`, `_seconds`, `_bytes`, etc.)
✅ **All metric names include unit suffixes** (`_usd`)
✅ **Service-specific units are clearly named** (`_dbu`, `_dpu`, `_dwu`, `_dtu`)

## Conclusion

**All sources have units properly defined** in their metadata keys or metric names, following the same pattern as BigQuery (metadata keys) and Snowflake (metric names).

No changes needed - units are already embedded in the naming convention!


