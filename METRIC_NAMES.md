# Metric Names for Cost Insights

## Standard Metric Names

When using `put_cost_information()`, use these metric names:

### Compute Sources (Real-Time Tracking)
- **Databricks**: `"databricks_dbu"` (Databricks Units)
- **Redshift**: `"redshift_execution_seconds"` or `"redshift_compute_hours"`
- **Azure Synapse**: `"synapse_dwu_seconds"` (Data Warehouse Units)
- **Azure SQL Database**: `"azuresql_dtu_seconds"` (Database Transaction Units)
- **PostgreSQL**: `"postgresql_execution_seconds"`
- **MySQL**: `"mysql_execution_seconds"`
- **Trino**: `"trino_execution_seconds"`
- **Athena**: `"athena_data_scanned_tb"` (Terabytes scanned)
- **EMR**: `"emr_cluster_hours"`
- **Dataproc**: `"dataproc_cluster_hours"`
- **AWS Glue**: `"glue_dpu_hours"` (Data Processing Units)
- **Azure Data Factory**: `"adf_execution_seconds"`

### Cloud Databases
- **RDS**: `"rds_instance_hours"` or `"rds_execution_seconds"`
- **Cloud SQL**: `"cloud_sql_instance_hours"` or `"cloud_sql_execution_seconds"`
- **Azure Database**: `"azure_database_compute_hours"` or `"azure_database_execution_seconds"`

### Storage Sources (Scheduled Assets)
- **S3**: `"s3_cost_usd"` ✅ (already defined)
- **GCS**: `"gcs_cost_usd"` ✅ (already defined)
- **Azure Data Lake Storage**: `"azure_storage_cost_usd"` ✅ (already defined)

## Reference Metric Names
- **Snowflake**: `"snowflake_credits"` (from reference)
- **BigQuery**: Uses metadata keys, not `put_cost_information` (real-time only)

## Notes

1. **Real-time sources** typically don't use `put_cost_information` - they emit `AssetObservation` directly
2. **Scheduled assets** use `put_cost_information` with these metric names
3. **Cost units** should be consistent (seconds, hours, credits, USD, etc.)


