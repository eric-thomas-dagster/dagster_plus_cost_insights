# Cost Units Documentation

## How Units Are Defined

### Real-Time Tracking (AssetObservation Metadata)
Units are defined in the **metadata key names**, following the BigQuery pattern:
- `__bigquery_bytes_billed` → unit: **bytes**
- `__bigquery_slots_ms` → unit: **milliseconds**

### Scheduled Assets (put_cost_information)
Units are defined in the **metric_name**, following the Snowflake pattern:
- `"snowflake_credits"` → unit: **credits**
- `"s3_cost_usd"` → unit: **USD**

## Our Implementation Units

### Real-Time Tracking Metadata Keys

#### Databricks
- `__databricks_dbu` → unit: **DBU** (Databricks Units)
- `__databricks_compute_time_ms` → unit: **milliseconds**

#### Redshift
- `__redshift_execution_time_ms` → unit: **milliseconds**
- `__redshift_bytes_scanned` → unit: **bytes**
- `__redshift_rows_processed` → unit: **rows** (count)

#### Azure Synapse
- `__synapse_dwu_seconds` → unit: **seconds** (Data Warehouse Units)
- `__synapse_execution_time_ms` → unit: **milliseconds**
- `__synapse_rows_processed` → unit: **rows** (count)

#### Azure SQL Database
- `__azuresql_dtu_seconds` → unit: **seconds** (Database Transaction Units)
- `__azuresql_execution_time_ms` → unit: **milliseconds**

#### PostgreSQL
- `__postgresql_execution_time_ms` → unit: **milliseconds**
- `__postgresql_rows_processed` → unit: **rows** (count)

#### MySQL
- `__mysql_execution_time_ms` → unit: **milliseconds**
- `__mysql_rows_processed` → unit: **rows** (count)

#### Trino
- `__trino_execution_time_ms` → unit: **milliseconds**
- `__trino_rows_processed` → unit: **rows** (count)
- `__trino_bytes_read` → unit: **bytes**

#### Athena
- `__athena_data_scanned_gb` → unit: **gigabytes**
- `__athena_execution_time_ms` → unit: **milliseconds**

#### AWS Glue
- `__glue_dpu_hours` → unit: **hours** (Data Processing Units)
- `__glue_execution_time_ms` → unit: **milliseconds**

#### Azure Data Factory
- `__adf_execution_time_ms` → unit: **milliseconds**
- `__adf_activity_runs` → unit: **count**

#### EMR
- `__emr_cluster_hours` → unit: **hours**
- `__emr_execution_time_ms` → unit: **milliseconds**
- `__emr_data_processed_gb` → unit: **gigabytes**

#### Dataproc
- `__dataproc_cluster_hours` → unit: **hours**
- `__dataproc_execution_time_ms` → unit: **milliseconds**
- `__dataproc_data_processed_gb` → unit: **gigabytes**

#### RDS
- `__rds_execution_time_ms` → unit: **milliseconds**
- `__rds_instance_hours` → unit: **hours**
- `__rds_storage_gb` → unit: **gigabytes**
- `__rds_iops` → unit: **IOPS** (count)
- `__rds_rows_processed` → unit: **rows** (count)

#### Cloud SQL
- `__cloud_sql_execution_time_ms` → unit: **milliseconds**
- `__cloud_sql_instance_hours` → unit: **hours**
- `__cloud_sql_storage_gb` → unit: **gigabytes**
- `__cloud_sql_rows_processed` → unit: **rows** (count)

#### Azure Database
- `__azure_database_execution_time_ms` → unit: **milliseconds**
- `__azure_database_compute_units` → unit: **vCores** (count)
- `__azure_database_storage_gb` → unit: **gigabytes**
- `__azure_database_rows_processed` → unit: **rows** (count)

### Scheduled Assets Metric Names

#### S3
- `"s3_cost_usd"` → unit: **USD**

#### GCS
- `"gcs_cost_usd"` → unit: **USD**

#### Azure Data Lake Storage
- `"azure_storage_cost_usd"` → unit: **USD**

## Unit Standards

### Time Units
- **milliseconds** (`_ms`): For execution time
- **seconds** (`_seconds`): For DWU/DTU consumption
- **hours** (`_hours`): For cluster/instance hours

### Data Units
- **bytes**: Raw bytes
- **gigabytes** (`_gb`): For storage and data scanned

### Cost Units
- **USD** (`_usd`): For actual dollar costs (storage sources)

### Count Units
- **rows**: Row counts
- **IOPS**: I/O operations per second
- **vCores**: Virtual CPU cores
- **DPU**: Data Processing Units
- **DBU**: Databricks Units

### Service-Specific Units
- **credits**: Snowflake credits
- **DBU**: Databricks Units
- **DPU**: AWS Glue Data Processing Units
- **DWU**: Azure Synapse Data Warehouse Units
- **DTU**: Azure SQL Database Transaction Units

## Verification

✅ **All metadata keys include unit suffixes** (`_ms`, `_gb`, `_hours`, `_dbu`, etc.)
✅ **All metric names include unit suffixes** (`_usd`, `_credits`, etc.)
✅ **Units are clearly defined** in key/name patterns


