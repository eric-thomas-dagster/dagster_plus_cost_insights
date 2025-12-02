# Missing Implementations and Additional Sources

## Current Coverage Analysis

### ✅ Fully Implemented

**Compute Sources (Real-Time Tracking):**
- Databricks ✅
- Redshift ✅
- Azure Synapse Analytics ✅
- Azure SQL Database ✅

**Storage Sources (Scheduled Assets):**
- AWS S3 ✅
- Google Cloud Storage (GCS) ✅
- Azure Data Lake Storage ✅

### ⚠️ Missing Implementations

**Storage Sources:**
- ❌ **No resource wrappers for storage operations** - Storage sources only have scheduled assets
  - This is actually OK for most cases (storage costs are typically from billing APIs)
  - But we might want wrappers for data transfer operations (PUT/GET requests)

**Common Sources Not Yet Implemented:**
- ❌ PostgreSQL / MySQL
- ❌ Trino / Presto
- ❌ Spark (EMR, Dataproc, on-prem)
- ❌ AWS Glue
- ❌ Azure Data Factory
- ❌ Google Cloud BigQuery (in reference, but we could add if needed)
- ❌ Snowflake (in reference, but we could add if needed)
- ❌ Cloud databases (RDS, Cloud SQL, etc.)

## Recommendations

### High Priority Additions

1. **PostgreSQL / MySQL** - Very common, similar to Redshift pattern
2. **Trino / Presto** - Common query engines
3. **AWS Glue** - Serverless ETL service
4. **Azure Data Factory** - Azure's ETL service
5. **Spark (EMR/Dataproc)** - Big data processing

### Medium Priority

6. **Cloud Databases** (RDS, Cloud SQL, Azure Database for PostgreSQL/MySQL)
7. **Data Transfer Costs** - For storage sources (S3, GCS, Azure)

### Lower Priority

8. **On-premise databases** - If you use them
9. **Other cloud services** - As needed


