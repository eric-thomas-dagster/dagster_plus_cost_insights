# Real-Time vs Scheduled Cost Tracking

## Principle

**If we can get cost information in real-time, we don't need scheduled assets.**

## Reference Patterns

### BigQuery (Real-Time Only)
- ✅ Resource wrapper tracks queries in real-time
- ✅ Emits `AssetObservation` events immediately
- ❌ No scheduled assets (not needed)

### Snowflake (Both Real-Time + Scheduled)
- ✅ Resource wrapper tracks queries in real-time
- ✅ Scheduled assets query `query_history` table (has 45min latency)
- **Reason for scheduled**: System table latency for comprehensive historical coverage

## Our Implementation Strategy

### Real-Time Only (Like BigQuery)
These sources can track costs in real-time via resource wrappers:
- ✅ **Databricks** - SQL queries tracked in real-time
- ✅ **Redshift** - Queries tracked in real-time via connection wrapper
- ✅ **Azure Synapse** - Queries tracked in real-time via connection wrapper
- ✅ **Azure SQL Database** - Queries tracked in real-time
- ✅ **PostgreSQL** - Queries tracked in real-time
- ✅ **MySQL** - Queries tracked in real-time
- ✅ **Trino** - Queries tracked in real-time
- ✅ **Athena** - Queries tracked in real-time
- ✅ **AWS Glue** - Job runs tracked in real-time
- ✅ **Azure Data Factory** - Pipeline runs tracked in real-time
- ✅ **EMR/Dataproc** - Job runs tracked in real-time
- ✅ **RDS/Cloud SQL/Azure Database** - Queries tracked in real-time

**No scheduled assets needed** - real-time tracking is sufficient.

### Scheduled Assets Only (Like Storage)
These sources need scheduled assets because:
- Cost data comes from billing APIs (not queryable in real-time)
- Data has latency (typically 24 hours)
- No query interception possible

- ✅ **S3** - Cost Explorer API
- ✅ **GCS** - Billing API
- ✅ **Azure Data Lake Storage** - Cost Management API

### Optional: Scheduled for Comprehensive Coverage
If a source has system tables with latency AND you want comprehensive historical coverage:
- Could add scheduled assets (like Snowflake)
- But not required if real-time tracking is sufficient

## Implementation Status

✅ **All compute sources**: Real-time tracking via resource wrappers
✅ **All storage sources**: Scheduled assets via billing APIs
✅ **No unnecessary scheduled assets**: Only where needed

## Key Takeaway

**Real-time tracking is the primary method. Scheduled assets are only for:**
1. Storage sources (billing APIs)
2. Optional comprehensive coverage for sources with system table latency (like Snowflake)


