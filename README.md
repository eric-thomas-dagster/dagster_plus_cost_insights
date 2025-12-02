# Dagster+ Cost Insights Extensions

Extended cost insights integrations for **Dagster+** (Dagster Cloud), providing cost tracking and attribution for additional data sources beyond the built-in Snowflake and BigQuery support.

> **Note:** This package is designed for Dagster+ (Cloud) only and requires access to internal Dagster+ APIs. It will not work with open-source Dagster.

## 🎯 What This Provides

Track costs for **Dagster-triggered operations** across 18+ data sources:

### Data Warehouses & Databases
- **Databricks** - SQL queries, job runs, notebooks, Asset Bundles, LakeFlow, DLT
- **Amazon Redshift** - Query execution and system table tracking
- **Azure Synapse Analytics** - Query execution and DWU consumption
- **Azure SQL Database** - Query tracking and DTU consumption
- **PostgreSQL** - Query execution time and rows processed
- **MySQL** - Query execution tracking
- **Trino/Presto** - Query execution and resource tracking

### Cloud Storage
- **AWS S3** - Storage and data transfer costs
- **Google Cloud Storage (GCS)** - Storage and operations
- **Azure Data Lake Storage** - Storage and transaction costs

### ETL & Orchestration
- **AWS Glue** - Job runs and DPU consumption
- **Azure Data Factory** - Pipeline runs and activity costs
- **AWS EMR** - Spark cluster costs
- **Google Dataproc** - Spark cluster costs

### Database Services
- **AWS Athena** - Serverless query costs
- **AWS RDS** - Database instance tracking
- **GCP Cloud SQL** - Database instance tracking
- **Azure Database** - Managed database tracking

## 🚀 Quick Start

### Installation

```bash
# Install base package
pip install dagster-insights

# Install with specific integrations
pip install dagster-insights[databricks]
pip install dagster-insights[redshift,aws]
pip install dagster-insights[all]  # Everything
```

### Basic Usage

Replace your existing data source resources with Insights versions:

```python
from dagster import Definitions, asset
from dagster_insights import InsightsDatabricksResource

@asset
def my_databricks_asset(databricks: InsightsDatabricksResource):
    with databricks.get_sql_client() as client:
        with client.cursor() as cur:
            cur.execute("SELECT * FROM my_table")
            # Costs automatically tracked and attributed to this asset!

defs = Definitions(
    assets=[my_databricks_asset],
    resources={
        "databricks": InsightsDatabricksResource(
            workspace_url="https://your-workspace.cloud.databricks.com",
            access_token={"env": "DATABRICKS_TOKEN"},
        )
    }
)
```

### With dbt

Supported for: Databricks, Redshift, PostgreSQL, Azure Synapse, Azure SQL, MySQL, Trino, and Athena

```python
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_insights import dbt_with_databricks_insights

@dbt_assets(manifest=manifest)
def my_dbt_assets(context, dbt: DbtCliResource):
    dbt_cli_invocation = dbt.cli(["build"], context=context)
    yield from dbt_with_databricks_insights(context, dbt_cli_invocation)
```

For other databases, use the corresponding wrapper function:
- `dbt_with_redshift_insights`
- `dbt_with_postgresql_insights`
- `dbt_with_synapse_insights`
- `dbt_with_azuresql_insights`
- `dbt_with_mysql_insights`
- `dbt_with_trino_insights`
- `dbt_with_athena_insights`

### Storage Cost Tracking

For object storage, add scheduled assets:

```python
from dagster import Definitions
from dagster_insights import create_s3_insights_asset_and_schedule

s3_insights = create_s3_insights_asset_and_schedule(
    start_date="2025-01-01",
    aws_resource_key="aws",
)

defs = Definitions(
    assets=[*your_assets, *s3_insights.assets],
    schedules=[s3_insights.schedule],
    resources={"aws": your_aws_resource},
)
```

## 📖 How It Works

### Cost Attribution Flow

1. **Resource Wrapper** intercepts operations (queries, jobs, etc.)
2. **Opaque ID Tagging** - Operations are tagged with Dagster context
3. **Cost Tracking** - Execution costs are measured (time, resources, DBUs, etc.)
4. **Asset Attribution** - Costs are attributed to specific Dagster assets/runs
5. **Insights Upload** - Cost data is uploaded to Dagster+ Insights

### What Gets Tracked

**✅ Tracked:**
- Queries executed by Dagster ops/assets
- Jobs submitted by Dagster
- Pipelines triggered by Dagster
- Storage used by Dagster assets
- Costs per Dagster run
- Costs per asset

**❌ Not Tracked:**
- Manual queries from UI/console
- Operations outside Dagster
- Other teams' usage
- Non-Dagster workloads

This is **by design** - the goal is to understand costs *within your Dagster pipelines*, not to replace your cloud billing dashboards.

## 🔌 Supported Integrations

### Real-Time Tracking (Compute Sources)

These track costs as operations execute:

| Source | Resource Class | dbt Support |
|--------|---------------|-------------|
| Databricks | `InsightsDatabricksResource` | ✅ |
| Redshift | `InsightsRedshiftResource` | ✅ |
| PostgreSQL | `InsightsPostgreSQLResource` | ✅ |
| Azure Synapse | `InsightsSynapseResource` | ✅ |
| Azure SQL | `InsightsAzureSQLResource` | ✅ |
| MySQL | `InsightsMySQLResource` | ✅ |
| Trino | `InsightsTrinoResource` | ✅ |
| AWS Glue | `InsightsGlueResource` | ❌ |
| Azure Data Factory | `InsightsDataFactoryResource` | ❌ |
| AWS EMR | `InsightsEMRResource` | ❌ |
| GCP Dataproc | `InsightsDataprocResource` | ❌ |
| AWS Athena | `InsightsAthenaResource` | ✅ |
| AWS RDS | `InsightsRDSResource` | ❌ |
| GCP Cloud SQL | `InsightsCloudSQLResource` | ❌ |
| Azure Database | `InsightsAzureDatabaseResource` | ❌ |

### Scheduled Tracking (Storage Sources)

These poll billing APIs on a schedule:

| Source | Function | Granularity |
|--------|----------|-------------|
| AWS S3 | `create_s3_insights_asset_and_schedule` | Daily |
| GCS | `create_gcs_insights_asset_and_schedule` | Daily |
| Azure Storage | `create_azure_insights_asset_and_schedule` | Daily |

## 📚 Examples

See the [`examples/`](./examples) directory for complete examples:

- [`databricks_example.py`](./examples/databricks_example.py) - Comprehensive Databricks usage
- [`redshift_example.py`](./examples/redshift_example.py) - Redshift with dbt
- [`storage_example.py`](./examples/storage_example.py) - S3, GCS, Azure storage tracking
- [`multi_source_example.py`](./examples/multi_source_example.py) - Using multiple sources

## 🔧 Incremental Adoption

You don't need to migrate everything at once:

```python
defs = Definitions(
    assets=[
        *legacy_assets,          # Keep using existing resources
        *new_insights_assets,    # Use Insights resources
    ],
    resources={
        "databricks_old": MyCustomResource(...),     # Keep
        "databricks": InsightsDatabricksResource(...), # Add new
    }
)
```

Start with a few assets, validate cost tracking works, then expand gradually.

## 🏗️ Architecture

### Package Structure

```
dagster_insights/
├── __init__.py                 # Main exports
├── insights_utils.py           # Shared utilities
│
├── databricks/                 # Databricks integration
│   ├── insights_databricks_resource.py
│   ├── databricks_utils.py
│   ├── dbt_wrapper.py
│   ├── workspace_client_wrapper.py
│   └── system_tables.py
│
├── redshift/                   # Redshift integration
│   ├── insights_redshift_resource.py
│   ├── redshift_utils.py
│   └── dbt_wrapper.py
│
├── storage/                    # Object storage
│   ├── s3/
│   ├── gcs/
│   └── azure/
│
└── [other integrations...]
```

### Design Patterns

1. **Resource Wrappers** - Wrap native clients to intercept operations
2. **Query Tagging** - Inject opaque IDs for cost matching
3. **Context Tracking** - Use Dagster execution context for attribution
4. **Cost Emission** - Use `AssetObservation` for real-time costs
5. **Scheduled Polling** - Query billing APIs for storage costs

## 📋 Requirements

- **Dagster+** (Cloud) account - Required
- **Python** 3.8 or higher
- **dagster** >= 1.5.0
- **dagster-cloud** package (for `put_cost_information` API)
- Source-specific dependencies (installed with extras)

## ⚠️ Important Notes

### Dagster+ Only

This package requires Dagster+ (Cloud) and will not work with open-source Dagster. It uses internal Dagster+ APIs for uploading cost data.

### Partial Coverage is OK

You don't need 100% coverage to get value. Even tracking costs for some of your assets provides useful insights.

### Not a Billing Dashboard Replacement

This tracks costs **within your Dagster pipelines**, not all costs in your cloud accounts. Use this alongside:
- AWS Cost Explorer
- Databricks billing console
- Azure Cost Management
- GCP Billing

## 🤝 Contributing

Contributions welcome! See [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## 📄 License

Apache License 2.0 - See [LICENSE](./LICENSE)

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/eric-thomas-dagster/dagster_plus_cost_insights/issues)
- **Discussions**: [GitHub Discussions](https://github.com/eric-thomas-dagster/dagster_plus_cost_insights/discussions)
- **Documentation**: [Full Documentation](./docs/)

## 🗺️ Roadmap

- [ ] Add more comprehensive tests
- [ ] Improve dbt integration for all sources
- [ ] Add Azure Databricks-specific optimizations
- [ ] Support for custom cost metrics
- [ ] Better error handling and retry logic
- [ ] Cost forecasting and anomaly detection

---

**Built for the Dagster community** ❤️
