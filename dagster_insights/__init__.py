"""
Dagster Insights extensions for additional data sources.

This package provides cost insights integrations for Databricks, Redshift, and object storage (S3, GCS, Azure Data Lake).
"""

import sys
from typing import Any

# Databricks imports
dagster_databricks_req_imports = {
    "InsightsDatabricksResource",
    "dbt_with_databricks_insights",
}
try:
    from dagster_insights.databricks.dbt_wrapper import (
        dbt_with_databricks_insights as dbt_with_databricks_insights,
    )
    from dagster_insights.databricks.insights_databricks_resource import (
        InsightsDatabricksResource as InsightsDatabricksResource,
    )
except ImportError:
    pass

# Redshift imports
dagster_redshift_req_imports = {
    "InsightsRedshiftResource",
    "dbt_with_redshift_insights",
}
try:
    from dagster_insights.redshift.dbt_wrapper import (
        dbt_with_redshift_insights as dbt_with_redshift_insights,
    )
    from dagster_insights.redshift.insights_redshift_resource import (
        InsightsRedshiftResource as InsightsRedshiftResource,
    )
except ImportError:
    pass

# S3 imports
dagster_s3_req_imports = {"create_s3_insights_asset_and_schedule"}
try:
    from dagster_insights.storage.s3.definitions import (
        create_s3_insights_asset_and_schedule as create_s3_insights_asset_and_schedule,
    )
except ImportError:
    pass

# GCS imports
dagster_gcs_req_imports = {"create_gcs_insights_asset_and_schedule"}
try:
    from dagster_insights.storage.gcs.definitions import (
        create_gcs_insights_asset_and_schedule as create_gcs_insights_asset_and_schedule,
    )
except ImportError:
    pass

# Azure Storage imports
dagster_azure_storage_req_imports = {"create_azure_insights_asset_and_schedule"}
try:
    from dagster_insights.storage.azure.definitions import (
        create_azure_insights_asset_and_schedule as create_azure_insights_asset_and_schedule,
    )
except ImportError:
    pass

# Azure Synapse imports
dagster_azure_synapse_req_imports = {
    "InsightsSynapseResource",
    "dbt_with_synapse_insights",
}
try:
    from dagster_insights.azure.synapse.dbt_wrapper import (
        dbt_with_synapse_insights as dbt_with_synapse_insights,
    )
    from dagster_insights.azure.synapse.insights_synapse_resource import (
        InsightsSynapseResource as InsightsSynapseResource,
    )
except ImportError:
    pass

# Azure SQL Database imports
dagster_azure_sql_req_imports = {
    "InsightsAzureSQLResource",
    "dbt_with_azuresql_insights",
}
try:
    from dagster_insights.azure.sql.insights_azuresql_resource import (
        InsightsAzureSQLResource as InsightsAzureSQLResource,
    )
    from dagster_insights.azure.sql.dbt_wrapper import (
        dbt_with_azuresql_insights as dbt_with_azuresql_insights,
    )
except ImportError:
    pass

# PostgreSQL imports
# PostgreSQL (psycopg2) is now a hard dependency, so always import the resource
from dagster_insights.postgresql.insights_postgresql_resource import (
    InsightsPostgreSQLResource as InsightsPostgreSQLResource,
)

# PostgreSQL cost tracking (requires dagster-cloud)
try:
    from dagster_insights.postgresql.definitions import (
        create_postgresql_insights_asset_and_schedule as create_postgresql_insights_asset_and_schedule,
    )
    from dagster_insights.postgresql.neon import (
        create_neon_insights_asset_and_schedule as create_neon_insights_asset_and_schedule,
        calculate_neon_hourly_cost as calculate_neon_hourly_cost,
    )
except ImportError:
    pass

# dbt wrapper is optional (requires dagster-dbt)
try:
    from dagster_insights.postgresql.dbt_wrapper import (
        dbt_with_postgresql_insights as dbt_with_postgresql_insights,
    )
except ImportError:
    pass

# MySQL imports
dagster_mysql_req_imports = {
    "InsightsMySQLResource",
    "dbt_with_mysql_insights",
}
try:
    from dagster_insights.mysql.insights_mysql_resource import (
        InsightsMySQLResource as InsightsMySQLResource,
    )
    from dagster_insights.mysql.dbt_wrapper import (
        dbt_with_mysql_insights as dbt_with_mysql_insights,
    )
except ImportError:
    pass

# AWS Glue imports
dagster_glue_req_imports = {"InsightsGlueResource"}
try:
    from dagster_insights.aws.glue.insights_glue_resource import (
        InsightsGlueResource as InsightsGlueResource,
    )
except ImportError:
    pass

# Azure Data Factory imports
dagster_adf_req_imports = {"InsightsDataFactoryResource"}
try:
    from dagster_insights.azure.data_factory.insights_data_factory_resource import (
        InsightsDataFactoryResource as InsightsDataFactoryResource,
    )
except ImportError:
    pass

# Trino imports
dagster_trino_req_imports = {
    "InsightsTrinoResource",
    "dbt_with_trino_insights",
}
try:
    from dagster_insights.trino.insights_trino_resource import (
        InsightsTrinoResource as InsightsTrinoResource,
    )
    from dagster_insights.trino.dbt_wrapper import (
        dbt_with_trino_insights as dbt_with_trino_insights,
    )
except ImportError:
    pass

# Spark EMR imports
dagster_emr_req_imports = {"InsightsEMRResource"}
try:
    from dagster_insights.spark.emr.insights_emr_resource import (
        InsightsEMRResource as InsightsEMRResource,
    )
except ImportError:
    pass

# Spark Dataproc imports
dagster_dataproc_req_imports = {"InsightsDataprocResource"}
try:
    from dagster_insights.spark.dataproc.insights_dataproc_resource import (
        InsightsDataprocResource as InsightsDataprocResource,
    )
except ImportError:
    pass

# AWS Athena imports
dagster_athena_req_imports = {
    "InsightsAthenaResource",
    "dbt_with_athena_insights",
}
try:
    from dagster_insights.aws.athena.insights_athena_resource import (
        InsightsAthenaResource as InsightsAthenaResource,
    )
    from dagster_insights.aws.athena.dbt_wrapper import (
        dbt_with_athena_insights as dbt_with_athena_insights,
    )
except ImportError:
    pass

# AWS RDS imports
dagster_rds_req_imports = {
    "InsightsRDSResource",
    "dbt_with_rds_insights",
}
try:
    from dagster_insights.aws.rds.insights_rds_resource import (
        InsightsRDSResource as InsightsRDSResource,
    )
    from dagster_insights.aws.rds.dbt_wrapper import (
        dbt_with_rds_insights as dbt_with_rds_insights,
    )
except ImportError:
    pass

# GCP Cloud SQL imports
dagster_cloud_sql_req_imports = {
    "InsightsCloudSQLResource",
    "dbt_with_cloud_sql_insights",
}
try:
    from dagster_insights.gcp.cloud_sql.insights_cloud_sql_resource import (
        InsightsCloudSQLResource as InsightsCloudSQLResource,
    )
    from dagster_insights.gcp.cloud_sql.dbt_wrapper import (
        dbt_with_cloud_sql_insights as dbt_with_cloud_sql_insights,
    )
except ImportError:
    pass

# Azure Database imports
dagster_azure_database_req_imports = {
    "InsightsAzureDatabaseResource",
    "dbt_with_azure_database_insights",
}
try:
    from dagster_insights.azure.database.insights_azure_database_resource import (
        InsightsAzureDatabaseResource as InsightsAzureDatabaseResource,
    )
    from dagster_insights.azure.database.dbt_wrapper import (
        dbt_with_azure_database_insights as dbt_with_azure_database_insights,
    )
except ImportError:
    pass


# This is overridden in order to provide a better error message
# when the user tries to import a symbol which relies on another integration
# being installed.
def __getattr__(name) -> Any:
    obj = sys.modules[__name__].__dict__.get(name)
    if not obj:
        if name in dagster_databricks_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`databricks-sdk` or `databricks-sql-connector`."
            )
        elif name in dagster_redshift_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`psycopg2-binary` or `redshift-connector`."
            )
        elif name in dagster_s3_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`boto3`."
            )
        elif name in dagster_gcs_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`google-cloud-billing`."
            )
        elif name in dagster_azure_storage_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`azure-mgmt-costmanagement`."
            )
        elif name in dagster_azure_synapse_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`pyodbc`."
            )
        elif name in dagster_azure_sql_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`pyodbc`."
            )
        elif name in dagster_postgresql_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`psycopg2-binary`."
            )
        elif name in dagster_mysql_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`pymysql`."
            )
        elif name in dagster_glue_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`boto3`."
            )
        elif name in dagster_adf_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`azure-mgmt-datafactory` and `azure-identity`."
            )
        elif name in dagster_trino_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`trino`."
            )
        elif name in dagster_emr_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`boto3`."
            )
        elif name in dagster_dataproc_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`google-cloud-dataproc`."
            )
        elif name in dagster_athena_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`boto3`."
            )
        elif name in dagster_rds_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`boto3` and `psycopg2-binary` or `pymysql`."
            )
        elif name in dagster_cloud_sql_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`google-cloud-sql-admin` and `psycopg2-binary` or `pymysql`."
            )
        elif name in dagster_azure_database_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "required dependencies are not installed. You may need to install "
                "`azure-mgmt-rdbms` and `psycopg2-binary` or `pymysql`."
            )
        else:
            raise AttributeError(f"module {__name__} has no attribute {name}")
    return obj

