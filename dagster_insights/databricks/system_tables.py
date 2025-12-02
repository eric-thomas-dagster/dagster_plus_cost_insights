"""Helper functions to query Databricks system tables for comprehensive cost tracking.

These functions can be used to track costs for execution methods that aren't
captured in real-time, such as:
- Interactive notebook sessions
- Jobs submitted outside of Dagster
- Asset bundles deployed separately
- LakeFlow pipelines
- Delta Live Tables
"""

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from dagster_insights.databricks.databricks_utils import OPAQUE_ID_SQL_SIGIL

if TYPE_CHECKING:
    from databricks.sql import Connection


def get_cost_data_from_system_tables(
    connection: "Connection",
    start_time: datetime,
    end_time: datetime,
) -> list[tuple[str, float, str]]:
    """Query Databricks system tables for comprehensive cost data.
    
    This queries the system.billing.usage table which contains detailed
    billing information for all execution methods.
    
    Args:
        connection: Databricks SQL connection
        start_time: Start of time window
        end_time: End of time window
        
    Returns:
        List of tuples: (opaque_id, cost_in_dbu, resource_id)
    """
    costs: list[tuple[str, float, str]] = []

    # Query system.billing.usage for costs tagged with Dagster opaque IDs
    query = f"""
    SELECT
        tags['dagster_snowflake_opaque_id'] as opaque_id,
        usage_quantity as dbu,
        sku_name,
        usage_start_time,
        usage_end_time
    FROM system.billing.usage
    WHERE usage_start_time >= '{start_time.isoformat()}'
      AND usage_end_time <= '{end_time.isoformat()}'
      AND tags['dagster_snowflake_opaque_id'] IS NOT NULL
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        for row in results:
            opaque_id = row[0]
            dbu = float(row[1]) if row[1] else 0.0
            resource_id = row[2] or "unknown"

            if opaque_id and dbu > 0:
                costs.append((opaque_id, dbu, resource_id))

    except Exception as e:
        # System tables may not be available in all Databricks plans
        # or may require special permissions
        pass

    return costs


def get_job_costs_from_system_tables(
    connection: "Connection",
    start_time: datetime,
    end_time: datetime,
) -> list[tuple[str, float, str]]:
    """Query system.operational_data.jobs_cost for job-specific cost data.
    
    This provides more detailed job cost information including:
    - Job run costs
    - Cluster costs
    - Task costs
    
    Args:
        connection: Databricks SQL connection
        start_time: Start of time window
        end_time: End of time window
        
    Returns:
        List of tuples: (opaque_id, cost_in_dbu, job_run_id)
    """
    costs: list[tuple[str, float, str]] = []

    query = f"""
    SELECT
        tags['dagster_snowflake_opaque_id'] as opaque_id,
        total_dbu,
        job_run_id,
        job_id
    FROM system.operational_data.jobs_cost
    WHERE start_time >= '{start_time.isoformat()}'
      AND start_time <= '{end_time.isoformat()}'
      AND tags['dagster_snowflake_opaque_id'] IS NOT NULL
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        for row in results:
            opaque_id = row[0]
            total_dbu = float(row[1]) if row[1] else 0.0
            job_run_id = str(row[2]) if row[2] else "unknown"

            if opaque_id and total_dbu > 0:
                costs.append((opaque_id, total_dbu, job_run_id))

    except Exception as e:
        # System tables may not be available
        pass

    return costs


def get_sql_query_costs_from_system_tables(
    connection: "Connection",
    start_time: datetime,
    end_time: datetime,
) -> list[tuple[str, float, str]]:
    """Query system.query.history for SQL query costs.
    
    This provides cost information for SQL queries executed through
    SQL warehouses, including queries that weren't tracked in real-time.
    
    Args:
        connection: Databricks SQL connection
        start_time: Start of time window
        end_time: End of time window
        
    Returns:
        List of tuples: (opaque_id, cost_in_dbu, query_id)
    """
    costs: list[tuple[str, float, str]] = []

    # Extract opaque IDs from query text
    opaque_ids_sql = rf"""
    regexp_extract(query_text, '{OPAQUE_ID_SQL_SIGIL}\\[\\[\\[(.*?)\\]\\]\\]', 1, 1, 'i', 1)
    """.strip()

    query = f"""
    SELECT
        {opaque_ids_sql} as opaque_id,
        query_id,
        execution_time_ms,
        total_dbu_used
    FROM system.query.history
    WHERE query_start_time >= '{start_time.isoformat()}'
      AND query_start_time <= '{end_time.isoformat()}'
      AND {opaque_ids_sql} IS NOT NULL
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        for row in results:
            opaque_id = row[0]
            query_id = str(row[1]) if row[1] else "unknown"
            # Use total_dbu_used if available, otherwise estimate from execution_time_ms
            dbu = float(row[3]) if len(row) > 3 and row[3] else 0.0
            if dbu == 0 and len(row) > 2 and row[2]:
                # Estimate DBU from execution time
                execution_time_ms = row[2]
                dbu = execution_time_ms / (1000 * 60 * 60) * 0.15  # Rough estimate

            if opaque_id and dbu > 0:
                costs.append((opaque_id, dbu, query_id))

    except Exception as e:
        # System tables may not be available
        pass

    return costs


