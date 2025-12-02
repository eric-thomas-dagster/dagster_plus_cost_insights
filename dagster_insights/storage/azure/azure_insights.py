"""Functions to extract Azure Data Lake Storage cost data from Azure Cost Management API."""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    try:
        from azure.mgmt.costmanagement import CostManagementClient
    except ImportError:
        CostManagementClient = None

QUERY_HISTORY_TIME_PADDING = timedelta(hours=1)


def get_cost_data_for_hour(
    cost_management_client: "CostManagementClient",
    subscription_id: str,
    start_hour: datetime,
    end_hour: datetime,
    container_name_filter: Optional[str] = None,
) -> list[tuple[str, float, str]]:
    """Query Azure Cost Management API for Data Lake Storage costs.

    Args:
        cost_management_client: Azure Cost Management client
        subscription_id: Azure subscription ID
        start_hour: Start of the time window
        end_hour: End of the time window
        container_name_filter: Optional container name to filter by

    Returns:
        List of tuples: (opaque_id_or_container_name, cost_in_usd, resource_id)
    """
    try:
        from azure.mgmt.costmanagement import CostManagementClient
        from azure.mgmt.costmanagement.models import QueryDefinition, QueryTimePeriod
    except ImportError:
        raise ImportError(
            "azure-mgmt-costmanagement is required for Azure cost insights. "
            "Install it with: pip install azure-mgmt-costmanagement"
        )

    costs: list[tuple[str, float, str]] = []

    # Adjust time window
    start_time = start_hour - QUERY_HISTORY_TIME_PADDING
    end_time = end_hour + QUERY_HISTORY_TIME_PADDING

    # Query Cost Management API
    query_definition = QueryDefinition(
        type="ActualCost",
        timeframe="Custom",
        time_period=QueryTimePeriod(
            from_property=start_time,
            to=end_time,
        ),
        dataset={
            "granularity": "Hourly",
            "aggregation": {
                "totalCost": {"name": "PreTaxCost", "function": "Sum"}
            },
            "grouping": [
                {"type": "Dimension", "name": "ServiceName"},
                {"type": "Dimension", "name": "ResourceId"},
            ],
            "filter": {
                "dimensions": {
                    "name": "ServiceName",
                    "operator": "In",
                    "values": ["Storage", "Azure Data Lake Storage"],
                }
            },
        },
    )

    try:
        query_result = cost_management_client.query.usage(
            scope=f"/subscriptions/{subscription_id}",
            parameters=query_definition,
        )

        for row in query_result.rows:
            service_name = row[0]
            resource_id = row[1]
            cost = float(row[2]) if len(row) > 2 else 0.0

            if cost <= 0:
                continue

            # Extract container name from resource ID
            container_name = "unknown_container"
            if "/containers/" in resource_id:
                container_name = resource_id.split("/containers/")[-1].split("/")[0]
            elif container_name_filter:
                container_name = container_name_filter

            opaque_id = f"azure_container:{container_name}"
            costs.append((opaque_id, cost, resource_id))

    except Exception as e:
        # Cost Management API may require additional setup
        pass

    return costs


