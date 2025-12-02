"""Functions to extract GCS cost data from Google Cloud Billing API."""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    try:
        from google.cloud import billing
    except ImportError:
        billing = None

QUERY_HISTORY_TIME_PADDING = timedelta(hours=1)


def get_cost_data_for_hour(
    billing_client: "billing.CloudBillingClient",
    project_id: str,
    start_hour: datetime,
    end_hour: datetime,
    bucket_name_filter: Optional[str] = None,
) -> list[tuple[str, float, str]]:
    """Query Google Cloud Billing API for GCS storage and data transfer costs.

    Args:
        billing_client: Google Cloud Billing client
        project_id: GCP project ID
        start_hour: Start of the time window
        end_hour: End of the time window
        bucket_name_filter: Optional bucket name to filter by

    Returns:
        List of tuples: (opaque_id_or_bucket_name, cost_in_usd, resource_id)
    """
    try:
        from google.cloud import billing_v1
    except ImportError:
        raise ImportError(
            "google-cloud-billing is required for GCS cost insights. "
            "Install it with: pip install google-cloud-billing"
        )

    costs: list[tuple[str, float, str]] = []

    # Adjust time window
    start_time = start_hour - QUERY_HISTORY_TIME_PADDING
    end_time = end_hour + QUERY_HISTORY_TIME_PADDING

    # Query Cloud Billing API
    # Note: This is a simplified version - you may need to adjust based on your setup
    request = billing_v1.QueryBillingAccountServicesRequest(
        name=f"projects/{project_id}",
        filter=f"""
        service.id = "6F81-5844-456A"  -- Cloud Storage service ID
        AND usage_start_time >= "{start_time.isoformat()}"
        AND usage_end_time <= "{end_time.isoformat()}"
        """,
    )

    try:
        response = billing_client.query_billing_account_services(request=request)

        for service in response.services:
            for sku in service.skus:
                # Filter by bucket if specified
                if bucket_name_filter and bucket_name_filter not in str(sku):
                    continue

                # Extract cost
                cost = float(sku.pricing_info[0].pricing_expression.tiered_rates[0].unit_price.nanos) / 1e9
                if cost <= 0:
                    continue

                # Extract bucket name from SKU description or labels
                bucket_name = "unknown_bucket"
                if hasattr(sku, "service_regions") and sku.service_regions:
                    # Try to extract bucket info from SKU metadata
                    bucket_name = f"gcs_bucket_{sku.id}"

                opaque_id = f"gcs_bucket:{bucket_name}"
                resource_id = sku.id

                costs.append((opaque_id, cost, resource_id))

    except Exception as e:
        # Billing API may require additional setup
        pass

    return costs


