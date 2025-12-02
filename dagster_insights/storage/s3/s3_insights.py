"""Functions to extract S3 cost data from AWS Cost Explorer API."""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    try:
        import boto3
        from mypy_boto3_ce import CostExplorerClient
    except ImportError:
        CostExplorerClient = None

QUERY_HISTORY_TIME_PADDING = timedelta(hours=1)  # Cost Explorer data may have delay


def get_cost_data_for_hour(
    cost_explorer_client: "CostExplorerClient",
    start_hour: datetime,
    end_hour: datetime,
    bucket_name_filter: Optional[str] = None,
) -> list[tuple[str, float, str]]:
    """Query AWS Cost Explorer API for S3 storage and data transfer costs.

    Args:
        cost_explorer_client: Boto3 Cost Explorer client
        start_hour: Start of the time window
        end_hour: End of the time window
        bucket_name_filter: Optional bucket name to filter by

    Returns:
        List of tuples: (opaque_id_or_bucket_name, cost_in_usd, resource_id)
    """
    try:
        import boto3
    except ImportError:
        raise ImportError(
            "boto3 is required for S3 cost insights. Install it with: pip install boto3"
        )

    costs: list[tuple[str, float, str]] = []

    # Adjust time window
    start_time = start_hour - QUERY_HISTORY_TIME_PADDING
    end_time = end_hour + QUERY_HISTORY_TIME_PADDING

    # Query Cost Explorer for S3 costs
    # Group by service (S3) and usage type
    response = cost_explorer_client.get_cost_and_usage(
        TimePeriod={
            "Start": start_time.strftime("%Y-%m-%d"),
            "End": end_time.strftime("%Y-%m-%d"),
        },
        Granularity="HOURLY",
        Metrics=["UnblendedCost"],
        GroupBy=[
            {"Type": "DIMENSION", "Key": "SERVICE"},
            {"Type": "DIMENSION", "Key": "USAGE_TYPE"},
            {"Type": "TAG", "Key": "BucketName"},  # If you tag buckets
        ],
        Filter={
            "Dimensions": {
                "Key": "SERVICE",
                "Values": ["Amazon Simple Storage Service"],
            }
        },
    )

    # Process results
    for result_by_time in response.get("ResultsByTime", []):
        for group in result_by_time.get("Groups", []):
            cost = float(group.get("Metrics", {}).get("UnblendedCost", {}).get("Amount", "0"))
            if cost <= 0:
                continue

            # Extract bucket name from keys
            keys = group.get("Keys", [])
            bucket_name = None
            for key in keys:
                if "BucketName" in key or bucket_name_filter:
                    # Extract bucket name from tag or dimension
                    bucket_name = key.split("$")[-1] if "$" in key else key
                    break

            if not bucket_name:
                bucket_name = "unknown_bucket"

            # Use bucket name as opaque ID (you may want to map this to actual opaque IDs)
            # For storage, we might not have query-level opaque IDs like compute sources
            opaque_id = f"s3_bucket:{bucket_name}"

            # Resource ID could be the bucket ARN or usage type
            resource_id = keys[1] if len(keys) > 1 else "s3_storage"

            costs.append((opaque_id, cost, resource_id))

    return costs


def get_s3_storage_size(
    s3_client,
    bucket_name: str,
    start_hour: datetime,
    end_hour: datetime,
) -> float:
    """Get S3 bucket storage size in GB for a time period.

    Note: S3 doesn't provide historical size data easily, so this may need
    to use CloudWatch metrics or current size as an approximation.
    """
    try:
        # Use CloudWatch to get bucket size
        # This is a simplified version - you may need to query CloudWatch metrics
        import boto3

        cloudwatch = boto3.client("cloudwatch")
        response = cloudwatch.get_metric_statistics(
            Namespace="AWS/S3",
            MetricName="BucketSizeBytes",
            Dimensions=[{"Name": "BucketName", "Value": bucket_name}],
            StartTime=start_hour,
            EndTime=end_hour,
            Period=3600,  # 1 hour
            Statistics=["Average"],
        )

        # Calculate average size in GB
        datapoints = response.get("Datapoints", [])
        if not datapoints:
            return 0.0

        total_bytes = sum(point.get("Average", 0) for point in datapoints)
        avg_bytes = total_bytes / len(datapoints) if datapoints else 0
        return avg_bytes / (1024**3)  # Convert to GB

    except Exception:
        return 0.0


