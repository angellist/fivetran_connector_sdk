"""AWS JIT Access Requests Connector for Fivetran Connector SDK.

Syncs elevated-access requests from the aws-jit platform's DynamoDB table
(`jit-access-requests`, single-table design) into the destination.

Only request METADATA items are synced (PK begins_with "REQUEST#", SK = "METADATA");
user-index and user-mapping items are skipped. The sync is a full table scan with
upserts and no deletes: DynamoDB expires items after 90 days (TTL), but rows already
synced are retained in the destination, so history accumulates from the first sync.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# For supporting Connector operations like update() and schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For reading from DynamoDB
import boto3
from boto3.dynamodb.types import TypeDeserializer

__DEFAULT_REGION = "us-west-2"
__DEFAULT_TABLE_NAME = "jit-access-requests"
__SCAN_PAGE_LIMIT = 500  # Items per Scan page

# DynamoDB attribute -> destination column. Explicit allowlist: internal plumbing
# attributes (PK, SK, GSI1PK, GSI1SK, TTL, EventBridge scheduler ARNs) are excluded.
__COLUMN_MAP = {
    "requestId": "request_id",
    "slackUserId": "slack_user_id",
    "slackUsername": "slack_username",
    "identityCenterUserId": "identity_center_user_id",
    "accountId": "account_id",
    "accountName": "account_name",
    "permissionSetArn": "permission_set_arn",
    "permissionSetName": "permission_set_name",
    "durationHours": "duration_hours",
    "reason": "reason",
    "additionalContext": "additional_context",
    "status": "status",
    "teamName": "team_name",
    "requestedAt": "requested_at",
    "approvedAt": "approved_at",
    "activatedAt": "activated_at",
    "expiresAt": "expires_at",
    "approverSlackUserId": "approver_slack_user_id",
    "approverUsername": "approver_username",
    "rejectionReason": "rejection_reason",
    "slackChannelId": "slack_channel_id",
    "slackMessageTs": "slack_message_ts",
    "manualEscalatedAt": "manual_escalated_at",
    "manualEscalatedBy": "manual_escalated_by",
    "autoEscalatedAt": "auto_escalated_at",
    "revokedAt": "revoked_at",
    "revokedBySlackUserId": "revoked_by_slack_user_id",
    "revokedByUsername": "revoked_by_username",
    "revocationFailedAt": "revocation_failed_at",
}

__TIMESTAMP_COLUMNS = {
    "requested_at",
    "approved_at",
    "activated_at",
    "expires_at",
    "manual_escalated_at",
    "auto_escalated_at",
    "revoked_at",
    "revocation_failed_at",
}


def validate_configuration(configuration: dict):
    """Validate the configuration dictionary to ensure it contains all required parameters.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: If any required configuration parameter is missing.
    """
    required_configs = ["aws_access_key_id", "aws_secret_access_key"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def get_dynamodb_client(configuration: dict):
    """Create a DynamoDB client from static IAM credentials in the configuration.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Returns:
        A boto3 DynamoDB client.
    """
    return boto3.client(
        "dynamodb",
        aws_access_key_id=configuration["aws_access_key_id"],
        aws_secret_access_key=configuration["aws_secret_access_key"],
        region_name=configuration.get("region", __DEFAULT_REGION),
    )


def schema(configuration: dict):
    """Define the schema for the destination table.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Returns:
        A list with the access_requests table definition.
    """
    columns = {"request_id": "STRING", "duration_hours": "INT"}
    for column in __COLUMN_MAP.values():
        if column in ("request_id", "duration_hours"):
            continue
        columns[column] = "UTC_DATETIME" if column in __TIMESTAMP_COLUMNS else "STRING"

    return [
        {
            "table": "access_requests",
            "primary_key": ["request_id"],
            "columns": columns,
        }
    ]


def map_item_to_row(item: dict) -> dict:
    """Map a deserialized DynamoDB request item to a destination row.

    Args:
        item: A deserialized DynamoDB item (plain Python types).

    Returns:
        A dict keyed by destination column names.
    """
    row = {}
    for attribute, column in __COLUMN_MAP.items():
        value = item.get(attribute)
        if value is None:
            row[column] = None
        elif column == "duration_hours":
            row[column] = int(value)
        else:
            row[column] = str(value)
    return row


def update(configuration: dict, state: dict):
    """Scan the DynamoDB table and upsert all access request records.

    Each sync performs a full paginated Scan filtered to request METADATA items.
    The table is small (a few thousand live items given the 90-day TTL), so a full
    scan per sync is cheap; upserts are idempotent and deletes are never emitted,
    which preserves TTL-expired history in the destination.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
        state: A dictionary that holds the state of the connector (unused; each sync rescans).
    """
    log.warning("AWS JIT Access Requests connector: starting sync")
    validate_configuration(configuration)

    client = get_dynamodb_client(configuration)
    table_name = configuration.get("table_name", __DEFAULT_TABLE_NAME)
    deserializer = TypeDeserializer()

    scan_kwargs = {
        "TableName": table_name,
        "FilterExpression": "begins_with(PK, :request_prefix) AND SK = :metadata",
        "ExpressionAttributeValues": {
            ":request_prefix": {"S": "REQUEST#"},
            ":metadata": {"S": "METADATA"},
        },
        "Limit": __SCAN_PAGE_LIMIT,
    }

    total = 0
    while True:
        response = client.scan(**scan_kwargs)
        for raw_item in response.get("Items", []):
            item = {key: deserializer.deserialize(value) for key, value in raw_item.items()}
            if not item.get("requestId"):
                log.warning(f"Skipping request item without requestId (PK={item.get('PK')})")
                continue
            op.upsert(table="access_requests", data=map_item_to_row(item))
            total += 1

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

    log.info(f"Synced {total} access requests")
    # Checkpoint an empty state: each sync is a full rescan, so no cursor is needed.
    op.checkpoint(state={})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
