# AWS JIT Access Requests Connector

Syncs elevated-access requests from the [aws-jit](https://github.com/angellist/aws-jit) platform's
DynamoDB table (`jit-access-requests` in the Security account, `us-west-2`) into the data warehouse.

## Why this connector exists

The DynamoDB table has a **90-day TTL**: request items are deleted 90 days after creation, so the
table alone cannot answer historical questions (e.g. "requests over time by team"). This connector
performs a full scan each sync and only ever **upserts** — it never emits deletes — so rows survive
in the destination after DynamoDB expires them, and history accumulates from the first sync onward.

## What it syncs

One table, `access_requests` (primary key `request_id`), containing every request `METADATA` item
(`PK begins_with "REQUEST#"`, `SK = "METADATA"`). User-index and user-mapping items, and internal
plumbing attributes (keys, GSI keys, TTL, EventBridge scheduler ARNs), are excluded. See
[docs/dynamodb-schema.md](https://github.com/angellist/aws-jit/blob/main/docs/dynamodb-schema.md)
in aws-jit for the source schema.

Key columns: `request_id`, `slack_user_id`, `slack_username`, `account_id`, `account_name`,
`permission_set_name`, `duration_hours`, `reason`, `status`, `team_name`, `requested_at`,
`approved_at`, `activated_at`, `expires_at`, `revoked_at`, approver/escalation fields.

## Configuration

| Key | Description |
|---|---|
| `aws_access_key_id` | Access key for an IAM user with `dynamodb:Scan`/`DescribeTable` on the table |
| `aws_secret_access_key` | Paired secret access key |
| `region` | AWS region (optional, default `us-west-2`) |
| `table_name` | DynamoDB table name (optional, default `jit-access-requests`) |

The IAM user (`fivetran-jit-reader`) is managed in `angellist/infra` under `pulumi/jit`; its access
key is created manually by an admin and pasted into the Fivetran connection configuration.

## Sync behavior

- Full paginated `Scan` per sync (the table holds at most a few thousand live items given the TTL,
  so this is cheap); an hourly-to-6-hourly schedule is plenty.
- Upsert-only, keyed by `request_id`; status transitions (PENDING → APPROVED → ACTIVE → EXPIRED …)
  update the existing row in place.
- State is not used; every sync is a full rescan.

## Testing locally

```bash
cd connectors/aws_jit
fivetran debug --configuration configuration.json  # with real credentials filled in
duckdb -ui files/warehouse.db                       # inspect the result
```
