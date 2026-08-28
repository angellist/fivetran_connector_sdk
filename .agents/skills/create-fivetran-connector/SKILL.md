---
name: create-fivetran-connector
description: "Step-by-step procedure for building a new custom Fivetran Connector SDK connector in this repo (angellist/fivetran_connector_sdk) that syncs a source (API, DynamoDB, etc.) into Snowflake. Covers the native-connector-first decision rule, scaffolding, schema()/update() patterns, upsert/checkpoint semantics, config placeholders, lint/format conventions, credentials via angellist/infra, README requirements, PR checklist, and MAR expectations. Use when asked to build a new data ingestion pipe or Fivetran connector."
---

# Create a Custom Fivetran Connector

Use this skill to add a new custom connector under `connectors/<name>/` that pipes a
source into the Snowflake warehouse via Fivetran's Connector SDK.

Reference implementation in this repo:
- `connectors/beehiiv/` — REST API source, incremental sync with cursors, retry/backoff,
  per-N-records checkpointing.

## 0. First decide: do you even need a custom connector?

A custom connector is the **last resort**, not the default. Check in this order:

1. **Native Fivetran connector exists for the source?** Use it. Fivetran maintains it, and
   it handles state, retries, and delete capture (soft-deletes via `_fivetran_deleted`).
   Company-specific filtering, renaming, and typing belong in a dbt staging model in
   angellist/data-tooling — a staging layer exists for every source anyway, so ingest-time
   shaping in a custom connector duplicates it.
2. **No native connector?** Write a custom connector — but write it *generically*
   (config-driven endpoints/tables/credentials, no company-specific column allowlists or
   business filters), because the standard for connectors in this repo is that they could be
   upstreamed to Fivetran's community examples and maintained there.
3. If the connector can only be written by hardcoding one team's table layout or field list,
   that shaping should move to dbt staging and the source should go through a native or
   generic connector instead.

Example: the `jit-access-requests` DynamoDB table was first piped with a bespoke `aws_jit`
connector (PR #16, closed) and then reworked onto Fivetran's native DynamoDB connector +
DynamoDB Streams + a dbt staging model, because the custom connector's METADATA filter and
column map fit exactly one table and nobody else's use case.

## 1. Scaffold

Create `connectors/<name>/` with exactly these files:

| File | Purpose |
|---|---|
| `connector.py` | The connector: `schema()`, `update()`, helpers, module docstring |
| `configuration.json` | Placeholder values ONLY (e.g. `"<YOUR_API_KEY>"`) — never real secrets |
| `requirements.txt` | Extra deps only (`requests` and `fivetran_connector_sdk` are pre-installed; add e.g. `boto3==...` pinned) |
| `README.md` | What it syncs, configuration table, sync behavior/caveats, local test command |

## 2. connector.py pattern

```python
"""<Source> Connector for Fivetran Connector SDK.
<what it syncs, sync strategy, any history/TTL caveats>
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


def validate_configuration(configuration: dict):
    """Raise ValueError for any missing required config key."""
    required_configs = ["api_key"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """Return list of {table, primary_key, columns} dicts."""
    ...


def update(configuration: dict, state: dict):
    """Fetch from source, op.upsert(...) rows, op.checkpoint(state) at safe points."""
    ...


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
```

Conventions (enforced by CI / reviewers):
- Docstrings on every function; module docstring linking the SDK Technical Reference and
  Best Practices docs.
- Module-level constants prefixed `__` (e.g. `__PAGE_LIMIT = 100`), with a short comment each.
- Column types: `STRING`, `INT`, `FLOAT`, `BOOLEAN`, `UTC_DATETIME`, `NAIVE_DATE`; dict/list
  values upserted as-is become `VARIANT` (JSON) automatically.
- snake_case destination column names. Keep any column mapping config-driven or generic —
  company-specific allowlists/renames belong in the dbt staging layer, not the connector.
- Small pure helper functions (e.g. `map_item_to_row`) so logic is testable without the SDK.
- Credentials come ONLY from `configuration` — never hardcoded, never read from env inside
  the connector.

## 3. Choose the sync strategy

- **Incremental (preferred for high-volume APIs):** keep a cursor in `state`
  (e.g. `state["last_synced_at"]`), request only newer records, `op.checkpoint(state)` every
  N records (`beehiiv` uses 1000) so an interrupted sync resumes.
- **Full rescan (small sources, ≤ tens of thousands of rows):** scan everything each sync and
  upsert all rows; `op.checkpoint(state={})` at the end.
- **History preservation:** if the source expires/deletes data (TTL) and the warehouse must
  retain it, NEVER emit `op.delete(...)` — upsert-only means rows survive in Snowflake after
  the source drops them. Document this prominently in the README. (Native connectors get the
  same effect via `_fivetran_deleted` soft-deletes, which dbt staging can keep.)
- Deterministic primary keys: derive a stable `id` from the source. Upserts are idempotent
  on the primary key.

## 4. Verify locally

```bash
flake8 connectors/<name>/connector.py --max-line-length 99   # .flake8 at repo root
black --check --line-length 99 connectors/<name>/connector.py
# With real credentials filled into configuration.json (do NOT commit them):
cd connectors/<name> && fivetran debug --configuration configuration.json
duckdb -ui files/warehouse.db   # inspect what landed
```

If credentials don't exist yet (e.g. the IAM user ships in a separate infra PR), say so
explicitly in the PR's Testing section and leave the `fivetran debug` checklist item unchecked.

## 5. Credentials / least-privilege access (angellist/infra)

For AWS sources, add a dedicated read-only IAM principal in `angellist/infra` (Pulumi,
TypeScript):
- **Native Fivetran connectors:** create an `aws.iam.Role` whose trust policy allows
  Fivetran's AWS account `834469178297` to assume it (i.e. we extend trust to Fivetran),
  gated on an `sts:ExternalId` condition (the External ID is tied to our Fivetran
  account — copy it from an existing Fivetran role's trust policy, e.g. `FivetranDynamoDBAccess`
  in support-services, or from the connection setup form). Scope the policy to the source
  resources; see `pulumi/jit/index.ts` (`fivetran-jit-reader`) for a DynamoDB example
  (Scan/DescribeTable + stream-read actions, streams enabled with `NEW_AND_OLD_IMAGES`).
- **Custom connectors:** `aws.iam.User` + inline `aws.iam.UserPolicy` scoped to exactly the
  actions/resources the connector needs. Do NOT create an `aws.iam.AccessKey` resource (keeps
  the secret out of Pulumi state) — after `pulumi up`, an admin runs
  `aws iam create-access-key --user-name <user>` and pastes the key into the Fivetran
  connection configuration.
- Verify with `bunx @biomejs/biome@2.3.2 check --diagnostic-level=error <file>`.

For API sources, the API key is provisioned in the vendor's dashboard and stored only in the
Fivetran connection config.

## 6. PRs and rollout

- Conventional Commit titles, signed commits (e.g. `feat: add <name> connector for <source>`).
- Follow the repo PR template (Jira ticket or request link, Description, Testing, Checklist).
- Rollout order: merge/apply the infra PR → issue credentials → a Fivetran admin creates the
  Connector SDK connection (destination schema e.g. `staging_<name>` in Snowflake, schedule
  hourly–6-hourly) → first sync lands → verify row counts in Snowflake.
- Managing the connection afterwards (sync status, trigger/pause, frequency) is covered by the
  `manage-fivetran-connectors` skill in angellist/data-tooling.

## 7. MAR (Monthly Active Rows) expectations

Fivetran bills on distinct primary-key rows written per month (deduped within the month, no
matter how many syncs touch a row). So:
- Incremental connectors: MAR ≈ new + updated rows per month.
- Full-rescan upsert-only connectors: MAR ≈ live row count in the source (every live row is
  re-upserted each sync but counts once per month). Retained-but-expired history adds nothing.
Estimate this in the PR/plan for any high-volume source.
