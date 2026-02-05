# Spec 005: Postgres Read Model (Prisma-Managed)

**Created**: 2026-02-05
**Updated**: 2026-02-05
**Status**: Proposed

## Overview

Postgres serves as the **primary queryable store** for:

- **Raw events**: searchable event archive (replaces Loki for event search)
- **Source checkpoints**: cursor persistence for reliable ingestion
- **Trend snapshots**: historical trend data for charts/tables
- **Brief results**: LLM-generated summaries
- **Consumer lag**: tracking for data freshness validation
- **Retention policies**: cleanup configuration

The schema is managed by **Prisma** in `packages/db/prisma/schema.prisma`.

## Why Postgres over Loki for Events?

- **Structured queries**: SQL is more powerful than LogQL for event analysis
- **Single storage layer**: eliminates dual-write complexity (Kafka + Loki + Postgres)
- **Cost efficiency**: Loki optimized for logs, not structured event queries
- **Full-text search**: Postgres GIN indexes provide good-enough FTS for MVP

Loki remains in the stack for **application logs only** (service debug logs, traces correlation).

## Local Dev Wiring

- Postgres container: `docker-compose.yml` as `postgres`
- Bootstrap SQL: `infra/postgres/init/0001_init.sql` (extensions only)
- Schema source of truth: `packages/db/prisma/schema.prisma`
- Grafana datasource: `infra/grafana/provisioning/datasources/datasource.yaml` (uid: `POSTGRES`)

### Applying Schema Changes

```bash
# Development (creates migration files)
cd packages/db && npm run db:migrate

# Production/CI (applies existing migrations)
cd packages/db && npm run db:migrate:deploy

# Quick push without migration (dev only)
cd packages/db && npm run db:push
```

## Tables

### `raw_events`

Queryable event archive. Events are **immutable** once written.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Auto-increment PK |
| `event_id` | TEXT UNIQUE | Stable per-source unique ID |
| `source` | ENUM | rss, news, hackernews, reddit, github, twitter |
| `fetched_at` | TIMESTAMPTZ | When collector fetched the item |
| `published_at` | TIMESTAMPTZ? | Original publish time (if known) |
| `url` | TEXT? | Source URL |
| `title` | TEXT? | Item title |
| `text` | TEXT | Primary content |
| `author_*` | TEXT? | Denormalized author info |
| `engagement_*` | INT? | Score, comments, likes, shares |
| `lang` | TEXT? | Detected language |
| `tags` | TEXT[] | Free-form tags |
| `extracted_hashtags` | TEXT[] | Parsed from content |
| `extracted_urls` | TEXT[] | Parsed from content |
| `topics` | TEXT[] | Canonical topic keys (set by Trends service backfill or Collector) |
| `source_meta` | JSONB? | Per-source metadata |

**Indexes**:
- `(source, fetched_at DESC)` — filter by source, recent first
- `(fetched_at DESC)` — global timeline
- `(published_at DESC)` — by publish time
- `GIN(topics)` — topic containment queries
- `GIN(search_vector)` — full-text search (see below)

### Full-Text Search

The `raw_events` table includes a generated `tsvector` column for efficient full-text search:

```sql
-- In migration
ALTER TABLE raw_events
ADD COLUMN search_vector tsvector
GENERATED ALWAYS AS (
  setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
  setweight(to_tsvector('english', coalesce(text, '')), 'B')
) STORED;

CREATE INDEX idx_raw_events_search ON raw_events USING GIN(search_vector);
```

**Query example**:
```sql
SELECT id, title, ts_rank(search_vector, query) AS rank
FROM raw_events, to_tsquery('english', 'bedrock & aws') AS query
WHERE search_vector @@ query
ORDER BY rank DESC
LIMIT 50;
```

**Note**: The `GENERATED ALWAYS AS ... STORED` column is automatically maintained by Postgres. No application code needed.

### `source_checkpoints`

Cursor persistence for reliable ingestion across restarts.

| Column | Type | Description |
|--------|------|-------------|
| `source` | TEXT | e.g., 'rss.aws_blog', 'reddit.r_aws' |
| `checkpoint_key` | TEXT | e.g., 'after_cursor', 'last_item_id' |
| `checkpoint_value` | TEXT | The cursor value |
| `updated_at` | TIMESTAMPTZ | Last update time |

**Primary Key**: `(source, checkpoint_key)`

**Example checkpoints**:
- `('reddit.r_aws', 'after_cursor', 't3_abc123')`
- `('hackernews', 'last_max_id', '39876543')`
- `('rss.aws_blog', 'last_guid', 'https://aws.amazon.com/...')`

### `trend_snapshots`

Append-only snapshot storage for historical trend charts.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Auto-increment PK |
| `generated_at` | TIMESTAMPTZ | When snapshot was produced |
| `window` | ENUM | 15m, 60m, 24h |
| `snapshot` | JSONB | Full TrendSnapshot protobuf as JSON |

### `brief_results`

Stores both success and failure results for audit.

| Column | Type | Description |
|--------|------|-------------|
| `request_id` | TEXT PK | From SummaryRequest |
| `produced_at` | TIMESTAMPTZ | When result was produced |
| `status` | ENUM | success, failure |
| `result` | JSONB | Full BriefResult protobuf as JSON |

### `consumer_lag`

Tracks Kafka consumer progress for **data freshness validation**.

| Column | Type | Description |
|--------|------|-------------|
| `consumer_group` | TEXT | e.g., 'trends-processor' |
| `topic` | TEXT | Kafka topic name |
| `partition` | INT | Partition number |
| `current_offset` | BIGINT | Consumer's current offset |
| `latest_offset` | BIGINT | Topic's latest offset |
| `lag_messages` | BIGINT | latest - current |
| `updated_at` | TIMESTAMPTZ | Last update |

**Primary Key**: `(consumer_group, topic, partition)`

Used by Trends service to verify data freshness before triggering briefs.

### `retention_policies`

Configures cleanup for each table.

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | TEXT PK | Table to clean |
| `retention_days` | INT | Days to keep (0 = no cleanup) |
| `enabled` | BOOLEAN | Whether cleanup runs |
| `last_cleanup_at` | TIMESTAMPTZ? | Last successful cleanup |

**Default policies** (via seed):
- `raw_events`: 14 days
- `trend_snapshots`: 90 days
- `brief_results`: 180 days
- `consumer_lag`: 7 days
- `source_checkpoints`: 0 (never delete)

## Write Responsibilities

Each table has a **single owner service** that is responsible for writes. This prevents coordination issues and race conditions.

| Table | Owner Service | Writes | Reads |
|-------|--------------|--------|-------|
| `raw_events` | Persister | Insert only (immutable) | Grafana, Trends (for evidence) |
| `source_checkpoints` | (deprecated) | — | — |
| `trend_snapshots` | Trends | Insert only (append) | Grafana |
| `brief_results` | Brief | Insert + idempotent upsert | Grafana |
| `consumer_lag` | Trends | Upsert (periodic) | Trends (freshness check), Grafana |
| `retention_policies` | Seed script / Admin | Initial seed only | Retention job |

### Ownership Rules

1. **Single writer per table**: Only one service writes to each table. This simplifies reasoning about data consistency.

2. **No cross-service writes**: Services don't write to tables owned by other services. For example:
   - Persister does NOT write to `trend_snapshots`
   - Brief does NOT write to `consumer_lag`

3. **Kafka as coordination layer**: If data needs to flow between services, it goes through Kafka, not direct database writes.

4. **Read access is shared**: Any service can read any table (for queries, not writes).

### Why Not "Persister Writes Everything"?

We considered routing all Postgres writes through the Persister, but decided against it:

- **Latency**: Trends and Brief would need to publish to Kafka, wait for Persister, then verify writes
- **Complexity**: Persister becomes a bottleneck and needs to understand all schemas
- **Coupling**: Schema changes in one domain (e.g., brief_results) require Persister changes

Instead, each service owns its domain tables and writes directly. The services are still decoupled via Kafka for event flow.

**Note**: The `source_checkpoints` table is no longer used. The Collector service stores checkpoints in local SQLite to maintain zero database dependencies. This table can be removed in a future migration.

## Retention Enforcement

A **retention cleanup job** runs daily to enforce retention policies.

### Job Specification

**Schedule**: Daily at 03:00 UTC (low-traffic period)

**Implementation**: `packages/db/src/retention-job.ts`

```typescript
interface CleanupResult {
  tableName: string;
  rowsDeleted: number;
  durationMs: number;
  error?: string;
}

async function runRetentionCleanup(): Promise<CleanupResult[]> {
  const policies = await prisma.retentionPolicy.findMany({
    where: { enabled: true, retentionDays: { gt: 0 } },
  });

  const results: CleanupResult[] = [];

  for (const policy of policies) {
    const start = Date.now();
    try {
      const deleted = await cleanupTable(policy.tableName, policy.retentionDays);

      await prisma.retentionPolicy.update({
        where: { tableName: policy.tableName },
        data: { lastCleanupAt: new Date() },
      });

      results.push({
        tableName: policy.tableName,
        rowsDeleted: deleted,
        durationMs: Date.now() - start,
      });

      log.info({ table: policy.tableName, deleted }, 'Retention cleanup completed');
    } catch (error) {
      results.push({
        tableName: policy.tableName,
        rowsDeleted: 0,
        durationMs: Date.now() - start,
        error: error.message,
      });

      log.error({ table: policy.tableName, error }, 'Retention cleanup failed');
    }
  }

  return results;
}

async function cleanupTable(tableName: string, retentionDays: number): Promise<number> {
  const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000);

  // Table-specific cleanup queries
  switch (tableName) {
    case 'raw_events':
      const r1 = await prisma.$executeRaw`
        DELETE FROM raw_events WHERE fetched_at < ${cutoff}
      `;
      return r1;

    case 'trend_snapshots':
      const r2 = await prisma.$executeRaw`
        DELETE FROM trend_snapshots WHERE generated_at < ${cutoff}
      `;
      return r2;

    case 'brief_results':
      const r3 = await prisma.$executeRaw`
        DELETE FROM brief_results WHERE produced_at < ${cutoff}
      `;
      return r3;

    case 'consumer_lag':
      const r4 = await prisma.$executeRaw`
        DELETE FROM consumer_lag WHERE updated_at < ${cutoff}
      `;
      return r4;

    default:
      throw new Error(`Unknown table: ${tableName}`);
  }
}
```

### Batch Deletion

For large tables (`raw_events`), delete in batches to avoid long-running transactions:

```typescript
async function cleanupTableBatched(
  tableName: string,
  retentionDays: number,
  batchSize: number = 10000
): Promise<number> {
  const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000);
  let totalDeleted = 0;
  let deleted: number;

  do {
    deleted = await prisma.$executeRaw`
      DELETE FROM raw_events
      WHERE id IN (
        SELECT id FROM raw_events
        WHERE fetched_at < ${cutoff}
        LIMIT ${batchSize}
      )
    `;
    totalDeleted += deleted;

    if (deleted > 0) {
      log.debug({ deleted, totalDeleted }, 'Batch deleted');
      await sleep(100); // Brief pause to reduce lock contention
    }
  } while (deleted === batchSize);

  return totalDeleted;
}
```

### Metrics

- `retention_cleanup_rows_deleted_total{table=...}`
- `retention_cleanup_duration_seconds{table=...}`
- `retention_cleanup_errors_total{table=...}`

### Running the Job

**Option 1**: Kubernetes CronJob
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: retention-cleanup
spec:
  schedule: "0 3 * * *"  # 03:00 UTC daily
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: cleanup
            image: rising-intelligence/db:latest
            command: ["npm", "run", "retention:cleanup"]
          restartPolicy: OnFailure
```

**Option 2**: Docker Compose with external cron
```bash
# Add to host crontab
0 3 * * * docker compose run --rm db npm run retention:cleanup >> /var/log/retention.log 2>&1
```

**Option 3**: In-process scheduler (simpler for MVP)
```typescript
// In a long-running service (e.g., Trends)
import { CronJob } from 'cron';

const retentionJob = new CronJob('0 3 * * *', async () => {
  log.info('Starting scheduled retention cleanup');
  await runRetentionCleanup();
}, null, true, 'UTC');
```

### Alerting

| Alert | Condition | Severity |
|-------|-----------|----------|
| Cleanup Failed | `retention_cleanup_errors_total` > 0 | Warning |
| Cleanup Stale | No cleanup in 48h (check `last_cleanup_at`) | Warning |
| Table Growth | `raw_events` count growing despite cleanup | Warning |

## Grafana Integration

Grafana is provisioned with a Postgres datasource to display:

- **Event Explorer**: Search raw_events by source, topic, date range, keywords
- **Trend History**: Time series from trend_snapshots
- **Latest Brief**: Most recent brief_results
- **Consumer Lag**: Data freshness monitoring

Example Grafana query for topic volume over time:
```sql
SELECT
  date_trunc('hour', fetched_at) AS time,
  unnest(topics) AS topic,
  COUNT(*) AS volume
FROM raw_events
WHERE fetched_at > NOW() - INTERVAL '24 hours'
GROUP BY 1, 2
ORDER BY 1
```
