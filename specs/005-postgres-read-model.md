# Spec 005: Postgres Read Model (Prisma-Managed)

**Created**: 2026-02-05
**Updated**: 2026-02-05
**Status**: Proposed

## Overview

Postgres serves as the **primary queryable store** for:

- **Raw events**: searchable event archive (replaces Loki for event search)
- **Trend snapshots**: historical trend data for charts/tables
- **Brief results**: LLM-generated summaries
- **Consumer lag**: tracking for data freshness validation
- **Discovery candidates**: emerging unknown terms for operator review
- **Retention policies**: cleanup configuration

**Note**: Source checkpoints are stored in the Collector's local SQLite (see `apps/collector/spec/data-model.md`), not Postgres. This keeps the Collector decoupled from the database.

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
- Grafana datasource: `infra/grafana/provisioning/datasources/datasources.yaml` (uid: `POSTGRES`)

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
| `source` | ENUM | rss, news, hackernews, reddit, github, bluesky, mastodon |
| `fetched_at` | TIMESTAMPTZ | When collector fetched the item |
| `published_at` | TIMESTAMPTZ? | Original publish time (if known) |
| `url` | TEXT? | Source URL |
| `title` | TEXT? | Item title |
| `text` | TEXT | Primary content |
| `author_*` | TEXT? | Denormalized author info |
| `engagement_*` | INT? | Score, comments, likes, shares |
| `lang` | TEXT? | Detected language |
| `tags` | TEXT[] | Raw tags from ingestion (MVP: mirrors `topics`; future: may include free-form tags) |
| `extracted_hashtags` | TEXT[] | Parsed from content |
| `extracted_urls` | TEXT[] | Parsed from content |
| `topics` | TEXT[] | Canonical topic keys used for trend computation (MVP: copied from `RawEvent.tags` by Persister) |
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

### `discovery_candidates`

Tracks emerging unknown terms that may warrant addition to the allowlist.

Written by Trends when an unknown term exceeds volume/acceleration thresholds; reviewed by the operator.

| Column | Type | Description |
|--------|------|-------------|
| `term` | TEXT PK | Candidate term |
| `first_seen_at` | TIMESTAMPTZ | First observed |
| `last_seen_at` | TIMESTAMPTZ | Most recent observation |
| `volume_24h` | INT | Count in last 24h |
| `peak_acceleration` | FLOAT8 | Max acceleration observed |
| `sample_urls` | TEXT[] | Evidence URLs |
| `sample_event_ids` | TEXT[] | Evidence event IDs |
| `sources` | ENUM[] | Sources the term appeared in |
| `status` | ENUM | pending, added, ignored |
| `added_to_allowlist_at` | TIMESTAMPTZ? | When accepted |
| `ignored_at` | TIMESTAMPTZ? | When dismissed |
| `notes` | TEXT? | Operator notes |

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
- `discovery_candidates`: 30 days

## Write Responsibilities

Each table has a **clear write owner**. Most tables are single-writer; `consumer_lag` is a multi-writer table but is **partitioned by `consumer_group`** (each service only writes its own rows).

| Table | Owner Service | Writes | Reads |
|-------|--------------|--------|-------|
| `raw_events` | Persister | Insert only (immutable) | Grafana, Trends (for evidence) |
| `trend_snapshots` | Trends | Insert only (append) | Grafana |
| `brief_results` | Brief | Insert + idempotent upsert | Grafana |
| `consumer_lag` | Persister + Trends | Upsert (periodic, per `consumer_group`) | Trends (freshness check), Grafana |
| `discovery_candidates` | Trends | Insert + update status | Operator, Grafana |
| `retention_policies` | Seed script / Admin | Seed + manual edits | Retention job |

### Ownership Rules

1. **Single writer per keyspace**: Only one service writes to a given row keyspace. Most tables are single-writer; `consumer_lag` is shared but partitioned by `consumer_group`.

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

## Retention Enforcement

A **retention cleanup job** runs daily to enforce retention policies.

### Job Specification

**Schedule**: Daily at 03:00 UTC (low-traffic period)

**Implementation**: `packages/db/src/retention-job.ts`

```typescript
const CLEANUP_HANDLERS = {
  raw_events: (cutoff: Date) =>
    prisma.rawEvent.deleteMany({ where: { fetchedAt: { lt: cutoff } } }),
  trend_snapshots: (cutoff: Date) =>
    prisma.trendSnapshot.deleteMany({ where: { generatedAt: { lt: cutoff } } }),
  brief_results: (cutoff: Date) =>
    prisma.briefResult.deleteMany({ where: { producedAt: { lt: cutoff } } }),
  consumer_lag: (cutoff: Date) =>
    prisma.consumerLag.deleteMany({ where: { updatedAt: { lt: cutoff } } }),
  discovery_candidates: (cutoff: Date) =>
    prisma.discoveryCandidate.deleteMany({ where: { lastSeenAt: { lt: cutoff } } }),
} as const;

// Safety: only tables in CLEANUP_HANDLERS are eligible for deletion.
// Unknown policies are skipped with a warning (no arbitrary table deletes).
```

### Batch Deletion

For large tables (`raw_events`), consider deleting in batches to avoid long-running transactions and heavy lock contention.

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

MVP behavior is **logs only** (success/failure per table). If/when we run this as a long-lived service, add metrics for:

- `ri_retention_cleanup_rows_deleted_total{table=...}`
- `ri_retention_cleanup_duration_seconds{table=...}`
- `ri_retention_cleanup_errors_total{table=...}`

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
0 3 * * * cd /path/to/rising-intelligence && npm run retention:cleanup >> /var/log/retention.log 2>&1
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
