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
- `GIN(to_tsvector(title || text))` — full-text search (future)

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

| Table | Writer | Notes |
|-------|--------|-------|
| `raw_events` | Persister service | Consumes `events.raw` from Kafka |
| `source_checkpoints` | (unused) | Collector uses local SQLite instead |
| `trend_snapshots` | Trends service | Periodic snapshot persistence |
| `brief_results` | Brief service | After LLM generation |
| `consumer_lag` | Trends service | Periodic update for freshness checks |
| `retention_policies` | Seed script / admin | Initial configuration |

**Note**: The `source_checkpoints` table is no longer used. The Collector service stores checkpoints in local SQLite to maintain zero database dependencies. This table can be removed in a future migration.

## Retention Enforcement

A scheduled job (cron or pg_cron extension) should run daily:

```sql
-- Example cleanup query for raw_events
DELETE FROM raw_events
WHERE fetched_at < NOW() - (
  SELECT retention_days * INTERVAL '1 day'
  FROM retention_policies
  WHERE table_name = 'raw_events' AND enabled = true
);
```

This can be implemented as:
- A simple script in `packages/db/src/cleanup.ts`
- A pg_cron job inside Postgres
- An external cron calling the cleanup script

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
