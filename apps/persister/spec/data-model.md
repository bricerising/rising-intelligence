# Data Model: Persister Service

## Overview

Persister is a Kafka consumer that materializes `RawEvent` messages to Postgres for querying.

## Contracts

- **Input**: `events.raw` Kafka topic (`RawEvent` protobuf)
- **Output**: `raw_events` Postgres table (see `specs/005-postgres-read-model.md`)

## Responsibilities

1. **Consume**: Read batches of `RawEvent` from Kafka
2. **Deserialize**: Parse protobuf messages
3. **Transform**: Normalize/map proto fields to Postgres columns
4. **Persist**: Batch insert to `raw_events` table
5. **Dedupe Cache**: Mark events as "seen" in Redis
6. **Commit**: Commit Kafka offsets after successful persist

## Idempotency

Persister MUST handle at-least-once delivery from Kafka:

- **Primary defense**: `ON CONFLICT (event_id) DO NOTHING` in Postgres
- **Secondary defense**: Redis `seen:{source}:{event_id}` cache for fast duplicate detection

### Postgres upsert strategy

```sql
INSERT INTO raw_events (
  event_id, source, fetched_at, published_at,
  url, title, text, author_id, author_handle, author_display_name,
  engagement_score, engagement_comments, engagement_likes, engagement_shares,
  lang, tags, extracted_hashtags, extracted_urls, source_meta
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
ON CONFLICT (event_id) DO NOTHING;
```

### Redis dedup cache

After successful Postgres write:

```
SET seen:{source}:{event_id} 1 EX 86400
```

TTL = 24 hours. This allows other services to quickly check if an event exists without querying Postgres.

## Field Mapping

| Proto Field | Postgres Column | Notes |
|-------------|-----------------|-------|
| `event_id` | `event_id` | Primary dedup key |
| `source` | `source` | Enum → TEXT |
| `fetched_at` | `fetched_at` | ISO8601 → TIMESTAMPTZ |
| `published_at` | `published_at` | Optional |
| `url` | `url` | Optional; normalized and tracking params stripped |
| `title` | `title` | Optional |
| `text` | `text` | Required |
| `author.id` | `author_id` | Denormalized |
| `author.handle` | `author_handle` | Denormalized |
| `author.display_name` | `author_display_name` | Denormalized |
| `engagement.score` | `engagement_score` | Optional |
| `engagement.comments` | `engagement_comments` | Optional |
| `engagement.likes` | `engagement_likes` | Optional |
| `engagement.shares` | `engagement_shares` | Optional |
| `lang` | `lang` | Optional; inferred when missing (`source_meta`/heuristic) |
| `tags` | `tags` | TEXT[]; inferred conservatively when empty |
| `extracted.hashtags` | `extracted_hashtags` | TEXT[] |
| `extracted.urls` | `extracted_urls` | TEXT[] |
| `source_meta_json` | `source_meta` | JSON string → JSONB, plus Persister `ri_quality` annotations |

## Ingest Quality Annotations

Persister appends `source_meta.ri_quality` metadata for downstream interpretation:

- URL quality (`normalized_url`, `invalid_url`, `url_issue_codes`)
- text quality (`low_information_text`, `text_issue_codes`)
- staleness (`stale_event`, `stale_age_hours`, `published_in_future`)
- inference markers (`inferred_topics`, `inferred_topic_count`, `inferred_lang`, `lang_inference_method`)

## Batching Strategy

To optimize throughput:

1. Accumulate events in memory buffer
2. Flush when:
   - Buffer reaches `BATCH_SIZE` (default: 100)
   - `BATCH_TIMEOUT_MS` elapsed since first event in buffer (default: 1000ms)
3. Use Postgres `COPY` or multi-row `INSERT` for efficiency
4. Commit Kafka offsets only after successful flush

## Error Handling

| Error Type | Action |
|------------|--------|
| Deserialization failure | Log error, skip event (don't block batch) |
| Postgres connection error | Retry with backoff, circuit breaker |
| Postgres constraint violation | Expected (duplicate), continue |
| Redis connection error | Fail readiness and pause processing until Redis recovers |

## Metrics

- `ri_persister_events_processed_total{source}`: Successfully persisted
- `ri_persister_events_skipped_total{reason}`: Skipped (duplicate, malformed)
- `ri_persister_batch_size`: Events per batch (histogram)
- `ri_persister_postgres_write_duration_seconds`: Insert latency
- `ri_persister_consumer_lag{partition}`: Messages behind latest

## Consumer Lag Tracking

Persister periodically writes its consumer lag to the `consumer_lag` Postgres table:

```sql
INSERT INTO consumer_lag (consumer_group, topic, partition, current_offset, latest_offset, lag_messages, updated_at)
VALUES ('persister', 'events.raw', $partition, $current, $latest, $lag, NOW())
ON CONFLICT (consumer_group, topic, partition)
DO UPDATE SET current_offset = $current, latest_offset = $latest, lag_messages = $lag, updated_at = NOW();
```

This enables the Trends service to check data freshness before generating briefs.
