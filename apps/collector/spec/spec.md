# Feature Specification: Collector Service

**Service**: `@rising-intelligence/collector`
**Created**: 2026-02-05
**Updated**: 2026-02-05
**Status**: Planned

## Overview

The Collector Service ingests external sources (RSS/blogs, Hacker News, Reddit, etc.), normalizes all items into the canonical `RawEvent` contract, and **publishes to Kafka only**.

The collector is intentionally simple:
- **Input**: External APIs (RSS, HN, Reddit, etc.)
- **Output**: Kafka (`events.raw`)
- **No database dependencies**: Does not write to Postgres or Redis directly

Downstream consumers (Persister, Trends) handle materialization to storage. This keeps ingestion fast, simple, and decoupled.

## Design Principles

### Kafka as the Driver

The collector's only job is to get data into Kafka. All derived state (Postgres, Redis) is handled by Kafka consumers downstream. Benefits:

- **Single responsibility**: Collector only ingests
- **No coupling**: External API issues don't affect database writes
- **Replay-friendly**: Can rebuild all state from Kafka

### Checkpoint-Only Deduplication (Option B)

The collector uses **source checkpoints** to avoid re-fetching old data, but accepts that some duplicates may enter Kafka:

- On each poll, fetch items newer than the checkpoint
- Publish to Kafka
- Update checkpoint
- If duplicates slip through (e.g., after restart), downstream consumers handle it

This is simpler than maintaining a Redis dedup cache in the collector, and aligns with Kafka's at-least-once semantics.

## User Scenarios & Testing

### User Story 1 — Continuous ingestion (Priority: P1)

As an operator, I can run the collector continuously so new items from configured sources appear in Kafka quickly and reliably.

**Independent Test**: Start the stack and verify new RSS items and HN stories appear on `events.raw` within 60 seconds.

**Acceptance Scenarios**:

1. **Given** valid source configuration, **When** the collector runs for 30 minutes, **Then** `events.raw` receives valid `RawEvent` messages for each enabled source.
2. **Given** a transient 429/5xx from a source, **When** the collector retries, **Then** it backs off with jitter and resumes without crashing.
3. **Given** the collector restarts, **When** it resumes, **Then** it continues from the last checkpoint with minimal duplicate events.

### User Story 2 — Checkpoint persistence (Priority: P1)

As an operator, I can restart the collector without re-processing large amounts of data.

**Independent Test**: Stop collector, restart, verify it resumes from checkpoint.

**Acceptance Scenarios**:

1. **Given** the collector has processed 100 items, **When** I check checkpoints, **Then** I see a checkpoint for each active source.
2. **Given** a checkpoint exists, **When** the collector restarts, **Then** it fetches only items newer than the checkpoint.

### Edge Cases

- Source clock skew (published timestamps inconsistent).
- RSS feeds with malformed dates or missing GUIDs.
- Reddit API rate limiting and "after" cursor drift.
- Duplicate URLs across sources (same article syndicated) — handled by downstream dedup.

## Constitution Requirements

- **Schema validation**: every emitted message MUST conform to `RawEvent`.
- **Idempotency**: emitted `event_id` MUST be stable and source-derived.
- **Backoff**: transient upstream failures MUST not cascade into tight retry loops.
- **No secrets**: credentials MUST never be logged or emitted to Kafka.
- **Kafka-only**: Collector MUST NOT write directly to Postgres or Redis.

## Requirements

### Functional Requirements

- **FR-001**: Service MUST support ingesting from RSS/Atom feeds.
- **FR-002**: Service MUST support ingesting from Hacker News (poll API).
- **FR-003**: Service MUST support ingesting from Reddit (poll new posts).
- **FR-004**: Service MUST publish normalized events to `events.raw` (Kafka).
- **FR-005**: Service MUST emit parse/normalize failures to `events.raw.dlq` (`DeadLetterEvent`).
- **FR-006**: Service MUST persist checkpoints to local storage (file or embedded DB) for restart recovery.
- **FR-007**: Service MUST implement exponential backoff with jitter for rate limits and errors.

### Non-Functional Requirements

- **NFR-001**: Ingest loop SHOULD make new items available within 60 seconds (best-effort).
- **NFR-002**: Service MUST tolerate upstream downtime without crashing.
- **NFR-003**: Service MUST have zero database dependencies (no Postgres, no Redis).
- **NFR-004**: Service MUST be stateless except for checkpoints (can run multiple instances with partitioned sources).

## Data Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ RSS Adapter │     │  HN Adapter │     │Reddit Adapt.│
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           ▼
                   ┌───────────────┐
                   │  Normalizer   │
                   │  + Validator  │
                   └───────┬───────┘
                           │
                           ▼
                   ┌───────────────┐
                   │    Kafka      │
                   │  events.raw   │
                   └───────────────┘
```

**No Postgres. No Redis. Just Kafka.**

## Checkpoint Strategy

Each source adapter maintains its own checkpoint in **local storage** (file-based or embedded SQLite):

| Source | Checkpoint Key | Value Example |
|--------|---------------|---------------|
| RSS | `rss.{feed_id}.last_guid` | `https://aws.amazon.com/blogs/...` |
| RSS | `rss.{feed_id}.last_published_at` | `2026-02-05T14:30:00Z` |
| Hacker News | `hackernews.last_max_id` | `39876543` |
| Reddit | `reddit.{subreddit}.after_cursor` | `t3_abc123` |
| GitHub | `github.{repo}.last_release_id` | `12345678` |

**Storage options** (in order of preference):
1. **SQLite file** (`checkpoints.db`): Simple, durable, queryable
2. **JSON file** (`checkpoints.json`): Simpler, but less safe on crash
3. **In-memory with periodic flush**: Fast, but loses state on crash

**Checkpoint update timing**: After successfully publishing a batch to Kafka, update the checkpoint. On restart, some events may be re-published (at-least-once), but downstream consumers handle dedup.

## Rate Limit Handling

Track rate limits **in memory** (no Redis needed):

```typescript
interface RateLimitState {
  source: string;
  windowStart: Date;
  requestsUsed: number;
  requestsLimit: number;
  backoffUntil?: Date;
}
```

**Backoff strategy**:
- On 429: exponential backoff starting at 30s, max 15m, with jitter
- On 5xx: exponential backoff starting at 5s, max 5m, with jitter
- On network error: exponential backoff starting at 5s, max 5m, with jitter

Rate limit state is lost on restart, which is fine — it recovers quickly.

## Success Criteria

- **SC-001**: 0 unhandled crashes in 24 hours of local soak.
- **SC-002**: `events.raw` shows steady flow and stable schema across sources.
- **SC-003**: Checkpoint advances monotonically; restart resumes from checkpoint.
- **SC-004**: Rate limits are respected; minimal 429 responses after initial backoff.
- **SC-005**: Service has zero Postgres/Redis dependencies.

## Assumptions

- Kafka/Redpanda is reachable from the service network.
- Source credentials (if needed) are provided via env vars.
- Local filesystem is available for checkpoint storage.

## Configuration

```
# Required
KAFKA_BROKERS=localhost:9092

# Checkpoint storage
CHECKPOINT_PATH=/data/checkpoints.db

# Source configuration
RSS_FEED_URLS=https://aws.amazon.com/blogs/aws/feed/,...
REDDIT_SUBREDDITS=aws,MachineLearning,technology
REDDIT_CLIENT_ID=...
REDDIT_CLIENT_SECRET=...
HN_MODE=top
HN_POLL_INTERVAL_SECONDS=300

# Optional
GITHUB_TRACKED_REPOS=vercel/next.js,openai/openai-python
GITHUB_TOKEN=...
```

Note: No `DATABASE_URL` or `REDIS_URL` — the collector doesn't need them.
