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

### Topic Extraction at Ingestion

The Collector extracts topics from event content **before** publishing to Kafka. This ensures:

- `RawEvent.tags` field is populated in the wire contract
- Downstream consumers (Persister, Trends) receive pre-enriched events
- Topic extraction logic is centralized (not duplicated across services)

Topic extraction uses the **same allowlist** (`TOPICS_ALLOWLIST_PATH`) as the Trends service, with pre-compiled regexes for performance.

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
- **FR-003a**: Service MUST support ingesting from Bluesky (poll or firehose). See `social-adapters.md`.
- **FR-003b**: Service MUST support ingesting from Mastodon (poll public/tag timelines). See `social-adapters.md`.
- **FR-004**: Service MUST publish normalized events to `events.raw` (Kafka).
- **FR-005**: Service MUST emit parse/normalize failures to `events.raw.dlq` (`DeadLetterEvent`).
- **FR-006**: Service MUST persist checkpoints to local storage (file or embedded DB) for restart recovery.
- **FR-007**: Service MUST implement exponential backoff with jitter for rate limits and errors.
- **FR-008**: Service MUST extract topics from event content using the allowlist before publishing.
- **FR-009**: Service MUST validate the allowlist on startup and fail loudly if regexes are invalid.
- **FR-010**: Service MUST pre-compile all regex matchers on startup for performance.

### Non-Functional Requirements

- **NFR-001**: Ingest loop SHOULD make new items available within 60 seconds (best-effort).
- **NFR-002**: Service MUST tolerate upstream downtime without crashing.
- **NFR-003**: Service MUST have zero database dependencies (no Postgres, no Redis).
- **NFR-004**: Service MUST be stateless except for checkpoints (can run multiple instances with partitioned sources).

## Data Flow

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ RSS Adapter │  │  HN Adapter │  │Reddit Adapt.│  │Bluesky Adpt.│  │Mastodon Apt.│
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │                │                │
       └────────────────┴────────────────┼────────────────┴────────────────┘
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

See `social-adapters.md` for Bluesky and Mastodon implementation details.

## Topic Extraction

The Collector extracts topics at ingestion time to ensure `RawEvent.tags` is populated before events reach Kafka.

### Allowlist Loading and Validation

On startup, the Collector:

1. Loads the allowlist from `TOPICS_ALLOWLIST_PATH`
2. Validates the YAML structure
3. Pre-compiles all regex matchers
4. Fails with a clear error if any regex is invalid

```typescript
interface CompiledAllowlist {
  topics: Array<{
    key: string;
    displayName: string;
    priority: number;  // Higher = more important (1-100)
    matchers: Array<{
      type: 'keyword' | 'regex';
      value?: string;      // for keyword
      pattern?: RegExp;    // pre-compiled for regex
    }>;
  }>;
  maxTopicsPerEvent: number;
  defaultPriority: number;
  mutedTopics: Set<string>;
}

function loadAllowlist(path: string): CompiledAllowlist {
  const raw = yaml.parse(fs.readFileSync(path, 'utf-8'));
  const defaultPriority = raw.defaults?.default_priority ?? 50;

  const topics = raw.topics.map((t: any) => ({
    key: t.key,
    displayName: t.display_name,
    priority: t.priority ?? defaultPriority,
    matchers: t.matchers.map((m: any) => {
      if (m.type === 'regex') {
        try {
          return { type: 'regex', pattern: new RegExp(m.pattern, 'i') };
        } catch (e) {
          throw new Error(`Invalid regex for topic ${t.key}: ${m.pattern}`);
        }
      }
      return { type: 'keyword', value: m.value.toLowerCase() };
    }),
  }));

  return {
    topics,
    maxTopicsPerEvent: raw.defaults?.max_topics_per_event ?? 5,
    defaultPriority,
    mutedTopics: new Set(raw.suppression?.muted_topics ?? []),
  };
}
```

### Extraction Logic

Topic extraction is **deterministic**: given the same content and allowlist, extraction always returns the same topics in the same order.

**Algorithm**:
1. Find ALL matching topics (no early exit)
2. Sort by priority (descending), then by key (alphabetically) for ties
3. Take top N (where N = `maxTopicsPerEvent`)

```typescript
interface TopicMatch {
  key: string;
  priority: number;
}

function extractTopics(
  event: { title?: string; text: string },
  allowlist: CompiledAllowlist
): string[] {
  const content = `${event.title ?? ''} ${event.text}`.toLowerCase();
  const matches: TopicMatch[] = [];

  // Step 1: Find ALL matching topics
  for (const topic of allowlist.topics) {
    if (allowlist.mutedTopics.has(topic.key)) continue;

    for (const matcher of topic.matchers) {
      const matched = matcher.type === 'keyword'
        ? content.includes(matcher.value!)
        : matcher.pattern!.test(content);

      if (matched) {
        matches.push({ key: topic.key, priority: topic.priority });
        break; // Only add topic once per topic
      }
    }
  }

  // Step 2: Sort by priority (desc), then key (asc) for determinism
  matches.sort((a, b) => {
    if (b.priority !== a.priority) return b.priority - a.priority;
    return a.key.localeCompare(b.key);
  });

  // Step 3: Take top N
  return matches
    .slice(0, allowlist.maxTopicsPerEvent)
    .map(m => m.key);
}
```

### Performance Considerations

- Regexes are pre-compiled once at startup (not per-event)
- Early exit when `maxTopicsPerEvent` is reached
- Muted topics are skipped entirely
- Keyword matching is faster than regex; order matchers accordingly

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

### Environment Variables

```bash
# Required
KAFKA_BROKERS=localhost:9092

# Checkpoint storage
CHECKPOINT_PATH=/data/checkpoints.db

# Topics allowlist (for topic extraction)
TOPICS_ALLOWLIST_PATH=/config/topics.allowlist.yaml

# Feeds configuration (curated RSS sources)
FEEDS_CONFIG_PATH=/config/feeds.yaml

# Hacker News
HN_ENABLED=true
HN_MODE=top
HN_POLL_INTERVAL_SECONDS=300

# Lobsters (high-signal programming community)
LOBSTERS_ENABLED=true
LOBSTERS_POLL_INTERVAL_SECONDS=1800

# Reddit credentials
REDDIT_ENABLED=true
REDDIT_CLIENT_ID=...
REDDIT_CLIENT_SECRET=...

# Social sources (see social-adapters.md for details)
BLUESKY_ENABLED=true
BLUESKY_MODE=polling
BLUESKY_POLL_INTERVAL_SECONDS=300
BLUESKY_QUERIES=aws,bedrock,ai,llm,typescript,rust,openai,anthropic

MASTODON_ENABLED=true
MASTODON_POLL_INTERVAL_SECONDS=600
MASTODON_INSTANCES=hachyderm.io,fosstodon.org,infosec.exchange
MASTODON_TAGS=aws,ai,machinelearning,typescript,rust,devops

# GitHub (optional - for releases tracking)
GITHUB_ENABLED=true
GITHUB_TOKEN=...
```

### Feeds Configuration File

See `infra/config/feeds.yaml` for the curated list of RSS feeds including:
- **Official Blogs**: AWS, Google Cloud, Azure, GitHub, Cloudflare
- **AI Research**: Google Research, OpenAI, DeepMind
- **Aggregators**: Techmeme, InfoQ
- **Open Source**: GitHub Trending, GitHub Releases

Each feed has configurable poll intervals and priority levels.

Note: No `DATABASE_URL` or `REDIS_URL` — the collector doesn't need them.

## Observability Endpoints

The Collector exposes HTTP endpoints for health checks and metrics:

### Health Check (`GET /health`)

```typescript
interface HealthStatus {
  status: 'healthy' | 'degraded' | 'unhealthy';
  checks: {
    kafka: 'ok' | 'error';
    checkpoints: 'ok' | 'error';
    allowlist: 'ok' | 'error';
  };
  uptime_seconds: number;
  last_event_at?: string; // ISO8601
}
```

**Health criteria**:
- `healthy`: Kafka reachable, checkpoints writable, allowlist loaded
- `degraded`: One source failing but others working
- `unhealthy`: Kafka unreachable OR checkpoints unwritable

**Returns**: 200 (healthy/degraded) or 503 (unhealthy)

### Metrics (`GET /metrics`)

Prometheus-format metrics for direct scraping (independent of Kafka):

```prometheus
# HELP ri_collector_events_published_total Events published to Kafka
# TYPE ri_collector_events_published_total counter
ri_collector_events_published_total{source="reddit"} 1234

# HELP ri_collector_poll_duration_seconds Duration of poll cycle
# TYPE ri_collector_poll_duration_seconds histogram
ri_collector_poll_duration_seconds_bucket{source="reddit",le="1"} 95

# HELP ri_collector_errors_total Errors by source and type
# TYPE ri_collector_errors_total counter
ri_collector_errors_total{source="reddit",type="rate_limit"} 2

# HELP ri_collector_last_poll_timestamp_seconds Unix timestamp of last successful poll
# TYPE ri_collector_last_poll_timestamp_seconds gauge
ri_collector_last_poll_timestamp_seconds{source="reddit"} 1707177600

# HELP ri_collector_checkpoints_written_total Checkpoint writes
# TYPE ri_collector_checkpoints_written_total counter
ri_collector_checkpoints_written_total{source="reddit"} 500
```

**Why both /health and /metrics?**
- `/health`: Quick liveness/readiness check for container orchestration
- `/metrics`: Detailed operational visibility, works even if Kafka is down

**Scrape config** (add to Prometheus/OTel):
```yaml
scrape_configs:
  - job_name: 'collector'
    static_configs:
      - targets: ['collector:3000']
    metrics_path: /metrics
    scrape_interval: 15s
```

## Graceful Shutdown

On SIGTERM/SIGINT, the Collector:

1. Stops accepting new poll cycles
2. Completes the current batch (with 30s timeout)
3. Flushes pending Kafka messages
4. Persists current checkpoints
5. Closes connections
6. Exits with code 0

```typescript
process.on('SIGTERM', async () => {
  log.info('Received SIGTERM, initiating graceful shutdown');

  // Stop polling
  stopPolling();

  // Wait for in-flight batches (max 30s)
  await Promise.race([
    waitForInflightBatches(),
    sleep(30_000),
  ]);

  // Flush Kafka producer
  await producer.flush({ timeout: 10_000 });

  // Persist checkpoints
  await checkpointStore.flush();

  // Close connections
  await producer.disconnect();

  log.info('Graceful shutdown complete');
  process.exit(0);
});
```

## Volume Mount Warning

**CRITICAL**: The checkpoint database (`CHECKPOINT_PATH`) MUST be mounted as a persistent volume in Docker/Kubernetes deployments.

```yaml
# docker-compose.yml
collector:
  volumes:
    - collector-checkpoints:/data  # REQUIRED for checkpoint persistence
  environment:
    - CHECKPOINT_PATH=/data/checkpoints.db
```

**If the volume is not mounted**:
- Checkpoints are lost on container restart
- Collector re-ingests ALL data from the beginning
- Duplicate events flood Kafka and downstream services

Verify the volume is mounted correctly:
```bash
docker compose exec collector ls -la /data/checkpoints.db
```
