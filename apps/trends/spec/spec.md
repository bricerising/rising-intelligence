# Feature Specification: Trends Service

**Service**: `@rising-intelligence/trends`
**Created**: 2026-02-05
**Updated**: 2026-02-05
**Status**: Planned

## Overview

The Trends Service consumes `RawEvent` from `events.raw`, extracts topics, computes windowed metrics (volume + acceleration), and:

1. Publishes ranked `TrendSnapshot` messages to `trends.snapshots`
2. Persists snapshots to Postgres for dashboards
3. Triggers briefs when conditions are met (daily schedule or threshold)
4. Maintains consumer lag tracking for data freshness validation

## User Scenarios & Testing

### User Story 1 — Top trends snapshot (Priority: P1)

As an operator, I can see the Top N trending topics over a time window so I can quickly understand what is gaining traction.

**Independent Test**: Feed a synthetic spike for a topic and verify it appears in the next snapshot with high acceleration.

**Acceptance Scenarios**:

1. **Given** events flowing, **When** the service runs, **Then** it publishes snapshots every N minutes.
2. **Given** a topic's volume doubles window-over-window, **When** snapshots are produced, **Then** the topic ranks higher than stable-volume topics.
3. **Given** the service restarts, **When** it resumes, **Then** it continues producing snapshots without corrupting counts (idempotent processing).

### User Story 2 — Data freshness validation (Priority: P1)

As an operator, I want briefs to only be generated when data is fresh, so I don't receive misleading summaries.

**Independent Test**: Pause the consumer, attempt to trigger a brief, verify it's skipped with appropriate logging.

**Acceptance Scenarios**:

1. **Given** consumer lag < threshold, **When** daily brief time arrives, **Then** a `SummaryRequest` is published.
2. **Given** consumer lag > threshold, **When** daily brief time arrives, **Then** NO `SummaryRequest` is published AND a warning is logged.
3. **Given** lag records are stale (not updated recently), **When** brief trigger runs, **Then** it's treated as stale data.

### Edge Cases

- High-frequency topics dominate volume ("AI" always-on) → require baseline normalization.
- Alias collisions ("Bedrock" vs unrelated "bedrock") → require matcher tuning.
- Backlog/consumer lag → snapshots become stale.
- Clock skew in event timestamps → use fetched_at for window assignment.

## Constitution Requirements

- **Determinism**: given the same event stream + allowlist, computed snapshots are stable.
- **Idempotency**: safe under at-least-once delivery; duplicates do not inflate long-term results.
- **Evidence**: snapshots include evidence references (event IDs / URLs) for traceability.
- **Freshness**: briefs are only triggered when data is sufficiently fresh.

## Requirements

### Functional Requirements

- **FR-001**: Service MUST consume `events.raw` from Kafka.
- **FR-002**: Service MUST compute `15m` and `60m` windows in MVP.
- **FR-003**: Service MUST publish `TrendSnapshot` to `trends.snapshots` (Kafka).
- **FR-004**: Service MUST persist snapshots to `trend_snapshots` (Postgres).
- **FR-005**: Service MUST support an allowlist + aliases for topic extraction.
- **FR-006**: Service SHOULD compute baselines (7-day) once enough data exists.
- **FR-007 (Daily brief trigger)**: Service MUST publish a daily `SummaryRequest` to `summary.requests` on a configured local schedule, **only if data freshness check passes**.
- **FR-008 (Threshold trigger, optional)**: Service SHOULD publish a threshold-triggered `SummaryRequest` when a topic spike crosses configured thresholds, **only if data freshness check passes**.
- **FR-009 (Consumer lag tracking)**: Service MUST periodically update `consumer_lag` table in Postgres.
- **FR-010 (Window state)**: Service MUST maintain window state in Redis for fast aggregation.

### Non-Functional Requirements

- **NFR-001**: Snapshot cadence SHOULD be <= 5 minutes.
- **NFR-002**: Processing MUST not fall behind indefinitely; consumer lag is observable.
- **NFR-003**: Service MUST continue operating if Redis is unavailable (with degraded performance).

## Window State Management

Window state is maintained in Redis for speed (see `specs/006`):

### Key Structures

| Key Pattern | Purpose | TTL |
|-------------|---------|-----|
| `window:{window}:{topic}:{bucket}` | Event count for topic in bucket | 3 × window |
| `prev:{window}:{topic}` | Previous window count | 2 × window |
| `evidence:{window}:{topic}` | Sorted set of top event IDs | 2 × window |
| `baseline:{window}:{topic}:{dow}` | 7-day baseline cache | 24h |
| `dedup:{window}:{bucket}` | Set of processed event_ids | 3 × window |

### Event Deduplication

To prevent duplicate events from inflating window counts, the service tracks processed `event_id`s per window bucket:

```typescript
async function processEvent(event: RawEvent): Promise<void> {
  const topics = getTopics(event);
  const bucket = getBucket(event.fetched_at, '60m');
  const dedupKey = `dedup:60m:${bucket}`;

  // Check if already processed in this window
  const isNew = await redis.sadd(dedupKey, event.event_id);
  if (isNew === 0) {
    // Duplicate - skip counting but don't error
    metrics.increment('trends_duplicates_skipped_total');
    return;
  }

  // Set TTL on first add (3 × window = 3 hours for 60m window)
  await redis.expire(dedupKey, 3 * 60 * 60);

  // Count for each topic
  for (const topic of topics) {
    await incrementTopicCount(topic, bucket);
    await updateEvidence(topic, event);
  }
}
```

**Why dedup at this layer?**
- Kafka delivers at-least-once (duplicates are expected)
- Consumer restarts replay from last committed offset
- Without dedup, replayed events inflate counts
- Dedup is cheap (Redis SADD is O(1))

**Recovery**: After restart, the dedup set may be incomplete. The first window after restart may have slightly inflated counts (from events counted before restart + replayed). This stabilizes after one window period.

### Window Alignment

Buckets align to clock time using **event time** (`fetched_at`):

- 15m: :00, :15, :30, :45
- 60m: :00
- 24h: midnight (configured TZ)

### Recovery on Restart

1. Consumer replays from last committed Kafka offset
2. Counts may temporarily inflate (duplicates counted)
3. After one window period, counts stabilize
4. Acceptable for MVP; can add dedup via event_id tracking later

## Data Freshness Check

Before triggering any brief:

```typescript
async function isDataFresh(): Promise<boolean> {
  const lagRecords = await prisma.consumerLag.findMany({
    where: { consumerGroup: 'trends-processor' },
  });

  // Check 1: Records exist and are recent
  const maxAge = Date.now() - config.MAX_LAG_AGE_MS; // default: 5 min
  const stale = lagRecords.some(r => r.updatedAt.getTime() < maxAge);
  if (stale) {
    log.warn('Consumer lag records are stale');
    return false;
  }

  // Check 2: Total lag is acceptable
  const totalLag = lagRecords.reduce((sum, r) => sum + Number(r.lagMessages), 0n);
  if (totalLag > config.MAX_LAG_MESSAGES) { // default: 100
    log.warn({ totalLag }, 'Consumer lag exceeds threshold');
    return false;
  }

  return true;
}
```

### Lag Tracking

The service periodically (every 30s) updates `consumer_lag`:

```typescript
async function updateLagTracking(consumer: Consumer) {
  const admin = kafka.admin();
  const offsets = await admin.fetchOffsets({ groupId: 'trends-processor' });
  const topicOffsets = await admin.fetchTopicOffsets('events.raw');

  for (const partition of offsets) {
    const latest = topicOffsets.find(t => t.partition === partition.partition);
    const lag = BigInt(latest.offset) - BigInt(partition.offset);

    await prisma.consumerLag.upsert({
      where: {
        consumerGroup_topic_partition: {
          consumerGroup: 'trends-processor',
          topic: 'events.raw',
          partition: partition.partition,
        },
      },
      update: {
        currentOffset: BigInt(partition.offset),
        latestOffset: BigInt(latest.offset),
        lagMessages: lag,
      },
      create: { ... },
    });
  }
}
```

## Topic Handling

Topics are **pre-extracted by the Collector** before events reach Kafka. The Trends service reads topics from `RawEvent.tags` directly:

```typescript
function getTopics(event: RawEvent): string[] {
  // Topics are pre-extracted by Collector
  return event.tags ?? [];
}
```

**Why pre-extraction?**
- Avoids duplicating regex logic across services
- Ensures consistent topic assignment for the same event
- Allows Persister to write topics to Postgres without extraction logic

The Trends service MAY validate that topics match the allowlist (for filtering muted topics) but does NOT re-extract.

## Baseline Computation

Baselines provide historical context for trend scoring. Without baselines, a topic that's "always busy" (like "Python") would score the same as a sudden spike.

### Data Source

Baselines are computed from the `trend_snapshots` table in Postgres:

```sql
SELECT
  topic,
  window,
  EXTRACT(DOW FROM generated_at) as day_of_week,
  EXTRACT(HOUR FROM generated_at) as hour,
  AVG(volume) as avg_volume,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY volume) as median_volume
FROM trend_snapshots,
  jsonb_array_elements(snapshot->'topics') as t(topic_data)
WHERE generated_at > NOW() - INTERVAL '7 days'
GROUP BY topic, window, day_of_week, hour;
```

### Computation Schedule

Baselines are recomputed **daily at midnight UTC**:

1. Query last 7 days of snapshots from Postgres
2. Aggregate by topic + window + day-of-week + hour
3. Compute mean and median volume
4. Cache results in Redis with 24h TTL

### Redis Caching

```typescript
async function computeAndCacheBaselines(): Promise<void> {
  const baselines = await queryBaselineAggregates();

  const pipeline = redis.pipeline();
  for (const b of baselines) {
    const key = `baseline:${b.window}:${b.topic}:${b.dayOfWeek}:${b.hour}`;
    pipeline.set(key, JSON.stringify({
      mean: b.avgVolume,
      median: b.medianVolume,
    }), 'EX', 86400); // 24h TTL
  }
  await pipeline.exec();

  log.info({ count: baselines.length }, 'Baselines cached');
}
```

### Baseline Lookup

When scoring a topic, fetch its baseline from Redis:

```typescript
async function getBaseline(
  topic: string,
  window: string,
  timestamp: Date
): Promise<number> {
  const dow = timestamp.getUTCDay(); // 0 = Sunday
  const hour = timestamp.getUTCHours();
  const key = `baseline:${window}:${topic}:${dow}:${hour}`;

  const cached = await redis.get(key);
  if (cached) {
    const { median } = JSON.parse(cached);
    return median;
  }

  // Fallback: no baseline data yet (first week)
  return 0;
}
```

### Cold Start Behavior

During the first 7 days (no historical data):
- `baseline_volume` = 0 for all topics
- `baseline_delta` = 0 (neutral contribution to score)
- Scoring relies on volume + acceleration only

After 7 days, baselines become meaningful and the full scoring formula applies.

## Scoring Algorithm

```typescript
function computeScore(metrics: TopicMetrics): number {
  const { volume, prev_volume, baseline_volume } = metrics;

  // Acceleration: how fast is it growing?
  const accel = prev_volume > 0
    ? (volume - prev_volume) / prev_volume
    : volume > 0 ? 1 : 0;

  // Baseline delta: is it unusual compared to historical?
  const baselineDelta = baseline_volume > 0
    ? (volume - baseline_volume) / baseline_volume
    : 0;

  // Normalize to 0-1 using sigmoid
  const normVolume = sigmoid(volume, config.VOLUME_MIDPOINT);
  const normAccel = sigmoid(accel, config.ACCEL_MIDPOINT);
  const normBaseline = sigmoid(baselineDelta, config.BASELINE_MIDPOINT);

  // Weighted combination
  const raw = (
    config.WEIGHT_VOLUME * normVolume +
    config.WEIGHT_ACCEL * normAccel +
    config.WEIGHT_BASELINE * normBaseline
  );

  return Math.round(raw * 100) / 10; // 0.0 - 10.0
}
```

## Success Criteria

- **SC-001**: Top trends are plausible and evidence-backed.
- **SC-002**: Lag stays under a configured threshold in local dev.
- **SC-003**: Briefs are never generated when data is stale.
- **SC-004**: Window counts stabilize after restart within one window period.

## Configuration

```
# Required
KAFKA_BROKERS=localhost:9092
DATABASE_URL=postgresql://user:pass@localhost:5432/rising_intelligence
REDIS_URL=redis://localhost:6379
TOPICS_ALLOWLIST_PATH=/config/topics.allowlist.yaml

# Scheduling (UTC recommended - see Timezone Handling)
DAILY_BRIEF_CRON=0 1 * * *  # 1:00 UTC daily
SNAPSHOT_INTERVAL_SECONDS=300

# Freshness thresholds
MAX_LAG_MESSAGES=100
MAX_LAG_AGE_MS=300000

# Scoring weights (sum to 1.0)
WEIGHT_VOLUME=0.3
WEIGHT_ACCEL=0.5
WEIGHT_BASELINE=0.2
```

## Timezone Handling

**All internal timestamps use UTC.** Local time is only used for display purposes.

- Window buckets align to UTC clock time (e.g., 14:00 UTC, 14:15 UTC)
- `fetched_at` from events is expected to be UTC ISO8601
- Daily brief triggers at a fixed UTC time (configured via cron)
- Grafana dashboards handle timezone conversion for display

**Why UTC?**
- Avoids DST-related bugs (missed or duplicate briefs)
- Deterministic window alignment across restarts
- Simpler baseline comparison (same UTC hour across days)

## Health Check

The Trends service exposes a `/health` endpoint for container orchestration:

```typescript
interface HealthStatus {
  status: 'healthy' | 'degraded' | 'unhealthy';
  checks: {
    kafka: 'ok' | 'error';
    postgres: 'ok' | 'error';
    redis: 'ok' | 'error';
  };
  consumer_lag: number;
  last_snapshot_at?: string;
  uptime_seconds: number;
}
```

**Health criteria**:
- `healthy`: All dependencies reachable; consumer lag < 1000; snapshot within last 10 minutes
- `degraded`: Redis unavailable OR lag > 1000 OR stale snapshots
- `unhealthy`: Kafka OR Postgres unreachable

**Endpoint**: `GET /health` returns 200 (healthy/degraded) or 503 (unhealthy)

## Graceful Shutdown

On SIGTERM/SIGINT, the Trends service:

1. Stops consuming new messages
2. Completes current window computation (if in progress)
3. Publishes final snapshot (if window is closing)
4. Commits Kafka offsets
5. Flushes Redis pipeline
6. Closes connections
7. Exits with code 0

```typescript
process.on('SIGTERM', async () => {
  log.info('Received SIGTERM, initiating graceful shutdown');

  // Stop consuming
  await consumer.pause([{ topic: 'events.raw' }]);

  // Complete in-progress window (max 30s)
  if (windowComputationInProgress) {
    await Promise.race([
      completeWindowComputation(),
      sleep(30_000),
    ]);
  }

  // Commit offsets
  await consumer.commitOffsets();

  // Close connections
  await Promise.all([
    consumer.disconnect(),
    producer.disconnect(),
    prisma.$disconnect(),
    redis.quit(),
  ]);

  log.info('Graceful shutdown complete');
  process.exit(0);
});
```
