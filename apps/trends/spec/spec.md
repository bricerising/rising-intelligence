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

## Topic Extraction

1. Load allowlist from `TOPICS_ALLOWLIST_PATH`
2. For each event, match against matchers (regex, keyword)
3. Return up to `max_topics_per_event` matches
4. Skip muted topics

```typescript
function extractTopics(event: RawEvent): string[] {
  const text = `${event.title ?? ''} ${event.text}`.toLowerCase();
  const matches: string[] = [];

  for (const topic of allowlist.topics) {
    if (allowlist.suppression.muted_topics.includes(topic.key)) continue;

    for (const matcher of topic.matchers) {
      if (matcher.type === 'keyword' && text.includes(matcher.value.toLowerCase())) {
        matches.push(topic.key);
        break;
      }
      if (matcher.type === 'regex' && new RegExp(matcher.pattern, 'i').test(text)) {
        matches.push(topic.key);
        break;
      }
    }

    if (matches.length >= allowlist.defaults.max_topics_per_event) break;
  }

  return matches;
}
```

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

# Scheduling
TZ=America/Los_Angeles
DAILY_BRIEF_LOCAL_TIME=17:00

# Freshness thresholds
MAX_LAG_MESSAGES=100
MAX_LAG_AGE_MS=300000

# Scoring weights (sum to 1.0)
WEIGHT_VOLUME=0.3
WEIGHT_ACCEL=0.5
WEIGHT_BASELINE=0.2
```
