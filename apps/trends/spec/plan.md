# Implementation Plan: Trends Service

## Overview

Build `apps/trends` as a pipeline-transport consumer/producer + periodic snapshot publisher with Redis-backed window state.

## Architecture (High Level)

- Input: `events.raw` (`RawEvent`) from Kafka
- Output:
  - `trends.snapshots` (Kafka)
  - `trend_snapshots` (Postgres)
- State:
  - Window buckets in Redis (`window:*`, `prev:*`, `evidence:*`)
  - Baseline cache in Redis (`baseline:*`)
  - Consumer lag tracking in Postgres (`consumer_lag`)
  - Snapshots in Postgres (`trend_snapshots`)

## Dependencies

```json
{
  "@rising-intelligence/db": "workspace:*",
  "@rising-intelligence/pipeline": "workspace:*",
  "@rising-intelligence/shared": "workspace:*",
  "ioredis": "^5.x",
  "yaml": "^2.x",
  "cron": "^3.x"
}
```

## Phases

### Phase 1: Topic keys + windowed aggregation

- Kafka consumer setup
- Allowlist loader + tag filtering (`RawEvent.tags`)
- Windowed counters in Redis (15m/60m)
- Snapshot publishing to Kafka + Postgres
- Basic scoring (volume only)

**Deliverables**:
- `src/index.ts` - service entry point
- `src/config.ts` - environment config
- `src/runtime-factory.ts` - consumer/producer connection wiring
- `src/process.ts` - batch strategies for raw events and heartbeats
- `src/allowlist.ts` - allowlist loader
- `src/extractor.ts` - topic key filtering (from `RawEvent.tags`)
- `src/redis/windows.ts` - window state management
- `src/snapshot.ts` - snapshot computation + publishing
- `src/db/snapshots.ts` - Postgres persistence

### Phase 2: Durable state + baselines + data freshness

- Previous window tracking for acceleration
- Baseline computation from Postgres history
- Consumer lag tracking
- Data freshness validation
- Evidence selection per topic

**Deliverables**:
- `src/redis/baselines.ts` - baseline cache
- `src/redis/evidence.ts` - evidence buffer (sorted set)
- `src/freshness.ts` - data freshness check
- `src/lag.ts` - consumer lag tracking
- `src/scoring.ts` - full scoring algorithm

### Phase 3: Request-driven brief compatibility + observability

- Keep Trends as the ranking source of truth (`trend_snapshots`)
- Do not auto-publish `summary.requests`
- Metrics + traces + health endpoints

**Deliverables**:
- `src/health.ts` - `/healthz` and `/readyz`
- Grafana dashboard for trends metrics

## Key Implementation Details

### Consumer Loop

```typescript
import {
  createConsumerConnection,
  createMessageBatchStrategy,
} from "@rising-intelligence/pipeline/transport";

async function run() {
  const consumer = await createConsumerConnection({
    brokers: config.KAFKA_BROKERS,
    clientId: config.KAFKA_CLIENT_ID,
    groupId: config.KAFKA_CONSUMER_GROUP,
    logger: log,
  });

  // Start background jobs
  startSnapshotPublisher();

  await consumer.consume({
    topics: [config.KAFKA_TOPIC_RAW_EVENTS, config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT],
    ctx,
    strategy: createBatchStrategies(ctx),
    fromBeginning: false,
  });
}
```

### Event Processing

```typescript
async function processEvent(event: RawEvent) {
  const topics = filterTopics(event.tags ?? [], allowlist);
  if (topics.length === 0) return;

  const bucket = getBucket(event.fetched_at, '60m');

  for (const topic of topics) {
    // Increment window counter
    await redis.incr(`window:60m:${topic}:${bucket}`);
    await redis.expire(`window:60m:${topic}:${bucket}`, 3 * 60 * 60); // 3h

    // Add to evidence buffer (sorted by engagement)
    const score = event.engagement?.score ?? 0;
    await redis.zadd(`evidence:60m:${topic}`, score, event.event_id);
    await redis.zremrangebyrank(`evidence:60m:${topic}`, 0, -11); // Keep top 10
  }
}
```

### Snapshot Publishing

```typescript
// Runs every 5 minutes
async function publishSnapshot(window: '15m' | '60m') {
  const now = new Date();
  const currentBucket = getBucket(now, window);
  const prevBucket = getPrevBucket(now, window);

  const topics: TopicMetrics[] = [];

  for (const topicConfig of allowlist.topics) {
    const topic = topicConfig.key;

    // Get current and previous volumes
    const volume = await redis.get(`window:${window}:${topic}:${currentBucket}`) ?? 0;
    const prevVolume = await redis.get(`prev:${window}:${topic}`) ?? 0;
    const baseline = await getBaseline(topic, window);

    // Get top evidence
    const evidenceIds = await redis.zrevrange(`evidence:${window}:${topic}`, 0, 9);

    const metrics: TopicMetrics = {
      topic,
      window,
      window_end: currentBucket,
      volume: Number(volume),
      prev_volume: Number(prevVolume),
      baseline_volume: baseline,
      acceleration: computeAcceleration(volume, prevVolume),
      baseline_delta: computeBaselineDelta(volume, baseline),
      score: 0, // Computed below
      evidence: { top_event_ids: evidenceIds, top_urls: [] },
    };

    metrics.score = computeScore(metrics);
    topics.push(metrics);
  }

  // Sort by score descending
  topics.sort((a, b) => b.score - a.score);

  const snapshot: TrendSnapshot = {
    generated_at: now.toISOString(),
    window,
    topics: topics.slice(0, config.TOP_N_TOPICS),
  };

  // Publish to Kafka
  await producer.send({
    topic: 'trends.snapshots',
    messages: [{ value: serialize(snapshot) }],
  });

  // Persist to Postgres
  await prisma.trendSnapshot.create({
    data: {
      generatedAt: now,
      window: mapWindow(window),
      snapshot: snapshot as any,
    },
  });

  // Rotate: current → prev
  for (const topicConfig of allowlist.topics) {
    const topic = topicConfig.key;
    const volume = await redis.get(`window:${window}:${topic}:${currentBucket}`);
    if (volume) {
      await redis.set(`prev:${window}:${topic}`, volume);
      await redis.expire(`prev:${window}:${topic}`, 2 * getWindowMs(window));
    }
  }
}
```

### Request-Driven Briefing Boundary

```typescript
// No automatic brief publishing in Trends.
// Requestors (for example riops) publish summary.requests explicitly.
function publishDailyBriefAutomatically(): never {
  throw new Error("Automatic brief triggering is disabled in request-driven mode");
}
```

## Testing Strategy

### Unit Tests

- Topic extraction: various text inputs → topic matches
- Scoring: edge cases (zero volume, missing baseline)
- Window bucketing: boundary cases

### Integration Tests

- Full consumer cycle with test events
- Snapshot publishing to Kafka + Postgres
- No automatic `summary.requests` publication

### Acceptance Tests

- Soak test: 24h run, verify snapshot cadence
- Spike test: inject high volume, verify acceleration detection
- Verify no automatic brief request publication from Trends runtime
