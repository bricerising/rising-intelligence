# Implementation Plan: Persister Service

## Overview

Build `apps/persister` as a lightweight Kafka consumer that materializes events to Postgres and Redis.

## Architecture (High Level)

- **Input**: Kafka (`events.raw`)
- **Output**: Postgres (`raw_events`), Redis (`seen:*` keys)
- **Dependencies**: Prisma (via `@rising-intelligence/db`), ioredis

```
Kafka (events.raw) → Consumer → [Postgres, Redis]
```

## Dependencies

```json
{
  "@rising-intelligence/db": "workspace:*",
  "@rising-intelligence/shared": "workspace:*",
  "kafkajs": "^2.x",
  "ioredis": "^5.x"
}
```

## Phases

### Phase 1: Basic consumer + Postgres write

- Kafka consumer setup
- Prisma integration for `raw_events`
- Idempotent writes with `ON CONFLICT`
- Manual offset commits

**Deliverables**:
- `src/index.ts` - service entry point
- `src/config.ts` - environment config
- `src/kafka/consumer.ts` - Kafka consumer
- `src/db/persist.ts` - Postgres write logic

### Phase 2: Redis integration + dedup cache

- Redis client setup
- Set `seen:{source}:{event_id}` with TTL
- Treat Redis as required: fail readiness and pause processing when unavailable

**Deliverables**:
- `src/redis/client.ts` - Redis client
- `src/redis/seen.ts` - seen cache operations

### Phase 3: Reliability + observability

- Retry logic for transient failures
- Metrics (consumer lag, write latency, errors)
- Health check endpoints

**Deliverables**:
- `src/retry.ts` - retry with backoff
- `src/metrics.ts` - Prometheus metrics
- `src/health.ts` - `/healthz` and `/readyz`

## Key Implementation Details

### Consumer Setup

```typescript
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'persister',
  brokers: config.KAFKA_BROKERS.split(','),
});

const consumer = kafka.consumer({
  groupId: 'persister',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'events.raw', fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message, partition, topic }) => {
      await processMessage(message);
      await consumer.commitOffsets([{
        topic,
        partition,
        offset: (BigInt(message.offset) + 1n).toString(),
      }]);
    },
  });
}
```

### Postgres Write (Idempotent)

```typescript
import { prisma, Source } from '@rising-intelligence/db';

async function persistToPostgres(event: RawEvent): Promise<boolean> {
  try {
    await prisma.rawEvent.create({
      data: {
        eventId: event.event_id,
        source: mapSource(event.source),
        fetchedAt: new Date(event.fetched_at),
        publishedAt: event.published_at ? new Date(event.published_at) : null,
        url: event.url,
        title: event.title,
        text: event.text,
        authorId: event.author?.id,
        authorHandle: event.author?.handle,
        authorDisplayName: event.author?.display_name,
        engagementScore: event.engagement?.score,
        engagementComments: event.engagement?.comments,
        engagementLikes: event.engagement?.likes,
        engagementShares: event.engagement?.shares,
        lang: event.lang,
        tags: event.tags ?? [], // MVP: mirrors canonical topics
        extractedHashtags: event.extracted?.hashtags ?? [],
        extractedUrls: event.extracted?.urls ?? [],
        topics: event.tags ?? [], // Canonical topic keys (from Collector extraction)
        sourceMeta: event.source_meta_json ? JSON.parse(event.source_meta_json) : null,
      },
    });
    return true; // New row created
  } catch (e: any) {
    if (e.code === 'P2002') {
      // Unique constraint violation = duplicate
      metrics.increment('ri_persister_events_skipped_total', { reason: 'duplicate' });
      return false;
    }
    throw e;
  }
}

function mapSource(source: string): Source {
  const mapping: Record<string, Source> = {
    SOURCE_RSS: "rss",
    SOURCE_NEWS: "news",
    SOURCE_HACKERNEWS: "hackernews",
    SOURCE_REDDIT: "reddit",
    SOURCE_GITHUB: "github",
    SOURCE_BLUESKY: "bluesky",
    SOURCE_MASTODON: "mastodon",
  };
  return mapping[source] ?? "rss";
}
```

### Redis Seen Cache

```typescript
import Redis from 'ioredis';

const redis = new Redis(config.REDIS_URL);
const SEEN_TTL_SECONDS = 86400; // 24 hours

async function markSeen(event: RawEvent): Promise<void> {
  const key = `seen:${event.source}:${event.event_id}`;

  try {
    await redis.set(key, '1', 'EX', SEEN_TTL_SECONDS);
  } catch (e) {
    log.error({ error: e, key }, 'Failed to set seen key in Redis');
    metrics.increment('ri_persister_errors_total', { error_type: 'redis' });
    throw e;
  }
}

// For other services to check
async function isSeen(source: string, eventId: string): Promise<boolean> {
  const key = `seen:${source}:${eventId}`;
  const exists = await redis.exists(key);
  return exists === 1;
}
```

### Main Processing Loop

```typescript
async function processMessage(message: KafkaMessage): Promise<void> {
  const startTime = Date.now();

  // Parse event
  let event: RawEvent;
  try {
    event = deserialize<RawEvent>(message.value);
  } catch (e) {
    log.error({ error: e }, 'Failed to parse event');
    metrics.increment('ri_persister_events_skipped_total', { reason: 'malformed' });
    return; // Skip malformed events
  }

  // Write to Postgres with retry
  await retry(
    () => persistToPostgres(event),
    {
      attempts: config.POSTGRES_RETRY_ATTEMPTS,
      delay: config.POSTGRES_RETRY_DELAY_MS,
      onRetry: (attempt, error) => {
        log.warn({ attempt, error }, 'Postgres write retry');
      },
    }
  );

  // Write to Redis (required for readiness)
  await markSeen(event);

  // Metrics
  const duration = Date.now() - startTime;
  metrics.observe('ri_persister_postgres_write_duration_seconds', duration / 1000);
  metrics.increment('ri_persister_events_processed_total');
}
```

### Retry Helper

```typescript
interface RetryOptions {
  attempts: number;
  delay: number;
  onRetry?: (attempt: number, error: Error) => void;
}

async function retry<T>(
  fn: () => Promise<T>,
  options: RetryOptions
): Promise<T> {
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= options.attempts; attempt++) {
    try {
      return await fn();
    } catch (e) {
      lastError = e as Error;
      if (attempt < options.attempts) {
        options.onRetry?.(attempt, lastError);
        await sleep(options.delay * attempt); // Linear backoff
      }
    }
  }

  throw lastError;
}
```

### Health Checks

```typescript
import express from 'express';

const app = express();

app.get('/healthz', (req, res) => {
  // Liveness: is the process running?
  res.status(200).json({ status: 'ok' });
});

app.get('/readyz', async (req, res) => {
  // Readiness: can we process messages?
  try {
    await prisma.$queryRaw`SELECT 1`;
    await redis.ping();
    res.status(200).json({ status: 'ready' });
  } catch (e) {
    res.status(503).json({ status: 'not ready', error: e.message });
  }
});

app.listen(config.HEALTH_PORT);
```

## Testing Strategy

### Unit Tests

- Event mapping: RawEvent → Prisma model
- Retry logic: various failure scenarios
- Source mapping: all enum values

### Integration Tests

- Full round-trip: Kafka → Postgres → verify row
- Duplicate handling: same event twice → one row
- Redis unavailable: service becomes not-ready and pauses processing

### Acceptance Tests

- Soak test: 24h with continuous events
- Chaos test: kill Postgres, verify recovery
- Lag test: slow Postgres, verify lag metrics

## Metrics

- `ri_persister_events_processed_total` - Counter
- `ri_persister_events_skipped_total{reason=duplicate|malformed}` - Counter
- `ri_persister_postgres_write_duration_seconds` - Histogram
- `ri_persister_redis_write_duration_seconds` - Histogram
- `ri_persister_errors_total{error_type=postgres|redis|parse}` - Counter
- `ri_persister_consumer_lag{partition}` - Gauge
