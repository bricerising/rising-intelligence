# Feature Specification: Persister Service

**Service**: `@rising-intelligence/persister`
**Created**: 2026-02-05
**Status**: Planned

## Overview

The Persister Service is a lightweight Kafka consumer that materializes `events.raw` into queryable storage:

- **Postgres**: `raw_events` table for search and Grafana dashboards
- **Redis**: `seen:{event_id}` cache for cross-service deduplication

This service exists to decouple ingestion (Collector) from storage writes, following the Kafka-centric architecture principle.

## Design Principles

### Single Responsibility

The persister does one thing: consume events and write them to storage. It doesn't:
- Transform events (that's the Collector's job)
- Extract topics (that's done by the Collector before publishing)
- Compute trends (that's the Trends service's job)
- Make decisions (it just persists)

### Pre-Enriched Events

Events arrive from `events.raw` with topics already extracted by the Collector. The `tags` field contains canonical topic keys (e.g., `["aws.bedrock", "ai.llm"]`). In MVP, the Persister writes these to both:

- `raw_events.tags` (raw tag list, mirror), and
- `raw_events.topics` (canonical topic keys used for trend computation),

without additional processing.

### Idempotent Writes

Since Kafka delivers at-least-once, the persister must handle duplicates:
- Postgres: `ON CONFLICT DO NOTHING` on `event_id`
- Redis: `SETNX` returns 0 for existing keys (no-op)

### Backpressure-Friendly

If Postgres or Redis is slow, the persister falls behind on Kafka. This is visible via consumer lag metrics. The system degrades gracefully — ingestion continues, queries are stale.

## User Scenarios & Testing

### User Story 1 — Event materialization (Priority: P1)

As an operator, I can query raw events in Postgres within seconds of them appearing in Kafka.

**Independent Test**: Publish an event to `events.raw`, verify it appears in `raw_events` table within 5 seconds.

**Acceptance Scenarios**:

1. **Given** an event in `events.raw`, **When** the persister processes it, **Then** it appears in `raw_events` and Redis.
2. **Given** a duplicate event (same `event_id`), **When** the persister processes it, **Then** no error occurs and no duplicate row is created.
3. **Given** Postgres is temporarily unavailable, **When** the persister retries, **Then** it recovers without data loss.

### User Story 2 — Deduplication cache (Priority: P1)

As a downstream service, I can check Redis to see if an event has been persisted.

**Independent Test**: Process an event, verify `seen:{event_id}` exists in Redis with TTL.

**Acceptance Scenarios**:

1. **Given** a persisted event, **When** I check Redis, **Then** `seen:{source}:{event_id}` exists.
2. **Given** 24 hours pass, **When** I check Redis, **Then** the key has expired (TTL).

## Constitution Requirements

- **Idempotency**: Duplicate events MUST NOT cause errors or duplicate rows.
- **Consistency**: Every event in Postgres MUST have a corresponding Redis key (best-effort).
- **Observability**: Consumer lag MUST be visible via metrics.

## Requirements

### Functional Requirements

- **FR-001**: Service MUST consume `events.raw` from Kafka.
- **FR-002**: Service MUST persist events to `raw_events` (Postgres) via Prisma.
- **FR-003**: Service MUST set `seen:{source}:{event_id}` in Redis with 24h TTL.
- **FR-004**: Service MUST handle duplicates gracefully (no errors, no duplicate rows).
- **FR-005**: Service MUST commit Kafka offsets only after successful persistence.

### Non-Functional Requirements

- **NFR-001**: P99 latency from Kafka receive to Postgres commit SHOULD be < 500ms.
- **NFR-002**: Service MUST tolerate Postgres/Redis downtime with retry + backoff.
- **NFR-003**: Consumer lag MUST be exposed as a Prometheus metric.

## Data Flow

```
┌─────────────────┐
│ Kafka           │
│ events.raw      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Persister     │
│   (consumer)    │
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌───────┐ ┌───────┐
│Postgres│ │ Redis │
│raw_events│ │seen:*│
└───────┘ └───────┘
```

## Processing Logic

```typescript
async function processMessage(message: KafkaMessage): Promise<void> {
  const event = deserialize<RawEvent>(message.value);

  // 1. Write to Postgres (idempotent)
  await prisma.rawEvent.create({
    data: mapToDb(event),
  }).catch((e) => {
    if (e.code === 'P2002') return; // Unique constraint = already exists
    throw e; // Rethrow other errors
  });

  // 2. Set Redis seen key (idempotent)
  const key = `seen:${event.source}:${event.event_id}`;
  await redis.set(key, '1', 'EX', 86400); // 24h TTL

  // 3. Kafka offset committed automatically on success
}
```

## Failure Handling

| Failure | Behavior |
|---------|----------|
| Postgres unavailable | Retry with backoff; don't commit offset |
| Redis unavailable | Log warning; continue (Redis is optional cache) |
| Malformed event | Log error; skip and commit offset (don't block) |
| Duplicate event | Silent no-op; commit offset |

## Success Criteria

- **SC-001**: 0 duplicate rows in `raw_events` after 24h soak with duplicate events.
- **SC-002**: Consumer lag stays < 100 messages under normal load.
- **SC-003**: P99 processing latency < 500ms.
- **SC-004**: Service recovers automatically from Postgres/Redis restarts.

## Configuration

```
# Required
KAFKA_BROKERS=localhost:9092
DATABASE_URL=postgresql://user:pass@localhost:5432/rising_intelligence
REDIS_URL=redis://localhost:6379

# Consumer config
KAFKA_CONSUMER_GROUP=persister
KAFKA_AUTO_COMMIT=false

# Retry config
POSTGRES_RETRY_ATTEMPTS=5
POSTGRES_RETRY_DELAY_MS=1000
```

## Metrics

- `persister_events_processed_total`
- `persister_events_skipped_total{reason=duplicate|malformed}`
- `persister_postgres_write_duration_seconds`
- `persister_redis_write_duration_seconds`
- `persister_consumer_lag{partition=...}`
- `persister_errors_total{type=postgres|redis|parse}`

## Health Check

The Persister exposes a `/health` endpoint for container orchestration:

```typescript
interface HealthStatus {
  status: 'healthy' | 'degraded' | 'unhealthy';
  checks: {
    kafka: 'ok' | 'error';
    postgres: 'ok' | 'error';
    redis: 'ok' | 'error';
  };
  consumer_lag: number;
  uptime_seconds: number;
}
```

**Health criteria**:
- `healthy`: Kafka, Postgres, Redis all reachable; consumer lag < 1000
- `degraded`: Redis unavailable (cache miss acceptable) OR lag > 1000
- `unhealthy`: Kafka OR Postgres unreachable

**Endpoint**: `GET /health` returns 200 (healthy/degraded) or 503 (unhealthy)

## Graceful Shutdown

On SIGTERM/SIGINT, the Persister:

1. Stops consuming new messages
2. Processes remaining in-flight batch (max 30s timeout)
3. Commits final Kafka offsets
4. Closes Postgres and Redis connections
5. Exits with code 0

```typescript
process.on('SIGTERM', async () => {
  log.info('Received SIGTERM, initiating graceful shutdown');

  // Stop consuming
  await consumer.pause([{ topic: 'events.raw' }]);

  // Process remaining messages (max 30s)
  await Promise.race([
    processRemainingMessages(),
    sleep(30_000),
  ]);

  // Commit final offsets
  await consumer.commitOffsets();

  // Close connections
  await Promise.all([
    consumer.disconnect(),
    prisma.$disconnect(),
    redis.quit(),
  ]);

  log.info('Graceful shutdown complete');
  process.exit(0);
});
```
