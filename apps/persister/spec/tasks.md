# Tasks: Persister Service

## Phase 1: Skeleton + contracts

### T001: Service skeleton

- **Acceptance**: Service starts, exports `/metrics`, emits a startup log with `service=persister`.

### T002: Kafka consumer setup

- **Acceptance**: Service connects to Kafka, subscribes to `events.raw`, logs received message count.

### T003: Proto deserialization

- **Acceptance**: `RawEvent` protobuf messages are correctly deserialized; malformed messages are logged and skipped.

## Phase 2: Persistence

### T004: Postgres connection + health

- **Acceptance**: Service connects to Postgres on startup, health endpoint reflects connection status.

### T005: Batch insert implementation

- **Acceptance**: Events are batched and inserted efficiently; `ON CONFLICT DO NOTHING` handles duplicates.

### T006: Redis dedup cache

- **Acceptance**: After successful Postgres write, `seen:{source}:{event_id}` is set in Redis with TTL.

## Phase 3: Reliability

### T007: Consumer offset management

- **Acceptance**: Offsets are committed only after successful Postgres write; restart resumes from last commit.

### T008: Consumer lag tracking

- **Acceptance**: `consumer_lag` table is updated periodically with current lag per partition.

### T009: Circuit breaker for Postgres

- **Acceptance**: Transient Postgres failures trigger backoff; sustained failures open circuit and pause consumption.

## Phase 4: Observability

### T010: Metrics implementation

- **Acceptance**: All metrics from `specs/008-observability-contracts.md` are exported.

### T011: Structured logging

- **Acceptance**: Logs include `traceId`, `spanId`, `kafkaTopic`, `partition`, `offset` as specified.

### T012: Trace spans

- **Acceptance**: `persister.process_batch`, `persister.write_postgres`, `persister.write_redis` spans appear in Tempo.
