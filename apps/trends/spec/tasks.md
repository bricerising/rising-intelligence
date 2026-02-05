# Tasks: Trends Service

## Phase 1: Skeleton + contracts

### T001: Service skeleton

- **Acceptance**: Service starts, exports `/metrics`, emits a startup log with `service=trends`.

### T002: Kafka consumer setup

- **Acceptance**: Service connects to Kafka, subscribes to `events.raw`, logs received message count.

### T003: Proto deserialization

- **Acceptance**: `RawEvent` protobuf messages are correctly deserialized.

## Phase 2: Topic Extraction

### T004: Topic allowlist loading

- **Acceptance**: Topics loaded from `TOPICS_ALLOWLIST_PATH`; validation errors logged on startup.

### T005: Topic matching

- **Acceptance**: Events are matched against topic matchers; `topics` array populated.

### T006: Window deduplication

- **Acceptance**: Duplicate events (by `event_id`) within a window are not double-counted (Redis SADD).

## Phase 3: Window Aggregation

### T007: Window counter implementation

- **Acceptance**: Redis counters track volume per topic per window bucket.

### T008: Previous window caching

- **Acceptance**: When window closes, current count moves to `prev:*` keys for acceleration calculation.

### T009: Evidence buffer

- **Acceptance**: Top evidence items (by engagement) are tracked per topic per window (Redis sorted set).

## Phase 4: Snapshot Generation

### T010: Snapshot computation

- **Acceptance**: At configured interval, compute scores for all topics and produce `TrendSnapshot`.

### T011: Score calculation

- **Acceptance**: Score balances volume, acceleration, and baseline delta as per spec.

### T012: Kafka snapshot publishing

- **Acceptance**: `TrendSnapshot` published to `trends.snapshots` topic.

### T013: Postgres snapshot persistence

- **Acceptance**: `TrendSnapshot` written to `trend_snapshots` table.

## Phase 5: Brief Triggering

### T014: Daily brief trigger

- **Acceptance**: At configured local time, check freshness and publish `SummaryRequest`.

### T015: Evidence retrieval

- **Acceptance**: Query Postgres `raw_events` to build evidence items for top topics.

### T016: Data freshness check

- **Acceptance**: Brief is skipped if consumer lag exceeds threshold; metric emitted.

### T017: Threshold alert trigger (optional)

- **Acceptance**: If topic score exceeds threshold, publish flash brief request.

## Phase 6: Baseline Computation

### T018: Baseline calculation

- **Acceptance**: 7-day median volume (same day-of-week) computed from Postgres and cached in Redis.

### T019: Baseline refresh

- **Acceptance**: Baselines are recomputed daily and used in score calculation.

## Phase 7: Observability

### T020: Metrics implementation

- **Acceptance**: All metrics from `specs/008-observability-contracts.md` are exported.

### T021: Consumer lag tracking

- **Acceptance**: `consumer_lag` table updated periodically with current lag.

### T022: Structured logging

- **Acceptance**: Logs include `traceId`, `spanId`, `kafkaTopic`, `partition`, `offset` as specified.

### T023: Trace spans

- **Acceptance**: `trends.process_event`, `trends.compute_snapshot` spans appear in Tempo.
