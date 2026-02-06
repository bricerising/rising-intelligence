# Tasks: Trends Service

## Progress

- 2026-02-06: Implemented T001-T007 baseline path (service skeleton, Kafka consumer, deserialization, allowlist loading/filtering, window dedup, and Redis window counters with periodic snapshot publishing).

## Phase 1: Skeleton + contracts

### T001: Service skeleton

- **Acceptance**: Service starts, exports `/metrics`, emits a startup log with `service=trends`.

### T002: Kafka consumer setup

- **Acceptance**: Service connects to Kafka, subscribes to `events.raw`, logs received message count.

### T003: Proto deserialization

- **Acceptance**: `RawEvent` protobuf messages are correctly deserialized.

## Phase 2: Topic Keys (from RawEvent.tags)

### T004: Topic allowlist loading

- **Acceptance**: Topics loaded from `TOPICS_ALLOWLIST_PATH`; validation errors logged on startup.

### T005: Topic filtering

- **Acceptance**: `RawEvent.tags` is filtered/validated against the allowlist (muted topics removed; unknown tags ignored) to produce the tracked topic keys.

### T006: Window deduplication

- **Acceptance**: Duplicate events (by `event_id`) within a window are not double-counted (Redis SADD).

## Phase 3: Window Aggregation

### T007: Window counter implementation

- **Acceptance**: Redis counters track volume per topic per window bucket.

### T007a: Source-weighted volume calculation

- **Acceptance**: Volume calculation applies source weights (RSS=1.0, HN=0.8, Reddit=0.3, etc.) instead of raw counts.

### T008: Previous window caching

- **Acceptance**: When window closes, current count moves to `prev:*` keys for acceleration calculation.

### T009: Evidence buffer

- **Acceptance**: Top evidence items (by engagement) are tracked per topic per window (Redis sorted set).

### T009a: Evidence source diversity

- **Acceptance**: Evidence selection includes at least one item per source type when available (not all Reddit).

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

- **Acceptance**: At configured UTC time, check freshness and publish `SummaryRequest`.

### T015: Evidence retrieval

- **Acceptance**: Query Postgres `raw_events` to build evidence items for top topics.

### T016: Data freshness check

- **Acceptance**: Brief is skipped if consumer lag exceeds threshold for either `trends-processor` or `persister` consumer groups; metric emitted.

### T017: Threshold alert trigger (optional)

- **Acceptance**: If topic score exceeds threshold, publish flash brief request.

## Phase 6: Baseline Computation

### T018: Baseline calculation

- **Acceptance**: 30-day median volume with day-of-week adjustment computed from Postgres and cached in Redis.

### T019: Baseline refresh

- **Acceptance**: Baselines are recomputed daily and used in score calculation.

## Phase 7: Topic Discovery

### T020: Candidate term extraction

- **Acceptance**: Unknown terms (hashtags, capitalized phrases) are extracted from events and counted separately.

### T021: Discovery threshold alerting

- **Acceptance**: Terms exceeding volume + acceleration thresholds are written to `discovery_candidates` table.

### T022: Discovery metrics

- **Acceptance**: `ri_trends_discovery_candidates_total` and `ri_trends_discovery_surfaced_total` metrics exported.

## Phase 8: Observability

### T023: Metrics implementation

- **Acceptance**: All metrics from `specs/008-observability-contracts.md` are exported.

### T024: Consumer lag tracking

- **Acceptance**: `consumer_lag` table updated periodically with current lag.

### T025: Structured logging

- **Acceptance**: Logs include `traceId`, `spanId`, `kafkaTopic`, `partition`, `offset` as specified.

### T026: Trace spans

- **Acceptance**: `trends.process_event`, `trends.compute_snapshot` spans appear in Tempo.

### T027: Collector health validation

- **Acceptance**: Brief triggering validates Collector heartbeats in addition to consumer lag.
