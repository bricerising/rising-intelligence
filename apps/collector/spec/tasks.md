# Tasks: Collector Service

## Phase 1: Skeleton + contracts

### T001: Service skeleton

- **Acceptance**: service starts, exports `/metrics`, emits a startup log with `service=collector`.

### T002: Kafka publish + DLQ

- **Acceptance**: invalid payloads are rejected and sent to `events.raw.dlq`; valid payloads land on `events.raw`.

## Phase 2: MVP sources

### T003: RSS/Atom adapter

- **Acceptance**: new feed entries are emitted exactly once per `event_id` per run.

### T004: Hacker News adapter

- **Acceptance**: top/new stories (configurable) are emitted with stable IDs.

### T005: Reddit adapter

- **Acceptance**: new posts from configured subreddits are emitted; 429 handling backs off.

## Phase 3: Ops hardening

### T006: Cursor checkpointing

- **Acceptance**: restart does not cause full backfill; cursor resumes.

### T007: Observability wiring

- **Acceptance**: traces appear in Tempo and logs correlate via `traceId`.

### T008: Health heartbeat publishing

- **Acceptance**: Collector publishes a heartbeat event to `collector.heartbeat` topic every 60 seconds per source, indicating the source is being actively polled.

### T009: Source staleness metrics

- **Acceptance**: Metrics `ri_collector_last_success_timestamp{source}` and `ri_collector_source_healthy{source}` are exported for alerting.

## Phase 4: Additional sources

### T010: Bluesky adapter

- **Acceptance**: Posts matching configured hashtags are emitted; optional Jetstream firehose mode.

### T011: Mastodon adapter

- **Acceptance**: Public timeline posts from configured instances are emitted; per-instance rate limiting.
