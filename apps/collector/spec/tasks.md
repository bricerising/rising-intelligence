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
