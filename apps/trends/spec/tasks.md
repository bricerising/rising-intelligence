# Tasks: Trends Service

## Phase 1: MVP snapshots

### T001: Service skeleton

- **Acceptance**: service starts, exports `/metrics`, consumes `events.raw`.

### T002: Topic allowlist + aliases

- **Acceptance**: configurable allowlist maps multiple aliases to canonical topic keys.

### T003: Windowed counts + acceleration

- **Acceptance**: snapshots include `volume` + `prev_volume` + `acceleration` for 15m/60m.

### T004: Publish `trends.snapshots`

- **Acceptance**: `TrendSnapshot` is produced on a fixed cadence with deterministic ordering.

### T005: Persist snapshots to Postgres

- **Acceptance**: each produced snapshot inserts a row into `trend_snapshots` (see `specs/005-postgres-read-model.md`).

## Phase 2: Baselines

### T006: Baseline computation

- **Acceptance**: baseline fields populate once enough history exists.

## Phase 3: Summary triggers

### T007: Daily summary request

- **Acceptance**: at the configured local time, a `SummaryRequest` is published to `summary.requests` built from the latest 24h + 60m context.

### T008: Threshold trigger (optional)

- **Acceptance**: when a topic crosses configured thresholds, a threshold `SummaryRequest` is published with bounded evidence.
