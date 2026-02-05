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

## Phase 2: Baselines

### T005: Baseline computation

- **Acceptance**: baseline fields populate once enough history exists.
