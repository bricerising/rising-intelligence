# Tasks: Ops CLI

## Phase 1: MVP

### T001: Schema registry publish (idempotent)

- **Acceptance**: repeated runs skip publishing when latest schema matches.

### T002: Retries + timeouts

- **Acceptance**: SR operations retry on transient errors (429/5xx/network) and respect timeout.

### T003: Docker Compose bootstrap

- **Acceptance**: `docker compose up` runs `ops-cli` and registers schemas on startup.

### T004: Manual SummaryRequest trigger command

- **Acceptance**: `riops brief trigger` publishes a valid `summary.requests` message with configurable topic/evidence and supports `--dry-run`.
