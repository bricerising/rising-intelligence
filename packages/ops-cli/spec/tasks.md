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

### T005: Query-mode defaults + filters

- **Acceptance**: `riops brief trigger` defaults to query mode (`topics=[]`) with configurable `--lookback-days`, `--topic-globs`, and `--max-events-per-topic`.

### T006: Explicit mode compatibility

- **Acceptance**: Existing explicit mode flags (`--topic-key`, `--evidence-url`, etc.) continue to work when provided.

### T007: Structured notes framing hints

- **Acceptance**: `riops brief trigger` supports `--report-timezone`, `--report-start-at`, and `--report-end-at` and includes them in emitted `SummaryRequest`.

### T008: Topic re-tag backfill command

- **Acceptance**: `riops topics retag` recomputes `raw_events.tags` + `raw_events.topics` from allowlist rules, defaults to rows with empty topic/tag arrays, supports `--all`, and includes a safe `--dry-run` mode.
