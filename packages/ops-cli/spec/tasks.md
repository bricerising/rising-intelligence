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

### T009: Feed-config flag for brief trigger

- **Acceptance**: `riops brief trigger` accepts repeatable `--feed-config <path>` flags and validates each path.

### T010: Topic-glob derivation from feed YAML

- **Acceptance**: `riops brief trigger` derives topic globs from all non-empty `topics` arrays across all YAML sections, including feeds marked `enabled: false`.

### T011: Merge derived and explicit topic globs

- **Acceptance**: Derived globs are unioned with explicit `--topic-globs`, deduped, and emitted in query payload.

### T012: Warning/error behavior

- **Acceptance**:
  - missing feed YAML path fails command with actionable error;
  - empty `topics: []` entries are ignored with warnings;
  - empty derived globs with explicit topic globs proceeds with warning.

### T013: Events enrichment backfill command

- **Acceptance**: `riops events enrich` runs ordered enrichment steps (default `retag,quality`) with `--dry-run`, `--steps`, `--source`, `--limit`, `--batch-size`, and `--missing-only`.

### T014: Postgres snapshot backup command + scheduler mode

- **Acceptance**: `riops db snapshot` creates `pg_dump` backup files with `--output-dir`, `--retention-days`, `--label`, and `--dry-run` support, and supports `--loop` with configurable interval for Compose automation.
