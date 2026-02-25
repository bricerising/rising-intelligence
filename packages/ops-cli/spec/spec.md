# Feature Specification: Ops CLI

**Package**: `@rising-intelligence/ops-cli` (binary: `riops`)  
**Created**: 2026-02-05  
**Status**: Planned

## Overview

`riops` is the single “go-to” interface for operating the local stack and infrastructure concerns:

- Schema Registry publishing and validation
- LGTM stack discovery and basic health checks (future)
- Kafka/Redpanda ops (including manual brief trigger requests)
- Postgres read-model ops (future)

Guiding rule: future agents SHOULD extend `riops` rather than adding one-off shell scripts.

Brief generation is request-driven. In this architecture, `riops brief trigger` is the primary supported way to initiate briefs.

## Constitution Requirements

- **Idempotency**: commands MUST be safe to re-run (no repeated side effects).
- **Retries**: networked operations MUST have bounded retries and sensible timeouts.
- **No secrets**: command output MUST not print secret values.

## User Scenarios & Testing

### Scenario 1 — Bootstrap Schema Registry (Priority: P1)

As an operator, I can bootstrap Schema Registry consistently on stack startup.

**Independent Test**: `docker compose up` runs `ops-cli` and registers subjects; repeated runs do not churn versions when schemas are unchanged.

### Scenario 2 — Manual Brief Trigger (Priority: P2)

As an operator, I can publish a valid `SummaryRequest` from the terminal to force brief generation on demand.

**Independent Test**: `riops brief trigger --dry-run` emits query-mode `SummaryRequest` (`topics=[]`, `query.lookback_days=7`, `query.topic_globs=["*"]`) and publishes keyed payload to `summary.requests`.

### Scenario 3 — Re-tag Existing Raw Events (Priority: P2)

As an operator, I can recompute `raw_events.tags` and `raw_events.topics` from the current allowlist rules to backfill previously untagged events.

**Independent Test**: `riops topics retag --dry-run` reports candidate updates without mutating rows; re-running without `--dry-run` updates matching rows idempotently.

### Scenario 4 — Feed-Config Derived Query Filters (Priority: P2)

As an operator, I can pass one or more feed YAML files to `riops brief trigger` so it derives topic globs from feed metadata and unions them with explicit topic globs.

**Independent Test**: `riops brief trigger --feed-config ... --topic-globs ... --dry-run` emits a query-mode `SummaryRequest` with merged `query.topic_globs` and warning/error behavior per policy.

### Scenario 5 — Backfill Event Enrichment (Priority: P2)

As an operator, I can retrofit existing `raw_events` with both topic re-tagging and ingest quality metadata annotations.

**Independent Test**: `riops events enrich --dry-run` reports updates for rows requiring retagging/quality annotation; re-running without `--dry-run` updates rows idempotently.

### Scenario 6 — Daily Postgres Snapshots (Priority: P1)

As an operator, I can create on-demand Postgres snapshots and run automated daily snapshots from Docker Compose for disaster recovery.

**Independent Test**: `riops db snapshot` writes a `pg_dump` backup file, and `docker compose up` starts a scheduler service that runs `riops db snapshot --loop` every 24 hours.

## Requirements

### Functional Requirements

- **FR-001**: CLI MUST support publishing Protobuf contracts to Schema Registry.
- **FR-002**: CLI SHOULD be runnable both locally and from Docker Compose.
- **FR-003**: CLI MUST load `.env` automatically in local runs (dev convenience).
- **FR-004**: CLI MUST support generating and publishing a manual `SummaryRequest` to Kafka for brief triggering.
- **FR-005**: `brief trigger` MUST default to query-mode requests (no required topic/evidence flags).
- **FR-006**: `brief trigger` MUST support query filters (`lookback_days`, `topic_globs`, `max_events_per_topic`) and enforce lookback guardrails.
- **FR-007**: `brief trigger` SHOULD preserve explicit mode as an opt-in backward-compatible path when topic/evidence flags are provided.
- **FR-008**: `brief trigger` is the canonical operator path for creating briefs; Trends no longer auto-publishes brief requests.
- **FR-009**: `brief trigger` SHOULD support optional notes-framing hints (`report.timezone`, `report.start_at`, `report.end_at`) for structured brief rendering.
- **FR-010**: CLI MUST support `topics retag` to recompute `raw_events.tags` and `raw_events.topics` from the allowlist.
- **FR-011**: `topics retag` MUST default to rows with empty `tags` or `topics` and support `--all` to process all rows.
- **FR-012**: `topics retag` MUST support `--dry-run`, `--source`, `--limit`, and `--batch-size` for safe operational execution.
- **FR-013**: `brief trigger` MUST support repeated `--feed-config <path>` flags for deriving topic globs from YAML feed config files.
- **FR-014**: Derived topic globs MUST be the union of all non-empty `topics` arrays found across all feed entries in selected YAML files, regardless of section name.
- **FR-015**: `brief trigger` MUST union derived globs with explicit `--topic-globs` values.
- **FR-016**: If a `--feed-config` file does not exist or is unreadable, command MUST fail with actionable error output.
- **FR-017**: Feed entries with empty `topics: []` MUST be ignored for derivation and SHOULD emit warnings.
- **FR-018**: Topic derivation MUST include entries even if `enabled: false` (briefing scope is based on collected data, not collection toggles).
- **FR-019**: If feed-derived globs are empty but explicit `--topic-globs` are provided, command MAY proceed and SHOULD warn.
- **FR-020**: If neither feed-derived globs nor explicit globs are provided, query mode defaults to wildcard (`*`).
- **FR-021**: CLI MUST support `events enrich` to run ordered enrichment steps against `raw_events` (default: `retag,quality`).
- **FR-022**: `events enrich` MUST include retagging behavior compatible with `topics retag`.
- **FR-023**: `events enrich` MUST support `--steps`, `--dry-run`, `--source`, `--limit`, and `--batch-size`.
- **FR-024**: `events enrich` MUST support `--missing-only` mode to restrict updates to rows with empty `tags/topics`.
- **FR-025**: CLI MUST support `db snapshot` to create Postgres backups using `pg_dump`.
- **FR-026**: `db snapshot` MUST support `--output-dir`, `--retention-days`, `--label`, and `--dry-run` for safe backup operations.
- **FR-027**: `db snapshot` MUST support a loop/scheduler mode (`--loop` + `--interval-seconds`) suitable for Docker Compose automation.
- **FR-028**: Snapshot logs/output MUST avoid printing database passwords or secret values.

### Non-Functional Requirements

- **NFR-001**: Each command MUST exit non-zero on failure with actionable error output.
- **NFR-002**: Commands MUST not hang indefinitely (timeouts).

## Success Criteria

- `ops-cli` can bootstrap the stack without manual steps.
- Ops tasks are discoverable and standardized for future agents.
