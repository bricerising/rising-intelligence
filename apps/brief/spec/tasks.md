# Tasks: Brief Service

## Progress

- 2026-02-06: Implemented T001-T003 baseline path (service skeleton, Kafka consumer setup, and SummaryRequest deserialization).
- 2026-02-06: Implemented idempotent processing, Postgres persistence, and `summary.results` publishing with budget-aware failure handling (initial T008-T010/T013 path).
- 2026-02-08: Added configurable `LLM_PROVIDER` with `http` provider support and schema validation for remote LLM responses.
- 2026-02-08: Added Docker Compose E2E harness with mock LLM server and end-to-end verification script (`test:e2e:brief:compose`).
- 2026-02-10: Added `LLM_PROVIDER=codex-cli` path to generate human-readable briefs via local Codex CLI, with response schema validation.
- 2026-02-10: Updated Docker image + Compose to support `LLM_PROVIDER=codex-cli` in containerized runs (Codex CLI install + host `${HOME}/.codex` mount).
- 2026-02-10: Implemented runtime prompt-injection hygiene for LLM inputs (evidence sanitization + suspicious-content logging metric), plus regression tests for sanitized payload generation.
- 2026-02-10: Updated specs for query-mode summary generation (`last N days` default), topic glob filtering, and executive-summary output (implementation pending).

## Phase 1: Skeleton + contracts

### T001: Service skeleton

- **Acceptance**: Service starts, exports `/metrics`, emits a startup log with `service=brief`.

### T002: Kafka consumer setup

- **Acceptance**: Service connects to Kafka, subscribes to `summary.requests`, logs received message count.

### T003: Proto deserialization

- **Acceptance**: `SummaryRequest` protobuf messages are correctly deserialized.

## Phase 2: LLM Integration

### T004: LLM client setup

- **Acceptance**: Service connects to configured LLM provider (OpenAI/Anthropic), verifies API key.

### T005: Prompt template implementation

- **Acceptance**: `SummaryRequest` data is rendered into prompt using template from `prompts.md`.

### T006: Brief generation

- **Acceptance**: LLM response is parsed and validated; `Brief` object is created with citations.

### T007: Citation validation

- **Acceptance**: All citation URLs are verified against the evidence set; briefs with fabricated URLs are rejected and retried.

## Phase 3: Budget & Idempotency

### T008: Budget tracking

- **Acceptance**: Daily LLM spend is tracked in Redis; requests exceeding budget are rejected.

### T009: Idempotent processing

- **Acceptance**: Duplicate `request_id` values are detected and skipped.

### T010: Postgres persistence

- **Acceptance**: `BriefResult` (success or failure) is written to `brief_results` table.

## Phase 4: Reliability

### T011: Circuit breaker for LLM

- **Acceptance**: Sustained LLM failures trigger circuit breaker; health endpoint reflects state.

### T012: Fallback to cheaper model

- **Acceptance**: When primary model fails, fallback to cheaper model is attempted.

### T013: Kafka result publishing

- **Acceptance**: `BriefResult` is published to `summary.results` topic.

## Phase 5: Observability

### T014: Metrics implementation

- **Acceptance**: All metrics from `specs/008-observability-contracts.md` are exported.

### T015: Structured logging

- **Acceptance**: Logs include `traceId`, `spanId`, `requestId` as specified.

### T016: Trace spans

- **Acceptance**: `brief.process_request`, `brief.call_llm`, `brief.persist_result` spans appear in Tempo.

## Phase 6: E2E Test Harness

### T017: Mock LLM provider path

- **Acceptance**: Brief service can call an HTTP endpoint (`LLM_PROVIDER=http`) and emit a valid `summary.results` success payload with response metadata.

### T018: Compose-driven E2E test

- **Acceptance**: `docker-compose.test.yml` profile boots Redis/Postgres/Redpanda + `brief` + mock LLM, and e2e script verifies success, duplicate skip, and budget-exceeded behavior.

## Phase 7: Query-Mode Executive Summaries

### T019: Query mode request parsing

- **Acceptance**: When `topics` is missing/empty, request parser resolves query mode with default `lookback_days=7` and max guardrail `30`.

### T020: Trend snapshot ranking in Brief service

- **Acceptance**: Query mode reads `trend_snapshots` for `TREND_WINDOW_60M` in lookback window and computes recent-weighted average score ranking.

### T021: Topic glob filtering

- **Acceptance**: `topic_globs` supports wildcard matching over canonical topic keys (`*`, `?`), is applied before ranking, and has deterministic tests.

### T022: Postgres evidence fetch in Brief service

- **Acceptance**: After ranking, query mode loads events from `raw_events` within lookback window and builds bounded per-topic evidence.

### T023: Executive summary output contract

- **Acceptance**: Generated briefs include a human-readable executive summary paragraph in `brief.notes` plus grounded highlights.

### T024: Query-mode test coverage

- **Acceptance**: Unit/integration tests cover lookback bounds, pre-ranking glob filtering, trend-snapshot ranking behavior, no-data failures, and budget interactions.

## Phase 8: Structured Notes Thin-Slice

### T025: SummaryRequest notes-framing hint parsing

- **Acceptance**: Parser accepts optional `report.timezone`, `report.start_at`, and `report.end_at` fields without breaking existing requests.

### T026: Standard notes prompt shaping

- **Acceptance**: LLM prompt requests sectioned markdown notes by default and preserves grounded highlight schema.

### T027: Notes URL grounding validation

- **Acceptance**: URLs found in generated `brief.notes` are normalized and validated against evidence URLs; mismatches produce non-retryable grounding failure.

### T028: Trigger-path wiring + tests

- **Acceptance**: `riops brief trigger` can emit notes-framing hints and brief tests cover report parsing, prompt shaping, and notes grounding failures.
