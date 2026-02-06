# Tasks: Brief Service

## Progress

- 2026-02-06: Implemented T001-T003 baseline path (service skeleton, Kafka consumer setup, and SummaryRequest deserialization).

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
