# Tasks: Brief Service

## Phase 1: Contracts + wiring

### T001: Service skeleton

- **Acceptance**: service consumes `summary.requests` and publishes `summary.results` using a stub generator.

### T002: Brief contract + validation

- **Acceptance**: invalid outputs are rejected; published `BriefResult` messages validate against schema.

### T003: Persist results to Postgres

- **Acceptance**: each produced `BriefResult` inserts a row into `brief_results` (see `specs/005-postgres-read-model.md`).

## Phase 2: LLM integration

### T004: Provider integration

- **Acceptance**: service can generate a brief using the configured LLM provider/model.

### T005: Budget enforcement

- **Acceptance**: requests beyond budget are rejected or degraded deterministically.
