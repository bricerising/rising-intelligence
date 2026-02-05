# Tasks: Brief Service

## Phase 1: Contracts + wiring

### T001: Service skeleton

- **Acceptance**: service consumes `summary.requests` and publishes `summary.results` using a stub generator.

### T002: Brief contract + validation

- **Acceptance**: invalid outputs are rejected; published briefs validate against schema.

## Phase 2: LLM integration

### T003: Provider integration

- **Acceptance**: service can generate a brief using the configured LLM provider/model.

### T004: Budget enforcement

- **Acceptance**: requests beyond budget are rejected or degraded deterministically.
