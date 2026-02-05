# Feature Specification: Ops CLI

**Package**: `@rising-intelligence/ops-cli` (binary: `riops`)  
**Created**: 2026-02-05  
**Status**: Planned

## Overview

`riops` is the single “go-to” interface for operating the local stack and infrastructure concerns:

- Schema Registry publishing and validation
- LGTM stack discovery and basic health checks (future)
- Kafka/Redpanda ops (future)
- Postgres read-model ops (future)

Guiding rule: future agents SHOULD extend `riops` rather than adding one-off shell scripts.

## Constitution Requirements

- **Idempotency**: commands MUST be safe to re-run (no repeated side effects).
- **Retries**: networked operations MUST have bounded retries and sensible timeouts.
- **No secrets**: command output MUST not print secret values.

## User Scenarios & Testing

### Scenario 1 — Bootstrap Schema Registry (Priority: P1)

As an operator, I can bootstrap Schema Registry consistently on stack startup.

**Independent Test**: `docker compose up` runs `ops-cli` and registers subjects; repeated runs do not churn versions when schemas are unchanged.

## Requirements

### Functional Requirements

- **FR-001**: CLI MUST support publishing Protobuf contracts to Schema Registry.
- **FR-002**: CLI SHOULD be runnable both locally and from Docker Compose.
- **FR-003**: CLI MUST load `.env` automatically in local runs (dev convenience).

### Non-Functional Requirements

- **NFR-001**: Each command MUST exit non-zero on failure with actionable error output.
- **NFR-002**: Commands MUST not hang indefinitely (timeouts).

## Success Criteria

- `ops-cli` can bootstrap the stack without manual steps.
- Ops tasks are discoverable and standardized for future agents.

