# Feature Specification: Shared Package

**Package**: `@rising-intelligence/shared`  
**Created**: 2026-02-05  
**Status**: Planned

## Overview

`@rising-intelligence/shared` is the “boring platform layer” for this monorepo. It exists to:

- define and distribute cross-service **contracts** (Protobuf),
- standardize **configuration + secrets** resolution,
- provide shared **naming conventions** (Schema Registry subjects, topic names), and
- prevent duplication across services and tooling (apps + `riops`).

Non-goal: becoming a generic “utils” dump. If a helper isn’t cross-cutting or used by at least two packages/services, it probably doesn’t belong here.

## User Scenarios & Testing

### Scenario 1 — Unified secret resolution (Priority: P1)

As an operator, I can provide secrets consistently to both services and the ops CLI.

**Independent Test**: Use `FOO` and `FOO_FILE` in local runs to verify `getSecretValue("FOO")` resolves correctly.

**Acceptance Scenarios**:

1. **Given** `FOO` is set, **When** `getSecretValue("FOO")` is called, **Then** it returns the env value.
2. **Given** `FOO` is unset and `FOO_FILE` points to a file containing a value, **When** called, **Then** it returns the file contents (newline-trimmed).
3. **Given** neither is set and the secret is required, **When** called, **Then** it fails fast with a clear error.

### Scenario 2 — Canonical contracts (Priority: P1)

As a developer, I can find and evolve the wire contracts in one place and publish them to Schema Registry.

**Independent Test**: `riops schema-registry publish-protos` publishes the Protobuf schemas without changing behavior when run repeatedly.

## Requirements

### Functional Requirements

- **FR-001**: Package MUST be the canonical home for Protobuf contracts.
- **FR-002**: Package MUST provide helpers for `.env` loading in local dev (CLI + non-container runs).
- **FR-003**: Package MUST provide a uniform secret resolution mechanism (`FOO` or `FOO_FILE`).
- **FR-004**: Package SHOULD define shared naming constants (Schema Registry subjects, Kafka topic names).

### Non-Functional Requirements

- **NFR-001**: No secret values are logged or accidentally stringified in error messages.
- **NFR-002**: Public APIs remain stable; breaking changes require spec update + coordinated rollout.

## Success Criteria

- Shared config/secrets behavior is identical across apps and `riops`.
- Contracts are discoverable and referenced from system specs.

