# Feature Specification: Trends Service

**Service**: `@rising-intelligence/trends`  
**Created**: 2026-02-05  
**Status**: Planned

## Overview

The Trends Service consumes `RawEvent` from `events.raw`, extracts topics, computes windowed metrics (volume + acceleration), and publishes ranked `TrendSnapshot` messages to `trends.snapshots`.

## User Scenarios & Testing

### User Story 1 — Top trends snapshot (Priority: P1)

As an operator, I can see the Top N trending topics over a time window so I can quickly understand what is gaining traction.

**Independent Test**: Feed a synthetic spike for a topic and verify it appears in the next snapshot with high acceleration.

**Acceptance Scenarios**:

1. **Given** events flowing, **When** the service runs, **Then** it publishes snapshots every N minutes.
2. **Given** a topic’s volume doubles window-over-window, **When** snapshots are produced, **Then** the topic ranks higher than stable-volume topics.
3. **Given** the service restarts, **When** it resumes, **Then** it continues producing snapshots without corrupting counts (idempotent processing).

### Edge Cases

- High-frequency topics dominate volume (“AI” always-on) → require baseline normalization.
- Alias collisions (“Bedrock” vs unrelated “bedrock”) → require matcher tuning.
- Backlog/consumer lag → snapshots become stale.

## Constitution Requirements

- **Determinism**: given the same event stream + allowlist, computed snapshots are stable.
- **Idempotency**: safe under at-least-once delivery; duplicates do not inflate long-term results.
- **Evidence**: snapshots include evidence references (event IDs / URLs) for traceability.

## Requirements

### Functional Requirements

- **FR-001**: Service MUST consume `events.raw`.
- **FR-002**: Service MUST compute `15m` and `60m` windows in MVP.
- **FR-003**: Service MUST publish `TrendSnapshot` to `trends.snapshots`.
- **FR-004**: Service MUST support an allowlist + aliases for topic extraction.
- **FR-005**: Service SHOULD compute baselines (7-day) once enough data exists.

### Non-Functional Requirements

- **NFR-001**: Snapshot cadence SHOULD be <= 5 minutes.
- **NFR-002**: Processing MUST not fall behind indefinitely; consumer lag is observable.

## Success Criteria

- **SC-001**: Top trends are plausible and evidence-backed.
- **SC-002**: Lag stays under a configured threshold in local dev.
