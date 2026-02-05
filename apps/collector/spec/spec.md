# Feature Specification: Collector Service

**Service**: `@rising-intelligence/collector`  
**Created**: 2026-02-05  
**Status**: Planned

## Overview

The Collector Service ingests external sources (RSS/blogs, Hacker News, Reddit, etc.), normalizes all items into the canonical `RawEvent` contract, and publishes them to the event bus (`events.raw`).

MVP intentionally concentrates multiple source adapters into one deployable to keep operational overhead reasonable. If/when a source becomes high-volume or operationally noisy, it can be split into its own service without changing downstream contracts.

## User Scenarios & Testing

### User Story 1 — Continuous ingestion (Priority: P1)

As an operator, I can run the collector continuously so new items from configured sources appear in the pipeline quickly and reliably.

**Independent Test**: Start the stack and verify new RSS items and HN stories appear on `events.raw` within 60 seconds.

**Acceptance Scenarios**:

1. **Given** valid source configuration, **When** the collector runs for 30 minutes, **Then** `events.raw` receives valid `RawEvent` messages for each enabled source.
2. **Given** a transient 429/5xx from a source, **When** the collector retries, **Then** it backs off with jitter and resumes without crashing.
3. **Given** the collector restarts, **When** it resumes, **Then** it does not re-emit more than at-least-once duplicates for already-seen items (dedupe by `event_id`).

### Edge Cases

- Source clock skew (published timestamps inconsistent).
- RSS feeds with malformed dates or missing GUIDs.
- Reddit API rate limiting and “after” cursor drift.
- Duplicate URLs across sources (same article syndicated).

## Constitution Requirements

- **Schema validation**: every emitted message MUST conform to `RawEvent`.
- **Idempotency**: emitted `event_id` MUST be stable and source-derived.
- **Backoff**: transient upstream failures MUST not cascade into tight retry loops.
- **No secrets**: credentials MUST never be logged or emitted to Kafka.

## Requirements

### Functional Requirements

- **FR-001**: Service MUST support ingesting from RSS/Atom feeds.
- **FR-002**: Service MUST support ingesting from Hacker News (poll API).
- **FR-003**: Service MUST support ingesting from Reddit (poll new posts/comments).
- **FR-004**: Service MUST publish normalized events to `events.raw`.
- **FR-005**: Service MUST emit parse/normalize failures to `events.raw.dlq` with safe context.
- **FR-006**: Service SHOULD mirror ingested events to Loki as structured logs for search/debug.

### Non-Functional Requirements

- **NFR-001**: Ingest loop SHOULD make new items available within 60 seconds (best-effort).
- **NFR-002**: Service MUST tolerate upstream downtime without data corruption.
- **NFR-003**: Service MUST be safe under restarts (checkpoint cursors; tolerate duplicates).

## Success Criteria

- **SC-001**: 0 unhandled crashes in 24 hours of local soak.
- **SC-002**: `events.raw` shows steady flow and stable schema across sources.

## Assumptions

- Kafka/Redpanda is reachable from the service network.
- Source credentials (if needed) are provided via env vars.
