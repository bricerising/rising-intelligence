# Feature Specification: Brief Service (LLM Summarizer)

**Service**: `@rising-intelligence/brief`  
**Created**: 2026-02-05  
**Status**: Planned

## Overview

The Brief Service consumes summary requests and produces evidence-grounded briefs:

- **Daily brief**: scheduled summary of top trends with citations and suggested actions.
- **Flash brief** (optional): short summary when a topic spikes.

The service is intentionally isolated so LLM latency/failures do not impact ingestion or trend computation.

## User Scenarios & Testing

### User Story 1 — Daily brief (Priority: P1)

As an operator, I receive a daily brief that explains what happened and what I should do next.

**Independent Test**: Trigger a daily brief request and verify the resulting brief contains citations for each trend.

**Acceptance Scenarios**:

1. **Given** Top N trends and evidence, **When** a daily request arrives, **Then** a brief is published to `summary.results`.
2. **Given** the LLM call fails, **When** retried within budget, **Then** the service recovers and emits a failure record if it ultimately cannot produce a brief.
3. **Given** a configured daily budget, **When** multiple requests arrive, **Then** the service enforces the budget (drops/degrades gracefully).

### Edge Cases

- Evidence set contains duplicates or near-duplicates.
- Evidence is missing (no curated sources for a topic).
- Model returns hallucinated facts → require grounding and “say uncertain” behavior.

## Constitution Requirements

- **Grounding**: every highlight MUST include citations.
- **Budgeting**: enforce daily cost/token budgets.
- **Safety**: do not emit secrets; redact sensitive content if configured.

## Requirements

### Functional Requirements

- **FR-001**: Service MUST consume `summary.requests`.
- **FR-002**: Service MUST produce `Brief` messages to `summary.results`.
- **FR-003**: Service MUST include citations for each highlight.
- **FR-004**: Service SHOULD store the prompt inputs/metadata for audit (without leaking secrets).

### Non-Functional Requirements

- **NFR-001**: Daily brief SHOULD complete within 2 minutes (LLM-dependent).
- **NFR-002**: Failures MUST be observable (metrics + logs + traces).

## Success Criteria

- **SC-001**: Briefs are consistently actionable and evidence-grounded.
- **SC-002**: Spend stays within configured budget in 3-day local soak.
