# Data Model: Brief Service

## Contracts

- Input: `summary.requests` (request envelope; TBD)
- Output: `Brief` (defined in `specs/001-real-time-personal-intelligence-system.md`)

## Evidence model (MVP)

Evidence SHOULD be represented as:

- `event_id` references into `events.raw`, and/or
- URLs to curated sources,

so the brief can be audited and replayed.
