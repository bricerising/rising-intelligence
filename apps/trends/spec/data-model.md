# Data Model: Trends Service

## Contracts

- Input: `RawEvent`
- Output: `TrendSnapshot`

Both contracts are defined in `specs/001-real-time-personal-intelligence-system.md`.

## Derived state

- Rolling windows for each topic (15m/60m).
- Optional baseline volumes per topic (30d, day-of-week/hour aware).
- A bounded evidence buffer per topic/window (for `SummaryRequest` construction).

## Idempotency (MVP)

Trends MUST tolerate at-least-once delivery without double-counting:

- primary defense: Collector dedupe by stable `event_id`
- secondary defense: Redis-based `event_id` dedupe with TTL (window + buffer)

## Evidence selection (MVP)

For each topic, keep a small “evidence buffer” (IDs/URLs) for the current window:

- prefer high-engagement items when available
- include at least one curated source when possible
- include a short `text_excerpt` so the Brief service does not need random-access reads in MVP
