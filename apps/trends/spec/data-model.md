# Data Model: Trends Service

## Contracts

- Input: `RawEvent`
- Output: `TrendSnapshot`

Both contracts are defined in `specs/001-real-time-personal-intelligence-system.md`.

## Derived state

- Rolling windows for each topic (15m/60m).
- Optional baseline volumes per topic (7d).

## Evidence selection (MVP)

For each topic, keep a small “evidence buffer” (IDs/URLs) for the current window:

- prefer high-engagement items when available
- include at least one curated source when possible
