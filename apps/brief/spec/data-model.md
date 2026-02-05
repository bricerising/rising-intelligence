# Data Model: Brief Service

## Contracts

- Input: `summary.requests` (`SummaryRequest`)
- Output: `summary.results` (`BriefResult`)

Canonical wire contracts:

- `packages/shared/contracts/proto/rising_intelligence/v1/contracts.proto`

## Evidence model (MVP)

Evidence SHOULD be represented as:

- bounded per-topic evidence items included in `SummaryRequest`:
  - `event_id`, `url`, `title`, short `text_excerpt`

so the brief can be audited and replayed.
