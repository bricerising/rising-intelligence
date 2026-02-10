# Data Model: Brief Service

## Contracts

- Input: `summary.requests` (`SummaryRequest`)
- Output: `summary.results` (`BriefResult`)

Canonical wire contracts:

- `packages/shared/contracts/proto/rising_intelligence/v1/contracts.proto`

## SummaryRequest Modes

The Brief service supports two request modes:

1. **Query mode (default)**: `topics` omitted or empty.
2. **Explicit mode**: `topics` provided (current backward-compatible path).

### Query Parameters (proposed extension)

`SummaryRequest` SHOULD support an optional `query` object:

```json
{
  "query": {
    "lookback_days": 7,
    "topic_globs": ["*"],
    "max_events_per_topic": 25
  }
}
```

Semantics:

- `lookback_days` (optional): number of UTC days to scan from `requested_at`; default `BRIEF_DEFAULT_LOOKBACK_DAYS=7`, max `BRIEF_MAX_LOOKBACK_DAYS=30`.
- `topic_globs` (optional): glob filters over canonical topic keys (for example `aws.*`, `ai.*`, `*.bedrock`).
- `max_events_per_topic` (optional): per-topic fetch cap to bound context and DB work.

## Evidence model

Evidence SHOULD be represented as:

- bounded per-topic evidence items, either:
  - included in `SummaryRequest` (explicit mode), or
  - fetched by Brief from Postgres `raw_events` (query mode)
  - `event_id`, `url`, `title`, short `text_excerpt`

so the brief can be audited and replayed.

## Query-Mode Storage Reads

When `topics` are omitted/empty, Brief resolves ranked topics from Trend outputs first, then fetches evidence:

1. Read `trend_snapshots` for `TREND_WINDOW_60M` within lookback window.
2. Apply `topic_globs` to topic keys before ranking.
3. Rank with recent-weighted average score (higher recent scores weigh more).
4. Select bounded topics (`budget.max_topics`/service cap).
5. Query `raw_events` for evidence on selected topics.

This keeps Trend service as the scoring source while allowing Brief to summarize across a lookback range.

`raw_events` query constraints:

- Time bound: `fetched_at >= requested_at - lookback_days`
- Topic bound: event `topics[]` matches at least one `topic_glob` (or all topics when no filter)
- Ordering: newest first for evidence selection
- Bounded selection: capped by `max_topics` and `max_evidence_per_topic` from budget/request config
