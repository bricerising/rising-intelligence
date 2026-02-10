# Data Model: Ops CLI

The CLI is command-driven; it does not own durable storage.

Key inputs:

- env vars (loaded via `@rising-intelligence/shared`)
- arguments parsed from `process.argv`

Key outputs:

- stdout/stderr (human-readable logs)
- exit code (0 success, non-zero failure)
- side-effecting network operations (for example: Schema Registry publish and Kafka `summary.requests` publish)

`brief trigger` emits one of two `SummaryRequest` shapes:

1. **Query mode (default)**:
   - `topics: []`
   - `query.lookback_days` (default `7`, max `30`)
   - `query.topic_globs` (default `["*"]`)
   - `query.max_events_per_topic` (optional)
2. **Explicit mode (optional)**:
   - `topics[]` with metric/evidence fields from CLI flags
