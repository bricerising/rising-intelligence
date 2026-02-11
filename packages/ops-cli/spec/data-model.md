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
   - `report.timezone` (optional IANA timezone string)
   - `report.start_at` / `report.end_at` (optional ISO8601 framing bounds)
2. **Explicit mode (optional)**:
   - `topics[]` with metric/evidence fields from CLI flags

`topics retag` uses:

- **Inputs**:
  - Postgres `raw_events` rows (`id`, `title`, `text`, `tags`, `topics`)
  - Topics allowlist YAML (default: `infra/config/topics.allowlist.yaml`)
- **Outputs**:
  - `raw_events.tags` and `raw_events.topics` updated in-place to match extracted canonical topics
- **Execution controls**:
  - `--dry-run` for non-mutating preview
  - `--all` or default missing-only mode
  - optional `--source`, `--limit`, `--batch-size`
