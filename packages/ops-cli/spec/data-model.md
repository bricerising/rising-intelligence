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
   - `query.topic_globs` (default `["*"]`; may be derived from one or more `--feed-config` YAML files and unioned with explicit `--topic-globs`)
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

`events enrich` uses:

- **Inputs**:
  - Postgres `raw_events` rows (full event fields required for enrichment transforms)
  - Topics allowlist YAML when `retag` step is enabled
- **Behavioral pipeline**:
  - Ordered step execution (default: `retag,quality`)
  - `retag`: recompute `tags/topics` from allowlist
  - `quality`: apply ingest-quality normalization/annotation rules (URL/text/lang/topic fallback metadata)
- **Outputs**:
  - In-place updates to changed fields (`tags`, `topics`, `url`, `text`, `lang`, `source_meta`)
- **Execution controls**:
  - `--steps <csv>` to choose/sequence steps
  - `--missing-only` to restrict updates to rows with empty `tags/topics`
  - optional `--source`, `--limit`, `--batch-size`
  - `--dry-run` preview mode

`db snapshot` uses:

- **Inputs**:
  - Postgres connection info (`DATABASE_URL` or `postgres-*` flags)
  - Output target directory (`--output-dir` or `POSTGRES_SNAPSHOT_DIR`)
- **Behavior**:
  - Runs `pg_dump` in custom/compressed format (`-Fc`)
  - Writes timestamped snapshot files (`postgres-<db>-<utc>.dump`)
  - Optionally appends sanitized labels to filenames
  - Optionally prunes snapshots older than `--retention-days`
  - Supports long-running loop mode (`--loop`) for scheduled snapshots
- **Outputs**:
  - Filesystem snapshots in output directory
  - Summary logs (file path, size, prune count)
- **Execution controls**:
  - `--dry-run` for non-mutating planning
  - `--interval-seconds` for loop cadence (default daily / 86400)

Feed-config derivation rules for `brief trigger`:

- Parse all `--feed-config` YAML files (repeatable flag).
- Traverse all sections and collect non-empty `topics` arrays.
- Ignore empty arrays and warn.
- Include topics from disabled feed entries.
- Union derived values with explicit `--topic-globs`.
- Fail if any selected feed-config file is missing/unreadable.
