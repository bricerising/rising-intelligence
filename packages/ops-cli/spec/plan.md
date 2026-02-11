# Implementation Plan: Ops CLI

## Phase 1: Schema Registry bootstrap (MVP)

- `schema-registry publish-protos`
  - idempotent subject registration (skip if up-to-date)
  - retries + timeouts for SR HTTP calls

## Phase 2: Brief trigger helpers (MVP+)

- `brief trigger`
  - build a valid query-mode `SummaryRequest` payload from CLI flags by default
  - support `lookback_days` + `topic_globs` + `max_events_per_topic`
  - keep explicit topic/evidence mode as opt-in compatibility
  - publish to Kafka `summary.requests`
  - support dry-run payload preview

## Phase 3: LGTM helpers (post-MVP)

- `lgtm urls` (already)
- `lgtm health` (planned): check Grafana/Loki/Tempo/Mimir readiness

## Phase 4: Infra ops (post-MVP)

- `kafka ensure-topics`
- `postgres migrate` (for read model evolution)

## Phase 5: Topic maintenance (MVP+)

- `topics list`
  - inspect canonical topics currently materialized in `raw_events`
  - support optional counts + minimum threshold filter
- `topics retag`
  - recompute `raw_events.tags` and `raw_events.topics` from allowlist rules
  - default to missing tags/topics for safe backfill
  - support `--all`, `--source`, `--limit`, `--batch-size`, and `--dry-run`

## Phase 6: Feed-config to topic-glob derivation (POS source-pack)

- `brief trigger --feed-config <path>` (repeatable)
  - parse one or more feed YAML files
  - derive globs from all non-empty `topics` arrays across all sections
  - include entries regardless of `enabled` status
  - union with explicit `--topic-globs`
  - ignore empty `topics: []` with warnings
  - fail fast when a selected feed-config path is missing/unreadable
  - if derivation is empty but explicit globs exist, proceed with warning
  - if both are absent, keep default wildcard (`*`)
