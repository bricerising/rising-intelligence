# Implementation Plan: Ops CLI

## Phase 1: Schema Registry bootstrap (MVP)

- `schema-registry publish-protos`
  - idempotent subject registration (skip if up-to-date)
  - retries + timeouts for SR HTTP calls

## Phase 2: Brief trigger helpers (MVP+)

- `brief trigger`
  - build a valid `SummaryRequest` payload from CLI flags
  - publish to Kafka `summary.requests`
  - support dry-run payload preview

## Phase 3: LGTM helpers (post-MVP)

- `lgtm urls` (already)
- `lgtm health` (planned): check Grafana/Loki/Tempo/Mimir readiness

## Phase 4: Infra ops (post-MVP)

- `kafka ensure-topics`
- `postgres migrate` (for read model evolution)
