# Implementation Plan: Ops CLI

## Phase 1: Schema Registry bootstrap (MVP)

- `schema-registry publish-protos`
  - idempotent subject registration (skip if up-to-date)
  - retries + timeouts for SR HTTP calls

## Phase 2: LGTM helpers (post-MVP)

- `lgtm urls` (already)
- `lgtm health` (planned): check Grafana/Loki/Tempo/Mimir readiness

## Phase 3: Infra ops (post-MVP)

- `kafka ensure-topics`
- `postgres migrate` (for read model evolution)

