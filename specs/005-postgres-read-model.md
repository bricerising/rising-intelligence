# Spec 005: Postgres Read Model (Grafana Datasource)

**Created**: 2026-02-05  
**Status**: Proposed

## Overview

Postgres is used as the queryable store for:

- trend snapshots (historical, for charts/tables), and
- brief results (latest summary + history).

Grafana is provisioned with a Postgres datasource so dashboards can show:

- metrics (Prometheus/Mimir),
- logs (Loki),
- traces (Tempo), and
- summaries/trend history (Postgres),

all in one UI.

## Local dev wiring

- Postgres container runs in `docker-compose.yml` as `postgres`.
- Initial schema is created on first DB init from:
  - `infra/postgres/init/0001_init.sql`
- Grafana datasource provisioning lives at:
  - `infra/grafana/provisioning/datasources/datasource.yaml` (uid: `POSTGRES`)

## Tables (MVP)

### `trend_snapshots`

Append-only snapshot storage.

- `generated_at` (timestamptz): when the snapshot was produced
- `window` (text): `15m` | `60m` | `24h`
- `snapshot` (jsonb): full `TrendSnapshot` as JSON

### `brief_results`

Append-only result storage for brief production.

- `request_id` (text, PK): from `SummaryRequest`
- `produced_at` (timestamptz)
- `status` (text): `success` | `failure`
- `result` (jsonb): full `BriefResult` as JSON

## Views (Grafana convenience)

These views assume JSON field names match the canonical model:

- `trend_topic_metrics`: expands `trend_snapshots.snapshot.topics[]` into rows.
- `brief_highlights`: expands `brief_results.result.brief.highlights[]` into rows.

See `infra/postgres/init/0001_init.sql` for definitions.

## Write responsibilities

- Trends service MUST persist each produced snapshot to `trend_snapshots`.
- Brief service MUST persist each produced result to `brief_results`.

The Postgres write path is part of the “read model” and does not replace Kafka as the event bus.

## Retention (suggested)

For a single-operator home deployment:

- `trend_snapshots`: 30–90 days
- `brief_results`: 90+ days

Retention can be implemented later via a periodic job.

