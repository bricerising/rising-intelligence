# Spec 002: Unified Observability Stack (LGTM + OpenTelemetry)

**Created**: 2026-02-05
**Updated**: 2026-02-05
**Status**: Proposed

## Overview

This project uses the LGTM stack (Loki, Grafana, Tempo, Mimir) with OpenTelemetry as the instrumentation/transport layer.

The goal is to make the system **operationally legible** from day 1:

- debug ingestion failures and rate limiting,
- detect consumer lag / backlog,
- understand trend computation timing and correctness,
- correlate "a brief was produced" back to the evidence that drove it.

## Data Storage Responsibilities

| Data Type | Storage | Notes |
|-----------|---------|-------|
| **Application logs** | Loki | Service debug logs, errors, trace correlation |
| **Raw events** | Postgres | Searchable event archive (see spec 005) |
| **Metrics** | Mimir | Prometheus-compatible metrics |
| **Traces** | Tempo | Distributed traces |

**Important**: Loki is for **application logs only**, not for raw event storage. Raw events (tweets, posts, articles) are stored in Postgres for efficient querying. This avoids dual-write complexity and leverages SQL's superior query capabilities.

## Architecture

```
┌────────────────────────┐      OTLP (gRPC/HTTP)      ┌─────────────────────────┐
│  Services (apps/*)      │───────────────────────────►│  OpenTelemetry Collector│
│  collector / trends /   │                            │                         │
│  brief                  │◄───────────────────────────┤                         │
└───────────┬────────────┘                            └─────┬────────┬────────┬─┘
            │                                               │        │        │
            │ stdout logs (optional)                         ▼        ▼        ▼
            ▼                                         ┌────────┐┌────────┐┌────────┐
      ┌───────────┐                                  │ Mimir   ││ Loki   ││ Tempo  │
      │ Promtail   │─────────────────────────────────►│ Metrics ││ Logs   ││ Traces │
      │ (local dev)│                                  └────┬────┘└────┬────┘└────┬────┘
      └───────────┘                                       ▼          ▼          ▼
                                                     ┌─────────────────────────┐
                                                     │        Grafana          │
                                                     │ (Dashboards + Explore)  │
                                                     └─────────────────────────┘
```

Notes:

- Services SHOULD emit OTLP traces/metrics/logs to the collector.
- In local dev, Promtail MAY scrape container stdout for “everything else” and push to Loki.
- Grafana is provisioned from `infra/grafana` so dashboards/datasources are versioned.

## Conventions

### Service naming

- Each service MUST set a stable `service.name` (OTel resource) matching its `apps/<service>` folder name.

### Logs

Logs MUST be structured JSON and include:

- `service`
- `traceId` and `spanId` (for log-to-trace navigation)
- `eventId` (when logging RawEvent handling)
- `topic` (when logging trend handling)
- `kafkaTopic`, `partition`, `offset` (for consumers)

### Metrics (minimum)

All services MUST expose `/metrics` for a local scrape (Prometheus format) OR export metrics via OTLP.

Minimum recommended metrics:

- Collector:
  - `ri_collector_events_ingested_total{source=...}`
  - `ri_collector_events_failed_total{source=...}`
  - `ri_collector_poll_duration_seconds{source=...}`
- Trends:
  - `ri_trends_snapshot_duration_seconds{window=...}`
  - `ri_trends_snapshot_published_total{window=...}`
  - `ri_trends_topic_score{topic=...,window=...}` (Top N only; bounded cardinality)
  - `ri_trends_topic_volume{topic=...,window=...}` (Top N only; bounded cardinality)
  - `ri_trends_consumer_lag{partition=...}` (or a proxy gauge)
- Brief:
  - `ri_brief_generation_total{status=\"success\"|\"failure\"|\"skipped\"}`
  - `ri_brief_generation_duration_seconds`
  - `ri_brief_llm_tokens_total{direction=\"input\"|\"output\"}`
  - `ri_brief_errors_total{error_type=...}`

### Traces

Traces SHOULD be created for:

- source fetch loops (per poll/stream cycle),
- event publish to Kafka,
- windowed trend computation runs,
- brief generation runs (including the LLM call).

## Local development

Local Compose SHOULD include:

- Grafana, Loki, Tempo, Mimir
- OpenTelemetry Collector (OTLP ingest + metric scrape)
- Promtail (optional, local dev only)

Provisioning is stored in:

- `infra/grafana/provisioning/datasources/`
- `infra/grafana/provisioning/dashboards/`
- `infra/grafana/dashboards/`

## Success criteria

- 100% of error logs include `traceId`.
- Grafana Explore can jump from log line → trace.
- `up` (or equivalent) is visible for all running services.
