# Spec 008: Observability Contracts

**Created**: 2026-02-05
**Status**: Proposed

## Overview

This spec defines the exact contracts for metrics, logs, and traces across all services. Consistency is critical for dashboards and alerts to work correctly.

## Metric Naming Conventions

All metrics follow Prometheus naming conventions:

- **Prefix**: `ri_` (rising_intelligence) for application metrics
- **Suffix by type**:
  - `_total` for counters
  - `_seconds` for durations (use histograms)
  - `_bytes` for sizes
  - `_ratio` for percentages (0.0-1.0)
  - `_info` for metadata (gauge with labels)
- **Snake_case**: All names use snake_case
- **Labels**: Keep cardinality bounded (see limits below)

### Label Cardinality Limits

| Label | Max Values | Notes |
|-------|-----------|-------|
| `service` | 4 | collector, persister, trends, brief |
| `source` | 7 | rss, news, hackernews, reddit, github, bluesky, mastodon |
| `topic` | 30 | Only top topics; use "other" for rest |
| `window` | 3 | 15m, 60m, 24h |
| `status` | 3 | success, failure, skipped |
| `error_type` | 10 | Categorized errors only |

**Never use unbounded labels** like `event_id`, `url`, or `user_id`.

---

## Metrics by Service

### Collector Service

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `ri_collector_events_ingested_total` | counter | `source` | Events successfully published to Kafka |
| `ri_collector_events_failed_total` | counter | `source`, `error_type` | Events that failed to ingest |
| `ri_collector_poll_duration_seconds` | histogram | `source` | Time to complete one poll cycle |
| `ri_collector_poll_items_count` | histogram | `source` | Items returned per poll |
| `ri_collector_checkpoint_updated_total` | counter | `source` | Checkpoint updates |
| `ri_collector_rate_limit_backoff_total` | counter | `source` | Times rate limit caused backoff |
| `ri_collector_topics_extracted_total` | counter | `topic` | Topics extracted (top 30 only) |
| `ri_collector_up` | gauge | — | 1 if service is healthy |

**Error types**: `parse_error`, `network_error`, `rate_limit`, `auth_error`, `kafka_error`

### Persister Service

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `ri_persister_events_processed_total` | counter | `source` | Events written to Postgres |
| `ri_persister_events_skipped_total` | counter | `reason` | Events not written |
| `ri_persister_postgres_write_duration_seconds` | histogram | — | Postgres insert latency |
| `ri_persister_redis_write_duration_seconds` | histogram | — | Redis set latency |
| `ri_persister_consumer_lag` | gauge | `partition` | Messages behind latest |
| `ri_persister_batch_size` | histogram | — | Events per batch |
| `ri_persister_errors_total` | counter | `error_type` | Errors by category |
| `ri_persister_up` | gauge | — | 1 if service is healthy |

**Skip reasons**: `duplicate`, `malformed`, `postgres_error`

### Trends Service

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `ri_trends_events_processed_total` | counter | — | Events consumed |
| `ri_trends_duplicates_skipped_total` | counter | — | Dedup hits |
| `ri_trends_topic_volume` | gauge | `topic`, `window` | Current window volume (top 30) |
| `ri_trends_topic_score` | gauge | `topic`, `window` | Current score (top 30) |
| `ri_trends_snapshot_published_total` | counter | `window` | Snapshots to Kafka |
| `ri_trends_snapshot_duration_seconds` | histogram | `window` | Time to compute snapshot |
| `ri_trends_brief_triggered_total` | counter | `type` | Deprecated in request-driven mode (expected 0) |
| `ri_trends_brief_skipped_stale_data_total` | counter | — | Deprecated in request-driven mode (expected 0) |
| `ri_trends_consumer_lag` | gauge | `partition` | Messages behind latest |
| `ri_trends_baseline_compute_duration_seconds` | histogram | — | Baseline computation time |
| `ri_trends_up` | gauge | — | 1 if service is healthy |

**Trigger types** (legacy): `daily`, `threshold`

### Brief Service

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `ri_brief_generation_total` | counter | `status` | Briefs attempted |
| `ri_brief_generation_duration_seconds` | histogram | — | LLM call + processing time |
| `ri_brief_duplicates_skipped_total` | counter | — | Idempotency hits |
| `ri_brief_budget_remaining_usd` | gauge | — | Today's remaining budget |
| `ri_brief_budget_exceeded_total` | counter | — | Requests rejected for budget |
| `ri_brief_llm_tokens_total` | counter | `direction` | Input/output tokens |
| `ri_brief_llm_cost_usd_total` | counter | — | Estimated LLM spend |
| `ri_brief_highlights_count` | histogram | — | Topics per brief |
| `ri_brief_citations_count` | histogram | — | Citations per brief |
| `ri_brief_errors_total` | counter | `error_type` | Errors by category |
| `ri_brief_up` | gauge | — | 1 if service is healthy |

**Status**: `success`, `failure`, `skipped`
**Direction**: `input`, `output`
**Error types**: `llm_error`, `parse_error`, `budget_exceeded`, `timeout`, `postgres_error`

---

## Histogram Buckets

Standard bucket configurations:

```typescript
// Duration (seconds) - for API calls, processing
const DURATION_BUCKETS = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60];

// Duration (seconds) - for LLM calls (longer)
const LLM_DURATION_BUCKETS = [1, 5, 10, 30, 60, 120, 300];

// Count (items per batch)
const COUNT_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000];

// Size (bytes)
const SIZE_BUCKETS = [100, 1000, 10000, 100000, 1000000];
```

---

## Log Schema

All services MUST emit structured JSON logs with these required fields:

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | ISO8601 with milliseconds |
| `level` | string | debug, info, warn, error |
| `service` | string | Service name |
| `message` | string | Human-readable message |
| `traceId` | string | OpenTelemetry trace ID (if available) |
| `spanId` | string | OpenTelemetry span ID (if available) |

### Optional Context Fields

| Field | Type | When |
|-------|------|------|
| `source` | string | When processing source-specific data |
| `eventId` | string | When processing a specific event |
| `topic` | string | When processing topic-specific data |
| `requestId` | string | For Brief service requests |
| `kafkaTopic` | string | For Kafka operations |
| `partition` | number | For Kafka operations |
| `offset` | number | For Kafka operations |
| `error` | string | Error message (never include stack in production) |
| `errorCode` | string | Categorized error code |
| `durationMs` | number | Operation duration |

### Log Examples

**Info log**:
```json
{
  "timestamp": "2026-02-05T14:30:00.123Z",
  "level": "info",
  "service": "collector",
  "message": "Poll completed",
  "traceId": "abc123",
  "spanId": "def456",
  "source": "reddit",
  "itemsCount": 25,
  "durationMs": 1234
}
```

**Error log**:
```json
{
  "timestamp": "2026-02-05T14:30:00.123Z",
  "level": "error",
  "service": "brief",
  "message": "Brief generation failed",
  "traceId": "abc123",
  "spanId": "def456",
  "requestId": "req-789",
  "error": "OpenAI API rate limited",
  "errorCode": "llm_rate_limit",
  "retryCount": 2
}
```

### Log Levels

| Level | When to use |
|-------|-------------|
| `debug` | Detailed diagnostic info (disabled in prod) |
| `info` | Normal operations (poll completed, event processed) |
| `warn` | Degraded but recoverable (rate limit, retry, cache miss) |
| `error` | Failed operation that needs attention |

**Never log**:
- Secrets (API keys, passwords, tokens)
- PII (user emails, names, IPs in most cases)
- Full request/response bodies (too verbose)
- Stack traces in production (use error code instead)

---

## Trace Spans

All services instrument these operations:

### Collector

- `collector.poll` - One poll cycle
  - `collector.fetch` - HTTP call to source API
  - `collector.normalize` - Parse and normalize items
  - `collector.extract_topics` - Topic extraction
  - `collector.publish` - Kafka produce

### Persister

- `persister.process_batch` - One batch of messages
  - `persister.deserialize` - Parse Kafka message
  - `persister.write_postgres` - Postgres insert
  - `persister.write_redis` - Redis set

### Trends

- `trends.process_event` - One event
  - `trends.dedup_check` - Redis dedup lookup
  - `trends.increment_counters` - Redis INCR
  - `trends.update_evidence` - Redis ZADD
- `trends.compute_snapshot` - Snapshot generation
  - `trends.get_baselines` - Redis/Postgres baseline lookup
  - `trends.compute_scores` - Score calculation
  - `trends.publish_snapshot` - Kafka produce

### Brief

- `brief.process_request` - One brief request
  - `brief.check_idempotency` - Postgres lookup
  - `brief.check_budget` - Redis budget lookup
  - `brief.build_prompt` - Prompt construction
  - `brief.call_llm` - LLM API call
  - `brief.parse_response` - Response parsing
  - `brief.persist_result` - Postgres insert

---

## Alert Definitions

### Critical Alerts (page immediately)

| Alert | Condition | For |
|-------|-----------|-----|
| ServiceDown | `ri_collector_up == 0 OR ri_persister_up == 0 OR ri_trends_up == 0 OR ri_brief_up == 0` | 5m |
| KafkaUnavailable | Kafka connection errors > 0 for all services | 2m |
| PostgresUnavailable | Postgres connection errors > 0 for Persister/Trends/Brief | 2m |
| NoBriefToday | No successful brief in 24h AND failures > 0 | 1h |

### Warning Alerts (investigate soon)

| Alert | Condition | For |
|-------|-----------|-----|
| HighConsumerLag | `ri_persister_consumer_lag > 5000 OR ri_trends_consumer_lag > 5000` | 15m |
| BriefGenerationFailed | `increase(ri_brief_generation_total{status="failure"}[1h]) > 0` | 0m |
| HighErrorRate | `rate(ri_collector_events_failed_total[5m]) > 0.1 OR rate(ri_persister_errors_total[5m]) > 0.1 OR rate(ri_trends_errors_total[5m]) > 0.1 OR rate(ri_brief_errors_total[5m]) > 0.1` | 10m |
| LLMBudgetLow | `ri_brief_budget_remaining_usd < 0.10` | 0m |
| RetentionCleanupFailed | No cleanup in 48h | 0m |
| RedisMemoryHigh | Redis used_memory > 80% maxmemory | 5m |

### Example Prometheus Alert Rules

```yaml
groups:
  - name: rising-intelligence
    rules:
      - alert: ServiceDown
        expr: ri_collector_up == 0 or ri_persister_up == 0 or ri_trends_up == 0 or ri_brief_up == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "One or more services are down"
          description: "At least one service has been unhealthy for more than 5 minutes"

      - alert: HighConsumerLag
        expr: ri_persister_consumer_lag > 5000 or ri_trends_consumer_lag > 5000
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "High consumer lag detected"
          description: "Persister or Trends lag has exceeded the threshold"

      - alert: BriefGenerationFailed
        expr: increase(ri_brief_generation_total{status="failure"}[1h]) > 0
        labels:
          severity: warning
        annotations:
          summary: "Brief generation failed"
          description: "Check brief service logs for details"

      - alert: NoBriefToday
        expr: |
          increase(ri_brief_generation_total{status="success"}[24h]) == 0
          and
          increase(ri_brief_generation_total{status="failure"}[24h]) > 0
        for: 1h
        labels:
          severity: critical
        annotations:
          summary: "No brief generated in 24 hours"
          description: "Brief generation has been failing. Check logs."
```

---

## Dashboard Panels

### System Overview Dashboard

1. **Service Health** - Stat panel showing up/down for each service
2. **Consumer Lag** - Time series, one line per service
3. **Events Ingested** - Time series, stacked by source
4. **Error Rate** - Time series, one line per service
5. **Kafka Topic Sizes** - Bar gauge per topic

### Trends Dashboard

1. **Top Trending Topics** - Table with topic, volume, acceleration, score
2. **Topic Volume Over Time** - Time series, one line per top 5 topics
3. **Topic Score Over Time** - Time series, one line per top 5 topics
4. **Snapshot Generation** - Stat showing last snapshot time
5. **Window Coverage** - Gauge showing data freshness

### Brief Dashboard

1. **Latest Brief** - Text panel showing most recent brief content
2. **Brief History** - Table of recent briefs with status
3. **LLM Cost** - Time series of daily spend
4. **Budget Remaining** - Gauge showing today's budget
5. **Generation Latency** - Histogram heatmap

---

## Implementation Notes

### TypeScript Metrics Library

Use `prom-client` with a shared registry:

```typescript
import { Registry, Counter, Histogram, Gauge } from 'prom-client';

const registry = new Registry();

// Example counter
const eventsIngested = new Counter({
  name: 'ri_collector_events_ingested_total',
  help: 'Events successfully published to Kafka',
  labelNames: ['source'],
  registers: [registry],
});

// Example histogram
const pollDuration = new Histogram({
  name: 'ri_collector_poll_duration_seconds',
  help: 'Time to complete one poll cycle',
  labelNames: ['source'],
  buckets: [0.1, 0.5, 1, 2.5, 5, 10, 30],
  registers: [registry],
});

// Expose metrics endpoint
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', registry.contentType);
  res.end(await registry.metrics());
});
```

### Logging Library

Use `pino` for structured JSON logs:

```typescript
import pino from 'pino';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  base: {
    service: 'collector',
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: {
    level: (label) => ({ level: label }),
  },
});

// With trace context
logger.info({
  traceId: span.spanContext().traceId,
  spanId: span.spanContext().spanId,
  source: 'reddit',
  itemsCount: 25,
}, 'Poll completed');
```

### OpenTelemetry Setup

```typescript
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';

const sdk = new NodeSDK({
  serviceName: 'collector',
  traceExporter: new OTLPTraceExporter({
    url: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318/v1/traces',
  }),
});

sdk.start();
```
