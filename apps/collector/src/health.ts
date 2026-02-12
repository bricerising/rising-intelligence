import type { Server } from "node:http";
import type { Logger } from "pino";
import {
  startHealthServer as startSharedHealthServer,
  createHistogram,
  observeHistogram,
  formatHistogram,
  quoteMetricLabelValue,
  type HealthHandlers,
  type HistogramState,
} from "@rising-intelligence/shared";
import { getConfig } from "./config.js";

const DURATION_BUCKETS_SECONDS = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60];
const COUNT_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000];
const TOPIC_LABEL_CARDINALITY_LIMIT = 30;
const OTHER_TOPIC_LABEL = "other";

export interface HealthStatus {
  status: "healthy" | "degraded" | "unhealthy";
  checks: {
    kafka: "ok" | "error";
    checkpoints: "ok" | "error";
    allowlist: "ok" | "error";
  };
  uptime_seconds: number;
  last_event_at?: string;
  sources: Record<string, SourceHealth>;
}

export interface SourceHealth {
  status: "healthy" | "degraded" | "error";
  last_poll_at?: string;
  items_fetched?: number;
  error_message?: string;
}

export type CollectorErrorType =
  | "parse_error"
  | "kafka_error"
  | "auth_error"
  | "rate_limit"
  | "network_error";

interface RssFeedErrorMetric {
  feed: string;
  feedUrl: string;
  errorType: CollectorErrorType;
  count: number;
}

export interface Metrics {
  eventsIngested: Map<string, number>;
  eventsFailed: Map<string, Map<string, number>>;
  rssFeedErrors: Map<string, RssFeedErrorMetric>;
  pollDurationSeconds: Map<string, HistogramState>;
  pollItemsCount: Map<string, HistogramState>;
  checkpointUpdated: Map<string, number>;
  rateLimitBackoff: Map<string, number>;
  topicsExtracted: Map<string, number>;
}

export function createMetrics(): Metrics {
  return {
    eventsIngested: new Map(),
    eventsFailed: new Map(),
    rssFeedErrors: new Map(),
    pollDurationSeconds: new Map(),
    pollItemsCount: new Map(),
    checkpointUpdated: new Map(),
    rateLimitBackoff: new Map(),
    topicsExtracted: new Map(),
  };
}

export interface HealthContext {
  startTime: number;
  kafkaHealthy: boolean;
  checkpointsHealthy: boolean;
  allowlistHealthy: boolean;
  lastEventAt?: Date;
  sourceHealth: Map<string, SourceHealth>;
  metrics: Metrics;
}

export function createHealthContext(): HealthContext {
  return {
    startTime: Date.now(),
    kafkaHealthy: false,
    checkpointsHealthy: false,
    allowlistHealthy: false,
    sourceHealth: new Map(),
    metrics: createMetrics(),
  };
}

function getOrCreateHistogram(
  map: Map<string, HistogramState>,
  source: string,
  buckets: number[]
): HistogramState {
  const existing = map.get(source);
  if (existing) {
    return existing;
  }

  const created = createHistogram(buckets);
  map.set(source, created);
  return created;
}

function createRssFeedErrorKey(
  feed: string,
  feedUrl: string,
  errorType: CollectorErrorType
): string {
  return `${feed}\u0000${feedUrl}\u0000${errorType}`;
}

export function getHealthStatus(ctx: HealthContext): HealthStatus {
  const sources: Record<string, SourceHealth> = {};
  for (const [name, health] of ctx.sourceHealth) {
    sources[name] = health;
  }

  let status: "healthy" | "degraded" | "unhealthy";
  if (!ctx.kafkaHealthy || !ctx.checkpointsHealthy) {
    status = "unhealthy";
  } else if (!ctx.allowlistHealthy) {
    status = "degraded";
  } else {
    const unhealthySources = Array.from(ctx.sourceHealth.values()).filter(
      (s) => s.status === "error"
    );
    status = unhealthySources.length > 0 ? "degraded" : "healthy";
  }

  return {
    status,
    checks: {
      kafka: ctx.kafkaHealthy ? "ok" : "error",
      checkpoints: ctx.checkpointsHealthy ? "ok" : "error",
      allowlist: ctx.allowlistHealthy ? "ok" : "error",
    },
    uptime_seconds: Math.floor((Date.now() - ctx.startTime) / 1000),
    last_event_at: ctx.lastEventAt?.toISOString(),
    sources,
  };
}

function toUnixTimestampSeconds(value: string | undefined): number {
  if (!value || value.trim().length === 0) {
    return 0;
  }

  const parsedMs = Date.parse(value);
  if (Number.isNaN(parsedMs)) {
    return 0;
  }

  return Math.floor(parsedMs / 1000);
}

export function formatMetrics(ctx: HealthContext): string {
  const lines: string[] = [];

  lines.push("# HELP ri_collector_events_ingested_total Events successfully published to Kafka");
  lines.push("# TYPE ri_collector_events_ingested_total counter");
  for (const [source, count] of ctx.metrics.eventsIngested) {
    lines.push(
      `ri_collector_events_ingested_total{source="${quoteMetricLabelValue(source)}"} ${count}`
    );
  }

  lines.push("# HELP ri_collector_events_failed_total Events that failed to ingest");
  lines.push("# TYPE ri_collector_events_failed_total counter");
  for (const [source, errorMap] of ctx.metrics.eventsFailed) {
    for (const [errorType, count] of errorMap) {
      lines.push(
        `ri_collector_events_failed_total{source="${quoteMetricLabelValue(source)}",error_type="${quoteMetricLabelValue(errorType)}"} ${count}`
      );
    }
  }

  lines.push("# HELP ri_collector_rss_feed_errors_total RSS feed-level errors");
  lines.push("# TYPE ri_collector_rss_feed_errors_total counter");
  for (const metric of ctx.metrics.rssFeedErrors.values()) {
    lines.push(
      `ri_collector_rss_feed_errors_total{source="rss",feed="${quoteMetricLabelValue(metric.feed)}",feed_url="${quoteMetricLabelValue(metric.feedUrl)}",error_type="${quoteMetricLabelValue(metric.errorType)}"} ${metric.count}`
    );
  }

  let wrotePollDurationMetadata = false;
  for (const [source, histogram] of ctx.metrics.pollDurationSeconds) {
    lines.push(
      ...formatHistogram(
        "ri_collector_poll_duration_seconds",
        "Poll cycle duration in seconds",
        histogram,
        { source },
        !wrotePollDurationMetadata
      )
    );
    wrotePollDurationMetadata = true;
  }

  let wrotePollItemsMetadata = false;
  for (const [source, histogram] of ctx.metrics.pollItemsCount) {
    lines.push(
      ...formatHistogram(
        "ri_collector_poll_items_count",
        "Items returned per poll cycle",
        histogram,
        { source },
        !wrotePollItemsMetadata
      )
    );
    wrotePollItemsMetadata = true;
  }

  lines.push("# HELP ri_collector_checkpoint_updated_total Checkpoint updates");
  lines.push("# TYPE ri_collector_checkpoint_updated_total counter");
  for (const [source, count] of ctx.metrics.checkpointUpdated) {
    lines.push(
      `ri_collector_checkpoint_updated_total{source="${quoteMetricLabelValue(source)}"} ${count}`
    );
  }

  lines.push("# HELP ri_collector_rate_limit_backoff_total Times rate limits triggered backoff");
  lines.push("# TYPE ri_collector_rate_limit_backoff_total counter");
  for (const [source, count] of ctx.metrics.rateLimitBackoff) {
    lines.push(
      `ri_collector_rate_limit_backoff_total{source="${quoteMetricLabelValue(source)}"} ${count}`
    );
  }

  lines.push("# HELP ri_collector_topics_extracted_total Topics extracted by key");
  lines.push("# TYPE ri_collector_topics_extracted_total counter");
  for (const [topic, count] of ctx.metrics.topicsExtracted) {
    lines.push(
      `ri_collector_topics_extracted_total{topic="${quoteMetricLabelValue(topic)}"} ${count}`
    );
  }

  const sourceEntries = [...ctx.sourceHealth.entries()].sort(([left], [right]) =>
    left.localeCompare(right)
  );

  const sourceLastSuccessTimestamp = sourceEntries.map(([source, health]) => ({
    source,
    health,
    lastSuccessTimestamp: toUnixTimestampSeconds(health.last_poll_at),
  }));

  lines.push("# HELP ri_collector_last_success_timestamp Last successful poll timestamp (Unix seconds)");
  lines.push("# TYPE ri_collector_last_success_timestamp gauge");
  for (const entry of sourceLastSuccessTimestamp) {
    lines.push(
      `ri_collector_last_success_timestamp{source="${quoteMetricLabelValue(entry.source)}"} ${entry.lastSuccessTimestamp}`
    );
  }

  lines.push("# HELP ri_collector_source_healthy Source health status (1=healthy, 0=not healthy)");
  lines.push("# TYPE ri_collector_source_healthy gauge");
  for (const entry of sourceLastSuccessTimestamp) {
    const isHealthy = entry.health.status === "healthy" && entry.lastSuccessTimestamp > 0;
    lines.push(
      `ri_collector_source_healthy{source="${quoteMetricLabelValue(entry.source)}"} ${isHealthy ? 1 : 0}`
    );
  }

  lines.push("# HELP ri_collector_up 1 when service dependencies are healthy");
  lines.push("# TYPE ri_collector_up gauge");
  lines.push(`ri_collector_up ${getHealthStatus(ctx).status === "unhealthy" ? 0 : 1}`);

  return `${lines.join("\n")}\n`;
}

export function createHandlers(ctx: HealthContext): HealthHandlers {
  return {
    getHealth() {
      const health = getHealthStatus(ctx);
      return { status: health.status, body: health };
    },
    isReady() {
      const ready = ctx.kafkaHealthy && ctx.checkpointsHealthy && ctx.allowlistHealthy;
      const health = getHealthStatus(ctx);
      return { ready, body: { ready, status: health.status } };
    },
    formatMetrics: () => formatMetrics(ctx),
  };
}

export function startHealthServer(ctx: HealthContext, logger: Logger): Server {
  const config = getConfig();
  return startSharedHealthServer(config.PORT, createHandlers(ctx), logger);
}

export function incrementEventsIngested(ctx: HealthContext, source: string, count = 1): void {
  const current = ctx.metrics.eventsIngested.get(source) ?? 0;
  ctx.metrics.eventsIngested.set(source, current + count);
}

export function incrementEventsFailed(
  ctx: HealthContext,
  source: string,
  errorType: CollectorErrorType,
  count = 1
): void {
  let errorMap = ctx.metrics.eventsFailed.get(source);
  if (!errorMap) {
    errorMap = new Map();
    ctx.metrics.eventsFailed.set(source, errorMap);
  }
  const current = errorMap.get(errorType) ?? 0;
  errorMap.set(errorType, current + count);
}

export interface IncrementRssFeedErrorInput {
  feed: string;
  feedUrl: string;
  errorType: CollectorErrorType;
}

export function incrementRssFeedError(
  ctx: HealthContext,
  input: IncrementRssFeedErrorInput,
  count = 1
): void {
  const metricKey = createRssFeedErrorKey(
    input.feed,
    input.feedUrl,
    input.errorType
  );
  const existing = ctx.metrics.rssFeedErrors.get(metricKey);

  if (!existing) {
    ctx.metrics.rssFeedErrors.set(metricKey, {
      feed: input.feed,
      feedUrl: input.feedUrl,
      errorType: input.errorType,
      count,
    });
    return;
  }

  existing.count += count;
}

export function observePollDuration(
  ctx: HealthContext,
  source: string,
  durationSeconds: number
): void {
  const histogram = getOrCreateHistogram(
    ctx.metrics.pollDurationSeconds,
    source,
    DURATION_BUCKETS_SECONDS
  );
  observeHistogram(histogram, durationSeconds);
}

export function observePollItemsCount(ctx: HealthContext, source: string, count: number): void {
  const histogram = getOrCreateHistogram(
    ctx.metrics.pollItemsCount,
    source,
    COUNT_BUCKETS
  );
  observeHistogram(histogram, count);
}

export function incrementCheckpointUpdated(ctx: HealthContext, source: string, count = 1): void {
  const current = ctx.metrics.checkpointUpdated.get(source) ?? 0;
  ctx.metrics.checkpointUpdated.set(source, current + count);
}

export function incrementRateLimitBackoff(ctx: HealthContext, source: string, count = 1): void {
  const current = ctx.metrics.rateLimitBackoff.get(source) ?? 0;
  ctx.metrics.rateLimitBackoff.set(source, current + count);
}

export function incrementTopicsExtracted(ctx: HealthContext, topic: string, count = 1): void {
  const existing = ctx.metrics.topicsExtracted.get(topic);
  if (existing !== undefined) {
    ctx.metrics.topicsExtracted.set(topic, existing + count);
    return;
  }

  const explicitTopicCount = [...ctx.metrics.topicsExtracted.keys()].filter(
    (key) => key !== OTHER_TOPIC_LABEL
  ).length;
  if (explicitTopicCount < TOPIC_LABEL_CARDINALITY_LIMIT) {
    ctx.metrics.topicsExtracted.set(topic, count);
    return;
  }

  const otherCount = ctx.metrics.topicsExtracted.get(OTHER_TOPIC_LABEL) ?? 0;
  ctx.metrics.topicsExtracted.set(OTHER_TOPIC_LABEL, otherCount + count);
}
