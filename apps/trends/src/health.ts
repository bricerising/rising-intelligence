import type { Server } from "node:http";
import type { Logger } from "pino";
import {
  startHealthServer as startSharedHealthServer,
  createHealthHandler as createSharedHealthHandler,
  quoteMetricLabelValue,
  type HistogramState,
  createHistogram,
  observeHistogram,
  formatHistogram,
  formatMetricLabels,
  getMaxConsumerLag,
  type HealthHandlers,
} from "@rising-intelligence/shared";
import { getConfig } from "./config.js";
import type { TrendWindow } from "./types.js";

const DURATION_BUCKETS_SECONDS = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60];

export interface Metrics {
  eventsProcessed: number;
  duplicatesSkipped: number;
  snapshotPublished: Map<TrendWindow, number>;
  errors: Map<string, number>;
  consumerLag: Map<number, bigint>;
  topicVolume: Map<string, number>;
  topicScore: Map<string, number>;
  snapshotDurationSeconds: Map<TrendWindow, HistogramState>;
}

export interface HealthContext {
  startTime: number;
  kafkaHealthy: boolean;
  postgresHealthy: boolean;
  redisHealthy: boolean;
  allowlistHealthy: boolean;
  lastEventAt?: Date;
  metrics: Metrics;
}

export interface HealthStatus {
  status: "healthy" | "degraded" | "unhealthy";
  checks: {
    kafka: "ok" | "error";
    postgres: "ok" | "error";
    redis: "ok" | "error";
    allowlist: "ok" | "error";
  };
  uptime_seconds: number;
  max_consumer_lag: string;
  last_event_at?: string;
}

function makeTopicWindowKey(topic: string, window: TrendWindow): string {
  return `${topic}|${window}`;
}

function parseTopicWindowKey(value: string): { topic: string; window: TrendWindow } {
  const [topic, window] = value.split("|");
  return {
    topic,
    window: window as TrendWindow,
  };
}

export function createMetrics(): Metrics {
  return {
    eventsProcessed: 0,
    duplicatesSkipped: 0,
    snapshotPublished: new Map(),
    errors: new Map(),
    consumerLag: new Map(),
    topicVolume: new Map(),
    topicScore: new Map(),
    snapshotDurationSeconds: new Map([
      ["15m", createHistogram(DURATION_BUCKETS_SECONDS)],
      ["60m", createHistogram(DURATION_BUCKETS_SECONDS)],
    ]),
  };
}

export function createHealthContext(): HealthContext {
  return {
    startTime: Date.now(),
    kafkaHealthy: false,
    postgresHealthy: false,
    redisHealthy: false,
    allowlistHealthy: false,
    metrics: createMetrics(),
  };
}

export function incrementEventsProcessed(ctx: HealthContext, count = 1): void {
  ctx.metrics.eventsProcessed += count;
}

export function incrementDuplicatesSkipped(ctx: HealthContext, count = 1): void {
  ctx.metrics.duplicatesSkipped += count;
}

export function incrementSnapshotPublished(ctx: HealthContext, window: TrendWindow, count = 1): void {
  const current = ctx.metrics.snapshotPublished.get(window) ?? 0;
  ctx.metrics.snapshotPublished.set(window, current + count);
}

export function incrementError(ctx: HealthContext, errorType: string, count = 1): void {
  const current = ctx.metrics.errors.get(errorType) ?? 0;
  ctx.metrics.errors.set(errorType, current + count);
}

export function setConsumerLag(ctx: HealthContext, partition: number, lag: bigint): void {
  ctx.metrics.consumerLag.set(partition, lag);
}

export function clearTopicMetrics(ctx: HealthContext, window: TrendWindow): void {
  const keys = [...ctx.metrics.topicVolume.keys()];
  for (const key of keys) {
    const parsed = parseTopicWindowKey(key);
    if (parsed.window !== window) {
      continue;
    }

    ctx.metrics.topicVolume.delete(key);
    ctx.metrics.topicScore.delete(key);
  }
}

export function setTopicMetrics(
  ctx: HealthContext,
  topic: string,
  window: TrendWindow,
  volume: number,
  score: number
): void {
  const key = makeTopicWindowKey(topic, window);
  ctx.metrics.topicVolume.set(key, volume);
  ctx.metrics.topicScore.set(key, score);
}

export function observeSnapshotDuration(
  ctx: HealthContext,
  window: TrendWindow,
  durationSeconds: number
): void {
  const histogram = ctx.metrics.snapshotDurationSeconds.get(window);
  if (!histogram) {
    return;
  }

  observeHistogram(histogram, durationSeconds);
}

export function getHealthStatus(ctx: HealthContext): HealthStatus {
  const maxLag = getMaxConsumerLag(ctx.metrics.consumerLag);
  const hasLagIssue = maxLag > 5000n;

  let status: "healthy" | "degraded" | "unhealthy";
  if (!ctx.kafkaHealthy || !ctx.postgresHealthy || !ctx.redisHealthy || !ctx.allowlistHealthy) {
    status = "unhealthy";
  } else if (hasLagIssue) {
    status = "degraded";
  } else {
    status = "healthy";
  }

  return {
    status,
    checks: {
      kafka: ctx.kafkaHealthy ? "ok" : "error",
      postgres: ctx.postgresHealthy ? "ok" : "error",
      redis: ctx.redisHealthy ? "ok" : "error",
      allowlist: ctx.allowlistHealthy ? "ok" : "error",
    },
    uptime_seconds: Math.floor((Date.now() - ctx.startTime) / 1000),
    max_consumer_lag: maxLag.toString(),
    last_event_at: ctx.lastEventAt?.toISOString(),
  };
}

export function formatMetrics(ctx: HealthContext): string {
  const lines: string[] = [];

  lines.push("# HELP ri_trends_events_processed_total Events consumed and applied to trend windows");
  lines.push("# TYPE ri_trends_events_processed_total counter");
  lines.push(`ri_trends_events_processed_total ${ctx.metrics.eventsProcessed}`);

  lines.push("# HELP ri_trends_duplicates_skipped_total Duplicate events skipped by dedup");
  lines.push("# TYPE ri_trends_duplicates_skipped_total counter");
  lines.push(`ri_trends_duplicates_skipped_total ${ctx.metrics.duplicatesSkipped}`);

  lines.push("# HELP ri_trends_snapshot_published_total Trend snapshots published by window");
  lines.push("# TYPE ri_trends_snapshot_published_total counter");
  for (const [window, count] of ctx.metrics.snapshotPublished) {
    lines.push(`ri_trends_snapshot_published_total{window="${window}"} ${count}`);
  }

  lines.push("# HELP ri_trends_errors_total Errors by category");
  lines.push("# TYPE ri_trends_errors_total counter");
  for (const [errorType, count] of ctx.metrics.errors) {
    lines.push(`ri_trends_errors_total{error_type="${quoteMetricLabelValue(errorType)}"} ${count}`);
  }

  lines.push("# HELP ri_trends_consumer_lag Messages behind latest by partition");
  lines.push("# TYPE ri_trends_consumer_lag gauge");
  for (const [partition, lag] of ctx.metrics.consumerLag) {
    lines.push(`ri_trends_consumer_lag{partition="${partition}"} ${lag.toString()}`);
  }

  lines.push("# HELP ri_trends_topic_volume Current topic volume by window");
  lines.push("# TYPE ri_trends_topic_volume gauge");
  for (const [key, volume] of ctx.metrics.topicVolume) {
    const parsed = parseTopicWindowKey(key);
    lines.push(
      `ri_trends_topic_volume{topic="${quoteMetricLabelValue(parsed.topic)}",window="${parsed.window}"} ${volume}`
    );
  }

  lines.push("# HELP ri_trends_topic_score Current topic score by window");
  lines.push("# TYPE ri_trends_topic_score gauge");
  for (const [key, score] of ctx.metrics.topicScore) {
    const parsed = parseTopicWindowKey(key);
    lines.push(
      `ri_trends_topic_score{topic="${quoteMetricLabelValue(parsed.topic)}",window="${parsed.window}"} ${score}`
    );
  }

  let wroteSnapshotDurationMetadata = false;
  for (const [window, histogram] of ctx.metrics.snapshotDurationSeconds) {
    lines.push(
      ...formatHistogram(
        "ri_trends_snapshot_duration_seconds",
        "Snapshot computation duration in seconds",
        histogram,
        { window },
        !wroteSnapshotDurationMetadata
      )
    );
    wroteSnapshotDurationMetadata = true;
  }

  lines.push("# HELP ri_trends_up 1 when service dependencies are healthy");
  lines.push("# TYPE ri_trends_up gauge");
  lines.push(`ri_trends_up ${getHealthStatus(ctx).status === "unhealthy" ? 0 : 1}`);

  return `${lines.join("\n")}\n`;
}

export function createHandlers(ctx: HealthContext): HealthHandlers {
  return {
    getHealth() {
      const health = getHealthStatus(ctx);
      return { status: health.status, body: health };
    },
    isReady() {
      const ready = ctx.kafkaHealthy && ctx.postgresHealthy && ctx.redisHealthy && ctx.allowlistHealthy;
      return {
        ready,
        body: {
          ready,
          kafka: ctx.kafkaHealthy,
          postgres: ctx.postgresHealthy,
          redis: ctx.redisHealthy,
          allowlist: ctx.allowlistHealthy,
        },
      };
    },
    formatMetrics: () => formatMetrics(ctx),
  };
}

export function createHealthHandler(ctx: HealthContext) {
  return createSharedHealthHandler(createHandlers(ctx));
}

export function startHealthServer(ctx: HealthContext, logger: Logger): Server {
  const config = getConfig();
  return startSharedHealthServer(config.PORT, createHandlers(ctx), logger);
}
