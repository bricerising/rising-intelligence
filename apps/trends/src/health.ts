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
  getMaxConsumerLag,
  type HealthHandlers,
} from "@rising-intelligence/shared/http";
import { getConfig } from "./config.js";
import type { TrendWindow } from "./types.js";

const DURATION_BUCKETS_SECONDS = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60];
const BRIEF_TRIGGER_TYPES = ["daily", "threshold"] as const;
export type BriefTriggerType = (typeof BRIEF_TRIGGER_TYPES)[number];

export interface TopicWindowMetricEntry {
  topic: string;
  window: TrendWindow;
  value: number;
}

export interface TopicWindowMetrics {
  set(topic: string, window: TrendWindow, volume: number, score: number): void;
  clear(window: TrendWindow): void;
  getVolume(topic: string, window: TrendWindow): number | undefined;
  getScore(topic: string, window: TrendWindow): number | undefined;
  volumeEntries(): Iterable<TopicWindowMetricEntry>;
  scoreEntries(): Iterable<TopicWindowMetricEntry>;
  volumeCount(): number;
  scoreCount(): number;
}

type TopicWindowMetricMap = Map<TrendWindow, Map<string, number>>;

function getOrCreateWindowTopicMap(
  metrics: TopicWindowMetricMap,
  window: TrendWindow
): Map<string, number> {
  const existing = metrics.get(window);
  if (existing) {
    return existing;
  }

  const created = new Map<string, number>();
  metrics.set(window, created);
  return created;
}

function getWindowTopicMetric(
  metrics: TopicWindowMetricMap,
  topic: string,
  window: TrendWindow
): number | undefined {
  return metrics.get(window)?.get(topic);
}

function clearWindowTopicMetric(metrics: TopicWindowMetricMap, window: TrendWindow): void {
  metrics.delete(window);
}

function countWindowTopicMetrics(metrics: TopicWindowMetricMap): number {
  let count = 0;
  for (const topicMetrics of metrics.values()) {
    count += topicMetrics.size;
  }
  return count;
}

function* iterateWindowTopicMetrics(
  metrics: TopicWindowMetricMap
): Iterable<TopicWindowMetricEntry> {
  for (const [window, topics] of metrics) {
    for (const [topic, value] of topics) {
      yield { topic, window, value };
    }
  }
}

class InMemoryTopicWindowMetrics implements TopicWindowMetrics {
  private readonly topicVolumeByWindow: TopicWindowMetricMap = new Map();
  private readonly topicScoreByWindow: TopicWindowMetricMap = new Map();

  set(topic: string, window: TrendWindow, volume: number, score: number): void {
    getOrCreateWindowTopicMap(this.topicVolumeByWindow, window).set(topic, volume);
    getOrCreateWindowTopicMap(this.topicScoreByWindow, window).set(topic, score);
  }

  clear(window: TrendWindow): void {
    clearWindowTopicMetric(this.topicVolumeByWindow, window);
    clearWindowTopicMetric(this.topicScoreByWindow, window);
  }

  getVolume(topic: string, window: TrendWindow): number | undefined {
    return getWindowTopicMetric(this.topicVolumeByWindow, topic, window);
  }

  getScore(topic: string, window: TrendWindow): number | undefined {
    return getWindowTopicMetric(this.topicScoreByWindow, topic, window);
  }

  volumeEntries(): Iterable<TopicWindowMetricEntry> {
    return iterateWindowTopicMetrics(this.topicVolumeByWindow);
  }

  scoreEntries(): Iterable<TopicWindowMetricEntry> {
    return iterateWindowTopicMetrics(this.topicScoreByWindow);
  }

  volumeCount(): number {
    return countWindowTopicMetrics(this.topicVolumeByWindow);
  }

  scoreCount(): number {
    return countWindowTopicMetrics(this.topicScoreByWindow);
  }
}

function createTopicWindowMetrics(): TopicWindowMetrics {
  return new InMemoryTopicWindowMetrics();
}

export interface Metrics {
  eventsProcessed: number;
  duplicatesSkipped: number;
  snapshotPublished: Map<TrendWindow, number>;
  briefTriggered: Map<BriefTriggerType, number>;
  briefSkippedStaleData: number;
  errors: Map<string, number>;
  consumerLag: Map<number, bigint>;
  topicMetrics: TopicWindowMetrics;
  snapshotDurationSeconds: Map<TrendWindow, HistogramState>;
  baselineComputeDurationSeconds: HistogramState;
}

export interface CollectorHeartbeatState {
  source: string;
  status: "healthy" | "degraded" | "error";
  timestamp: Date;
  lastFetchAt: Date;
  itemsFetched: number;
  errorMessage?: string;
}

export interface HealthContext {
  startTime: number;
  kafkaHealthy: boolean;
  postgresHealthy: boolean;
  redisHealthy: boolean;
  allowlistHealthy: boolean;
  lastEventAt?: Date;
  collectorHeartbeats: Map<string, CollectorHeartbeatState>;
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

export function createMetrics(): Metrics {
  return {
    eventsProcessed: 0,
    duplicatesSkipped: 0,
    snapshotPublished: new Map(),
    briefTriggered: new Map(),
    briefSkippedStaleData: 0,
    errors: new Map(),
    consumerLag: new Map(),
    topicMetrics: createTopicWindowMetrics(),
    snapshotDurationSeconds: new Map([
      ["15m", createHistogram(DURATION_BUCKETS_SECONDS)],
      ["60m", createHistogram(DURATION_BUCKETS_SECONDS)],
    ]),
    baselineComputeDurationSeconds: createHistogram(DURATION_BUCKETS_SECONDS),
  };
}

export function createHealthContext(): HealthContext {
  return {
    startTime: Date.now(),
    kafkaHealthy: false,
    postgresHealthy: false,
    redisHealthy: false,
    allowlistHealthy: false,
    collectorHeartbeats: new Map(),
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

export function incrementBriefTriggered(
  ctx: HealthContext,
  type: BriefTriggerType,
  count = 1
): void {
  const current = ctx.metrics.briefTriggered.get(type) ?? 0;
  ctx.metrics.briefTriggered.set(type, current + count);
}

export function incrementBriefSkippedStaleData(ctx: HealthContext, count = 1): void {
  ctx.metrics.briefSkippedStaleData += count;
}

export function incrementError(ctx: HealthContext, errorType: string, count = 1): void {
  const current = ctx.metrics.errors.get(errorType) ?? 0;
  ctx.metrics.errors.set(errorType, current + count);
}

export function setConsumerLag(ctx: HealthContext, partition: number, lag: bigint): void {
  ctx.metrics.consumerLag.set(partition, lag);
}

export function clearTopicMetrics(ctx: HealthContext, window: TrendWindow): void {
  ctx.metrics.topicMetrics.clear(window);
}

export function setTopicMetrics(
  ctx: HealthContext,
  topic: string,
  window: TrendWindow,
  volume: number,
  score: number
): void {
  ctx.metrics.topicMetrics.set(topic, window, volume, score);
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

export function observeBaselineComputeDuration(
  ctx: HealthContext,
  durationSeconds: number
): void {
  observeHistogram(ctx.metrics.baselineComputeDurationSeconds, durationSeconds);
}

export function getHealthStatus(ctx: HealthContext): HealthStatus {
  const config = getConfig();
  const maxLag = getMaxConsumerLag(ctx.metrics.consumerLag);
  const hasLagIssue = maxLag > BigInt(config.MAX_LAG_MESSAGES);

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

  lines.push("# HELP ri_trends_brief_triggered_total Brief trigger events by type");
  lines.push("# TYPE ri_trends_brief_triggered_total counter");
  for (const type of BRIEF_TRIGGER_TYPES) {
    lines.push(
      `ri_trends_brief_triggered_total{type="${type}"} ${ctx.metrics.briefTriggered.get(type) ?? 0}`
    );
  }

  lines.push("# HELP ri_trends_brief_skipped_stale_data_total Brief triggers skipped due to stale data");
  lines.push("# TYPE ri_trends_brief_skipped_stale_data_total counter");
  lines.push(`ri_trends_brief_skipped_stale_data_total ${ctx.metrics.briefSkippedStaleData}`);

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
  for (const { topic, window, value } of ctx.metrics.topicMetrics.volumeEntries()) {
    lines.push(
      `ri_trends_topic_volume{topic="${quoteMetricLabelValue(topic)}",window="${window}"} ${value}`
    );
  }

  lines.push("# HELP ri_trends_topic_score Current topic score by window");
  lines.push("# TYPE ri_trends_topic_score gauge");
  for (const { topic, window, value } of ctx.metrics.topicMetrics.scoreEntries()) {
    lines.push(
      `ri_trends_topic_score{topic="${quoteMetricLabelValue(topic)}",window="${window}"} ${value}`
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

  lines.push(
    ...formatHistogram(
      "ri_trends_baseline_compute_duration_seconds",
      "Baseline computation duration in seconds",
      ctx.metrics.baselineComputeDurationSeconds
    )
  );

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
