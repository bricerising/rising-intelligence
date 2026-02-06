import type { Server } from "node:http";
import type { Logger } from "pino";
import {
  startHealthServer as startSharedHealthServer,
  quoteMetricLabelValue,
  type HealthHandlers,
} from "@rising-intelligence/shared";
import { getConfig } from "./config.js";

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

export interface Metrics {
  eventsPublished: Map<string, number>;
  eventsDlq: Map<string, number>;
  pollDuration: Map<string, number[]>;
  errors: Map<string, Map<string, number>>;
  lastPollTimestamp: Map<string, number>;
  checkpointsWritten: Map<string, number>;
}

export function createMetrics(): Metrics {
  return {
    eventsPublished: new Map(),
    eventsDlq: new Map(),
    pollDuration: new Map(),
    errors: new Map(),
    lastPollTimestamp: new Map(),
    checkpointsWritten: new Map(),
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

export function formatMetrics(ctx: HealthContext): string {
  const lines: string[] = [];

  lines.push("# HELP ri_collector_events_published_total Events published to Kafka");
  lines.push("# TYPE ri_collector_events_published_total counter");
  for (const [source, count] of ctx.metrics.eventsPublished) {
    lines.push(`ri_collector_events_published_total{source="${quoteMetricLabelValue(source)}"} ${count}`);
  }

  lines.push("# HELP ri_collector_events_dlq_total Events sent to DLQ");
  lines.push("# TYPE ri_collector_events_dlq_total counter");
  for (const [source, count] of ctx.metrics.eventsDlq) {
    lines.push(`ri_collector_events_dlq_total{source="${quoteMetricLabelValue(source)}"} ${count}`);
  }

  lines.push("# HELP ri_collector_errors_total Errors by source and type");
  lines.push("# TYPE ri_collector_errors_total counter");
  for (const [source, errorMap] of ctx.metrics.errors) {
    for (const [errorType, count] of errorMap) {
      lines.push(`ri_collector_errors_total{source="${quoteMetricLabelValue(source)}",type="${quoteMetricLabelValue(errorType)}"} ${count}`);
    }
  }

  lines.push("# HELP ri_collector_last_poll_timestamp_seconds Unix timestamp of last successful poll");
  lines.push("# TYPE ri_collector_last_poll_timestamp_seconds gauge");
  for (const [source, timestamp] of ctx.metrics.lastPollTimestamp) {
    lines.push(`ri_collector_last_poll_timestamp_seconds{source="${quoteMetricLabelValue(source)}"} ${Math.floor(timestamp / 1000)}`);
  }

  lines.push("# HELP ri_collector_checkpoints_written_total Checkpoint writes");
  lines.push("# TYPE ri_collector_checkpoints_written_total counter");
  for (const [source, count] of ctx.metrics.checkpointsWritten) {
    lines.push(`ri_collector_checkpoints_written_total{source="${quoteMetricLabelValue(source)}"} ${count}`);
  }

  lines.push("# HELP ri_collector_uptime_seconds Service uptime in seconds");
  lines.push("# TYPE ri_collector_uptime_seconds gauge");
  lines.push(`ri_collector_uptime_seconds ${Math.floor((Date.now() - ctx.startTime) / 1000)}`);

  return lines.join("\n") + "\n";
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

// Metric helpers
export function incrementEventsPublished(ctx: HealthContext, source: string, count = 1): void {
  const current = ctx.metrics.eventsPublished.get(source) ?? 0;
  ctx.metrics.eventsPublished.set(source, current + count);
}

export function incrementEventsDlq(ctx: HealthContext, source: string, count = 1): void {
  const current = ctx.metrics.eventsDlq.get(source) ?? 0;
  ctx.metrics.eventsDlq.set(source, current + count);
}

export function incrementError(ctx: HealthContext, source: string, errorType: string): void {
  let errorMap = ctx.metrics.errors.get(source);
  if (!errorMap) {
    errorMap = new Map();
    ctx.metrics.errors.set(source, errorMap);
  }
  const current = errorMap.get(errorType) ?? 0;
  errorMap.set(errorType, current + 1);
}

export function recordLastPoll(ctx: HealthContext, source: string): void {
  ctx.metrics.lastPollTimestamp.set(source, Date.now());
}

export function incrementCheckpointsWritten(ctx: HealthContext, source: string): void {
  const current = ctx.metrics.checkpointsWritten.get(source) ?? 0;
  ctx.metrics.checkpointsWritten.set(source, current + 1);
}
