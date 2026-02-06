import type { Server } from "node:http";
import type { Logger } from "pino";
import {
  startHealthServer as startSharedHealthServer,
  quoteMetricLabelValue,
  type HealthHandlers,
} from "@rising-intelligence/shared";
import { getConfig } from "./config.js";

export interface Metrics {
  requestsConsumed: number;
  malformedMessages: number;
  errors: Map<string, number>;
}

export interface HealthContext {
  startTime: number;
  kafkaHealthy: boolean;
  metrics: Metrics;
}

export interface HealthStatus {
  status: "healthy" | "unhealthy";
  checks: {
    kafka: "ok" | "error";
  };
  uptime_seconds: number;
}

export function createHealthContext(): HealthContext {
  return {
    startTime: Date.now(),
    kafkaHealthy: false,
    metrics: {
      requestsConsumed: 0,
      malformedMessages: 0,
      errors: new Map(),
    },
  };
}

export function incrementConsumed(ctx: HealthContext, count = 1): void {
  ctx.metrics.requestsConsumed += count;
}

export function incrementMalformed(ctx: HealthContext, count = 1): void {
  ctx.metrics.malformedMessages += count;
}

export function incrementError(ctx: HealthContext, errorType: string, count = 1): void {
  const current = ctx.metrics.errors.get(errorType) ?? 0;
  ctx.metrics.errors.set(errorType, current + count);
}

export function getHealthStatus(ctx: HealthContext): HealthStatus {
  return {
    status: ctx.kafkaHealthy ? "healthy" : "unhealthy",
    checks: {
      kafka: ctx.kafkaHealthy ? "ok" : "error",
    },
    uptime_seconds: Math.floor((Date.now() - ctx.startTime) / 1000),
  };
}

export function formatMetrics(ctx: HealthContext): string {
  const lines: string[] = [];

  lines.push("# HELP ri_brief_requests_consumed_total Summary requests consumed from Kafka");
  lines.push("# TYPE ri_brief_requests_consumed_total counter");
  lines.push(`ri_brief_requests_consumed_total ${ctx.metrics.requestsConsumed}`);

  lines.push("# HELP ri_brief_messages_malformed_total Malformed summary request messages");
  lines.push("# TYPE ri_brief_messages_malformed_total counter");
  lines.push(`ri_brief_messages_malformed_total ${ctx.metrics.malformedMessages}`);

  lines.push("# HELP ri_brief_errors_total Errors by category");
  lines.push("# TYPE ri_brief_errors_total counter");
  for (const [errorType, count] of ctx.metrics.errors) {
    lines.push(`ri_brief_errors_total{error_type="${quoteMetricLabelValue(errorType)}"} ${count}`);
  }

  lines.push("# HELP ri_brief_up 1 when service dependencies are healthy");
  lines.push("# TYPE ri_brief_up gauge");
  lines.push(`ri_brief_up ${ctx.kafkaHealthy ? 1 : 0}`);

  return `${lines.join("\n")}\n`;
}

export function createHandlers(ctx: HealthContext): HealthHandlers {
  return {
    getHealth() {
      const health = getHealthStatus(ctx);
      return { status: health.status, body: health };
    },
    isReady() {
      const ready = ctx.kafkaHealthy;
      return { ready, body: { ready } };
    },
    formatMetrics: () => formatMetrics(ctx),
  };
}

export function startHealthServer(ctx: HealthContext, logger: Logger): Server {
  const config = getConfig();
  return startSharedHealthServer(config.PORT, createHandlers(ctx), logger);
}
