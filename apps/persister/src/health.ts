import type { Server } from "node:http";
import type { Logger } from "pino";
import {
  startHealthServer as startSharedHealthServer,
  createHistogram,
  observeHistogram,
  formatHistogram,
  quoteMetricLabelValue,
  getMaxConsumerLag,
  type HealthHandlers,
  type HistogramState,
} from "@rising-intelligence/shared";
import { getConfig } from "./config.js";

export interface Metrics {
  eventsProcessed: Map<string, number>;
  eventsSkipped: Map<string, number>;
  errors: Map<string, number>;
  consumerLag: Map<number, bigint>;
  postgresWriteDurationSeconds: HistogramState;
  redisWriteDurationSeconds: HistogramState;
  batchSize: HistogramState;
}

export interface HealthContext {
  startTime: number;
  kafkaHealthy: boolean;
  postgresHealthy: boolean;
  redisHealthy: boolean;
  circuitOpen: boolean;
  lastEventAt?: Date;
  metrics: Metrics;
}

export interface HealthStatus {
  status: "healthy" | "degraded" | "unhealthy";
  checks: {
    kafka: "ok" | "error";
    postgres: "ok" | "error";
    redis: "ok" | "error";
    circuit: "closed" | "open";
  };
  uptime_seconds: number;
  max_consumer_lag: string;
  last_event_at?: string;
}

const DURATION_BUCKETS_SECONDS = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60];
const COUNT_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000];

export function createMetrics(): Metrics {
  return {
    eventsProcessed: new Map(),
    eventsSkipped: new Map(),
    errors: new Map(),
    consumerLag: new Map(),
    postgresWriteDurationSeconds: createHistogram(DURATION_BUCKETS_SECONDS),
    redisWriteDurationSeconds: createHistogram(DURATION_BUCKETS_SECONDS),
    batchSize: createHistogram(COUNT_BUCKETS),
  };
}

export function createHealthContext(): HealthContext {
  return {
    startTime: Date.now(),
    kafkaHealthy: false,
    postgresHealthy: false,
    redisHealthy: false,
    circuitOpen: false,
    metrics: createMetrics(),
  };
}

export function incrementEventsProcessed(ctx: HealthContext, source: string, count = 1): void {
  const current = ctx.metrics.eventsProcessed.get(source) ?? 0;
  ctx.metrics.eventsProcessed.set(source, current + count);
}

export function incrementEventsSkipped(ctx: HealthContext, reason: string, count = 1): void {
  const current = ctx.metrics.eventsSkipped.get(reason) ?? 0;
  ctx.metrics.eventsSkipped.set(reason, current + count);
}

export function incrementError(ctx: HealthContext, errorType: string, count = 1): void {
  const current = ctx.metrics.errors.get(errorType) ?? 0;
  ctx.metrics.errors.set(errorType, current + count);
}

export function observePostgresWriteDuration(ctx: HealthContext, durationSeconds: number): void {
  observeHistogram(ctx.metrics.postgresWriteDurationSeconds, durationSeconds);
}

export function observeRedisWriteDuration(ctx: HealthContext, durationSeconds: number): void {
  observeHistogram(ctx.metrics.redisWriteDurationSeconds, durationSeconds);
}

export function observeBatchSize(ctx: HealthContext, size: number): void {
  observeHistogram(ctx.metrics.batchSize, size);
}

export function setConsumerLag(ctx: HealthContext, partition: number, lag: bigint): void {
  ctx.metrics.consumerLag.set(partition, lag);
}

export function getHealthStatus(ctx: HealthContext): HealthStatus {
  const maxLag = getMaxConsumerLag(ctx.metrics.consumerLag);
  const hasLagIssue = maxLag > 1000n;

  let status: "healthy" | "degraded" | "unhealthy";
  if (!ctx.kafkaHealthy || !ctx.postgresHealthy || !ctx.redisHealthy) {
    status = "unhealthy";
  } else if (hasLagIssue || ctx.circuitOpen) {
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
      circuit: ctx.circuitOpen ? "open" : "closed",
    },
    uptime_seconds: Math.floor((Date.now() - ctx.startTime) / 1000),
    max_consumer_lag: maxLag.toString(),
    last_event_at: ctx.lastEventAt?.toISOString(),
  };
}

export function formatMetrics(ctx: HealthContext): string {
  const lines: string[] = [];

  lines.push("# HELP ri_persister_events_processed_total Events written to Postgres");
  lines.push("# TYPE ri_persister_events_processed_total counter");
  for (const [source, count] of ctx.metrics.eventsProcessed) {
    lines.push(`ri_persister_events_processed_total{source="${quoteMetricLabelValue(source)}"} ${count}`);
  }

  lines.push("# HELP ri_persister_events_skipped_total Events skipped by reason");
  lines.push("# TYPE ri_persister_events_skipped_total counter");
  for (const [reason, count] of ctx.metrics.eventsSkipped) {
    lines.push(`ri_persister_events_skipped_total{reason="${quoteMetricLabelValue(reason)}"} ${count}`);
  }

  lines.push("# HELP ri_persister_errors_total Errors by category");
  lines.push("# TYPE ri_persister_errors_total counter");
  for (const [errorType, count] of ctx.metrics.errors) {
    lines.push(`ri_persister_errors_total{error_type="${quoteMetricLabelValue(errorType)}"} ${count}`);
  }

  lines.push("# HELP ri_persister_consumer_lag Messages behind latest by partition");
  lines.push("# TYPE ri_persister_consumer_lag gauge");
  for (const [partition, lag] of ctx.metrics.consumerLag) {
    lines.push(`ri_persister_consumer_lag{partition="${partition}"} ${lag.toString()}`);
  }

  lines.push(
    ...formatHistogram(
      "ri_persister_postgres_write_duration_seconds",
      "Postgres write duration in seconds",
      ctx.metrics.postgresWriteDurationSeconds
    )
  );

  lines.push(
    ...formatHistogram(
      "ri_persister_redis_write_duration_seconds",
      "Redis write duration in seconds",
      ctx.metrics.redisWriteDurationSeconds
    )
  );

  lines.push(
    ...formatHistogram(
      "ri_persister_batch_size",
      "Kafka batch size",
      ctx.metrics.batchSize
    )
  );

  lines.push("# HELP ri_persister_up 1 when service dependencies are healthy");
  lines.push("# TYPE ri_persister_up gauge");
  lines.push(`ri_persister_up ${getHealthStatus(ctx).status === "unhealthy" ? 0 : 1}`);

  return `${lines.join("\n")}\n`;
}

export function createHandlers(ctx: HealthContext): HealthHandlers {
  return {
    getHealth() {
      const health = getHealthStatus(ctx);
      return { status: health.status, body: health };
    },
    isReady() {
      const ready = ctx.kafkaHealthy && ctx.postgresHealthy && ctx.redisHealthy && !ctx.circuitOpen;
      return {
        ready,
        body: {
          ready,
          circuit_open: ctx.circuitOpen,
          kafka: ctx.kafkaHealthy,
          postgres: ctx.postgresHealthy,
          redis: ctx.redisHealthy,
        },
      };
    },
    formatMetrics: () => formatMetrics(ctx),
  };
}

export function startHealthServer(ctx: HealthContext, logger: Logger): Server {
  const config = getConfig();
  return startSharedHealthServer(config.PORT, createHandlers(ctx), logger);
}
