import type { Server } from "node:http";
import type { Logger } from "pino";
import {
  startHealthServer as startSharedHealthServer,
  createHistogram,
  observeHistogram,
  formatHistogram,
  quoteMetricLabelValue,
  type HistogramState,
  type HealthHandlers,
} from "@rising-intelligence/shared";
import { getConfig } from "./config.js";

const LLM_DURATION_BUCKETS_SECONDS = [1, 5, 10, 30, 60, 120, 300];
const COUNT_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000];
const GENERATION_STATUSES = ["success", "failure", "skipped"] as const;
const TOKEN_DIRECTIONS = ["input", "output"] as const;

export type BriefGenerationStatus = (typeof GENERATION_STATUSES)[number];
export type TokenDirection = (typeof TOKEN_DIRECTIONS)[number];

export interface Metrics {
  generation: Map<BriefGenerationStatus, number>;
  generationDurationSeconds: HistogramState;
  duplicatesSkipped: number;
  budgetRemainingUsd: number;
  budgetExceeded: number;
  suspiciousContent: number;
  llmTokens: Map<TokenDirection, number>;
  llmCostUsdTotal: number;
  highlightsCount: HistogramState;
  citationsCount: HistogramState;
  errors: Map<string, number>;
}

export interface HealthContext {
  startTime: number;
  kafkaHealthy: boolean;
  postgresHealthy: boolean;
  redisHealthy: boolean;
  metrics: Metrics;
}

export interface HealthStatus {
  status: "healthy" | "unhealthy";
  checks: {
    kafka: "ok" | "error";
    postgres: "ok" | "error";
    redis: "ok" | "error";
  };
  uptime_seconds: number;
}

export function createHealthContext(initialBudgetUsd = 0): HealthContext {
  return {
    startTime: Date.now(),
    kafkaHealthy: false,
    postgresHealthy: false,
    redisHealthy: false,
    metrics: {
      generation: new Map(),
      generationDurationSeconds: createHistogram(LLM_DURATION_BUCKETS_SECONDS),
      duplicatesSkipped: 0,
      budgetRemainingUsd: initialBudgetUsd,
      budgetExceeded: 0,
      suspiciousContent: 0,
      llmTokens: new Map(),
      llmCostUsdTotal: 0,
      highlightsCount: createHistogram(COUNT_BUCKETS),
      citationsCount: createHistogram(COUNT_BUCKETS),
      errors: new Map(),
    },
  };
}

export function incrementGeneration(
  ctx: HealthContext,
  status: BriefGenerationStatus,
  count = 1
): void {
  const current = ctx.metrics.generation.get(status) ?? 0;
  ctx.metrics.generation.set(status, current + count);
}

export function observeGenerationDuration(ctx: HealthContext, durationSeconds: number): void {
  observeHistogram(ctx.metrics.generationDurationSeconds, durationSeconds);
}

export function incrementDuplicatesSkipped(ctx: HealthContext, count = 1): void {
  ctx.metrics.duplicatesSkipped += count;
}

export function setBudgetRemainingUsd(ctx: HealthContext, amount: number): void {
  ctx.metrics.budgetRemainingUsd = amount;
}

export function incrementBudgetExceeded(ctx: HealthContext, count = 1): void {
  ctx.metrics.budgetExceeded += count;
}

export function incrementSuspiciousContent(ctx: HealthContext, count = 1): void {
  ctx.metrics.suspiciousContent += count;
}

export function incrementLlmTokens(
  ctx: HealthContext,
  direction: TokenDirection,
  count: number
): void {
  const current = ctx.metrics.llmTokens.get(direction) ?? 0;
  ctx.metrics.llmTokens.set(direction, current + count);
}

export function incrementLlmCostUsd(ctx: HealthContext, amount: number): void {
  ctx.metrics.llmCostUsdTotal += amount;
}

export function observeHighlightsCount(ctx: HealthContext, count: number): void {
  observeHistogram(ctx.metrics.highlightsCount, count);
}

export function observeCitationsCount(ctx: HealthContext, count: number): void {
  observeHistogram(ctx.metrics.citationsCount, count);
}

export function incrementError(ctx: HealthContext, errorType: string, count = 1): void {
  const current = ctx.metrics.errors.get(errorType) ?? 0;
  ctx.metrics.errors.set(errorType, current + count);
}

export function getHealthStatus(ctx: HealthContext): HealthStatus {
  return {
    status: ctx.kafkaHealthy && ctx.postgresHealthy && ctx.redisHealthy ? "healthy" : "unhealthy",
    checks: {
      kafka: ctx.kafkaHealthy ? "ok" : "error",
      postgres: ctx.postgresHealthy ? "ok" : "error",
      redis: ctx.redisHealthy ? "ok" : "error",
    },
    uptime_seconds: Math.floor((Date.now() - ctx.startTime) / 1000),
  };
}

export function formatMetrics(ctx: HealthContext): string {
  const lines: string[] = [];

  lines.push("# HELP ri_brief_generation_total Brief generation attempts by status");
  lines.push("# TYPE ri_brief_generation_total counter");
  for (const status of GENERATION_STATUSES) {
    lines.push(
      `ri_brief_generation_total{status="${status}"} ${ctx.metrics.generation.get(status) ?? 0}`
    );
  }

  lines.push(
    ...formatHistogram(
      "ri_brief_generation_duration_seconds",
      "Brief generation duration in seconds",
      ctx.metrics.generationDurationSeconds
    )
  );

  lines.push("# HELP ri_brief_duplicates_skipped_total Duplicate summary requests skipped");
  lines.push("# TYPE ri_brief_duplicates_skipped_total counter");
  lines.push(`ri_brief_duplicates_skipped_total ${ctx.metrics.duplicatesSkipped}`);

  lines.push("# HELP ri_brief_budget_remaining_usd Remaining daily budget in USD");
  lines.push("# TYPE ri_brief_budget_remaining_usd gauge");
  lines.push(`ri_brief_budget_remaining_usd ${ctx.metrics.budgetRemainingUsd}`);

  lines.push("# HELP ri_brief_budget_exceeded_total Summary requests skipped due to budget");
  lines.push("# TYPE ri_brief_budget_exceeded_total counter");
  lines.push(`ri_brief_budget_exceeded_total ${ctx.metrics.budgetExceeded}`);

  lines.push("# HELP ri_brief_suspicious_content_total Evidence excerpts flagged as prompt-like");
  lines.push("# TYPE ri_brief_suspicious_content_total counter");
  lines.push(`ri_brief_suspicious_content_total ${ctx.metrics.suspiciousContent}`);

  lines.push("# HELP ri_brief_llm_tokens_total LLM tokens by direction");
  lines.push("# TYPE ri_brief_llm_tokens_total counter");
  for (const direction of TOKEN_DIRECTIONS) {
    lines.push(
      `ri_brief_llm_tokens_total{direction="${direction}"} ${ctx.metrics.llmTokens.get(direction) ?? 0}`
    );
  }

  lines.push("# HELP ri_brief_llm_cost_usd_total Estimated LLM cost in USD");
  lines.push("# TYPE ri_brief_llm_cost_usd_total counter");
  lines.push(`ri_brief_llm_cost_usd_total ${ctx.metrics.llmCostUsdTotal}`);

  lines.push(
    ...formatHistogram(
      "ri_brief_highlights_count",
      "Highlights per generated brief",
      ctx.metrics.highlightsCount
    )
  );

  lines.push(
    ...formatHistogram(
      "ri_brief_citations_count",
      "Citations per generated brief",
      ctx.metrics.citationsCount
    )
  );

  lines.push("# HELP ri_brief_errors_total Errors by category");
  lines.push("# TYPE ri_brief_errors_total counter");
  for (const [errorType, count] of ctx.metrics.errors) {
    lines.push(`ri_brief_errors_total{error_type="${quoteMetricLabelValue(errorType)}"} ${count}`);
  }

  lines.push("# HELP ri_brief_up 1 when service dependencies are healthy");
  lines.push("# TYPE ri_brief_up gauge");
  lines.push(`ri_brief_up ${getHealthStatus(ctx).status === "healthy" ? 1 : 0}`);

  return `${lines.join("\n")}\n`;
}

export function createHandlers(ctx: HealthContext): HealthHandlers {
  return {
    getHealth() {
      const health = getHealthStatus(ctx);
      return { status: health.status, body: health };
    },
    isReady() {
      const ready = ctx.kafkaHealthy && ctx.postgresHealthy && ctx.redisHealthy;
      return { ready, body: { ready } };
    },
    formatMetrics: () => formatMetrics(ctx),
  };
}

export function startHealthServer(ctx: HealthContext, logger: Logger): Server {
  const config = getConfig();
  return startSharedHealthServer(config.PORT, createHandlers(ctx), logger);
}
