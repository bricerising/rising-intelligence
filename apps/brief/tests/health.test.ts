import { describe, expect, it, vi, beforeEach } from "vitest";

import { createHealthHandler } from "@rising-intelligence/shared/http";
import {
  createHealthContext,
  createHandlers,
  getHealthStatus,
  formatMetrics,
  incrementGeneration,
  observeGenerationDuration,
  incrementDuplicatesSkipped,
  incrementBudgetExceeded,
  incrementSuspiciousContent,
  incrementLlmTokens,
  incrementLlmCostUsd,
  observeHighlightsCount,
  observeCitationsCount,
  incrementError,
  setBudgetRemainingUsd,
  type HealthContext,
} from "../src/health.js";

function makeRequest(method: string, url: string) {
  return { method, url } as import("node:http").IncomingMessage;
}

function makeResponse() {
  let statusCode = 0;
  let headers: Record<string, string> = {};
  let body = "";
  const res = {
    writeHead(code: number, hdrs?: Record<string, string>) {
      statusCode = code;
      if (hdrs) headers = hdrs;
    },
    end(data?: string) {
      if (data) body = data;
    },
    get statusCode() {
      return statusCode;
    },
    get headers() {
      return headers;
    },
    get body() {
      return body;
    },
  };
  return res as unknown as import("node:http").ServerResponse & {
    statusCode: number;
    headers: Record<string, string>;
    body: string;
  };
}

describe("brief health", () => {
  let ctx: HealthContext;

  beforeEach(() => {
    ctx = createHealthContext(5);
  });

  describe("getHealthStatus", () => {
    it("returns unhealthy when kafka is not connected", () => {
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;
      const status = getHealthStatus(ctx);
      expect(status.status).toBe("unhealthy");
      expect(status.checks.kafka).toBe("error");
    });

    it("returns unhealthy when postgres is not connected", () => {
      ctx.kafkaHealthy = true;
      ctx.redisHealthy = true;
      const status = getHealthStatus(ctx);
      expect(status.status).toBe("unhealthy");
      expect(status.checks.postgres).toBe("error");
    });

    it("returns unhealthy when redis is not connected", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      const status = getHealthStatus(ctx);
      expect(status.status).toBe("unhealthy");
      expect(status.checks.redis).toBe("error");
    });

    it("returns healthy when kafka/postgres/redis are connected", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;

      const status = getHealthStatus(ctx);
      expect(status.status).toBe("healthy");
      expect(status.checks.kafka).toBe("ok");
      expect(status.checks.postgres).toBe("ok");
      expect(status.checks.redis).toBe("ok");
    });
  });

  describe("metric incrementers", () => {
    it("tracks generation outcomes", () => {
      incrementGeneration(ctx, "skipped");
      incrementGeneration(ctx, "failure", 2);
      incrementGeneration(ctx, "success", 3);

      expect(ctx.metrics.generation.get("skipped")).toBe(1);
      expect(ctx.metrics.generation.get("failure")).toBe(2);
      expect(ctx.metrics.generation.get("success")).toBe(3);
    });

    it("tracks budget and token/cost counters", () => {
      setBudgetRemainingUsd(ctx, 4.25);
      incrementBudgetExceeded(ctx, 2);
      incrementSuspiciousContent(ctx, 3);
      incrementLlmTokens(ctx, "input", 120);
      incrementLlmTokens(ctx, "output", 80);
      incrementLlmCostUsd(ctx, 0.13);

      expect(ctx.metrics.budgetRemainingUsd).toBe(4.25);
      expect(ctx.metrics.budgetExceeded).toBe(2);
      expect(ctx.metrics.suspiciousContent).toBe(3);
      expect(ctx.metrics.llmTokens.get("input")).toBe(120);
      expect(ctx.metrics.llmTokens.get("output")).toBe(80);
      expect(ctx.metrics.llmCostUsdTotal).toBeCloseTo(0.13);
    });

    it("tracks duration/count histograms", () => {
      observeGenerationDuration(ctx, 12);
      observeHighlightsCount(ctx, 4);
      observeCitationsCount(ctx, 7);
      incrementDuplicatesSkipped(ctx, 1);

      expect(ctx.metrics.generationDurationSeconds.count).toBe(1);
      expect(ctx.metrics.highlightsCount.count).toBe(1);
      expect(ctx.metrics.citationsCount.count).toBe(1);
      expect(ctx.metrics.duplicatesSkipped).toBe(1);
    });

    it("tracks categorized errors", () => {
      incrementError(ctx, "parse_error");
      incrementError(ctx, "parse_error");
      incrementError(ctx, "llm_error");

      expect(ctx.metrics.errors.get("parse_error")).toBe(2);
      expect(ctx.metrics.errors.get("llm_error")).toBe(1);
    });
  });

  describe("formatMetrics", () => {
    it("outputs Prometheus-compatible text", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;
      incrementGeneration(ctx, "skipped", 2);
      incrementGeneration(ctx, "failure", 1);
      observeGenerationDuration(ctx, 8);
      incrementDuplicatesSkipped(ctx, 1);
      incrementBudgetExceeded(ctx, 1);
      incrementSuspiciousContent(ctx, 1);
      incrementLlmTokens(ctx, "input", 100);
      incrementLlmCostUsd(ctx, 0.05);
      observeHighlightsCount(ctx, 3);
      observeCitationsCount(ctx, 6);
      incrementError(ctx, "parse_error", 2);

      const output = formatMetrics(ctx);

      expect(output).toContain('ri_brief_generation_total{status="skipped"} 2');
      expect(output).toContain('ri_brief_generation_total{status="failure"} 1');
      expect(output).toContain("ri_brief_generation_duration_seconds_bucket");
      expect(output).toContain("ri_brief_duplicates_skipped_total 1");
      expect(output).toContain("ri_brief_budget_remaining_usd 5");
      expect(output).toContain("ri_brief_budget_exceeded_total 1");
      expect(output).toContain("ri_brief_suspicious_content_total 1");
      expect(output).toContain('ri_brief_llm_tokens_total{direction="input"} 100');
      expect(output).toContain("ri_brief_llm_cost_usd_total 0.05");
      expect(output).toContain("ri_brief_highlights_count_bucket");
      expect(output).toContain("ri_brief_citations_count_bucket");
      expect(output).toContain('ri_brief_errors_total{error_type="parse_error"} 2');
      expect(output).toContain("ri_brief_up 1");
    });

    it("shows up=0 when unhealthy", () => {
      const output = formatMetrics(ctx);
      expect(output).toContain("ri_brief_up 0");
    });

    it("escapes special characters in error_type labels", () => {
      incrementError(ctx, 'quote"in"label');

      const output = formatMetrics(ctx);
      expect(output).toContain('error_type="quote\\"in\\"label"');
    });
  });

  describe("createHealthHandler", () => {
    it("returns 200 for /health when healthy", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;

      const handler = createHealthHandler(createHandlers(ctx));
      const res = makeResponse();
      handler(makeRequest("GET", "/health"), res);

      expect(res.statusCode).toBe(200);
      const body = JSON.parse(res.body);
      expect(body.status).toBe("healthy");
    });

    it("returns 503 for /health when unhealthy", () => {
      const handler = createHealthHandler(createHandlers(ctx));
      const res = makeResponse();
      handler(makeRequest("GET", "/health"), res);

      expect(res.statusCode).toBe(503);
    });

    it("returns 200 for /ready only when kafka+postgres+redis are healthy", () => {
      const handler = createHealthHandler(createHandlers(ctx));
      const unhealthy = makeResponse();
      handler(makeRequest("GET", "/ready"), unhealthy);
      expect(unhealthy.statusCode).toBe(503);

      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;

      const healthy = makeResponse();
      handler(makeRequest("GET", "/ready"), healthy);
      expect(healthy.statusCode).toBe(200);
      expect(JSON.parse(healthy.body).ready).toBe(true);
    });

    it("returns metrics on /metrics", () => {
      const handler = createHealthHandler(createHandlers(ctx));
      const res = makeResponse();
      handler(makeRequest("GET", "/metrics"), res);

      expect(res.statusCode).toBe(200);
      expect(res.headers["Content-Type"]).toBe("text/plain; charset=utf-8");
      expect(res.body).toContain("ri_brief_generation_total");
    });
  });
});
