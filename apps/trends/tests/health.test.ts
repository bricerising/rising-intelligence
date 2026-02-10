import { describe, expect, it, vi, beforeEach } from "vitest";

vi.mock("../src/config.js", () => ({
  getConfig: () => ({
    SERVICE_NAME: "trends",
    PORT: 3000,
    LOG_LEVEL: "info",
    MAX_LAG_MESSAGES: 100,
  }),
}));

import {
  createHealthContext,
  getHealthStatus,
  formatMetrics,
  createHealthHandler,
  incrementEventsProcessed,
  incrementDuplicatesSkipped,
  incrementSnapshotPublished,
  incrementBriefTriggered,
  incrementBriefSkippedStaleData,
  incrementError,
  setConsumerLag,
  setTopicMetrics,
  clearTopicMetrics,
  observeSnapshotDuration,
  observeBaselineComputeDuration,
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

describe("trends health", () => {
  let ctx: HealthContext;

  beforeEach(() => {
    ctx = createHealthContext();
  });

  describe("getHealthStatus", () => {
    it("returns unhealthy when no deps are connected", () => {
      const status = getHealthStatus(ctx);
      expect(status.status).toBe("unhealthy");
      expect(status.checks.kafka).toBe("error");
      expect(status.checks.postgres).toBe("error");
      expect(status.checks.redis).toBe("error");
      expect(status.checks.allowlist).toBe("error");
    });

    it("returns healthy when all deps are connected", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;
      ctx.allowlistHealthy = true;

      const status = getHealthStatus(ctx);
      expect(status.status).toBe("healthy");
      expect(status.checks.kafka).toBe("ok");
      expect(status.checks.postgres).toBe("ok");
      expect(status.checks.redis).toBe("ok");
      expect(status.checks.allowlist).toBe("ok");
    });

    it("returns degraded when lag is high", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;
      ctx.allowlistHealthy = true;
      setConsumerLag(ctx, 0, 101n);

      const status = getHealthStatus(ctx);
      expect(status.status).toBe("degraded");
    });

    it("does not degrade when lag equals configured threshold", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;
      ctx.allowlistHealthy = true;
      setConsumerLag(ctx, 0, 100n);

      const status = getHealthStatus(ctx);
      expect(status.status).toBe("healthy");
    });

    it("returns unhealthy over degraded when deps are down", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = false;
      ctx.redisHealthy = true;
      ctx.allowlistHealthy = true;
      setConsumerLag(ctx, 0, 6000n);

      const status = getHealthStatus(ctx);
      expect(status.status).toBe("unhealthy");
    });

    it("includes last_event_at when set", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;
      ctx.allowlistHealthy = true;
      ctx.lastEventAt = new Date("2026-02-06T10:00:00.000Z");

      const status = getHealthStatus(ctx);
      expect(status.last_event_at).toBe("2026-02-06T10:00:00.000Z");
    });

    it("omits last_event_at when unset", () => {
      const status = getHealthStatus(ctx);
      expect(status.last_event_at).toBeUndefined();
    });
  });

  describe("metric incrementers", () => {
    it("increments events processed", () => {
      incrementEventsProcessed(ctx);
      incrementEventsProcessed(ctx, 5);
      expect(ctx.metrics.eventsProcessed).toBe(6);
    });

    it("increments duplicates skipped", () => {
      incrementDuplicatesSkipped(ctx);
      incrementDuplicatesSkipped(ctx, 3);
      expect(ctx.metrics.duplicatesSkipped).toBe(4);
    });

    it("increments snapshot published by window", () => {
      incrementSnapshotPublished(ctx, "15m");
      incrementSnapshotPublished(ctx, "15m", 2);
      incrementSnapshotPublished(ctx, "60m");

      expect(ctx.metrics.snapshotPublished.get("15m")).toBe(3);
      expect(ctx.metrics.snapshotPublished.get("60m")).toBe(1);
    });

    it("increments brief trigger counters", () => {
      incrementBriefTriggered(ctx, "daily");
      incrementBriefTriggered(ctx, "threshold", 2);
      incrementBriefSkippedStaleData(ctx, 3);

      expect(ctx.metrics.briefTriggered.get("daily")).toBe(1);
      expect(ctx.metrics.briefTriggered.get("threshold")).toBe(2);
      expect(ctx.metrics.briefSkippedStaleData).toBe(3);
    });

    it("increments errors by type", () => {
      incrementError(ctx, "parse_error");
      incrementError(ctx, "parse_error");
      incrementError(ctx, "redis_error");

      expect(ctx.metrics.errors.get("parse_error")).toBe(2);
      expect(ctx.metrics.errors.get("redis_error")).toBe(1);
    });

    it("sets consumer lag per partition", () => {
      setConsumerLag(ctx, 0, 100n);
      setConsumerLag(ctx, 1, 200n);
      setConsumerLag(ctx, 0, 50n);

      expect(ctx.metrics.consumerLag.get(0)).toBe(50n);
      expect(ctx.metrics.consumerLag.get(1)).toBe(200n);
    });
  });

  describe("topic metrics", () => {
    it("sets and clears topic metrics by window", () => {
      setTopicMetrics(ctx, "aws.bedrock", "15m", 10, 20);
      setTopicMetrics(ctx, "ai.openai", "15m", 5, 8);
      setTopicMetrics(ctx, "aws.bedrock", "60m", 30, 50);

      clearTopicMetrics(ctx, "15m");

      expect(ctx.metrics.topicVolume.size).toBe(1);
      expect(ctx.metrics.topicScore.size).toBe(1);
      expect(ctx.metrics.topicVolume.has("aws.bedrock|60m")).toBe(true);
    });
  });

  describe("observeSnapshotDuration", () => {
    it("records values in histogram", () => {
      observeSnapshotDuration(ctx, "15m", 0.05);
      observeSnapshotDuration(ctx, "15m", 1.5);

      const hist = ctx.metrics.snapshotDurationSeconds.get("15m")!;
      expect(hist.count).toBe(2);
      expect(hist.sum).toBeCloseTo(1.55);
    });

    it("records baseline compute duration", () => {
      observeBaselineComputeDuration(ctx, 0.2);
      observeBaselineComputeDuration(ctx, 0.8);

      expect(ctx.metrics.baselineComputeDurationSeconds.count).toBe(2);
    });

    it("ignores negative values", () => {
      observeSnapshotDuration(ctx, "15m", -1);

      const hist = ctx.metrics.snapshotDurationSeconds.get("15m")!;
      expect(hist.count).toBe(0);
    });
  });

  describe("formatMetrics", () => {
    it("outputs Prometheus-compatible text", () => {
      incrementEventsProcessed(ctx, 42);
      incrementDuplicatesSkipped(ctx, 3);
      incrementBriefTriggered(ctx, "daily", 2);
      incrementBriefSkippedStaleData(ctx, 1);
      incrementError(ctx, "parse_error", 2);
      setConsumerLag(ctx, 0, 100n);
      observeBaselineComputeDuration(ctx, 0.4);

      const output = formatMetrics(ctx);

      expect(output).toContain("ri_trends_events_processed_total 42");
      expect(output).toContain("ri_trends_duplicates_skipped_total 3");
      expect(output).toContain('ri_trends_brief_triggered_total{type="daily"} 2');
      expect(output).toContain("ri_trends_brief_skipped_stale_data_total 1");
      expect(output).toContain('ri_trends_errors_total{error_type="parse_error"} 2');
      expect(output).toContain('ri_trends_consumer_lag{partition="0"} 100');
      expect(output).toContain("ri_trends_baseline_compute_duration_seconds_bucket");
      expect(output).toContain("ri_trends_up");
    });

    it("escapes special characters in label values", () => {
      incrementError(ctx, 'quote"in"label');

      const output = formatMetrics(ctx);
      expect(output).toContain('error_type="quote\\"in\\"label"');
    });

    it("includes histogram buckets", () => {
      observeSnapshotDuration(ctx, "15m", 0.05);

      const output = formatMetrics(ctx);
      expect(output).toContain("ri_trends_snapshot_duration_seconds_bucket");
      expect(output).toContain("ri_trends_snapshot_duration_seconds_sum");
      expect(output).toContain("ri_trends_snapshot_duration_seconds_count");
    });

    it("emits snapshot histogram metadata only once", () => {
      observeSnapshotDuration(ctx, "15m", 0.05);
      observeSnapshotDuration(ctx, "60m", 0.1);

      const output = formatMetrics(ctx);

      const helpLines = output.match(/^# HELP ri_trends_snapshot_duration_seconds /gm) ?? [];
      const typeLines = output.match(/^# TYPE ri_trends_snapshot_duration_seconds histogram$/gm) ?? [];

      expect(helpLines).toHaveLength(1);
      expect(typeLines).toHaveLength(1);
    });
  });

  describe("createHealthHandler", () => {
    it("returns 200 for /health when healthy", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;
      ctx.allowlistHealthy = true;

      const handler = createHealthHandler(ctx);
      const res = makeResponse();
      handler(makeRequest("GET", "/health"), res);

      expect(res.statusCode).toBe(200);
      const body = JSON.parse(res.body);
      expect(body.status).toBe("healthy");
    });

    it("returns 503 for /health when unhealthy", () => {
      const handler = createHealthHandler(ctx);
      const res = makeResponse();
      handler(makeRequest("GET", "/health"), res);

      expect(res.statusCode).toBe(503);
      const body = JSON.parse(res.body);
      expect(body.status).toBe("unhealthy");
    });

    it("returns 200 for /healthz alias", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;
      ctx.allowlistHealthy = true;

      const handler = createHealthHandler(ctx);
      const res = makeResponse();
      handler(makeRequest("GET", "/healthz"), res);

      expect(res.statusCode).toBe(200);
    });

    it("returns 200 for /ready when all deps ready", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = true;
      ctx.redisHealthy = true;
      ctx.allowlistHealthy = true;

      const handler = createHealthHandler(ctx);
      const res = makeResponse();
      handler(makeRequest("GET", "/ready"), res);

      expect(res.statusCode).toBe(200);
      const body = JSON.parse(res.body);
      expect(body.ready).toBe(true);
    });

    it("returns 503 for /ready when not ready", () => {
      ctx.kafkaHealthy = true;
      ctx.postgresHealthy = false;
      ctx.redisHealthy = true;
      ctx.allowlistHealthy = true;

      const handler = createHealthHandler(ctx);
      const res = makeResponse();
      handler(makeRequest("GET", "/ready"), res);

      expect(res.statusCode).toBe(503);
      const body = JSON.parse(res.body);
      expect(body.ready).toBe(false);
    });

    it("returns 503 for /readyz alias when not ready", () => {
      const handler = createHealthHandler(ctx);
      const res = makeResponse();
      handler(makeRequest("GET", "/readyz"), res);

      expect(res.statusCode).toBe(503);
    });

    it("returns metrics on /metrics", () => {
      const handler = createHealthHandler(ctx);
      const res = makeResponse();
      handler(makeRequest("GET", "/metrics"), res);

      expect(res.statusCode).toBe(200);
      expect(res.headers["Content-Type"]).toBe("text/plain; charset=utf-8");
      expect(res.body).toContain("ri_trends_events_processed_total");
    });

    it("returns 404 for unknown paths", () => {
      const handler = createHealthHandler(ctx);
      const res = makeResponse();
      handler(makeRequest("GET", "/unknown"), res);

      expect(res.statusCode).toBe(404);
    });

    it("returns 405 for non-GET methods", () => {
      const handler = createHealthHandler(ctx);
      const res = makeResponse();
      handler(makeRequest("POST", "/health"), res);

      expect(res.statusCode).toBe(405);
    });
  });
});
