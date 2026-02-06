import { describe, it, expect } from "vitest";
import {
  createHealthContext,
  createHealthHandler,
  getHealthStatus,
  incrementCheckpointsWritten,
  incrementError,
  incrementEventsDlq,
  incrementEventsPublished,
  recordLastPoll,
} from "../src/health.js";

function createMockRes() {
  const res = {
    statusCode: 0,
    headers: {} as Record<string, string>,
    body: "",
    writeHead: (statusCode: number, headers?: Record<string, string>) => {
      res.statusCode = statusCode;
      res.headers = headers ?? {};
      return res;
    },
    end: (body?: string) => {
      res.body = body ?? "";
      return res;
    },
  };
  return res;
}

describe("health handler", () => {
  it("serves /health, /ready, and /metrics", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.checkpointsHealthy = true;
    ctx.allowlistHealthy = true;
    incrementEventsPublished(ctx, "rss", 2);

    const handler = createHealthHandler(ctx);

    const healthRes = createMockRes();
    handler({ method: "GET", url: "/health" } as any, healthRes as any);
    expect(healthRes.statusCode).toBe(200);
    expect(JSON.parse(healthRes.body)).toMatchObject({ status: "healthy" });

    const readyRes = createMockRes();
    handler({ method: "GET", url: "/ready" } as any, readyRes as any);
    expect(readyRes.statusCode).toBe(200);
    expect(JSON.parse(readyRes.body)).toMatchObject({ ready: true });

    const metricsRes = createMockRes();
    handler({ method: "GET", url: "/metrics" } as any, metricsRes as any);
    expect(metricsRes.statusCode).toBe(200);
    expect(metricsRes.body).toContain(
      'ri_collector_events_published_total{source="rss"} 2'
    );
  });

  it("returns 405 for non-GET methods and 404 for unknown routes", () => {
    const handler = createHealthHandler(createHealthContext());

    const methodRes = createMockRes();
    handler({ method: "POST", url: "/health" } as any, methodRes as any);
    expect(methodRes.statusCode).toBe(405);

    const notFoundRes = createMockRes();
    handler({ method: "GET", url: "/nope" } as any, notFoundRes as any);
    expect(notFoundRes.statusCode).toBe(404);
  });

  it("returns degraded health when a source is in error state", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.checkpointsHealthy = true;
    ctx.allowlistHealthy = true;
    ctx.sourceHealth.set("rss", {
      status: "error",
      error_message: "feed timeout",
    });

    const health = getHealthStatus(ctx);
    expect(health.status).toBe("degraded");
    expect(health.sources.rss).toMatchObject({
      status: "error",
      error_message: "feed timeout",
    });
  });

  it("returns unhealthy health when kafka or checkpoints fail", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = false;
    ctx.checkpointsHealthy = true;
    ctx.allowlistHealthy = true;

    const health = getHealthStatus(ctx);
    expect(health.status).toBe("unhealthy");
    expect(health.checks.kafka).toBe("error");
  });

  it("returns 503 for /ready when dependencies are not ready", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.checkpointsHealthy = false;
    ctx.allowlistHealthy = true;
    const handler = createHealthHandler(ctx);

    const readyRes = createMockRes();
    handler({ method: "GET", url: "/ready" } as any, readyRes as any);

    expect(readyRes.statusCode).toBe(503);
    expect(JSON.parse(readyRes.body)).toMatchObject({ ready: false });
  });

  it("exposes error and checkpoint metrics in /metrics output", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.checkpointsHealthy = true;
    ctx.allowlistHealthy = true;

    incrementEventsPublished(ctx, "rss", 3);
    incrementEventsDlq(ctx, "rss", 1);
    incrementError(ctx, "rss", "transient");
    incrementCheckpointsWritten(ctx, "rss");
    recordLastPoll(ctx, "rss");

    const handler = createHealthHandler(ctx);
    const metricsRes = createMockRes();
    handler({ method: "GET", url: "/metrics" } as any, metricsRes as any);

    expect(metricsRes.statusCode).toBe(200);
    expect(metricsRes.body).toContain(
      'ri_collector_events_dlq_total{source="rss"} 1'
    );
    expect(metricsRes.body).toContain(
      'ri_collector_errors_total{source="rss",type="transient"} 1'
    );
    expect(metricsRes.body).toContain(
      'ri_collector_checkpoints_written_total{source="rss"} 1'
    );
    expect(metricsRes.body).toContain(
      'ri_collector_last_poll_timestamp_seconds{source="rss"}'
    );
  });
});
