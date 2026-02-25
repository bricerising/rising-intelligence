import { describe, expect, it } from "vitest";
import { createHealthHandler } from "@rising-intelligence/shared/http";
import {
  createHealthContext,
  createHandlers,
  getHealthStatus,
  incrementError,
  incrementEventsProcessed,
  incrementEventsSkipped,
  observeBatchSize,
  observePostgresWriteDuration,
  observeRedisWriteDuration,
  setConsumerLag,
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

function makeHandler(ctx: ReturnType<typeof createHealthContext>) {
  return createHealthHandler(createHandlers(ctx));
}

describe("persister health", () => {
  it("serves /health /ready /metrics", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;

    incrementEventsProcessed(ctx, "rss", 3);
    incrementEventsSkipped(ctx, "duplicate", 2);
    incrementError(ctx, "parse_error", 1);
    setConsumerLag(ctx, 0, 12n);
    observePostgresWriteDuration(ctx, 0.15);
    observeRedisWriteDuration(ctx, 0.03);
    observeBatchSize(ctx, 5);

    const handler = makeHandler(ctx);

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
      'ri_persister_events_processed_total{source="rss"} 3'
    );
    expect(metricsRes.body).toContain(
      'ri_persister_events_skipped_total{reason="duplicate"} 2'
    );
    expect(metricsRes.body).toContain(
      'ri_persister_errors_total{error_type="parse_error"} 1'
    );
    expect(metricsRes.body).toContain(
      'ri_persister_consumer_lag{partition="0"} 12'
    );
    expect(metricsRes.body).toContain("ri_persister_postgres_write_duration_seconds_bucket");
    expect(metricsRes.body).toContain("ri_persister_batch_size_bucket");
  });

  it("returns unhealthy when redis is unavailable", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = false;

    const status = getHealthStatus(ctx);
    expect(status.status).toBe("unhealthy");
    expect(status.checks.redis).toBe("error");
  });

  it("returns unhealthy when postgres is unavailable", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = false;
    ctx.redisHealthy = true;

    const status = getHealthStatus(ctx);
    expect(status.status).toBe("unhealthy");
    expect(status.checks.postgres).toBe("error");
  });

  it("returns 405 for non-GET methods and 404 for unknown routes", () => {
    const handler = makeHandler(createHealthContext());

    const methodRes = createMockRes();
    handler({ method: "POST", url: "/health" } as any, methodRes as any);
    expect(methodRes.statusCode).toBe(405);

    const notFoundRes = createMockRes();
    handler({ method: "GET", url: "/nope" } as any, notFoundRes as any);
    expect(notFoundRes.statusCode).toBe(404);
  });

  it("returns 503 readiness when circuit is open", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;
    ctx.circuitOpen = true;

    const handler = makeHandler(ctx);
    const readyRes = createMockRes();
    handler({ method: "GET", url: "/ready" } as any, readyRes as any);

    expect(readyRes.statusCode).toBe(503);
    expect(JSON.parse(readyRes.body)).toMatchObject({
      ready: false,
      circuit_open: true,
    });
  });

  it("returns degraded when consumer lag exceeds 1000", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;

    setConsumerLag(ctx, 0, 1001n);

    const status = getHealthStatus(ctx);
    expect(status.status).toBe("degraded");
    expect(status.max_consumer_lag).toBe("1001");
  });

  it("returns healthy when consumer lag is exactly 1000", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;

    setConsumerLag(ctx, 0, 1000n);

    const status = getHealthStatus(ctx);
    expect(status.status).toBe("healthy");
  });

  it("returns degraded when circuit is open", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;
    ctx.circuitOpen = true;

    const status = getHealthStatus(ctx);
    expect(status.status).toBe("degraded");
    expect(status.checks.circuit).toBe("open");
  });

  it("returns unhealthy when kafka is down", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = false;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;

    const status = getHealthStatus(ctx);
    expect(status.status).toBe("unhealthy");
    expect(status.checks.kafka).toBe("error");
  });

  it("serves /healthz and /readyz aliases", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;

    const handler = makeHandler(ctx);

    const healthzRes = createMockRes();
    handler({ method: "GET", url: "/healthz" } as any, healthzRes as any);
    expect(healthzRes.statusCode).toBe(200);
    expect(JSON.parse(healthzRes.body)).toMatchObject({ status: "healthy" });

    const readyzRes = createMockRes();
    handler({ method: "GET", url: "/readyz" } as any, readyzRes as any);
    expect(readyzRes.statusCode).toBe(200);
    expect(JSON.parse(readyzRes.body)).toMatchObject({ ready: true });
  });

  it("returns 503 on /health when unhealthy", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = false;
    ctx.postgresHealthy = false;

    const handler = makeHandler(ctx);
    const res = createMockRes();
    handler({ method: "GET", url: "/health" } as any, res as any);

    expect(res.statusCode).toBe(503);
    expect(JSON.parse(res.body).status).toBe("unhealthy");
  });

  it("includes ri_persister_up gauge in metrics", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;

    const handler = makeHandler(ctx);
    const res = createMockRes();
    handler({ method: "GET", url: "/metrics" } as any, res as any);

    expect(res.body).toContain("ri_persister_up 1");
  });

  it("reports ri_persister_up 0 when unhealthy", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = false;
    ctx.postgresHealthy = false;

    const handler = makeHandler(ctx);
    const res = createMockRes();
    handler({ method: "GET", url: "/metrics" } as any, res as any);

    expect(res.body).toContain("ri_persister_up 0");
  });

  it("formats histogram buckets correctly", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;

    observePostgresWriteDuration(ctx, 0.03);
    observePostgresWriteDuration(ctx, 0.12);
    observePostgresWriteDuration(ctx, 0.8);

    const handler = makeHandler(ctx);
    const res = createMockRes();
    handler({ method: "GET", url: "/metrics" } as any, res as any);

    // 0.03 falls in le=0.05 bucket, 0.12 in le=0.25, 0.8 in le=1
    expect(res.body).toContain('ri_persister_postgres_write_duration_seconds_bucket{le="0.05"} 1');
    expect(res.body).toContain('ri_persister_postgres_write_duration_seconds_bucket{le="0.25"} 2');
    expect(res.body).toContain('ri_persister_postgres_write_duration_seconds_bucket{le="1"} 3');
    expect(res.body).toContain('ri_persister_postgres_write_duration_seconds_bucket{le="+Inf"} 3');
    expect(res.body).toContain("ri_persister_postgres_write_duration_seconds_count 3");
  });

  it("includes last_event_at in health when set", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;
    ctx.lastEventAt = new Date("2026-02-06T12:00:00.000Z");

    const status = getHealthStatus(ctx);
    expect(status.last_event_at).toBe("2026-02-06T12:00:00.000Z");
  });

  it("omits last_event_at from health when not set", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;

    const status = getHealthStatus(ctx);
    expect(status.last_event_at).toBeUndefined();
  });

  it("reports max lag across multiple partitions", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;

    setConsumerLag(ctx, 0, 100n);
    setConsumerLag(ctx, 1, 500n);
    setConsumerLag(ctx, 2, 200n);

    const status = getHealthStatus(ctx);
    expect(status.max_consumer_lag).toBe("500");
  });

  it("handles empty metrics without errors", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.postgresHealthy = true;
    ctx.redisHealthy = true;

    const handler = makeHandler(ctx);
    const res = createMockRes();
    handler({ method: "GET", url: "/metrics" } as any, res as any);

    expect(res.statusCode).toBe(200);
    expect(res.body).toContain("ri_persister_up 1");
    expect(res.body).toContain("ri_persister_postgres_write_duration_seconds_count 0");
  });
});
