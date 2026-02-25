import { describe, it, expect } from "vitest";
import { createHealthHandler } from "@rising-intelligence/shared/http";
import {
  createHealthContext,
  createHandlers,
  getHealthStatus,
  incrementCheckpointUpdated,
  incrementEventsFailed,
  incrementRssFeedError,
  incrementEventsIngested,
  observePollDuration,
  observePollItemsCount,
  incrementRateLimitBackoff,
  incrementTopicsExtracted,
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
    incrementEventsIngested(ctx, "rss", 2);

    const handler = createHealthHandler(createHandlers(ctx));

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
      'ri_collector_events_ingested_total{source="rss"} 2'
    );
  });

  it("returns 405 for non-GET methods and 404 for unknown routes", () => {
    const handler = createHealthHandler(createHandlers(createHealthContext()));

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
    const handler = createHealthHandler(createHandlers(ctx));

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

    incrementEventsIngested(ctx, "rss", 3);
    incrementEventsFailed(ctx, "rss", "network_error", 1);
    incrementRssFeedError(
      ctx,
      {
        feed: "OpenAI News",
        feedUrl: "https://openai.com/news/rss.xml",
        errorType: "parse_error",
      }
    );
    incrementCheckpointUpdated(ctx, "rss");
    incrementRateLimitBackoff(ctx, "rss", 1);
    incrementTopicsExtracted(ctx, "aws.bedrock", 2);
    observePollDuration(ctx, "rss", 1.2);
    observePollItemsCount(ctx, "rss", 3);

    const handler = createHealthHandler(createHandlers(ctx));
    const metricsRes = createMockRes();
    handler({ method: "GET", url: "/metrics" } as any, metricsRes as any);

    expect(metricsRes.statusCode).toBe(200);
    expect(metricsRes.body).toContain(
      'ri_collector_events_failed_total{source="rss",error_type="network_error"} 1'
    );
    expect(metricsRes.body).toContain(
      'ri_collector_rss_feed_errors_total{source="rss",feed="OpenAI News",feed_url="https://openai.com/news/rss.xml",error_type="parse_error"} 1'
    );
    expect(metricsRes.body).toContain(
      'ri_collector_checkpoint_updated_total{source="rss"} 1'
    );
    expect(metricsRes.body).toContain(
      'ri_collector_rate_limit_backoff_total{source="rss"} 1'
    );
    expect(metricsRes.body).toContain(
      'ri_collector_topics_extracted_total{topic="aws.bedrock"} 2'
    );
    expect(metricsRes.body).toContain("ri_collector_poll_duration_seconds_bucket");
    expect(metricsRes.body).toContain("ri_collector_poll_items_count_bucket");
  });

  it("exports per-source staleness gauges for alerting", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.checkpointsHealthy = true;
    ctx.allowlistHealthy = true;

    const rssLastSuccess = "2026-02-11T00:00:05.000Z";
    ctx.sourceHealth.set("rss", {
      status: "healthy",
      last_poll_at: rssLastSuccess,
      items_fetched: 3,
    });
    ctx.sourceHealth.set("hackernews", {
      status: "error",
      last_poll_at: "2026-02-10T23:59:05.000Z",
      error_message: "rate limited",
    });

    const handler = createHealthHandler(createHandlers(ctx));
    const metricsRes = createMockRes();
    handler({ method: "GET", url: "/metrics" } as any, metricsRes as any);

    expect(metricsRes.statusCode).toBe(200);
    expect(metricsRes.body).toContain(
      `ri_collector_last_success_timestamp{source="rss"} ${Math.floor(Date.parse(rssLastSuccess) / 1000)}`
    );
    expect(metricsRes.body).toContain('ri_collector_source_healthy{source="rss"} 1');
    expect(metricsRes.body).toContain('ri_collector_source_healthy{source="hackernews"} 0');
  });

  it("marks source_healthy=0 when no successful poll timestamp exists", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.checkpointsHealthy = true;
    ctx.allowlistHealthy = true;
    ctx.sourceHealth.set("rss", {
      status: "healthy",
    });

    const handler = createHealthHandler(createHandlers(ctx));
    const metricsRes = createMockRes();
    handler({ method: "GET", url: "/metrics" } as any, metricsRes as any);

    expect(metricsRes.statusCode).toBe(200);
    expect(metricsRes.body).toContain('ri_collector_last_success_timestamp{source="rss"} 0');
    expect(metricsRes.body).toContain('ri_collector_source_healthy{source="rss"} 0');
  });

  it("caps topic metric cardinality and aggregates overflow into other", () => {
    const ctx = createHealthContext();
    ctx.kafkaHealthy = true;
    ctx.checkpointsHealthy = true;
    ctx.allowlistHealthy = true;

    for (let i = 0; i < 40; i += 1) {
      incrementTopicsExtracted(ctx, `topic.${i}`);
    }

    const handler = createHealthHandler(createHandlers(ctx));
    const metricsRes = createMockRes();
    handler({ method: "GET", url: "/metrics" } as any, metricsRes as any);

    expect(metricsRes.statusCode).toBe(200);
    expect(metricsRes.body).toContain('ri_collector_topics_extracted_total{topic="other"} 10');
    expect(metricsRes.body).toContain('ri_collector_topics_extracted_total{topic="topic.29"} 1');
    expect(metricsRes.body).not.toContain('ri_collector_topics_extracted_total{topic="topic.39"}');
  });
});
