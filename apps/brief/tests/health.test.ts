import { describe, expect, it, vi, beforeEach } from "vitest";

vi.mock("../src/config.js", () => ({
  getConfig: () => ({
    SERVICE_NAME: "brief",
    PORT: 3000,
    LOG_LEVEL: "info",
  }),
}));

import { createHealthHandler } from "@rising-intelligence/shared";
import {
  createHealthContext,
  createHandlers,
  getHealthStatus,
  formatMetrics,
  incrementConsumed,
  incrementMalformed,
  incrementError,
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
    ctx = createHealthContext();
  });

  describe("getHealthStatus", () => {
    it("returns unhealthy when kafka is not connected", () => {
      const status = getHealthStatus(ctx);
      expect(status.status).toBe("unhealthy");
      expect(status.checks.kafka).toBe("error");
    });

    it("returns healthy when kafka is connected", () => {
      ctx.kafkaHealthy = true;

      const status = getHealthStatus(ctx);
      expect(status.status).toBe("healthy");
      expect(status.checks.kafka).toBe("ok");
    });

    it("includes uptime", () => {
      const status = getHealthStatus(ctx);
      expect(typeof status.uptime_seconds).toBe("number");
      expect(status.uptime_seconds).toBeGreaterThanOrEqual(0);
    });
  });

  describe("metric incrementers", () => {
    it("increments consumed count", () => {
      incrementConsumed(ctx);
      incrementConsumed(ctx, 5);
      expect(ctx.metrics.requestsConsumed).toBe(6);
    });

    it("increments malformed count", () => {
      incrementMalformed(ctx);
      incrementMalformed(ctx, 3);
      expect(ctx.metrics.malformedMessages).toBe(4);
    });

    it("increments errors by type", () => {
      incrementError(ctx, "parse_error");
      incrementError(ctx, "parse_error");
      incrementError(ctx, "llm_error");

      expect(ctx.metrics.errors.get("parse_error")).toBe(2);
      expect(ctx.metrics.errors.get("llm_error")).toBe(1);
    });
  });

  describe("formatMetrics", () => {
    it("outputs Prometheus-compatible text", () => {
      incrementConsumed(ctx, 10);
      incrementMalformed(ctx, 2);
      incrementError(ctx, "parse_error", 3);
      ctx.kafkaHealthy = true;

      const output = formatMetrics(ctx);

      expect(output).toContain("ri_brief_requests_consumed_total 10");
      expect(output).toContain("ri_brief_messages_malformed_total 2");
      expect(output).toContain('ri_brief_errors_total{error_type="parse_error"} 3');
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

    it("handles /healthz alias", () => {
      ctx.kafkaHealthy = true;

      const handler = createHealthHandler(createHandlers(ctx));
      const res = makeResponse();
      handler(makeRequest("GET", "/healthz"), res);

      expect(res.statusCode).toBe(200);
    });

    it("returns 200 for /ready when kafka healthy", () => {
      ctx.kafkaHealthy = true;

      const handler = createHealthHandler(createHandlers(ctx));
      const res = makeResponse();
      handler(makeRequest("GET", "/ready"), res);

      expect(res.statusCode).toBe(200);
      const body = JSON.parse(res.body);
      expect(body.ready).toBe(true);
    });

    it("returns 503 for /ready when kafka unhealthy", () => {
      const handler = createHealthHandler(createHandlers(ctx));
      const res = makeResponse();
      handler(makeRequest("GET", "/ready"), res);

      expect(res.statusCode).toBe(503);
      const body = JSON.parse(res.body);
      expect(body.ready).toBe(false);
    });

    it("handles /readyz alias", () => {
      const handler = createHealthHandler(createHandlers(ctx));
      const res = makeResponse();
      handler(makeRequest("GET", "/readyz"), res);

      expect(res.statusCode).toBe(503);
    });

    it("returns metrics on /metrics", () => {
      const handler = createHealthHandler(createHandlers(ctx));
      const res = makeResponse();
      handler(makeRequest("GET", "/metrics"), res);

      expect(res.statusCode).toBe(200);
      expect(res.headers["Content-Type"]).toBe("text/plain; charset=utf-8");
      expect(res.body).toContain("ri_brief_requests_consumed_total");
    });

    it("returns 404 for unknown paths", () => {
      const handler = createHealthHandler(createHandlers(ctx));
      const res = makeResponse();
      handler(makeRequest("GET", "/unknown"), res);

      expect(res.statusCode).toBe(404);
    });

    it("returns 405 for non-GET methods", () => {
      const handler = createHealthHandler(createHandlers(ctx));
      const res = makeResponse();
      handler(makeRequest("POST", "/health"), res);

      expect(res.statusCode).toBe(405);
    });
  });
});
