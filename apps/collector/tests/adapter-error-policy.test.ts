import { describe, expect, it, vi } from "vitest";
import { createAdapterErrorPolicy } from "../src/adapter-error-policy.js";
import { createHealthContext } from "../src/health.js";
import type { Source } from "../src/types.js";

function createBackoffController() {
  return {
    waitRateLimit: vi.fn(async () => undefined),
    waitTransient: vi.fn(async () => undefined),
  };
}

function getFailedCount(
  source: Source,
  errorType: string,
  context: ReturnType<typeof createHealthContext>
): number {
  return context.metrics.eventsFailed.get(source)?.get(errorType) ?? 0;
}

describe("adapter error policy", () => {
  it("uses the rate-limit strategy for HTTP 429 errors", async () => {
    const policy = createAdapterErrorPolicy();
    const backoff = createBackoffController();
    const healthContext = createHealthContext();

    await policy.handle(new Error("HTTP 429"), {
      healthContext,
      adapterSource: "rss",
      backoff,
    });

    expect(getFailedCount("rss", "rate_limit", healthContext)).toBe(1);
    expect(healthContext.metrics.rateLimitBackoff.get("rss")).toBe(1);
    expect(backoff.waitRateLimit).toHaveBeenCalledOnce();
    expect(backoff.waitTransient).not.toHaveBeenCalled();
  });

  it("uses the transient strategy for retryable network errors", async () => {
    const policy = createAdapterErrorPolicy();
    const backoff = createBackoffController();
    const healthContext = createHealthContext();

    await policy.handle(new Error("ECONNRESET while fetching adapter feed"), {
      healthContext,
      adapterSource: "hackernews",
      backoff,
    });

    expect(getFailedCount("hackernews", "network_error", healthContext)).toBe(1);
    expect(healthContext.metrics.rateLimitBackoff.get("hackernews")).toBeUndefined();
    expect(backoff.waitTransient).toHaveBeenCalledOnce();
    expect(backoff.waitRateLimit).not.toHaveBeenCalled();
  });

  it("uses the fallback strategy for unknown errors", async () => {
    const policy = createAdapterErrorPolicy();
    const backoff = createBackoffController();
    const healthContext = createHealthContext();

    await policy.handle(new Error("Kafka broker unavailable"), {
      healthContext,
      adapterSource: "lobsters",
      backoff,
    });

    expect(getFailedCount("lobsters", "kafka_error", healthContext)).toBe(1);
    expect(backoff.waitTransient).toHaveBeenCalledOnce();
    expect(backoff.waitRateLimit).not.toHaveBeenCalled();
  });

  it("falls back to parse_error for non-Error values", async () => {
    const policy = createAdapterErrorPolicy();
    const backoff = createBackoffController();
    const healthContext = createHealthContext();

    await policy.handle({ status: "unexpected" }, {
      healthContext,
      adapterSource: "rss",
      backoff,
    });

    expect(getFailedCount("rss", "parse_error", healthContext)).toBe(1);
    expect(backoff.waitTransient).toHaveBeenCalledOnce();
    expect(backoff.waitRateLimit).not.toHaveBeenCalled();
  });

  it("requires at least one strategy", () => {
    expect(() => createAdapterErrorPolicy([])).toThrow(
      "Adapter error policy requires at least one strategy"
    );
  });
});
