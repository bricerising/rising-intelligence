import { describe, expect, it, vi } from "vitest";
import { createHealthContext } from "../src/health.js";
import { processSummaryRequest } from "../src/process.js";
import type { ParsedSummaryRequest } from "../src/types.js";

function makeLogger() {
  const logger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: vi.fn(),
  };
  logger.child.mockReturnValue(logger);
  return logger as any;
}

function makeRequest(): ParsedSummaryRequest {
  return {
    requestId: "req-1",
    requestedAt: new Date("2026-02-06T10:00:00.000Z"),
    type: "daily",
    windows: [1, 2],
    budget: {
      dailyBudgetUsd: 5,
      maxTopics: 5,
      maxEvidencePerTopic: 3,
      maxOutputTokens: 1200,
    },
    topics: [
      {
        topic: "aws.bedrock",
        metrics: [
          {
            topic: "aws.bedrock",
            window: 2,
            score: 12,
            volume: 18,
            acceleration: 0.6,
          },
        ],
        evidence: [
          {
            eventId: "evt-1",
            source: "rss",
            url: "https://example.com/1",
            title: "Update",
            publishedAt: new Date("2026-02-06T09:30:00.000Z"),
            fetchedAt: new Date("2026-02-06T09:40:00.000Z"),
            textExcerpt: "details",
          },
        ],
      },
    ],
  };
}

describe("processSummaryRequest", () => {
  it("persists and publishes a successful brief result", async () => {
    const create = vi.fn().mockResolvedValue(undefined);
    const producer = {
      send: vi.fn().mockResolvedValue(undefined),
    };
    const redis = {
      get: vi.fn().mockResolvedValue("0"),
      incrbyfloat: vi.fn().mockResolvedValue("0.02"),
      expire: vi.fn().mockResolvedValue(1),
    };
    const ctx = {
      config: {
        KAFKA_TOPIC_SUMMARY_RESULTS: "summary.results",
        LLM_DAILY_BUDGET_USD: 5,
      },
      logger: makeLogger(),
      healthContext: createHealthContext(5),
      prisma: {
        briefResult: {
          findUnique: vi.fn().mockResolvedValue(null),
          create,
        },
      },
      redis,
      producer,
    } as any;

    await processSummaryRequest(ctx, makeRequest());

    expect(create).toHaveBeenCalledOnce();
    expect(producer.send).toHaveBeenCalledOnce();
    expect(ctx.healthContext.metrics.generation.get("success")).toBe(1);
    expect(ctx.healthContext.metrics.llmCostUsdTotal).toBeGreaterThan(0);
  });

  it("skips duplicate requests without publishing", async () => {
    const producer = {
      send: vi.fn().mockResolvedValue(undefined),
    };
    const ctx = {
      config: {
        KAFKA_TOPIC_SUMMARY_RESULTS: "summary.results",
        LLM_DAILY_BUDGET_USD: 5,
      },
      logger: makeLogger(),
      healthContext: createHealthContext(5),
      prisma: {
        briefResult: {
          findUnique: vi.fn().mockResolvedValue({ requestId: "req-1" }),
          create: vi.fn(),
        },
      },
      redis: {
        get: vi.fn(),
        incrbyfloat: vi.fn(),
        expire: vi.fn(),
      },
      producer,
    } as any;

    await processSummaryRequest(ctx, makeRequest());

    expect(ctx.prisma.briefResult.create).not.toHaveBeenCalled();
    expect(producer.send).not.toHaveBeenCalled();
    expect(ctx.healthContext.metrics.duplicatesSkipped).toBe(1);
    expect(ctx.healthContext.metrics.generation.get("skipped")).toBe(1);
  });
});
