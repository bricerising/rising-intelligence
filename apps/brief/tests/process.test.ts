import { afterEach, describe, expect, it, vi } from "vitest";
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

function makeContext(overrides: Record<string, unknown> = {}) {
  const create = vi.fn().mockResolvedValue(undefined);
  return {
    config: {
      KAFKA_TOPIC_SUMMARY_RESULTS: "summary.results",
      LLM_PROVIDER: "internal",
      LLM_DAILY_BUDGET_USD: 5,
      LLM_ENDPOINT_URL: "http://mock-llm:8080/v1/generate",
      LLM_TIMEOUT_MS: 5000,
    },
    logger: makeLogger(),
    healthContext: createHealthContext(5),
    prisma: {
      briefResult: {
        findUnique: vi.fn().mockResolvedValue(null),
        create,
      },
    },
    redis: {
      eval: vi.fn().mockResolvedValue([1, "0.02"]),
    },
    producer: {
      send: vi.fn().mockResolvedValue(undefined),
    },
    ...overrides,
  } as any;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("processSummaryRequest", () => {
  it("persists and publishes a successful brief result", async () => {
    const ctx = makeContext();

    await processSummaryRequest(ctx, makeRequest());

    expect(ctx.prisma.briefResult.create).toHaveBeenCalledOnce();
    expect(ctx.redis.eval).toHaveBeenCalledOnce();
    expect(ctx.producer.send).toHaveBeenCalledOnce();
    expect(ctx.healthContext.metrics.generation.get("success")).toBe(1);
    expect(ctx.healthContext.metrics.llmCostUsdTotal).toBeGreaterThan(0);
  });

  it("uses http provider when configured", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        title: "Mock Brief",
        highlights: [
          {
            topic: "aws.bedrock",
            what_happened: "Model update landed [1]",
            why_it_matters: "Lower latency for key workloads",
            suggested_action: "Re-check production defaults",
            citations: ["https://example.com/1"],
          },
        ],
        usage: {
          prompt_tokens: 120,
          completion_tokens: 80,
        },
        meta: {
          provider: "test-llm",
          model: "mock-v1",
          estimated_cost_usd: 0.02,
        },
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const ctx = makeContext({
      config: {
        KAFKA_TOPIC_SUMMARY_RESULTS: "summary.results",
        LLM_PROVIDER: "http",
        LLM_ENDPOINT_URL: "http://mock-llm:8080/v1/generate",
        LLM_TIMEOUT_MS: 5000,
        LLM_DAILY_BUDGET_USD: 5,
      },
    });

    await processSummaryRequest(ctx, makeRequest());

    expect(fetchMock).toHaveBeenCalledOnce();
    expect(ctx.prisma.briefResult.create).toHaveBeenCalledOnce();
    expect(ctx.producer.send).toHaveBeenCalledOnce();
    expect(ctx.healthContext.metrics.llmTokens.get("input")).toBe(120);
    expect(ctx.healthContext.metrics.llmTokens.get("output")).toBe(80);
  });

  it("republishes persisted result for duplicate requests", async () => {
    const existingPayload = {
      request_id: "req-1",
      produced_at: "2026-02-06T10:01:00.000Z",
      brief: {
        brief_id: "brief:req-1",
        generated_at: "2026-02-06T10:01:00.000Z",
        window: 1,
        title: "Existing Brief",
        highlights: [
          {
            topic: "aws.bedrock",
            what_happened: "Already generated",
            why_it_matters: "Still relevant",
            suggested_action: "Read source",
            citations: ["https://example.com/1"],
          },
        ],
        notes: "All highlights include source citations.",
        meta: {
          provider: "internal",
          model: "rule-based-v1",
          input_tokens: 100,
          output_tokens: 100,
          estimated_cost_usd: 0.02,
        },
      },
    };
    const ctx = makeContext({
      prisma: {
        briefResult: {
          findUnique: vi.fn().mockResolvedValue({
            status: "success",
            result: existingPayload,
          }),
          create: vi.fn(),
        },
      },
    });

    await processSummaryRequest(ctx, makeRequest());

    expect(ctx.prisma.briefResult.create).not.toHaveBeenCalled();
    expect(ctx.redis.eval).not.toHaveBeenCalled();
    expect(ctx.producer.send).toHaveBeenCalledOnce();
    expect(ctx.healthContext.metrics.duplicatesSkipped).toBe(1);
    expect(ctx.healthContext.metrics.generation.get("skipped")).toBe(1);
  });

  it("emits non-retryable failure when LLM citations are not grounded", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        title: "Ungrounded Brief",
        highlights: [
          {
            topic: "aws.bedrock",
            what_happened: "Model update landed [1]",
            why_it_matters: "Lower latency for key workloads",
            suggested_action: "Re-check production defaults",
            citations: ["https://not-in-evidence.example.com/1"],
          },
        ],
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const ctx = makeContext({
      config: {
        KAFKA_TOPIC_SUMMARY_RESULTS: "summary.results",
        LLM_PROVIDER: "http",
        LLM_ENDPOINT_URL: "http://mock-llm:8080/v1/generate",
        LLM_TIMEOUT_MS: 5000,
        LLM_DAILY_BUDGET_USD: 5,
      },
      redis: {
        eval: vi
          .fn()
          .mockResolvedValueOnce([1, "0.02"])
          .mockResolvedValueOnce("0"),
      },
    });

    await processSummaryRequest(ctx, makeRequest());

    expect(ctx.prisma.briefResult.create).toHaveBeenCalledOnce();
    const persistedPayload = ctx.prisma.briefResult.create.mock.calls[0][0].data.result;
    expect((persistedPayload as any).failure.error_code).toBe("grounding_error");
    expect((persistedPayload as any).failure.retryable).toBe(false);
    expect(ctx.producer.send).toHaveBeenCalledOnce();
    expect(ctx.healthContext.metrics.generation.get("failure")).toBe(1);
  });
});
