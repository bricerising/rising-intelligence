import { Source } from "@rising-intelligence/db";
import { describe, expect, it, vi } from "vitest";
import { createHealthContext } from "../src/health.js";
import {
  createQueryModeRequestResolver,
  type QueryModeRequestResolverContext,
} from "../src/query-mode-request-facade.js";
import type { ParsedSummaryRequest } from "../src/types.js";

function makeLogger() {
  return {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: vi.fn(),
  } as any;
}

function makeRequest(overrides: Partial<ParsedSummaryRequest> = {}): ParsedSummaryRequest {
  return {
    requestId: "req-query-1",
    requestedAt: new Date("2026-02-20T12:00:00.000Z"),
    type: "daily",
    windows: [],
    budget: {
      dailyBudgetUsd: 5,
      maxTopics: 2,
      maxEvidencePerTopic: 3,
      maxOutputTokens: 1200,
    },
    query: {
      lookbackDays: 7,
      topicGlobs: ["aws.*", "data.*"],
      maxEventsPerTopic: 4,
      evidenceStrategy: "diversity",
    },
    report: null,
    topics: [],
    ...overrides,
  };
}

function makeContext(overrides: Partial<QueryModeRequestResolverContext> = {}): QueryModeRequestResolverContext {
  return {
    config: {
      BRIEF_DEFAULT_LOOKBACK_DAYS: 7,
      BRIEF_MAX_LOOKBACK_DAYS: 30,
      BRIEF_MAX_QUERY_EVENTS_PER_TOPIC: 25,
    } as any,
    healthContext: createHealthContext(5),
    prisma: {
      briefTrendSnapshot: {
        findMany: vi.fn().mockResolvedValue([]),
      },
      rawEvent: {
        findMany: vi.fn().mockResolvedValue([]),
      },
    } as any,
    ...overrides,
  };
}

describe("query mode request resolver", () => {
  it("passes through explicit-topic requests without querying storage", async () => {
    const resolver = createQueryModeRequestResolver();
    const logger = makeLogger();
    const ctx = makeContext();
    const request = makeRequest({
      topics: [
        {
          topic: "aws.bedrock",
          metrics: [],
          evidence: [],
        },
      ],
      query: null,
    });

    const resolved = await resolver.resolve(ctx, request, logger);

    expect(resolved).toBe(request);
    expect(ctx.prisma.briefTrendSnapshot.findMany).not.toHaveBeenCalled();
    expect(ctx.prisma.rawEvent.findMany).not.toHaveBeenCalled();
  });

  it("hydrates query-mode topics and appends strategy-driven coverage warnings", async () => {
    const resolver = createQueryModeRequestResolver();
    const logger = makeLogger();
    const ctx = makeContext({
      prisma: {
        briefTrendSnapshot: {
          findMany: vi.fn().mockResolvedValue([
            {
              generatedAt: new Date("2026-02-20T11:00:00.000Z"),
              snapshot: {
                topics: [
                  { topic: "aws.bedrock", score: 9, volume: 8, acceleration: 1.2 },
                  { topic: "data.kafka", score: 7, volume: 6, acceleration: 0.8 },
                ],
              },
            },
          ]),
        },
        rawEvent: {
          findMany: vi.fn().mockResolvedValue([
            {
              eventId: "evt-relevant",
              source: Source.rss,
              url: "https://example.com/bedrock",
              title: "Bedrock launch notes",
              publishedAt: new Date("2026-02-20T10:00:00.000Z"),
              fetchedAt: new Date("2026-02-20T10:05:00.000Z"),
              text: "Highlights from the latest Bedrock release.",
              topics: ["aws.bedrock"],
              engagementScore: 3,
            },
            {
              eventId: "evt-irrelevant",
              source: Source.rss,
              url: "https://example.com/general",
              title: "General platform notes",
              publishedAt: new Date("2026-02-20T09:00:00.000Z"),
              fetchedAt: new Date("2026-02-20T09:05:00.000Z"),
              text: "No direct topic references in this update.",
              topics: ["aws.bedrock"],
              engagementScore: 1,
            },
          ]),
        },
      } as any,
    });

    const request = makeRequest({
      budget: {
        dailyBudgetUsd: 5,
        maxTopics: 1,
        maxEvidencePerTopic: 2,
        maxOutputTokens: 1200,
      },
      query: {
        lookbackDays: 7,
        topicGlobs: ["*"],
        maxEventsPerTopic: 5,
        evidenceStrategy: "recency",
      },
    });

    const resolved = await resolver.resolve(ctx, request, logger);

    expect(resolved.windows).toEqual([2]);
    expect(resolved.query).toEqual({
      lookbackDays: 7,
      topicGlobs: ["*"],
      maxEventsPerTopic: 2,
      evidenceStrategy: "recency",
    });
    expect(resolved.topics).toHaveLength(1);
    expect(resolved.topics[0]?.topic).toBe("aws.bedrock");
    expect(resolved.topics[0]?.evidence).toHaveLength(1);
    expect(ctx.healthContext.postgresHealthy).toBe(true);
    expect(resolved.coverageWarnings).toEqual([
      "1 subtopic(s) were excluded by top-level topic cap (1).",
      "1 candidate event(s) were excluded by topic relevance checks.",
    ]);
  });

  it("returns no_coverage when ranked topics have no grounded evidence", async () => {
    const resolver = createQueryModeRequestResolver();
    const logger = makeLogger();
    const ctx = makeContext({
      prisma: {
        briefTrendSnapshot: {
          findMany: vi.fn().mockResolvedValue([
            {
              generatedAt: new Date("2026-02-20T11:00:00.000Z"),
              snapshot: {
                topics: [
                  { topic: "aws.bedrock", score: 9, volume: 8, acceleration: 1.2 },
                ],
              },
            },
          ]),
        },
        rawEvent: {
          findMany: vi.fn().mockResolvedValue([]),
        },
      } as any,
    });

    await expect(resolver.resolve(ctx, makeRequest(), logger)).rejects.toMatchObject({
      name: "NonRetryableProcessingError",
      code: "no_coverage",
    });
  });

  it("returns invalid_request when lookback exceeds configured maximum", async () => {
    const resolver = createQueryModeRequestResolver();
    const logger = makeLogger();
    const ctx = makeContext();
    const request = makeRequest({
      query: {
        lookbackDays: 31,
        topicGlobs: ["*"],
        maxEventsPerTopic: 4,
        evidenceStrategy: "diversity",
      },
    });

    await expect(resolver.resolve(ctx, request, logger)).rejects.toMatchObject({
      name: "NonRetryableProcessingError",
      code: "invalid_request",
    });
    expect(ctx.prisma.briefTrendSnapshot.findMany).not.toHaveBeenCalled();
  });

  it("marks postgres unhealthy when trend snapshot query fails", async () => {
    const resolver = createQueryModeRequestResolver();
    const logger = makeLogger();
    const dbFailure = new Error("postgres unavailable");
    const ctx = makeContext({
      prisma: {
        briefTrendSnapshot: {
          findMany: vi.fn().mockRejectedValue(dbFailure),
        },
        rawEvent: {
          findMany: vi.fn().mockResolvedValue([]),
        },
      } as any,
    });

    await expect(resolver.resolve(ctx, makeRequest(), logger)).rejects.toBe(dbFailure);
    expect(ctx.healthContext.postgresHealthy).toBe(false);
  });
});
