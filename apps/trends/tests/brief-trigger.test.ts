import { describe, expect, it, vi } from "vitest";
import { createHealthContext } from "../src/health.js";
import { maybeTriggerDailySummaryRequest } from "../src/brief-trigger.js";

function makeConfig() {
  return {
    DAILY_BRIEF_ENABLED: true,
    DAILY_BRIEF_UTC_HOUR: 1,
    DAILY_BRIEF_UTC_MINUTE: 0,
    KAFKA_CONSUMER_GROUP: "trends-processor",
    PERSISTER_CONSUMER_GROUP: "persister",
    KAFKA_TOPIC_RAW_EVENTS: "events.raw",
    KAFKA_TOPIC_SUMMARY_REQUESTS: "summary.requests",
    MAX_LAG_AGE_MS: 5 * 60 * 1000,
    MAX_LAG_MESSAGES: 100,
    BRIEF_MAX_TOPICS: 10,
    BRIEF_MAX_EVIDENCE_PER_TOPIC: 5,
    BRIEF_DAILY_BUDGET_USD: 5,
    BRIEF_MAX_OUTPUT_TOKENS: 2000,
  } as any;
}

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

describe("maybeTriggerDailySummaryRequest", () => {
  it("publishes a daily summary request when schedule and freshness checks pass", async () => {
    const producer = {
      send: vi.fn().mockResolvedValue(undefined),
    };
    const prisma = {
      consumerLag: {
        findMany: vi.fn().mockResolvedValue([
          {
            consumerGroup: "trends-processor",
            lagMessages: 5n,
            updatedAt: new Date("2026-02-06T01:04:00.000Z"),
          },
          {
            consumerGroup: "persister",
            lagMessages: 7n,
            updatedAt: new Date("2026-02-06T01:04:00.000Z"),
          },
        ]),
      },
      rawEvent: {
        findMany: vi.fn().mockResolvedValue([
          {
            eventId: "evt-1",
            source: "rss",
            url: "https://example.com/1",
            title: "Title",
            publishedAt: new Date("2026-02-06T00:40:00.000Z"),
            fetchedAt: new Date("2026-02-06T00:45:00.000Z"),
            text: "Evidence text",
            engagementScore: 10,
            engagementComments: 5,
            engagementLikes: 4,
            engagementShares: 1,
          },
        ]),
      },
    };

    const healthContext = createHealthContext();
    const nextDateKey = await maybeTriggerDailySummaryRequest({
      config: makeConfig(),
      logger: makeLogger(),
      prisma: prisma as any,
      producer: producer as any,
      healthContext,
      snapshots: [
        {
          window: "60m",
          generatedAt: new Date("2026-02-06T01:00:00.000Z"),
          topMetrics: [
            {
              topic: "aws.bedrock",
              window: "60m",
              volume: 25,
              prevVolume: 10,
              acceleration: 1.5,
              baselineVolume: 12,
              baselineDelta: 1.08,
              score: 62.5,
              evidenceEventIds: ["evt-1"],
            },
          ],
        },
      ],
      lastDailyTriggerDate: null,
      now: new Date("2026-02-06T01:05:00.000Z"),
    });

    expect(nextDateKey).toBe("2026-02-06");
    expect(producer.send).toHaveBeenCalledOnce();
    const sendPayload = producer.send.mock.calls[0][0];
    expect(sendPayload.topic).toBe("summary.requests");

    const wire = JSON.parse(sendPayload.messages[0].value.toString("utf-8"));
    expect(wire.request_id).toBe("daily:2026-02-06");
    expect(wire.type).toBe(1);
    expect(wire.topics[0].topic).toBe("aws.bedrock");
    expect(healthContext.metrics.briefTriggered.get("daily")).toBe(1);
  });

  it("skips trigger and increments stale metric when lag records are stale", async () => {
    const producer = {
      send: vi.fn().mockResolvedValue(undefined),
    };
    const prisma = {
      consumerLag: {
        findMany: vi.fn().mockResolvedValue([
          {
            consumerGroup: "trends-processor",
            lagMessages: 5n,
            updatedAt: new Date("2026-02-06T00:00:00.000Z"),
          },
          {
            consumerGroup: "persister",
            lagMessages: 7n,
            updatedAt: new Date("2026-02-06T00:00:00.000Z"),
          },
        ]),
      },
      rawEvent: {
        findMany: vi.fn(),
      },
    };

    const healthContext = createHealthContext();
    const nextDateKey = await maybeTriggerDailySummaryRequest({
      config: makeConfig(),
      logger: makeLogger(),
      prisma: prisma as any,
      producer: producer as any,
      healthContext,
      snapshots: [
        {
          window: "60m",
          generatedAt: new Date("2026-02-06T01:00:00.000Z"),
          topMetrics: [
            {
              topic: "aws.bedrock",
              window: "60m",
              volume: 25,
              prevVolume: 10,
              acceleration: 1.5,
              baselineVolume: 12,
              baselineDelta: 1.08,
              score: 62.5,
              evidenceEventIds: ["evt-1"],
            },
          ],
        },
      ],
      lastDailyTriggerDate: null,
      now: new Date("2026-02-06T01:05:00.000Z"),
    });

    expect(nextDateKey).toBeNull();
    expect(producer.send).not.toHaveBeenCalled();
    expect(healthContext.metrics.briefSkippedStaleData).toBe(1);
  });
});
