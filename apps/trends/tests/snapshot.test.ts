import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createHealthContext } from "../src/health.js";
import { publishSnapshots } from "../src/snapshot.js";

describe("trends snapshot publishing", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-02-06T10:20:00.000Z"));
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("publishes ranked snapshots, persists output, and updates health metrics", async () => {
    const bucket = "2026-02-06T10:15:00.000Z";
    const rawValues = new Map<string, string | null>([
      [`window:15m:aws.bedrock:${bucket}`, "8"],
      ["prev:15m:aws.bedrock", "2"],
      [`window:15m:ai.openai:${bucket}`, "10"],
      ["prev:15m:ai.openai", "10"],
    ]);

    const pipeline = {
      set: vi.fn().mockReturnThis(),
      expire: vi.fn().mockReturnThis(),
      exec: vi.fn().mockResolvedValue([]),
    };

    const redis = {
      get: vi.fn(async (key: string) => rawValues.get(key) ?? null),
      zrevrange: vi.fn(async (key: string) =>
        key.includes("aws.bedrock") ? ["event-2", "event-1"] : ["event-3"]
      ),
      pipeline: vi.fn(() => pipeline),
    };

    const producer = {
      send: vi.fn().mockResolvedValue(undefined),
    };

    const prisma = {
      trendSnapshot: {
        create: vi.fn().mockResolvedValue(undefined),
      },
    };

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
    };

    const healthContext = createHealthContext();

    await publishSnapshots({
      config: {
        WINDOWS: ["15m"],
        TOP_N_TOPICS: 1,
        MAX_EVIDENCE_PER_TOPIC: 5,
        KAFKA_TOPIC_TRENDS_SNAPSHOTS: "trends.snapshots",
      } as any,
      logger: logger as any,
      redis: redis as any,
      producer: producer as any,
      prisma: prisma as any,
      allowlist: {
        topics: [
          { key: "aws.bedrock", displayName: "Bedrock", priority: 90 },
          { key: "ai.openai", displayName: "OpenAI", priority: 80 },
        ],
        topicMap: new Map(),
        mutedTopics: new Set<string>(),
        maxTopicsPerEvent: 5,
      },
      healthContext,
    });

    expect(producer.send).toHaveBeenCalledOnce();
    const produceRequest = (producer.send as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(produceRequest.topic).toBe("trends.snapshots");

    const wirePayload = JSON.parse(produceRequest.messages[0].value.toString("utf-8"));
    expect(wirePayload.window).toBe(1);
    expect(wirePayload.topics).toHaveLength(1);
    expect(wirePayload.topics[0].topic).toBe("aws.bedrock");
    expect(wirePayload.topics[0].score).toBe(32);
    expect(wirePayload.topics[0].evidence.top_event_ids).toEqual(["event-2", "event-1"]);

    expect(prisma.trendSnapshot.create).toHaveBeenCalledOnce();
    expect(healthContext.metrics.snapshotPublished.get("15m")).toBe(1);
    expect(healthContext.metrics.topicMetrics.getVolume("aws.bedrock", "15m")).toBe(8);
    expect(healthContext.metrics.topicMetrics.getScore("aws.bedrock", "15m")).toBe(32);
    expect(healthContext.metrics.baselineComputeDurationSeconds.count).toBe(1);

    expect(pipeline.set).toHaveBeenCalledWith("prev:15m:aws.bedrock", "8");
    expect(pipeline.expire).toHaveBeenCalledWith("prev:15m:aws.bedrock", 1800);
    expect(pipeline.exec).toHaveBeenCalledOnce();
  });
});
