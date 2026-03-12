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

    const evidenceByKey = new Map<string, string[]>([
      ["evidence:15m:aws.bedrock", ["event-2", "event-1"]],
      ["evidence:15m:ai.openai", ["event-3"]],
    ]);

    // Track pipeline commands in order, then return matching results on exec()
    const snapshotPipelineCommands: Array<{ cmd: string; args: unknown[] }> = [];
    const snapshotPipeline = {
      get: vi.fn((key: string) => {
        snapshotPipelineCommands.push({ cmd: "get", args: [key] });
        return snapshotPipeline;
      }),
      zrevrange: vi.fn((key: string, start: number, stop: number) => {
        snapshotPipelineCommands.push({ cmd: "zrevrange", args: [key, start, stop] });
        return snapshotPipeline;
      }),
      exec: vi.fn(async () =>
        snapshotPipelineCommands.map(({ cmd, args }) => {
          if (cmd === "get") return [null, rawValues.get(args[0] as string) ?? null];
          if (cmd === "zrevrange") return [null, evidenceByKey.get(args[0] as string) ?? []];
          return [null, null];
        })
      ),
    };

    // writePreviousWindowCounts pipeline
    const writePipeline = {
      set: vi.fn().mockReturnThis(),
      expire: vi.fn().mockReturnThis(),
      exec: vi.fn().mockResolvedValue([]),
    };

    let pipelineCallCount = 0;
    const redis = {
      pipeline: vi.fn(() => {
        pipelineCallCount++;
        // First pipeline call is computeWindowMetrics, second is writePreviousWindowCounts
        return pipelineCallCount === 1 ? snapshotPipeline : writePipeline;
      }),
    };

    const publishedMessages: Array<{ topic: string; key: string; value: Buffer }> = [];
    const producer = {
      publish: vi.fn(async (topic: string, key: string, value: Buffer) => {
        publishedMessages.push({ topic, key, value });
      }),
      publishBatch: vi.fn(async () => false),
      disconnect: vi.fn(async () => undefined),
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

    expect(producer.publish).toHaveBeenCalledOnce();
    const [publishedTopic, , publishedValue] = (producer.publish as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(publishedTopic).toBe("trends.snapshots");

    const wirePayload = JSON.parse((publishedValue as Buffer).toString("utf-8"));
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

    expect(writePipeline.set).toHaveBeenCalledWith("prev:15m:aws.bedrock", "8");
    expect(writePipeline.expire).toHaveBeenCalledWith("prev:15m:aws.bedrock", 1800);
    expect(writePipeline.exec).toHaveBeenCalledOnce();
  });
});
