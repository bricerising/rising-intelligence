import { beforeEach, describe, expect, it, vi } from "vitest";
import type { BatchContext, BatchStrategy, PipelineMessage } from "@rising-intelligence/pipeline/transport";
import type pino from "pino";
import type { TrendsContext } from "../src/process.js";

const mocks = vi.hoisted(() => ({
  applyEventToWindows: vi.fn(),
}));

vi.mock("../src/redis.js", () => ({
  applyEventToWindows: mocks.applyEventToWindows,
}));

vi.mock("../src/config.js", () => ({
  getConfig: () => ({
    SERVICE_NAME: "trends",
    PORT: 3000,
    LOG_LEVEL: "info",
    KAFKA_CONSUMER_GROUP: "trends-processor",
    WINDOWS: ["15m", "60m"] as const,
    MAX_EVIDENCE_PER_TOPIC: 10,
    CONSUMER_LAG_UPDATE_INTERVAL_MS: 15000,
  }),
}));

import { createBatchStrategies } from "../src/process.js";
import { createHealthContext, type HealthContext } from "../src/health.js";

function makeLogger(): pino.Logger {
  return {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: vi.fn().mockReturnThis(),
  } as unknown as pino.Logger;
}

function makePipelineMessage(position: string, value: unknown | null): PipelineMessage {
  return {
    key: null,
    value: value === null ? null : Buffer.from(JSON.stringify(value), "utf-8"),
    position,
    timestamp: Date.now().toString(),
  };
}

function makeValidPayload(eventId = "rss:1") {
  return {
    event_id: eventId,
    source: 1,
    fetched_at: "2026-02-06T10:00:00.000Z",
    text: "hello world",
    tags: ["aws.bedrock"],
    engagement: { score: 10 },
  };
}

function makeAllowlist() {
  return {
    topics: [
      { key: "aws.bedrock", displayName: "Bedrock", priority: 90 },
    ],
    topicMap: new Map([["aws.bedrock", { key: "aws.bedrock", displayName: "Bedrock", priority: 90 }]]),
    mutedTopics: new Set<string>(),
    maxTopicsPerEvent: 5,
  };
}

function makeBatchContext(overrides: Partial<BatchContext> = {}): BatchContext {
  return {
    topic: "events.raw",
    partition: 0,
    highWatermark: "100",
    isActive: vi.fn().mockReturnValue(true),
    keepAlive: vi.fn().mockResolvedValue(undefined),
    acknowledge: vi.fn(),
    commit: vi.fn().mockResolvedValue(undefined),
    pause: vi.fn().mockReturnValue(vi.fn()),
    ...overrides,
  };
}

function makeContext(overrides: Partial<TrendsContext> = {}): TrendsContext {
  return {
    config: {
      KAFKA_CONSUMER_GROUP: "trends-processor",
      KAFKA_TOPIC_RAW_EVENTS: "events.raw",
      KAFKA_TOPIC_COLLECTOR_HEARTBEAT: "collector.heartbeat",
      WINDOWS: ["15m", "60m"],
      MAX_EVIDENCE_PER_TOPIC: 10,
      CONSUMER_LAG_UPDATE_INTERVAL_MS: 15000,
    } as TrendsContext["config"],
    logger: makeLogger(),
    healthContext: createHealthContext(),
    prisma: {
      consumerLag: { upsert: vi.fn().mockResolvedValue(undefined) },
    } as unknown as TrendsContext["prisma"],
    redis: {} as TrendsContext["redis"],
    allowlist: makeAllowlist(),
    lagWriteTimestamps: new Map(),
    ...overrides,
  };
}

function getStrategy(
  ctx: TrendsContext,
  topic: string
): BatchStrategy<TrendsContext> {
  const strategies = createBatchStrategies(ctx);
  const strategy = strategies.get(topic);
  if (!strategy) {
    throw new Error(`No strategy for topic: ${topic}`);
  }
  return strategy;
}

describe("trends processBatch", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.applyEventToWindows.mockResolvedValue({
      duplicate: false,
      buckets: { "15m": "2026-02-06T10:00:00.000Z" },
    });
  });

  it("processes valid messages and resolves offsets", async () => {
    const msg = makePipelineMessage("1", makeValidPayload());
    const batch = makeBatchContext();
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, [msg]);

    expect(batch.acknowledge).toHaveBeenCalledWith("1");
    expect(batch.commit).toHaveBeenCalledOnce();
    expect(batch.keepAlive).toHaveBeenCalled();
    expect(ctx.healthContext.metrics.eventsProcessed).toBe(1);
  });

  it("skips messages with null value", async () => {
    const msg = makePipelineMessage("1", null);
    const batch = makeBatchContext();
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, [msg]);

    expect(batch.acknowledge).toHaveBeenCalledWith("1");
    expect(ctx.healthContext.metrics.errors.get("parse_error")).toBe(1);
    expect(mocks.applyEventToWindows).not.toHaveBeenCalled();
  });

  it("skips messages with invalid JSON and resolves offset", async () => {
    const msg: PipelineMessage = {
      key: null,
      value: Buffer.from("{bad-json", "utf-8"),
      position: "1",
      timestamp: Date.now().toString(),
    };
    const batch = makeBatchContext();
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, [msg]);

    expect(batch.acknowledge).toHaveBeenCalledWith("1");
    expect(ctx.healthContext.metrics.errors.get("parse_error")).toBe(1);
  });

  it("skips events with no tracked topics", async () => {
    const payload = makeValidPayload();
    payload.tags = ["unknown.topic"];
    const msg = makePipelineMessage("1", payload);
    const batch = makeBatchContext();
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, [msg]);

    expect(batch.acknowledge).toHaveBeenCalledWith("1");
    expect(mocks.applyEventToWindows).not.toHaveBeenCalled();
  });

  it("increments duplicatesSkipped when dedup detects duplicate", async () => {
    mocks.applyEventToWindows.mockResolvedValue({ duplicate: true, buckets: {} });
    const msg = makePipelineMessage("1", makeValidPayload());
    const batch = makeBatchContext();
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, [msg]);

    expect(ctx.healthContext.metrics.duplicatesSkipped).toBe(1);
    expect(ctx.healthContext.metrics.eventsProcessed).toBe(0);
  });

  it("throws on Redis error and marks redis unhealthy", async () => {
    mocks.applyEventToWindows.mockRejectedValue(new Error("REDIS_DOWN"));
    const msg = makePipelineMessage("1", makeValidPayload());
    const batch = makeBatchContext();
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await expect(strategy.processBatch(ctx, batch, [msg])).rejects.toThrow("REDIS_DOWN");
    expect(ctx.healthContext.redisHealthy).toBe(false);
    expect(ctx.healthContext.metrics.errors.get("redis_error")).toBe(1);
  });

  it("returns early when consumer is not running", async () => {
    const msg = makePipelineMessage("1", makeValidPayload());
    const batch = makeBatchContext({ isActive: vi.fn().mockReturnValue(false) });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, [msg]);

    expect(batch.acknowledge).not.toHaveBeenCalled();
    expect(mocks.applyEventToWindows).not.toHaveBeenCalled();
  });

  it("returns early when batch is stale", async () => {
    const msg = makePipelineMessage("1", makeValidPayload());
    const batch = makeBatchContext({ isActive: vi.fn().mockReturnValue(false) });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, [msg]);

    expect(batch.acknowledge).not.toHaveBeenCalled();
  });

  it("stops processing when batch becomes stale mid-loop", async () => {
    const msg1 = makePipelineMessage("1", makeValidPayload("rss:1"));
    const msg2 = makePipelineMessage("2", makeValidPayload("rss:2"));
    let callCount = 0;
    const batch = makeBatchContext({
      isActive: vi.fn(() => {
        callCount++;
        // Active for first few checks, then becomes stale after first message processed
        return callCount <= 2;
      }),
    });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    mocks.applyEventToWindows
      .mockResolvedValueOnce({ duplicate: false, buckets: {} })
      .mockResolvedValue({ duplicate: false, buckets: {} });

    await strategy.processBatch(ctx, batch, [msg1, msg2]);

    expect(mocks.applyEventToWindows).toHaveBeenCalledTimes(1);
    // Lag update should not run when batch becomes inactive
  });

  it("processes multiple messages in order", async () => {
    const acknowledgeOrder: string[] = [];
    const msg1 = makePipelineMessage("1", makeValidPayload("rss:1"));
    const msg2 = makePipelineMessage("2", makeValidPayload("rss:2"));
    const batch = makeBatchContext({
      acknowledge: vi.fn((position: string) => acknowledgeOrder.push(position)),
    });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, [msg1, msg2]);

    expect(acknowledgeOrder).toEqual(["1", "2"]);
    expect(ctx.healthContext.metrics.eventsProcessed).toBe(2);
  });

  it("heartbeats once per interval and once on flush", async () => {
    const messages = Array.from({ length: 50 }, (_, index) =>
      makePipelineMessage(`${index + 1}`, makeValidPayload(`rss:${index + 1}`))
    );
    const batch = makeBatchContext();
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, messages);

    expect(batch.keepAlive).toHaveBeenCalledTimes(2);
    expect(batch.commit).toHaveBeenCalledOnce();
    expect(batch.acknowledge).toHaveBeenCalledTimes(50);
  });

  it("updates consumer lag when interval has elapsed", async () => {
    const msg = makePipelineMessage("50", makeValidPayload());
    const batch = makeBatchContext();
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, [msg]);

    const upsert = ctx.prisma.consumerLag.upsert as ReturnType<typeof vi.fn>;
    expect(upsert).toHaveBeenCalledOnce();
    expect(ctx.healthContext.postgresHealthy).toBe(true);
  });

  it("throttles consumer lag writes within interval", async () => {
    const msg1 = makePipelineMessage("50", makeValidPayload("rss:1"));
    const msg2 = makePipelineMessage("51", makeValidPayload("rss:2"));
    const batch = makeBatchContext();
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "events.raw");

    await strategy.processBatch(ctx, batch, [msg1]);

    // Second batch within interval
    const batch2 = makeBatchContext();
    await strategy.processBatch(ctx, batch2, [msg2]);

    const upsert = ctx.prisma.consumerLag.upsert as ReturnType<typeof vi.fn>;
    expect(upsert).toHaveBeenCalledOnce();
  });

  it("handles postgres lag upsert failure gracefully", async () => {
    const upsert = vi.fn().mockRejectedValue(new Error("PG_DOWN"));
    const ctx = makeContext({
      prisma: {
        consumerLag: { upsert },
      } as unknown as TrendsContext["prisma"],
    });
    const msg = makePipelineMessage("50", makeValidPayload());
    const batch = makeBatchContext();
    const strategy = getStrategy(ctx, "events.raw");

    // Should not throw - postgres failure is non-fatal
    await strategy.processBatch(ctx, batch, [msg]);

    expect(ctx.healthContext.postgresHealthy).toBe(false);
    expect(ctx.healthContext.metrics.errors.get("postgres_error")).toBe(1);
  });

  it("stores collector heartbeat state from heartbeat batches", async () => {
    const heartbeatMessage = makePipelineMessage("1", {
      source: 1,
      timestamp: "2026-02-06T10:00:00.000Z",
      last_fetch_at: "2026-02-06T09:59:30.000Z",
      items_fetched: 12,
      status: 1,
      error_message: "",
    });
    const batch = makeBatchContext({ topic: "collector.heartbeat" });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "collector.heartbeat");

    await strategy.processBatch(ctx, batch, [heartbeatMessage]);

    expect(batch.acknowledge).toHaveBeenCalledWith("1");
    expect(batch.commit).toHaveBeenCalledOnce();
    expect(ctx.healthContext.collectorHeartbeats.get("rss")).toMatchObject({
      source: "rss",
      status: "healthy",
      itemsFetched: 12,
    });
  });

  it("accepts numeric-string heartbeat status and items_fetched values", async () => {
    const heartbeatMessage = makePipelineMessage("1", {
      source: 1,
      timestamp: "2026-02-06T10:00:00.000Z",
      last_fetch_at: "2026-02-06T09:59:30.000Z",
      items_fetched: "7",
      status: "1",
      error_message: "",
    });
    const batch = makeBatchContext({ topic: "collector.heartbeat" });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "collector.heartbeat");

    await strategy.processBatch(ctx, batch, [heartbeatMessage]);

    expect(ctx.healthContext.collectorHeartbeats.get("rss")).toMatchObject({
      source: "rss",
      status: "healthy",
      itemsFetched: 7,
    });
  });

  it("accepts named collector status aliases", async () => {
    const heartbeatMessage = makePipelineMessage("1", {
      source: 1,
      timestamp: "2026-02-06T10:00:00.000Z",
      last_fetch_at: "2026-02-06T09:59:30.000Z",
      items_fetched: 7,
      status: "collector_status_degraded",
      error_message: "rate limited",
    });
    const batch = makeBatchContext({ topic: "collector.heartbeat" });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "collector.heartbeat");

    await strategy.processBatch(ctx, batch, [heartbeatMessage]);

    expect(ctx.healthContext.collectorHeartbeats.get("rss")).toMatchObject({
      source: "rss",
      status: "degraded",
      itemsFetched: 7,
      errorMessage: "rate limited",
    });
  });

  it("skips malformed collector heartbeat payloads and advances offsets", async () => {
    const msg: PipelineMessage = {
      key: null,
      value: Buffer.from("{bad-json", "utf-8"),
      position: "1",
      timestamp: Date.now().toString(),
    };
    const batch = makeBatchContext({ topic: "collector.heartbeat" });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "collector.heartbeat");

    await strategy.processBatch(ctx, batch, [msg]);

    expect(batch.acknowledge).toHaveBeenCalledWith("1");
    expect(batch.commit).toHaveBeenCalledOnce();
    expect(ctx.healthContext.metrics.errors.get("parse_error")).toBe(1);
  });

  it("skips collector heartbeat payloads with invalid source types", async () => {
    const heartbeatMessage = makePipelineMessage("1", {
      source: { value: 1 },
      timestamp: "2026-02-06T10:00:00.000Z",
      last_fetch_at: "2026-02-06T09:59:30.000Z",
      items_fetched: 12,
      status: 1,
      error_message: "",
    });
    const batch = makeBatchContext({ topic: "collector.heartbeat" });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "collector.heartbeat");

    await strategy.processBatch(ctx, batch, [heartbeatMessage]);

    expect(batch.acknowledge).toHaveBeenCalledWith("1");
    expect(batch.commit).toHaveBeenCalledOnce();
    expect(ctx.healthContext.collectorHeartbeats.size).toBe(0);
    expect(ctx.healthContext.metrics.errors.get("parse_error")).toBe(1);
  });

  it("skips collector heartbeat payloads with unsupported status values", async () => {
    const heartbeatMessage = makePipelineMessage("1", {
      source: 1,
      timestamp: "2026-02-06T10:00:00.000Z",
      last_fetch_at: "2026-02-06T09:59:30.000Z",
      items_fetched: 12,
      status: "unknown",
      error_message: "",
    });
    const batch = makeBatchContext({ topic: "collector.heartbeat" });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "collector.heartbeat");

    await strategy.processBatch(ctx, batch, [heartbeatMessage]);

    expect(batch.acknowledge).toHaveBeenCalledWith("1");
    expect(batch.commit).toHaveBeenCalledOnce();
    expect(ctx.healthContext.collectorHeartbeats.size).toBe(0);
    expect(ctx.healthContext.metrics.errors.get("parse_error")).toBe(1);
  });

  it("keeps the newest heartbeat when messages arrive out of order", async () => {
    const newerHeartbeatMessage = makePipelineMessage("1", {
      source: 1,
      timestamp: "2026-02-06T10:00:00.000Z",
      last_fetch_at: "2026-02-06T09:59:30.000Z",
      items_fetched: 12,
      status: 1,
      error_message: "",
    });
    const olderHeartbeatMessage = makePipelineMessage("2", {
      source: 1,
      timestamp: "2026-02-06T09:58:00.000Z",
      last_fetch_at: "2026-02-06T09:57:30.000Z",
      items_fetched: 2,
      status: 3,
      error_message: "timed out",
    });
    const batch = makeBatchContext({ topic: "collector.heartbeat" });
    const ctx = makeContext();
    const strategy = getStrategy(ctx, "collector.heartbeat");

    await strategy.processBatch(ctx, batch, [newerHeartbeatMessage, olderHeartbeatMessage]);

    expect(batch.acknowledge).toHaveBeenCalledWith("1");
    expect(batch.acknowledge).toHaveBeenCalledWith("2");
    expect(batch.commit).toHaveBeenCalledOnce();
    expect(ctx.healthContext.collectorHeartbeats.get("rss")).toMatchObject({
      source: "rss",
      status: "healthy",
      itemsFetched: 12,
      errorMessage: undefined,
    });
  });
});
