import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { EachBatchPayload, KafkaMessage, Batch } from "kafkajs";
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

import { processBatch, processCollectorHeartbeatBatch } from "../src/process.js";
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

function makeMessage(offset: string, value: unknown | null): KafkaMessage {
  return {
    offset,
    key: null,
    value: value === null ? null : Buffer.from(JSON.stringify(value), "utf-8"),
    timestamp: Date.now().toString(),
    attributes: 0,
    headers: {},
    size: 0,
  } as KafkaMessage;
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

function makePayload(
  messages: KafkaMessage[],
  overrides: Partial<EachBatchPayload> = {}
): EachBatchPayload {
  return {
    batch: {
      topic: "events.raw",
      partition: 0,
      highWatermark: "100",
      messages,
    } as Batch,
    isRunning: () => true,
    isStale: () => false,
    resolveOffset: vi.fn(),
    commitOffsetsIfNecessary: vi.fn().mockResolvedValue(undefined),
    heartbeat: vi.fn().mockResolvedValue(undefined),
    uncommittedOffsets: vi.fn(),
    ...overrides,
  } as unknown as EachBatchPayload;
}

function makeContext(overrides: Partial<TrendsContext> = {}): TrendsContext {
  return {
    config: {
      KAFKA_CONSUMER_GROUP: "trends-processor",
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

describe("trends processBatch", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.applyEventToWindows.mockResolvedValue({
      duplicate: false,
      buckets: { "15m": "2026-02-06T10:00:00.000Z" },
    });
  });

  it("processes valid messages and resolves offsets", async () => {
    const msg = makeMessage("1", makeValidPayload());
    const payload = makePayload([msg]);
    const ctx = makeContext();

    await processBatch(ctx, payload);

    expect(payload.resolveOffset).toHaveBeenCalledWith("1");
    expect(payload.commitOffsetsIfNecessary).toHaveBeenCalledOnce();
    expect(payload.heartbeat).toHaveBeenCalledOnce();
    expect(ctx.healthContext.metrics.eventsProcessed).toBe(1);
  });

  it("skips messages with null value", async () => {
    const msg = makeMessage("1", null);
    const payload = makePayload([msg]);
    const ctx = makeContext();

    await processBatch(ctx, payload);

    expect(payload.resolveOffset).toHaveBeenCalledWith("1");
    expect(ctx.healthContext.metrics.errors.get("parse_error")).toBe(1);
    expect(mocks.applyEventToWindows).not.toHaveBeenCalled();
  });

  it("skips messages with invalid JSON and resolves offset", async () => {
    const msg: KafkaMessage = {
      offset: "1",
      key: null,
      value: Buffer.from("{bad-json", "utf-8"),
      timestamp: Date.now().toString(),
      attributes: 0,
      headers: {},
      size: 0,
    } as KafkaMessage;
    const payload = makePayload([msg]);
    const ctx = makeContext();

    await processBatch(ctx, payload);

    expect(payload.resolveOffset).toHaveBeenCalledWith("1");
    expect(ctx.healthContext.metrics.errors.get("parse_error")).toBe(1);
  });

  it("skips events with no tracked topics", async () => {
    const payload_data = makeValidPayload();
    payload_data.tags = ["unknown.topic"];
    const msg = makeMessage("1", payload_data);
    const payload = makePayload([msg]);
    const ctx = makeContext();

    await processBatch(ctx, payload);

    expect(payload.resolveOffset).toHaveBeenCalledWith("1");
    expect(mocks.applyEventToWindows).not.toHaveBeenCalled();
  });

  it("increments duplicatesSkipped when dedup detects duplicate", async () => {
    mocks.applyEventToWindows.mockResolvedValue({ duplicate: true, buckets: {} });
    const msg = makeMessage("1", makeValidPayload());
    const payload = makePayload([msg]);
    const ctx = makeContext();

    await processBatch(ctx, payload);

    expect(ctx.healthContext.metrics.duplicatesSkipped).toBe(1);
    expect(ctx.healthContext.metrics.eventsProcessed).toBe(0);
  });

  it("throws on Redis error and marks redis unhealthy", async () => {
    mocks.applyEventToWindows.mockRejectedValue(new Error("REDIS_DOWN"));
    const msg = makeMessage("1", makeValidPayload());
    const payload = makePayload([msg]);
    const ctx = makeContext();

    await expect(processBatch(ctx, payload)).rejects.toThrow("REDIS_DOWN");
    expect(ctx.healthContext.redisHealthy).toBe(false);
    expect(ctx.healthContext.metrics.errors.get("redis_error")).toBe(1);
  });

  it("returns early when consumer is not running", async () => {
    const msg = makeMessage("1", makeValidPayload());
    const payload = makePayload([msg], { isRunning: () => false });
    const ctx = makeContext();

    await processBatch(ctx, payload);

    expect(payload.resolveOffset).not.toHaveBeenCalled();
    expect(mocks.applyEventToWindows).not.toHaveBeenCalled();
  });

  it("returns early when batch is stale", async () => {
    const msg = makeMessage("1", makeValidPayload());
    const payload = makePayload([msg], { isStale: () => true });
    const ctx = makeContext();

    await processBatch(ctx, payload);

    expect(payload.resolveOffset).not.toHaveBeenCalled();
  });

  it("processes multiple messages in order", async () => {
    const resolveOrder: string[] = [];
    const msg1 = makeMessage("1", makeValidPayload("rss:1"));
    const msg2 = makeMessage("2", makeValidPayload("rss:2"));
    const resolveOffset = vi.fn((offset: string) => resolveOrder.push(offset));
    const payload = makePayload([msg1, msg2], { resolveOffset });
    const ctx = makeContext();

    await processBatch(ctx, payload);

    expect(resolveOrder).toEqual(["1", "2"]);
    expect(ctx.healthContext.metrics.eventsProcessed).toBe(2);
  });

  it("updates consumer lag when interval has elapsed", async () => {
    const msg = makeMessage("50", makeValidPayload());
    const payload = makePayload([msg]);
    const ctx = makeContext();

    await processBatch(ctx, payload);

    const upsert = ctx.prisma.consumerLag.upsert as ReturnType<typeof vi.fn>;
    expect(upsert).toHaveBeenCalledOnce();
    expect(ctx.healthContext.postgresHealthy).toBe(true);
  });

  it("throttles consumer lag writes within interval", async () => {
    const msg1 = makeMessage("50", makeValidPayload("rss:1"));
    const payload1 = makePayload([msg1]);
    const ctx = makeContext();

    await processBatch(ctx, payload1);

    // Second batch within interval
    const msg2 = makeMessage("51", makeValidPayload("rss:2"));
    const payload2 = makePayload([msg2]);
    await processBatch(ctx, payload2);

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
    const msg = makeMessage("50", makeValidPayload());
    const payload = makePayload([msg]);

    // Should not throw - postgres failure is non-fatal
    await processBatch(ctx, payload);

    expect(ctx.healthContext.postgresHealthy).toBe(false);
    expect(ctx.healthContext.metrics.errors.get("postgres_error")).toBe(1);
  });

  it("stores collector heartbeat state from heartbeat batches", async () => {
    const heartbeatMessage = makeMessage("1", {
      source: 1,
      timestamp: "2026-02-06T10:00:00.000Z",
      last_fetch_at: "2026-02-06T09:59:30.000Z",
      items_fetched: 12,
      status: 1,
      error_message: "",
    });
    const payload = makePayload([heartbeatMessage], {
      batch: {
        topic: "collector.heartbeat",
        partition: 0,
        highWatermark: "2",
        messages: [heartbeatMessage],
      },
    });
    const ctx = makeContext();

    await processCollectorHeartbeatBatch(ctx, payload);

    expect(payload.resolveOffset).toHaveBeenCalledWith("1");
    expect(payload.commitOffsetsIfNecessary).toHaveBeenCalledOnce();
    expect(ctx.healthContext.collectorHeartbeats.get("rss")).toMatchObject({
      source: "rss",
      status: "healthy",
      itemsFetched: 12,
    });
  });

  it("accepts numeric-string heartbeat status and items_fetched values", async () => {
    const heartbeatMessage = makeMessage("1", {
      source: 1,
      timestamp: "2026-02-06T10:00:00.000Z",
      last_fetch_at: "2026-02-06T09:59:30.000Z",
      items_fetched: "7",
      status: "1",
      error_message: "",
    });
    const payload = makePayload([heartbeatMessage], {
      batch: {
        topic: "collector.heartbeat",
        partition: 0,
        highWatermark: "2",
        messages: [heartbeatMessage],
      },
    });
    const ctx = makeContext();

    await processCollectorHeartbeatBatch(ctx, payload);

    expect(ctx.healthContext.collectorHeartbeats.get("rss")).toMatchObject({
      source: "rss",
      status: "healthy",
      itemsFetched: 7,
    });
  });

  it("skips malformed collector heartbeat payloads and advances offsets", async () => {
    const msg: KafkaMessage = {
      offset: "1",
      key: null,
      value: Buffer.from("{bad-json", "utf-8"),
      timestamp: Date.now().toString(),
      attributes: 0,
      headers: {},
      size: 0,
    } as KafkaMessage;
    const payload = makePayload([msg], {
      batch: {
        topic: "collector.heartbeat",
        partition: 0,
        highWatermark: "2",
        messages: [msg],
      },
    });
    const ctx = makeContext();

    await processCollectorHeartbeatBatch(ctx, payload);

    expect(payload.resolveOffset).toHaveBeenCalledWith("1");
    expect(payload.commitOffsetsIfNecessary).toHaveBeenCalledOnce();
    expect(ctx.healthContext.metrics.errors.get("parse_error")).toBe(1);
  });
});
