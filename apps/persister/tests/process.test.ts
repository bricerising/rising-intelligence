import { describe, expect, it, vi, beforeEach } from "vitest";
import { Source } from "@rising-intelligence/db";
import { createHealthContext } from "../src/health.js";
import { PostgresCircuitBreaker } from "../src/circuit-breaker.js";
import type { PersisterContext } from "../src/process.js";
import type { ParsedRawEvent } from "../src/types.js";

const persistMocks = vi.hoisted(() => ({
  persistBatch: vi.fn(),
  upsertConsumerLag: vi.fn(),
}));

const redisMocks = vi.hoisted(() => ({
  markEventsSeen: vi.fn(),
}));

const deserializeMocks = vi.hoisted(() => ({
  deserializeRawEvent: vi.fn(),
}));

vi.mock("../src/persist.js", () => ({
  persistBatch: persistMocks.persistBatch,
  upsertConsumerLag: persistMocks.upsertConsumerLag,
}));

vi.mock("../src/redis.js", () => ({
  markEventsSeen: redisMocks.markEventsSeen,
  createRedisClient: vi.fn(),
  disconnectRedis: vi.fn(),
}));

vi.mock("../src/deserialize.js", () => ({
  deserializeRawEvent: deserializeMocks.deserializeRawEvent,
}));

function createEvent(eventId: string, source: Source = Source.rss): ParsedRawEvent {
  return {
    eventId,
    source,
    fetchedAt: new Date("2026-02-06T10:00:00.000Z"),
    publishedAt: null,
    url: null,
    title: null,
    text: "text",
    authorId: null,
    authorHandle: null,
    authorDisplayName: null,
    engagementScore: null,
    engagementComments: null,
    engagementLikes: null,
    engagementShares: null,
    lang: null,
    tags: [],
    extractedHashtags: [],
    extractedUrls: [],
    sourceMeta: null,
  };
}

function createMockLogger() {
  return {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: vi.fn().mockReturnThis(),
  } as any;
}

function createMockContext(overrides: Partial<PersisterContext> = {}): PersisterContext {
  return {
    config: {
      SERVICE_NAME: "persister",
      PORT: 3000,
      LOG_LEVEL: "info" as const,
      KAFKA_BROKERS: "localhost:9092",
      KAFKA_CLIENT_ID: "persister",
      KAFKA_CONSUMER_GROUP: "persister",
      KAFKA_TOPIC_RAW_EVENTS: "events.raw",
      DATABASE_URL: "postgresql://localhost/test",
      POSTGRES_HOST: "localhost",
      POSTGRES_PORT: 5432,
      POSTGRES_DB: "test",
      POSTGRES_USER: "test",
      REDIS_URL: "redis://localhost:6379",
      SEEN_TTL_SECONDS: 86400,
      CONSUMER_LAG_UPDATE_INTERVAL_MS: 15000,
      POSTGRES_CIRCUIT_FAILURE_THRESHOLD: 3,
      POSTGRES_CIRCUIT_OPEN_MS: 30000,
      SHUTDOWN_TIMEOUT_MS: 30000,
    },
    logger: createMockLogger(),
    healthContext: createHealthContext(),
    healthServer: {} as any,
    prisma: {} as any,
    redis: { pipeline: vi.fn() } as any,
    kafkaContext: { kafka: {} as any, consumer: {} as any },
    circuitBreaker: new PostgresCircuitBreaker(3, 30000),
    lagWriteTimestamps: new Map(),
    ...overrides,
  };
}

function createMockPayload(
  messages: Array<{ offset: string; value: Buffer | null }> = [],
  overrides: Record<string, any> = {}
) {
  return {
    batch: {
      topic: "events.raw",
      partition: 0,
      highWatermark: "100",
      messages,
      ...overrides.batch,
    },
    isRunning: vi.fn().mockReturnValue(true),
    isStale: vi.fn().mockReturnValue(false),
    pause: vi.fn().mockReturnValue(vi.fn()),
    resolveOffset: vi.fn(),
    commitOffsetsIfNecessary: vi.fn().mockResolvedValue(undefined),
    heartbeat: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as any;
}

describe("processBatch", () => {
  let processBatch: typeof import("../src/process.js").processBatch;

  beforeEach(async () => {
    vi.clearAllMocks();
    const mod = await import("../src/process.js");
    processBatch = mod.processBatch;
  });

  it("skips batch when not running", async () => {
    const ctx = createMockContext();
    const payload = createMockPayload([], { isRunning: vi.fn().mockReturnValue(false) });

    await processBatch(ctx, payload);

    expect(persistMocks.persistBatch).not.toHaveBeenCalled();
    expect(payload.resolveOffset).not.toHaveBeenCalled();
  });

  it("skips batch when stale", async () => {
    const ctx = createMockContext();
    const payload = createMockPayload([], { isStale: vi.fn().mockReturnValue(true) });

    await processBatch(ctx, payload);

    expect(persistMocks.persistBatch).not.toHaveBeenCalled();
  });

  it("pauses partition when circuit breaker is open", async () => {
    const cb = new PostgresCircuitBreaker(1, 50);
    cb.recordFailure();
    const ctx = createMockContext({ circuitBreaker: cb });

    const resume = vi.fn();
    const payload = createMockPayload([], { pause: vi.fn().mockReturnValue(resume) });

    await processBatch(ctx, payload);

    expect(payload.pause).toHaveBeenCalledOnce();
    expect(resume).toHaveBeenCalledOnce();
    expect(payload.heartbeat).toHaveBeenCalled();
    expect(payload.commitOffsetsIfNecessary).not.toHaveBeenCalled();
    expect(ctx.healthContext.circuitOpen).toBe(true);
    expect(ctx.logger.warn).toHaveBeenCalledWith(
      expect.objectContaining({ kafkaTopic: "events.raw" }),
      "Postgres circuit open; pausing partition"
    );
  });

  it("resumes partition if heartbeat fails while circuit breaker is open", async () => {
    const cb = new PostgresCircuitBreaker(1, 10);
    cb.recordFailure();
    const ctx = createMockContext({ circuitBreaker: cb });

    const resume = vi.fn();
    const payload = createMockPayload([], {
      pause: vi.fn().mockReturnValue(resume),
      heartbeat: vi.fn().mockRejectedValue(new Error("heartbeat failed")),
    });

    await expect(processBatch(ctx, payload)).rejects.toThrow("heartbeat failed");
    expect(resume).toHaveBeenCalledOnce();
  });

  it("deserializes valid messages and persists them", async () => {
    const event = createEvent("rss:1");
    deserializeMocks.deserializeRawEvent.mockReturnValue(event);
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 1,
      inserted: 1,
      duplicates: 0,
      insertedBySource: new Map([[Source.rss, 1]]),
    });
    redisMocks.markEventsSeen.mockResolvedValue(undefined);

    const ctx = createMockContext();
    const payload = createMockPayload([
      { offset: "10", value: Buffer.from("{}") },
    ]);

    await processBatch(ctx, payload);

    expect(deserializeMocks.deserializeRawEvent).toHaveBeenCalledOnce();
    expect(persistMocks.persistBatch).toHaveBeenCalledWith(ctx.prisma, [event]);
    expect(payload.resolveOffset).toHaveBeenCalledWith("10");
    expect(payload.commitOffsetsIfNecessary).toHaveBeenCalledOnce();
    expect(payload.heartbeat).toHaveBeenCalledOnce();
  });

  it("skips messages with null value", async () => {
    const event = createEvent("rss:1");
    deserializeMocks.deserializeRawEvent.mockReturnValue(event);
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 1,
      inserted: 1,
      duplicates: 0,
      insertedBySource: new Map([[Source.rss, 1]]),
    });
    redisMocks.markEventsSeen.mockResolvedValue(undefined);

    const ctx = createMockContext();
    const payload = createMockPayload([
      { offset: "10", value: null },
      { offset: "11", value: Buffer.from("{}") },
    ]);

    await processBatch(ctx, payload);

    expect(deserializeMocks.deserializeRawEvent).toHaveBeenCalledTimes(1);
    expect(ctx.healthContext.metrics.eventsSkipped.get("malformed")).toBe(1);
    expect(ctx.healthContext.metrics.errors.get("parse_error")).toBe(1);
  });

  it("skips messages that fail deserialization", async () => {
    deserializeMocks.deserializeRawEvent
      .mockImplementationOnce(() => { throw new Error("bad json"); })
      .mockReturnValueOnce(createEvent("rss:2"));
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 1,
      inserted: 1,
      duplicates: 0,
      insertedBySource: new Map([[Source.rss, 1]]),
    });
    redisMocks.markEventsSeen.mockResolvedValue(undefined);

    const ctx = createMockContext();
    const payload = createMockPayload([
      { offset: "10", value: Buffer.from("bad") },
      { offset: "11", value: Buffer.from("good") },
    ]);

    await processBatch(ctx, payload);

    expect(ctx.healthContext.metrics.eventsSkipped.get("malformed")).toBe(1);
    expect(persistMocks.persistBatch).toHaveBeenCalledWith(ctx.prisma, [expect.objectContaining({ eventId: "rss:2" })]);
  });

  it("does not call persistBatch when all messages fail deserialization", async () => {
    deserializeMocks.deserializeRawEvent.mockImplementation(() => { throw new Error("bad"); });

    const ctx = createMockContext();
    const payload = createMockPayload([
      { offset: "10", value: Buffer.from("bad1") },
      { offset: "11", value: Buffer.from("bad2") },
    ]);

    await processBatch(ctx, payload);

    expect(persistMocks.persistBatch).not.toHaveBeenCalled();
    expect(ctx.healthContext.metrics.eventsSkipped.get("malformed")).toBe(2);
  });

  it("does not reset circuit breaker when all messages fail deserialization", async () => {
    deserializeMocks.deserializeRawEvent.mockImplementation(() => { throw new Error("bad"); });

    const cb = new PostgresCircuitBreaker(3, 30000);
    cb.recordFailure();
    cb.recordFailure();
    // 2 failures recorded, circuit not open yet

    const ctx = createMockContext({ circuitBreaker: cb });
    const payload = createMockPayload([
      { offset: "10", value: Buffer.from("bad") },
    ]);

    await processBatch(ctx, payload);

    // Circuit breaker should NOT have been reset - no DB call was made
    // Next failure should open the circuit (3 of 3)
    expect(cb.recordFailure()).toBe(true);
  });

  it("does not mark postgres healthy when no Postgres write succeeds", async () => {
    deserializeMocks.deserializeRawEvent.mockImplementation(() => { throw new Error("bad"); });

    const ctx = createMockContext();
    ctx.healthContext.postgresHealthy = false;
    ctx.lagWriteTimestamps.set("events.raw:0", Date.now());

    const payload = createMockPayload([
      { offset: "10", value: Buffer.from("bad") },
    ]);

    await processBatch(ctx, payload);

    expect(persistMocks.persistBatch).not.toHaveBeenCalled();
    expect(ctx.healthContext.postgresHealthy).toBe(false);
  });

  it("clears stale circuitOpen health state when breaker is closed", async () => {
    deserializeMocks.deserializeRawEvent.mockImplementation(() => { throw new Error("bad"); });

    const ctx = createMockContext();
    ctx.healthContext.circuitOpen = true;
    ctx.lagWriteTimestamps.set("events.raw:0", Date.now());

    const payload = createMockPayload([
      { offset: "10", value: Buffer.from("bad") },
    ]);

    await processBatch(ctx, payload);

    expect(ctx.healthContext.circuitOpen).toBe(false);
  });

  it("records circuit breaker failure and rethrows on persist error", async () => {
    const event = createEvent("rss:1");
    deserializeMocks.deserializeRawEvent.mockReturnValue(event);
    persistMocks.persistBatch.mockRejectedValue(new Error("connection refused"));

    const ctx = createMockContext();
    const payload = createMockPayload([
      { offset: "10", value: Buffer.from("{}") },
    ]);

    await expect(processBatch(ctx, payload)).rejects.toThrow("connection refused");

    expect(ctx.healthContext.postgresHealthy).toBe(false);
    expect(ctx.healthContext.metrics.errors.get("postgres_error")).toBe(1);
    expect(ctx.logger.error).toHaveBeenCalledWith(
      expect.objectContaining({ error: expect.objectContaining({ message: "connection refused" }) }),
      "Failed to persist Kafka batch"
    );
  });

  it("sets lastEventAt on successful persist", async () => {
    const event = createEvent("rss:1");
    deserializeMocks.deserializeRawEvent.mockReturnValue(event);
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 1,
      inserted: 1,
      duplicates: 0,
      insertedBySource: new Map([[Source.rss, 1]]),
    });
    redisMocks.markEventsSeen.mockResolvedValue(undefined);

    const ctx = createMockContext();
    const payload = createMockPayload([
      { offset: "10", value: Buffer.from("{}") },
    ]);

    expect(ctx.healthContext.lastEventAt).toBeUndefined();

    await processBatch(ctx, payload);

    expect(ctx.healthContext.lastEventAt).toBeInstanceOf(Date);
  });

  it("calculates consumer lag from offsets", async () => {
    const event = createEvent("rss:1");
    deserializeMocks.deserializeRawEvent.mockReturnValue(event);
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 1,
      inserted: 1,
      duplicates: 0,
      insertedBySource: new Map([[Source.rss, 1]]),
    });
    redisMocks.markEventsSeen.mockResolvedValue(undefined);
    persistMocks.upsertConsumerLag.mockResolvedValue(undefined);

    const ctx = createMockContext();
    const messages = [{ offset: "50", value: Buffer.from("{}") }];
    const payload = createMockPayload(messages);
    payload.batch.highWatermark = "100";

    await processBatch(ctx, payload);

    // currentOffset = 50 + 1 = 51, latestOffset = 100, lag = 49
    expect(ctx.healthContext.metrics.consumerLag.get(0)).toBe(49n);
  });

  it("handles lag update failure gracefully", async () => {
    const event = createEvent("rss:1");
    deserializeMocks.deserializeRawEvent.mockReturnValue(event);
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 1,
      inserted: 1,
      duplicates: 0,
      insertedBySource: new Map([[Source.rss, 1]]),
    });
    redisMocks.markEventsSeen.mockResolvedValue(undefined);
    persistMocks.upsertConsumerLag.mockRejectedValue(new Error("lag write failed"));

    const ctx = createMockContext();
    const payload = createMockPayload([
      { offset: "50", value: Buffer.from("{}") },
    ]);

    // Should not throw
    await processBatch(ctx, payload);

    expect(ctx.logger.warn).toHaveBeenCalledWith(
      expect.objectContaining({ error: expect.objectContaining({ message: "lag write failed" }) }),
      "Failed to update consumer lag"
    );
    expect(ctx.healthContext.metrics.errors.get("postgres_error")).toBe(1);
  });
});

describe("persistAndMarkSeen", () => {
  let persistAndMarkSeen: typeof import("../src/process.js").persistAndMarkSeen;

  beforeEach(async () => {
    vi.clearAllMocks();
    const mod = await import("../src/process.js");
    persistAndMarkSeen = mod.persistAndMarkSeen;
  });

  it("persists to Postgres and marks seen in Redis", async () => {
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 2,
      inserted: 2,
      duplicates: 0,
      insertedBySource: new Map([[Source.rss, 2]]),
    });
    redisMocks.markEventsSeen.mockResolvedValue(undefined);

    const events = [createEvent("rss:1"), createEvent("rss:2")];
    const ctx = createMockContext();

    await persistAndMarkSeen(ctx, events);

    expect(persistMocks.persistBatch).toHaveBeenCalledWith(ctx.prisma, events);
    expect(redisMocks.markEventsSeen).toHaveBeenCalledWith(ctx.redis, events, 86400);
    expect(ctx.healthContext.metrics.eventsProcessed.get(Source.rss)).toBe(2);
  });

  it("increments duplicate skip count", async () => {
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 3,
      inserted: 1,
      duplicates: 2,
      insertedBySource: new Map([[Source.rss, 1]]),
    });
    redisMocks.markEventsSeen.mockResolvedValue(undefined);

    const events = [createEvent("rss:1"), createEvent("rss:2"), createEvent("rss:3")];
    const ctx = createMockContext();

    await persistAndMarkSeen(ctx, events);

    expect(ctx.healthContext.metrics.eventsSkipped.get("duplicate")).toBe(2);
  });

  it("skips Redis when redis is null", async () => {
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 1,
      inserted: 1,
      duplicates: 0,
      insertedBySource: new Map([[Source.rss, 1]]),
    });

    const ctx = createMockContext({ redis: null });

    await persistAndMarkSeen(ctx, [createEvent("rss:1")]);

    expect(redisMocks.markEventsSeen).not.toHaveBeenCalled();
  });

  it("continues when Redis write fails", async () => {
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 1,
      inserted: 1,
      duplicates: 0,
      insertedBySource: new Map([[Source.rss, 1]]),
    });
    redisMocks.markEventsSeen.mockRejectedValue(new Error("READONLY"));

    const ctx = createMockContext();

    // Should not throw
    await persistAndMarkSeen(ctx, [createEvent("rss:1")]);

    expect(ctx.healthContext.redisHealthy).toBe(false);
    expect(ctx.healthContext.metrics.errors.get("redis_error")).toBe(1);
    expect(ctx.logger.warn).toHaveBeenCalledWith(
      expect.objectContaining({ error: expect.objectContaining({ message: "READONLY" }) }),
      "Failed to write seen keys to Redis; continuing"
    );
  });

  it("sets redisHealthy to true on Redis success", async () => {
    persistMocks.persistBatch.mockResolvedValue({
      attempted: 1,
      inserted: 1,
      duplicates: 0,
      insertedBySource: new Map([[Source.rss, 1]]),
    });
    redisMocks.markEventsSeen.mockResolvedValue(undefined);

    const ctx = createMockContext();
    ctx.healthContext.redisHealthy = false;

    await persistAndMarkSeen(ctx, [createEvent("rss:1")]);

    expect(ctx.healthContext.redisHealthy).toBe(true);
  });
});

describe("updateLag", () => {
  let updateLag: typeof import("../src/process.js").updateLag;

  beforeEach(async () => {
    vi.clearAllMocks();
    const mod = await import("../src/process.js");
    updateLag = mod.updateLag;
  });

  it("writes lag to Postgres on first call", async () => {
    persistMocks.upsertConsumerLag.mockResolvedValue(undefined);

    const ctx = createMockContext();
    const payload = createMockPayload([], {
      batch: { topic: "events.raw", partition: 0, highWatermark: "100", messages: [] },
    });

    const wrote = await updateLag(ctx, payload, 50n, 100n, 50n);

    expect(persistMocks.upsertConsumerLag).toHaveBeenCalledWith(ctx.prisma, expect.objectContaining({
      consumerGroup: "persister",
      topic: "events.raw",
      partition: 0,
      currentOffset: 50n,
      latestOffset: 100n,
      lagMessages: 50n,
    }));
    expect(wrote).toBe(true);
    expect(ctx.healthContext.metrics.consumerLag.get(0)).toBe(50n);
  });

  it("throttles Postgres writes within interval", async () => {
    persistMocks.upsertConsumerLag.mockResolvedValue(undefined);

    const ctx = createMockContext();
    const payload = createMockPayload([], {
      batch: { topic: "events.raw", partition: 0, highWatermark: "100", messages: [] },
    });

    // First call writes
    await updateLag(ctx, payload, 50n, 100n, 50n);
    expect(persistMocks.upsertConsumerLag).toHaveBeenCalledTimes(1);

    // Second call within interval is throttled (same partition)
    const wrote = await updateLag(ctx, payload, 60n, 100n, 40n);
    expect(persistMocks.upsertConsumerLag).toHaveBeenCalledTimes(1);
    expect(wrote).toBe(false);

    // But health context lag is always updated
    expect(ctx.healthContext.metrics.consumerLag.get(0)).toBe(40n);
  });

  it("allows writes to different partitions independently", async () => {
    persistMocks.upsertConsumerLag.mockResolvedValue(undefined);

    const ctx = createMockContext();
    const payload0 = createMockPayload([], {
      batch: { topic: "events.raw", partition: 0, highWatermark: "100", messages: [] },
    });
    const payload1 = createMockPayload([], {
      batch: { topic: "events.raw", partition: 1, highWatermark: "200", messages: [] },
    });

    await updateLag(ctx, payload0, 50n, 100n, 50n);
    await updateLag(ctx, payload1, 150n, 200n, 50n);

    expect(persistMocks.upsertConsumerLag).toHaveBeenCalledTimes(2);
  });
});
