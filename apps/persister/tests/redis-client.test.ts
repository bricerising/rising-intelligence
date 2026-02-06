import { beforeEach, describe, expect, it, vi } from "vitest";

const redisMocks = vi.hoisted(() => ({
  Redis: vi.fn(),
}));

vi.mock("ioredis", () => ({
  Redis: redisMocks.Redis,
}));

describe("createRedisClient", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
  });

  it("returns null when REDIS_URL is not configured", async () => {
    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
    } as any;

    const { createRedisClient } = await import("../src/redis.js");
    const client = await createRedisClient({ REDIS_URL: "" } as any, logger);

    expect(client).toBeNull();
    expect(redisMocks.Redis).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledWith("REDIS_URL not configured; seen-cache disabled");
  });

  it("returns connected redis client when ping succeeds", async () => {
    const redisClient = {
      ping: vi.fn().mockResolvedValue("PONG"),
      disconnect: vi.fn(),
    };
    redisMocks.Redis.mockImplementation(() => redisClient);

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
    } as any;

    const { createRedisClient } = await import("../src/redis.js");
    const client = await createRedisClient({ REDIS_URL: "redis://cache:6379" } as any, logger);

    expect(redisMocks.Redis).toHaveBeenCalledWith("redis://cache:6379", {
      maxRetriesPerRequest: 1,
    });
    expect(redisClient.ping).toHaveBeenCalledOnce();
    expect(client).toBe(redisClient);
    expect(logger.info).toHaveBeenCalledWith(
      { redisUrl: "redis://cache:6379" },
      "Redis connected"
    );
  });

  it("redacts password from REDIS_URL in success logs", async () => {
    const redisClient = {
      ping: vi.fn().mockResolvedValue("PONG"),
      disconnect: vi.fn(),
    };
    redisMocks.Redis.mockImplementation(() => redisClient);

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
    } as any;

    const { createRedisClient } = await import("../src/redis.js");
    await createRedisClient({ REDIS_URL: "redis://:secret@cache:6379/0" } as any, logger);

    expect(logger.info).toHaveBeenCalledWith(
      { redisUrl: "redis://:***@cache:6379/0" },
      "Redis connected"
    );
  });

  it("logs invalid REDIS_URL as redacted placeholder on successful ping", async () => {
    const redisClient = {
      ping: vi.fn().mockResolvedValue("PONG"),
      disconnect: vi.fn(),
    };
    redisMocks.Redis.mockImplementation(() => redisClient);

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
    } as any;

    const { createRedisClient } = await import("../src/redis.js");
    await createRedisClient({ REDIS_URL: "not-a-valid-url" } as any, logger);

    expect(logger.info).toHaveBeenCalledWith(
      { redisUrl: "<invalid-redis-url>" },
      "Redis connected"
    );
  });

  it("disconnects and returns null when ping fails", async () => {
    const error = new Error("ECONNREFUSED");
    const redisClient = {
      ping: vi.fn().mockRejectedValue(error),
      disconnect: vi.fn(),
    };
    redisMocks.Redis.mockImplementation(() => redisClient);

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
    } as any;

    const { createRedisClient } = await import("../src/redis.js");
    const client = await createRedisClient({ REDIS_URL: "redis://cache:6379" } as any, logger);

    expect(client).toBeNull();
    expect(redisClient.disconnect).toHaveBeenCalledOnce();
    expect(logger.warn).toHaveBeenCalledWith(
      { err: error },
      "Failed to connect to Redis; continuing without cache"
    );
  });
});
