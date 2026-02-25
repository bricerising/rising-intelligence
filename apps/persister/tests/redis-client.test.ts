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

  it("returns connected redis client when ping succeeds", async () => {
    const redisClient = {
      ping: vi.fn().mockResolvedValue("PONG"),
      disconnect: vi.fn(),
    };
    redisMocks.Redis.mockImplementation(() => redisClient);

    const logger = {
      info: vi.fn(),
      error: vi.fn(),
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
      error: vi.fn(),
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
      error: vi.fn(),
    } as any;

    const { createRedisClient } = await import("../src/redis.js");
    await createRedisClient({ REDIS_URL: "not-a-valid-url" } as any, logger);

    expect(logger.info).toHaveBeenCalledWith(
      { redisUrl: "<invalid-redis-url>" },
      "Redis connected"
    );
  });

  it("disconnects and throws when ping fails", async () => {
    const error = new Error("ECONNREFUSED");
    const redisClient = {
      ping: vi.fn().mockRejectedValue(error),
      disconnect: vi.fn(),
    };
    redisMocks.Redis.mockImplementation(() => redisClient);

    const logger = {
      info: vi.fn(),
      error: vi.fn(),
    } as any;

    const { createRedisClient } = await import("../src/redis.js");
    await expect(
      createRedisClient({ REDIS_URL: "redis://cache:6379" } as any, logger)
    ).rejects.toThrow("ECONNREFUSED");

    expect(redisClient.disconnect).toHaveBeenCalledOnce();
    expect(logger.error).toHaveBeenCalledWith(
      { error },
      "Failed to connect to Redis"
    );
  });
});
