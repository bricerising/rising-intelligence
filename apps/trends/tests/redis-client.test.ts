import { beforeEach, describe, expect, it, vi } from "vitest";

const redisMocks = vi.hoisted(() => ({
  Redis: vi.fn(),
}));

vi.mock("ioredis", () => ({
  Redis: redisMocks.Redis,
}));

describe("trends redis client", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
  });

  it("connects and logs redacted redis URL", async () => {
    const redisClient = {
      connect: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn(),
      quit: vi.fn().mockResolvedValue("OK"),
    };
    redisMocks.Redis.mockImplementation(() => redisClient);

    const logger = {
      info: vi.fn(),
      error: vi.fn(),
    } as any;

    const { createRedisClient } = await import("../src/redis.js");
    const client = await createRedisClient("redis://:secret@cache:6379/0", logger);

    expect(client).toBe(redisClient);
    expect(redisMocks.Redis).toHaveBeenCalledWith("redis://:secret@cache:6379/0", {
      maxRetriesPerRequest: 3,
      enableReadyCheck: true,
      lazyConnect: true,
    });
    expect(redisClient.connect).toHaveBeenCalledOnce();
    expect(logger.info).toHaveBeenCalledWith(
      { redisUrl: "redis://:***@cache:6379/0" },
      "Redis connected"
    );
  });

  it("disconnects and throws when connect fails", async () => {
    const redisClient = {
      connect: vi.fn().mockRejectedValue(new Error("ECONNREFUSED")),
      disconnect: vi.fn(),
      quit: vi.fn().mockResolvedValue("OK"),
    };
    redisMocks.Redis.mockImplementation(() => redisClient);

    const logger = {
      info: vi.fn(),
      error: vi.fn(),
    } as any;

    const { createRedisClient } = await import("../src/redis.js");
    await expect(createRedisClient("redis://cache:6379", logger)).rejects.toThrow(
      "ECONNREFUSED"
    );

    expect(redisClient.disconnect).toHaveBeenCalledOnce();
    expect(logger.error).toHaveBeenCalledWith(
      { error: expect.objectContaining({ message: "ECONNREFUSED" }) },
      "Failed to connect to Redis"
    );
  });
});

describe("trends disconnectRedis", () => {
  it("calls quit when redis client exists", async () => {
    const redis = { quit: vi.fn().mockResolvedValue("OK") } as any;
    const logger = { info: vi.fn() } as any;
    const { disconnectRedis } = await import("../src/redis.js");

    await disconnectRedis(redis, logger);

    expect(redis.quit).toHaveBeenCalledOnce();
    expect(logger.info).toHaveBeenCalledWith("Redis disconnected");
  });

  it("does nothing when redis client is null", async () => {
    const logger = { info: vi.fn() } as any;
    const { disconnectRedis } = await import("../src/redis.js");

    await disconnectRedis(null, logger);

    expect(logger.info).not.toHaveBeenCalled();
  });
});
