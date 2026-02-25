import { Redis } from "ioredis";
import { redactUrlPassword } from "@rising-intelligence/shared/http";
import type pino from "pino";
import type { Config } from "./config.js";
import type { ParsedRawEvent } from "./types.js";

export async function createRedisClient(
  config: Config,
  logger: pino.Logger
): Promise<Redis> {
  const redis = new Redis(config.REDIS_URL, {
    maxRetriesPerRequest: 1,
  });

  try {
    await redis.ping();
    logger.info(
      { redisUrl: redactUrlPassword(config.REDIS_URL, "<invalid-redis-url>") },
      "Redis connected"
    );
    return redis;
  } catch (error) {
    logger.error({ error }, "Failed to connect to Redis");
    redis.disconnect();
    throw error;
  }
}

export async function markEventsSeen(
  redis: Redis,
  events: ParsedRawEvent[],
  ttlSeconds: number
): Promise<void> {
  if (events.length === 0) {
    return;
  }

  const pipeline = redis.pipeline();
  for (const event of events) {
    const key = `seen:${event.source}:${event.eventId}`;
    pipeline.set(key, "1", "EX", ttlSeconds, "NX");
  }

  const results = await pipeline.exec();
  if (!results) {
    throw new Error("Redis pipeline execution returned null");
  }

  for (const [error] of results) {
    if (error) {
      throw error;
    }
  }
}

export async function disconnectRedis(redis: Redis | null): Promise<void> {
  if (!redis) {
    return;
  }

  await redis.quit();
}
