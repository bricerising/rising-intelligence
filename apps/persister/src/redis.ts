import { Redis } from "ioredis";
import type pino from "pino";
import type { Config } from "./config.js";
import type { ParsedRawEvent } from "./types.js";

function redactRedisUrl(redisUrl: string): string {
  try {
    const parsed = new URL(redisUrl);
    if (parsed.password) {
      parsed.password = "***";
    }
    return parsed.toString();
  } catch {
    return "<invalid-redis-url>";
  }
}

export async function createRedisClient(
  config: Config,
  logger: pino.Logger
): Promise<Redis | null> {
  if (!config.REDIS_URL) {
    logger.warn("REDIS_URL not configured; seen-cache disabled");
    return null;
  }

  const redis = new Redis(config.REDIS_URL, {
    maxRetriesPerRequest: 1,
  });

  try {
    await redis.ping();
    logger.info({ redisUrl: redactRedisUrl(config.REDIS_URL) }, "Redis connected");
    return redis;
  } catch (error) {
    logger.warn({ err: error }, "Failed to connect to Redis; continuing without cache");
    redis.disconnect();
    return null;
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
