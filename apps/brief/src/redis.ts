import { Redis } from "ioredis";
import type { Logger } from "pino";
import { redactUrlPassword } from "@rising-intelligence/shared";

export async function createRedisClient(redisUrl: string, logger: Logger): Promise<Redis> {
  const redis = new Redis(redisUrl, {
    maxRetriesPerRequest: 3,
    enableReadyCheck: true,
    lazyConnect: true,
  });

  try {
    await redis.connect();
    logger.info({ redisUrl: redactUrlPassword(redisUrl, "<invalid-redis-url>") }, "Redis connected");
    return redis;
  } catch (error) {
    logger.error({ error }, "Failed to connect to Redis");
    redis.disconnect();
    throw error;
  }
}

export async function disconnectRedis(redis: Redis | null, logger: Logger): Promise<void> {
  if (!redis) {
    return;
  }

  await redis.quit();
  logger.info("Redis disconnected");
}
