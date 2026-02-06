import { Redis } from "ioredis";
import type { Logger } from "pino";
import type { TrendWindow, ParsedRawEvent } from "./types.js";

const WINDOW_SECONDS: Record<TrendWindow, number> = {
  "15m": 15 * 60,
  "60m": 60 * 60,
};

function getWindowSeconds(window: TrendWindow): number {
  return WINDOW_SECONDS[window];
}

export function getBucketStart(timestamp: Date, window: TrendWindow): string {
  const windowMs = getWindowSeconds(window) * 1000;
  const bucketMs = Math.floor(timestamp.getTime() / windowMs) * windowMs;
  return new Date(bucketMs).toISOString();
}

export function getCounterKey(window: TrendWindow, topic: string, bucket: string): string {
  return `window:${window}:${topic}:${bucket}`;
}

export function getPreviousCounterKey(window: TrendWindow, topic: string): string {
  return `prev:${window}:${topic}`;
}

export function getEvidenceKey(window: TrendWindow, topic: string): string {
  return `evidence:${window}:${topic}`;
}

export function getDedupKey(window: TrendWindow, bucket: string): string {
  return `dedup:${window}:${bucket}`;
}

function chooseDedupWindow(windows: TrendWindow[]): TrendWindow {
  const sorted = [...windows].sort(
    (a, b) => getWindowSeconds(b) - getWindowSeconds(a)
  );
  return sorted[0];
}

export interface WindowUpdateResult {
  duplicate: boolean;
  buckets: Partial<Record<TrendWindow, string>>;
}

export async function createRedisClient(redisUrl: string, logger: Logger): Promise<Redis> {
  const redis = new Redis(redisUrl, {
    maxRetriesPerRequest: 3,
    enableReadyCheck: true,
    lazyConnect: true,
  });

  await redis.connect();
  logger.info({ redisUrl }, "Redis connected");
  return redis;
}

export async function disconnectRedis(redis: Redis | null, logger: Logger): Promise<void> {
  if (!redis) {
    return;
  }

  await redis.quit();
  logger.info("Redis disconnected");
}

export async function applyEventToWindows(
  redis: Redis,
  event: ParsedRawEvent,
  topics: string[],
  windows: TrendWindow[],
  maxEvidencePerTopic: number
): Promise<WindowUpdateResult> {
  if (topics.length === 0 || windows.length === 0) {
    return { duplicate: false, buckets: {} };
  }

  const buckets: Partial<Record<TrendWindow, string>> = {};
  for (const window of windows) {
    buckets[window] = getBucketStart(event.fetchedAt, window);
  }

  const dedupWindow = chooseDedupWindow(windows);
  const dedupBucket = buckets[dedupWindow];
  if (!dedupBucket) {
    throw new Error(`Missing dedup bucket for window ${dedupWindow}`);
  }

  const dedupKey = getDedupKey(dedupWindow, dedupBucket);
  const dedupAdded = await redis.sadd(dedupKey, event.eventId);
  await redis.expire(dedupKey, getWindowSeconds(dedupWindow) * 3);

  if (dedupAdded === 0) {
    return { duplicate: true, buckets };
  }

  const engagementScore = event.engagementScore ?? 0;
  const pipeline = redis.pipeline();

  for (const window of windows) {
    const bucket = buckets[window];
    if (!bucket) {
      continue;
    }

    const ttlSeconds = getWindowSeconds(window) * 3;
    const evidenceTtlSeconds = getWindowSeconds(window) * 2;

    for (const topic of topics) {
      const counterKey = getCounterKey(window, topic, bucket);
      const evidenceKey = getEvidenceKey(window, topic);

      pipeline.incr(counterKey);
      pipeline.expire(counterKey, ttlSeconds);

      pipeline.zadd(evidenceKey, engagementScore, event.eventId);
      pipeline.zremrangebyrank(evidenceKey, 0, -(maxEvidencePerTopic + 1));
      pipeline.expire(evidenceKey, evidenceTtlSeconds);
    }
  }

  await pipeline.exec();

  return { duplicate: false, buckets };
}

export async function writePreviousWindowCounts(
  redis: Redis,
  window: TrendWindow,
  countsByTopic: Map<string, number>
): Promise<void> {
  const ttlSeconds = getWindowSeconds(window) * 2;
  const pipeline = redis.pipeline();

  for (const [topic, count] of countsByTopic) {
    const key = getPreviousCounterKey(window, topic);
    pipeline.set(key, count.toString());
    pipeline.expire(key, ttlSeconds);
  }

  await pipeline.exec();
}
