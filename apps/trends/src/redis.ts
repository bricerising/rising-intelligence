import { Redis } from "ioredis";
import type { Logger } from "pino";
import { redactUrlPassword } from "@rising-intelligence/shared";
import type { TrendWindow, ParsedRawEvent } from "./types.js";

const WINDOW_SECONDS: Record<TrendWindow, number> = {
  "15m": 15 * 60,
  "60m": 60 * 60,
};

const APPLY_EVENT_TO_WINDOWS_SCRIPT = `
local dedupKey = KEYS[1]
local eventId = ARGV[1]
local dedupTtlSeconds = tonumber(ARGV[2])
local maxEvidencePerTopic = tonumber(ARGV[3])
local engagementScore = tonumber(ARGV[4])
local planCount = tonumber(ARGV[5])

local dedupAdded = redis.call("SADD", dedupKey, eventId)
redis.call("EXPIRE", dedupKey, dedupTtlSeconds)

if dedupAdded == 0 then
  return 0
end

local argIndex = 6
for _ = 1, planCount do
  local counterKey = ARGV[argIndex]
  local counterTtl = tonumber(ARGV[argIndex + 1])
  local evidenceKey = ARGV[argIndex + 2]
  local evidenceTtl = tonumber(ARGV[argIndex + 3])

  redis.call("INCR", counterKey)
  redis.call("EXPIRE", counterKey, counterTtl)
  redis.call("ZADD", evidenceKey, engagementScore, eventId)
  redis.call("ZREMRANGEBYRANK", evidenceKey, 0, -(maxEvidencePerTopic + 1))
  redis.call("EXPIRE", evidenceKey, evidenceTtl)

  argIndex = argIndex + 4
end

return 1
`;

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
  logger.info({ redisUrl: redactUrlPassword(redisUrl, "<invalid-redis-url>") }, "Redis connected");
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
  const dedupTtlSeconds = getWindowSeconds(dedupWindow) * 3;
  const engagementScore = event.engagementScore ?? 0;
  const windowPlans: Array<{
    counterKey: string;
    counterTtlSeconds: number;
    evidenceKey: string;
    evidenceTtlSeconds: number;
  }> = [];

  for (const window of windows) {
    const bucket = buckets[window];
    if (!bucket) {
      continue;
    }

    const ttlSeconds = getWindowSeconds(window) * 3;
    const evidenceTtlSeconds = getWindowSeconds(window) * 2;

    for (const topic of topics) {
      windowPlans.push({
        counterKey: getCounterKey(window, topic, bucket),
        counterTtlSeconds: ttlSeconds,
        evidenceKey: getEvidenceKey(window, topic),
        evidenceTtlSeconds,
      });
    }
  }

  const planArgs = windowPlans.flatMap((plan) => [
    plan.counterKey,
    plan.counterTtlSeconds,
    plan.evidenceKey,
    plan.evidenceTtlSeconds,
  ]);
  const scriptResult = await redis.eval(
    APPLY_EVENT_TO_WINDOWS_SCRIPT,
    1,
    dedupKey,
    event.eventId,
    dedupTtlSeconds,
    maxEvidencePerTopic,
    engagementScore,
    windowPlans.length,
    ...planArgs
  );
  const dedupAdded = typeof scriptResult === "number"
    ? scriptResult
    : Number.parseInt(String(scriptResult), 10);
  if (Number.isNaN(dedupAdded) || (dedupAdded !== 0 && dedupAdded !== 1)) {
    throw new Error(`Unexpected dedup script result: ${String(scriptResult)}`);
  }
  if (dedupAdded === 0) {
    return { duplicate: true, buckets };
  }

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
