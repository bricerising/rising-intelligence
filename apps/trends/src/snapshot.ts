import { TrendWindow as DbTrendWindow, PrismaClient } from "@rising-intelligence/db";
import type { Producer } from "kafkajs";
import type { Redis } from "ioredis";
import type { Logger } from "pino";
import type { Config } from "./config.js";
import type { CompiledAllowlist } from "./allowlist.js";
import type { TopicSnapshotMetric, TrendWindow } from "./types.js";
import type { HealthContext } from "./health.js";
import {
  clearTopicMetrics,
  incrementSnapshotPublished,
  observeSnapshotDuration,
  setTopicMetrics,
} from "./health.js";
import {
  getBucketStart,
  getCounterKey,
  getEvidenceKey,
  getPreviousCounterKey,
  writePreviousWindowCounts,
} from "./redis.js";
import { publishSnapshot } from "./kafka/producer.js";

const TOPIC_METRIC_LIMIT = 30;

function mapWindowToProto(window: TrendWindow): number {
  switch (window) {
    case "15m":
      return 1;
    case "60m":
      return 2;
    default:
      return 0;
  }
}

function mapWindowToDb(window: TrendWindow): DbTrendWindow {
  switch (window) {
    case "15m":
      return DbTrendWindow.WINDOW_15M;
    case "60m":
      return DbTrendWindow.WINDOW_60M;
    default:
      throw new Error(`Unsupported snapshot window ${window}`);
  }
}

function parseCount(value: string | null): number {
  if (value === null) {
    return 0;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function computeAcceleration(volume: number, prevVolume: number): number {
  if (prevVolume <= 0) {
    return volume > 0 ? 1 : 0;
  }

  return (volume - prevVolume) / prevVolume;
}

function computeScore(volume: number, acceleration: number): number {
  const accelerationBoost = Math.max(0, acceleration * volume);
  return volume + accelerationBoost;
}

function toTrendMetricWire(metric: TopicSnapshotMetric, generatedAtIso: string) {
  return {
    topic: metric.topic,
    window: mapWindowToProto(metric.window),
    window_end: generatedAtIso,
    volume: metric.volume,
    prev_volume: metric.prevVolume,
    acceleration: metric.acceleration,
    baseline_volume: metric.baselineVolume,
    baseline_delta: metric.baselineDelta,
    score: metric.score,
    evidence: {
      top_urls: [],
      top_event_ids: metric.evidenceEventIds,
    },
  };
}

export interface SnapshotContext {
  config: Config;
  logger: Logger;
  redis: Redis;
  producer: Producer;
  prisma: PrismaClient;
  allowlist: CompiledAllowlist;
  healthContext: HealthContext;
}

async function computeWindowMetrics(
  redis: Redis,
  allowlist: CompiledAllowlist,
  window: TrendWindow,
  bucket: string,
  maxEvidencePerTopic: number
): Promise<{ metrics: TopicSnapshotMetric[]; countsByTopic: Map<string, number> }> {
  const metrics: TopicSnapshotMetric[] = [];
  const countsByTopic = new Map<string, number>();

  for (const topic of allowlist.topics) {
    const [currentRaw, previousRaw, evidenceEventIds] = await Promise.all([
      redis.get(getCounterKey(window, topic.key, bucket)),
      redis.get(getPreviousCounterKey(window, topic.key)),
      redis.zrevrange(getEvidenceKey(window, topic.key), 0, maxEvidencePerTopic - 1),
    ]);

    const volume = parseCount(currentRaw);
    const prevVolume = parseCount(previousRaw);
    const acceleration = computeAcceleration(volume, prevVolume);
    const score = computeScore(volume, acceleration);

    countsByTopic.set(topic.key, volume);
    metrics.push({
      topic: topic.key,
      window,
      volume,
      prevVolume,
      acceleration,
      baselineVolume: 0,
      baselineDelta: 0,
      score,
      evidenceEventIds,
    });
  }

  return {
    metrics,
    countsByTopic,
  };
}

async function publishWindowSnapshot(
  ctx: SnapshotContext,
  window: TrendWindow
): Promise<void> {
  const startTime = Date.now();
  const generatedAt = new Date();
  const generatedAtIso = generatedAt.toISOString();
  const bucket = getBucketStart(generatedAt, window);

  const { metrics, countsByTopic } = await computeWindowMetrics(
    ctx.redis,
    ctx.allowlist,
    window,
    bucket,
    ctx.config.MAX_EVIDENCE_PER_TOPIC
  );

  metrics.sort((left, right) => {
    if (right.score !== left.score) {
      return right.score - left.score;
    }
    return left.topic.localeCompare(right.topic);
  });

  const topMetrics = metrics.slice(0, ctx.config.TOP_N_TOPICS);

  clearTopicMetrics(ctx.healthContext, window);
  for (const metric of topMetrics.slice(0, TOPIC_METRIC_LIMIT)) {
    setTopicMetrics(ctx.healthContext, metric.topic, window, metric.volume, metric.score);
  }

  const snapshot = {
    generated_at: generatedAtIso,
    window: mapWindowToProto(window),
    topics: topMetrics.map((metric) => toTrendMetricWire(metric, generatedAtIso)),
  };

  const snapshotKey = `${window}:${generatedAtIso}`;
  await publishSnapshot(
    ctx.producer,
    ctx.config.KAFKA_TOPIC_TRENDS_SNAPSHOTS,
    snapshotKey,
    Buffer.from(JSON.stringify(snapshot), "utf-8"),
    ctx.logger
  );

  await ctx.prisma.trendSnapshot.create({
    data: {
      generatedAt,
      window: mapWindowToDb(window),
      snapshot: snapshot as any,
    },
  });

  await writePreviousWindowCounts(ctx.redis, window, countsByTopic);

  incrementSnapshotPublished(ctx.healthContext, window);
  observeSnapshotDuration(
    ctx.healthContext,
    window,
    (Date.now() - startTime) / 1000
  );
}

export async function publishSnapshots(ctx: SnapshotContext): Promise<void> {
  for (const window of ctx.config.WINDOWS) {
    await publishWindowSnapshot(ctx, window);
  }
}
