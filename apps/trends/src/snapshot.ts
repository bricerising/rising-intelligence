import { TrendWindow as DbTrendWindow, PrismaClient, type Prisma } from "@rising-intelligence/db";
import type { ProducerConnection } from "@rising-intelligence/pipeline/transport";
import type { Redis } from "ioredis";
import type { Logger } from "pino";
import type { Config } from "./config.js";
import type { CompiledAllowlist } from "./allowlist.js";
import type { TopicSnapshotMetric, TrendWindow } from "./types.js";
import type { HealthContext } from "./health.js";
import {
  clearTopicMetrics,
  incrementSnapshotPublished,
  observeBaselineComputeDuration,
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
import {
  createTrendsSnapshotPublisher,
  type TrendsSnapshotPublisher,
} from "./publishing-facade.js";

const TOPIC_METRIC_LIMIT = 30;
const DOW = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"] as const;
const WINDOW_TO_PROTO: Record<TrendWindow, number> = {
  "15m": 1,
  "60m": 2,
};
const WINDOW_TO_DB: Record<TrendWindow, DbTrendWindow> = {
  "15m": DbTrendWindow.WINDOW_15M,
  "60m": DbTrendWindow.WINDOW_60M,
};

function mapWindowToProto(window: TrendWindow): number {
  return WINDOW_TO_PROTO[window];
}

function mapWindowToDb(window: TrendWindow): DbTrendWindow {
  return WINDOW_TO_DB[window];
}

function parseCount(value: string | null): number {
  if (value === null) {
    return 0;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function getDayOfWeekKey(timestamp: Date): string {
  return DOW[timestamp.getUTCDay()];
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
  producer: ProducerConnection;
  prisma: PrismaClient;
  allowlist: CompiledAllowlist;
  healthContext: HealthContext;
}

export interface PublishedWindowSnapshot {
  window: TrendWindow;
  generatedAt: Date;
  topMetrics: TopicSnapshotMetric[];
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
  const dayOfWeek = getDayOfWeekKey(new Date(bucket));

  // Build a single pipeline for all topics (4 commands per topic)
  const pipeline = redis.pipeline();
  for (const topic of allowlist.topics) {
    pipeline.get(getCounterKey(window, topic.key, bucket));
    pipeline.get(getPreviousCounterKey(window, topic.key));
    pipeline.get(`baseline:${window}:${topic.key}:${dayOfWeek}`);
    pipeline.zrevrange(getEvidenceKey(window, topic.key), 0, maxEvidencePerTopic - 1);
  }

  const results = await pipeline.exec();
  if (!results) {
    throw new Error("Redis pipeline execution returned null");
  }

  for (let i = 0; i < allowlist.topics.length; i++) {
    const topic = allowlist.topics[i];
    const base = i * 4;
    const currentRaw = results[base][1] as string | null;
    const previousRaw = results[base + 1][1] as string | null;
    const baselineRaw = results[base + 2][1] as string | null;
    const evidenceEventIds = (results[base + 3][1] as string[] | null) ?? [];

    const volume = parseCount(currentRaw);
    const prevVolume = parseCount(previousRaw);
    const baselineVolume = parseCount(baselineRaw);
    const baselineDelta = baselineVolume > 0 ? (volume - baselineVolume) / baselineVolume : 0;
    const acceleration = computeAcceleration(volume, prevVolume);
    const score = computeScore(volume, acceleration);

    countsByTopic.set(topic.key, volume);
    metrics.push({
      topic: topic.key,
      window,
      volume,
      prevVolume,
      acceleration,
      baselineVolume,
      baselineDelta,
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
  publisher: TrendsSnapshotPublisher,
  window: TrendWindow
): Promise<PublishedWindowSnapshot> {
  const startTime = Date.now();
  const generatedAt = new Date();
  const generatedAtIso = generatedAt.toISOString();
  const bucket = getBucketStart(generatedAt, window);

  const baselineComputeStart = Date.now();
  const { metrics, countsByTopic } = await computeWindowMetrics(
    ctx.redis,
    ctx.allowlist,
    window,
    bucket,
    ctx.config.MAX_EVIDENCE_PER_TOPIC
  );
  observeBaselineComputeDuration(
    ctx.healthContext,
    (Date.now() - baselineComputeStart) / 1000
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
  await publisher.publishSnapshot(snapshotKey, snapshot);

  await ctx.prisma.trendSnapshot.create({
    data: {
      generatedAt,
      window: mapWindowToDb(window),
      snapshot: snapshot as Prisma.InputJsonValue,
    },
  });

  await writePreviousWindowCounts(ctx.redis, window, countsByTopic);

  incrementSnapshotPublished(ctx.healthContext, window);
  observeSnapshotDuration(
    ctx.healthContext,
    window,
    (Date.now() - startTime) / 1000
  );

  return {
    window,
    generatedAt,
    topMetrics,
  };
}

export async function publishSnapshots(ctx: SnapshotContext): Promise<PublishedWindowSnapshot[]> {
  const publisher = createTrendsSnapshotPublisher({
    connection: ctx.producer,
    logger: ctx.logger,
    topic: ctx.config.KAFKA_TOPIC_TRENDS_SNAPSHOTS,
  });

  const publishedSnapshots: PublishedWindowSnapshot[] = [];
  for (const window of ctx.config.WINDOWS) {
    publishedSnapshots.push(await publishWindowSnapshot(ctx, publisher, window));
  }
  return publishedSnapshots;
}
