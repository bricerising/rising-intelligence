import { Source, type PrismaClient } from "@rising-intelligence/db";
import type { Producer } from "kafkajs";
import { serializeError } from "@rising-intelligence/shared";
import type pino from "pino";
import type { Config } from "./config.js";
import {
  incrementBriefSkippedStaleData,
  incrementBriefTriggered,
  incrementError,
  type HealthContext,
} from "./health.js";
import { publishSummaryRequest } from "./kafka/producer.js";
import type { PublishedWindowSnapshot } from "./snapshot.js";
import type { TopicSnapshotMetric, TrendWindow } from "./types.js";

interface TriggerContext {
  config: Config;
  logger: pino.Logger;
  prisma: PrismaClient;
  producer: Producer;
  healthContext: HealthContext;
  snapshots: PublishedWindowSnapshot[];
  lastDailyTriggerDate: string | null;
  now?: Date;
}

interface TopicMetricWithWindowEnd {
  metric: TopicSnapshotMetric;
  windowEndIso: string;
}

function mapTrendWindowToProto(window: TrendWindow): number {
  if (window === "15m") {
    return 1;
  }
  if (window === "60m") {
    return 2;
  }
  return 0;
}

function mapSourceToProto(source: Source): number {
  if (source === Source.rss) {
    return 1;
  }
  if (source === Source.news) {
    return 2;
  }
  if (source === Source.hackernews) {
    return 3;
  }
  if (source === Source.reddit) {
    return 4;
  }
  if (source === Source.github) {
    return 5;
  }
  if (source === Source.bluesky) {
    return 7;
  }
  return 8;
}

function buildMetricsByTopic(
  snapshots: PublishedWindowSnapshot[]
): Map<string, TopicMetricWithWindowEnd[]> {
  const metricsByTopic = new Map<string, TopicMetricWithWindowEnd[]>();

  for (const snapshot of snapshots) {
    const windowEndIso = snapshot.generatedAt.toISOString();
    for (const metric of snapshot.topMetrics) {
      const existing = metricsByTopic.get(metric.topic);
      if (existing) {
        existing.push({ metric, windowEndIso });
      } else {
        metricsByTopic.set(metric.topic, [{ metric, windowEndIso }]);
      }
    }
  }

  return metricsByTopic;
}

function getPrimarySnapshot(
  snapshots: PublishedWindowSnapshot[]
): PublishedWindowSnapshot | null {
  if (snapshots.length === 0) {
    return null;
  }

  return snapshots.find((snapshot) => snapshot.window === "60m") ?? snapshots[0];
}

async function loadEvidenceForTopic(
  prisma: PrismaClient,
  topic: string,
  maxEvidencePerTopic: number
) {
  const rawEvents = await prisma.rawEvent.findMany({
    where: {
      topics: {
        has: topic,
      },
    },
    orderBy: {
      fetchedAt: "desc",
    },
    take: maxEvidencePerTopic,
    select: {
      eventId: true,
      source: true,
      url: true,
      title: true,
      publishedAt: true,
      fetchedAt: true,
      text: true,
      engagementScore: true,
      engagementComments: true,
      engagementLikes: true,
      engagementShares: true,
    },
  });

  return rawEvents.map((event) => ({
    event_id: event.eventId,
    source: mapSourceToProto(event.source),
    url: event.url ?? "",
    title: event.title ?? "",
    published_at: event.publishedAt ? event.publishedAt.toISOString() : "",
    fetched_at: event.fetchedAt.toISOString(),
    text_excerpt: event.text.slice(0, 500),
    engagement: {
      score: event.engagementScore ?? 0,
      comments: event.engagementComments ?? 0,
      likes: event.engagementLikes ?? 0,
      shares: event.engagementShares ?? 0,
    },
  }));
}

async function isDataFresh(
  config: Config,
  prisma: PrismaClient,
  logger: pino.Logger,
  now: Date
): Promise<boolean> {
  const requiredConsumerGroups = [config.KAFKA_CONSUMER_GROUP, config.PERSISTER_CONSUMER_GROUP];
  const lagRows = await prisma.consumerLag.findMany({
    where: {
      topic: config.KAFKA_TOPIC_RAW_EVENTS,
      consumerGroup: {
        in: requiredConsumerGroups,
      },
    },
    select: {
      consumerGroup: true,
      lagMessages: true,
      updatedAt: true,
    },
  });

  if (lagRows.length === 0) {
    logger.warn("Skipping daily brief trigger: consumer lag records are missing");
    return false;
  }

  const staleCutoffMs = now.getTime() - config.MAX_LAG_AGE_MS;
  const maxAllowedLag = BigInt(config.MAX_LAG_MESSAGES);

  for (const group of requiredConsumerGroups) {
    const groupRows = lagRows.filter((row) => row.consumerGroup === group);
    if (groupRows.length === 0) {
      logger.warn({ consumerGroup: group }, "Skipping daily brief trigger: missing lag records");
      return false;
    }

    if (groupRows.some((row) => row.updatedAt.getTime() < staleCutoffMs)) {
      logger.warn({ consumerGroup: group }, "Skipping daily brief trigger: stale lag records");
      return false;
    }

    const totalLag = groupRows.reduce((sum, row) => sum + row.lagMessages, 0n);
    if (totalLag > maxAllowedLag) {
      logger.warn(
        { consumerGroup: group, totalLag: totalLag.toString(), maxAllowedLag: maxAllowedLag.toString() },
        "Skipping daily brief trigger: lag exceeds threshold"
      );
      return false;
    }
  }

  return true;
}

function shouldTriggerDailyBrief(
  config: Config,
  now: Date,
  lastDailyTriggerDate: string | null
): { shouldTrigger: boolean; dateKey: string } {
  const dateKey = now.toISOString().slice(0, 10);
  if (!config.DAILY_BRIEF_ENABLED) {
    return { shouldTrigger: false, dateKey };
  }

  if (lastDailyTriggerDate === dateKey) {
    return { shouldTrigger: false, dateKey };
  }

  const nowMinute = now.getUTCHours() * 60 + now.getUTCMinutes();
  const triggerMinute = config.DAILY_BRIEF_UTC_HOUR * 60 + config.DAILY_BRIEF_UTC_MINUTE;
  if (nowMinute < triggerMinute) {
    return { shouldTrigger: false, dateKey };
  }

  return { shouldTrigger: true, dateKey };
}

export async function maybeTriggerDailySummaryRequest(ctx: TriggerContext): Promise<string | null> {
  const now = ctx.now ?? new Date();
  const logger = ctx.logger.child({ component: "brief-trigger" });

  const trigger = shouldTriggerDailyBrief(ctx.config, now, ctx.lastDailyTriggerDate);
  if (!trigger.shouldTrigger) {
    return ctx.lastDailyTriggerDate;
  }

  if (ctx.snapshots.length === 0) {
    return ctx.lastDailyTriggerDate;
  }

  const fresh = await isDataFresh(ctx.config, ctx.prisma, logger, now);
  if (!fresh) {
    incrementBriefSkippedStaleData(ctx.healthContext);
    return ctx.lastDailyTriggerDate;
  }

  const primarySnapshot = getPrimarySnapshot(ctx.snapshots);
  if (!primarySnapshot) {
    return ctx.lastDailyTriggerDate;
  }

  const selectedTopics = primarySnapshot.topMetrics
    .slice(0, ctx.config.BRIEF_MAX_TOPICS)
    .map((metric) => metric.topic);
  if (selectedTopics.length === 0) {
    logger.info("Skipping daily brief trigger: no ranked topics available");
    return ctx.lastDailyTriggerDate;
  }

  const metricsByTopic = buildMetricsByTopic(ctx.snapshots);
  const topicInputs = await Promise.all(
    selectedTopics.map(async (topic) => {
      const metrics = (metricsByTopic.get(topic) ?? []).map(({ metric, windowEndIso }) => ({
        topic: metric.topic,
        window: mapTrendWindowToProto(metric.window),
        window_end: windowEndIso,
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
      }));
      const evidence = await loadEvidenceForTopic(
        ctx.prisma,
        topic,
        ctx.config.BRIEF_MAX_EVIDENCE_PER_TOPIC
      );
      return {
        topic,
        metrics,
        evidence,
      };
    })
  );

  const requestId = `daily:${trigger.dateKey}`;
  const windows = [...new Set(ctx.snapshots.map((snapshot) => mapTrendWindowToProto(snapshot.window)))];
  const summaryRequest = {
    request_id: requestId,
    requested_at: now.toISOString(),
    type: 1,
    windows,
    topics: topicInputs,
    budget: {
      daily_budget_usd: ctx.config.BRIEF_DAILY_BUDGET_USD,
      max_topics: ctx.config.BRIEF_MAX_TOPICS,
      max_evidence_per_topic: ctx.config.BRIEF_MAX_EVIDENCE_PER_TOPIC,
      max_output_tokens: ctx.config.BRIEF_MAX_OUTPUT_TOKENS,
    },
  };

  try {
    await publishSummaryRequest(
      ctx.producer,
      ctx.config.KAFKA_TOPIC_SUMMARY_REQUESTS,
      requestId,
      Buffer.from(JSON.stringify(summaryRequest), "utf-8"),
      logger
    );
    incrementBriefTriggered(ctx.healthContext, "daily");
    logger.info(
      {
        requestId,
        topicCount: summaryRequest.topics.length,
        windowCount: summaryRequest.windows.length,
      },
      "Published daily summary request"
    );
    return trigger.dateKey;
  } catch (error) {
    incrementError(ctx.healthContext, "brief_trigger_error");
    logger.error(
      { requestId, error: serializeError(error) },
      "Failed to publish daily summary request"
    );
    return ctx.lastDailyTriggerDate;
  }
}
