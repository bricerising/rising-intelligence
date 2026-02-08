import { BriefStatus, Prisma, type PrismaClient } from "@rising-intelligence/db";
import type { Producer } from "kafkajs";
import type { Redis } from "ioredis";
import { serializeError } from "@rising-intelligence/shared";
import type pino from "pino";
import type { Config } from "./config.js";
import {
  incrementBudgetExceeded,
  incrementDuplicatesSkipped,
  incrementError,
  incrementGeneration,
  incrementLlmCostUsd,
  incrementLlmTokens,
  observeCitationsCount,
  observeHighlightsCount,
  setBudgetRemainingUsd,
  type HealthContext,
} from "./health.js";
import { publishBriefResult } from "./kafka/producer.js";
import type { ParsedSummaryRequest, ParsedSummaryTopic } from "./types.js";

interface ProcessContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
  prisma: PrismaClient;
  redis: Redis;
  producer: Producer;
}

const BUDGET_KEY_PREFIX = "brief:budget";
const BUDGET_KEY_TTL_SECONDS = 48 * 60 * 60;

function getBudgetDateKey(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function getBudgetKey(dateKey: string): string {
  return `${BUDGET_KEY_PREFIX}:${dateKey}`;
}

function estimateTokenCount(text: string): number {
  return Math.max(1, Math.ceil(text.length / 4));
}

function estimateRequestCostUsd(request: ParsedSummaryRequest): number {
  const evidenceCount = request.topics.reduce((sum, topic) => sum + topic.evidence.length, 0);
  return Number((0.01 + request.topics.length * 0.002 + evidenceCount * 0.0005).toFixed(4));
}

function dedupeStrings(values: Array<string | null>): string[] {
  const unique = new Set<string>();
  for (const value of values) {
    if (!value) {
      continue;
    }
    const trimmed = value.trim();
    if (trimmed.length === 0) {
      continue;
    }
    unique.add(trimmed);
  }
  return [...unique];
}

function selectPrimaryMetric(topic: ParsedSummaryTopic) {
  return topic.metrics.find((metric) => metric.window === 2) ?? topic.metrics[0] ?? null;
}

function buildHighlight(topic: ParsedSummaryTopic) {
  const primaryMetric = selectPrimaryMetric(topic);
  const citations = dedupeStrings(topic.evidence.map((evidence) => evidence.url)).slice(0, 3);
  const score = primaryMetric ? primaryMetric.score.toFixed(1) : "0.0";
  const volume = primaryMetric ? Math.round(primaryMetric.volume) : topic.evidence.length;
  const acceleration = primaryMetric ? primaryMetric.acceleration.toFixed(2) : "0.00";

  return {
    topic: topic.topic,
    why_it_matters:
      citations.length > 0
        ? `${topic.topic} is sustaining measurable momentum with ${volume} recent signals.`
        : `${topic.topic} is trending, but source coverage is still thin.`,
    what_happened: `${topic.topic} reached score ${score} with volume ${volume} and acceleration ${acceleration}.`,
    suggested_action:
      citations.length > 0
        ? `Review ${citations[0]} and validate whether this trend impacts current priorities.`
        : "Monitor this topic and wait for stronger evidence before acting.",
    citations,
  };
}

function buildSuccessResult(
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number
) {
  const budgetMaxTopics = request.budget?.maxTopics ?? request.topics.length;
  const topics = request.topics.slice(0, budgetMaxTopics);
  const highlights = topics.map((topic) => buildHighlight(topic));
  const totalCitations = highlights.reduce((sum, highlight) => sum + highlight.citations.length, 0);
  const inputTokens = estimateTokenCount(JSON.stringify(request));
  const outputTokens = estimateTokenCount(JSON.stringify(highlights));
  const window = request.type === "daily" ? 1 : 2;
  const missingCitationTopics = highlights
    .filter((highlight) => highlight.citations.length === 0)
    .map((highlight) => highlight.topic);
  const notes =
    missingCitationTopics.length > 0
      ? `Limited coverage for topics: ${missingCitationTopics.join(", ")}.`
      : "All highlights include source citations.";

  return {
    payload: {
      request_id: request.requestId,
      produced_at: producedAt.toISOString(),
      brief: {
        brief_id: `brief:${request.requestId}`,
        generated_at: producedAt.toISOString(),
        window,
        title: `Trend Brief ${producedAt.toISOString().slice(0, 10)}`,
        highlights,
        notes,
        meta: {
          provider: "internal",
          model: "rule-based-v1",
          input_tokens: inputTokens,
          output_tokens: outputTokens,
          estimated_cost_usd: estimatedCostUsd,
        },
      },
    },
    metrics: {
      highlightsCount: highlights.length,
      citationsCount: totalCitations,
      inputTokens,
      outputTokens,
    },
  };
}

function buildFailureResult(
  requestId: string,
  producedAt: Date,
  code: string,
  message: string,
  retryable: boolean
) {
  return {
    request_id: requestId,
    produced_at: producedAt.toISOString(),
    failure: {
      error_code: code,
      error_message: message,
      retryable,
    },
  };
}

function isDuplicateKeyError(error: unknown): boolean {
  return error instanceof Prisma.PrismaClientKnownRequestError && error.code === "P2002";
}

async function persistResult(
  prisma: PrismaClient,
  payload: Record<string, unknown>,
  status: BriefStatus
): Promise<"created" | "duplicate"> {
  try {
    await prisma.briefResult.create({
      data: {
        requestId: payload.request_id as string,
        producedAt: new Date(payload.produced_at as string),
        status,
        result: payload as Prisma.InputJsonValue,
      },
    });
    return "created";
  } catch (error) {
    if (isDuplicateKeyError(error)) {
      return "duplicate";
    }
    throw error;
  }
}

async function readSpentBudgetUsd(redis: Redis, dateKey: string): Promise<number> {
  const raw = await redis.get(getBudgetKey(dateKey));
  if (!raw) {
    return 0;
  }
  const parsed = Number.parseFloat(raw);
  return Number.isFinite(parsed) ? parsed : 0;
}

async function recordBudgetSpendUsd(redis: Redis, dateKey: string, amount: number): Promise<number> {
  const key = getBudgetKey(dateKey);
  const nextRaw = await redis.incrbyfloat(key, amount);
  await redis.expire(key, BUDGET_KEY_TTL_SECONDS);
  const parsed = Number.parseFloat(nextRaw);
  return Number.isFinite(parsed) ? parsed : 0;
}

async function emitFailureResult(
  ctx: ProcessContext,
  requestId: string,
  producedAt: Date,
  code: string,
  message: string,
  retryable: boolean
): Promise<void> {
  const failureResult = buildFailureResult(requestId, producedAt, code, message, retryable);
  const persisted = await persistResult(ctx.prisma, failureResult, BriefStatus.failure);
  ctx.healthContext.postgresHealthy = true;
  if (persisted === "duplicate") {
    incrementDuplicatesSkipped(ctx.healthContext);
    return;
  }

  await publishBriefResult(
    ctx.producer,
    ctx.config.KAFKA_TOPIC_SUMMARY_RESULTS,
    requestId,
    Buffer.from(JSON.stringify(failureResult), "utf-8"),
    ctx.logger
  );
}

export async function processSummaryRequest(
  ctx: ProcessContext,
  request: ParsedSummaryRequest
): Promise<void> {
  const logger = ctx.logger.child({ requestId: request.requestId });
  const producedAt = new Date();

  try {
    const existing = await ctx.prisma.briefResult.findUnique({
      where: { requestId: request.requestId },
      select: { requestId: true },
    });
    ctx.healthContext.postgresHealthy = true;
    if (existing) {
      incrementDuplicatesSkipped(ctx.healthContext);
      incrementGeneration(ctx.healthContext, "skipped");
      logger.info("Skipping duplicate summary request");
      return;
    }
  } catch (error) {
    ctx.healthContext.postgresHealthy = false;
    incrementError(ctx.healthContext, "postgres_error");
    logger.error({ error: serializeError(error) }, "Failed to check brief idempotency");
    throw error;
  }

  const dateKey = getBudgetDateKey(request.requestedAt);
  const dailyBudgetUsd = request.budget?.dailyBudgetUsd || ctx.config.LLM_DAILY_BUDGET_USD;
  const estimatedCostUsd = estimateRequestCostUsd(request);

  let spentBudgetUsd: number;
  try {
    spentBudgetUsd = await readSpentBudgetUsd(ctx.redis, dateKey);
    ctx.healthContext.redisHealthy = true;
  } catch (error) {
    ctx.healthContext.redisHealthy = false;
    incrementError(ctx.healthContext, "redis_error");
    logger.error({ error: serializeError(error) }, "Failed to read daily budget usage");
    throw error;
  }

  setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - spentBudgetUsd));

  if (spentBudgetUsd + estimatedCostUsd > dailyBudgetUsd) {
    incrementBudgetExceeded(ctx.healthContext);
    incrementGeneration(ctx.healthContext, "skipped");
    try {
      await emitFailureResult(
        ctx,
        request.requestId,
        producedAt,
        "budget_exceeded",
        "Daily brief budget exceeded",
        false
      );
      logger.info(
        {
          spentBudgetUsd,
          estimatedCostUsd,
          dailyBudgetUsd,
        },
        "Skipped summary request due to budget limit"
      );
    } catch (error) {
      incrementError(ctx.healthContext, "publish_error");
      logger.error({ error: serializeError(error) }, "Failed to emit budget-exceeded brief result");
      throw error;
    }
    return;
  }

  const successResult = buildSuccessResult(request, producedAt, estimatedCostUsd);
  try {
    const persisted = await persistResult(ctx.prisma, successResult.payload, BriefStatus.success);
    ctx.healthContext.postgresHealthy = true;
    if (persisted === "duplicate") {
      incrementDuplicatesSkipped(ctx.healthContext);
      incrementGeneration(ctx.healthContext, "skipped");
      logger.info("Detected duplicate summary request during persist");
      return;
    }

    await publishBriefResult(
      ctx.producer,
      ctx.config.KAFKA_TOPIC_SUMMARY_RESULTS,
      request.requestId,
      Buffer.from(JSON.stringify(successResult.payload), "utf-8"),
      logger
    );
    await recordBudgetSpendUsd(ctx.redis, dateKey, estimatedCostUsd);
    ctx.healthContext.redisHealthy = true;

    incrementGeneration(ctx.healthContext, "success");
    incrementLlmCostUsd(ctx.healthContext, estimatedCostUsd);
    incrementLlmTokens(ctx.healthContext, "input", successResult.metrics.inputTokens);
    incrementLlmTokens(ctx.healthContext, "output", successResult.metrics.outputTokens);
    observeHighlightsCount(ctx.healthContext, successResult.metrics.highlightsCount);
    observeCitationsCount(ctx.healthContext, successResult.metrics.citationsCount);

    const updatedSpend = spentBudgetUsd + estimatedCostUsd;
    setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - updatedSpend));
    logger.info(
      {
        topicCount: successResult.metrics.highlightsCount,
        citationsCount: successResult.metrics.citationsCount,
        estimatedCostUsd,
      },
      "Summary request processed"
    );
  } catch (error) {
    incrementError(ctx.healthContext, "generation_error");
    incrementGeneration(ctx.healthContext, "failure");
    logger.error({ error: serializeError(error) }, "Failed to process summary request");
    try {
      await emitFailureResult(
        ctx,
        request.requestId,
        producedAt,
        "processing_error",
        error instanceof Error ? error.message : "Unknown brief processing error",
        true
      );
    } catch (emitError) {
      incrementError(ctx.healthContext, "publish_error");
      logger.error({ error: serializeError(emitError) }, "Failed to emit failure brief result");
      throw emitError;
    }
  }
}
