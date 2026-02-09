import { BriefStatus, Prisma, type PrismaClient } from "@rising-intelligence/db";
import type { Producer } from "kafkajs";
import type { Redis } from "ioredis";
import { serializeError } from "@rising-intelligence/shared";
import type pino from "pino";
import { z } from "zod";
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
const BUDGET_RESERVATION_SCRIPT = `
local key = KEYS[1]
local max_budget = tonumber(ARGV[1])
local amount = tonumber(ARGV[2])
local ttl_seconds = tonumber(ARGV[3])

local current = tonumber(redis.call("GET", key) or "0")
if (current + amount) > max_budget then
  return {0, tostring(current)}
end

local next = redis.call("INCRBYFLOAT", key, amount)
redis.call("EXPIRE", key, ttl_seconds)
return {1, tostring(next)}
`;
const BUDGET_RELEASE_SCRIPT = `
local key = KEYS[1]
local amount = tonumber(ARGV[1])
local ttl_seconds = tonumber(ARGV[2])

local current = tonumber(redis.call("GET", key) or "0")
local next = current - amount
if next < 0 then
  next = 0
end

redis.call("SET", key, tostring(next))
redis.call("EXPIRE", key, ttl_seconds)
return tostring(next)
`;

const LlmHighlightSchema = z.object({
  topic: z.string().min(1),
  what_happened: z.string().min(1),
  why_it_matters: z.string().min(1),
  suggested_action: z.string().min(1),
  citations: z.array(z.string().url()).min(1),
});

const LlmResponseSchema = z.object({
  title: z.string().min(1),
  highlights: z.array(LlmHighlightSchema).default([]),
  notes: z.string().optional(),
  usage: z
    .object({
      prompt_tokens: z.number().int().nonnegative(),
      completion_tokens: z.number().int().nonnegative(),
    })
    .optional(),
  meta: z
    .object({
      provider: z.string().min(1).optional(),
      model: z.string().min(1).optional(),
      estimated_cost_usd: z.number().nonnegative().optional(),
    })
    .optional(),
});

type NormalizedHighlight = z.infer<typeof LlmHighlightSchema>;

interface SuccessResult {
  payload: {
    request_id: string;
    produced_at: string;
    brief: {
      brief_id: string;
      generated_at: string;
      window: number;
      title: string;
      highlights: NormalizedHighlight[];
      notes: string;
      meta: {
        provider: string;
        model: string;
        input_tokens: number;
        output_tokens: number;
        estimated_cost_usd: number;
      };
    };
  };
  metrics: {
    highlightsCount: number;
    citationsCount: number;
    inputTokens: number;
    outputTokens: number;
    estimatedCostUsd: number;
  };
}

class LlmGenerationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LlmGenerationError";
  }
}

class NonRetryableProcessingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NonRetryableProcessingError";
  }
}

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

function canonicalizeUrl(value: string): string | null {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }

  try {
    const url = new URL(trimmed);
    url.hash = "";
    url.hostname = url.hostname.toLowerCase();
    if (url.pathname.length > 1 && url.pathname.endsWith("/")) {
      url.pathname = url.pathname.slice(0, -1);
    }
    return url.toString();
  } catch {
    return null;
  }
}

function dedupeCanonicalUrls(values: Array<string | null>): string[] {
  const unique = new Set<string>();
  for (const value of values) {
    if (!value) {
      continue;
    }
    const canonical = canonicalizeUrl(value);
    if (!canonical) {
      continue;
    }
    unique.add(canonical);
  }
  return [...unique];
}

function createEvidenceUrlSet(request: ParsedSummaryRequest): Set<string> {
  const urls = request.topics.flatMap((topic) => topic.evidence.map((evidence) => evidence.url));
  return new Set(dedupeCanonicalUrls(urls));
}

function filterGroundedCitations(citations: string[], evidenceUrls: Set<string>): string[] {
  const filtered = dedupeCanonicalUrls(citations);
  return filtered.filter((citation) => evidenceUrls.has(citation)).slice(0, 3);
}

function selectPrimaryMetric(topic: ParsedSummaryTopic) {
  return topic.metrics.find((metric) => metric.window === 2) ?? topic.metrics[0] ?? null;
}

function buildInternalHighlight(topic: ParsedSummaryTopic): NormalizedHighlight {
  const primaryMetric = selectPrimaryMetric(topic);
  const citations = dedupeCanonicalUrls(topic.evidence.map((evidence) => evidence.url)).slice(0, 3);
  const score = primaryMetric ? primaryMetric.score.toFixed(1) : "0.0";
  const volume = primaryMetric ? Math.round(primaryMetric.volume) : topic.evidence.length;
  const acceleration = primaryMetric ? primaryMetric.acceleration.toFixed(2) : "0.00";

  return {
    topic: topic.topic,
    why_it_matters: `${topic.topic} is sustaining measurable momentum with ${volume} recent signals.`,
    what_happened: `${topic.topic} reached score ${score} with volume ${volume} and acceleration ${acceleration}.`,
    suggested_action: `Review ${citations[0] ?? "the supporting sources"} and validate whether this trend impacts current priorities.`,
    citations,
  };
}

function normalizeLlmHighlight(highlight: NormalizedHighlight): NormalizedHighlight {
  return {
    topic: highlight.topic.trim(),
    what_happened: highlight.what_happened.trim(),
    why_it_matters: highlight.why_it_matters.trim(),
    suggested_action: highlight.suggested_action.trim(),
    citations: dedupeCanonicalUrls(highlight.citations).slice(0, 3),
  };
}

function deriveDefaultNotes(highlights: NormalizedHighlight[]): string {
  return highlights.length === 0
    ? "No grounded highlights were produced for this request."
    : "All highlights include source citations.";
}

function buildSummaryRequestPayload(request: ParsedSummaryRequest): Record<string, unknown> {
  return {
    request_id: request.requestId,
    requested_at: request.requestedAt.toISOString(),
    type: request.type,
    windows: request.windows,
    topics: request.topics.map((topic) => ({
      topic: topic.topic,
      metrics: topic.metrics.map((metric) => ({
        topic: metric.topic,
        window: metric.window,
        score: metric.score,
        volume: metric.volume,
        acceleration: metric.acceleration,
      })),
      evidence: topic.evidence.map((evidence) => ({
        event_id: evidence.eventId,
        source: evidence.source,
        url: evidence.url ?? "",
        title: evidence.title ?? "",
        published_at: evidence.publishedAt ? evidence.publishedAt.toISOString() : "",
        fetched_at: evidence.fetchedAt ? evidence.fetchedAt.toISOString() : "",
        text_excerpt: evidence.textExcerpt ?? "",
      })),
    })),
    budget: request.budget
      ? {
          daily_budget_usd: request.budget.dailyBudgetUsd,
          max_topics: request.budget.maxTopics,
          max_evidence_per_topic: request.budget.maxEvidencePerTopic,
          max_output_tokens: request.budget.maxOutputTokens,
        }
      : null,
  };
}

async function callHttpLlm(
  config: Config,
  request: ParsedSummaryRequest,
  logger: pino.Logger
): Promise<z.infer<typeof LlmResponseSchema>> {
  let response: Response;
  try {
    response = await fetch(config.LLM_ENDPOINT_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(buildSummaryRequestPayload(request)),
      signal: AbortSignal.timeout(config.LLM_TIMEOUT_MS),
    });
  } catch (error) {
    throw new LlmGenerationError(
      `LLM endpoint request failed: ${error instanceof Error ? error.message : "unknown error"}`
    );
  }

  if (!response.ok) {
    const responseBody = await response.text();
    logger.warn(
      {
        status: response.status,
        body: responseBody.slice(0, 512),
      },
      "LLM endpoint returned non-2xx status"
    );
    throw new LlmGenerationError(`LLM endpoint returned HTTP ${response.status}`);
  }

  let decoded: unknown;
  try {
    decoded = await response.json();
  } catch (error) {
    throw new LlmGenerationError(
      `LLM endpoint returned invalid JSON: ${error instanceof Error ? error.message : "unknown error"}`
    );
  }

  const parsed = LlmResponseSchema.safeParse(decoded);
  if (!parsed.success) {
    throw new LlmGenerationError(`LLM endpoint response validation failed: ${parsed.error.issues[0]?.message}`);
  }

  return parsed.data;
}

function buildSuccessPayload(
  request: ParsedSummaryRequest,
  producedAt: Date,
  title: string,
  highlights: NormalizedHighlight[],
  notes: string,
  provider: string,
  model: string,
  inputTokens: number,
  outputTokens: number,
  estimatedCostUsd: number
): SuccessResult {
  const window = request.type === "daily" ? 1 : 2;
  const totalCitations = highlights.reduce((sum, highlight) => sum + highlight.citations.length, 0);

  return {
    payload: {
      request_id: request.requestId,
      produced_at: producedAt.toISOString(),
      brief: {
        brief_id: `brief:${request.requestId}`,
        generated_at: producedAt.toISOString(),
        window,
        title,
        highlights,
        notes,
        meta: {
          provider,
          model,
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
      estimatedCostUsd,
    },
  };
}

function enforceGroundedHighlights(
  request: ParsedSummaryRequest,
  highlights: NormalizedHighlight[]
): NormalizedHighlight[] {
  const evidenceUrls = createEvidenceUrlSet(request);
  if (evidenceUrls.size === 0) {
    throw new NonRetryableProcessingError("No evidence URLs were provided in the summary request");
  }

  const groundedHighlights = highlights
    .map((highlight) => ({
      ...highlight,
      citations: filterGroundedCitations(highlight.citations, evidenceUrls),
    }))
    .filter((highlight) => highlight.citations.length > 0);

  if (groundedHighlights.length === 0) {
    throw new NonRetryableProcessingError(
      "Brief generation produced no grounded highlights with valid evidence citations"
    );
  }

  return groundedHighlights;
}

function buildInternalSuccessResult(
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number
): SuccessResult {
  const budgetMaxTopics = request.budget?.maxTopics ?? request.topics.length;
  const topics = request.topics.slice(0, budgetMaxTopics);
  const highlights = enforceGroundedHighlights(
    request,
    topics.map((topic) => buildInternalHighlight(topic))
  );
  const inputTokens = estimateTokenCount(JSON.stringify(request));
  const outputTokens = estimateTokenCount(JSON.stringify(highlights));
  const notes = deriveDefaultNotes(highlights);

  return buildSuccessPayload(
    request,
    producedAt,
    `Trend Brief ${producedAt.toISOString().slice(0, 10)}`,
    highlights,
    notes,
    "internal",
    "rule-based-v1",
    inputTokens,
    outputTokens,
    estimatedCostUsd
  );
}

async function buildHttpSuccessResult(
  ctx: ProcessContext,
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number
): Promise<SuccessResult> {
  const llmResponse = await callHttpLlm(ctx.config, request, ctx.logger);
  const maxTopics = request.budget?.maxTopics ?? llmResponse.highlights.length;
  const highlights = enforceGroundedHighlights(
    request,
    llmResponse.highlights.slice(0, maxTopics).map(normalizeLlmHighlight)
  );
  const inputTokens = llmResponse.usage?.prompt_tokens ?? estimateTokenCount(JSON.stringify(request));
  const outputTokens =
    llmResponse.usage?.completion_tokens ?? estimateTokenCount(JSON.stringify(highlights));
  const notes = llmResponse.notes?.trim() || deriveDefaultNotes(highlights);

  return buildSuccessPayload(
    request,
    producedAt,
    llmResponse.title,
    highlights,
    notes,
    llmResponse.meta?.provider?.trim() || "http",
    llmResponse.meta?.model?.trim() || "http-v1",
    inputTokens,
    outputTokens,
    estimatedCostUsd
  );
}

async function buildSuccessResult(
  ctx: ProcessContext,
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number
): Promise<SuccessResult> {
  if (ctx.config.LLM_PROVIDER === "internal") {
    return buildInternalSuccessResult(request, producedAt, estimatedCostUsd);
  }
  if (ctx.config.LLM_PROVIDER === "http") {
    return buildHttpSuccessResult(ctx, request, producedAt, estimatedCostUsd);
  }

  throw new LlmGenerationError(`Unsupported LLM provider: ${ctx.config.LLM_PROVIDER}`);
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

function toNumeric(value: unknown): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }
  if (typeof value === "string") {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function parseBudgetReservationResult(result: unknown): { reserved: boolean; spentUsd: number } {
  if (!Array.isArray(result) || result.length < 2) {
    throw new Error("Unexpected Redis budget reservation response");
  }

  const reserved = toNumeric(result[0]) === 1;
  const spentUsd = toNumeric(result[1]);
  return { reserved, spentUsd };
}

async function reserveBudgetSpendUsd(
  redis: Redis,
  dateKey: string,
  dailyBudgetUsd: number,
  amountUsd: number
): Promise<{ reserved: boolean; spentUsd: number }> {
  const key = getBudgetKey(dateKey);
  const result = await redis.eval(
    BUDGET_RESERVATION_SCRIPT,
    1,
    key,
    dailyBudgetUsd.toString(),
    amountUsd.toString(),
    BUDGET_KEY_TTL_SECONDS.toString()
  );
  return parseBudgetReservationResult(result);
}

async function releaseBudgetReservationUsd(
  redis: Redis,
  dateKey: string,
  amountUsd: number
): Promise<number> {
  const key = getBudgetKey(dateKey);
  const result = await redis.eval(
    BUDGET_RELEASE_SCRIPT,
    1,
    key,
    amountUsd.toString(),
    BUDGET_KEY_TTL_SECONDS.toString()
  );
  return toNumeric(result);
}

function asResultPayload(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Persisted brief result payload is not an object");
  }

  const payload = value as Record<string, unknown>;
  if (typeof payload.request_id !== "string" || payload.request_id.length === 0) {
    throw new Error("Persisted brief result payload is missing request_id");
  }
  if (typeof payload.produced_at !== "string" || payload.produced_at.length === 0) {
    throw new Error("Persisted brief result payload is missing produced_at");
  }
  return payload;
}

async function loadPersistedResult(
  prisma: PrismaClient,
  requestId: string
): Promise<{ status: BriefStatus; payload: Record<string, unknown> } | null> {
  const existing = await prisma.briefResult.findUnique({
    where: { requestId },
    select: { status: true, result: true },
  });
  if (!existing) {
    return null;
  }

  return {
    status: existing.status,
    payload: asResultPayload(existing.result),
  };
}

async function republishPersistedResult(
  ctx: ProcessContext,
  requestId: string,
  logger: pino.Logger
): Promise<BriefStatus | null> {
  const existing = await loadPersistedResult(ctx.prisma, requestId);
  ctx.healthContext.postgresHealthy = true;
  if (!existing) {
    return null;
  }

  await publishBriefResult(
    ctx.producer,
    ctx.config.KAFKA_TOPIC_SUMMARY_RESULTS,
    requestId,
    Buffer.from(JSON.stringify(existing.payload), "utf-8"),
    logger
  );
  return existing.status;
}

async function rollbackBudgetReservation(
  ctx: ProcessContext,
  logger: pino.Logger,
  dateKey: string,
  reservedAmountUsd: number,
  dailyBudgetUsd: number
): Promise<void> {
  const spentBudgetUsd = await releaseBudgetReservationUsd(ctx.redis, dateKey, reservedAmountUsd);
  ctx.healthContext.redisHealthy = true;
  setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - spentBudgetUsd));
  logger.info({ spentBudgetUsd }, "Rolled back brief budget reservation");
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
    const republishedStatus = await republishPersistedResult(ctx, requestId, ctx.logger);
    if (!republishedStatus) {
      throw new Error(`Unable to republish existing failure result for request ${requestId}`);
    }
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
  let existingResult: { status: BriefStatus; payload: Record<string, unknown> } | null = null;

  try {
    existingResult = await loadPersistedResult(ctx.prisma, request.requestId);
    ctx.healthContext.postgresHealthy = true;
  } catch (error) {
    ctx.healthContext.postgresHealthy = false;
    incrementError(ctx.healthContext, "idempotency_error");
    logger.error({ error: serializeError(error) }, "Failed to load persisted brief result");
    throw error;
  }

  if (existingResult) {
    incrementDuplicatesSkipped(ctx.healthContext);
    incrementGeneration(ctx.healthContext, "skipped");
    try {
      await publishBriefResult(
        ctx.producer,
        ctx.config.KAFKA_TOPIC_SUMMARY_RESULTS,
        request.requestId,
        Buffer.from(JSON.stringify(existingResult.payload), "utf-8"),
        logger
      );
      logger.info({ status: existingResult.status }, "Republished persisted brief result for duplicate request");
      return;
    } catch (error) {
      incrementError(ctx.healthContext, "publish_error");
      logger.error(
        { error: serializeError(error) },
        "Failed to republish persisted brief result for duplicate request"
      );
      throw error;
    }
  }

  const dateKey = getBudgetDateKey(request.requestedAt);
  const dailyBudgetUsd = request.budget?.dailyBudgetUsd ?? ctx.config.LLM_DAILY_BUDGET_USD;
  const estimatedCostUsd = estimateRequestCostUsd(request);
  let budgetReserved = false;
  let spentBudgetUsd = 0;

  try {
    const reservation = await reserveBudgetSpendUsd(
      ctx.redis,
      dateKey,
      dailyBudgetUsd,
      estimatedCostUsd
    );
    budgetReserved = reservation.reserved;
    spentBudgetUsd = reservation.spentUsd;
    ctx.healthContext.redisHealthy = true;
    setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - spentBudgetUsd));
  } catch (error) {
    ctx.healthContext.redisHealthy = false;
    incrementError(ctx.healthContext, "redis_error");
    logger.error({ error: serializeError(error) }, "Failed to reserve daily budget");
    throw error;
  }

  if (!budgetReserved) {
    incrementBudgetExceeded(ctx.healthContext);
    incrementGeneration(ctx.healthContext, "skipped");
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
    return;
  }

  let persistedCreated = false;
  try {
    const successResult = await buildSuccessResult(ctx, request, producedAt, estimatedCostUsd);
    const persisted = await persistResult(ctx.prisma, successResult.payload, BriefStatus.success);
    ctx.healthContext.postgresHealthy = true;
    if (persisted === "duplicate") {
      await rollbackBudgetReservation(ctx, logger, dateKey, estimatedCostUsd, dailyBudgetUsd);
      budgetReserved = false;
      incrementDuplicatesSkipped(ctx.healthContext);
      incrementGeneration(ctx.healthContext, "skipped");
      const republishedStatus = await republishPersistedResult(ctx, request.requestId, logger);
      if (!republishedStatus) {
        throw new Error(`Persisted result missing after duplicate insert for request ${request.requestId}`);
      }
      logger.info({ status: republishedStatus }, "Detected duplicate during persist and republished stored result");
      return;
    }
    persistedCreated = true;

    await publishBriefResult(
      ctx.producer,
      ctx.config.KAFKA_TOPIC_SUMMARY_RESULTS,
      request.requestId,
      Buffer.from(JSON.stringify(successResult.payload), "utf-8"),
      logger
    );

    incrementGeneration(ctx.healthContext, "success");
    incrementLlmCostUsd(ctx.healthContext, successResult.metrics.estimatedCostUsd);
    incrementLlmTokens(ctx.healthContext, "input", successResult.metrics.inputTokens);
    incrementLlmTokens(ctx.healthContext, "output", successResult.metrics.outputTokens);
    observeHighlightsCount(ctx.healthContext, successResult.metrics.highlightsCount);
    observeCitationsCount(ctx.healthContext, successResult.metrics.citationsCount);

    setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - spentBudgetUsd));
    logger.info(
      {
        topicCount: successResult.metrics.highlightsCount,
        citationsCount: successResult.metrics.citationsCount,
        estimatedCostUsd: successResult.metrics.estimatedCostUsd,
      },
      "Summary request processed"
    );
  } catch (error) {
    if (budgetReserved && !persistedCreated) {
      try {
        await rollbackBudgetReservation(ctx, logger, dateKey, estimatedCostUsd, dailyBudgetUsd);
        budgetReserved = false;
      } catch (rollbackError) {
        ctx.healthContext.redisHealthy = false;
        incrementError(ctx.healthContext, "redis_error");
        logger.error(
          { error: serializeError(rollbackError) },
          "Failed to roll back reserved brief budget"
        );
        throw rollbackError;
      }
    }

    if (error instanceof NonRetryableProcessingError) {
      incrementError(ctx.healthContext, "grounding_error");
      incrementGeneration(ctx.healthContext, "failure");
      logger.warn({ error: serializeError(error) }, "Brief request failed non-retryable validation");
      await emitFailureResult(
        ctx,
        request.requestId,
        producedAt,
        "grounding_error",
        error.message,
        false
      );
      return;
    }

    if (persistedCreated) {
      incrementError(ctx.healthContext, "publish_error");
      logger.error(
        { error: serializeError(error) },
        "Persisted brief result but failed to publish; will retry from Kafka"
      );
      throw error;
    }

    incrementError(
      ctx.healthContext,
      error instanceof LlmGenerationError ? "llm_error" : "generation_error"
    );
    incrementGeneration(ctx.healthContext, "failure");
    logger.error({ error: serializeError(error) }, "Failed to process summary request");
    throw error;
  }
}
