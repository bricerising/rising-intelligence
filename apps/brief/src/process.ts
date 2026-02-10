import { BriefStatus, Prisma, TrendWindow, type PrismaClient } from "@rising-intelligence/db";
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
import { executeCodexCli } from "./llm/codex-cli.js";
import { publishBriefResult } from "./kafka/producer.js";
import type { ParsedSummaryRequest, ParsedSummaryTopic } from "./types.js";
import { compileTopicGlobMatchers, matchesAnyTopicGlob } from "./topic-glob.js";
import type { EvidenceStrategy } from "./types.js";
import { Source } from "@rising-intelligence/db";

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
const TREND_WINDOW_60M_PROTO = 2;
const DEFAULT_QUERY_TOPIC_GLOBS = ["*"];
const DEFAULT_QUERY_MAX_TOPICS = 10;
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
const BUDGET_SETTLE_SCRIPT = `
local key = KEYS[1]
local delta = tonumber(ARGV[1])
local ttl_seconds = tonumber(ARGV[2])

local current = tonumber(redis.call("GET", key) or "0")
local next = current + delta
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

const TrendSnapshotTopicSchema = z.object({
  topic: z.string().min(1),
  score: z.coerce.number().default(0),
  volume: z.coerce.number().default(0),
  acceleration: z.coerce.number().default(0),
});

const TrendSnapshotPayloadSchema = z.object({
  topics: z.array(TrendSnapshotTopicSchema).default([]),
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
    costUsd: number;
  };
}

class LlmGenerationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LlmGenerationError";
  }
}

class NonRetryableProcessingError extends Error {
  public readonly code: string;

  constructor(message: string, code = "grounding_error") {
    super(message);
    this.name = "NonRetryableProcessingError";
    this.code = code;
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

function isQueryModeRequest(request: ParsedSummaryRequest): boolean {
  return request.topics.length === 0;
}

function resolveLookbackDays(config: Config, request: ParsedSummaryRequest): number {
  const lookbackDays = request.query?.lookbackDays ?? config.BRIEF_DEFAULT_LOOKBACK_DAYS;
  if (!Number.isInteger(lookbackDays) || lookbackDays <= 0) {
    throw new NonRetryableProcessingError(
      `Invalid query.lookback_days: ${lookbackDays}`,
      "invalid_request"
    );
  }
  if (lookbackDays > config.BRIEF_MAX_LOOKBACK_DAYS) {
    throw new NonRetryableProcessingError(
      `query.lookback_days must be <= ${config.BRIEF_MAX_LOOKBACK_DAYS}`,
      "invalid_request"
    );
  }
  return lookbackDays;
}

function resolveTopicGlobs(request: ParsedSummaryRequest): string[] {
  const topicGlobs = request.query?.topicGlobs;
  if (!topicGlobs || topicGlobs.length === 0) {
    return DEFAULT_QUERY_TOPIC_GLOBS;
  }
  return topicGlobs;
}

function resolveMaxTopics(request: ParsedSummaryRequest): number {
  const maxTopics = request.budget?.maxTopics ?? DEFAULT_QUERY_MAX_TOPICS;
  if (!Number.isInteger(maxTopics) || maxTopics <= 0) {
    throw new NonRetryableProcessingError(
      `Invalid max_topics budget value: ${maxTopics}`,
      "invalid_request"
    );
  }
  return maxTopics;
}

function resolveMaxEventsPerTopic(config: Config, request: ParsedSummaryRequest): number {
  const budgetCap = request.budget?.maxEvidencePerTopic;
  const requested = request.query?.maxEventsPerTopic ?? budgetCap ?? config.BRIEF_MAX_QUERY_EVENTS_PER_TOPIC;
  if (!Number.isInteger(requested) || requested <= 0) {
    throw new NonRetryableProcessingError(
      `Invalid query.max_events_per_topic: ${requested}`,
      "invalid_request"
    );
  }

  let resolved = requested;
  if (budgetCap && Number.isInteger(budgetCap) && budgetCap > 0) {
    resolved = Math.min(resolved, budgetCap);
  }
  resolved = Math.min(resolved, config.BRIEF_MAX_QUERY_EVENTS_PER_TOPIC);
  return Math.max(1, resolved);
}

const CURATED_SOURCES = new Set<Source>([Source.rss, Source.news, Source.github]);
const DISCUSSION_SOURCES = new Set<Source>([
  Source.reddit,
  Source.hackernews,
  Source.bluesky,
  Source.mastodon,
]);

interface RawEventForSelection {
  eventId: string;
  source: Source;
  fetchedAt: Date;
  engagementScore: number | null;
}

function selectEvidence<T extends RawEventForSelection>(
  events: T[],
  strategy: EvidenceStrategy,
  maxCount: number
): T[] {
  if (events.length === 0) {
    return [];
  }

  if (strategy === "recency") {
    // Already ordered by fetchedAt desc from query
    return events.slice(0, maxCount);
  }

  if (strategy === "engagement") {
    const sorted = [...events].sort((a, b) => {
      const scoreA = a.engagementScore ?? 0;
      const scoreB = b.engagementScore ?? 0;
      if (scoreB !== scoreA) {
        return scoreB - scoreA;
      }
      // Tie-break by recency
      return b.fetchedAt.getTime() - a.fetchedAt.getTime();
    });
    return sorted.slice(0, maxCount);
  }

  // Diversity strategy
  const curated: T[] = [];
  const discussion: T[] = [];
  const other: T[] = [];

  for (const event of events) {
    if (CURATED_SOURCES.has(event.source)) {
      curated.push(event);
    } else if (DISCUSSION_SOURCES.has(event.source)) {
      discussion.push(event);
    } else {
      other.push(event);
    }
  }

  const selected: T[] = [];

  // Try to get at least 1 from curated and 1 from discussion
  if (curated.length > 0) {
    selected.push(curated[0]);
  }
  if (discussion.length > 0 && selected.length < maxCount) {
    selected.push(discussion[0]);
  }

  // Fill remaining slots by engagement score across all categories
  const remaining: T[] = [];
  if (selected.length < curated.length) {
    remaining.push(...curated.slice(selected.includes(curated[0]) ? 1 : 0));
  }
  if (selected.length < discussion.length) {
    remaining.push(...discussion.slice(selected.includes(discussion[0]) ? 1 : 0));
  }
  remaining.push(...other);

  remaining.sort((a, b) => {
    const scoreA = a.engagementScore ?? 0;
    const scoreB = b.engagementScore ?? 0;
    if (scoreB !== scoreA) {
      return scoreB - scoreA;
    }
    return b.fetchedAt.getTime() - a.fetchedAt.getTime();
  });

  const slotsRemaining = maxCount - selected.length;
  selected.push(...remaining.slice(0, slotsRemaining));

  return selected;
}

function computeRecentWeight(snapshotGeneratedAt: Date, requestedAt: Date): number {
  const ageMs = Math.max(0, requestedAt.getTime() - snapshotGeneratedAt.getTime());
  const ageHours = ageMs / (60 * 60 * 1000);
  return 1 / (1 + ageHours);
}

interface RankedTopicAccumulator {
  weightedScore: number;
  weightedVolume: number;
  weightedAcceleration: number;
  weightSum: number;
  latestGeneratedAtMs: number;
}

interface RankedTopicScore {
  topic: string;
  score: number;
  volume: number;
  acceleration: number;
  latestGeneratedAtMs: number;
}

function rankTopicsFromSnapshots(
  snapshots: Array<{ generatedAt: Date; snapshot: Prisma.JsonValue }>,
  requestedAt: Date,
  topicMatchers: RegExp[],
  maxTopics: number
): RankedTopicScore[] {
  const byTopic = new Map<string, RankedTopicAccumulator>();

  for (const row of snapshots) {
    const parsedSnapshot = TrendSnapshotPayloadSchema.safeParse(row.snapshot);
    if (!parsedSnapshot.success) {
      continue;
    }

    const weight = computeRecentWeight(row.generatedAt, requestedAt);
    const generatedAtMs = row.generatedAt.getTime();
    for (const metric of parsedSnapshot.data.topics) {
      const topic = metric.topic.trim();
      if (!topic || !matchesAnyTopicGlob(topic, topicMatchers)) {
        continue;
      }

      const existing = byTopic.get(topic) ?? {
        weightedScore: 0,
        weightedVolume: 0,
        weightedAcceleration: 0,
        weightSum: 0,
        latestGeneratedAtMs: generatedAtMs,
      };
      existing.weightedScore += metric.score * weight;
      existing.weightedVolume += metric.volume * weight;
      existing.weightedAcceleration += metric.acceleration * weight;
      existing.weightSum += weight;
      existing.latestGeneratedAtMs = Math.max(existing.latestGeneratedAtMs, generatedAtMs);
      byTopic.set(topic, existing);
    }
  }

  const rankedTopics = [...byTopic.entries()]
    .map(([topic, accumulator]) => {
      const denominator = accumulator.weightSum <= 0 ? 1 : accumulator.weightSum;
      return {
        topic,
        score: accumulator.weightedScore / denominator,
        volume: accumulator.weightedVolume / denominator,
        acceleration: accumulator.weightedAcceleration / denominator,
        latestGeneratedAtMs: accumulator.latestGeneratedAtMs,
      };
    })
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      if (right.latestGeneratedAtMs !== left.latestGeneratedAtMs) {
        return right.latestGeneratedAtMs - left.latestGeneratedAtMs;
      }
      return left.topic.localeCompare(right.topic);
    });

  return rankedTopics.slice(0, maxTopics);
}

async function buildQueryModeRequest(
  ctx: ProcessContext,
  request: ParsedSummaryRequest,
  logger: pino.Logger
): Promise<ParsedSummaryRequest> {
  const lookbackDays = resolveLookbackDays(ctx.config, request);
  const topicGlobs = resolveTopicGlobs(request);
  let topicMatchers: RegExp[];
  try {
    topicMatchers = compileTopicGlobMatchers(topicGlobs);
  } catch (error) {
    throw new NonRetryableProcessingError(
      `Invalid topic glob filter: ${error instanceof Error ? error.message : "unknown error"}`,
      "invalid_request"
    );
  }
  const maxTopics = resolveMaxTopics(request);
  const maxEventsPerTopic = resolveMaxEventsPerTopic(ctx.config, request);

  const lookbackStart = new Date(request.requestedAt.getTime() - lookbackDays * 24 * 60 * 60 * 1000);

  let snapshots: Array<{ generatedAt: Date; snapshot: Prisma.JsonValue }>;
  try {
    snapshots = await ctx.prisma.briefTrendSnapshot.findMany({
      where: {
        window: TrendWindow.WINDOW_60M,
        generatedAt: {
          gte: lookbackStart,
          lte: request.requestedAt,
        },
      },
      orderBy: {
        generatedAt: "desc",
      },
      select: {
        generatedAt: true,
        snapshot: true,
      },
    });
    ctx.healthContext.postgresHealthy = true;
  } catch (error) {
    ctx.healthContext.postgresHealthy = false;
    throw error;
  }

  const coverageWarnings: string[] = [];

  if (snapshots.length === 0) {
    logger.warn({ lookbackDays }, "No trend snapshots found in lookback window");
    coverageWarnings.push("No trend data available for the requested lookback period.");
  }

  const rankedTopics = rankTopicsFromSnapshots(
    snapshots,
    request.requestedAt,
    topicMatchers,
    maxTopics
  );

  if (rankedTopics.length === 0) {
    logger.warn(
      { topicGlobCount: topicGlobs.length, lookbackDays },
      "No topics matched query filters"
    );
    coverageWarnings.push(
      "No topics matched the specified filters. Consider broadening topic_globs or lookback window."
    );
  }

  // Fetch all evidence in a single query
  const evidenceStrategy = request.query?.evidenceStrategy ?? "diversity";
  let allEvents: Array<{
    eventId: string;
    source: Source;
    url: string | null;
    title: string | null;
    publishedAt: Date | null;
    fetchedAt: Date;
    text: string;
    topics: string[];
    engagementScore: number | null;
  }>;

  try {
    const topicKeys = rankedTopics.map((t) => t.topic);
    allEvents = await ctx.prisma.rawEvent.findMany({
      where: {
        topics: {
          hasSome: topicKeys,
        },
        fetchedAt: {
          gte: lookbackStart,
          lte: request.requestedAt,
        },
      },
      orderBy: {
        fetchedAt: "desc",
      },
      select: {
        eventId: true,
        source: true,
        url: true,
        title: true,
        publishedAt: true,
        fetchedAt: true,
        text: true,
        topics: true,
        engagementScore: true,
      },
    });
    ctx.healthContext.postgresHealthy = true;
  } catch (error) {
    ctx.healthContext.postgresHealthy = false;
    throw error;
  }

  // Partition events by topic
  const eventsByTopic = new Map<string, typeof allEvents>();
  for (const event of allEvents) {
    for (const topicKey of event.topics) {
      if (!rankedTopics.some((t) => t.topic === topicKey)) {
        continue;
      }
      const existing = eventsByTopic.get(topicKey);
      if (existing) {
        existing.push(event);
      } else {
        eventsByTopic.set(topicKey, [event]);
      }
    }
  }

  // Apply evidence selection strategy per topic
  const hydratedTopics: ParsedSummaryTopic[] = rankedTopics.map((rankedTopic) => {
    const topicEvents = eventsByTopic.get(rankedTopic.topic) ?? [];
    const selectedEvents = selectEvidence(topicEvents, evidenceStrategy, maxEventsPerTopic);

    return {
      topic: rankedTopic.topic,
      metrics: [
        {
          topic: rankedTopic.topic,
          window: TREND_WINDOW_60M_PROTO,
          score: rankedTopic.score,
          volume: rankedTopic.volume,
          acceleration: rankedTopic.acceleration,
        },
      ],
      evidence: selectedEvents.map((event) => ({
        eventId: event.eventId,
        source: event.source,
        url: event.url ?? null,
        title: event.title ?? null,
        publishedAt: event.publishedAt,
        fetchedAt: event.fetchedAt,
        textExcerpt: event.text.slice(0, 500),
      })),
    };
  });

  const topicsWithEvidence = hydratedTopics.filter((topic) => topic.evidence.length > 0);

  if (topicsWithEvidence.length === 0) {
    logger.warn(
      { rankedTopicCount: rankedTopics.length, lookbackDays },
      "No evidence found for any ranked topics"
    );
    coverageWarnings.push(
      "No recent activity found for trending topics in the lookback window."
    );
  }

  logger.info(
    {
      lookbackDays,
      topicGlobCount: topicGlobs.length,
      rankedTopicCount: rankedTopics.length,
      selectedTopicCount: topicsWithEvidence.length,
      maxEventsPerTopic,
      coverageWarningCount: coverageWarnings.length,
    },
    "Resolved query-mode summary request using trend snapshots and raw events"
  );

  return {
    ...request,
    windows: [TREND_WINDOW_60M_PROTO],
    query: {
      lookbackDays,
      topicGlobs,
      maxEventsPerTopic,
    },
    topics: topicsWithEvidence.length > 0 ? topicsWithEvidence : hydratedTopics,
    coverageWarnings,
  };
}

async function resolveRequestForGeneration(
  ctx: ProcessContext,
  request: ParsedSummaryRequest,
  logger: pino.Logger
): Promise<ParsedSummaryRequest> {
  if (!isQueryModeRequest(request)) {
    return request;
  }
  return buildQueryModeRequest(ctx, request, logger);
}

function normalizeUsd(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  if (value <= 0) {
    return 0;
  }

  return Number(value.toFixed(6));
}

function normalizeUsdDelta(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }

  const rounded = Number(value.toFixed(6));
  return rounded === 0 ? 0 : rounded;
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

function deriveDefaultNotes(request: ParsedSummaryRequest, highlights: NormalizedHighlight[]): string {
  if (highlights.length === 0) {
    return "Executive summary: Limited coverage. No grounded highlights were produced for this request.";
  }

  const lookbackDays = request.query?.lookbackDays;
  const lookbackSuffix =
    lookbackDays && lookbackDays > 0 ? ` over the last ${lookbackDays} day(s)` : "";
  const primaryTopic = highlights[0]?.topic ?? "the selected topics";
  const additionalTopics = highlights.length - 1;
  const topicTail =
    additionalTopics > 0 ? ` with ${additionalTopics} additional topic(s)` : "";

  return `Executive summary: ${primaryTopic} led the strongest grounded signals${lookbackSuffix}${topicTail}.`;
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
    query: request.query
      ? {
          lookback_days: request.query.lookbackDays,
          topic_globs: request.query.topicGlobs,
          max_events_per_topic: request.query.maxEventsPerTopic,
        }
      : null,
  };
}

function buildCodexCliPrompt(request: ParsedSummaryRequest): string {
  const payload = buildSummaryRequestPayload(request);
  const maxTopics = request.budget?.maxTopics ?? request.topics.length;
  const maxEvidencePerTopic =
    request.budget?.maxEvidencePerTopic ??
    Math.max(...request.topics.map((topic) => topic.evidence.length), 0);
  const maxOutputTokens = request.budget?.maxOutputTokens ?? 1200;

  return [
    "You are generating a human-readable engineering intelligence brief from a structured summary request.",
    "Use only the evidence included in SUMMARY_REQUEST_JSON. Do not invent facts or URLs.",
    "Each highlight must include concrete what_happened, why_it_matters, suggested_action, and citations.",
    `Keep output concise and practical. Limit highlights to at most ${maxTopics} and per-topic evidence references to at most ${maxEvidencePerTopic}.`,
    `Target no more than ${maxOutputTokens} tokens in total output.`,
    "Return valid JSON only with this shape:",
    '{ "title": string, "highlights": [{ "topic": string, "what_happened": string, "why_it_matters": string, "suggested_action": string, "citations": string[] }], "notes": string, "usage": { "prompt_tokens": number, "completion_tokens": number }, "meta": { "provider": string, "model": string, "estimated_cost_usd": number } }',
    "If usage or cost are unknown, set them to 0.",
    `SUMMARY_REQUEST_JSON:\n${JSON.stringify(payload, null, 2)}`,
  ].join("\n\n");
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

async function callCodexCliLlm(
  config: Config,
  request: ParsedSummaryRequest,
  logger: pino.Logger
): Promise<z.infer<typeof LlmResponseSchema>> {
  const prompt = buildCodexCliPrompt(request);
  let decoded: unknown;
  try {
    decoded = await executeCodexCli(config, prompt, logger);
  } catch (error) {
    throw new LlmGenerationError(
      `Codex CLI request failed: ${error instanceof Error ? error.message : "unknown error"}`
    );
  }

  const parsed = LlmResponseSchema.safeParse(decoded);
  if (!parsed.success) {
    throw new LlmGenerationError(
      `Codex CLI response validation failed: ${parsed.error.issues[0]?.message}`
    );
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
  costUsd: number
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
          estimated_cost_usd: costUsd,
        },
      },
    },
    metrics: {
      highlightsCount: highlights.length,
      citationsCount: totalCitations,
      inputTokens,
      outputTokens,
      costUsd,
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
  let notes = deriveDefaultNotes(request, highlights);

  // Append coverage warnings from query mode
  if (request.coverageWarnings && request.coverageWarnings.length > 0) {
    const warningsText = request.coverageWarnings.join(" ");
    notes = notes ? `${notes}\n\nCoverage Note: ${warningsText}` : `Coverage Note: ${warningsText}`;
  }

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
    normalizeUsd(estimatedCostUsd)
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
  let notes = llmResponse.notes?.trim() || deriveDefaultNotes(request, highlights);

  // Append coverage warnings from query mode
  if (request.coverageWarnings && request.coverageWarnings.length > 0) {
    const warningsText = request.coverageWarnings.join(" ");
    notes = notes ? `${notes}\n\nCoverage Note: ${warningsText}` : `Coverage Note: ${warningsText}`;
  }

  const costUsd = normalizeUsd(llmResponse.meta?.estimated_cost_usd ?? estimatedCostUsd);

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
    costUsd
  );
}

async function buildCodexCliSuccessResult(
  ctx: ProcessContext,
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number
): Promise<SuccessResult> {
  const llmResponse = await callCodexCliLlm(ctx.config, request, ctx.logger);
  const maxTopics = request.budget?.maxTopics ?? llmResponse.highlights.length;
  const highlights = enforceGroundedHighlights(
    request,
    llmResponse.highlights.slice(0, maxTopics).map(normalizeLlmHighlight)
  );
  const inputTokens = llmResponse.usage?.prompt_tokens ?? estimateTokenCount(JSON.stringify(request));
  const outputTokens =
    llmResponse.usage?.completion_tokens ?? estimateTokenCount(JSON.stringify(highlights));
  let notes = llmResponse.notes?.trim() || deriveDefaultNotes(request, highlights);

  // Append coverage warnings from query mode
  if (request.coverageWarnings && request.coverageWarnings.length > 0) {
    const warningsText = request.coverageWarnings.join(" ");
    notes = notes ? `${notes}\n\nCoverage Note: ${warningsText}` : `Coverage Note: ${warningsText}`;
  }

  const costUsd = normalizeUsd(llmResponse.meta?.estimated_cost_usd ?? estimatedCostUsd);
  const defaultModel = ctx.config.LLM_CODEX_MODEL.trim() || "codex-cli";

  return buildSuccessPayload(
    request,
    producedAt,
    llmResponse.title,
    highlights,
    notes,
    llmResponse.meta?.provider?.trim() || "codex-cli",
    llmResponse.meta?.model?.trim() || defaultModel,
    inputTokens,
    outputTokens,
    costUsd
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
  if (ctx.config.LLM_PROVIDER === "codex-cli") {
    return buildCodexCliSuccessResult(ctx, request, producedAt, estimatedCostUsd);
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
  prisma: PrismaClient,
  redis: Redis,
  dateKey: string,
  dailyBudgetUsd: number,
  amountUsd: number
): Promise<{ reserved: boolean; spentUsd: number }> {
  // Try Redis first (fast path)
  const key = getBudgetKey(dateKey);
  try {
    const result = await redis.eval(
      BUDGET_RESERVATION_SCRIPT,
      1,
      key,
      dailyBudgetUsd.toString(),
      amountUsd.toString(),
      BUDGET_KEY_TTL_SECONDS.toString()
    );
    const cached = parseBudgetReservationResult(result);
    if (cached.reserved) {
      // Async write to Postgres (fire and forget for performance)
      void prisma.briefBudgetTracking.upsert({
        where: { date: dateKey },
        create: {
          date: dateKey,
          spentUsd: amountUsd,
          budgetUsd: dailyBudgetUsd,
          requestCount: 1,
        },
        update: {
          spentUsd: { increment: amountUsd },
          requestCount: { increment: 1 },
        },
      });
      return cached;
    }
  } catch (redisError) {
    // Redis failed, fall back to Postgres
  }

  // Postgres slow path (or Redis returned false)
  const record = await prisma.briefBudgetTracking.upsert({
    where: { date: dateKey },
    create: {
      date: dateKey,
      spentUsd: 0,
      budgetUsd: dailyBudgetUsd,
      requestCount: 0,
    },
    update: {},
    select: { spentUsd: true },
  });

  const currentSpent = Number(record.spentUsd);
  if (currentSpent + amountUsd > dailyBudgetUsd) {
    // Sync Redis with Postgres
    await redis.set(key, currentSpent.toString(), "EX", BUDGET_KEY_TTL_SECONDS);
    return { reserved: false, spentUsd: currentSpent };
  }

  // Reserve in Postgres
  await prisma.briefBudgetTracking.update({
    where: { date: dateKey },
    data: {
      spentUsd: { increment: amountUsd },
      requestCount: { increment: 1 },
    },
  });

  const newSpent = currentSpent + amountUsd;
  // Update Redis cache
  await redis.set(key, newSpent.toString(), "EX", BUDGET_KEY_TTL_SECONDS);
  return { reserved: true, spentUsd: newSpent };
}

async function releaseBudgetReservationUsd(
  prisma: PrismaClient,
  redis: Redis,
  dateKey: string,
  amountUsd: number
): Promise<number> {
  // Update Postgres first (source of truth)
  const record = await prisma.briefBudgetTracking.findUnique({
    where: { date: dateKey },
    select: { spentUsd: true },
  });

  if (!record) {
    return 0;
  }

  const currentSpent = Number(record.spentUsd);
  const newSpent = Math.max(0, currentSpent - amountUsd);

  await prisma.briefBudgetTracking.update({
    where: { date: dateKey },
    data: { spentUsd: newSpent },
  });

  // Sync to Redis
  const key = getBudgetKey(dateKey);
  await redis.set(key, newSpent.toString(), "EX", BUDGET_KEY_TTL_SECONDS);
  return newSpent;
}

async function settleBudgetSpendUsd(
  prisma: PrismaClient,
  redis: Redis,
  dateKey: string,
  deltaUsd: number
): Promise<number> {
  // Update Postgres first (source of truth)
  const record = await prisma.briefBudgetTracking.findUnique({
    where: { date: dateKey },
    select: { spentUsd: true },
  });

  if (!record) {
    return 0;
  }

  const currentSpent = Number(record.spentUsd);
  const newSpent = Math.max(0, currentSpent + deltaUsd);

  await prisma.briefBudgetTracking.update({
    where: { date: dateKey },
    data: { spentUsd: newSpent },
  });

  // Sync to Redis
  const key = getBudgetKey(dateKey);
  await redis.set(key, newSpent.toString(), "EX", BUDGET_KEY_TTL_SECONDS);
  return newSpent;
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
  const spentBudgetUsd = await releaseBudgetReservationUsd(
    ctx.prisma,
    ctx.redis,
    dateKey,
    reservedAmountUsd
  );
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

  let requestForGeneration: ParsedSummaryRequest;
  try {
    requestForGeneration = await resolveRequestForGeneration(ctx, request, logger);
  } catch (error) {
    if (error instanceof NonRetryableProcessingError) {
      incrementError(
        ctx.healthContext,
        error.code === "grounding_error" ? "grounding_error" : "generation_error"
      );
      incrementGeneration(ctx.healthContext, "failure");
      logger.warn({ error: serializeError(error) }, "Summary request failed non-retryable pre-processing");
      await emitFailureResult(
        ctx,
        request.requestId,
        producedAt,
        error.code,
        error.message,
        false
      );
      return;
    }

    incrementError(ctx.healthContext, "generation_error");
    incrementGeneration(ctx.healthContext, "failure");
    logger.error({ error: serializeError(error) }, "Failed to resolve summary request");
    throw error;
  }

  const dateKey = getBudgetDateKey(producedAt);
  const dailyBudgetUsd = requestForGeneration.budget?.dailyBudgetUsd ?? ctx.config.LLM_DAILY_BUDGET_USD;
  const estimatedCostUsd = normalizeUsd(estimateRequestCostUsd(requestForGeneration));
  let budgetReserved = false;
  let spentBudgetUsd = 0;
  let reservedCostUsd = estimatedCostUsd;

  try {
    const reservation = await reserveBudgetSpendUsd(
      ctx.prisma,
      ctx.redis,
      dateKey,
      dailyBudgetUsd,
      reservedCostUsd
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
        reservedCostUsd,
        dailyBudgetUsd,
      },
      "Skipped summary request due to budget limit"
    );
    return;
  }

  let persistedCreated = false;
  try {
    const successResult = await buildSuccessResult(
      ctx,
      requestForGeneration,
      producedAt,
      estimatedCostUsd
    );
    const persisted = await persistResult(ctx.prisma, successResult.payload, BriefStatus.success);
    ctx.healthContext.postgresHealthy = true;
    if (persisted === "duplicate") {
      await rollbackBudgetReservation(ctx, logger, dateKey, reservedCostUsd, dailyBudgetUsd);
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

    const costDeltaUsd = normalizeUsdDelta(successResult.metrics.costUsd - reservedCostUsd);
    if (costDeltaUsd !== 0) {
      try {
        spentBudgetUsd = await settleBudgetSpendUsd(ctx.prisma, ctx.redis, dateKey, costDeltaUsd);
        reservedCostUsd = normalizeUsd(successResult.metrics.costUsd);
        ctx.healthContext.redisHealthy = true;
      } catch (error) {
        ctx.healthContext.redisHealthy = false;
        incrementError(ctx.healthContext, "redis_error");
        logger.warn(
          {
            costDeltaUsd,
            error: serializeError(error),
          },
          "Failed to settle reserved budget to final brief cost"
        );
      }
    }

    await publishBriefResult(
      ctx.producer,
      ctx.config.KAFKA_TOPIC_SUMMARY_RESULTS,
      request.requestId,
      Buffer.from(JSON.stringify(successResult.payload), "utf-8"),
      logger
    );

    incrementGeneration(ctx.healthContext, "success");
    incrementLlmCostUsd(ctx.healthContext, successResult.metrics.costUsd);
    incrementLlmTokens(ctx.healthContext, "input", successResult.metrics.inputTokens);
    incrementLlmTokens(ctx.healthContext, "output", successResult.metrics.outputTokens);
    observeHighlightsCount(ctx.healthContext, successResult.metrics.highlightsCount);
    observeCitationsCount(ctx.healthContext, successResult.metrics.citationsCount);

    setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - spentBudgetUsd));
    logger.info(
      {
        topicCount: successResult.metrics.highlightsCount,
        citationsCount: successResult.metrics.citationsCount,
        costUsd: successResult.metrics.costUsd,
      },
      "Summary request processed"
    );
  } catch (error) {
    if (budgetReserved && !persistedCreated) {
      try {
        await rollbackBudgetReservation(ctx, logger, dateKey, reservedCostUsd, dailyBudgetUsd);
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
      incrementError(
        ctx.healthContext,
        error.code === "grounding_error" ? "grounding_error" : "generation_error"
      );
      incrementGeneration(ctx.healthContext, "failure");
      logger.warn({ error: serializeError(error) }, "Brief request failed non-retryable validation");
      await emitFailureResult(
        ctx,
        request.requestId,
        producedAt,
        error.code,
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
