import { BriefStatus, Prisma, Source, TrendWindow, type PrismaClient } from "@rising-intelligence/db";
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
import {
  createSummaryRequestGroundingFacade,
  EVIDENCE_EXCERPT_MAX_LENGTH,
} from "./grounding-facade.js";
import { executeCodexCli } from "./llm/codex-cli.js";
import {
  createBriefResultPublisher,
  type BriefResultPublisher,
} from "./publishing-facade.js";
import {
  buildFailureBriefResultPayload,
  parseBriefResultPayload,
  type BriefResultPayload,
} from "./result-payload-adapter.js";
import type {
  EvidenceStrategy,
  LlmProvider,
  ParsedSummaryEvidence,
  ParsedSummaryRequest,
  ParsedSummaryTopic,
} from "./types.js";
import { compileTopicGlobMatchers, matchesAnyTopicGlob } from "./topic-glob.js";

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
const NO_COVERAGE_ERROR_CODE = "no_coverage";
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
type ParsedLlmResponse = z.infer<typeof LlmResponseSchema>;
const groundingFacade = createSummaryRequestGroundingFacade();

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

function toGroundingError(message: string): NonRetryableProcessingError {
  return new NonRetryableProcessingError(message);
}

function toNoCoverageError(message: string): NonRetryableProcessingError {
  return new NonRetryableProcessingError(message, NO_COVERAGE_ERROR_CODE);
}

function classifyRetryableFailureCode(error: LlmGenerationError): "llm_error" | "timeout" {
  const message = error.message.toLowerCase();
  if (
    message.includes("timeout") ||
    message.includes("timed out") ||
    message.includes("etimedout") ||
    message.includes("abort")
  ) {
    return "timeout";
  }

  return "llm_error";
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
  publishedAt: Date | null;
  fetchedAt: Date;
  engagementScore: number | null;
}

interface QueryModeRawEvent extends RawEventForSelection {
  url: string;
  title: string | null;
  publishedAt: Date | null;
  text: string;
  topics: string[];
}

interface EvidenceSelectionStrategy {
  readonly name: EvidenceStrategy;
  select<T extends RawEventForSelection>(events: T[], maxCount: number): T[];
}

const GENERIC_TOPIC_SEGMENTS = new Set([
  "ai",
  "cloud",
  "data",
  "devtools",
  "framework",
  "infra",
  "language",
  "ml",
  "observability",
  "platform",
  "security",
  "web",
]);

const TOPIC_RELEVANCE_ALIASES: Record<string, readonly string[]> = {
  "data.kafka": ["kafka", "redpanda"],
  "observability.opentelemetry": ["opentelemetry", "otel"],
} as const;

const TOPIC_RELEVANCE_MIN_BODY_MATCHES = 2;

interface TopicRelevanceMatcher {
  exactTermRegexes: RegExp[];
}

const topicRelevanceMatcherCache = new Map<string, TopicRelevanceMatcher | null>();

function escapeRegexLiteral(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function deriveTopicRelevanceTerms(topicKey: string): string[] {
  const segments = topicKey
    .split(/[._-]/)
    .map((segment) => segment.trim().toLowerCase())
    .filter((segment) => segment.length >= 3)
    .filter((segment) => !GENERIC_TOPIC_SEGMENTS.has(segment));
  const aliases = (TOPIC_RELEVANCE_ALIASES[topicKey] ?? []).map((term) => term.toLowerCase());
  return [...new Set([...segments, ...aliases])];
}

function buildTopicRelevanceMatcher(topicKey: string): TopicRelevanceMatcher | null {
  const terms = deriveTopicRelevanceTerms(topicKey);
  if (terms.length === 0) {
    return null;
  }
  return {
    exactTermRegexes: terms.map((term) => new RegExp(`\\b${escapeRegexLiteral(term)}\\b`, "i")),
  };
}

function getTopicRelevanceMatcher(topicKey: string): TopicRelevanceMatcher | null {
  if (topicRelevanceMatcherCache.has(topicKey)) {
    return topicRelevanceMatcherCache.get(topicKey) ?? null;
  }
  const matcher = buildTopicRelevanceMatcher(topicKey);
  topicRelevanceMatcherCache.set(topicKey, matcher);
  return matcher;
}

function countRegexMatches(content: string, regex: RegExp): number {
  if (!content) {
    return 0;
  }
  const globalRegex = new RegExp(regex.source, "gi");
  const matches = content.match(globalRegex);
  return matches ? matches.length : 0;
}

function isEventRelevantToTopic(event: QueryModeRawEvent, topicKey: string): boolean {
  const matcher = getTopicRelevanceMatcher(topicKey);
  if (!matcher) {
    return true;
  }

  const titleAndUrl = `${event.title ?? ""} ${event.url}`.trim();
  const titleOrUrlMatch = matcher.exactTermRegexes.some((regex) => regex.test(titleAndUrl));
  if (titleOrUrlMatch) {
    return true;
  }

  const bodyMatchCount = matcher.exactTermRegexes.reduce(
    (count, regex) => count + countRegexMatches(event.text, regex),
    0
  );
  return bodyMatchCount >= TOPIC_RELEVANCE_MIN_BODY_MATCHES;
}

function getEventRecencyTime(event: RawEventForSelection): number {
  return (event.publishedAt ?? event.fetchedAt).getTime();
}

function sortByEngagementThenRecency<T extends RawEventForSelection>(events: T[]): T[] {
  return [...events].sort((left, right) => {
    const leftScore = left.engagementScore ?? 0;
    const rightScore = right.engagementScore ?? 0;
    if (rightScore !== leftScore) {
      return rightScore - leftScore;
    }
    return getEventRecencyTime(right) - getEventRecencyTime(left);
  });
}

function selectEvidenceByRecency<T extends RawEventForSelection>(events: T[], maxCount: number): T[] {
  // Upstream query orders by publishedAt DESC then fetchedAt DESC.
  return events.slice(0, maxCount);
}

function selectEvidenceByEngagement<T extends RawEventForSelection>(events: T[], maxCount: number): T[] {
  return sortByEngagementThenRecency(events).slice(0, maxCount);
}

function selectEvidenceByDiversity<T extends RawEventForSelection>(events: T[], maxCount: number): T[] {
  const curated: T[] = [];
  const discussion: T[] = [];
  const other: T[] = [];

  for (const event of events) {
    if (CURATED_SOURCES.has(event.source)) {
      curated.push(event);
      continue;
    }
    if (DISCUSSION_SOURCES.has(event.source)) {
      discussion.push(event);
      continue;
    }
    other.push(event);
  }

  const selected: T[] = [];
  if (curated.length > 0) {
    selected.push(curated[0]);
  }
  if (discussion.length > 0 && selected.length < maxCount) {
    selected.push(discussion[0]);
  }

  const curatedStartIndex = curated.length > 0 && selected[0] === curated[0] ? 1 : 0;
  const discussionStartIndex = discussion.length > 0 && selected.includes(discussion[0]) ? 1 : 0;
  const remaining = [
    ...curated.slice(curatedStartIndex),
    ...discussion.slice(discussionStartIndex),
    ...other,
  ];

  selected.push(...sortByEngagementThenRecency(remaining).slice(0, maxCount - selected.length));
  return selected;
}

const RECENCY_EVIDENCE_SELECTION_STRATEGY: EvidenceSelectionStrategy = {
  name: "recency",
  select: selectEvidenceByRecency,
};

const ENGAGEMENT_EVIDENCE_SELECTION_STRATEGY: EvidenceSelectionStrategy = {
  name: "engagement",
  select: selectEvidenceByEngagement,
};

const DIVERSITY_EVIDENCE_SELECTION_STRATEGY: EvidenceSelectionStrategy = {
  name: "diversity",
  select: selectEvidenceByDiversity,
};

const EVIDENCE_SELECTION_STRATEGIES: Record<EvidenceStrategy, EvidenceSelectionStrategy> = {
  recency: RECENCY_EVIDENCE_SELECTION_STRATEGY,
  engagement: ENGAGEMENT_EVIDENCE_SELECTION_STRATEGY,
  diversity: DIVERSITY_EVIDENCE_SELECTION_STRATEGY,
};

function selectEvidence<T extends RawEventForSelection>(
  events: T[],
  strategy: EvidenceStrategy,
  maxCount: number
): T[] {
  if (events.length === 0 || maxCount <= 0) {
    return [];
  }

  const selectionStrategy = EVIDENCE_SELECTION_STRATEGIES[strategy];
  return selectionStrategy.select(events, maxCount);
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

function getTopLevelTopicGroup(topicKey: string): string {
  const normalized = topicKey.trim().toLowerCase();
  if (!normalized) {
    return "";
  }

  const separatorIndex = normalized.indexOf(".");
  if (separatorIndex === -1) {
    return normalized;
  }
  return normalized.slice(0, separatorIndex);
}

function selectTopLevelTopicGroups(
  rankedTopics: RankedTopicScore[],
  maxTopicGroups: number
): Set<string> {
  const groupedScores = new Map<string, { score: number; latestGeneratedAtMs: number }>();

  for (const rankedTopic of rankedTopics) {
    const group = getTopLevelTopicGroup(rankedTopic.topic);
    if (!group) {
      continue;
    }

    const existing = groupedScores.get(group) ?? {
      score: 0,
      latestGeneratedAtMs: rankedTopic.latestGeneratedAtMs,
    };
    existing.score += rankedTopic.score;
    existing.latestGeneratedAtMs = Math.max(existing.latestGeneratedAtMs, rankedTopic.latestGeneratedAtMs);
    groupedScores.set(group, existing);
  }

  return new Set(
    [...groupedScores.entries()]
      .sort((left, right) => {
        if (right[1].score !== left[1].score) {
          return right[1].score - left[1].score;
        }
        if (right[1].latestGeneratedAtMs !== left[1].latestGeneratedAtMs) {
          return right[1].latestGeneratedAtMs - left[1].latestGeneratedAtMs;
        }
        return left[0].localeCompare(right[0]);
      })
      .slice(0, maxTopicGroups)
      .map(([group]) => group)
  );
}

function rankTopicsFromSnapshots(
  snapshots: Array<{ generatedAt: Date; snapshot: Prisma.JsonValue }>,
  requestedAt: Date,
  topicMatchers: RegExp[]
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

  return rankedTopics;
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
    topicMatchers
  );
  const selectedTopLevelTopicGroups = selectTopLevelTopicGroups(rankedTopics, maxTopics);
  const selectedRankedTopics = rankedTopics.filter((rankedTopic) =>
    selectedTopLevelTopicGroups.has(getTopLevelTopicGroup(rankedTopic.topic))
  );

  if (selectedRankedTopics.length === 0) {
    logger.warn(
      { topicGlobCount: topicGlobs.length, lookbackDays },
      "No topics matched query filters"
    );
    throw toNoCoverageError(
      snapshots.length === 0
        ? "No trend snapshots were found in the requested lookback window."
        : "No topics matched query filters in the requested lookback window."
    );
  }

  // Fetch all evidence in a single query
  const evidenceStrategy = request.query?.evidenceStrategy ?? "diversity";
  const rankedTopicKeys = new Set(selectedRankedTopics.map((topic) => topic.topic));
  let allEvents: QueryModeRawEvent[];

  try {
    const fetchedEvents = await ctx.prisma.rawEvent.findMany({
      where: {
        topics: {
          hasSome: [...rankedTopicKeys],
        },
        publishedAt: {
          gte: lookbackStart,
          lte: request.requestedAt,
        },
        url: {
          not: null, // Only fetch events with URLs for grounding
        },
      },
      orderBy: [{ publishedAt: "desc" }, { fetchedAt: "desc" }],
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
    allEvents = fetchedEvents.filter((event): event is (typeof fetchedEvents)[number] & { url: string } => {
      return event.url !== null;
    });
    ctx.healthContext.postgresHealthy = true;
  } catch (error) {
    ctx.healthContext.postgresHealthy = false;
    throw error;
  }

  // Partition events by topic
  const eventsByTopic = new Map<string, typeof allEvents>();
  const relevanceFilteredByTopic = new Map<string, number>();
  for (const event of allEvents) {
    for (const topicKey of event.topics) {
      if (!rankedTopicKeys.has(topicKey)) {
        continue;
      }
      if (!isEventRelevantToTopic(event, topicKey)) {
        relevanceFilteredByTopic.set(topicKey, (relevanceFilteredByTopic.get(topicKey) ?? 0) + 1);
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
  const hydratedTopics: ParsedSummaryTopic[] = selectedRankedTopics.map((rankedTopic) => {
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
        textExcerpt: event.text.slice(0, EVIDENCE_EXCERPT_MAX_LENGTH),
      })),
    };
  });

  const topicsWithEvidence = hydratedTopics.filter((topic) => topic.evidence.length > 0);

  if (topicsWithEvidence.length === 0) {
    logger.warn(
      { rankedTopicCount: selectedRankedTopics.length, lookbackDays },
      "No evidence found for any ranked topics"
    );
    throw toNoCoverageError("No recent activity was found for matched topics in the lookback window.");
  }

  if (selectedRankedTopics.length < rankedTopics.length) {
    const excludedByTopLevelCap = rankedTopics.length - selectedRankedTopics.length;
    coverageWarnings.push(
      `${excludedByTopLevelCap} subtopic(s) were excluded by top-level topic cap (${maxTopics}).`
    );
  }

  if (topicsWithEvidence.length < hydratedTopics.length) {
    const missingTopicCount = hydratedTopics.length - topicsWithEvidence.length;
    coverageWarnings.push(
      `${missingTopicCount} ranked topic(s) were excluded due to missing grounded evidence.`
    );
  }

  const relevanceFilteredCount = [...relevanceFilteredByTopic.values()].reduce(
    (count, filtered) => count + filtered,
    0
  );
  if (relevanceFilteredCount > 0) {
    coverageWarnings.push(
      `${relevanceFilteredCount} candidate event(s) were excluded by topic relevance checks.`
    );
  }

  logger.info(
    {
      lookbackDays,
      topicGlobCount: topicGlobs.length,
      candidateTopicCount: rankedTopics.length,
      rankedTopicCount: selectedRankedTopics.length,
      selectedTopLevelTopicCount: selectedTopLevelTopicGroups.size,
      selectedTopicCount: topicsWithEvidence.length,
      maxEventsPerTopic,
      coverageWarningCount: coverageWarnings.length,
      relevanceFilteredCount,
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
      evidenceStrategy,
    },
    topics: topicsWithEvidence,
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

type SignalCategory =
  | "security"
  | "reliability"
  | "lifecycle"
  | "governance"
  | "cost"
  | "feature";

interface EvidenceInsight {
  summary: string;
  citation: string | null;
  categories: Set<SignalCategory>;
  score: number;
  recencyMs: number;
}

const DATE_ONLY_TITLE_REGEX =
  /^(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2},\s+\d{4}$/i;

const GENERIC_EVIDENCE_SNIPPET_PATTERNS: RegExp[] = [
  /^the following release notes cover/i,
  /^for a comprehensive list/i,
  /^you can also see and filter all release notes/i,
  /^get the latest updates on azure/i,
  /^subscribe to notifications to stay informed/i,
  /^skip to main content/i,
  /^overview guides reference samples resources/i,
  /^welcome to /i,
  /^reading time:/i,
  /^table of contents/i,
  /^transcript$/i,
];

const SIGNAL_CATEGORY_PATTERNS: Record<SignalCategory, RegExp> = {
  security:
    /\b(cve-|vulnerability|security|privilege escalation|exploit|patch|xss|rce|authn|authz|jwt|oidc|iam)\b/i,
  reliability:
    /\b(outage|incident|degradation|latency|error rates?|unavailable|downtime|fail(?:ed|ure)|partition|control plane)\b/i,
  lifecycle:
    /\b(deprecat(?:e|ed|ion)|sunset|end of support|eol|removed support|no longer supported|upgrade required)\b/i,
  governance:
    /\b(policy|organization policy|compliance|governance|trust policy|permission|identity provider)\b/i,
  cost: /\b(pricing|cost|finops|optimi[sz]e|idle|throughput|latency reduction|ttlb|storage tier|ssd)\b/i,
  feature: /\b(generally available|ga|public preview|preview|launched|now available|release update|added support)\b/i,
};

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function truncateText(value: string, maxChars: number): string {
  if (value.length <= maxChars) {
    return value;
  }
  return `${value.slice(0, maxChars - 1).trimEnd()}…`;
}

function getEvidenceRecencyMs(evidence: ParsedSummaryEvidence): number {
  return (evidence.publishedAt ?? evidence.fetchedAt ?? new Date(0)).getTime();
}

function getEvidenceHostname(url: string | null): string | null {
  if (!url) {
    return null;
  }
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return null;
  }
}

function isPreferredCloudHost(topic: string, hostname: string | null): boolean {
  if (!hostname) {
    return false;
  }

  const normalizedTopic = normalizeTopicKey(topic);
  if (normalizedTopic.startsWith("aws.")) {
    return hostname.endsWith("aws.amazon.com") || hostname.endsWith("docs.aws.amazon.com");
  }
  if (normalizedTopic.startsWith("cloud.gcp")) {
    return (
      hostname.endsWith("cloud.google.com") ||
      hostname.endsWith("docs.cloud.google.com") ||
      hostname.endsWith("status.cloud.google.com")
    );
  }
  if (normalizedTopic.startsWith("cloud.azure")) {
    return hostname.endsWith("azure.microsoft.com") || hostname.endsWith("learn.microsoft.com");
  }
  if (normalizedTopic.startsWith("cloud.terraform")) {
    return (
      hostname.endsWith("hashicorp.com") ||
      hostname.endsWith("terraform.io") ||
      hostname.endsWith("aws.amazon.com") ||
      hostname.endsWith("cloud.google.com") ||
      hostname.endsWith("azure.microsoft.com")
    );
  }
  return false;
}

function hasPreferredCloudHostRule(topic: string): boolean {
  const normalizedTopic = normalizeTopicKey(topic);
  return (
    normalizedTopic.startsWith("aws.") ||
    normalizedTopic.startsWith("cloud.gcp") ||
    normalizedTopic.startsWith("cloud.azure") ||
    normalizedTopic.startsWith("cloud.terraform")
  );
}

function isLowSignalTitle(value: string): boolean {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return true;
  }
  if (DATE_ONLY_TITLE_REGEX.test(normalized)) {
    return true;
  }
  if (normalized.length < 12) {
    return true;
  }
  return GENERIC_EVIDENCE_SNIPPET_PATTERNS.some((pattern) => pattern.test(normalized));
}

function extractFirstMeaningfulSentence(value: string, maxChars: number): string | null {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return null;
  }

  const candidates = normalized
    .split(/(?<=[.!?])\s+|\s*\n+\s*/)
    .map((candidate) => candidate.trim())
    .filter((candidate) => candidate.length >= 24)
    .filter((candidate) => !GENERIC_EVIDENCE_SNIPPET_PATTERNS.some((pattern) => pattern.test(candidate)));
  if (candidates.length === 0) {
    return null;
  }

  return truncateText(candidates[0], maxChars);
}

function detectSignalCategories(value: string): Set<SignalCategory> {
  const categories = new Set<SignalCategory>();
  const normalized = normalizeWhitespace(value);
  for (const [category, pattern] of Object.entries(SIGNAL_CATEGORY_PATTERNS) as Array<
    [SignalCategory, RegExp]
  >) {
    if (pattern.test(normalized)) {
      categories.add(category);
    }
  }
  return categories;
}

function countTopicEvidenceTermMatches(topic: string, value: string): number {
  const matcher = getTopicRelevanceMatcher(topic);
  if (!matcher) {
    return 1;
  }
  return matcher.exactTermRegexes.reduce(
    (count, regex) => count + countRegexMatches(value, regex),
    0
  );
}

function buildEvidenceInsight(topic: ParsedSummaryTopic, evidence: ParsedSummaryEvidence): EvidenceInsight | null {
  const title = normalizeWhitespace(evidence.title ?? "");
  const excerptSentence = extractFirstMeaningfulSentence(evidence.textExcerpt ?? "", 170);
  const citation = evidence.url
    ? groundingFacade.dedupeCanonicalUrls([evidence.url])[0] ?? null
    : null;

  let summary = "";
  if (!isLowSignalTitle(title)) {
    summary = truncateText(title, 140);
    if (excerptSentence) {
      const titleFingerprint = normalizeTextFingerprint(summary);
      const excerptFingerprint = normalizeTextFingerprint(excerptSentence);
      if (excerptFingerprint.length > 0 && !excerptFingerprint.includes(titleFingerprint)) {
        summary = truncateText(`${summary} — ${excerptSentence}`, 220);
      }
    }
  } else if (excerptSentence) {
    summary = excerptSentence;
  } else {
    const hostname = getEvidenceHostname(evidence.url);
    if (hostname) {
      summary = `Update reported by ${hostname}`;
    }
  }

  if (!summary) {
    return null;
  }

  const categories = detectSignalCategories([title, evidence.textExcerpt ?? "", evidence.url ?? ""].join(" "));
  const hostname = getEvidenceHostname(evidence.url);
  const preferredHost = isPreferredCloudHost(topic.topic, hostname);
  const topicTermMatches = countTopicEvidenceTermMatches(
    topic.topic,
    `${title} ${evidence.textExcerpt ?? ""} ${evidence.url ?? ""}`
  );
  let score = 0;
  score += isLowSignalTitle(title) ? -2 : 3;
  score += excerptSentence ? 2 : 0;
  score += categories.size;
  score += preferredHost ? 4 : 0;
  score += Math.min(topicTermMatches, 2);
  if (topicTermMatches === 0 && !preferredHost) {
    score -= 4;
  }
  if (hasPreferredCloudHostRule(topic.topic) && !preferredHost) {
    score -= 2;
  }
  score += evidence.publishedAt ? 1 : 0;

  return {
    summary,
    citation,
    categories,
    score,
    recencyMs: getEvidenceRecencyMs(evidence),
  };
}

function collectTopEvidenceInsights(topic: ParsedSummaryTopic, limit: number): EvidenceInsight[] {
  const seen = new Set<string>();
  const insights = topic.evidence
    .map((evidence) => buildEvidenceInsight(topic, evidence))
    .filter((insight): insight is EvidenceInsight => insight !== null)
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      return right.recencyMs - left.recencyMs;
    })
    .filter((insight) => {
      const key = normalizeTextFingerprint(insight.summary);
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });

  return insights.slice(0, limit);
}

function buildInternalWhyItMatters(topic: string, categories: Set<SignalCategory>): string {
  const parts: string[] = [];
  if (categories.has("security")) {
    parts.push("Security-related changes may require immediate remediation to reduce exposure.");
  }
  if (categories.has("reliability")) {
    parts.push("Reliability and incident signals can impact SLOs if dependency failure paths are untested.");
  }
  if (categories.has("lifecycle")) {
    parts.push("Lifecycle/deprecation updates can break runtimes and automation if upgrades are delayed.");
  }
  if (categories.has("governance")) {
    parts.push("Identity and policy shifts can block deploys unless controls and trust policies are updated.");
  }
  if (categories.has("cost")) {
    parts.push("Cost and performance changes can materially alter spend and latency assumptions.");
  }
  if (categories.has("feature")) {
    parts.push("New GA/preview capabilities may reduce custom platform work once validated.");
  }

  if (parts.length === 0) {
    return `Recent ${topic} updates include concrete platform changes that may affect near-term delivery plans.`;
  }
  return parts.slice(0, 2).join(" ");
}

function buildInternalSuggestedAction(categories: Set<SignalCategory>): string {
  const actions: string[] = [];
  if (categories.has("security")) {
    actions.push("Prioritize patch validation and configuration audits for affected services.");
  }
  if (categories.has("reliability")) {
    actions.push("Run failover and alert drills for impacted dependency paths.");
  }
  if (categories.has("lifecycle")) {
    actions.push("Inventory impacted runtimes/services and stage upgrades before enforcement dates.");
  }
  if (categories.has("governance")) {
    actions.push("Review IAM/trust policy baselines and update policy-as-code checks.");
  }
  if (categories.has("cost")) {
    actions.push("Benchmark cost/latency impact in non-production before broad rollout.");
  }
  if (categories.has("feature")) {
    actions.push("Pilot new capabilities in non-production with clear rollback criteria.");
  }

  if (actions.length === 0) {
    return "Review cited changes, assign owners, and schedule validation work this week.";
  }
  return actions.slice(0, 2).join(" ");
}

function buildInternalHighlight(topic: ParsedSummaryTopic): NormalizedHighlight {
  const fallbackCitations = groundingFacade.dedupeCanonicalUrls(topic.evidence.map((evidence) => evidence.url));
  const insights = collectTopEvidenceInsights(topic, 2);
  const citations = groundingFacade.dedupeCanonicalUrls(
    insights.map((insight) => insight.citation).filter((citation): citation is string => Boolean(citation))
  );
  const categories = new Set<SignalCategory>();
  for (const insight of insights) {
    for (const category of insight.categories) {
      categories.add(category);
    }
  }

  const whatHappened =
    insights.length > 0
      ? ensureSentenceEnding(insights.map((insight) => insight.summary).join("; "))
      : `Recent updates were detected for ${topic.topic}.`;

  return {
    topic: topic.topic,
    what_happened: whatHappened,
    why_it_matters: buildInternalWhyItMatters(topic.topic, categories),
    suggested_action: buildInternalSuggestedAction(categories),
    citations: citations.length > 0 ? citations : fallbackCitations,
  };
}

function normalizeLlmHighlight(highlight: NormalizedHighlight): NormalizedHighlight {
  return {
    topic: highlight.topic.trim(),
    what_happened: highlight.what_happened.trim(),
    why_it_matters: highlight.why_it_matters.trim(),
    suggested_action: highlight.suggested_action.trim(),
    citations: groundingFacade.dedupeCanonicalUrls(highlight.citations),
  };
}

function normalizeTopicKey(topic: string): string {
  return topic.trim().toLowerCase();
}

function normalizeTextFingerprint(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLowerCase();
}

function ensureSentenceEnding(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return trimmed;
  }
  if (/[.!?]$/.test(trimmed)) {
    return trimmed;
  }
  return `${trimmed}.`;
}

function mergeNarrativeFields(values: string[]): string {
  const merged: string[] = [];
  const seen = new Set<string>();

  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed) {
      continue;
    }

    const fingerprint = normalizeTextFingerprint(trimmed);
    if (seen.has(fingerprint)) {
      continue;
    }
    seen.add(fingerprint);
    merged.push(ensureSentenceEnding(trimmed));
  }

  return merged.join(" ");
}

function mergeHighlightsByTopic(highlights: NormalizedHighlight[]): NormalizedHighlight[] {
  const merged: NormalizedHighlight[] = [];
  const indexByTopic = new Map<string, number>();

  for (const highlight of highlights) {
    const topicKey = normalizeTopicKey(highlight.topic);
    const existingIndex = indexByTopic.get(topicKey);
    if (existingIndex === undefined) {
      merged.push({
        ...highlight,
        citations: groundingFacade.dedupeCanonicalUrls(highlight.citations),
      });
      indexByTopic.set(topicKey, merged.length - 1);
      continue;
    }

    const existing = merged[existingIndex];
    merged[existingIndex] = {
      topic: existing.topic,
      what_happened: mergeNarrativeFields([existing.what_happened, highlight.what_happened]),
      why_it_matters: mergeNarrativeFields([existing.why_it_matters, highlight.why_it_matters]),
      suggested_action: mergeNarrativeFields([existing.suggested_action, highlight.suggested_action]),
      citations: groundingFacade.dedupeCanonicalUrls([...existing.citations, ...highlight.citations]),
    };
  }

  return merged;
}

interface TopicEvidenceScope {
  canonicalTopic: string;
  normalizedTopic: string;
  topLevelGroup: string;
  evidenceUrls: Set<string>;
}

function buildTopicEvidenceScopes(request: ParsedSummaryRequest): Map<string, TopicEvidenceScope> {
  const scopes = new Map<string, TopicEvidenceScope>();
  for (const topic of request.topics) {
    const canonicalTopic = topic.topic.trim();
    const topicKey = normalizeTopicKey(canonicalTopic);
    scopes.set(topicKey, {
      canonicalTopic,
      normalizedTopic: topicKey,
      topLevelGroup: getTopLevelTopicGroup(topicKey),
      evidenceUrls: new Set(groundingFacade.dedupeCanonicalUrls(topic.evidence.map((evidence) => evidence.url))),
    });
  }
  return scopes;
}

function countScopeCitationOverlap(scope: TopicEvidenceScope, citations: string[]): number {
  return citations.reduce((count, citation) => count + (scope.evidenceUrls.has(citation) ? 1 : 0), 0);
}

function resolveGroundedTopicScope(
  topicEvidenceScopes: Map<string, TopicEvidenceScope>,
  requestedTopicKey: string,
  groundedCitations: string[]
): TopicEvidenceScope | null {
  const declaredScope = topicEvidenceScopes.get(requestedTopicKey);
  if (declaredScope && countScopeCitationOverlap(declaredScope, groundedCitations) > 0) {
    return declaredScope;
  }

  const requestedTopLevelGroup = getTopLevelTopicGroup(requestedTopicKey);
  let bestScope: TopicEvidenceScope | null = null;
  let bestOverlap = 0;
  let bestSameTopLevelGroup = false;

  for (const scope of topicEvidenceScopes.values()) {
    const overlap = countScopeCitationOverlap(scope, groundedCitations);
    if (overlap <= 0) {
      continue;
    }

    const sameTopLevelGroup =
      requestedTopLevelGroup.length > 0 && scope.topLevelGroup === requestedTopLevelGroup;
    if (
      overlap > bestOverlap ||
      (overlap === bestOverlap && sameTopLevelGroup && !bestSameTopLevelGroup)
    ) {
      bestScope = scope;
      bestOverlap = overlap;
      bestSameTopLevelGroup = sameTopLevelGroup;
    }
  }

  return bestScope;
}

function formatReportDate(value: Date | undefined, timezone: string | undefined): string | null {
  if (!value) {
    return null;
  }

  try {
    return new Intl.DateTimeFormat("en-US", {
      timeZone: timezone ?? "UTC",
      year: "numeric",
      month: "long",
      day: "numeric",
    }).format(value);
  } catch {
    return value.toISOString();
  }
}

function deriveStructuredNotes(
  request: ParsedSummaryRequest,
  highlights: NormalizedHighlight[]
): string {
  const timezone = request.report?.timezone;
  const startAt = request.report?.startAt;
  const endAt = request.report?.endAt ?? request.requestedAt;
  const startText = formatReportDate(startAt, timezone);
  const endText = formatReportDate(endAt, timezone) ?? request.requestedAt.toISOString();
  const lookbackDays = request.query?.lookbackDays;
  const timeframe =
    startText !== null
      ? `${startText} through ${endText}`
      : lookbackDays && lookbackDays > 0
        ? `the last ${lookbackDays} day(s), ending ${endText}`
        : `up to ${endText}`;

  const topTopics = highlights.slice(0, 3).map((highlight) => highlight.topic);
  const topTopicSentence =
    topTopics.length > 0 ? topTopics.join(", ") : "No dominant topics were confidently grounded";
  const distinctSentences = (
    values: string[],
    limit: number,
    fallback: string
  ): string => {
    const sentenceCandidates = values
      .map((value) => normalizeWhitespace(value))
      .flatMap((value) => value.split(/(?<=[.!?])\s+/))
      .map((value) => value.trim())
      .filter((value) => value.length > 0)
      .map((value) => ensureSentenceEnding(value));
    const deduped = [...new Set(sentenceCandidates)];
    if (deduped.length === 0) {
      return fallback;
    }
    return deduped.slice(0, limit).join(" ");
  };
  const topicLandscapeLines =
    highlights.length > 0
      ? highlights
          .slice(0, 3)
          .map((highlight) => `- **${highlight.topic}**: ${highlight.why_it_matters}`)
      : ["- Limited coverage: no grounded highlights were produced for this request."];
  const riskAndGovernance = distinctSentences(
    highlights.map((highlight) => highlight.why_it_matters),
    2,
    "No grounded risk signals were available in this run."
  );
  const executionAndEconomics = distinctSentences(
    highlights.map((highlight) => highlight.suggested_action),
    2,
    "No grounded execution actions were available in this run."
  );
  const outlookText =
    topTopics.length > 0
      ? `Near-term execution focus is likely to remain on ${topTopicSentence} as teams operationalize the cited changes.`
      : "Collect additional grounded evidence before setting near-term outlook assumptions.";

  return [
    "# State of Signals and Where They're Going",
    "",
    "## Method and scope",
    `This report summarizes topic-level evidence gathered over ${timeframe}${timezone ? ` (${timezone})` : ""}.`,
    "",
    "## Dominant shifts",
    `Current signals are clustering around ${topTopicSentence}, with emphasis on concrete operational and platform updates.`,
    "",
    "## Topic landscape",
    ...topicLandscapeLines,
    "",
    "## Risk and governance",
    riskAndGovernance,
    "",
    "## Execution and economics",
    executionAndEconomics,
    "",
    "## Outlook",
    outlookText,
  ].join("\n");
}

function deriveDefaultNotes(request: ParsedSummaryRequest, highlights: NormalizedHighlight[]): string {
  return deriveStructuredNotes(request, highlights);
}

function buildCodexCliPrompt(
  request: ParsedSummaryRequest,
  logger?: pino.Logger,
  healthContext?: HealthContext
): string {
  const payload = groundingFacade.buildSummaryRequestPayload(request, {
    logger,
    healthContext,
  });
  const maxTopics = resolveHighlightLimit(request, request.topics.length);
  const maxEvidencePerTopic =
    request.budget?.maxEvidencePerTopic ??
    Math.max(...request.topics.map((topic) => topic.evidence.length), 0);
  const maxOutputTokens = request.budget?.maxOutputTokens ?? 1200;

  const promptSections = [
    "You are generating a human-readable engineering intelligence brief from a structured summary request.",
    "Use only the evidence included in SUMMARY_REQUEST_JSON. Do not invent facts or URLs.",
    "Trend metrics (score, volume, acceleration) are ranking inputs only. Do not repeat these numbers in highlights.",
    "Do not write phrases like '<topic> reached score ...'. Summarize concrete events from evidence (releases, incidents, CVEs, deprecations, region/feature launches, policy/pricing changes).",
    "Each highlight must include concrete what_happened, why_it_matters, suggested_action, and citations.",
    "Do not emit duplicate topics in highlights. If multiple points map to the same topic, combine them into one highlight.",
    `Keep output concise and practical. Limit highlights to at most ${maxTopics} and per-topic evidence references to at most ${maxEvidencePerTopic}.`,
    `Target no more than ${maxOutputTokens} tokens in total output.`,
    "Return valid JSON only with this shape:",
    '{ "title": string, "highlights": [{ "topic": string, "what_happened": string, "why_it_matters": string, "suggested_action": string, "citations": string[] }], "notes": string, "usage": { "prompt_tokens": number, "completion_tokens": number }, "meta": { "provider": string, "model": string, "estimated_cost_usd": number } }',
    "If usage or cost are unknown, set them to 0.",
  ];

  promptSections.push(
    "STANDARD NOTES FORMAT (always required): Render notes as markdown using this structure in order:",
    "# State of Signals and Where They're Going",
    "## Method and scope",
    "## Dominant shifts",
    "## Topic landscape",
    "## Risk and governance",
    "## Execution and economics",
    "## Outlook",
    "If notes include URLs, every URL MUST come from the evidence in SUMMARY_REQUEST_JSON."
  );

  promptSections.push(`SUMMARY_REQUEST_JSON:\n${JSON.stringify(payload, null, 2)}`);
  return promptSections.join("\n\n");
}

async function callHttpLlm(
  config: Config,
  request: ParsedSummaryRequest,
  logger: pino.Logger,
  healthContext?: HealthContext
): Promise<ParsedLlmResponse> {
  let response: Response;
  try {
    response = await fetch(config.LLM_ENDPOINT_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(
        groundingFacade.buildSummaryRequestPayload(request, {
          logger,
          healthContext,
        })
      ),
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
  logger: pino.Logger,
  healthContext?: HealthContext
): Promise<ParsedLlmResponse> {
  const prompt = buildCodexCliPrompt(request, logger, healthContext);
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
  const evidenceUrls = groundingFacade.createEvidenceUrlSet(request);
  const topicEvidenceScopes = buildTopicEvidenceScopes(request);
  if (evidenceUrls.size === 0) {
    throw new NonRetryableProcessingError("No evidence URLs were provided in the summary request");
  }

  const groundedHighlights = highlights
    .map((highlight) => {
      const globallyGroundedCitations = groundingFacade.filterGroundedCitations(
        highlight.citations,
        evidenceUrls
      );
      if (globallyGroundedCitations.length === 0) {
        return null;
      }

      const topicScope = resolveGroundedTopicScope(
        topicEvidenceScopes,
        normalizeTopicKey(highlight.topic),
        globallyGroundedCitations
      );
      if (!topicScope) {
        return null;
      }

      const topicScopedCitations = globallyGroundedCitations.filter((citation) =>
        topicScope.evidenceUrls.has(citation)
      );
      if (topicScopedCitations.length === 0) {
        return null;
      }

      return {
        ...highlight,
        topic: topicScope.canonicalTopic,
        citations: topicScopedCitations,
      };
    })
    .filter((highlight): highlight is NormalizedHighlight => highlight !== null)
    .filter((highlight) => highlight.citations.length > 0);

  const mergedHighlights = mergeHighlightsByTopic(groundedHighlights);

  if (mergedHighlights.length === 0) {
    throw new NonRetryableProcessingError(
      "Brief generation produced no grounded highlights with valid evidence citations"
    );
  }

  return mergedHighlights;
}

interface BuildSuccessResultInput {
  ctx: ProcessContext;
  request: ParsedSummaryRequest;
  producedAt: Date;
  estimatedCostUsd: number;
}

interface LlmProviderStrategy {
  readonly provider: LlmProvider;
  build(input: BuildSuccessResultInput): Promise<SuccessResult>;
}

function isNoGroundedHighlightError(error: unknown): boolean {
  return (
    error instanceof NonRetryableProcessingError &&
    error.message === "Brief generation produced no grounded highlights with valid evidence citations"
  );
}

function isCodexOutputArtifactError(error: unknown): boolean {
  if (!(error instanceof LlmGenerationError)) {
    return false;
  }
  const message = error.message.toLowerCase();
  const isArtifactMissing =
    (message.includes("last-message.txt") || message.includes("output file missing")) &&
    message.includes("enoent");
  const isTempStorageFailure =
    (message.includes("enospc") ||
      message.includes("no space left on device") ||
      message.includes("eacces") ||
      message.includes("permission denied")) &&
    (message.includes("mkdtemp") ||
      message.includes("mkdir") ||
      message.includes("codex-tmp") ||
      message.includes(".tmp"));

  return (
    message.includes("codex cli request failed") &&
    (isArtifactMissing || isTempStorageFailure)
  );
}

function resolveHighlightLimit(request: ParsedSummaryRequest, availableCount: number): number {
  if (request.query) {
    return availableCount;
  }

  const maxTopics = request.budget?.maxTopics;
  if (typeof maxTopics !== "number" || !Number.isInteger(maxTopics) || maxTopics <= 0) {
    return availableCount;
  }

  return Math.min(maxTopics, availableCount);
}

function selectInternalFallbackTopics(
  request: ParsedSummaryRequest,
  preferredCount?: number,
  conciseQueryMode = false
): ParsedSummaryTopic[] {
  let limit = resolveHighlightLimit(request, request.topics.length);

  // Query-mode can include many subtopics for evidence selection; apply concise cap only for fallback paths.
  if (conciseQueryMode && request.query) {
    const maxTopics = request.budget?.maxTopics;
    if (typeof maxTopics === "number" && Number.isInteger(maxTopics) && maxTopics > 0) {
      limit = Math.min(limit, maxTopics);
    }
  }

  if (typeof preferredCount === "number" && Number.isInteger(preferredCount) && preferredCount > 0) {
    limit = Math.min(limit, preferredCount);
  }

  const boundedLimit = Math.max(1, Math.min(limit, request.topics.length));
  return request.topics.slice(0, boundedLimit);
}

function appendCoverageWarnings(notes: string, request: ParsedSummaryRequest): string {
  if (!request.coverageWarnings || request.coverageWarnings.length === 0) {
    return notes;
  }

  const warningsText = request.coverageWarnings.join(" ");
  return notes ? `${notes}\n\nCoverage Note: ${warningsText}` : `Coverage Note: ${warningsText}`;
}

function buildInternalSuccessResult(
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number,
  preferredTopicCount?: number,
  conciseQueryMode = false,
  provider = "internal",
  model = "rule-based-v1"
): SuccessResult {
  const topics = selectInternalFallbackTopics(request, preferredTopicCount, conciseQueryMode);
  const highlights = enforceGroundedHighlights(
    request,
    topics.map((topic) => buildInternalHighlight(topic))
  );
  const inputTokens = estimateTokenCount(JSON.stringify(request));
  const outputTokens = estimateTokenCount(JSON.stringify(highlights));
  let notes = deriveDefaultNotes(request, highlights);
  notes = appendCoverageWarnings(notes, request);
  notes = groundingFacade.enforceGroundedNotes(request, notes, toGroundingError);

  return buildSuccessPayload(
    request,
    producedAt,
    `Trend Brief ${producedAt.toISOString().slice(0, 10)}`,
    highlights,
    notes,
    provider,
    model,
    inputTokens,
    outputTokens,
    normalizeUsd(estimatedCostUsd)
  );
}

function buildLlmBackedSuccessResult(
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number,
  llmResponse: ParsedLlmResponse,
  defaultProvider: string,
  defaultModel: string,
  logger: pino.Logger
): SuccessResult {
  const maxTopics = resolveHighlightLimit(request, llmResponse.highlights.length);
  const llmHighlights = llmResponse.highlights.slice(0, maxTopics).map(normalizeLlmHighlight);
  let highlights: NormalizedHighlight[];
  let usedInternalFallback = false;

  try {
    highlights = enforceGroundedHighlights(request, llmHighlights);
  } catch (error) {
    if (!isNoGroundedHighlightError(error)) {
      throw error;
    }

    const fallbackTopics = selectInternalFallbackTopics(request, llmHighlights.length, true);
    highlights = enforceGroundedHighlights(
      request,
      fallbackTopics.map((topic) => buildInternalHighlight(topic))
    );
    usedInternalFallback = true;
    logger.warn(
      {
        requestId: request.requestId,
        llmHighlightCount: llmHighlights.length,
        fallbackHighlightCount: highlights.length,
      },
      "LLM highlights failed grounding; using internal grounded fallback highlights"
    );
  }
  const inputTokens = llmResponse.usage?.prompt_tokens ?? estimateTokenCount(JSON.stringify(request));
  const outputTokens =
    llmResponse.usage?.completion_tokens ?? estimateTokenCount(JSON.stringify(highlights));

  let notes = usedInternalFallback
    ? deriveDefaultNotes(request, highlights)
    : llmResponse.notes?.trim() || deriveDefaultNotes(request, highlights);
  notes = appendCoverageWarnings(notes, request);
  notes = groundingFacade.enforceGroundedNotes(request, notes, toGroundingError);

  return buildSuccessPayload(
    request,
    producedAt,
    llmResponse.title,
    highlights,
    notes,
    usedInternalFallback ? "internal" : llmResponse.meta?.provider?.trim() || defaultProvider,
    usedInternalFallback ? "rule-based-fallback-v1" : llmResponse.meta?.model?.trim() || defaultModel,
    inputTokens,
    outputTokens,
    normalizeUsd(llmResponse.meta?.estimated_cost_usd ?? estimatedCostUsd)
  );
}

async function buildHttpSuccessResult(input: BuildSuccessResultInput): Promise<SuccessResult> {
  const { ctx, request, producedAt, estimatedCostUsd } = input;
  const llmResponse = await callHttpLlm(ctx.config, request, ctx.logger, ctx.healthContext);
  return buildLlmBackedSuccessResult(
    request,
    producedAt,
    estimatedCostUsd,
    llmResponse,
    "http",
    "http-v1",
    ctx.logger
  );
}

async function buildCodexCliSuccessResult(input: BuildSuccessResultInput): Promise<SuccessResult> {
  const { ctx, request, producedAt, estimatedCostUsd } = input;
  try {
    const llmResponse = await callCodexCliLlm(ctx.config, request, ctx.logger, ctx.healthContext);
    const defaultModel = ctx.config.LLM_CODEX_MODEL.trim() || "codex-cli";

    return buildLlmBackedSuccessResult(
      request,
      producedAt,
      estimatedCostUsd,
      llmResponse,
      "codex-cli",
      defaultModel,
      ctx.logger
    );
  } catch (error) {
    if (!isCodexOutputArtifactError(error)) {
      throw error;
    }

    ctx.logger.warn(
      {
        requestId: request.requestId,
        error: serializeError(error),
      },
      "Codex CLI execution unavailable; using internal grounded fallback brief"
    );
    return buildInternalSuccessResult(
      request,
      producedAt,
      estimatedCostUsd,
      request.budget?.maxTopics,
      true,
      "internal",
      "rule-based-fallback-v1"
    );
  }
}

const LLM_PROVIDER_STRATEGIES: Record<LlmProvider, LlmProviderStrategy> = {
  internal: {
    provider: "internal",
    async build({ request, producedAt, estimatedCostUsd }) {
      return buildInternalSuccessResult(request, producedAt, estimatedCostUsd);
    },
  },
  http: {
    provider: "http",
    build: buildHttpSuccessResult,
  },
  "codex-cli": {
    provider: "codex-cli",
    build: buildCodexCliSuccessResult,
  },
};

function resolveLlmProvider(ctx: ProcessContext, request: ParsedSummaryRequest): LlmProvider {
  return request.llmProvider ?? ctx.config.LLM_PROVIDER;
}

async function buildSuccessResult(
  ctx: ProcessContext,
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number
): Promise<SuccessResult> {
  const llmProvider = resolveLlmProvider(ctx, request);
  const strategy = LLM_PROVIDER_STRATEGIES[llmProvider];
  if (!strategy) {
    throw new LlmGenerationError(`Unsupported LLM provider: ${llmProvider}`);
  }

  return strategy.build({
    ctx,
    request,
    producedAt,
    estimatedCostUsd,
  });
}

function isDuplicateKeyError(error: unknown): boolean {
  return error instanceof Prisma.PrismaClientKnownRequestError && error.code === "P2002";
}

async function persistResult(
  prisma: PrismaClient,
  payload: BriefResultPayload,
  status: BriefStatus
): Promise<"created" | "duplicate"> {
  try {
    await prisma.briefResult.create({
      data: {
        requestId: payload.request_id,
        producedAt: new Date(payload.produced_at),
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

function toBudgetDate(dateKey: string): Date {
  return new Date(`${dateKey}T00:00:00Z`);
}

async function syncBudgetCacheBestEffort(
  redis: Redis,
  dateKey: string,
  spentUsd: number,
  logger: pino.Logger
): Promise<void> {
  try {
    await redis.set(
      getBudgetKey(dateKey),
      spentUsd.toString(),
      "EX",
      BUDGET_KEY_TTL_SECONDS
    );
  } catch (error) {
    logger.warn(
      {
        dateKey,
        spentUsd,
        error: serializeError(error),
      },
      "Failed to sync brief budget cache; continuing with Postgres source of truth"
    );
  }
}

interface BudgetReservationInput {
  prisma: PrismaClient;
  redis: Redis;
  logger: pino.Logger;
  dateKey: string;
  dailyBudgetUsd: number;
  amountUsd: number;
}

interface BudgetReservationResult {
  reserved: boolean;
  spentUsd: number;
}

type BudgetReservationPassReason =
  | "cache_unavailable"
  | "requires_source_of_truth";

type BudgetReservationDecision =
  | { kind: "handled"; result: BudgetReservationResult }
  | { kind: "pass"; reason: BudgetReservationPassReason };

interface BudgetReservationHandler {
  setNext(next: BudgetReservationHandler): BudgetReservationHandler;
  reserve(input: BudgetReservationInput): Promise<BudgetReservationResult>;
}

abstract class AbstractBudgetReservationHandler implements BudgetReservationHandler {
  private nextHandler: BudgetReservationHandler | null = null;

  setNext(next: BudgetReservationHandler): BudgetReservationHandler {
    this.nextHandler = next;
    return next;
  }

  async reserve(input: BudgetReservationInput): Promise<BudgetReservationResult> {
    const decision = await this.tryReserve(input);
    if (decision.kind === "handled") {
      return decision.result;
    }

    if (!this.nextHandler) {
      throw new Error(
        `Budget reservation chain terminated without a handler for reason: ${decision.reason}`
      );
    }

    return this.nextHandler.reserve(input);
  }

  protected abstract tryReserve(
    input: BudgetReservationInput
  ): Promise<BudgetReservationDecision>;
}

class RedisBudgetReservationHandler extends AbstractBudgetReservationHandler {
  protected async tryReserve(
    input: BudgetReservationInput
  ): Promise<BudgetReservationDecision> {
    const {
      prisma,
      redis,
      logger,
      dateKey,
      dailyBudgetUsd,
      amountUsd,
    } = input;

    try {
      const result = await redis.eval(
        BUDGET_RESERVATION_SCRIPT,
        1,
        getBudgetKey(dateKey),
        dailyBudgetUsd.toString(),
        amountUsd.toString(),
        BUDGET_KEY_TTL_SECONDS.toString()
      );
      const cached = parseBudgetReservationResult(result);
      if (!cached.reserved) {
        return { kind: "pass", reason: "requires_source_of_truth" };
      }

      const budgetDate = toBudgetDate(dateKey);
      void prisma.briefBudgetTracking.upsert({
        where: { date: budgetDate },
        create: {
          date: budgetDate,
          spentUsd: cached.spentUsd,
          budgetUsd: dailyBudgetUsd,
          requestCount: 1,
        },
        update: {
          spentUsd: { increment: amountUsd },
          requestCount: { increment: 1 },
        },
      }).catch((error) => {
        logger.warn(
          { dateKey, error: serializeError(error) },
          "Failed to asynchronously mirror reserved budget to Postgres"
        );
      });

      return {
        kind: "handled",
        result: cached,
      };
    } catch (error) {
      logger.warn(
        {
          dateKey,
          amountUsd,
          error: serializeError(error),
        },
        "Redis budget reservation unavailable; falling back to Postgres"
      );
      return { kind: "pass", reason: "cache_unavailable" };
    }
  }
}

class PostgresBudgetReservationHandler extends AbstractBudgetReservationHandler {
  protected async tryReserve(
    input: BudgetReservationInput
  ): Promise<BudgetReservationDecision> {
    const {
      prisma,
      redis,
      logger,
      dateKey,
      dailyBudgetUsd,
      amountUsd,
    } = input;

    const budgetDate = toBudgetDate(dateKey);
    const record = await prisma.briefBudgetTracking.upsert({
      where: { date: budgetDate },
      create: {
        date: budgetDate,
        spentUsd: 0,
        budgetUsd: dailyBudgetUsd,
        requestCount: 0,
      },
      update: {},
      select: { spentUsd: true },
    });

    const maxSpendBeforeReservation = Math.max(0, dailyBudgetUsd - amountUsd);
    const whereClause = Number.isFinite(maxSpendBeforeReservation)
      ? { date: budgetDate, spentUsd: { lte: maxSpendBeforeReservation } }
      : { date: budgetDate };

    const updateResult = await prisma.briefBudgetTracking.updateMany({
      // Ensure concurrent workers cannot oversubscribe budget.
      where: whereClause,
      data: {
        spentUsd: { increment: amountUsd },
        requestCount: { increment: 1 },
      },
    });

    if (updateResult.count === 0) {
      const latest = await prisma.briefBudgetTracking.findUnique({
        where: { date: budgetDate },
        select: { spentUsd: true },
      });
      const spentUsd = Number(latest?.spentUsd ?? record.spentUsd);
      await syncBudgetCacheBestEffort(redis, dateKey, spentUsd, logger);
      return {
        kind: "handled",
        result: { reserved: false, spentUsd },
      };
    }

    const latest = await prisma.briefBudgetTracking.findUnique({
      where: { date: budgetDate },
      select: { spentUsd: true },
    });
    const newSpent = Number(latest?.spentUsd ?? Number(record.spentUsd) + amountUsd);
    await syncBudgetCacheBestEffort(redis, dateKey, newSpent, logger);
    return {
      kind: "handled",
      result: { reserved: true, spentUsd: newSpent },
    };
  }
}

function createBudgetReservationHandlerChain(): BudgetReservationHandler {
  const redisHandler = new RedisBudgetReservationHandler();
  redisHandler.setNext(new PostgresBudgetReservationHandler());
  return redisHandler;
}

const BUDGET_RESERVATION_HANDLER_CHAIN = createBudgetReservationHandlerChain();

async function reserveBudgetSpendUsd(
  prisma: PrismaClient,
  redis: Redis,
  logger: pino.Logger,
  dateKey: string,
  dailyBudgetUsd: number,
  amountUsd: number
): Promise<BudgetReservationResult> {
  const input: BudgetReservationInput = {
    prisma,
    redis,
    logger,
    dateKey,
    dailyBudgetUsd,
    amountUsd,
  };

  return BUDGET_RESERVATION_HANDLER_CHAIN.reserve(input);
}

async function releaseBudgetReservationUsd(
  prisma: PrismaClient,
  redis: Redis,
  logger: pino.Logger,
  dateKey: string,
  amountUsd: number
): Promise<number> {
  // Update Postgres first (source of truth)
  const budgetDate = toBudgetDate(dateKey);
  const record = await prisma.briefBudgetTracking.findUnique({
    where: { date: budgetDate },
    select: { spentUsd: true },
  });

  if (!record) {
    return 0;
  }

  const currentSpent = Number(record.spentUsd);
  const newSpent = Math.max(0, currentSpent - amountUsd);

  await prisma.briefBudgetTracking.update({
    where: { date: budgetDate },
    data: { spentUsd: newSpent },
  });

  await syncBudgetCacheBestEffort(redis, dateKey, newSpent, logger);
  return newSpent;
}

async function settleBudgetSpendUsd(
  prisma: PrismaClient,
  redis: Redis,
  logger: pino.Logger,
  dateKey: string,
  deltaUsd: number
): Promise<number> {
  // Update Postgres first (source of truth)
  const budgetDate = toBudgetDate(dateKey);
  const record = await prisma.briefBudgetTracking.findUnique({
    where: { date: budgetDate },
    select: { spentUsd: true },
  });

  if (!record) {
    return 0;
  }

  const currentSpent = Number(record.spentUsd);
  const newSpent = Math.max(0, currentSpent + deltaUsd);

  await prisma.briefBudgetTracking.update({
    where: { date: budgetDate },
    data: { spentUsd: newSpent },
  });

  await syncBudgetCacheBestEffort(redis, dateKey, newSpent, logger);
  return newSpent;
}

async function loadPersistedResult(
  prisma: PrismaClient,
  requestId: string
): Promise<{ status: BriefStatus; payload: BriefResultPayload } | null> {
  const existing = await prisma.briefResult.findUnique({
    where: { requestId },
    select: { status: true, result: true },
  });
  if (!existing) {
    return null;
  }

  return {
    status: existing.status,
    payload: parseBriefResultPayload(existing.result),
  };
}

async function republishPersistedResult(
  ctx: ProcessContext,
  requestId: string,
  publisher: BriefResultPublisher<BriefResultPayload>
): Promise<BriefStatus | null> {
  const existing = await loadPersistedResult(ctx.prisma, requestId);
  ctx.healthContext.postgresHealthy = true;
  if (!existing) {
    return null;
  }

  await publisher.publishResult(requestId, existing.payload);
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
    logger,
    dateKey,
    reservedAmountUsd
  );
  ctx.healthContext.redisHealthy = true;
  setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - spentBudgetUsd));
  logger.info({ spentBudgetUsd }, "Rolled back brief budget reservation");
}

async function emitFailureResult(
  ctx: ProcessContext,
  publisher: BriefResultPublisher<BriefResultPayload>,
  requestId: string,
  producedAt: Date,
  code: string,
  message: string,
  retryable: boolean
): Promise<void> {
  const failureResult = buildFailureBriefResultPayload(
    requestId,
    producedAt,
    code,
    message,
    retryable
  );
  const persisted = await persistResult(ctx.prisma, failureResult, BriefStatus.failure);
  ctx.healthContext.postgresHealthy = true;
  if (persisted === "duplicate") {
    incrementDuplicatesSkipped(ctx.healthContext);
    const republishedStatus = await republishPersistedResult(ctx, requestId, publisher);
    if (!republishedStatus) {
      throw new Error(`Unable to republish existing failure result for request ${requestId}`);
    }
    return;
  }

  await publisher.publishResult(requestId, failureResult);
}

export async function processSummaryRequest(
  ctx: ProcessContext,
  request: ParsedSummaryRequest
): Promise<void> {
  const logger = ctx.logger.child({ requestId: request.requestId });
  const publisher = createBriefResultPublisher<BriefResultPayload>({
    producer: ctx.producer,
    logger,
    topic: ctx.config.KAFKA_TOPIC_SUMMARY_RESULTS,
  });
  const producedAt = new Date();
  let existingResult: { status: BriefStatus; payload: BriefResultPayload } | null = null;

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
      await publisher.publishResult(request.requestId, existingResult.payload);
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
        publisher,
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
      logger,
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
      publisher,
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
      const republishedStatus = await republishPersistedResult(ctx, request.requestId, publisher);
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
        spentBudgetUsd = await settleBudgetSpendUsd(
          ctx.prisma,
          ctx.redis,
          logger,
          dateKey,
          costDeltaUsd
        );
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

    await publisher.publishResult(request.requestId, successResult.payload);

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
        publisher,
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

    if (error instanceof LlmGenerationError) {
      const failureCode = classifyRetryableFailureCode(error);
      incrementError(ctx.healthContext, failureCode);
      incrementGeneration(ctx.healthContext, "failure");
      logger.error(
        { error: serializeError(error), failureCode },
        "Failed to process summary request due to retryable LLM error"
      );
      await emitFailureResult(
        ctx,
        publisher,
        request.requestId,
        producedAt,
        failureCode,
        error.message,
        true
      );
      return;
    }

    incrementError(
      ctx.healthContext,
      "generation_error"
    );
    incrementGeneration(ctx.healthContext, "failure");
    logger.error({ error: serializeError(error) }, "Failed to process summary request");
    throw error;
  }
}
