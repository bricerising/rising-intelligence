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
import type {
  EvidenceStrategy,
  LlmProvider,
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

type EvidenceSelectionStrategy = <T extends RawEventForSelection>(
  events: T[],
  maxCount: number
) => T[];

function sortByEngagementThenRecency<T extends RawEventForSelection>(events: T[]): T[] {
  return [...events].sort((left, right) => {
    const leftScore = left.engagementScore ?? 0;
    const rightScore = right.engagementScore ?? 0;
    if (rightScore !== leftScore) {
      return rightScore - leftScore;
    }
    return right.fetchedAt.getTime() - left.fetchedAt.getTime();
  });
}

function selectEvidenceByRecency<T extends RawEventForSelection>(events: T[], maxCount: number): T[] {
  // Upstream query orders by fetchedAt DESC.
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

const EVIDENCE_SELECTION_STRATEGIES: Record<EvidenceStrategy, EvidenceSelectionStrategy> = {
  recency: selectEvidenceByRecency,
  engagement: selectEvidenceByEngagement,
  diversity: selectEvidenceByDiversity,
};

function selectEvidence<T extends RawEventForSelection>(
  events: T[],
  strategy: EvidenceStrategy,
  maxCount: number
): T[] {
  if (events.length === 0 || maxCount <= 0) {
    return [];
  }

  return EVIDENCE_SELECTION_STRATEGIES[strategy](events, maxCount);
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
  const rankedTopicKeys = new Set(rankedTopics.map((topic) => topic.topic));
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
    const fetchedEvents = await ctx.prisma.rawEvent.findMany({
      where: {
        topics: {
          hasSome: [...rankedTopicKeys],
        },
        fetchedAt: {
          gte: lookbackStart,
          lte: request.requestedAt,
        },
        url: {
          not: null, // Only fetch events with URLs for grounding
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
    allEvents = fetchedEvents.filter(
      (event): event is (typeof fetchedEvents)[number] & { url: string } => event.url !== null
    );
    ctx.healthContext.postgresHealthy = true;
  } catch (error) {
    ctx.healthContext.postgresHealthy = false;
    throw error;
  }

  // Partition events by topic
  const eventsByTopic = new Map<string, typeof allEvents>();
  for (const event of allEvents) {
    for (const topicKey of event.topics) {
      if (!rankedTopicKeys.has(topicKey)) {
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
        textExcerpt: event.text.slice(0, EVIDENCE_EXCERPT_MAX_LENGTH),
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
      evidenceStrategy,
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

function selectPrimaryMetric(topic: ParsedSummaryTopic) {
  return topic.metrics.find((metric) => metric.window === 2) ?? topic.metrics[0] ?? null;
}

function buildInternalHighlight(topic: ParsedSummaryTopic): NormalizedHighlight {
  const primaryMetric = selectPrimaryMetric(topic);
  const citations = groundingFacade.dedupeCanonicalUrls(topic.evidence.map((evidence) => evidence.url));
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
    citations: groundingFacade.dedupeCanonicalUrls(highlight.citations),
  };
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
  const topicLandscapeLines =
    highlights.length > 0
      ? highlights
          .slice(0, 3)
          .map((highlight) => `- **${highlight.topic}**: ${highlight.why_it_matters}`)
      : ["- Limited coverage: no grounded highlights were produced for this request."];

  return [
    "# State of Signals and Where They're Going",
    "",
    "## Method and scope",
    `This report summarizes topic-level evidence gathered over ${timeframe}${timezone ? ` (${timezone})` : ""}.`,
    "",
    "## Dominant shifts",
    `Current signals are clustering around ${topTopicSentence}, indicating momentum is moving from isolated updates to coordinated pattern-level changes.`,
    "",
    "## Topic landscape",
    ...topicLandscapeLines,
    "",
    "## Risk and governance",
    "As operational depth increases, runtime policy controls, review gates, and continuous evaluation remain the gating requirements for safe deployment.",
    "",
    "## Execution and economics",
    "Execution quality is increasingly shaped by workflow automation and routing decisions that balance latency, quality, and spend.",
    "",
    "## Outlook",
    "Near-term advantage is likely to come from teams that combine grounded trend monitoring with fast operational experimentation.",
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
  const maxTopics = request.budget?.maxTopics ?? request.topics.length;
  const maxEvidencePerTopic =
    request.budget?.maxEvidencePerTopic ??
    Math.max(...request.topics.map((topic) => topic.evidence.length), 0);
  const maxOutputTokens = request.budget?.maxOutputTokens ?? 1200;

  const promptSections = [
    "You are generating a human-readable engineering intelligence brief from a structured summary request.",
    "Use only the evidence included in SUMMARY_REQUEST_JSON. Do not invent facts or URLs.",
    "Each highlight must include concrete what_happened, why_it_matters, suggested_action, and citations.",
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
  if (evidenceUrls.size === 0) {
    throw new NonRetryableProcessingError("No evidence URLs were provided in the summary request");
  }

  const groundedHighlights = highlights
    .map((highlight) => ({
      ...highlight,
      citations: groundingFacade.filterGroundedCitations(highlight.citations, evidenceUrls),
    }))
    .filter((highlight) => highlight.citations.length > 0);

  if (groundedHighlights.length === 0) {
    throw new NonRetryableProcessingError(
      "Brief generation produced no grounded highlights with valid evidence citations"
    );
  }

  return groundedHighlights;
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
  notes = appendCoverageWarnings(notes, request);
  notes = groundingFacade.enforceGroundedNotes(request, notes, toGroundingError);

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

function buildLlmBackedSuccessResult(
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number,
  llmResponse: ParsedLlmResponse,
  defaultProvider: string,
  defaultModel: string
): SuccessResult {
  const maxTopics = request.budget?.maxTopics ?? llmResponse.highlights.length;
  const highlights = enforceGroundedHighlights(
    request,
    llmResponse.highlights.slice(0, maxTopics).map(normalizeLlmHighlight)
  );
  const inputTokens = llmResponse.usage?.prompt_tokens ?? estimateTokenCount(JSON.stringify(request));
  const outputTokens =
    llmResponse.usage?.completion_tokens ?? estimateTokenCount(JSON.stringify(highlights));

  let notes = llmResponse.notes?.trim() || deriveDefaultNotes(request, highlights);
  notes = appendCoverageWarnings(notes, request);
  notes = groundingFacade.enforceGroundedNotes(request, notes, toGroundingError);

  return buildSuccessPayload(
    request,
    producedAt,
    llmResponse.title,
    highlights,
    notes,
    llmResponse.meta?.provider?.trim() || defaultProvider,
    llmResponse.meta?.model?.trim() || defaultModel,
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
    "http-v1"
  );
}

async function buildCodexCliSuccessResult(input: BuildSuccessResultInput): Promise<SuccessResult> {
  const { ctx, request, producedAt, estimatedCostUsd } = input;
  const llmResponse = await callCodexCliLlm(ctx.config, request, ctx.logger, ctx.healthContext);
  const defaultModel = ctx.config.LLM_CODEX_MODEL.trim() || "codex-cli";

  return buildLlmBackedSuccessResult(
    request,
    producedAt,
    estimatedCostUsd,
    llmResponse,
    "codex-cli",
    defaultModel
  );
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

interface BudgetReservationStrategy {
  reserve(input: BudgetReservationInput): Promise<BudgetReservationResult | null>;
}

class RedisBudgetReservationStrategy implements BudgetReservationStrategy {
  async reserve(input: BudgetReservationInput): Promise<BudgetReservationResult | null> {
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
        return null;
      }

      const budgetDate = toBudgetDate(dateKey);
      void prisma.briefBudgetTracking.upsert({
        where: { date: budgetDate },
        create: {
          date: budgetDate,
          spentUsd: amountUsd,
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

      return cached;
    } catch {
      return null;
    }
  }
}

class PostgresBudgetReservationStrategy implements BudgetReservationStrategy {
  async reserve(input: BudgetReservationInput): Promise<BudgetReservationResult> {
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
      return { reserved: false, spentUsd };
    }

    const latest = await prisma.briefBudgetTracking.findUnique({
      where: { date: budgetDate },
      select: { spentUsd: true },
    });
    const newSpent = Number(latest?.spentUsd ?? Number(record.spentUsd) + amountUsd);
    await syncBudgetCacheBestEffort(redis, dateKey, newSpent, logger);
    return { reserved: true, spentUsd: newSpent };
  }
}

const BUDGET_RESERVATION_STRATEGIES: readonly BudgetReservationStrategy[] = [
  new RedisBudgetReservationStrategy(),
  new PostgresBudgetReservationStrategy(),
];

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

  for (const strategy of BUDGET_RESERVATION_STRATEGIES) {
    const result = await strategy.reserve(input);
    if (result !== null) {
      return result;
    }
  }

  throw new Error("No budget reservation strategy produced a result");
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
  publisher: BriefResultPublisher
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
  publisher: BriefResultPublisher,
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
  const publisher = createBriefResultPublisher({
    producer: ctx.producer,
    logger,
    topic: ctx.config.KAFKA_TOPIC_SUMMARY_RESULTS,
  });
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

    incrementError(
      ctx.healthContext,
      error instanceof LlmGenerationError ? "llm_error" : "generation_error"
    );
    incrementGeneration(ctx.healthContext, "failure");
    logger.error({ error: serializeError(error) }, "Failed to process summary request");
    throw error;
  }
}
