import { TrendWindow, type Prisma, type PrismaClient } from "@rising-intelligence/db";
import type { Logger } from "pino";
import type { Config } from "./config.js";
import type { HealthContext } from "./health.js";
import { EVIDENCE_EXCERPT_MAX_LENGTH } from "./grounding-facade.js";
import { toNoCoverageError, NonRetryableProcessingError } from "./processing-errors.js";
import {
  getTopLevelTopicGroup,
  isEventRelevantToTopic,
  rankTopicsFromSnapshots,
  selectEvidence,
  selectTopLevelTopicGroups,
  type QueryModeRawEvent,
} from "./query-mode-selection.js";
import { compileTopicGlobMatchers } from "./topic-glob.js";
import type { ParsedSummaryRequest, ParsedSummaryTopic } from "./types.js";

const TREND_WINDOW_60M_PROTO = 2;
const DEFAULT_QUERY_TOPIC_GLOBS = ["*"];
const DEFAULT_QUERY_MAX_TOPICS = 10;

export interface QueryModeRequestResolverContext {
  config: Config;
  healthContext: HealthContext;
  prisma: PrismaClient;
}

export interface QueryModeRequestResolver {
  resolve(
    ctx: QueryModeRequestResolverContext,
    request: ParsedSummaryRequest,
    logger: Logger
  ): Promise<ParsedSummaryRequest>;
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

async function loadRankedTopics(
  ctx: QueryModeRequestResolverContext,
  request: ParsedSummaryRequest,
  lookbackDays: number,
  lookbackStart: Date,
  topicGlobs: string[],
  maxTopics: number,
  logger: Logger
): Promise<{
  rankedTopics: ReturnType<typeof rankTopicsFromSnapshots>;
  selectedRankedTopics: ReturnType<typeof rankTopicsFromSnapshots>;
  selectedTopLevelTopicGroups: Set<string>;
  coverageWarnings: string[];
}> {
  let topicMatchers: RegExp[];
  try {
    topicMatchers = compileTopicGlobMatchers(topicGlobs);
  } catch (error) {
    throw new NonRetryableProcessingError(
      `Invalid topic glob filter: ${error instanceof Error ? error.message : "unknown error"}`,
      "invalid_request"
    );
  }

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

  return {
    rankedTopics,
    selectedRankedTopics,
    selectedTopLevelTopicGroups,
    coverageWarnings,
  };
}

async function hydrateTopics(
  ctx: QueryModeRequestResolverContext,
  request: ParsedSummaryRequest,
  selectedRankedTopics: ReturnType<typeof rankTopicsFromSnapshots>,
  lookbackStart: Date,
  maxEventsPerTopic: number
): Promise<{
  topicsWithEvidence: ParsedSummaryTopic[];
  hydratedTopics: ParsedSummaryTopic[];
  relevanceFilteredByTopic: Map<string, number>;
}> {
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
          not: null,
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
        url: event.url,
        title: event.title ?? null,
        publishedAt: event.publishedAt,
        fetchedAt: event.fetchedAt,
        textExcerpt: event.text.slice(0, EVIDENCE_EXCERPT_MAX_LENGTH),
      })),
    };
  });

  return {
    topicsWithEvidence: hydratedTopics.filter((topic) => topic.evidence.length > 0),
    hydratedTopics,
    relevanceFilteredByTopic,
  };
}

function summarizeCoverageWarnings(
  rankedTopics: ReturnType<typeof rankTopicsFromSnapshots>,
  selectedRankedTopics: ReturnType<typeof rankTopicsFromSnapshots>,
  hydratedTopics: ParsedSummaryTopic[],
  topicsWithEvidence: ParsedSummaryTopic[],
  relevanceFilteredByTopic: Map<string, number>,
  maxTopics: number,
  coverageWarnings: string[]
): { warnings: string[]; relevanceFilteredCount: number } {
  const warnings = [...coverageWarnings];

  if (selectedRankedTopics.length < rankedTopics.length) {
    const excludedByTopLevelCap = rankedTopics.length - selectedRankedTopics.length;
    warnings.push(
      `${excludedByTopLevelCap} subtopic(s) were excluded by top-level topic cap (${maxTopics}).`
    );
  }

  if (topicsWithEvidence.length < hydratedTopics.length) {
    const missingTopicCount = hydratedTopics.length - topicsWithEvidence.length;
    warnings.push(
      `${missingTopicCount} ranked topic(s) were excluded due to missing grounded evidence.`
    );
  }

  const relevanceFilteredCount = [...relevanceFilteredByTopic.values()].reduce(
    (count, filtered) => count + filtered,
    0
  );
  if (relevanceFilteredCount > 0) {
    warnings.push(
      `${relevanceFilteredCount} candidate event(s) were excluded by topic relevance checks.`
    );
  }

  return {
    warnings,
    relevanceFilteredCount,
  };
}

class PrismaQueryModeRequestResolverFacade implements QueryModeRequestResolver {
  async resolve(
    ctx: QueryModeRequestResolverContext,
    request: ParsedSummaryRequest,
    logger: Logger
  ): Promise<ParsedSummaryRequest> {
    if (!isQueryModeRequest(request)) {
      return request;
    }

    const lookbackDays = resolveLookbackDays(ctx.config, request);
    const topicGlobs = resolveTopicGlobs(request);
    const maxTopics = resolveMaxTopics(request);
    const maxEventsPerTopic = resolveMaxEventsPerTopic(ctx.config, request);
    const lookbackStart = new Date(request.requestedAt.getTime() - lookbackDays * 24 * 60 * 60 * 1000);

    const {
      rankedTopics,
      selectedRankedTopics,
      selectedTopLevelTopicGroups,
      coverageWarnings,
    } = await loadRankedTopics(
      ctx,
      request,
      lookbackDays,
      lookbackStart,
      topicGlobs,
      maxTopics,
      logger
    );

    const {
      topicsWithEvidence,
      hydratedTopics,
      relevanceFilteredByTopic,
    } = await hydrateTopics(ctx, request, selectedRankedTopics, lookbackStart, maxEventsPerTopic);

    if (topicsWithEvidence.length === 0) {
      logger.warn(
        { rankedTopicCount: selectedRankedTopics.length, lookbackDays },
        "No evidence found for any ranked topics"
      );
      throw toNoCoverageError("No recent activity was found for matched topics in the lookback window.");
    }

    const coverageSummary = summarizeCoverageWarnings(
      rankedTopics,
      selectedRankedTopics,
      hydratedTopics,
      topicsWithEvidence,
      relevanceFilteredByTopic,
      maxTopics,
      coverageWarnings
    );

    logger.info(
      {
        lookbackDays,
        topicGlobCount: topicGlobs.length,
        candidateTopicCount: rankedTopics.length,
        rankedTopicCount: selectedRankedTopics.length,
        selectedTopLevelTopicCount: selectedTopLevelTopicGroups.size,
        selectedTopicCount: topicsWithEvidence.length,
        maxEventsPerTopic,
        coverageWarningCount: coverageSummary.warnings.length,
        relevanceFilteredCount: coverageSummary.relevanceFilteredCount,
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
        evidenceStrategy: request.query?.evidenceStrategy ?? "diversity",
      },
      topics: topicsWithEvidence,
      coverageWarnings: coverageSummary.warnings,
    };
  }
}

const QUERY_MODE_REQUEST_RESOLVER = new PrismaQueryModeRequestResolverFacade();

export function createQueryModeRequestResolver(): QueryModeRequestResolver {
  return QUERY_MODE_REQUEST_RESOLVER;
}
