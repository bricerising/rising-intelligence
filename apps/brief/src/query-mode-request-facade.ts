import {
  buildBriefEvidenceRecordFromCollectedContent,
  BRIEF_EVIDENCE_EXCERPT_MAX_LENGTH,
} from "@rising-intelligence/pipeline";
import { TrendWindow, type Prisma, type PrismaClient } from "@rising-intelligence/db";
import type { Logger } from "pino";
import type { Config } from "./config.js";
import type { HealthContext } from "./health.js";
import { toNoCoverageError, NonRetryableProcessingError } from "./processing-errors.js";
import {
  getTopLevelTopicGroup,
  isEventRelevantToTopic,
  rankTopicsFromSnapshots,
  selectEvidence,
  selectTopLevelTopicGroups,
  type QueryModeRawEvent,
} from "./query-mode-selection.js";
import { createTopicGlobMatcherSet } from "./topic-glob.js";
import type {
  EvidenceStrategy,
  ParsedSummaryRequest,
  ParsedSummaryTopic,
} from "./types.js";
import { createPostgresHealthProxy } from "./postgres-health-proxy.js";

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

interface TrendSnapshotRecord {
  generatedAt: Date;
  snapshot: Prisma.JsonValue;
}

interface QueryModeStorage {
  loadTrendSnapshots(
    lookbackStart: Date,
    requestedAt: Date
  ): Promise<TrendSnapshotRecord[]>;
  loadRawEvents(
    topicKeys: readonly string[],
    lookbackStart: Date,
    requestedAt: Date
  ): Promise<QueryModeRawEvent[]>;
}

class PrismaQueryModeStorageAdapter implements QueryModeStorage {
  constructor(private readonly prisma: PrismaClient) {}

  async loadTrendSnapshots(
    lookbackStart: Date,
    requestedAt: Date
  ): Promise<TrendSnapshotRecord[]> {
    return this.prisma.briefTrendSnapshot.findMany({
      where: {
        window: TrendWindow.WINDOW_60M,
        generatedAt: {
          gte: lookbackStart,
          lte: requestedAt,
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
  }

  async loadRawEvents(
    topicKeys: readonly string[],
    lookbackStart: Date,
    requestedAt: Date
  ): Promise<QueryModeRawEvent[]> {
    const fetchedEvents = await this.prisma.rawEvent.findMany({
      where: {
        topics: {
          hasSome: [...topicKeys],
        },
        publishedAt: {
          gte: lookbackStart,
          lte: requestedAt,
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

    const events: QueryModeRawEvent[] = [];
    for (const event of fetchedEvents) {
      if (event.url === null) {
        continue;
      }

      events.push({
        ...event,
        url: event.url,
      });
    }
    return events;
  }
}

function createQueryModeStorage(ctx: QueryModeRequestResolverContext): QueryModeStorage {
  return createPostgresHealthProxy(
    new PrismaQueryModeStorageAdapter(ctx.prisma),
    ctx.healthContext,
    ["loadTrendSnapshots", "loadRawEvents"]
  );
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
  const requested =
    request.query?.maxEventsPerTopic ?? budgetCap ?? config.BRIEF_MAX_QUERY_EVENTS_PER_TOPIC;
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

function resolveEvidenceStrategy(request: ParsedSummaryRequest): EvidenceStrategy {
  return request.query?.evidenceStrategy ?? "diversity";
}

interface ResolvedQueryModeParameters {
  lookbackDays: number;
  topicGlobs: string[];
  maxTopics: number;
  maxEventsPerTopic: number;
  evidenceStrategy: EvidenceStrategy;
  lookbackStart: Date;
}

function resolveQueryModeParameters(
  config: Config,
  request: ParsedSummaryRequest
): ResolvedQueryModeParameters {
  const lookbackDays = resolveLookbackDays(config, request);
  const lookbackStart = new Date(
    request.requestedAt.getTime() - lookbackDays * 24 * 60 * 60 * 1000
  );

  return {
    lookbackDays,
    topicGlobs: resolveTopicGlobs(request),
    maxTopics: resolveMaxTopics(request),
    maxEventsPerTopic: resolveMaxEventsPerTopic(config, request),
    evidenceStrategy: resolveEvidenceStrategy(request),
    lookbackStart,
  };
}

interface RankedTopicSelection {
  rankedTopics: ReturnType<typeof rankTopicsFromSnapshots>;
  selectedRankedTopics: ReturnType<typeof rankTopicsFromSnapshots>;
  selectedTopLevelTopicGroups: Set<string>;
  coverageWarnings: string[];
}

async function loadRankedTopics(
  storage: QueryModeStorage,
  request: ParsedSummaryRequest,
  parameters: ResolvedQueryModeParameters,
  logger: Logger
): Promise<RankedTopicSelection> {
  let topicMatchers: ReturnType<typeof createTopicGlobMatcherSet>;
  try {
    topicMatchers = createTopicGlobMatcherSet(parameters.topicGlobs);
  } catch (error) {
    throw new NonRetryableProcessingError(
      `Invalid topic glob filter: ${error instanceof Error ? error.message : "unknown error"}`,
      "invalid_request"
    );
  }

  const snapshots = await storage.loadTrendSnapshots(
    parameters.lookbackStart,
    request.requestedAt
  );

  const coverageWarnings: string[] = [];
  if (snapshots.length === 0) {
    logger.warn(
      { lookbackDays: parameters.lookbackDays },
      "No trend snapshots found in lookback window"
    );
    coverageWarnings.push("No trend data available for the requested lookback period.");
  }

  const rankedTopics = rankTopicsFromSnapshots(
    snapshots,
    request.requestedAt,
    topicMatchers
  );
  const selectedTopLevelTopicGroups = selectTopLevelTopicGroups(
    rankedTopics,
    parameters.maxTopics
  );
  const selectedRankedTopics = rankedTopics.filter((rankedTopic) =>
    selectedTopLevelTopicGroups.has(getTopLevelTopicGroup(rankedTopic.topic))
  );

  if (selectedRankedTopics.length === 0) {
    logger.warn(
      {
        topicGlobCount: topicMatchers.globs.length,
        lookbackDays: parameters.lookbackDays,
      },
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

interface HydratedTopicSelection {
  topicsWithEvidence: ParsedSummaryTopic[];
  hydratedTopics: ParsedSummaryTopic[];
  relevanceFilteredByTopic: Map<string, number>;
}

async function hydrateTopics(
  storage: QueryModeStorage,
  request: ParsedSummaryRequest,
  selectedRankedTopics: ReturnType<typeof rankTopicsFromSnapshots>,
  parameters: ResolvedQueryModeParameters
): Promise<HydratedTopicSelection> {
  const rankedTopicKeys = new Set(selectedRankedTopics.map((topic) => topic.topic));
  const allEvents = await storage.loadRawEvents(
    [...rankedTopicKeys],
    parameters.lookbackStart,
    request.requestedAt
  );

  const eventsByTopic = new Map<string, typeof allEvents>();
  const relevanceFilteredByTopic = new Map<string, number>();
  for (const event of allEvents) {
    for (const topicKey of event.topics) {
      if (!rankedTopicKeys.has(topicKey)) {
        continue;
      }
      if (!isEventRelevantToTopic(event, topicKey)) {
        relevanceFilteredByTopic.set(
          topicKey,
          (relevanceFilteredByTopic.get(topicKey) ?? 0) + 1
        );
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
    const selectedEvents = selectEvidence(
      topicEvents,
      parameters.evidenceStrategy,
      parameters.maxEventsPerTopic
    );

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
      evidence: selectedEvents.map((event) =>
        buildBriefEvidenceRecordFromCollectedContent(
          {
            eventId: event.eventId,
            source: event.source,
            url: event.url,
            title: event.title,
            publishedAt: event.publishedAt,
            fetchedAt: event.fetchedAt,
            text: event.text,
          },
          {
            excerptMaxLength: BRIEF_EVIDENCE_EXCERPT_MAX_LENGTH,
          }
        )
      ),
    };
  });

  return {
    topicsWithEvidence: hydratedTopics.filter((topic) => topic.evidence.length > 0),
    hydratedTopics,
    relevanceFilteredByTopic,
  };
}

function countRelevanceFilteredEvents(
  relevanceFilteredByTopic: Map<string, number>
): number {
  return [...relevanceFilteredByTopic.values()].reduce(
    (count, filtered) => count + filtered,
    0
  );
}

interface CoverageWarningRuleContext {
  rankedTopicCount: number;
  selectedRankedTopicCount: number;
  hydratedTopicCount: number;
  topicsWithEvidenceCount: number;
  relevanceFilteredCount: number;
  maxTopics: number;
}

interface CoverageWarningRule {
  readonly name: string;
  build(context: CoverageWarningRuleContext): string | null;
}

const TOP_LEVEL_TOPIC_CAP_WARNING_RULE: CoverageWarningRule = {
  name: "top-level-cap",
  build(context): string | null {
    if (context.selectedRankedTopicCount >= context.rankedTopicCount) {
      return null;
    }

    const excludedByTopLevelCap =
      context.rankedTopicCount - context.selectedRankedTopicCount;
    return `${excludedByTopLevelCap} subtopic(s) were excluded by top-level topic cap (${context.maxTopics}).`;
  },
};

const MISSING_EVIDENCE_WARNING_RULE: CoverageWarningRule = {
  name: "missing-evidence",
  build(context): string | null {
    if (context.topicsWithEvidenceCount >= context.hydratedTopicCount) {
      return null;
    }

    const missingTopicCount =
      context.hydratedTopicCount - context.topicsWithEvidenceCount;
    return `${missingTopicCount} ranked topic(s) were excluded due to missing grounded evidence.`;
  },
};

const RELEVANCE_FILTER_WARNING_RULE: CoverageWarningRule = {
  name: "relevance-filter",
  build(context): string | null {
    if (context.relevanceFilteredCount <= 0) {
      return null;
    }

    return `${context.relevanceFilteredCount} candidate event(s) were excluded by topic relevance checks.`;
  },
};

const COVERAGE_WARNING_RULES: readonly CoverageWarningRule[] = [
  TOP_LEVEL_TOPIC_CAP_WARNING_RULE,
  MISSING_EVIDENCE_WARNING_RULE,
  RELEVANCE_FILTER_WARNING_RULE,
];

function summarizeCoverageWarnings(
  baseCoverageWarnings: readonly string[],
  context: CoverageWarningRuleContext
): string[] {
  const warnings = [...baseCoverageWarnings];

  for (const rule of COVERAGE_WARNING_RULES) {
    const warning = rule.build(context);
    if (warning) {
      warnings.push(warning);
    }
  }

  return warnings;
}

interface QueryModeRequestResolutionStrategyInput {
  ctx: QueryModeRequestResolverContext;
  request: ParsedSummaryRequest;
  logger: Logger;
}

interface QueryModeRequestResolutionStrategy {
  readonly name: string;
  canResolve(request: ParsedSummaryRequest): boolean;
  resolve(input: QueryModeRequestResolutionStrategyInput): Promise<ParsedSummaryRequest>;
}

const PASSTHROUGH_REQUEST_RESOLUTION_STRATEGY: QueryModeRequestResolutionStrategy = {
  name: "passthrough",
  canResolve(request): boolean {
    return !isQueryModeRequest(request);
  },
  async resolve({ request }): Promise<ParsedSummaryRequest> {
    return request;
  },
};

async function resolveQueryModeRequest(
  input: QueryModeRequestResolutionStrategyInput
): Promise<ParsedSummaryRequest> {
  const { ctx, request, logger } = input;
  const storage = createQueryModeStorage(ctx);
  const parameters = resolveQueryModeParameters(ctx.config, request);

  const rankedTopicSelection = await loadRankedTopics(
    storage,
    request,
    parameters,
    logger
  );

  const hydratedTopicSelection = await hydrateTopics(
    storage,
    request,
    rankedTopicSelection.selectedRankedTopics,
    parameters
  );

  if (hydratedTopicSelection.topicsWithEvidence.length === 0) {
    logger.warn(
      {
        rankedTopicCount: rankedTopicSelection.selectedRankedTopics.length,
        lookbackDays: parameters.lookbackDays,
      },
      "No evidence found for any ranked topics"
    );
    throw toNoCoverageError(
      "No recent activity was found for matched topics in the lookback window."
    );
  }

  const relevanceFilteredCount = countRelevanceFilteredEvents(
    hydratedTopicSelection.relevanceFilteredByTopic
  );

  const coverageWarnings = summarizeCoverageWarnings(
    rankedTopicSelection.coverageWarnings,
    {
      rankedTopicCount: rankedTopicSelection.rankedTopics.length,
      selectedRankedTopicCount: rankedTopicSelection.selectedRankedTopics.length,
      hydratedTopicCount: hydratedTopicSelection.hydratedTopics.length,
      topicsWithEvidenceCount: hydratedTopicSelection.topicsWithEvidence.length,
      relevanceFilteredCount,
      maxTopics: parameters.maxTopics,
    }
  );

  logger.info(
    {
      lookbackDays: parameters.lookbackDays,
      topicGlobCount: parameters.topicGlobs.length,
      candidateTopicCount: rankedTopicSelection.rankedTopics.length,
      rankedTopicCount: rankedTopicSelection.selectedRankedTopics.length,
      selectedTopLevelTopicCount:
        rankedTopicSelection.selectedTopLevelTopicGroups.size,
      selectedTopicCount: hydratedTopicSelection.topicsWithEvidence.length,
      maxEventsPerTopic: parameters.maxEventsPerTopic,
      coverageWarningCount: coverageWarnings.length,
      relevanceFilteredCount,
    },
    "Resolved query-mode summary request using trend snapshots and raw events"
  );

  return {
    ...request,
    windows: [TREND_WINDOW_60M_PROTO],
    query: {
      lookbackDays: parameters.lookbackDays,
      topicGlobs: parameters.topicGlobs,
      maxEventsPerTopic: parameters.maxEventsPerTopic,
      evidenceStrategy: parameters.evidenceStrategy,
    },
    topics: hydratedTopicSelection.topicsWithEvidence,
    coverageWarnings,
  };
}

const QUERY_MODE_REQUEST_RESOLUTION_STRATEGY: QueryModeRequestResolutionStrategy = {
  name: "query-mode",
  canResolve: isQueryModeRequest,
  resolve: resolveQueryModeRequest,
};

const DEFAULT_QUERY_MODE_REQUEST_RESOLUTION_STRATEGIES: readonly QueryModeRequestResolutionStrategy[] = [
  PASSTHROUGH_REQUEST_RESOLUTION_STRATEGY,
  QUERY_MODE_REQUEST_RESOLUTION_STRATEGY,
];

class PrismaQueryModeRequestResolverFacade implements QueryModeRequestResolver {
  constructor(
    private readonly resolutionStrategies: readonly QueryModeRequestResolutionStrategy[] =
      DEFAULT_QUERY_MODE_REQUEST_RESOLUTION_STRATEGIES
  ) {}

  async resolve(
    ctx: QueryModeRequestResolverContext,
    request: ParsedSummaryRequest,
    logger: Logger
  ): Promise<ParsedSummaryRequest> {
    for (const strategy of this.resolutionStrategies) {
      if (!strategy.canResolve(request)) {
        continue;
      }

      return strategy.resolve({
        ctx,
        request,
        logger,
      });
    }

    throw new Error("No query-mode request resolution strategy matched request");
  }
}

const QUERY_MODE_REQUEST_RESOLVER = new PrismaQueryModeRequestResolverFacade();

export function createQueryModeRequestResolver(): QueryModeRequestResolver {
  return QUERY_MODE_REQUEST_RESOLVER;
}
