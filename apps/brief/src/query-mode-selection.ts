import { Prisma, Source } from "@rising-intelligence/db";
import { z } from "zod";
import type { EvidenceStrategy } from "./types.js";
import type { TopicGlobMatcherSet } from "./topic-glob.js";

export interface RawEventForSelection {
  eventId: string;
  source: Source;
  publishedAt: Date | null;
  fetchedAt: Date;
  engagementScore: number | null;
}

export interface QueryModeRawEvent extends RawEventForSelection {
  url: string;
  title: string | null;
  text: string;
  topics: string[];
}

interface EvidenceSelectionStrategyHandler {
  readonly name: EvidenceStrategy;
  select<T extends RawEventForSelection>(events: readonly T[], maxCount: number): T[];
}

const CURATED_SOURCES = new Set<Source>([Source.rss, Source.news, Source.github]);
const DISCUSSION_SOURCES = new Set<Source>([
  Source.reddit,
  Source.hackernews,
  Source.bluesky,
  Source.mastodon,
]);

type DiversitySourceBucket = "curated" | "discussion" | "other";

interface DiversitySourceBucketStrategy {
  readonly name: DiversitySourceBucket;
  matches(source: Source): boolean;
}

const DIVERSITY_SOURCE_BUCKET_STRATEGIES: ReadonlyArray<DiversitySourceBucketStrategy> = [
  {
    name: "curated",
    matches(source) {
      return CURATED_SOURCES.has(source);
    },
  },
  {
    name: "discussion",
    matches(source) {
      return DISCUSSION_SOURCES.has(source);
    },
  },
  {
    name: "other",
    matches() {
      return true;
    },
  },
];

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

interface TopicRelevanceRuleContext {
  event: QueryModeRawEvent;
  matcher: TopicRelevanceMatcher;
}

interface TopicRelevanceRule {
  readonly name: string;
  evaluate(context: TopicRelevanceRuleContext): boolean | null;
}

export interface TopicRelevanceMatcherCache {
  resolve(
    topicKey: string,
    build: (topicKey: string) => TopicRelevanceMatcher | null
  ): TopicRelevanceMatcher | null;
}

const DEFAULT_TOPIC_RELEVANCE_MATCHER_CACHE_MAX_ENTRIES = 512;

class LruTopicRelevanceMatcherCache implements TopicRelevanceMatcherCache {
  private readonly cache = new Map<string, TopicRelevanceMatcher | null>();

  constructor(private readonly maxEntries: number) {
    if (!Number.isInteger(maxEntries) || maxEntries <= 0) {
      throw new Error("Topic relevance matcher cache maxEntries must be a positive integer");
    }
  }

  resolve(
    topicKey: string,
    build: (topicKey: string) => TopicRelevanceMatcher | null
  ): TopicRelevanceMatcher | null {
    if (this.cache.has(topicKey)) {
      const cached = this.cache.get(topicKey) ?? null;
      // Refresh insertion order to keep recently-used keys.
      this.cache.delete(topicKey);
      this.cache.set(topicKey, cached);
      return cached;
    }

    const matcher = build(topicKey);
    this.cache.set(topicKey, matcher);
    this.evictLeastRecentlyUsedIfNeeded();
    return matcher;
  }

  private evictLeastRecentlyUsedIfNeeded(): void {
    if (this.cache.size <= this.maxEntries) {
      return;
    }

    const oldestKey = this.cache.keys().next().value;
    if (typeof oldestKey === "string") {
      this.cache.delete(oldestKey);
    }
  }
}

export function createTopicRelevanceMatcherCache(
  maxEntries = DEFAULT_TOPIC_RELEVANCE_MATCHER_CACHE_MAX_ENTRIES
): TopicRelevanceMatcherCache {
  return new LruTopicRelevanceMatcherCache(maxEntries);
}

const topicRelevanceMatcherCache = createTopicRelevanceMatcherCache();

const TrendSnapshotTopicSchema = z.object({
  topic: z.string().min(1),
  score: z.coerce.number().default(0),
  volume: z.coerce.number().default(0),
  acceleration: z.coerce.number().default(0),
});

const TrendSnapshotPayloadSchema = z.object({
  topics: z.array(TrendSnapshotTopicSchema).default([]),
});

interface RankedTopicAccumulator {
  weightedScore: number;
  weightedVolume: number;
  weightedAcceleration: number;
  weightSum: number;
  latestGeneratedAtMs: number;
}

export interface RankedTopicScore {
  topic: string;
  score: number;
  volume: number;
  acceleration: number;
  latestGeneratedAtMs: number;
}

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
  return topicRelevanceMatcherCache.resolve(topicKey, buildTopicRelevanceMatcher);
}

function countRegexMatches(content: string, regex: RegExp): number {
  if (!content) {
    return 0;
  }
  const globalRegex = new RegExp(regex.source, "gi");
  const matches = content.match(globalRegex);
  return matches ? matches.length : 0;
}

const TITLE_OR_URL_TOPIC_RELEVANCE_RULE: TopicRelevanceRule = {
  name: "title_or_url",
  evaluate({ event, matcher }) {
    const titleAndUrl = `${event.title ?? ""} ${event.url}`.trim();
    const titleOrUrlMatch = matcher.exactTermRegexes.some((regex) => regex.test(titleAndUrl));
    return titleOrUrlMatch ? true : null;
  },
};

const BODY_MATCH_THRESHOLD_TOPIC_RELEVANCE_RULE: TopicRelevanceRule = {
  name: "body_match_threshold",
  evaluate({ event, matcher }) {
    let bodyMatchCount = 0;
    for (const regex of matcher.exactTermRegexes) {
      bodyMatchCount += countRegexMatches(event.text, regex);
      if (bodyMatchCount >= TOPIC_RELEVANCE_MIN_BODY_MATCHES) {
        return true;
      }
    }
    return false;
  },
};

const TOPIC_RELEVANCE_RULES: ReadonlyArray<TopicRelevanceRule> = [
  TITLE_OR_URL_TOPIC_RELEVANCE_RULE,
  BODY_MATCH_THRESHOLD_TOPIC_RELEVANCE_RULE,
];

export function countTopicRelevanceTermMatches(
  topicKey: string,
  value: string,
  fallbackWhenNoMatcher = 0
): number {
  const matcher = getTopicRelevanceMatcher(topicKey);
  if (!matcher) {
    return fallbackWhenNoMatcher;
  }

  return matcher.exactTermRegexes.reduce(
    (count, regex) => count + countRegexMatches(value, regex),
    0
  );
}

function getEventRecencyTime(event: RawEventForSelection): number {
  return (event.publishedAt ?? event.fetchedAt).getTime();
}

function sortByEngagementThenRecency<T extends RawEventForSelection>(events: readonly T[]): T[] {
  return [...events].sort((left, right) => {
    const leftScore = left.engagementScore ?? 0;
    const rightScore = right.engagementScore ?? 0;
    if (rightScore !== leftScore) {
      return rightScore - leftScore;
    }
    return getEventRecencyTime(right) - getEventRecencyTime(left);
  });
}

function resolveDiversitySourceBucket(source: Source): DiversitySourceBucket {
  for (const strategy of DIVERSITY_SOURCE_BUCKET_STRATEGIES) {
    if (strategy.matches(source)) {
      return strategy.name;
    }
  }
  return "other";
}

function selectEvidenceByRecency<T extends RawEventForSelection>(events: readonly T[], maxCount: number): T[] {
  // Upstream query orders by publishedAt DESC then fetchedAt DESC.
  return events.slice(0, maxCount);
}

function selectEvidenceByEngagement<T extends RawEventForSelection>(
  events: readonly T[],
  maxCount: number
): T[] {
  return sortByEngagementThenRecency(events).slice(0, maxCount);
}

function selectEvidenceByDiversity<T extends RawEventForSelection>(
  events: readonly T[],
  maxCount: number
): T[] {
  const eventsByBucket: Record<DiversitySourceBucket, T[]> = {
    curated: [],
    discussion: [],
    other: [],
  };

  for (const event of events) {
    eventsByBucket[resolveDiversitySourceBucket(event.source)].push(event);
  }

  const curated = eventsByBucket.curated;
  const discussion = eventsByBucket.discussion;
  const other = eventsByBucket.other;

  const selected: T[] = [];
  const selectedSet = new Set<T>();
  const seed = (event: T | undefined): void => {
    if (!event || selected.length >= maxCount || selectedSet.has(event)) {
      return;
    }
    selected.push(event);
    selectedSet.add(event);
  };

  seed(curated[0]);
  seed(discussion[0]);

  const remaining = [...curated, ...discussion, ...other].filter((event) => !selectedSet.has(event));

  selected.push(...sortByEngagementThenRecency(remaining).slice(0, maxCount - selected.length));
  return selected;
}

const RECENCY_EVIDENCE_SELECTION_STRATEGY: EvidenceSelectionStrategyHandler = {
  name: "recency",
  select: selectEvidenceByRecency,
};

const ENGAGEMENT_EVIDENCE_SELECTION_STRATEGY: EvidenceSelectionStrategyHandler = {
  name: "engagement",
  select: selectEvidenceByEngagement,
};

const DIVERSITY_EVIDENCE_SELECTION_STRATEGY: EvidenceSelectionStrategyHandler = {
  name: "diversity",
  select: selectEvidenceByDiversity,
};

const EVIDENCE_SELECTION_STRATEGIES = {
  recency: RECENCY_EVIDENCE_SELECTION_STRATEGY,
  engagement: ENGAGEMENT_EVIDENCE_SELECTION_STRATEGY,
  diversity: DIVERSITY_EVIDENCE_SELECTION_STRATEGY,
} as const satisfies Record<EvidenceStrategy, EvidenceSelectionStrategyHandler>;

function resolveEvidenceSelectionStrategy(strategy: string): EvidenceSelectionStrategyHandler {
  if (!Object.prototype.hasOwnProperty.call(EVIDENCE_SELECTION_STRATEGIES, strategy)) {
    throw new Error(`Unsupported evidence strategy: ${strategy}`);
  }

  return EVIDENCE_SELECTION_STRATEGIES[strategy as EvidenceStrategy];
}

function computeRecentWeight(snapshotGeneratedAt: Date, requestedAt: Date): number {
  const ageMs = Math.max(0, requestedAt.getTime() - snapshotGeneratedAt.getTime());
  const ageHours = ageMs / (60 * 60 * 1000);
  return 1 / (1 + ageHours);
}

export function isEventRelevantToTopic(event: QueryModeRawEvent, topicKey: string): boolean {
  const matcher = getTopicRelevanceMatcher(topicKey);
  if (!matcher) {
    return true;
  }

  const context: TopicRelevanceRuleContext = { event, matcher };
  for (const rule of TOPIC_RELEVANCE_RULES) {
    const decision = rule.evaluate(context);
    if (decision !== null) {
      return decision;
    }
  }
  return false;
}

export function selectEvidence<T extends RawEventForSelection>(
  events: readonly T[],
  strategy: EvidenceStrategy,
  maxCount: number
): T[] {
  if (events.length === 0 || maxCount <= 0) {
    return [];
  }

  const selectionStrategy = resolveEvidenceSelectionStrategy(strategy);
  return selectionStrategy.select(events, maxCount);
}

export function getTopLevelTopicGroup(topicKey: string): string {
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

export function selectTopLevelTopicGroups(
  rankedTopics: readonly RankedTopicScore[],
  maxTopicGroups: number
): Set<string> {
  if (!Number.isFinite(maxTopicGroups) || maxTopicGroups <= 0) {
    return new Set();
  }
  const boundedMaxTopicGroups = Math.floor(maxTopicGroups);
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
      .slice(0, boundedMaxTopicGroups)
      .map(([group]) => group)
  );
}

export function rankTopicsFromSnapshots(
  snapshots: ReadonlyArray<{ generatedAt: Date; snapshot: Prisma.JsonValue }>,
  requestedAt: Date,
  topicMatchers: TopicGlobMatcherSet
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
      if (!topic || !topicMatchers.matches(topic)) {
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

  return [...byTopic.entries()]
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
}
