import {
  createBriefingJobPayload,
  parseCanonicalSource,
  type BriefingJobPayload,
  type BriefingLlmProvider as LlmProvider,
} from "@rising-intelligence/pipeline";
import {
  createProducerConnection,
  type PipelineLogger,
} from "@rising-intelligence/pipeline/transport";
import { createPrismaClient } from "@rising-intelligence/db";
import {
  BRIEF_CONFIG_DEFAULTS,
  BRIEF_KAFKA_TOPICS,
  BRIEF_TRIGGER_DEFAULTS,
} from "@rising-intelligence/brief/contract";
import { getEnvString } from "@rising-intelligence/shared/env";
import type { CliFlags } from "../../lib/args.js";
import {
  getBooleanFlag,
  getStringFlag,
  parseKafkaBrokers,
} from "../../lib/flags.js";
import {
  deriveTopicGlobsFromFeedConfigs,
  getRepeatedStringFlag,
  normalizeTopicGlobs,
} from "./feed-config.js";
import {
  setupRequestResultWaiter,
  type RequestResultWaiter,
} from "./result-waiter.js";
import { resolveTopicsDatabaseUrl } from "../topics/database-url.js";

const NOOP_LOGGER: PipelineLogger = {
  error() {},
  warn() {},
  info() {},
  debug() {},
};

type RequestType = "daily" | "threshold";
type QueryEvidenceStrategy = "diversity" | "recency" | "engagement";

interface TriggerBriefCommonConfig {
  kafkaBrokers: string[];
  kafkaClientId: string;
  summaryRequestsTopic: string;
  summaryResultsTopic: string;
  requestId: string;
  requestedAtIso: string;
  requestType: RequestType;
  windows: number[];
  warnings: string[];
  dailyBudgetUsd: number;
  maxTopics: number;
  maxEvidencePerTopic: number;
  maxOutputTokens: number;
  reportTimezone?: string;
  reportStartAtIso?: string;
  reportEndAtIso?: string;
  llmProvider?: LlmProvider;
  dryRun: boolean;
  noWait: boolean;
  timeoutSeconds: number;
}

interface QueryModeTriggerBriefConfig extends TriggerBriefCommonConfig {
  mode: "query";
  queryLookbackDays: number;
  queryTopicGlobs: string[];
  queryMaxEventsPerTopic: number;
  queryEvidenceStrategy: QueryEvidenceStrategy;
}

interface ExplicitModeTriggerBriefConfig extends TriggerBriefCommonConfig {
  mode: "explicit";
  topicKey: string;
  score: number;
  volume: number;
  acceleration: number;
  evidenceUrl: string;
  evidenceSource: ReturnType<typeof parseCanonicalSource>;
  evidenceTitle: string;
  evidenceExcerpt: string;
}

type TriggerBriefConfig = QueryModeTriggerBriefConfig | ExplicitModeTriggerBriefConfig;

interface QueryModeSelection {
  mode: "query";
}

interface ExplicitModeSelection {
  mode: "explicit";
  topicKey: string;
  evidenceUrl: string;
}

type TriggerModeSelection = QueryModeSelection | ExplicitModeSelection;

function parseNumber(rawValue: string, key: string): number {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid numeric value for ${key}: ${rawValue}`);
  }
  return parsed;
}

function getNumberFlag(flags: CliFlags, name: string): number | undefined {
  const value = getStringFlag(flags, name);
  return value === undefined ? undefined : parseNumber(value, `--${name}`);
}

function parseNumberEnv(name: string, rawValue: string | undefined, defaultValue: number): number {
  if (rawValue === undefined) {
    return defaultValue;
  }
  return parseNumber(rawValue, name);
}

function assertPositiveInteger(value: number, key: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Expected ${key} to be a positive integer, received: ${value}`);
  }
  return value;
}

function assertNonNegative(value: number, key: string): number {
  if (value < 0) {
    throw new Error(`Expected ${key} to be non-negative, received: ${value}`);
  }
  return value;
}

function parseRequestType(rawType: string): RequestType {
  const normalized = rawType.trim().toLowerCase();
  if (normalized === "daily" || normalized === "threshold") {
    return normalized;
  }

  throw new Error(`Invalid request type: ${rawType}. Supported values: daily, threshold`);
}

function parseWindow(rawValue: string): number {
  if (!/^\d+$/u.test(rawValue)) {
    throw new Error(`Invalid window value '${rawValue}'. Supported values are 1, 2, 3`);
  }

  const parsed = Number(rawValue);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 3) {
    throw new Error(`Invalid window ${parsed}. Supported values are 1, 2, 3`);
  }

  return parsed;
}

function parseWindows(rawValue: string): number[] {
  const windowTokens = rawValue
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (windowTokens.length === 0) {
    throw new Error(`Invalid windows value: ${rawValue}`);
  }

  const parsed = windowTokens.map(parseWindow);

  return [...new Set(parsed)];
}

function parseIsoDate(rawValue: string): string {
  const parsed = new Date(rawValue);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid ISO timestamp: ${rawValue}`);
  }
  return parsed.toISOString();
}

function parseTopicGlobs(rawValue: string): string[] {
  const globs = rawValue
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (globs.length === 0) {
    throw new Error("topic-globs resolved to an empty value");
  }
  return normalizeTopicGlobs(globs);
}

function resolveQueryTopicGlobs(
  flags: CliFlags
): { queryTopicGlobs: string[]; warnings: string[] } {
  const warnings: string[] = [];
  const explicitTopicGlobsRaw = getStringFlag(flags, "topic-globs");
  const explicitTopicGlobs = explicitTopicGlobsRaw
    ? parseTopicGlobs(explicitTopicGlobsRaw)
    : [];

  const feedConfigPaths = getRepeatedStringFlag(flags, "feed-config")
    .map((path) => path.trim())
    .filter((path) => path.length > 0);

  let derivedTopicGlobs: string[] = [];
  if (feedConfigPaths.length > 0) {
    const derived = deriveTopicGlobsFromFeedConfigs(feedConfigPaths);
    derivedTopicGlobs = derived.topicGlobs;
    warnings.push(...derived.warnings);

    if (derivedTopicGlobs.length === 0 && explicitTopicGlobs.length > 0) {
      warnings.push(
        "No topic globs derived from --feed-config files; proceeding with explicit --topic-globs."
      );
    }
  }

  const merged = normalizeTopicGlobs([...derivedTopicGlobs, ...explicitTopicGlobs]);
  if (merged.length > 0) {
    return { queryTopicGlobs: merged, warnings };
  }

  return { queryTopicGlobs: ["*"], warnings };
}

function parseEvidenceStrategy(rawValue: string): QueryEvidenceStrategy {
  const normalized = rawValue.trim().toLowerCase();
  if (normalized === "diversity" || normalized === "recency" || normalized === "engagement") {
    return normalized;
  }

  throw new Error(
    `Invalid evidence-strategy: ${rawValue}. Must be one of: diversity, recency, engagement`
  );
}

function parseLlmProvider(rawValue: string): LlmProvider {
  const normalized = rawValue.trim().toLowerCase();
  if (normalized === "internal" || normalized === "http" || normalized === "codex-cli") {
    return normalized;
  }

  throw new Error(
    `Invalid llm-provider: ${rawValue}. Must be one of: internal, http, codex-cli`
  );
}

function parseOptionalIsoDate(rawValue: string | undefined, flagName: string): string | undefined {
  if (!rawValue || rawValue.trim().length === 0) {
    return undefined;
  }
  try {
    return parseIsoDate(rawValue);
  } catch {
    throw new Error(`Invalid ISO timestamp for ${flagName}: ${rawValue}`);
  }
}

function parseOptionalTimezone(rawValue: string | undefined): string | undefined {
  if (!rawValue || rawValue.trim().length === 0) {
    return undefined;
  }

  const timezone = rawValue.trim();
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: timezone });
  } catch {
    throw new Error(`Invalid report timezone: ${rawValue}`);
  }
  return timezone;
}

function resolveTimeoutSeconds(flags: CliFlags): number {
  const rawTimeout = getStringFlag(flags, "timeout");
  if (rawTimeout === undefined) {
    return 300;
  }

  return assertPositiveInteger(parseNumber(rawTimeout, "--timeout"), "--timeout");
}

function resolveMode(flags: CliFlags): TriggerModeSelection {
  const topicKey = getStringFlag(flags, "topic-key")?.trim();
  const evidenceUrl = getStringFlag(flags, "evidence-url")?.trim();

  if (topicKey && evidenceUrl) {
    return {
      mode: "explicit",
      topicKey,
      evidenceUrl,
    };
  }

  if (topicKey || evidenceUrl) {
    throw new Error("Explicit mode requires both --topic-key and --evidence-url");
  }

  return { mode: "query" };
}

type TriggerBriefConfigBase = Omit<TriggerBriefCommonConfig, "windows" | "warnings">;

interface ResolveTriggerModeConfigInput<TSelection extends TriggerModeSelection> {
  flags: CliFlags;
  modeSelection: TSelection;
  parsedWindows: number[];
  parsedLookbackDays: number;
  baseConfig: TriggerBriefConfigBase;
}

interface TriggerModeStrategy<
  TSelection extends TriggerModeSelection,
  TConfig extends TriggerBriefConfig,
> {
  readonly mode: TSelection["mode"];
  buildConfig(input: ResolveTriggerModeConfigInput<TSelection>): TConfig;
}

const QUERY_TRIGGER_MODE_STRATEGY: TriggerModeStrategy<
  QueryModeSelection,
  QueryModeTriggerBriefConfig
> = {
  mode: "query",
  buildConfig({
    flags,
    parsedWindows,
    parsedLookbackDays,
    baseConfig,
  }): QueryModeTriggerBriefConfig {
    if (!parsedWindows.includes(2)) {
      throw new Error("Query mode requires TREND_WINDOW_60M (window=2)");
    }

    const queryTopicResolution = resolveQueryTopicGlobs(flags);
    const requestedQueryMaxEvents =
      getNumberFlag(flags, "max-events-per-topic") ??
      parseNumberEnv(
        "BRIEF_MAX_QUERY_EVENTS_PER_TOPIC",
        getEnvString("BRIEF_MAX_QUERY_EVENTS_PER_TOPIC"),
        BRIEF_CONFIG_DEFAULTS.briefMaxQueryEventsPerTopic
      );
    const queryMaxEventsPerTopic = Math.min(
      assertPositiveInteger(requestedQueryMaxEvents, "--max-events-per-topic"),
      baseConfig.maxEvidencePerTopic
    );
    const queryEvidenceStrategyRaw =
      getStringFlag(flags, "evidence-strategy") || BRIEF_TRIGGER_DEFAULTS.queryEvidenceStrategy;

    return {
      ...baseConfig,
      mode: "query",
      windows: [2],
      warnings: queryTopicResolution.warnings,
      queryLookbackDays: parsedLookbackDays,
      queryTopicGlobs: queryTopicResolution.queryTopicGlobs,
      queryMaxEventsPerTopic,
      queryEvidenceStrategy: parseEvidenceStrategy(queryEvidenceStrategyRaw),
    };
  },
};

const EXPLICIT_TRIGGER_MODE_STRATEGY: TriggerModeStrategy<
  ExplicitModeSelection,
  ExplicitModeTriggerBriefConfig
> = {
  mode: "explicit",
  buildConfig({
    flags,
    modeSelection,
    parsedWindows,
    baseConfig,
  }): ExplicitModeTriggerBriefConfig {
    const score = getNumberFlag(flags, "score") ?? 8.5;
    const volume = getNumberFlag(flags, "volume") ?? 100;
    const acceleration = getNumberFlag(flags, "acceleration") ?? 0.4;
    const evidenceSourceRaw = getStringFlag(flags, "evidence-source") || "rss";

    return {
      ...baseConfig,
      mode: "explicit",
      windows: parsedWindows,
      warnings: [],
      topicKey: modeSelection.topicKey,
      score: assertNonNegative(score, "--score"),
      volume: assertNonNegative(volume, "--volume"),
      acceleration,
      evidenceUrl: modeSelection.evidenceUrl,
      evidenceSource: parseCanonicalSource(evidenceSourceRaw),
      evidenceTitle:
        getStringFlag(flags, "evidence-title") || "Manual summary request trigger",
      evidenceExcerpt:
        getStringFlag(flags, "evidence-excerpt") ||
        "Manual summary request trigger generated via riops.",
    };
  },
};

type TriggerMode = TriggerModeSelection["mode"];

type TriggerModeSelectionByMode = {
  query: QueryModeSelection;
  explicit: ExplicitModeSelection;
};

type TriggerBriefConfigByMode = {
  query: QueryModeTriggerBriefConfig;
  explicit: ExplicitModeTriggerBriefConfig;
};

type TriggerModeStrategyByMode = {
  [TMode in TriggerMode]: TriggerModeStrategy<
    TriggerModeSelectionByMode[TMode],
    TriggerBriefConfigByMode[TMode]
  >;
};

const TRIGGER_MODE_STRATEGY_BY_MODE: TriggerModeStrategyByMode = {
  query: QUERY_TRIGGER_MODE_STRATEGY,
  explicit: EXPLICIT_TRIGGER_MODE_STRATEGY,
};

interface TriggerModeStrategyFactory {
  buildConfig(input: ResolveTriggerModeConfigInput<TriggerModeSelection>): TriggerBriefConfig;
}

class DefaultTriggerModeStrategyFactory implements TriggerModeStrategyFactory {
  constructor(
    private readonly strategies: TriggerModeStrategyByMode
  ) {
    if (this.strategies.query.mode !== "query") {
      throw new Error(
        `Trigger mode strategy key "query" mismatched strategy mode "${this.strategies.query.mode}"`
      );
    }
    if (this.strategies.explicit.mode !== "explicit") {
      throw new Error(
        `Trigger mode strategy key "explicit" mismatched strategy mode "${this.strategies.explicit.mode}"`
      );
    }
  }

  buildConfig(input: ResolveTriggerModeConfigInput<TriggerModeSelection>): TriggerBriefConfig {
    if (input.modeSelection.mode === "query") {
      return this.strategies.query.buildConfig({
        ...input,
        modeSelection: input.modeSelection,
      });
    }

    return this.strategies.explicit.buildConfig({
      ...input,
      modeSelection: input.modeSelection,
    });
  }
}

function createTriggerModeStrategyFactory(
  strategies: TriggerModeStrategyByMode
): TriggerModeStrategyFactory {
  return new DefaultTriggerModeStrategyFactory(strategies);
}

const TRIGGER_MODE_STRATEGY_FACTORY: TriggerModeStrategyFactory =
  createTriggerModeStrategyFactory(TRIGGER_MODE_STRATEGY_BY_MODE);

class TriggerBriefConfigBuilder {
  constructor(
    private readonly flags: CliFlags,
    private readonly modeStrategyFactory: TriggerModeStrategyFactory
  ) {}

  build(): TriggerBriefConfig {
    const kafkaBrokersRaw =
      getStringFlag(this.flags, "kafka-brokers")
      || getEnvString("KAFKA_BROKERS")
      || BRIEF_CONFIG_DEFAULTS.kafkaBrokers;
    const requestedAtRaw = getStringFlag(this.flags, "requested-at") || new Date().toISOString();
    const requestTypeRaw = getStringFlag(this.flags, "type") || BRIEF_TRIGGER_DEFAULTS.requestType;
    const windowsRaw =
      getStringFlag(this.flags, "windows") || BRIEF_TRIGGER_DEFAULTS.windows.join(",");
    const modeSelection = resolveMode(this.flags);
    const parsedWindows = parseWindows(windowsRaw);

    const dailyBudgetUsd =
      getNumberFlag(this.flags, "daily-budget-usd") ??
      parseNumberEnv(
        "BRIEF_DAILY_BUDGET_USD",
        getEnvString("BRIEF_DAILY_BUDGET_USD") || getEnvString("LLM_DAILY_BUDGET_USD"),
        BRIEF_TRIGGER_DEFAULTS.dailyBudgetUsd
      );
    const maxTopics =
      getNumberFlag(this.flags, "max-topics") ??
      parseNumberEnv("BRIEF_MAX_TOPICS", getEnvString("BRIEF_MAX_TOPICS"), BRIEF_TRIGGER_DEFAULTS.maxTopics);
    const maxEvidencePerTopic =
      getNumberFlag(this.flags, "max-evidence-per-topic") ??
      parseNumberEnv(
        "BRIEF_MAX_EVIDENCE_PER_TOPIC",
        getEnvString("BRIEF_MAX_EVIDENCE_PER_TOPIC"),
        BRIEF_TRIGGER_DEFAULTS.maxEvidencePerTopic
      );
    const maxOutputTokens =
      getNumberFlag(this.flags, "max-output-tokens") ??
      parseNumberEnv(
        "BRIEF_MAX_OUTPUT_TOKENS",
        getEnvString("BRIEF_MAX_OUTPUT_TOKENS"),
        BRIEF_TRIGGER_DEFAULTS.maxOutputTokens
      );

    const maxLookbackDays =
      getNumberFlag(this.flags, "max-lookback-days") ??
      parseNumberEnv(
        "BRIEF_MAX_LOOKBACK_DAYS",
        getEnvString("BRIEF_MAX_LOOKBACK_DAYS"),
        BRIEF_CONFIG_DEFAULTS.briefMaxLookbackDays
      );
    const lookbackDays =
      getNumberFlag(this.flags, "lookback-days") ??
      parseNumberEnv(
        "BRIEF_DEFAULT_LOOKBACK_DAYS",
        getEnvString("BRIEF_DEFAULT_LOOKBACK_DAYS"),
        BRIEF_CONFIG_DEFAULTS.briefDefaultLookbackDays
      );

    const parsedMaxLookbackDays = assertPositiveInteger(maxLookbackDays, "--max-lookback-days");
    const parsedLookbackDays = assertPositiveInteger(lookbackDays, "--lookback-days");
    if (parsedLookbackDays > parsedMaxLookbackDays) {
      throw new Error(
        `--lookback-days (${parsedLookbackDays}) must be <= --max-lookback-days (${parsedMaxLookbackDays})`
      );
    }

    const reportTimezone = parseOptionalTimezone(getStringFlag(this.flags, "report-timezone"));
    const reportStartAtIso = parseOptionalIsoDate(
      getStringFlag(this.flags, "report-start-at"),
      "--report-start-at"
    );
    const reportEndAtIso = parseOptionalIsoDate(
      getStringFlag(this.flags, "report-end-at"),
      "--report-end-at"
    );

    if (reportStartAtIso && reportEndAtIso) {
      const startAt = new Date(reportStartAtIso).getTime();
      const endAt = new Date(reportEndAtIso).getTime();
      if (startAt > endAt) {
        throw new Error("--report-start-at must be <= --report-end-at");
      }
    }

    const validatedMaxEvidencePerTopic = assertPositiveInteger(
      maxEvidencePerTopic,
      "--max-evidence-per-topic"
    );
    const baseConfig: TriggerBriefConfigBase = {
      kafkaBrokers: parseKafkaBrokers(kafkaBrokersRaw),
      kafkaClientId: getStringFlag(this.flags, "kafka-client-id") || "riops-brief-trigger",
      summaryRequestsTopic:
        getStringFlag(this.flags, "summary-requests-topic")
        || getEnvString("KAFKA_TOPIC_SUMMARY_REQUESTS")
        || BRIEF_KAFKA_TOPICS.summaryRequests,
      summaryResultsTopic:
        getStringFlag(this.flags, "summary-results-topic")
        || getEnvString("KAFKA_TOPIC_SUMMARY_RESULTS")
        || BRIEF_KAFKA_TOPICS.summaryResults,
      requestId: getStringFlag(this.flags, "request-id") || `manual-${Date.now()}`,
      requestedAtIso: parseIsoDate(requestedAtRaw),
      requestType: parseRequestType(requestTypeRaw),
      dailyBudgetUsd: assertNonNegative(dailyBudgetUsd, "--daily-budget-usd"),
      maxTopics: assertPositiveInteger(maxTopics, "--max-topics"),
      maxEvidencePerTopic: validatedMaxEvidencePerTopic,
      maxOutputTokens: assertPositiveInteger(maxOutputTokens, "--max-output-tokens"),
      reportTimezone,
      reportStartAtIso,
      reportEndAtIso,
      llmProvider:
        parseLlmProvider(
          getStringFlag(this.flags, "llm-provider")
            || getEnvString("BRIEF_LLM_PROVIDER")
            || getEnvString("LLM_PROVIDER")
            || BRIEF_CONFIG_DEFAULTS.llmProvider
        ),
      dryRun: getBooleanFlag(this.flags, "dry-run"),
      noWait: getBooleanFlag(this.flags, "no-wait"),
      timeoutSeconds: resolveTimeoutSeconds(this.flags),
    };

    return this.modeStrategyFactory.buildConfig({
      flags: this.flags,
      modeSelection,
      parsedWindows,
      parsedLookbackDays,
      baseConfig,
    });
  }
}

function resolveConfig(flags: CliFlags): TriggerBriefConfig {
  return new TriggerBriefConfigBuilder(flags, TRIGGER_MODE_STRATEGY_FACTORY).build();
}

function buildTriggerJobPayload(config: TriggerBriefConfig): BriefingJobPayload {
  const commonInput = {
    requestId: config.requestId,
    requestedAt: config.requestedAtIso,
    type: config.requestType,
    windows: config.windows,
    budget: {
      dailyBudgetUsd: config.dailyBudgetUsd,
      maxTopics: config.maxTopics,
      maxEvidencePerTopic: config.maxEvidencePerTopic,
      maxOutputTokens: config.maxOutputTokens,
    },
    report:
      config.reportTimezone !== undefined ||
      config.reportStartAtIso !== undefined ||
      config.reportEndAtIso !== undefined
        ? {
            timezone: config.reportTimezone,
            startAt: config.reportStartAtIso,
            endAt: config.reportEndAtIso,
          }
        : undefined,
    llmProvider: config.llmProvider,
  };

  if (config.mode === "query") {
    return createBriefingJobPayload({
      ...commonInput,
      query: {
        lookbackDays: config.queryLookbackDays,
        topicGlobs: config.queryTopicGlobs,
        maxEventsPerTopic: config.queryMaxEventsPerTopic,
        evidenceStrategy: config.queryEvidenceStrategy,
      },
      topics: [],
    });
  }

  const primaryWindow = config.windows.includes(2) ? 2 : config.windows[0];
  return createBriefingJobPayload({
    ...commonInput,
    topics: [
      {
        topic: config.topicKey,
        metrics: [
          {
            topic: config.topicKey,
            window: primaryWindow,
            score: config.score,
            volume: config.volume,
            acceleration: config.acceleration,
          },
        ],
        evidence: [
          {
            eventId: `${config.requestId}-event-1`,
            source: config.evidenceSource,
            url: config.evidenceUrl,
            title: config.evidenceTitle,
            publishedAt: config.requestedAtIso,
            fetchedAt: config.requestedAtIso,
            textExcerpt: config.evidenceExcerpt,
          },
        ],
      },
    ],
  });
}

interface FreshnessIssue {
  category: string;
  message: string;
}

const CONSUMER_LAG_CATEGORY = "consumer_lag";
const DATABASE_CATEGORY = "database";
const EXPECTED_CONSUMER_GROUPS = ["trends-processor", "persister"] as const;
const MAX_LAG_MESSAGES = 100;
const MAX_LAG_AGE_MS = 300_000;

interface ConsumerLagRecord {
  consumerGroup: string;
  lagMessages: number | bigint;
  updatedAt: Date;
}

interface ConsumerLagFreshnessContext {
  lagRecords: readonly ConsumerLagRecord[];
  nowMs: number;
}

interface ConsumerLagFreshnessCheck {
  readonly name: string;
  evaluate(context: ConsumerLagFreshnessContext): FreshnessIssue[];
}

const REQUIRE_CONSUMER_LAG_RECORDS_CHECK: ConsumerLagFreshnessCheck = {
  name: "require-consumer-lag-records",
  evaluate({ lagRecords }): FreshnessIssue[] {
    if (lagRecords.length > 0) {
      return [];
    }

    return [
      {
        category: CONSUMER_LAG_CATEGORY,
        message: "No consumer lag records found for events.raw",
      },
    ];
  },
};

const STALE_CONSUMER_LAG_RECORDS_CHECK: ConsumerLagFreshnessCheck = {
  name: "stale-consumer-lag-records",
  evaluate({ lagRecords, nowMs }): FreshnessIssue[] {
    if (lagRecords.length === 0) {
      return [];
    }

    const staleRecords = lagRecords.filter(
      (record) => nowMs - record.updatedAt.getTime() > MAX_LAG_AGE_MS
    );
    if (staleRecords.length === 0) {
      return [];
    }

    const groups = [...new Set(staleRecords.map((record) => record.consumerGroup))].join(", ");
    return [
      {
        category: CONSUMER_LAG_CATEGORY,
        message: `Consumer lag records are stale (>5 min old) for: ${groups}`,
      },
    ];
  },
};

const CONSUMER_GROUP_LAG_BUDGET_CHECK: ConsumerLagFreshnessCheck = {
  name: "consumer-group-lag-budget",
  evaluate({ lagRecords }): FreshnessIssue[] {
    if (lagRecords.length === 0) {
      return [];
    }

    const issues: FreshnessIssue[] = [];
    for (const groupId of EXPECTED_CONSUMER_GROUPS) {
      const groupRecords = lagRecords.filter((record) => record.consumerGroup === groupId);
      if (groupRecords.length === 0) {
        issues.push({
          category: CONSUMER_LAG_CATEGORY,
          message: `Missing consumer lag records for ${groupId}`,
        });
        continue;
      }

      const totalLag = groupRecords.reduce(
        (sum, record) => sum + Number(record.lagMessages),
        0
      );
      if (totalLag > MAX_LAG_MESSAGES) {
        issues.push({
          category: CONSUMER_LAG_CATEGORY,
          message: `${groupId} lag is ${totalLag} messages (threshold: ${MAX_LAG_MESSAGES})`,
        });
      }
    }

    return issues;
  },
};

const CONSUMER_LAG_FRESHNESS_CHECKS: readonly ConsumerLagFreshnessCheck[] = [
  REQUIRE_CONSUMER_LAG_RECORDS_CHECK,
  STALE_CONSUMER_LAG_RECORDS_CHECK,
  CONSUMER_GROUP_LAG_BUDGET_CHECK,
];

export function evaluateConsumerLagFreshness(
  lagRecords: readonly ConsumerLagRecord[],
  nowMs = Date.now()
): FreshnessIssue[] {
  const context: ConsumerLagFreshnessContext = {
    lagRecords,
    nowMs,
  };

  return CONSUMER_LAG_FRESHNESS_CHECKS.flatMap((check) => check.evaluate(context));
}

async function checkDataFreshness(flags: CliFlags): Promise<FreshnessIssue[]> {
  const databaseUrl = resolveTopicsDatabaseUrl(flags);

  const prisma = createPrismaClient({ databaseUrl });

  try {
    await prisma.$connect();

    const lagRecords = await prisma.consumerLag.findMany({
      where: {
        topic: "events.raw",
        consumerGroup: { in: [...EXPECTED_CONSUMER_GROUPS] },
      },
    });

    return evaluateConsumerLagFreshness(lagRecords);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return [
      {
        category: DATABASE_CATEGORY,
        message: `Failed to query database: ${errorMessage}`,
      },
    ];
  } finally {
    await prisma.$disconnect();
  }
}

// Note: Collector heartbeat checking would require reading from collector.heartbeat Kafka topic.
// A future check strategy can extend CONSUMER_LAG_FRESHNESS_CHECKS without changing caller flow.

interface BriefResult {
  request_id: string;
  produced_at: string;
  brief?: {
    brief_id: string;
    generated_at: string;
    window: number;
    title: string;
    highlights: Array<{
      topic: string;
      what_happened: string;
      why_it_matters: string;
      suggested_action: string;
      citations: string[];
    }>;
    notes?: string;
    meta?: {
      provider?: string;
      model?: string;
      input_tokens?: number;
      output_tokens?: number;
      estimated_cost_usd?: number;
    };
  };
  failure?: {
    error_code: string;
    error_message: string;
    retryable: boolean;
  };
}

function formatBriefResult(result: BriefResult): string {
  const lines: string[] = [];

  lines.push(`\n${"=".repeat(80)}`);
  lines.push(`Brief Result: ${result.request_id}`);
  lines.push(`Produced at: ${result.produced_at}`);
  lines.push("=".repeat(80));

  if (result.failure) {
    lines.push(`\n❌ FAILED: ${result.failure.error_code}`);
    lines.push(`Message: ${result.failure.error_message}`);
    lines.push(`Retryable: ${result.failure.retryable ? "Yes" : "No"}`);
    return lines.join("\n");
  }

  if (!result.brief) {
    lines.push("\n⚠️  No brief data in result");
    return lines.join("\n");
  }

  const brief = result.brief;
  lines.push(`\n📊 ${brief.title}`);
  lines.push(`Generated: ${brief.generated_at}`);

  if (brief.meta) {
    lines.push(
      `\nMeta: ${brief.meta.provider || "unknown"}/${brief.meta.model || "unknown"} | ` +
        `Tokens: ${brief.meta.input_tokens || 0} in / ${brief.meta.output_tokens || 0} out | ` +
        `Cost: $${(brief.meta.estimated_cost_usd || 0).toFixed(4)}`
    );
  }

  lines.push(`\n${"─".repeat(80)}`);
  lines.push("HIGHLIGHTS");
  lines.push("─".repeat(80));

  for (const highlight of brief.highlights) {
    lines.push(`\n🔹 ${highlight.topic.toUpperCase()}`);
    lines.push(`\n  What happened:\n    ${highlight.what_happened}`);
    lines.push(`\n  Why it matters:\n    ${highlight.why_it_matters}`);
    lines.push(`\n  Suggested action:\n    ${highlight.suggested_action}`);
    if (highlight.citations.length > 0) {
      lines.push(`\n  Citations:`);
      for (const citation of highlight.citations) {
        lines.push(`    - ${citation}`);
      }
    }
  }

  if (brief.notes && brief.notes.trim()) {
    lines.push(`\n${"─".repeat(80)}`);
    lines.push("NOTES");
    lines.push("─".repeat(80));
    lines.push(`\n${brief.notes}`);
  }

  lines.push(`\n${"=".repeat(80)}\n`);
  return lines.join("\n");
}

export async function briefTrigger(flags: CliFlags): Promise<void> {
  const config = resolveConfig(flags);
  const payload = buildTriggerJobPayload(config);

  if (config.warnings.length > 0) {
    for (const warning of config.warnings) {
      console.warn(`⚠️  ${warning}`);
    }
    console.warn("");
  }

  if (config.dryRun) {
    console.log(
      JSON.stringify(
        {
          kafka_brokers: config.kafkaBrokers,
          topic: config.summaryRequestsTopic,
          key: config.requestId,
          mode: config.mode,
          payload,
        },
        null,
        2
      )
    );
    return;
  }

  // Check data freshness
  const freshnessIssues = await checkDataFreshness(flags);
  if (freshnessIssues.length > 0) {
    console.warn("⚠️  Data freshness warnings:");
    for (const issue of freshnessIssues) {
      console.warn(`  [${issue.category}] ${issue.message}`);
    }
    console.warn("Proceeding with brief request anyway...\n");
  }

  // Set up result consumer BEFORE publishing the request to avoid race condition
  let waiter: RequestResultWaiter<BriefResult> | undefined;
  if (!config.noWait) {
    console.log(`⏳ Setting up result listener (timeout: ${config.timeoutSeconds}s)...`);
    waiter = await setupRequestResultWaiter<BriefResult>({
      kafkaBrokers: config.kafkaBrokers,
      kafkaClientId: config.kafkaClientId,
      groupId: `riops-brief-trigger-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
      topic: config.summaryResultsTopic,
      requestId: config.requestId,
      timeoutSeconds: config.timeoutSeconds,
      timeoutErrorMessage: `Timeout waiting for brief result after ${config.timeoutSeconds}s`,
      parseResult(rawValue): BriefResult | null {
        return JSON.parse(rawValue) as BriefResult;
      },
      fromBeginning: true,
      startupDelayMs: 200,
    });
  }

  // Now publish the request
  const producer = await createProducerConnection({
    brokers: config.kafkaBrokers,
    clientId: config.kafkaClientId,
    logger: NOOP_LOGGER,
  });
  try {
    await producer.publish(
      config.summaryRequestsTopic,
      config.requestId,
      Buffer.from(JSON.stringify(payload), "utf-8")
    );
  } finally {
    await producer.disconnect();
  }

  console.log(
    `✅ Published ${config.mode} summary request ${config.requestId} to ${config.summaryRequestsTopic}`
  );

  if (config.noWait) {
    console.log(`Request ID: ${config.requestId}`);
    return;
  }

  // Wait for the result
  try {
    const result = await waiter!.waitForResult();

    const formatted = formatBriefResult(result);
    console.log(formatted);

    if (result.failure) {
      process.exit(1);
    }
  } catch (error) {
    console.error(`\n❌ ${(error as Error).message}`);
    console.error(`Request ID: ${config.requestId}`);
    process.exit(1);
  } finally {
    if (waiter) {
      await waiter.disconnect();
    }
  }
}
