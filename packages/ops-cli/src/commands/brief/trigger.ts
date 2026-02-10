import { Kafka } from "kafkajs";
import { PrismaClient } from "@rising-intelligence/db";
import { getEnvString, parseCanonicalSource } from "@rising-intelligence/shared";

type Flags = Record<string, string | boolean>;
type TriggerMode = "query" | "explicit";

const TOPIC_GLOB_PATTERN = /^[A-Za-z0-9.*?_-]+$/;

interface TriggerBriefConfig {
  kafkaBrokers: string[];
  kafkaClientId: string;
  summaryRequestsTopic: string;
  requestId: string;
  requestedAtIso: string;
  requestType: "daily" | "threshold";
  windows: number[];
  mode: TriggerMode;
  queryLookbackDays: number;
  queryTopicGlobs: string[];
  queryMaxEventsPerTopic: number;
  queryEvidenceStrategy: "diversity" | "recency" | "engagement";
  topicKey?: string;
  score?: number;
  volume?: number;
  acceleration?: number;
  evidenceUrl?: string;
  evidenceSource?: string;
  evidenceTitle?: string;
  evidenceExcerpt?: string;
  dailyBudgetUsd: number;
  maxTopics: number;
  maxEvidencePerTopic: number;
  maxOutputTokens: number;
  dryRun: boolean;
}

function getStringFlag(flags: Flags, name: string): string | undefined {
  const value = flags[name];
  return typeof value === "string" ? value : undefined;
}

function getBooleanFlag(flags: Flags, name: string): boolean {
  return flags[name] === true;
}

function parseNumber(rawValue: string, key: string): number {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid numeric value for ${key}: ${rawValue}`);
  }
  return parsed;
}

function getNumberFlag(flags: Flags, name: string): number | undefined {
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

function parseRequestType(rawType: string): "daily" | "threshold" {
  const normalized = rawType.trim().toLowerCase();
  if (normalized === "daily") {
    return "daily";
  }
  if (normalized === "threshold") {
    return "threshold";
  }
  throw new Error(`Invalid request type: ${rawType}. Supported values: daily, threshold`);
}

function parseWindows(rawValue: string): number[] {
  const parsed = rawValue
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0)
    .map((value) => Number.parseInt(value, 10))
    .filter((value) => Number.isInteger(value));

  if (parsed.length === 0) {
    throw new Error(`Invalid windows value: ${rawValue}`);
  }

  const deduped = [...new Set(parsed)];
  for (const window of deduped) {
    if (window < 1 || window > 3) {
      throw new Error(`Invalid window ${window}. Supported values are 1, 2, 3`);
    }
  }

  return deduped;
}

function parseIsoDate(rawValue: string): string {
  const parsed = new Date(rawValue);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid ISO timestamp: ${rawValue}`);
  }
  return parsed.toISOString();
}

function parseKafkaBrokers(rawValue: string): string[] {
  const brokers = rawValue
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (brokers.length === 0) {
    throw new Error("KAFKA_BROKERS resolved to an empty value");
  }
  return brokers;
}

function parseTopicGlobs(rawValue: string): string[] {
  const globs = rawValue
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (globs.length === 0) {
    throw new Error("topic-globs resolved to an empty value");
  }

  const deduped: string[] = [];
  const seen = new Set<string>();
  for (const glob of globs) {
    if (!TOPIC_GLOB_PATTERN.test(glob)) {
      throw new Error(`Invalid topic glob pattern: ${glob}`);
    }
    if (!seen.has(glob)) {
      seen.add(glob);
      deduped.push(glob);
    }
  }
  return deduped;
}

function parseEvidenceStrategy(rawValue: string): "diversity" | "recency" | "engagement" {
  const normalized = rawValue.trim().toLowerCase();
  if (normalized === "diversity" || normalized === "recency" || normalized === "engagement") {
    return normalized;
  }
  throw new Error(
    `Invalid evidence-strategy: ${rawValue}. Must be one of: diversity, recency, engagement`
  );
}

function resolveMode(flags: Flags): { mode: TriggerMode; topicKey?: string; evidenceUrl?: string } {
  const topicKey = getStringFlag(flags, "topic-key")?.trim();
  const evidenceUrl = getStringFlag(flags, "evidence-url")?.trim();

  const hasTopicKey = Boolean(topicKey);
  const hasEvidenceUrl = Boolean(evidenceUrl);
  if (hasTopicKey !== hasEvidenceUrl) {
    throw new Error("Explicit mode requires both --topic-key and --evidence-url");
  }

  if (hasTopicKey && hasEvidenceUrl) {
    return {
      mode: "explicit",
      topicKey,
      evidenceUrl,
    };
  }
  return { mode: "query" };
}

function resolveConfig(flags: Flags): TriggerBriefConfig {
  const kafkaBrokersRaw =
    getStringFlag(flags, "kafka-brokers") || getEnvString("KAFKA_BROKERS") || "localhost:9092";
  const requestedAtRaw = getStringFlag(flags, "requested-at") || new Date().toISOString();
  const requestTypeRaw = getStringFlag(flags, "type") || "daily";
  const windowsRaw = getStringFlag(flags, "windows") || "2";

  const modeConfig = resolveMode(flags);
  const parsedWindows = parseWindows(windowsRaw);
  const windows =
    modeConfig.mode === "query"
      ? parsedWindows.includes(2)
        ? [2]
        : (() => {
            throw new Error("Query mode requires TREND_WINDOW_60M (window=2)");
          })()
      : parsedWindows;

  const dailyBudgetUsd =
    getNumberFlag(flags, "daily-budget-usd") ??
    parseNumberEnv(
      "BRIEF_DAILY_BUDGET_USD",
      getEnvString("BRIEF_DAILY_BUDGET_USD") || getEnvString("LLM_DAILY_BUDGET_USD"),
      5
    );
  const maxTopics =
    getNumberFlag(flags, "max-topics") ??
    parseNumberEnv("BRIEF_MAX_TOPICS", getEnvString("BRIEF_MAX_TOPICS"), 5);
  const maxEvidencePerTopic =
    getNumberFlag(flags, "max-evidence-per-topic") ??
    parseNumberEnv(
      "BRIEF_MAX_EVIDENCE_PER_TOPIC",
      getEnvString("BRIEF_MAX_EVIDENCE_PER_TOPIC"),
      3
    );
  const maxOutputTokens =
    getNumberFlag(flags, "max-output-tokens") ??
    parseNumberEnv("BRIEF_MAX_OUTPUT_TOKENS", getEnvString("BRIEF_MAX_OUTPUT_TOKENS"), 1200);

  const maxLookbackDays =
    getNumberFlag(flags, "max-lookback-days") ??
    parseNumberEnv("BRIEF_MAX_LOOKBACK_DAYS", getEnvString("BRIEF_MAX_LOOKBACK_DAYS"), 30);
  const lookbackDays =
    getNumberFlag(flags, "lookback-days") ??
    parseNumberEnv(
      "BRIEF_DEFAULT_LOOKBACK_DAYS",
      getEnvString("BRIEF_DEFAULT_LOOKBACK_DAYS"),
      7
    );

  const parsedMaxLookbackDays = assertPositiveInteger(maxLookbackDays, "--max-lookback-days");
  const parsedLookbackDays = assertPositiveInteger(lookbackDays, "--lookback-days");
  if (parsedLookbackDays > parsedMaxLookbackDays) {
    throw new Error(
      `--lookback-days (${parsedLookbackDays}) must be <= --max-lookback-days (${parsedMaxLookbackDays})`
    );
  }

  const queryTopicGlobs = parseTopicGlobs(getStringFlag(flags, "topic-globs") || "*");
  const requestedQueryMaxEvents =
    getNumberFlag(flags, "max-events-per-topic") ??
    parseNumberEnv(
      "BRIEF_MAX_QUERY_EVENTS_PER_TOPIC",
      getEnvString("BRIEF_MAX_QUERY_EVENTS_PER_TOPIC"),
      maxEvidencePerTopic
    );
  const queryMaxEventsPerTopic = Math.min(
    assertPositiveInteger(requestedQueryMaxEvents, "--max-events-per-topic"),
    assertPositiveInteger(maxEvidencePerTopic, "--max-evidence-per-topic")
  );

  const queryEvidenceStrategyRaw = getStringFlag(flags, "evidence-strategy") || "diversity";
  const queryEvidenceStrategy = parseEvidenceStrategy(queryEvidenceStrategyRaw);

  const topicKey = modeConfig.topicKey;
  const evidenceUrl = modeConfig.evidenceUrl;
  const score = modeConfig.mode === "explicit" ? getNumberFlag(flags, "score") ?? 8.5 : undefined;
  const volume = modeConfig.mode === "explicit" ? getNumberFlag(flags, "volume") ?? 100 : undefined;
  const acceleration =
    modeConfig.mode === "explicit" ? getNumberFlag(flags, "acceleration") ?? 0.4 : undefined;
  const evidenceSourceRaw = getStringFlag(flags, "evidence-source") || "rss";

  return {
    kafkaBrokers: parseKafkaBrokers(kafkaBrokersRaw),
    kafkaClientId: getStringFlag(flags, "kafka-client-id") || "riops-brief-trigger",
    summaryRequestsTopic:
      getStringFlag(flags, "summary-requests-topic") ||
      getEnvString("KAFKA_TOPIC_SUMMARY_REQUESTS") ||
      "summary.requests",
    requestId: getStringFlag(flags, "request-id") || `manual-${Date.now()}`,
    requestedAtIso: parseIsoDate(requestedAtRaw),
    requestType: parseRequestType(requestTypeRaw),
    windows,
    mode: modeConfig.mode,
    queryLookbackDays: parsedLookbackDays,
    queryTopicGlobs,
    queryMaxEventsPerTopic,
    queryEvidenceStrategy,
    topicKey,
    score: score === undefined ? undefined : assertNonNegative(score, "--score"),
    volume: volume === undefined ? undefined : assertNonNegative(volume, "--volume"),
    acceleration,
    evidenceUrl,
    evidenceSource: modeConfig.mode === "explicit" ? parseCanonicalSource(evidenceSourceRaw) : undefined,
    evidenceTitle:
      modeConfig.mode === "explicit"
        ? getStringFlag(flags, "evidence-title") || "Manual summary request trigger"
        : undefined,
    evidenceExcerpt:
      modeConfig.mode === "explicit"
        ? getStringFlag(flags, "evidence-excerpt") ||
          "Manual summary request trigger generated via riops."
        : undefined,
    dailyBudgetUsd: assertNonNegative(dailyBudgetUsd, "--daily-budget-usd"),
    maxTopics: assertPositiveInteger(maxTopics, "--max-topics"),
    maxEvidencePerTopic: assertPositiveInteger(maxEvidencePerTopic, "--max-evidence-per-topic"),
    maxOutputTokens: assertPositiveInteger(maxOutputTokens, "--max-output-tokens"),
    dryRun: getBooleanFlag(flags, "dry-run"),
  };
}

function buildSummaryRequest(config: TriggerBriefConfig) {
  const nowIso = config.requestedAtIso;
  const basePayload = {
    request_id: config.requestId,
    requested_at: nowIso,
    type: config.requestType,
    windows: config.windows,
    budget: {
      daily_budget_usd: config.dailyBudgetUsd,
      max_topics: config.maxTopics,
      max_evidence_per_topic: config.maxEvidencePerTopic,
      max_output_tokens: config.maxOutputTokens,
    },
  };

  if (config.mode === "query") {
    return {
      ...basePayload,
      query: {
        lookback_days: config.queryLookbackDays,
        topic_globs: config.queryTopicGlobs,
        max_events_per_topic: config.queryMaxEventsPerTopic,
        evidence_strategy: config.queryEvidenceStrategy,
      },
      topics: [],
    };
  }

  const topicKey = config.topicKey as string;
  const evidenceUrl = config.evidenceUrl as string;
  const primaryWindow = config.windows.includes(2) ? 2 : config.windows[0];

  return {
    ...basePayload,
    topics: [
      {
        topic: topicKey,
        metrics: [
          {
            topic: topicKey,
            window: primaryWindow,
            score: config.score,
            volume: config.volume,
            acceleration: config.acceleration,
          },
        ],
        evidence: [
          {
            event_id: `${config.requestId}-event-1`,
            source: config.evidenceSource,
            url: evidenceUrl,
            title: config.evidenceTitle,
            published_at: nowIso,
            fetched_at: nowIso,
            text_excerpt: config.evidenceExcerpt,
          },
        ],
      },
    ],
  };
}

interface FreshnessIssue {
  category: string;
  message: string;
}

async function checkDataFreshness(): Promise<FreshnessIssue[]> {
  const issues: FreshnessIssue[] = [];
  const databaseUrl = getEnvString("DATABASE_URL");
  if (!databaseUrl) {
    issues.push({
      category: "config",
      message: "DATABASE_URL not set, unable to check data freshness",
    });
    return issues;
  }

  const prisma = new PrismaClient({ datasources: { db: { url: databaseUrl } } });

  try {
    await prisma.$connect();

    // Check consumer lag
    const MAX_LAG_MESSAGES = 100;
    const MAX_LAG_AGE_MS = 300_000; // 5 minutes

    const lagRecords = await prisma.consumerLag.findMany({
      where: {
        topic: "events.raw",
        consumerGroup: { in: ["trends-processor", "persister"] },
      },
    });

    if (lagRecords.length === 0) {
      issues.push({
        category: "consumer_lag",
        message: "No consumer lag records found for events.raw",
      });
    } else {
      const now = Date.now();
      const staleRecords = lagRecords.filter((r) => now - r.updatedAt.getTime() > MAX_LAG_AGE_MS);
      if (staleRecords.length > 0) {
        const groups = staleRecords.map((r) => r.consumerGroup).join(", ");
        issues.push({
          category: "consumer_lag",
          message: `Consumer lag records are stale (>5 min old) for: ${groups}`,
        });
      }

      for (const groupId of ["trends-processor", "persister"] as const) {
        const groupRecords = lagRecords.filter((r) => r.consumerGroup === groupId);
        if (groupRecords.length === 0) {
          issues.push({
            category: "consumer_lag",
            message: `Missing consumer lag records for ${groupId}`,
          });
          continue;
        }

        const totalLag = groupRecords.reduce((sum, r) => sum + Number(r.lagMessages), 0);
        if (totalLag > MAX_LAG_MESSAGES) {
          issues.push({
            category: "consumer_lag",
            message: `${groupId} lag is ${totalLag} messages (threshold: ${MAX_LAG_MESSAGES})`,
          });
        }
      }
    }

    // Note: Collector heartbeat checking would require reading from collector.heartbeat Kafka topic
    // which is more complex in a CLI tool. For MVP, we just check consumer lag.
    // A full implementation could use kafkajs admin client to read recent heartbeat messages.
  } catch (error) {
    issues.push({
      category: "database",
      message: `Failed to query database: ${(error as Error).message}`,
    });
  } finally {
    await prisma.$disconnect();
  }

  return issues;
}

export async function briefTrigger(flags: Flags): Promise<void> {
  const config = resolveConfig(flags);

  // Check data freshness
  const freshnessIssues = await checkDataFreshness();
  if (freshnessIssues.length > 0) {
    // eslint-disable-next-line no-console
    console.warn("⚠️  Data freshness warnings:");
    for (const issue of freshnessIssues) {
      // eslint-disable-next-line no-console
      console.warn(`  [${issue.category}] ${issue.message}`);
    }
    // eslint-disable-next-line no-console
    console.warn("Proceeding with brief request anyway...\n");
  }

  const payload = buildSummaryRequest(config);

  if (config.dryRun) {
    // eslint-disable-next-line no-console
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

  const kafka = new Kafka({
    clientId: config.kafkaClientId,
    brokers: config.kafkaBrokers,
  });
  const producer = kafka.producer({ allowAutoTopicCreation: false });

  try {
    await producer.connect();
    await producer.send({
      topic: config.summaryRequestsTopic,
      messages: [
        {
          key: config.requestId,
          value: Buffer.from(JSON.stringify(payload), "utf-8"),
        },
      ],
    });
  } finally {
    await producer.disconnect();
  }

  // eslint-disable-next-line no-console
  console.log(
    `Published ${config.mode} summary request ${config.requestId} to ${config.summaryRequestsTopic} via ${config.kafkaBrokers.join(",")}`
  );
}
