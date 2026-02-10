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
  summaryResultsTopic: string;
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
  reportTimezone?: string;
  reportStartAtIso?: string;
  reportEndAtIso?: string;
  llmProvider?: string;
  dryRun: boolean;
  noWait: boolean;
  timeoutSeconds: number;
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
  const reportTimezone = parseOptionalTimezone(getStringFlag(flags, "report-timezone"));
  const reportStartAtIso = parseOptionalIsoDate(
    getStringFlag(flags, "report-start-at"),
    "--report-start-at"
  );
  const reportEndAtIso = parseOptionalIsoDate(
    getStringFlag(flags, "report-end-at"),
    "--report-end-at"
  );

  if (reportStartAtIso && reportEndAtIso) {
    const startAt = new Date(reportStartAtIso).getTime();
    const endAt = new Date(reportEndAtIso).getTime();
    if (startAt > endAt) {
      throw new Error("--report-start-at must be <= --report-end-at");
    }
  }

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
    reportTimezone,
    reportStartAtIso,
    reportEndAtIso,
    llmProvider: getStringFlag(flags, "llm-provider"),
    dryRun: getBooleanFlag(flags, "dry-run"),
    noWait: getBooleanFlag(flags, "no-wait"),
    timeoutSeconds: getNumberFlag(flags, "timeout") ?? 300,
    summaryResultsTopic:
      getStringFlag(flags, "summary-results-topic") ||
      getEnvString("KAFKA_TOPIC_SUMMARY_RESULTS") ||
      "summary.results",
  };
}

function buildSummaryRequest(config: TriggerBriefConfig) {
  const nowIso = config.requestedAtIso;
  const report =
    config.reportTimezone !== undefined ||
    config.reportStartAtIso !== undefined ||
    config.reportEndAtIso !== undefined
      ? {
          ...(config.reportTimezone && { timezone: config.reportTimezone }),
          ...(config.reportStartAtIso && { start_at: config.reportStartAtIso }),
          ...(config.reportEndAtIso && { end_at: config.reportEndAtIso }),
        }
      : undefined;
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
    ...(report && { report }),
    ...(config.llmProvider && { llm_provider: config.llmProvider }),
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

interface BriefResultWaiter {
  consumer: ReturnType<Kafka["consumer"]>;
  resultPromise: Promise<BriefResult>;
}

async function setupBriefResultConsumer(
  kafka: Kafka,
  requestId: string,
  resultsTopic: string,
  timeoutSeconds: number
): Promise<BriefResultWaiter> {
  const consumer = kafka.consumer({
    groupId: `riops-brief-trigger-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
  });

  await consumer.connect();
  // Subscribe from beginning and use eachBatch with manual offset control
  await consumer.subscribe({ topic: resultsTopic, fromBeginning: true });

  const timeoutMs = timeoutSeconds * 1000;
  const startTime = Date.now();

  const resultPromise = new Promise<BriefResult>((resolve, reject) => {
    const timeoutHandle = setTimeout(() => {
      reject(new Error(`Timeout waiting for brief result after ${timeoutSeconds}s`));
    }, timeoutMs);

    void consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) {
          return;
        }

        try {
          const result = JSON.parse(message.value.toString("utf-8")) as BriefResult;

          // Only process messages for our specific request
          if (result.request_id === requestId) {
            clearTimeout(timeoutHandle);
            resolve(result);
            // Don't await stop() - just trigger it and let the promise resolution handle cleanup
            void consumer.stop();
          }
        } catch (error) {
          // Ignore parse errors for messages not matching our request
        }

        // Check if we've exceeded timeout (safety check)
        if (Date.now() - startTime > timeoutMs) {
          clearTimeout(timeoutHandle);
          reject(new Error(`Timeout waiting for brief result after ${timeoutSeconds}s`));
          void consumer.stop();
        }
      },
    });
  });

  // Give the consumer a moment to start polling before returning
  await new Promise((resolve) => setTimeout(resolve, 200));

  return { consumer, resultPromise };
}

async function waitForBriefResult(
  waiter: BriefResultWaiter
): Promise<BriefResult> {
  try {
    return await waiter.resultPromise;
  } finally {
    await waiter.consumer.disconnect();
  }
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

  // Set up result consumer BEFORE publishing the request to avoid race condition
  let waiter: BriefResultWaiter | undefined;
  if (!config.noWait) {
    // eslint-disable-next-line no-console
    console.log(`⏳ Setting up result listener (timeout: ${config.timeoutSeconds}s)...`);
    waiter = await setupBriefResultConsumer(
      kafka,
      config.requestId,
      config.summaryResultsTopic,
      config.timeoutSeconds
    );
  }

  // Now publish the request
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
    `✅ Published ${config.mode} summary request ${config.requestId} to ${config.summaryRequestsTopic}`
  );

  if (config.noWait) {
    // eslint-disable-next-line no-console
    console.log(`Request ID: ${config.requestId}`);
    return;
  }

  // Wait for the result
  try {
    const result = await waitForBriefResult(waiter!);

    const formatted = formatBriefResult(result);
    // eslint-disable-next-line no-console
    console.log(formatted);

    if (result.failure) {
      process.exit(1);
    }
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error(`\n❌ ${(error as Error).message}`);
    // eslint-disable-next-line no-console
    console.error(`Request ID: ${config.requestId}`);
    process.exit(1);
  }
}
