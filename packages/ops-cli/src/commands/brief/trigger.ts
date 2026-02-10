import { Kafka } from "kafkajs";
import { getEnvString, parseCanonicalSource } from "@rising-intelligence/shared";

type Flags = Record<string, string | boolean>;

interface TriggerBriefConfig {
  kafkaBrokers: string[];
  kafkaClientId: string;
  summaryRequestsTopic: string;
  requestId: string;
  requestedAtIso: string;
  requestType: "daily" | "threshold";
  windows: number[];
  topicKey: string;
  score: number;
  volume: number;
  acceleration: number;
  evidenceUrl: string;
  evidenceSource: string;
  evidenceTitle: string;
  evidenceExcerpt: string;
  dailyBudgetUsd: number;
  maxTopics: number;
  maxEvidencePerTopic: number;
  maxOutputTokens: number;
  dryRun: boolean;
}

function getStringFlag(flags: Flags, name: string): string | undefined {
  const value = flags[name];
  if (typeof value === "string") {
    return value;
  }
  return undefined;
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
  if (value === undefined) {
    return undefined;
  }
  return parseNumber(value, `--${name}`);
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

function requiredStringFlag(flags: Flags, name: string): string {
  const value = getStringFlag(flags, name)?.trim();
  if (!value) {
    throw new Error(`Missing required flag: --${name}`);
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

function resolveConfig(flags: Flags): TriggerBriefConfig {
  const kafkaBrokersRaw =
    getStringFlag(flags, "kafka-brokers") || getEnvString("KAFKA_BROKERS") || "localhost:9092";

  const requestedAtRaw = getStringFlag(flags, "requested-at") || new Date().toISOString();
  const requestTypeRaw = getStringFlag(flags, "type") || "daily";
  const windowsRaw = getStringFlag(flags, "windows") || "1,2";
  const topicKey = requiredStringFlag(flags, "topic-key");
  const evidenceUrl = requiredStringFlag(flags, "evidence-url");
  const evidenceSourceRaw = getStringFlag(flags, "evidence-source") || "rss";
  const score = getNumberFlag(flags, "score") ?? 8.5;
  const volume = getNumberFlag(flags, "volume") ?? 100;
  const acceleration = getNumberFlag(flags, "acceleration") ?? 0.4;
  const dailyBudgetUsd =
    getNumberFlag(flags, "daily-budget-usd") ??
    parseNumberEnv(
      "BRIEF_DAILY_BUDGET_USD",
      getEnvString("BRIEF_DAILY_BUDGET_USD") || getEnvString("LLM_DAILY_BUDGET_USD"),
      5,
    );
  const maxTopics =
    getNumberFlag(flags, "max-topics") ??
    parseNumberEnv("BRIEF_MAX_TOPICS", getEnvString("BRIEF_MAX_TOPICS"), 5);
  const maxEvidencePerTopic =
    getNumberFlag(flags, "max-evidence-per-topic") ??
    parseNumberEnv(
      "BRIEF_MAX_EVIDENCE_PER_TOPIC",
      getEnvString("BRIEF_MAX_EVIDENCE_PER_TOPIC"),
      3,
    );
  const maxOutputTokens =
    getNumberFlag(flags, "max-output-tokens") ??
    parseNumberEnv("BRIEF_MAX_OUTPUT_TOKENS", getEnvString("BRIEF_MAX_OUTPUT_TOKENS"), 1200);

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
    windows: parseWindows(windowsRaw),
    topicKey,
    score: assertNonNegative(score, "--score"),
    volume: assertNonNegative(volume, "--volume"),
    acceleration,
    evidenceUrl,
    evidenceSource: parseCanonicalSource(evidenceSourceRaw),
    evidenceTitle: getStringFlag(flags, "evidence-title") || "Manual summary request trigger",
    evidenceExcerpt:
      getStringFlag(flags, "evidence-excerpt") ||
      "Manual summary request trigger generated via riops.",
    dailyBudgetUsd: assertNonNegative(dailyBudgetUsd, "--daily-budget-usd"),
    maxTopics: assertPositiveInteger(maxTopics, "--max-topics"),
    maxEvidencePerTopic: assertPositiveInteger(maxEvidencePerTopic, "--max-evidence-per-topic"),
    maxOutputTokens: assertPositiveInteger(maxOutputTokens, "--max-output-tokens"),
    dryRun: getBooleanFlag(flags, "dry-run"),
  };
}

function buildSummaryRequest(config: TriggerBriefConfig) {
  const nowIso = config.requestedAtIso;
  const primaryWindow = config.windows.includes(2) ? 2 : config.windows[0];

  return {
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
            event_id: `${config.requestId}-event-1`,
            source: config.evidenceSource,
            url: config.evidenceUrl,
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

export async function briefTrigger(flags: Flags): Promise<void> {
  const config = resolveConfig(flags);
  const payload = buildSummaryRequest(config);

  if (config.dryRun) {
    // eslint-disable-next-line no-console
    console.log(
      JSON.stringify(
        {
          kafka_brokers: config.kafkaBrokers,
          topic: config.summaryRequestsTopic,
          key: config.requestId,
          payload,
        },
        null,
        2,
      ),
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
    `Published summary request ${config.requestId} to ${config.summaryRequestsTopic} via ${config.kafkaBrokers.join(",")}`,
  );
}
