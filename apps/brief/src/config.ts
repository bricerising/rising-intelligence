import { z } from "zod";
import {
  type ServiceConfig,
  parseConfig,
} from "@rising-intelligence/shared/config";
import { loadDotEnv } from "@rising-intelligence/shared/env";
import { getSecretValue } from "@rising-intelligence/shared/secrets";
import {
  resolveDatabaseUrl,
  resolvePostgresPassword,
} from "@rising-intelligence/shared/database";

const LOG_LEVELS = ["trace", "debug", "info", "warn", "error"] as const;
const LLM_PROVIDERS = ["internal", "http", "codex-cli"] as const;

export const BRIEF_CONFIG_DEFAULTS = {
  serviceName: "brief",
  port: 3000,
  logLevel: "info",
  kafkaBrokers: "localhost:9092",
  kafkaClientId: "brief",
  kafkaConsumerGroup: "brief-processor",
  kafkaTopicSummaryRequests: "summary.requests",
  kafkaTopicSummaryResults: "summary.results",
  kafkaTopicTrendSnapshots: "trends.snapshots",
  postgresHost: "localhost",
  postgresPort: 5432,
  postgresDb: "rising_intelligence",
  postgresUser: "rising",
  redisUrl: "redis://localhost:6379",
  llmProvider: "codex-cli",
  llmEndpointUrl: "http://localhost:8088/v1/generate",
  llmTimeoutMs: 300000,
  llmCodexCliCommand: "codex",
  llmCodexModel: "",
  llmCodexProfile: "",
  llmCodexTimeoutMs: 300000,
  llmDailyBudgetUsd: 5,
  briefDefaultLookbackDays: 7,
  briefMaxLookbackDays: 30,
  briefMaxQueryEventsPerTopic: 25,
  shutdownTimeoutMs: 30000,
} as const satisfies {
  serviceName: string;
  port: number;
  logLevel: (typeof LOG_LEVELS)[number];
  kafkaBrokers: string;
  kafkaClientId: string;
  kafkaConsumerGroup: string;
  kafkaTopicSummaryRequests: string;
  kafkaTopicSummaryResults: string;
  kafkaTopicTrendSnapshots: string;
  postgresHost: string;
  postgresPort: number;
  postgresDb: string;
  postgresUser: string;
  redisUrl: string;
  llmProvider: (typeof LLM_PROVIDERS)[number];
  llmEndpointUrl: string;
  llmTimeoutMs: number;
  llmCodexCliCommand: string;
  llmCodexModel: string;
  llmCodexProfile: string;
  llmCodexTimeoutMs: number;
  llmDailyBudgetUsd: number;
  briefDefaultLookbackDays: number;
  briefMaxLookbackDays: number;
  briefMaxQueryEventsPerTopic: number;
  shutdownTimeoutMs: number;
};

const ConfigSchema = z.object({
  SERVICE_NAME: z.string().default(BRIEF_CONFIG_DEFAULTS.serviceName),
  PORT: z.coerce.number().int().positive().default(BRIEF_CONFIG_DEFAULTS.port),
  LOG_LEVEL: z.enum(LOG_LEVELS).default(BRIEF_CONFIG_DEFAULTS.logLevel),

  KAFKA_BROKERS: z.string().default(BRIEF_CONFIG_DEFAULTS.kafkaBrokers),
  KAFKA_CLIENT_ID: z.string().default(BRIEF_CONFIG_DEFAULTS.kafkaClientId),
  KAFKA_CONSUMER_GROUP: z.string().default(BRIEF_CONFIG_DEFAULTS.kafkaConsumerGroup),
  KAFKA_TOPIC_SUMMARY_REQUESTS: z
    .string()
    .default(BRIEF_CONFIG_DEFAULTS.kafkaTopicSummaryRequests),
  KAFKA_TOPIC_SUMMARY_RESULTS: z
    .string()
    .default(BRIEF_CONFIG_DEFAULTS.kafkaTopicSummaryResults),
  KAFKA_TOPIC_TREND_SNAPSHOTS: z
    .string()
    .default(BRIEF_CONFIG_DEFAULTS.kafkaTopicTrendSnapshots),

  DATABASE_URL: z.string().optional(),
  POSTGRES_HOST: z.string().default(BRIEF_CONFIG_DEFAULTS.postgresHost),
  POSTGRES_PORT: z.coerce.number().int().positive().default(BRIEF_CONFIG_DEFAULTS.postgresPort),
  POSTGRES_DB: z.string().default(BRIEF_CONFIG_DEFAULTS.postgresDb),
  POSTGRES_USER: z.string().default(BRIEF_CONFIG_DEFAULTS.postgresUser),
  POSTGRES_PASSWORD: z.string().optional(),

  REDIS_URL: z.string().min(1).default(BRIEF_CONFIG_DEFAULTS.redisUrl),
  LLM_PROVIDER: z.enum(LLM_PROVIDERS).default(BRIEF_CONFIG_DEFAULTS.llmProvider),
  LLM_ENDPOINT_URL: z.string().url().default(BRIEF_CONFIG_DEFAULTS.llmEndpointUrl),
  LLM_TIMEOUT_MS: z
    .coerce
    .number()
    .int()
    .positive()
    .default(BRIEF_CONFIG_DEFAULTS.llmTimeoutMs),
  LLM_CODEX_CLI_COMMAND: z
    .string()
    .min(1)
    .default(BRIEF_CONFIG_DEFAULTS.llmCodexCliCommand),
  LLM_CODEX_MODEL: z.string().default(BRIEF_CONFIG_DEFAULTS.llmCodexModel),
  LLM_CODEX_PROFILE: z.string().default(BRIEF_CONFIG_DEFAULTS.llmCodexProfile),
  LLM_CODEX_TIMEOUT_MS: z
    .coerce
    .number()
    .int()
    .positive()
    .default(BRIEF_CONFIG_DEFAULTS.llmCodexTimeoutMs),
  LLM_DAILY_BUDGET_USD: z.coerce.number().nonnegative().default(BRIEF_CONFIG_DEFAULTS.llmDailyBudgetUsd),
  BRIEF_DEFAULT_LOOKBACK_DAYS: z
    .coerce
    .number()
    .int()
    .positive()
    .default(BRIEF_CONFIG_DEFAULTS.briefDefaultLookbackDays),
  BRIEF_MAX_LOOKBACK_DAYS: z
    .coerce
    .number()
    .int()
    .positive()
    .default(BRIEF_CONFIG_DEFAULTS.briefMaxLookbackDays),
  BRIEF_MAX_QUERY_EVENTS_PER_TOPIC: z
    .coerce
    .number()
    .int()
    .positive()
    .default(BRIEF_CONFIG_DEFAULTS.briefMaxQueryEventsPerTopic),

  SHUTDOWN_TIMEOUT_MS: z
    .coerce
    .number()
    .int()
    .positive()
    .default(BRIEF_CONFIG_DEFAULTS.shutdownTimeoutMs),
});

type RawConfig = z.infer<typeof ConfigSchema>;

export type Config = RawConfig & {
  DATABASE_URL: string;
};

// Compile-time check: Config satisfies the shared ServiceConfig contract.
type _AssertServiceConfig = Config extends ServiceConfig ? true : never;
const _assert: _AssertServiceConfig = true; void _assert;

export function loadConfig(): Config {
  loadDotEnv();

  const env: Record<string, string | undefined> = { ...process.env };
  if (!env.POSTGRES_PASSWORD) {
    const secret = getSecretValue("POSTGRES_PASSWORD");
    if (secret) {
      env.POSTGRES_PASSWORD = secret;
    }
  }

  const parsed = parseConfig(ConfigSchema, env);
  if (parsed.BRIEF_MAX_LOOKBACK_DAYS < parsed.BRIEF_DEFAULT_LOOKBACK_DAYS) {
    throw new Error(
      "Configuration validation failed: BRIEF_MAX_LOOKBACK_DAYS must be >= BRIEF_DEFAULT_LOOKBACK_DAYS"
    );
  }

  const password = resolvePostgresPassword(env);
  const databaseUrl = resolveDatabaseUrl(parsed.DATABASE_URL, {
    host: parsed.POSTGRES_HOST,
    port: parsed.POSTGRES_PORT,
    db: parsed.POSTGRES_DB,
    user: parsed.POSTGRES_USER,
    password,
  });

  return {
    ...parsed,
    DATABASE_URL: databaseUrl,
  };
}

let cachedConfig: Config | null = null;

export function getConfig(): Config {
  if (!cachedConfig) {
    cachedConfig = loadConfig();
  }

  return cachedConfig;
}
