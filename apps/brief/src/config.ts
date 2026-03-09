import { z } from "zod";
import {
  getSecretValue,
  loadDotEnv,
  parseConfig,
} from "@rising-intelligence/shared/config";
import {
  resolveDatabaseUrl,
  resolvePostgresPassword,
} from "@rising-intelligence/shared/database";

const LOG_LEVELS = ["trace", "debug", "info", "warn", "error"] as const;
const LLM_PROVIDERS = ["internal", "http", "codex-cli"] as const;

const ConfigSchema = z.object({
  SERVICE_NAME: z.string().default("brief"),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.enum(LOG_LEVELS).default("info"),

  KAFKA_BROKERS: z.string().default("localhost:9092"),
  KAFKA_CLIENT_ID: z.string().default("brief"),
  KAFKA_CONSUMER_GROUP: z.string().default("brief-processor"),
  KAFKA_TOPIC_SUMMARY_REQUESTS: z.string().default("summary.requests"),
  KAFKA_TOPIC_SUMMARY_RESULTS: z.string().default("summary.results"),
  KAFKA_TOPIC_TREND_SNAPSHOTS: z.string().default("trends.snapshots"),

  DATABASE_URL: z.string().optional(),
  POSTGRES_HOST: z.string().default("localhost"),
  POSTGRES_PORT: z.coerce.number().int().positive().default(5432),
  POSTGRES_DB: z.string().default("rising_intelligence"),
  POSTGRES_USER: z.string().default("rising"),
  POSTGRES_PASSWORD: z.string().optional(),

  REDIS_URL: z.string().min(1).default("redis://localhost:6379"),
  LLM_PROVIDER: z.enum(LLM_PROVIDERS).default("codex-cli"),
  LLM_ENDPOINT_URL: z.string().url().default("http://localhost:8088/v1/generate"),
  LLM_TIMEOUT_MS: z.coerce.number().int().positive().default(300000),
  LLM_CODEX_CLI_COMMAND: z.string().min(1).default("codex"),
  LLM_CODEX_MODEL: z.string().default(""),
  LLM_CODEX_PROFILE: z.string().default(""),
  LLM_CODEX_TIMEOUT_MS: z.coerce.number().int().positive().default(300000),
  LLM_DAILY_BUDGET_USD: z.coerce.number().nonnegative().default(5),
  BRIEF_DEFAULT_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  BRIEF_MAX_LOOKBACK_DAYS: z.coerce.number().int().positive().default(30),
  BRIEF_MAX_QUERY_EVENTS_PER_TOPIC: z.coerce.number().int().positive().default(25),

  SHUTDOWN_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
});

type RawConfig = z.infer<typeof ConfigSchema>;

export type Config = RawConfig & {
  DATABASE_URL: string;
};

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
