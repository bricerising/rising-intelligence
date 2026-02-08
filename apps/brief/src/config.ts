import { z } from "zod";
import {
  getSecretValue,
  loadDotEnv,
  parseConfig,
  resolveDatabaseUrl,
  resolvePostgresPassword,
} from "@rising-intelligence/shared";

const LOG_LEVELS = ["trace", "debug", "info", "warn", "error"] as const;

const ConfigSchema = z.object({
  SERVICE_NAME: z.string().default("brief"),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.enum(LOG_LEVELS).default("info"),

  KAFKA_BROKERS: z.string().default("localhost:9092"),
  KAFKA_CLIENT_ID: z.string().default("brief"),
  KAFKA_CONSUMER_GROUP: z.string().default("brief-generator"),
  KAFKA_TOPIC_SUMMARY_REQUESTS: z.string().default("summary.requests"),
  KAFKA_TOPIC_SUMMARY_RESULTS: z.string().default("summary.results"),

  DATABASE_URL: z.string().optional(),
  POSTGRES_HOST: z.string().default("localhost"),
  POSTGRES_PORT: z.coerce.number().int().positive().default(5432),
  POSTGRES_DB: z.string().default("rising_intelligence"),
  POSTGRES_USER: z.string().default("rising"),
  POSTGRES_PASSWORD: z.string().optional(),

  REDIS_URL: z.string().min(1).default("redis://localhost:6379"),
  LLM_DAILY_BUDGET_USD: z.coerce.number().nonnegative().default(5),

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
