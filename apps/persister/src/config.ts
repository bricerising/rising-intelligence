import { z } from "zod";
import { getSecretValue, loadDotEnv } from "@rising-intelligence/shared";

const ConfigSchema = z.object({
  SERVICE_NAME: z.string().default("persister"),
  PORT: z.coerce.number().default(3000),
  LOG_LEVEL: z.enum(["trace", "debug", "info", "warn", "error"]).default("info"),

  KAFKA_BROKERS: z.string().default("localhost:9092"),
  KAFKA_CLIENT_ID: z.string().default("persister"),
  KAFKA_CONSUMER_GROUP: z.string().default("persister"),
  KAFKA_TOPIC_RAW_EVENTS: z.string().default("events.raw"),

  DATABASE_URL: z.string().optional(),
  POSTGRES_HOST: z.string().default("localhost"),
  POSTGRES_PORT: z.coerce.number().default(5432),
  POSTGRES_DB: z.string().default("rising_intelligence"),
  POSTGRES_USER: z.string().default("rising"),
  POSTGRES_PASSWORD: z.string().optional(),

  REDIS_URL: z.string().optional().default("redis://localhost:6379"),
  SEEN_TTL_SECONDS: z.coerce.number().int().positive().default(86400),

  CONSUMER_LAG_UPDATE_INTERVAL_MS: z.coerce.number().int().positive().default(15000),

  POSTGRES_CIRCUIT_FAILURE_THRESHOLD: z.coerce.number().int().positive().default(5),
  POSTGRES_CIRCUIT_OPEN_MS: z.coerce.number().int().positive().default(30000),

  SHUTDOWN_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
});

export type Config = z.infer<typeof ConfigSchema> & {
  DATABASE_URL: string;
};

function resolvePostgresPassword(env: Record<string, string | undefined>): string {
  if (env.POSTGRES_PASSWORD && env.POSTGRES_PASSWORD.trim().length > 0) {
    return env.POSTGRES_PASSWORD;
  }

  const secret = getSecretValue("POSTGRES_PASSWORD");
  if (secret && secret.trim().length > 0) {
    return secret;
  }

  return "rising";
}

function buildDatabaseUrl(parsed: z.infer<typeof ConfigSchema>, password: string): string {
  const username = encodeURIComponent(parsed.POSTGRES_USER);
  const encodedPassword = encodeURIComponent(password);
  const database = encodeURIComponent(parsed.POSTGRES_DB);

  return `postgresql://${username}:${encodedPassword}@${parsed.POSTGRES_HOST}:${parsed.POSTGRES_PORT}/${database}`;
}

export function loadConfig(): Config {
  loadDotEnv();

  const env: Record<string, string | undefined> = { ...process.env };
  if (!env.POSTGRES_PASSWORD) {
    const secret = getSecretValue("POSTGRES_PASSWORD");
    if (secret) {
      env.POSTGRES_PASSWORD = secret;
    }
  }

  const result = ConfigSchema.safeParse(env);
  if (!result.success) {
    console.error("Configuration validation failed:");
    for (const issue of result.error.issues) {
      console.error(`  ${issue.path.join(".")}: ${issue.message}`);
    }
    process.exit(1);
  }

  const parsed = result.data;
  const password = resolvePostgresPassword(env);

  const databaseUrl =
    parsed.DATABASE_URL && parsed.DATABASE_URL.trim().length > 0
      ? parsed.DATABASE_URL
      : buildDatabaseUrl(parsed, password);

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
