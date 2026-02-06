import { z } from "zod";
import { getSecretValue, loadDotEnv } from "@rising-intelligence/shared";
import type { TrendWindow } from "./types.js";

const LOG_LEVELS = ["trace", "debug", "info", "warn", "error"] as const;
const SUPPORTED_WINDOWS: TrendWindow[] = ["15m", "60m"];

const ConfigSchema = z.object({
  SERVICE_NAME: z.string().default("trends"),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.enum(LOG_LEVELS).default("info"),

  KAFKA_BROKERS: z.string().default("localhost:9092"),
  KAFKA_CLIENT_ID: z.string().default("trends"),
  KAFKA_CONSUMER_GROUP: z.string().default("trends-processor"),
  KAFKA_TOPIC_RAW_EVENTS: z.string().default("events.raw"),
  KAFKA_TOPIC_TRENDS_SNAPSHOTS: z.string().default("trends.snapshots"),

  DATABASE_URL: z.string().optional(),
  POSTGRES_HOST: z.string().default("localhost"),
  POSTGRES_PORT: z.coerce.number().int().positive().default(5432),
  POSTGRES_DB: z.string().default("rising_intelligence"),
  POSTGRES_USER: z.string().default("rising"),
  POSTGRES_PASSWORD: z.string().optional(),

  REDIS_URL: z.string().default("redis://localhost:6379"),
  TOPICS_ALLOWLIST_PATH: z.string().default("./config/topics.allowlist.yaml"),

  TREND_WINDOWS: z.string().default("15m,60m"),
  TOP_N_TOPICS: z.coerce.number().int().positive().default(10),
  MAX_EVIDENCE_PER_TOPIC: z.coerce.number().int().positive().default(10),
  SNAPSHOT_INTERVAL_SECONDS: z.coerce.number().int().positive().default(300),
  CONSUMER_LAG_UPDATE_INTERVAL_MS: z.coerce.number().int().positive().default(15000),
  SHUTDOWN_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
});

type RawConfig = z.infer<typeof ConfigSchema>;

export type Config = Omit<RawConfig, "DATABASE_URL" | "TREND_WINDOWS"> & {
  DATABASE_URL: string;
  WINDOWS: TrendWindow[];
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

function buildDatabaseUrl(parsed: RawConfig, password: string): string {
  const username = encodeURIComponent(parsed.POSTGRES_USER);
  const encodedPassword = encodeURIComponent(password);
  const database = encodeURIComponent(parsed.POSTGRES_DB);

  return `postgresql://${username}:${encodedPassword}@${parsed.POSTGRES_HOST}:${parsed.POSTGRES_PORT}/${database}`;
}

function parseWindows(raw: string): TrendWindow[] {
  const parsed = raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0) as TrendWindow[];

  if (parsed.length === 0) {
    throw new Error("TREND_WINDOWS must include at least one supported window");
  }

  for (const window of parsed) {
    if (!SUPPORTED_WINDOWS.includes(window)) {
      throw new Error(
        `Unsupported trend window '${window}'. Supported windows: ${SUPPORTED_WINDOWS.join(", ")}`
      );
    }
  }

  return [...new Set(parsed)];
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

  let windows: TrendWindow[];
  try {
    windows = parseWindows(parsed.TREND_WINDOWS);
  } catch (error) {
    console.error((error as Error).message);
    process.exit(1);
  }

  return {
    ...parsed,
    DATABASE_URL: databaseUrl,
    WINDOWS: windows,
  };
}

let cachedConfig: Config | null = null;

export function getConfig(): Config {
  if (!cachedConfig) {
    cachedConfig = loadConfig();
  }

  return cachedConfig;
}
