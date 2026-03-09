import { z } from "zod";
import {
  loadDotEnv,
  parseConfig,
  zBooleanEnv,
} from "@rising-intelligence/shared/config";
import {
  resolvePostgresPassword,
  resolveDatabaseUrl,
} from "@rising-intelligence/shared/database";
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
  PERSISTER_CONSUMER_GROUP: z.string().default("persister"),
  KAFKA_TOPIC_RAW_EVENTS: z.string().default("events.raw"),
  KAFKA_TOPIC_COLLECTOR_HEARTBEAT: z.string().default("collector.heartbeat"),
  KAFKA_TOPIC_TRENDS_SNAPSHOTS: z.string().default("trends.snapshots"),
  KAFKA_TOPIC_SUMMARY_REQUESTS: z.string().default("summary.requests"),

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
  DAILY_BRIEF_ENABLED: zBooleanEnv("true"),
  DAILY_BRIEF_UTC_HOUR: z.coerce.number().int().min(0).max(23).default(1),
  DAILY_BRIEF_UTC_MINUTE: z.coerce.number().int().min(0).max(59).default(0),
  MAX_LAG_MESSAGES: z.coerce.number().int().nonnegative().default(100),
  MAX_LAG_AGE_MS: z.coerce.number().int().positive().default(300000),
  MAX_SOURCE_HEARTBEAT_AGE_MS: z.coerce.number().int().positive().default(300000),
  MIN_HEALTHY_SOURCES: z.coerce.number().int().positive().default(2),
  BRIEF_DAILY_BUDGET_USD: z.coerce.number().nonnegative().default(5),
  BRIEF_MAX_TOPICS: z.coerce.number().int().positive().default(10),
  BRIEF_MAX_EVIDENCE_PER_TOPIC: z.coerce.number().int().positive().default(5),
  BRIEF_MAX_OUTPUT_TOKENS: z.coerce.number().int().positive().default(2000),
  SHUTDOWN_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
});

type RawConfig = z.infer<typeof ConfigSchema>;

export type Config = Omit<RawConfig, "DATABASE_URL" | "TREND_WINDOWS"> & {
  DATABASE_URL: string;
  WINDOWS: TrendWindow[];
};

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
  const parsed = parseConfig(ConfigSchema, env);
  const password = resolvePostgresPassword(env);
  const databaseUrl = resolveDatabaseUrl(parsed.DATABASE_URL, {
    host: parsed.POSTGRES_HOST,
    port: parsed.POSTGRES_PORT,
    db: parsed.POSTGRES_DB,
    user: parsed.POSTGRES_USER,
    password,
  });

  const windows = parseWindows(parsed.TREND_WINDOWS);

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
