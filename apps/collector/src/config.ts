import { z } from "zod";
import { getSecretValue, loadDotEnv, parseConfig, zBooleanEnv } from "@rising-intelligence/shared";

const ConfigSchema = z.object({
  // Service
  SERVICE_NAME: z.string().default("collector"),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.enum(["trace", "debug", "info", "warn", "error"]).default("info"),

  // Kafka
  KAFKA_BROKERS: z.string().default("localhost:9092"),
  KAFKA_CLIENT_ID: z.string().default("collector"),

  // Checkpoint storage
  CHECKPOINT_PATH: z.string().default("./data/checkpoints.db"),

  // Topics allowlist
  TOPICS_ALLOWLIST_PATH: z.string().default("./config/topics.allowlist.yaml"),

  // Feeds config
  FEEDS_CONFIG_PATH: z.string().default("./config/feeds.yaml"),

  // Hacker News
  HN_ENABLED: zBooleanEnv("true"),
  HN_MODE: z.enum(["top", "new", "best"]).default("top"),
  HN_POLL_INTERVAL_SECONDS: z.coerce.number().int().positive().default(300),
  HN_MAX_ITEMS_PER_POLL: z.coerce.number().int().positive().default(30),

  // Lobsters
  LOBSTERS_ENABLED: zBooleanEnv("true"),
  LOBSTERS_POLL_INTERVAL_SECONDS: z.coerce.number().int().positive().default(600),
  LOBSTERS_MAX_ITEMS_PER_POLL: z.coerce.number().int().positive().default(25),

  // Reddit
  REDDIT_ENABLED: zBooleanEnv("false"),
  REDDIT_CLIENT_ID: z.string().optional(),
  REDDIT_CLIENT_SECRET: z.string().optional(),
  REDDIT_SUBREDDITS: z.string().default("aws,MachineLearning,programming"),
  REDDIT_POLL_INTERVAL_SECONDS: z.coerce.number().int().positive().default(300),
  REDDIT_MAX_ITEMS_PER_POLL: z.coerce.number().int().positive().default(25),

  // RSS
  RSS_ENABLED: zBooleanEnv("true"),
  RSS_POLL_INTERVAL_SECONDS: z.coerce.number().int().positive().default(300),

  // Bluesky
  BLUESKY_ENABLED: zBooleanEnv("false"),
  BLUESKY_POLL_INTERVAL_SECONDS: z.coerce.number().int().positive().default(300),
  BLUESKY_QUERIES: z.string().default("aws,bedrock,ai,llm,typescript,rust"),

  // Mastodon
  MASTODON_ENABLED: zBooleanEnv("false"),
  MASTODON_POLL_INTERVAL_SECONDS: z.coerce.number().int().positive().default(600),
  MASTODON_INSTANCES: z.string().default("hachyderm.io,fosstodon.org"),
  MASTODON_TAGS: z.string().default("aws,ai,machinelearning,typescript,rust"),

  // GitHub
  GITHUB_ENABLED: zBooleanEnv("false"),
  GITHUB_TOKEN: z.string().optional(),
  GITHUB_POLL_INTERVAL_SECONDS: z.coerce.number().int().positive().default(3600),

  // Graceful shutdown
  SHUTDOWN_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
});

export type Config = z.infer<typeof ConfigSchema>;

export function loadConfig(): Config {
  loadDotEnv();

  const env: Record<string, string | undefined> = { ...process.env };

  // Resolve secrets from _FILE variants
  for (const key of ["REDDIT_CLIENT_SECRET", "GITHUB_TOKEN"]) {
    if (!env[key]) {
      const secret = getSecretValue(key);
      if (secret) {
        env[key] = secret;
      }
    }
  }

  return parseConfig(ConfigSchema, env);
}

let _config: Config | null = null;

export function getConfig(): Config {
  if (!_config) {
    _config = loadConfig();
  }
  return _config;
}
