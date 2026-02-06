import { z } from "zod";
import { loadDotEnv } from "@rising-intelligence/shared";

const LOG_LEVELS = ["trace", "debug", "info", "warn", "error"] as const;

const ConfigSchema = z.object({
  SERVICE_NAME: z.string().default("brief"),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.enum(LOG_LEVELS).default("info"),

  KAFKA_BROKERS: z.string().default("localhost:9092"),
  KAFKA_CLIENT_ID: z.string().default("brief"),
  KAFKA_CONSUMER_GROUP: z.string().default("brief-generator"),
  KAFKA_TOPIC_SUMMARY_REQUESTS: z.string().default("summary.requests"),

  SHUTDOWN_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
});

export type Config = z.infer<typeof ConfigSchema>;

export function loadConfig(): Config {
  loadDotEnv();

  const result = ConfigSchema.safeParse(process.env);
  if (!result.success) {
    console.error("Configuration validation failed:");
    for (const issue of result.error.issues) {
      console.error(`  ${issue.path.join(".")}: ${issue.message}`);
    }
    process.exit(1);
  }

  return result.data;
}

let cachedConfig: Config | null = null;

export function getConfig(): Config {
  if (!cachedConfig) {
    cachedConfig = loadConfig();
  }

  return cachedConfig;
}
