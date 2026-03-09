import { describe, expect, it, vi } from "vitest";
import type { Config } from "../src/config.js";
import { createServiceBootstrap } from "../src/service-runtime.js";

function createConfig(overrides: Partial<Config> = {}): Config {
  return {
    SERVICE_NAME: "brief",
    PORT: 3000,
    LOG_LEVEL: "info",
    KAFKA_BROKERS: "localhost:9092",
    KAFKA_CLIENT_ID: "brief",
    KAFKA_CONSUMER_GROUP: "brief-processor",
    KAFKA_TOPIC_SUMMARY_REQUESTS: "summary.requests",
    KAFKA_TOPIC_SUMMARY_RESULTS: "summary.results",
    KAFKA_TOPIC_TREND_SNAPSHOTS: "trends.snapshots",
    DATABASE_URL: "postgresql://localhost/rising_intelligence",
    POSTGRES_HOST: "localhost",
    POSTGRES_PORT: 5432,
    POSTGRES_DB: "rising_intelligence",
    POSTGRES_USER: "rising",
    POSTGRES_PASSWORD: undefined,
    REDIS_URL: "redis://localhost:6379",
    LLM_PROVIDER: "codex-cli",
    LLM_ENDPOINT_URL: "http://localhost:8088/v1/generate",
    LLM_TIMEOUT_MS: 300000,
    LLM_CODEX_CLI_COMMAND: "codex",
    LLM_CODEX_MODEL: "",
    LLM_CODEX_PROFILE: "",
    LLM_CODEX_TIMEOUT_MS: 300000,
    LLM_DAILY_BUDGET_USD: 5,
    BRIEF_DEFAULT_LOOKBACK_DAYS: 7,
    BRIEF_MAX_LOOKBACK_DAYS: 30,
    BRIEF_MAX_QUERY_EVENTS_PER_TOPIC: 25,
    SHUTDOWN_TIMEOUT_MS: 30000,
    ...overrides,
  };
}

describe("service runtime", () => {
  it("keeps config and logger ownership local to the service bootstrap", () => {
    const config = createConfig();
    const loadConfig = vi.fn(() => config);
    const bootstrapLogger = {
      child: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      fatal: vi.fn(),
    } as any;
    const runtimeLogger = {
      child: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      fatal: vi.fn(),
    } as any;
    const createLogger = vi.fn(() => bootstrapLogger);

    const bootstrap = createServiceBootstrap(loadConfig, createLogger);

    expect(bootstrap.getConfig()).toBe(config);
    expect(bootstrap.getConfig()).toBe(config);
    expect(loadConfig).toHaveBeenCalledTimes(1);
    expect(bootstrap.getLogger()).toBe(bootstrapLogger);
    expect(createLogger).toHaveBeenCalledWith(config.SERVICE_NAME, config.LOG_LEVEL);

    bootstrap.setRuntimeLogger(runtimeLogger);

    expect(bootstrap.getLogger()).toBe(runtimeLogger);
  });
});
