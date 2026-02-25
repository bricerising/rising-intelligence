import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const sharedMocks = vi.hoisted(() => ({
  loadDotEnv: vi.fn(),
}));

vi.mock("@rising-intelligence/shared/config", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@rising-intelligence/shared/config")>();
  return {
    ...actual,
    loadDotEnv: sharedMocks.loadDotEnv,
  };
});

describe("brief config", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it("loads defaults", async () => {
    const { loadConfig } = await import("../src/config.js");
    const config = loadConfig();

    expect(sharedMocks.loadDotEnv).toHaveBeenCalledOnce();
    expect(config.SERVICE_NAME).toBe("brief");
    expect(config.KAFKA_TOPIC_SUMMARY_REQUESTS).toBe("summary.requests");
    expect(config.KAFKA_CONSUMER_GROUP).toBe("brief-processor");
    expect(config.REDIS_URL).toBe("redis://localhost:6379");
    expect(config.LLM_PROVIDER).toBe("codex-cli");
    expect(config.LLM_ENDPOINT_URL).toBe("http://localhost:8088/v1/generate");
    expect(config.LLM_TIMEOUT_MS).toBe(300000);
    expect(config.LLM_CODEX_CLI_COMMAND).toBe("codex");
    expect(config.LLM_CODEX_MODEL).toBe("");
    expect(config.LLM_CODEX_PROFILE).toBe("");
    expect(config.LLM_CODEX_TIMEOUT_MS).toBe(300000);
    expect(config.LLM_DAILY_BUDGET_USD).toBe(5);
    expect(config.BRIEF_DEFAULT_LOOKBACK_DAYS).toBe(7);
    expect(config.BRIEF_MAX_LOOKBACK_DAYS).toBe(30);
    expect(config.BRIEF_MAX_QUERY_EVENTS_PER_TOPIC).toBe(25);
  });

  it("coerces numeric config values", async () => {
    process.env.PORT = "3100";
    process.env.LLM_PROVIDER = "http";
    process.env.LLM_ENDPOINT_URL = "http://mock-llm:8080/v1/generate";
    process.env.LLM_TIMEOUT_MS = "25000";
    process.env.LLM_CODEX_CLI_COMMAND = "codex";
    process.env.LLM_CODEX_MODEL = "gpt-5-codex";
    process.env.LLM_CODEX_PROFILE = "default";
    process.env.LLM_CODEX_TIMEOUT_MS = "65000";
    process.env.LLM_DAILY_BUDGET_USD = "7.5";
    process.env.BRIEF_DEFAULT_LOOKBACK_DAYS = "5";
    process.env.BRIEF_MAX_LOOKBACK_DAYS = "20";
    process.env.BRIEF_MAX_QUERY_EVENTS_PER_TOPIC = "40";
    process.env.SHUTDOWN_TIMEOUT_MS = "45000";

    const { loadConfig } = await import("../src/config.js");
    const config = loadConfig();

    expect(config.PORT).toBe(3100);
    expect(config.LLM_PROVIDER).toBe("http");
    expect(config.LLM_ENDPOINT_URL).toBe("http://mock-llm:8080/v1/generate");
    expect(config.LLM_TIMEOUT_MS).toBe(25000);
    expect(config.LLM_CODEX_CLI_COMMAND).toBe("codex");
    expect(config.LLM_CODEX_MODEL).toBe("gpt-5-codex");
    expect(config.LLM_CODEX_PROFILE).toBe("default");
    expect(config.LLM_CODEX_TIMEOUT_MS).toBe(65000);
    expect(config.LLM_DAILY_BUDGET_USD).toBe(7.5);
    expect(config.BRIEF_DEFAULT_LOOKBACK_DAYS).toBe(5);
    expect(config.BRIEF_MAX_LOOKBACK_DAYS).toBe(20);
    expect(config.BRIEF_MAX_QUERY_EVENTS_PER_TOPIC).toBe(40);
    expect(config.SHUTDOWN_TIMEOUT_MS).toBe(45000);
  });

  it("throws when lookback max is below default", async () => {
    process.env.BRIEF_DEFAULT_LOOKBACK_DAYS = "10";
    process.env.BRIEF_MAX_LOOKBACK_DAYS = "7";
    const { loadConfig } = await import("../src/config.js");
    expect(() => loadConfig()).toThrow(
      "BRIEF_MAX_LOOKBACK_DAYS must be >= BRIEF_DEFAULT_LOOKBACK_DAYS"
    );
  });

  it("throws on invalid config", async () => {
    process.env.PORT = "0";
    const { loadConfig } = await import("../src/config.js");
    expect(() => loadConfig()).toThrow("Configuration validation failed");
  });

  it("getConfig returns cached object", async () => {
    const { getConfig } = await import("../src/config.js");
    const first = getConfig();
    const second = getConfig();
    expect(first).toBe(second);
  });
});
