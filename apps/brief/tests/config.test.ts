import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const sharedMocks = vi.hoisted(() => ({
  loadDotEnv: vi.fn(),
}));

vi.mock("@rising-intelligence/shared", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@rising-intelligence/shared")>();
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
    expect(config.KAFKA_CONSUMER_GROUP).toBe("brief-generator");
    expect(config.REDIS_URL).toBe("redis://localhost:6379");
    expect(config.LLM_DAILY_BUDGET_USD).toBe(5);
  });

  it("coerces numeric config values", async () => {
    process.env.PORT = "3100";
    process.env.LLM_DAILY_BUDGET_USD = "7.5";
    process.env.SHUTDOWN_TIMEOUT_MS = "45000";

    const { loadConfig } = await import("../src/config.js");
    const config = loadConfig();

    expect(config.PORT).toBe(3100);
    expect(config.LLM_DAILY_BUDGET_USD).toBe(7.5);
    expect(config.SHUTDOWN_TIMEOUT_MS).toBe(45000);
  });

  it("exits on invalid config", async () => {
    process.env.PORT = "0";
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const exitSpy = vi.spyOn(process, "exit").mockImplementation(((code?: number) => {
      throw new Error(`process.exit:${code}`);
    }) as any);

    try {
      const { loadConfig } = await import("../src/config.js");
      expect(() => loadConfig()).toThrow("process.exit:1");
      expect(exitSpy).toHaveBeenCalledWith(1);
    } finally {
      errorSpy.mockRestore();
      exitSpy.mockRestore();
    }
  });

  it("getConfig returns cached object", async () => {
    const { getConfig } = await import("../src/config.js");
    const first = getConfig();
    const second = getConfig();
    expect(first).toBe(second);
  });
});
