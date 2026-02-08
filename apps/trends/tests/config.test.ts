import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const sharedMocks = vi.hoisted(() => ({
  loadDotEnv: vi.fn(),
  getSecretValue: vi.fn(),
}));

vi.mock("@rising-intelligence/shared", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@rising-intelligence/shared")>();
  return {
    ...actual,
    loadDotEnv: sharedMocks.loadDotEnv,
    getSecretValue: sharedMocks.getSecretValue,
  };
});

describe("trends config", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    sharedMocks.getSecretValue.mockReturnValue(undefined);
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it("loads defaults and parses windows", async () => {
    const { loadConfig } = await import("../src/config.js");
    const config = loadConfig();

    expect(sharedMocks.loadDotEnv).toHaveBeenCalledOnce();
    expect(config.SERVICE_NAME).toBe("trends");
    expect(config.KAFKA_CONSUMER_GROUP).toBe("trends-processor");
    expect(config.KAFKA_TOPIC_RAW_EVENTS).toBe("events.raw");
    expect(config.KAFKA_TOPIC_TRENDS_SNAPSHOTS).toBe("trends.snapshots");
    expect(config.WINDOWS).toEqual(["15m", "60m"]);
    expect(config.DATABASE_URL).toBe("postgresql://rising:rising@localhost:5432/rising_intelligence");
  });

  it("uses provided DATABASE_URL when set", async () => {
    process.env.DATABASE_URL = "postgresql://custom:custom@db:5432/custom";

    const { loadConfig } = await import("../src/config.js");
    const config = loadConfig();

    expect(config.DATABASE_URL).toBe("postgresql://custom:custom@db:5432/custom");
  });

  it("uses secret password when postgres password env is missing", async () => {
    process.env.POSTGRES_PASSWORD = "secret-pass";
    process.env.POSTGRES_USER = "svc";
    process.env.POSTGRES_DB = "ri";
    process.env.POSTGRES_HOST = "postgres";
    process.env.POSTGRES_PORT = "5433";

    const { loadConfig } = await import("../src/config.js");
    const config = loadConfig();

    expect(config.DATABASE_URL).toBe("postgresql://svc:secret-pass@postgres:5433/ri");
  });

  it("deduplicates configured windows", async () => {
    process.env.TREND_WINDOWS = "60m,15m,60m";

    const { loadConfig } = await import("../src/config.js");
    const config = loadConfig();

    expect(config.WINDOWS).toEqual(["60m", "15m"]);
  });

  it("throws on unsupported windows", async () => {
    process.env.TREND_WINDOWS = "5m";
    const { loadConfig } = await import("../src/config.js");
    expect(() => loadConfig()).toThrow("Unsupported trend window");
  });

  it("getConfig returns the cached instance", async () => {
    const { getConfig } = await import("../src/config.js");
    const first = getConfig();
    const second = getConfig();
    expect(first).toBe(second);
  });
});
