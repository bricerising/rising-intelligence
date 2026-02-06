import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const sharedMocks = vi.hoisted(() => ({
  loadDotEnv: vi.fn(),
  getSecretValue: vi.fn(),
}));

vi.mock("@rising-intelligence/shared", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@rising-intelligence/shared")>();
  return {
    ...actual,
    getSecretValue: sharedMocks.getSecretValue,
    loadDotEnv: sharedMocks.loadDotEnv,
  };
});

describe("persister config", () => {
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

  it("loads defaults and builds DATABASE_URL from postgres fields", async () => {
    const { loadConfig } = await import("../src/config.js");

    const config = loadConfig();

    expect(sharedMocks.loadDotEnv).toHaveBeenCalledOnce();
    expect(config.SERVICE_NAME).toBe("persister");
    expect(config.KAFKA_CONSUMER_GROUP).toBe("persister");
    expect(config.KAFKA_TOPIC_RAW_EVENTS).toBe("events.raw");
    expect(config.DATABASE_URL).toBe("postgresql://rising:rising@localhost:5432/rising_intelligence");
    expect(config.REDIS_URL).toBe("redis://localhost:6379");
    expect(config.SEEN_TTL_SECONDS).toBe(86400);
  });

  it("prefers explicit DATABASE_URL when provided", async () => {
    process.env.DATABASE_URL = "postgresql://custom:custom@db:5432/custom";
    process.env.POSTGRES_PASSWORD = "ignored";

    const { loadConfig } = await import("../src/config.js");

    const config = loadConfig();

    expect(config.DATABASE_URL).toBe("postgresql://custom:custom@db:5432/custom");
  });

  it("uses secret-based postgres password when env password is missing", async () => {
    sharedMocks.getSecretValue.mockImplementation((key: string) => {
      if (key === "POSTGRES_PASSWORD") {
        return "secret-pass";
      }
      return undefined;
    });

    process.env.POSTGRES_USER = "svc";
    process.env.POSTGRES_DB = "ri";
    process.env.POSTGRES_HOST = "postgres";
    process.env.POSTGRES_PORT = "5433";

    const { loadConfig } = await import("../src/config.js");

    const config = loadConfig();

    expect(config.DATABASE_URL).toBe("postgresql://svc:secret-pass@postgres:5433/ri");
  });

  it("coerces numeric values", async () => {
    process.env.PORT = "3100";
    process.env.SEEN_TTL_SECONDS = "3600";
    process.env.CONSUMER_LAG_UPDATE_INTERVAL_MS = "5000";

    const { loadConfig } = await import("../src/config.js");

    const config = loadConfig();

    expect(config.PORT).toBe(3100);
    expect(config.SEEN_TTL_SECONDS).toBe(3600);
    expect(config.CONSUMER_LAG_UPDATE_INTERVAL_MS).toBe(5000);
  });

  it("URL-encodes postgres username/password/database when building DATABASE_URL", async () => {
    process.env.POSTGRES_USER = "svc@team";
    process.env.POSTGRES_PASSWORD = "p@ss word";
    process.env.POSTGRES_DB = "ri/main";

    const { loadConfig } = await import("../src/config.js");
    const config = loadConfig();

    expect(config.DATABASE_URL).toBe(
      "postgresql://svc%40team:p%40ss%20word@localhost:5432/ri%2Fmain"
    );
  });

  it("exits when configuration validation fails", async () => {
    process.env.SEEN_TTL_SECONDS = "0";
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

  it("getConfig caches the same object instance", async () => {
    const { getConfig } = await import("../src/config.js");

    const first = getConfig();
    const second = getConfig();

    expect(first).toBe(second);
  });
});
