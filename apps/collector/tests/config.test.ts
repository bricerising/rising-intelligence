import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock the shared package - pass through real implementations for zod helpers
const sharedMocks = vi.hoisted(() => ({
  getSecretValue: vi.fn(),
  loadDotEnv: vi.fn(),
}));

vi.mock("@rising-intelligence/shared", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@rising-intelligence/shared")>();
  return {
    ...actual,
    getSecretValue: sharedMocks.getSecretValue,
    loadDotEnv: sharedMocks.loadDotEnv,
  };
});

const { getSecretValue, loadDotEnv } = sharedMocks;

describe("config", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    process.env = { ...originalEnv };
    // Clear any cached config
    vi.clearAllMocks();
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe("loadConfig", () => {
    it("loads config with default values", async () => {
      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(loadDotEnv).toHaveBeenCalledTimes(1);
      expect(config.SERVICE_NAME).toBe("collector");
      expect(config.PORT).toBe(3000);
      expect(config.LOG_LEVEL).toBe("info");
      expect(config.KAFKA_BROKERS).toBe("localhost:9092");
      expect(config.KAFKA_CLIENT_ID).toBe("collector");
      expect(config.CHECKPOINT_PATH).toBe("./data/checkpoints.db");
      expect(config.TOPICS_ALLOWLIST_PATH).toBe("./config/topics.allowlist.yaml");
      expect(config.FEEDS_CONFIG_PATH).toBe("./config/feeds.yaml");
    });

    it("loads Hacker News defaults", async () => {
      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.HN_ENABLED).toBe(true);
      expect(config.HN_MODE).toBe("top");
      expect(config.HN_POLL_INTERVAL_SECONDS).toBe(300);
      expect(config.HN_MAX_ITEMS_PER_POLL).toBe(30);
    });

    it("loads Lobsters defaults", async () => {
      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.LOBSTERS_ENABLED).toBe(true);
      expect(config.LOBSTERS_POLL_INTERVAL_SECONDS).toBe(600);
      expect(config.LOBSTERS_MAX_ITEMS_PER_POLL).toBe(25);
    });

    it("loads Reddit defaults (disabled by default)", async () => {
      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.REDDIT_ENABLED).toBe(false);
      expect(config.REDDIT_SUBREDDITS).toBe("aws,MachineLearning,programming");
      expect(config.REDDIT_POLL_INTERVAL_SECONDS).toBe(300);
    });

    it("loads RSS defaults", async () => {
      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.RSS_ENABLED).toBe(true);
      expect(config.RSS_POLL_INTERVAL_SECONDS).toBe(300);
    });

    it("loads disabled source defaults", async () => {
      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.BLUESKY_ENABLED).toBe(false);
      expect(config.MASTODON_ENABLED).toBe(false);
      expect(config.GITHUB_ENABLED).toBe(false);
    });

    it("overrides defaults from environment variables", async () => {
      process.env.SERVICE_NAME = "custom-collector";
      process.env.PORT = "8080";
      process.env.LOG_LEVEL = "debug";
      process.env.KAFKA_BROKERS = "kafka1:9092,kafka2:9092";
      process.env.HN_ENABLED = "false";
      process.env.HN_MODE = "best";
      process.env.LOBSTERS_MAX_ITEMS_PER_POLL = "50";

      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.SERVICE_NAME).toBe("custom-collector");
      expect(config.PORT).toBe(8080);
      expect(config.LOG_LEVEL).toBe("debug");
      expect(config.KAFKA_BROKERS).toBe("kafka1:9092,kafka2:9092");
      expect(config.HN_ENABLED).toBe(false);
      expect(config.HN_MODE).toBe("best");
      expect(config.LOBSTERS_MAX_ITEMS_PER_POLL).toBe(50);
    });

    it("coerces numeric values correctly", async () => {
      process.env.PORT = "3001";
      process.env.HN_POLL_INTERVAL_SECONDS = "600";
      process.env.SHUTDOWN_TIMEOUT_MS = "60000";

      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.PORT).toBe(3001);
      expect(typeof config.PORT).toBe("number");
      expect(config.HN_POLL_INTERVAL_SECONDS).toBe(600);
      expect(typeof config.HN_POLL_INTERVAL_SECONDS).toBe("number");
      expect(config.SHUTDOWN_TIMEOUT_MS).toBe(60000);
    });

    it("transforms boolean strings correctly", async () => {
      process.env.HN_ENABLED = "true";
      process.env.LOBSTERS_ENABLED = "false";
      process.env.REDDIT_ENABLED = "true";
      process.env.BLUESKY_ENABLED = "false";

      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.HN_ENABLED).toBe(true);
      expect(config.LOBSTERS_ENABLED).toBe(false);
      expect(config.REDDIT_ENABLED).toBe(true);
      expect(config.BLUESKY_ENABLED).toBe(false);
    });

    it("validates HN_MODE enum", async () => {
      process.env.HN_MODE = "top";

      const { loadConfig } = await import("../src/config.js");
      const config = loadConfig();

      expect(config.HN_MODE).toBe("top");
    });

    it("validates LOG_LEVEL enum", async () => {
      for (const level of ["trace", "debug", "info", "warn", "error"]) {
        vi.resetModules();
        process.env.LOG_LEVEL = level;

        const { loadConfig } = await import("../src/config.js");
        const config = loadConfig();

        expect(config.LOG_LEVEL).toBe(level);
      }
    });
  });

  describe("secret resolution", () => {
    it("resolves REDDIT_CLIENT_SECRET from secret file", async () => {
      (getSecretValue as any).mockReturnValue("secret-from-file");

      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(getSecretValue).toHaveBeenCalledWith("REDDIT_CLIENT_SECRET");
      expect(config.REDDIT_CLIENT_SECRET).toBe("secret-from-file");
    });

    it("resolves GITHUB_TOKEN from secret file", async () => {
      (getSecretValue as any).mockReturnValue("github-token-from-file");

      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(getSecretValue).toHaveBeenCalledWith("GITHUB_TOKEN");
      expect(config.GITHUB_TOKEN).toBe("github-token-from-file");
    });

    it("prefers environment variable over secret file", async () => {
      process.env.REDDIT_CLIENT_SECRET = "from-env";
      (getSecretValue as any).mockReturnValue("from-file");

      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.REDDIT_CLIENT_SECRET).toBe("from-env");
    });

    it("handles missing secret gracefully", async () => {
      (getSecretValue as any).mockReturnValue(undefined);

      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.REDDIT_CLIENT_SECRET).toBeUndefined();
      expect(config.GITHUB_TOKEN).toBeUndefined();
    });
  });

  describe("getConfig singleton", () => {
    it("caches config after first load", async () => {
      const { getConfig } = await import("../src/config.js");

      const config1 = getConfig();
      const config2 = getConfig();

      expect(config1).toBe(config2); // Same reference
    });
  });

  describe("optional fields", () => {
    it("allows optional Reddit credentials", async () => {
      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.REDDIT_CLIENT_ID).toBeUndefined();
      expect(config.REDDIT_CLIENT_SECRET).toBeUndefined();
    });

    it("allows optional GitHub token", async () => {
      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.GITHUB_TOKEN).toBeUndefined();
    });
  });

  describe("full config paths", () => {
    it("accepts custom paths for config files", async () => {
      process.env.CHECKPOINT_PATH = "/data/custom/checkpoints.db";
      process.env.TOPICS_ALLOWLIST_PATH = "/config/custom/topics.yaml";
      process.env.FEEDS_CONFIG_PATH = "/config/custom/feeds.yaml";

      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.CHECKPOINT_PATH).toBe("/data/custom/checkpoints.db");
      expect(config.TOPICS_ALLOWLIST_PATH).toBe("/config/custom/topics.yaml");
      expect(config.FEEDS_CONFIG_PATH).toBe("/config/custom/feeds.yaml");
    });
  });

  describe("comma-separated lists", () => {
    it("preserves comma-separated values as strings", async () => {
      process.env.KAFKA_BROKERS = "kafka1:9092,kafka2:9092,kafka3:9092";
      process.env.REDDIT_SUBREDDITS = "rust,golang,typescript";
      process.env.BLUESKY_QUERIES = "ai,ml,llm";
      process.env.MASTODON_INSTANCES = "hachyderm.io,fosstodon.org";
      process.env.MASTODON_TAGS = "aws,cloud";

      const { loadConfig } = await import("../src/config.js");

      const config = loadConfig();

      expect(config.KAFKA_BROKERS).toBe("kafka1:9092,kafka2:9092,kafka3:9092");
      expect(config.REDDIT_SUBREDDITS).toBe("rust,golang,typescript");
      expect(config.BLUESKY_QUERIES).toBe("ai,ml,llm");
      expect(config.MASTODON_INSTANCES).toBe("hachyderm.io,fosstodon.org");
      expect(config.MASTODON_TAGS).toBe("aws,cloud");
    });
  });
});
