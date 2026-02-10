import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CollectorAdapterFactoryConfig } from "../../src/adapters/factory.js";
import type { ContentFetcherConfig } from "../../src/content-fetcher.js";

const adapterMocks = vi.hoisted(() => {
  const rssAdapter = {
    name: "rss",
    source: "rss",
    pollIntervalMs: 1_000,
    initialize: vi.fn(),
    fetch: async function* () {},
    shutdown: vi.fn(),
  };

  const hackerNewsAdapter = {
    name: "hackernews",
    source: "hackernews",
    pollIntervalMs: 1_000,
    initialize: vi.fn(),
    fetch: async function* () {},
    shutdown: vi.fn(),
  };

  const lobstersAdapter = {
    name: "lobsters",
    source: "lobsters",
    pollIntervalMs: 1_000,
    initialize: vi.fn(),
    fetch: async function* () {},
    shutdown: vi.fn(),
  };

  return {
    rssAdapter,
    hackerNewsAdapter,
    lobstersAdapter,
    createRSSAdapter: vi.fn(() => rssAdapter),
    createHackerNewsAdapter: vi.fn(() => hackerNewsAdapter),
    createLobstersAdapter: vi.fn(() => lobstersAdapter),
  };
});

vi.mock("../../src/adapters/rss.js", () => ({
  createRSSAdapter: adapterMocks.createRSSAdapter,
}));

vi.mock("../../src/adapters/hackernews.js", () => ({
  createHackerNewsAdapter: adapterMocks.createHackerNewsAdapter,
}));

vi.mock("../../src/adapters/lobsters.js", () => ({
  createLobstersAdapter: adapterMocks.createLobstersAdapter,
}));

function createLogger() {
  const logger = {
    child: vi.fn(),
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  } as any;

  logger.child.mockImplementation(() => logger);
  return logger;
}

function createAdapterConfig(
  overrides: Partial<CollectorAdapterFactoryConfig> = {}
): CollectorAdapterFactoryConfig {
  return {
    RSS_ENABLED: true,
    RSS_POLL_INTERVAL_SECONDS: 120,
    FEEDS_CONFIG_PATH: "./config/feeds.yaml",
    HN_ENABLED: true,
    HN_MODE: "best",
    HN_POLL_INTERVAL_SECONDS: 90,
    HN_MAX_ITEMS_PER_POLL: 25,
    LOBSTERS_ENABLED: false,
    LOBSTERS_POLL_INTERVAL_SECONDS: 600,
    LOBSTERS_MAX_ITEMS_PER_POLL: 10,
    REDDIT_ENABLED: false,
    BLUESKY_ENABLED: false,
    MASTODON_ENABLED: false,
    GITHUB_ENABLED: false,
    ...overrides,
  };
}

const contentFetcherConfig: ContentFetcherConfig = {
  enabled: false,
  timeoutMs: 1_000,
  maxContentLength: 20_000,
  minContentLength: 100,
  domainDelayMs: 0,
  userAgent: "test-agent",
  blockedDomains: new Set<string>(),
};

describe("collector adapter factory", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("creates enabled implemented adapters with expected constructor args", async () => {
    const { buildCollectorAdapters } = await import(
      "../../src/adapters/factory.js"
    );
    const logger = createLogger();
    const checkpointStore = {} as any;
    const config = createAdapterConfig();

    const result = buildCollectorAdapters({
      config,
      checkpointStore,
      logger,
      contentFetcherConfig,
    });

    expect(result.adapters).toEqual([
      adapterMocks.rssAdapter,
      adapterMocks.hackerNewsAdapter,
    ]);
    expect(result.unsupportedEnabledAdapters).toEqual([]);

    expect(adapterMocks.createRSSAdapter).toHaveBeenCalledWith(
      "./config/feeds.yaml",
      120_000,
      checkpointStore,
      logger,
      contentFetcherConfig
    );
    expect(adapterMocks.createHackerNewsAdapter).toHaveBeenCalledWith(
      "best",
      90_000,
      25,
      checkpointStore,
      logger,
      contentFetcherConfig
    );
    expect(adapterMocks.createLobstersAdapter).not.toHaveBeenCalled();
    expect(logger.child).toHaveBeenCalledWith({ adapter: "rss" });
    expect(logger.child).toHaveBeenCalledWith({ adapter: "hackernews" });
  });

  it("reports enabled unsupported adapters from the same registry", async () => {
    const { buildCollectorAdapters } = await import(
      "../../src/adapters/factory.js"
    );
    const logger = createLogger();
    const checkpointStore = {} as any;
    const config = createAdapterConfig({
      RSS_ENABLED: false,
      HN_ENABLED: false,
      REDDIT_ENABLED: true,
      MASTODON_ENABLED: true,
      GITHUB_ENABLED: true,
    });

    const result = buildCollectorAdapters({
      config,
      checkpointStore,
      logger,
      contentFetcherConfig,
    });

    expect(result.adapters).toEqual([]);
    expect(result.unsupportedEnabledAdapters).toEqual([
      "reddit",
      "mastodon",
      "github",
    ]);
  });
});
