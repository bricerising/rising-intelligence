import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  createCollectorAdapterFactory,
  type CollectorAdapterFactoryConfig,
} from "../../src/adapters/factory.js";
import type { ContentFetcherConfig } from "../../src/content-fetcher.js";
import type { SourceAdapter } from "../../src/types.js";

function createSourceAdapter(name: string, source: SourceAdapter["source"]): SourceAdapter {
  return {
    name,
    source,
    pollIntervalMs: 1_000,
    async initialize() {
      return undefined;
    },
    async *fetch() {
      return;
    },
    async shutdown() {
      return undefined;
    },
  };
}

const adapterMocks = {
  rssAdapter: createSourceAdapter("rss", "rss"),
  hackerNewsAdapter: createSourceAdapter("hackernews", "hackernews"),
  lobstersAdapter: createSourceAdapter("lobsters", "lobsters"),
};

const constructorMocks = {
  createRSSAdapter: vi.fn(() => adapterMocks.rssAdapter),
  createHackerNewsAdapter: vi.fn(() => adapterMocks.hackerNewsAdapter),
  createLobstersAdapter: vi.fn(() => adapterMocks.lobstersAdapter),
};

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

  it("creates enabled implemented adapters with expected constructor args", () => {
    const logger = createLogger();
    const checkpointStore = {} as any;
    const config = createAdapterConfig();
    const factory = createCollectorAdapterFactory(constructorMocks);

    const result = factory.build({
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

    expect(constructorMocks.createRSSAdapter).toHaveBeenCalledWith(
      {
        feedsConfigPath: "./config/feeds.yaml",
        pollIntervalMs: 120_000,
        checkpoints: checkpointStore,
        logger,
        contentFetcherConfig,
      }
    );
    expect(constructorMocks.createHackerNewsAdapter).toHaveBeenCalledWith(
      {
        mode: "best",
        pollIntervalMs: 90_000,
        maxItems: 25,
        checkpoints: checkpointStore,
        logger,
        contentFetcherConfig,
      }
    );
    expect(constructorMocks.createLobstersAdapter).not.toHaveBeenCalled();
    expect(logger.child).toHaveBeenCalledWith({ adapter: "rss" });
    expect(logger.child).toHaveBeenCalledWith({ adapter: "hackernews" });
  });

  it("reports enabled unsupported adapters from the same registry", () => {
    const logger = createLogger();
    const checkpointStore = {} as any;
    const config = createAdapterConfig({
      RSS_ENABLED: false,
      HN_ENABLED: false,
      REDDIT_ENABLED: true,
      MASTODON_ENABLED: true,
      GITHUB_ENABLED: true,
    });
    const factory = createCollectorAdapterFactory(constructorMocks);

    const result = factory.build({
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
