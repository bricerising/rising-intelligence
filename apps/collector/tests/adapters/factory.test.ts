import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  createCollectorAdapterFactory,
  type CollectorAdapterFactoryConfig,
} from "../../src/adapters/factory.js";
import type { ContentFetcherConfig } from "../../src/content-fetcher.js";
import type { MarketFilterProfile } from "../../src/market-filters.js";
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
    EDGAR_FORMS_ALLOWLIST: "8-K,6-K,10-Q,10-K,20-F,40-F",
    EDGAR_FETCH_DETAIL_METADATA: true,
    EDGAR_DOWNLOAD_PRIMARY_DOCS: false,
    EDGAR_POLL_INTERVAL_SECONDS: 1800,
    EDGAR_POLL_JITTER_RATIO: 0.4,
    SEC_USER_AGENT: "Collector Test test@example.com",
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

const marketFilterProfiles: MarketFilterProfile[] = [
  {
    key: "pos",
    name: "POS",
    matchers: [],
  },
];

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
      marketFilterProfiles,
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
        marketFilterProfiles,
        edgarFormsAllowlist: ["8-K", "6-K", "10-Q", "10-K", "20-F", "40-F"],
        edgarFetchDetailMetadata: true,
        edgarDownloadPrimaryDocs: false,
        edgarPollIntervalSeconds: 1800,
        edgarPollJitterRatio: 0.4,
        secUserAgent: "Collector Test test@example.com",
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
      marketFilterProfiles,
    });

    expect(result.adapters).toEqual([]);
    expect(result.unsupportedEnabledAdapters).toEqual([
      "reddit",
      "mastodon",
      "github",
    ]);
  });

  it("passes RSS feed error callback to the RSS adapter constructor when provided", () => {
    const logger = createLogger();
    const checkpointStore = {} as any;
    const config = createAdapterConfig({ HN_ENABLED: false });
    const onRssFeedError = vi.fn();
    const factory = createCollectorAdapterFactory(constructorMocks);

    factory.build({
      config,
      checkpointStore,
      logger,
      contentFetcherConfig,
      marketFilterProfiles,
      onRssFeedError,
    });

    expect(constructorMocks.createRSSAdapter).toHaveBeenCalledWith(
      expect.objectContaining({
        onFeedError: onRssFeedError,
      })
    );
  });

  it("deduplicates EDGAR allowlist forms before building RSS adapter input", () => {
    const logger = createLogger();
    const checkpointStore = {} as any;
    const config = createAdapterConfig({
      HN_ENABLED: false,
      EDGAR_FORMS_ALLOWLIST: "8-K, 8-K,10-Q, ,10-Q",
    });
    const factory = createCollectorAdapterFactory(constructorMocks);

    factory.build({
      config,
      checkpointStore,
      logger,
      contentFetcherConfig,
      marketFilterProfiles,
    });

    expect(constructorMocks.createRSSAdapter).toHaveBeenCalledWith(
      expect.objectContaining({
        edgarFormsAllowlist: ["8-K", "10-Q"],
      })
    );
  });

  it("fails fast when a constructor override is not a function", () => {
    expect(() =>
      createCollectorAdapterFactory({
        createRSSAdapter: 123 as unknown as typeof constructorMocks.createRSSAdapter,
      })
    ).toThrow('Collector adapter constructor override "createRSSAdapter" must be a function');
  });
});
