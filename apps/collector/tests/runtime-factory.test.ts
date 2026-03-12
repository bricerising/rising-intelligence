import type { Server } from "node:http";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Config } from "../src/config.js";
import type { ContentFetcherConfig } from "../src/content-fetcher.js";
import { createHealthContext } from "../src/health.js";
import type { MarketFilterProfile } from "../src/market-filters.js";
import type { CollectorIngestionAdapterFactory } from "../src/adapters/factory.js";
import {
  createCollectorRuntimeFactory,
  type CollectorRuntimeFactoryDependencies,
} from "../src/runtime-factory.js";
import type { SourceAdapter } from "../src/types.js";

function createConfig(overrides: Partial<Config> = {}): Config {
  return {
    SERVICE_NAME: "collector",
    PORT: 3000,
    LOG_LEVEL: "info",
    KAFKA_BROKERS: "localhost:9092",
    KAFKA_CLIENT_ID: "collector",
    CHECKPOINT_PATH: "./data/checkpoints.db",
    TOPICS_ALLOWLIST_PATH: "./config/topics.allowlist.yaml",
    FEEDS_CONFIG_PATH: "./config/feeds.tech.yaml",
    MARKET_FILTERS_DIR: "./config/market-filters",
    HN_ENABLED: true,
    HN_MODE: "best",
    HN_POLL_INTERVAL_SECONDS: 300,
    HN_MAX_ITEMS_PER_POLL: 25,
    LOBSTERS_ENABLED: true,
    LOBSTERS_POLL_INTERVAL_SECONDS: 600,
    LOBSTERS_MAX_ITEMS_PER_POLL: 25,
    REDDIT_ENABLED: false,
    REDDIT_SUBREDDITS: "aws,MachineLearning,programming",
    REDDIT_POLL_INTERVAL_SECONDS: 300,
    REDDIT_MAX_ITEMS_PER_POLL: 25,
    RSS_ENABLED: true,
    RSS_POLL_INTERVAL_SECONDS: 300,
    EDGAR_ENABLED: true,
    EDGAR_FORMS_ALLOWLIST: "8-K,6-K,10-Q",
    EDGAR_FETCH_DETAIL_METADATA: true,
    EDGAR_DOWNLOAD_PRIMARY_DOCS: false,
    EDGAR_POLL_INTERVAL_SECONDS: 1800,
    EDGAR_POLL_JITTER_RATIO: 0.4,
    SEC_USER_AGENT: "Collector Test test@example.com",
    BLUESKY_ENABLED: false,
    BLUESKY_POLL_INTERVAL_SECONDS: 300,
    BLUESKY_QUERIES: "aws,bedrock,ai",
    MASTODON_ENABLED: false,
    MASTODON_POLL_INTERVAL_SECONDS: 600,
    MASTODON_INSTANCES: "hachyderm.io,fosstodon.org",
    MASTODON_TAGS: "aws,ai,machinelearning",
    GITHUB_ENABLED: false,
    GITHUB_POLL_INTERVAL_SECONDS: 3600,
    SHUTDOWN_TIMEOUT_MS: 30000,
    ...overrides,
  };
}

function createLogger() {
  const createMockLogger = () =>
    ({
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      child: vi.fn(),
    }) as any;

  const logger = createMockLogger();
  const childLoggers = new Map<string, any>();

  logger.child.mockImplementation((bindings: { component?: unknown }) => {
    const component = String(bindings.component);
    const existing = childLoggers.get(component);
    if (existing) {
      return existing;
    }

    const childLogger = createMockLogger();
    childLogger.child.mockReturnValue(childLogger);
    childLoggers.set(component, childLogger);
    return childLogger;
  });

  return Object.assign(logger, {
    childFor(component: string) {
      const childLogger = childLoggers.get(component);
      if (!childLogger) {
        throw new Error(`Expected logger child for component "${component}"`);
      }
      return childLogger;
    },
  });
}

function createSourceAdapter(
  name: string,
  source: SourceAdapter["source"],
  overrides: Partial<SourceAdapter> = {}
): SourceAdapter {
  return {
    name,
    source,
    pollIntervalMs: 1_000,
    initialize: vi.fn(async () => undefined),
    async *fetch() {
      return;
    },
    shutdown: vi.fn(async () => undefined),
    ...overrides,
  };
}

function createDependencies(
  overrides: Partial<CollectorRuntimeFactoryDependencies> = {}
) {
  const healthContext = createHealthContext();
  const healthServer = {} as Server;
  const checkpointStore = {
    initialize: vi.fn(async () => undefined),
    close: vi.fn(() => undefined),
  } as any;
  const allowlist = {
    topics: [{ key: "aws.bedrock", displayName: "AWS Bedrock", priority: 90 }],
    topicMap: new Map(),
    mutedTopics: new Set<string>(),
    maxTopicsPerEvent: 5,
  } as any;
  const kafkaProducerConnection = {
    publish: vi.fn(async () => undefined),
    publishBatch: vi.fn(async () => false),
    disconnect: vi.fn(async () => undefined),
  };
  const marketFilterProfiles: MarketFilterProfile[] = [
    { key: "pos", name: "POS", matchers: [] },
  ];
  const environment: NodeJS.ProcessEnv = { ...process.env };
  const contentFetcherConfig: ContentFetcherConfig = {
    enabled: false,
    timeoutMs: 1_000,
    maxContentLength: 20_000,
    minContentLength: 100,
    domainDelayMs: 0,
    userAgent: "test-agent",
    blockedDomains: new Set<string>(),
  };
  const adapters: SourceAdapter[] = [createSourceAdapter("rss", "rss")];
  const adapterFactory = {
    buildIngestionAdapters: vi.fn(() => ({
      adapters,
      unsupportedEnabledAdapters: [],
    })),
  } as CollectorIngestionAdapterFactory;

  const dependencies: CollectorRuntimeFactoryDependencies = {
    createHealthContext: vi.fn(() => healthContext),
    startHealthServer: vi.fn(() => healthServer),
    closeHealthServer: vi.fn(async () => undefined),
    createCheckpointStore: vi.fn(() => checkpointStore),
    initializeCheckpointStore: vi.fn(async () => undefined),
    closeCheckpointStore: vi.fn(() => undefined),
    loadAllowlist: vi.fn(() => allowlist),
    createKafkaProducer: vi.fn(async () => kafkaProducerConnection),
    disconnectProducer: vi.fn(async () => undefined),
    loadMarketFilterProfiles: vi.fn(() => marketFilterProfiles),
    getEnvironment: vi.fn(() => environment),
    createContentFetcherConfig: vi.fn(() => contentFetcherConfig),
    createCollectorAdapterFactory: vi.fn(() => adapterFactory),
    ...overrides,
  };

  return {
    dependencies,
    healthContext,
    healthServer,
    checkpointStore,
    allowlist,
    kafkaProducerConnection,
    marketFilterProfiles,
    environment,
    contentFetcherConfig,
    adapters,
    adapterFactory,
  };
}

describe("createCollectorRuntimeFactory", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("uses default dependencies when an override is explicitly undefined", async () => {
    const config = createConfig();
    const loggerHarness = createLogger();
    const setup = createDependencies();
    const factory = createCollectorRuntimeFactory({
      ...setup.dependencies,
      createHealthContext: undefined,
    });

    const ctx = await factory.createRuntime(config, loggerHarness);

    expect(ctx.healthContext).not.toBe(setup.healthContext);
    expect(setup.dependencies.createHealthContext).not.toHaveBeenCalled();
  });

  it("creates a runtime context with healthy dependencies and initialized adapters", async () => {
    const config = createConfig();
    const loggerHarness = createLogger();
    const {
      dependencies,
      healthContext,
      healthServer,
      checkpointStore,
      allowlist,
      kafkaProducerConnection,
      marketFilterProfiles,
      environment,
      adapters,
      contentFetcherConfig,
    } = createDependencies();
    const configuredAdapterFactory = {
      buildIngestionAdapters: vi.fn(() => ({
        adapters,
        unsupportedEnabledAdapters: ["reddit"],
      })),
    } as CollectorIngestionAdapterFactory;
    dependencies.createCollectorAdapterFactory = vi.fn(() => configuredAdapterFactory);
    const factory = createCollectorRuntimeFactory(dependencies);

    const ctx = await factory.createRuntime(config, loggerHarness);

    expect(ctx.config).toBe(config);
    expect(ctx.logger).toBe(loggerHarness);
    expect(ctx.healthContext).toBe(healthContext);
    expect(ctx.healthServer).toBe(healthServer);
    expect(ctx.checkpointStore).toBe(checkpointStore);
    expect(ctx.allowlist).toBe(allowlist);
    expect(ctx.kafkaContext.producer).toBe(kafkaProducerConnection);
    expect(ctx.marketFilterProfiles).toBe(marketFilterProfiles);
    expect(ctx.adapters).toEqual(adapters);
    expect(ctx.shutdownRequested).toBe(false);
    expect(ctx.lastSeenCleanupAt).toBe(0);

    expect(dependencies.createCheckpointStore).toHaveBeenCalledWith(
      config.CHECKPOINT_PATH,
      loggerHarness.childFor("checkpoint")
    );
    expect(dependencies.initializeCheckpointStore).toHaveBeenCalledWith(checkpointStore);
    expect(dependencies.loadAllowlist).toHaveBeenCalledWith(config.TOPICS_ALLOWLIST_PATH);
    expect(dependencies.createKafkaProducer).toHaveBeenCalledWith(config, loggerHarness);
    expect(dependencies.loadMarketFilterProfiles).toHaveBeenCalledWith(config.MARKET_FILTERS_DIR);
    expect(dependencies.getEnvironment).toHaveBeenCalledTimes(1);
    expect(dependencies.createContentFetcherConfig).toHaveBeenCalledWith(environment);
    expect(dependencies.createCollectorAdapterFactory).toHaveBeenCalledTimes(1);
    expect(configuredAdapterFactory.buildIngestionAdapters).toHaveBeenCalledWith({
      config,
      checkpointStore,
      logger: loggerHarness,
      contentFetcherConfig,
      marketFilterProfiles,
      onRssFeedError: expect.any(Function),
      onRssFeedSuccess: expect.any(Function),
    });

    expect(adapters[0].initialize).toHaveBeenCalledTimes(1);
    expect(loggerHarness.child).toHaveBeenCalledWith({ component: "checkpoint" });
    expect(loggerHarness.warn).toHaveBeenCalledWith(
      { unsupportedAdapters: ["reddit"] },
      "Adapter flags enabled without implementation in this collector build"
    );
    expect(healthContext.checkpointsHealthy).toBe(true);
    expect(healthContext.allowlistHealthy).toBe(true);
    expect(healthContext.kafkaHealthy).toBe(true);
    expect(healthContext.sourceHealth.get("rss")).toEqual({ status: "healthy" });
  });

  it("rolls back initialized resources in reverse order when adapter initialization fails", async () => {
    const config = createConfig();
    const loggerHarness = createLogger();
    const cleanupOrder: string[] = [];
    const adapterOne = createSourceAdapter("rss", "rss", {
      shutdown: vi.fn(async () => {
        cleanupOrder.push("adapter-rss");
      }),
    });
    const adapterTwo = createSourceAdapter("hackernews", "hackernews", {
      initialize: vi.fn(async () => {
        throw new Error("adapter init failed");
      }),
    });
    const setup = createDependencies({
      disconnectProducer: vi.fn(async () => {
        cleanupOrder.push("kafka-producer");
      }),
      closeCheckpointStore: vi.fn(() => {
        cleanupOrder.push("checkpoint-store");
      }),
      closeHealthServer: vi.fn(async () => {
        cleanupOrder.push("health-server");
      }),
    });
    const failingAdapterFactory = {
      buildIngestionAdapters: vi.fn(() => ({
        adapters: [adapterOne, adapterTwo],
        unsupportedEnabledAdapters: [],
      })),
    } as CollectorIngestionAdapterFactory;
    setup.dependencies.createCollectorAdapterFactory = vi.fn(() => failingAdapterFactory);
    const factory = createCollectorRuntimeFactory(setup.dependencies);

    await expect(factory.createRuntime(config, loggerHarness)).rejects.toThrow("adapter init failed");

    expect(cleanupOrder).toEqual([
      "adapter-rss",
      "kafka-producer",
      "checkpoint-store",
      "health-server",
    ]);
  });

  it("fails fast when a dependency override is not a function", () => {
    expect(() =>
      createCollectorRuntimeFactory({
        createKafkaProducer: 123 as unknown as CollectorRuntimeFactoryDependencies["createKafkaProducer"],
      })
    ).toThrow('Collector runtime dependency override "createKafkaProducer" must be a function');
  });
});
