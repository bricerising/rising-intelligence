import type { Server } from "node:http";
import {
  createProducerConnection,
  type ProducerConnection,
} from "@rising-intelligence/pipeline/transport";
import {
  createFunctionDependencyFactory,
  createRuntimeCompositionRoot,
  healthServerSpec,
  kafkaProducerSpec,
  type FunctionDependencyOverrides,
} from "@rising-intelligence/shared/lifecycle";
import { closeServer } from "@rising-intelligence/shared/http";
import { createComponentLoggerFactory } from "@rising-intelligence/shared/logging";
import type pino from "pino";
import {
  createCollectorAdapterFactory,
  type CollectorIngestionAdapterFactory,
} from "./adapters/factory.js";
import { CheckpointStore } from "./checkpoint.js";
import type { Config } from "./config.js";
import {
  createContentFetcherConfig,
  type ContentFetcherConfig,
} from "./content-fetcher.js";
import {
  createHealthContext,
  incrementRssFeedError,
  recordFeedError,
  recordFeedSuccess,
  startHealthServer,
  type HealthContext,
} from "./health.js";
import {
  loadMarketFilterProfiles,
  type MarketFilterProfile,
} from "./market-filters.js";
import { loadAllowlist, type CompiledAllowlist } from "@rising-intelligence/pipeline";
import type { CollectorIngestionAdapter } from "./types.js";

type RuntimeLoggerComponent = "checkpoint";
interface PipelineProducerContext {
  producer: ProducerConnection;
}

export interface CollectorRuntimeFactoryDependencies {
  createHealthContext(): HealthContext;
  startHealthServer(ctx: HealthContext, logger: pino.Logger): Server;
  closeHealthServer(server: Server): Promise<void>;
  createCheckpointStore(path: string, logger: pino.Logger): CheckpointStore;
  initializeCheckpointStore(store: CheckpointStore): Promise<void>;
  closeCheckpointStore(store: CheckpointStore): void;
  loadAllowlist(path: string): CompiledAllowlist;
  createKafkaProducer(config: Config, logger: pino.Logger): Promise<ProducerConnection>;
  disconnectProducer(producer: ProducerConnection, logger: pino.Logger): Promise<void>;
  loadMarketFilterProfiles(path: string): MarketFilterProfile[];
  getEnvironment(): NodeJS.ProcessEnv;
  createContentFetcherConfig(env: NodeJS.ProcessEnv): ContentFetcherConfig;
  createCollectorAdapterFactory(): CollectorIngestionAdapterFactory;
}

export interface CollectorRuntimeContext {
  config: Config;
  logger: pino.Logger;
  kafkaContext: PipelineProducerContext;
  healthContext: HealthContext;
  healthServer: Server;
  checkpointStore: CheckpointStore;
  allowlist: CompiledAllowlist;
  marketFilterProfiles: MarketFilterProfile[];
  adapters: CollectorIngestionAdapter[];
  shutdownRequested: boolean;
  lastSeenCleanupAt: number;
}

export interface CollectorRuntimeFactory {
  createRuntime(config: Config, logger: pino.Logger): Promise<CollectorRuntimeContext>;
}

type DependencyOverrides = FunctionDependencyOverrides<CollectorRuntimeFactoryDependencies>;

const DEFAULT_DEPENDENCIES: CollectorRuntimeFactoryDependencies = {
  createHealthContext,
  startHealthServer,
  closeHealthServer: closeServer,
  createCheckpointStore(path, logger): CheckpointStore {
    return new CheckpointStore(path, logger);
  },
  initializeCheckpointStore(store): Promise<void> {
    return store.initialize();
  },
  closeCheckpointStore(store): void {
    store.close();
  },
  loadAllowlist,
  async createKafkaProducer(config, logger): Promise<ProducerConnection> {
    return createProducerConnection({
      brokers: config.KAFKA_BROKERS,
      clientId: config.KAFKA_CLIENT_ID,
      logger,
    });
  },
  async disconnectProducer(producer, logger): Promise<void> {
    await producer.disconnect();
    logger.info("Kafka producer disconnected");
  },
  loadMarketFilterProfiles,
  getEnvironment(): NodeJS.ProcessEnv {
    return process.env;
  },
  createContentFetcherConfig,
  createCollectorAdapterFactory,
};

class DefaultCollectorRuntimeFactory implements CollectorRuntimeFactory {
  constructor(
    private readonly dependencies: CollectorRuntimeFactoryDependencies
  ) {}

  async createRuntime(config: Config, logger: pino.Logger): Promise<CollectorRuntimeContext> {
    const { startup, resources } = createRuntimeCompositionRoot(logger);
    const adapterFactory = this.dependencies.createCollectorAdapterFactory();
    const componentLoggers =
      createComponentLoggerFactory<RuntimeLoggerComponent>(logger);

    return startup.run(async () => {
      const healthContext = this.dependencies.createHealthContext();
      const healthServer = await resources.connect(healthServerSpec(
        () => this.dependencies.startHealthServer(healthContext, logger),
        (server) => this.dependencies.closeHealthServer(server)
      ));

      const checkpointStore = await resources.connect({
        name: "checkpoint-store",
        connect: async () => {
          const store = this.dependencies.createCheckpointStore(
            config.CHECKPOINT_PATH,
            componentLoggers.create("checkpoint")
          );
          await this.dependencies.initializeCheckpointStore(store);
          return store;
        },
        disconnect: async (store) => {
          this.dependencies.closeCheckpointStore(store);
        },
        rollbackAction: "close",
      });
      healthContext.checkpointsHealthy = true;

      let allowlist: CompiledAllowlist;
      try {
        allowlist = this.dependencies.loadAllowlist(config.TOPICS_ALLOWLIST_PATH);
        healthContext.allowlistHealthy = true;
        logger.info(
          { topicCount: allowlist.topics.length },
          "Topics allowlist loaded"
        );
      } catch (error) {
        logger.error({ error }, "Failed to load topics allowlist");
        healthContext.allowlistHealthy = false;
        throw error;
      }

      const producerConnection = await resources.connect(kafkaProducerSpec(
        () => this.dependencies.createKafkaProducer(config, logger),
        (connection) => this.dependencies.disconnectProducer(connection, logger)
      ));
      healthContext.kafkaHealthy = true;

      let marketFilterProfiles: MarketFilterProfile[];
      try {
        marketFilterProfiles = this.dependencies.loadMarketFilterProfiles(config.MARKET_FILTERS_DIR);
        logger.info(
          { profileCount: marketFilterProfiles.length, dir: config.MARKET_FILTERS_DIR },
          "Market filter profiles loaded"
        );
      } catch (error) {
        logger.error(
          { error, dir: config.MARKET_FILTERS_DIR },
          "Failed to load market filter profiles"
        );
        throw error;
      }

      const contentFetcherConfig = this.dependencies.createContentFetcherConfig(
        this.dependencies.getEnvironment()
      );
      logger.info(
        { enabled: contentFetcherConfig.enabled, timeoutMs: contentFetcherConfig.timeoutMs },
        "Content fetcher configuration loaded"
      );

      const { adapters, unsupportedEnabledAdapters } = adapterFactory.buildIngestionAdapters({
        config,
        checkpointStore,
        logger,
        contentFetcherConfig,
        marketFilterProfiles,
        onRssFeedError: ({ feed, feedUrl, errorType }) => {
          incrementRssFeedError(healthContext, { feed, feedUrl, errorType });
          recordFeedError(healthContext, { feed, feedUrl });
        },
        onRssFeedSuccess: ({ feed, feedUrl, itemCount }) => {
          recordFeedSuccess(healthContext, { feed, feedUrl, itemCount });
        },
      });

      if (unsupportedEnabledAdapters.length > 0) {
        logger.warn(
          { unsupportedAdapters: unsupportedEnabledAdapters },
          "Adapter flags enabled without implementation in this collector build"
        );
      }

      for (const adapter of adapters) {
        await resources.connect({
          name: `adapter-${adapter.name}`,
          connect: async () => {
            await adapter.initialize();
            return adapter;
          },
          disconnect: (initializedAdapter) => initializedAdapter.shutdown(),
          rollbackAction: "shutdown",
        });
        healthContext.sourceHealth.set(adapter.name, {
          status: "healthy",
        });
        logger.info({ adapter: adapter.name }, "Adapter initialized");
      }

      return {
        config,
        logger,
        kafkaContext: {
          producer: producerConnection,
        },
        healthContext,
        healthServer,
        checkpointStore,
        allowlist,
        marketFilterProfiles,
        adapters,
        shutdownRequested: false,
        lastSeenCleanupAt: 0,
      };
    });
  }
}

export function createCollectorRuntimeFactory(
  overrides: DependencyOverrides = {}
): CollectorRuntimeFactory {
  return createFunctionDependencyFactory({
    targetName: "Collector runtime dependency",
    defaults: DEFAULT_DEPENDENCIES,
    overrides,
    create(dependencies): CollectorRuntimeFactory {
      return new DefaultCollectorRuntimeFactory(dependencies);
    },
  });
}
