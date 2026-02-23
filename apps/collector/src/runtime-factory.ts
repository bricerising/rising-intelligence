import type { Server } from "node:http";
import {
  createFunctionDependencyFactory,
  closeServer,
  createStartupFacade,
  createStartupResourceConnector,
  type FunctionDependencyOverrides,
} from "@rising-intelligence/shared";
import type pino from "pino";
import {
  createCollectorAdapterFactory,
  type CollectorAdapterFactory,
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
  startHealthServer,
  type HealthContext,
} from "./health.js";
import {
  createKafkaProducer,
  disconnectProducer,
  type KafkaProducerContext,
} from "./kafka/producer.js";
import {
  loadMarketFilterProfiles,
  type MarketFilterProfile,
} from "./market-filters.js";
import { loadAllowlist, type CompiledAllowlist } from "./topics/extractor.js";
import type { SourceAdapter } from "./types.js";

type KafkaProducer = KafkaProducerContext["producer"];

export interface CollectorRuntimeFactoryDependencies {
  createHealthContext(): HealthContext;
  startHealthServer(ctx: HealthContext, logger: pino.Logger): Server;
  closeHealthServer(server: Server): Promise<void>;
  createCheckpointStore(path: string, logger: pino.Logger): CheckpointStore;
  initializeCheckpointStore(store: CheckpointStore): Promise<void>;
  closeCheckpointStore(store: CheckpointStore): void;
  loadAllowlist(path: string): CompiledAllowlist;
  createKafkaProducer(logger: pino.Logger): Promise<KafkaProducerContext>;
  disconnectProducer(producer: KafkaProducer, logger: pino.Logger): Promise<void>;
  loadMarketFilterProfiles(path: string): MarketFilterProfile[];
  getEnvironment(): NodeJS.ProcessEnv;
  createContentFetcherConfig(env: NodeJS.ProcessEnv): ContentFetcherConfig;
  createCollectorAdapterFactory(): CollectorAdapterFactory;
}

export interface CollectorRuntimeContext {
  config: Config;
  logger: pino.Logger;
  kafkaContext: KafkaProducerContext;
  healthContext: HealthContext;
  healthServer: Server;
  checkpointStore: CheckpointStore;
  allowlist: CompiledAllowlist;
  marketFilterProfiles: MarketFilterProfile[];
  adapters: SourceAdapter[];
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
  createKafkaProducer,
  disconnectProducer,
  loadMarketFilterProfiles,
  getEnvironment(): NodeJS.ProcessEnv {
    return process.env;
  },
  createContentFetcherConfig,
  createCollectorAdapterFactory,
};

class DefaultCollectorRuntimeFactory implements CollectorRuntimeFactory {
  private readonly adapterFactory: CollectorAdapterFactory;

  constructor(
    private readonly dependencies: CollectorRuntimeFactoryDependencies
  ) {
    this.adapterFactory = dependencies.createCollectorAdapterFactory();
  }

  async createRuntime(config: Config, logger: pino.Logger): Promise<CollectorRuntimeContext> {
    const startup = createStartupFacade(logger);
    const resources = createStartupResourceConnector(startup);

    return startup.run(async () => {
      const healthContext = this.dependencies.createHealthContext();
      const healthServer = await resources.connect({
        name: "health-server",
        connect: () => this.dependencies.startHealthServer(healthContext, logger),
        disconnect: (server) => this.dependencies.closeHealthServer(server),
        rollbackAction: "close",
      });

      const checkpointStore = await resources.connect({
        name: "checkpoint-store",
        connect: async () => {
          const store = this.dependencies.createCheckpointStore(
            config.CHECKPOINT_PATH,
            logger.child({ component: "checkpoint" })
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

      const kafkaContext = await resources.connect({
        name: "kafka-producer",
        connect: () => this.dependencies.createKafkaProducer(logger),
        disconnect: (context) => this.dependencies.disconnectProducer(context.producer, logger),
        rollbackAction: "disconnect",
      });
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

      const { adapters, unsupportedEnabledAdapters } = this.adapterFactory.build({
        config,
        checkpointStore,
        logger,
        contentFetcherConfig,
        marketFilterProfiles,
        onRssFeedError: ({ feed, feedUrl, errorType }) => {
          incrementRssFeedError(healthContext, {
            feed,
            feedUrl,
            errorType,
          });
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
        kafkaContext,
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
