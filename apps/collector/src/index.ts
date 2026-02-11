import { Server } from "node:http";
import {
  closeServer,
  serializeError,
  runService,
  runShutdownSteps,
  createServiceLogger,
  BackoffManager,
  sleep,
  isRateLimitError,
  isTransientError,
} from "@rising-intelligence/shared";
import type pino from "pino";
import { getConfig } from "./config.js";
import {
  createKafkaProducer,
  disconnectProducer,
  KafkaProducerContext,
} from "./kafka/producer.js";
import {
  type CollectorErrorType,
  createHealthContext,
  startHealthServer,
  HealthContext,
  incrementEventsFailed,
  observePollDuration,
  observePollItemsCount,
  incrementCheckpointUpdated,
  incrementRateLimitBackoff,
} from "./health.js";
import { CheckpointStore } from "./checkpoint.js";
import { loadAllowlist, CompiledAllowlist } from "./topics/extractor.js";
import type { SourceAdapter, CollectorHeartbeat } from "./types.js";
import { createContentFetcherConfig } from "./content-fetcher.js";
import { buildCollectorAdapters } from "./adapters/factory.js";
import { createCollectorEventProcessor } from "./ingestion-pipeline.js";
import { createCollectorPublisher } from "./publishing-facade.js";

interface CollectorContext {
  config: ReturnType<typeof getConfig>;
  logger: pino.Logger;
  kafkaContext: KafkaProducerContext;
  healthContext: HealthContext;
  healthServer: Server;
  checkpointStore: CheckpointStore;
  allowlist: CompiledAllowlist;
  adapters: SourceAdapter[];
  shutdownRequested: boolean;
  lastSeenCleanupAt: number;
}

function mapUnknownErrorType(error: unknown): CollectorErrorType {
  if (!(error instanceof Error)) {
    return "parse_error";
  }

  const normalized = error.message.toLowerCase();
  if (normalized.includes("kafka")) {
    return "kafka_error";
  }
  if (
    normalized.includes("auth")
    || normalized.includes("unauthorized")
    || normalized.includes("forbidden")
  ) {
    return "auth_error";
  }
  return "parse_error";
}

async function initializeCollector(): Promise<CollectorContext> {
  const config = getConfig();
  const logger = createServiceLogger(config.SERVICE_NAME, config.LOG_LEVEL);

  logger.info({ service: config.SERVICE_NAME }, "Starting collector service");

  const healthContext = createHealthContext();
  const healthServer = startHealthServer(healthContext, logger);

  const checkpointStore = new CheckpointStore(
    config.CHECKPOINT_PATH,
    logger.child({ component: "checkpoint" })
  );
  await checkpointStore.initialize();
  healthContext.checkpointsHealthy = true;

  let allowlist: CompiledAllowlist;
  try {
    allowlist = loadAllowlist(config.TOPICS_ALLOWLIST_PATH);
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

  const kafkaContext = await createKafkaProducer(logger);
  healthContext.kafkaHealthy = true;

  const contentFetcherConfig = createContentFetcherConfig(process.env);
  logger.info(
    { enabled: contentFetcherConfig.enabled, timeoutMs: contentFetcherConfig.timeoutMs },
    "Content fetcher configuration loaded"
  );

  const { adapters, unsupportedEnabledAdapters } = buildCollectorAdapters({
    config,
    checkpointStore,
    logger,
    contentFetcherConfig,
  });

  if (unsupportedEnabledAdapters.length > 0) {
    logger.warn(
      { unsupportedAdapters: unsupportedEnabledAdapters },
      "Adapter flags enabled without implementation in this collector build"
    );
  }

  for (const adapter of adapters) {
    await adapter.initialize();
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
    adapters,
    shutdownRequested: false,
    lastSeenCleanupAt: 0,
  };
}

async function runAdapter(
  ctx: CollectorContext,
  adapter: SourceAdapter
): Promise<void> {
  const { kafkaContext, healthContext, checkpointStore, allowlist } = ctx;
  const adapterLogger = ctx.logger.child({ adapter: adapter.name });
  const publisher = createCollectorPublisher({
    producer: kafkaContext.producer,
    logger: adapterLogger,
  });
  const backoff = new BackoffManager(adapter.name, adapterLogger);
  const eventProcessor = createCollectorEventProcessor({
    adapterName: adapter.name,
    adapterSource: adapter.source,
    allowlist,
    checkpointStore,
    healthContext,
    logger: adapterLogger,
    publishRawEvent: (event) => publisher.publishRawEvent(event),
    publishDeadLetterEvent: (event) => publisher.publishDeadLetterEvent(event),
  });

  while (!ctx.shutdownRequested) {
    const pollStartTime = Date.now();
    try {
      let batchCount = 0;
      let lastCheckpointKey: string | null = null;
      let lastCheckpointValue: string | null = null;

      for await (const { event, checkpointKey, checkpointValue } of adapter.fetch()) {
        if (ctx.shutdownRequested) break;

        lastCheckpointKey = checkpointKey;
        lastCheckpointValue = checkpointValue;

        const processingResult = await eventProcessor.process(event);
        if (processingResult.status === "ingested") {
          batchCount++;
        }
      }

      if (lastCheckpointKey && lastCheckpointValue) {
        checkpointStore.setCheckpoint(
          adapter.name,
          lastCheckpointKey,
          lastCheckpointValue
        );
        incrementCheckpointUpdated(healthContext, adapter.source);
      }

      healthContext.sourceHealth.set(adapter.name, {
        status: "healthy",
        last_poll_at: new Date().toISOString(),
        items_fetched: batchCount,
      });
      observePollDuration(healthContext, adapter.source, (Date.now() - pollStartTime) / 1000);
      observePollItemsCount(healthContext, adapter.source, batchCount);

      const now = Date.now();
      if (now - ctx.lastSeenCleanupAt > 60 * 60 * 1000) {
        checkpointStore.cleanupSeen("-7 days");
        ctx.lastSeenCleanupAt = now;
      }

      const heartbeat: CollectorHeartbeat = {
        source: adapter.source,
        timestamp: new Date().toISOString(),
        last_fetch_at: new Date().toISOString(),
        items_fetched: batchCount,
        status: "healthy",
      };
      await publisher.publishHeartbeat(heartbeat);

      adapterLogger.info({ batchCount }, "Poll cycle complete");
      backoff.reset();

      if (ctx.shutdownRequested) break;
      await sleep(adapter.pollIntervalMs);

    } catch (error) {
      adapterLogger.error({ error }, "Adapter error");

      const lastSuccessfulPollAt = healthContext.sourceHealth.get(adapter.name)?.last_poll_at;

      healthContext.sourceHealth.set(adapter.name, {
        status: "error",
        error_message: error instanceof Error ? error.message : String(error),
      });

      const heartbeat: CollectorHeartbeat = {
        source: adapter.source,
        timestamp: new Date().toISOString(),
        last_fetch_at: lastSuccessfulPollAt ?? new Date().toISOString(),
        items_fetched: 0,
        status: "error",
        error_message: error instanceof Error ? error.message : String(error),
      };
      try {
        await publisher.publishHeartbeat(heartbeat);
      } catch {
        // Ignore heartbeat publish errors
      }

      if (ctx.shutdownRequested) break;
      if (isRateLimitError(error)) {
        incrementEventsFailed(healthContext, adapter.source, "rate_limit");
        incrementRateLimitBackoff(healthContext, adapter.source);
        await backoff.waitRateLimit();
      } else if (isTransientError(error)) {
        incrementEventsFailed(healthContext, adapter.source, "network_error");
        await backoff.waitTransient();
      } else {
        incrementEventsFailed(
          healthContext,
          adapter.source,
          mapUnknownErrorType(error)
        );
        await backoff.waitTransient();
      }
    }
  }
}

async function gracefulShutdown(ctx: CollectorContext): Promise<void> {
  const { logger, kafkaContext, healthServer, checkpointStore, adapters } = ctx;

  ctx.shutdownRequested = true;

  for (const adapter of adapters) {
    try {
      await adapter.shutdown();
      logger.info({ adapter: adapter.name }, "Adapter shut down");
    } catch (error) {
      logger.error(
        { adapter: adapter.name, error: serializeError(error) },
        "Adapter shutdown error"
      );
    }
  }

  checkpointStore.close();
  logger.info("Checkpoints flushed");

  await runShutdownSteps(logger, [
    {
      name: "kafka-producer",
      run: async () => disconnectProducer(kafkaContext.producer, logger),
      errorMessage: "Kafka producer disconnect failed",
      onSuccess: () => {
        ctx.healthContext.kafkaHealthy = false;
      },
    },
    {
      name: "health-server",
      run: async () => closeServer(healthServer),
      errorMessage: "Health server close failed",
      onSuccess: () => {
        logger.info("Health server closed");
      },
    },
  ]);
}

let _logger: pino.Logger | null = null;
let _runtimeConfig: ReturnType<typeof getConfig> | null = null;

function getRuntimeConfig(): ReturnType<typeof getConfig> {
  if (!_runtimeConfig) {
    _runtimeConfig = getConfig();
  }
  return _runtimeConfig;
}

runService<CollectorContext>({
  name: getRuntimeConfig().SERVICE_NAME,
  shutdownTimeoutMs: getRuntimeConfig().SHUTDOWN_TIMEOUT_MS,
  getLogger() {
    if (!_logger) {
      const config = getRuntimeConfig();
      _logger = createServiceLogger(config.SERVICE_NAME, config.LOG_LEVEL);
    }
    return _logger;
  },
  async initialize() {
    const ctx = await initializeCollector();
    _logger = ctx.logger;
    return ctx;
  },
  async run(ctx) {
    ctx.logger.info(
      { adapterCount: ctx.adapters.length },
      "Starting adapter loops"
    );
    await Promise.all(
      ctx.adapters.map((adapter) => runAdapter(ctx, adapter))
    );
  },
  async shutdown(ctx) {
    await gracefulShutdown(ctx);
  },
});
