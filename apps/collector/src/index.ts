import { Server } from "node:http";
import {
  closeServer,
  serializeError,
  runService,
  createServiceLogger,
  BackoffManager,
  sleep,
  isRateLimitError,
  isTransientError,
} from "@rising-intelligence/shared";
import type pino from "pino";
import { loadConfig } from "./config.js";
import {
  createKafkaProducer,
  disconnectProducer,
  publishEvent,
  TOPICS,
  KafkaProducerContext,
} from "./kafka/producer.js";
import {
  createHealthContext,
  startHealthServer,
  HealthContext,
  incrementEventsIngested,
  incrementEventsFailed,
  observePollDuration,
  observePollItemsCount,
  incrementCheckpointUpdated,
  incrementRateLimitBackoff,
  incrementTopicsExtracted,
} from "./health.js";
import { CheckpointStore } from "./checkpoint.js";
import { loadAllowlist, extractTopics, CompiledAllowlist } from "./topics/extractor.js";
import { serializeRawEvent, serializeDeadLetterEvent, serializeHeartbeat, generateDlqId } from "./serializer.js";
import type { SourceAdapter, DeadLetterEvent, CollectorHeartbeat } from "./types.js";

// Import adapters
import { createRSSAdapter } from "./adapters/rss.js";
import { createHackerNewsAdapter } from "./adapters/hackernews.js";
import { createLobstersAdapter } from "./adapters/lobsters.js";

interface CollectorContext {
  config: ReturnType<typeof loadConfig>;
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

type CollectorConfig = ReturnType<typeof loadConfig>;

interface AdapterFactory {
  name: string;
  isEnabled(config: CollectorConfig): boolean;
  create(config: CollectorConfig, checkpointStore: CheckpointStore, logger: pino.Logger): SourceAdapter;
}

const ADAPTER_FACTORIES: ReadonlyArray<AdapterFactory> = [
  {
    name: "rss",
    isEnabled: (config) => config.RSS_ENABLED,
    create: (config, checkpointStore, logger) =>
      createRSSAdapter(
        config.FEEDS_CONFIG_PATH,
        config.RSS_POLL_INTERVAL_SECONDS * 1000,
        checkpointStore,
        logger.child({ adapter: "rss" })
      ),
  },
  {
    name: "hackernews",
    isEnabled: (config) => config.HN_ENABLED,
    create: (config, checkpointStore, logger) =>
      createHackerNewsAdapter(
        config.HN_MODE,
        config.HN_POLL_INTERVAL_SECONDS * 1000,
        config.HN_MAX_ITEMS_PER_POLL,
        checkpointStore,
        logger.child({ adapter: "hackernews" })
      ),
  },
  {
    name: "lobsters",
    isEnabled: (config) => config.LOBSTERS_ENABLED,
    create: (config, checkpointStore, logger) =>
      createLobstersAdapter(
        config.LOBSTERS_POLL_INTERVAL_SECONDS * 1000,
        config.LOBSTERS_MAX_ITEMS_PER_POLL,
        checkpointStore,
        logger.child({ adapter: "lobsters" })
      ),
  },
];

function mapUnknownErrorType(error: unknown): string {
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

function buildAdapters(config: CollectorConfig, checkpointStore: CheckpointStore, logger: pino.Logger): SourceAdapter[] {
  const adapters: SourceAdapter[] = [];

  for (const factory of ADAPTER_FACTORIES) {
    if (!factory.isEnabled(config)) {
      continue;
    }
    adapters.push(factory.create(config, checkpointStore, logger));
  }

  return adapters;
}

async function initializeCollector(): Promise<CollectorContext> {
  const config = loadConfig();
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

  const adapters = buildAdapters(config, checkpointStore, logger);

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
  const backoff = new BackoffManager(adapter.name, adapterLogger);

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

        if (checkpointStore.hasSeen(adapter.source, event.event_id)) {
          adapterLogger.debug({ eventId: event.event_id }, "Duplicate event skipped");
          continue;
        }

        const topics = extractTopics(
          { title: event.title, text: event.text },
          allowlist
        );
        event.tags = topics;
        for (const topic of topics) {
          incrementTopicsExtracted(healthContext, topic);
        }

        if (!event.event_id || !event.text) {
          const dlqEvent: DeadLetterEvent = {
            dlq_id: generateDlqId(),
            occurred_at: new Date().toISOString(),
            source: adapter.name,
            error_code: "VALIDATION_FAILED",
            error_message: "Missing required fields: event_id or text",
            raw_reference: event.url ?? event.event_id,
          };

          await publishEvent(
            kafkaContext.producer,
            TOPICS.DLQ,
            dlqEvent.dlq_id,
            serializeDeadLetterEvent(dlqEvent),
            adapterLogger
          );
          incrementEventsFailed(healthContext, adapter.source, "parse_error");
          continue;
        }

        await publishEvent(
          kafkaContext.producer,
          TOPICS.RAW_EVENTS,
          event.event_id,
          serializeRawEvent(event),
          adapterLogger
        );

        checkpointStore.markSeen(adapter.source, event.event_id);
        incrementEventsIngested(healthContext, adapter.source);
        healthContext.lastEventAt = new Date();

        batchCount++;
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
      await publishEvent(
        kafkaContext.producer,
        TOPICS.HEARTBEAT,
        adapter.source,
        serializeHeartbeat(heartbeat),
        adapterLogger
      );

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
        await publishEvent(
          kafkaContext.producer,
          TOPICS.HEARTBEAT,
          adapter.source,
          serializeHeartbeat(heartbeat),
          adapterLogger
        );
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

  try {
    await disconnectProducer(kafkaContext.producer, logger);
    ctx.healthContext.kafkaHealthy = false;
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Kafka producer disconnect failed");
  }

  try {
    await closeServer(healthServer);
    logger.info("Health server closed");
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Health server close failed");
  }
}

let _logger: pino.Logger | null = null;

runService<CollectorContext>({
  name: "collector",
  shutdownTimeoutMs: 30000,
  getLogger() {
    if (!_logger) {
      _logger = createServiceLogger("collector", "info");
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
