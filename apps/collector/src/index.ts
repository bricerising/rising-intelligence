import { Server } from "node:http";
import { Producer } from "kafkajs";
import { loadConfig, getConfig } from "./config.js";
import { getLogger, createChildLogger } from "./logger.js";
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
  incrementEventsPublished,
  incrementEventsDlq,
  incrementError,
  recordLastPoll,
  incrementCheckpointsWritten,
} from "./health.js";
import { CheckpointStore } from "./checkpoint.js";
import { loadAllowlist, extractTopics, CompiledAllowlist } from "./topics/extractor.js";
import { BackoffManager, sleep, isRateLimitError, isTransientError } from "./backoff.js";
import { serializeRawEvent, serializeDeadLetterEvent, serializeHeartbeat, generateDlqId } from "./serializer.js";
import type { SourceAdapter, RawEvent, DeadLetterEvent, CollectorHeartbeat, Source } from "./types.js";

// Import adapters
import { createRSSAdapter } from "./adapters/rss.js";
import { createHackerNewsAdapter } from "./adapters/hackernews.js";
import { createLobstersAdapter } from "./adapters/lobsters.js";

interface CollectorContext {
  config: ReturnType<typeof loadConfig>;
  logger: ReturnType<typeof getLogger>;
  kafkaContext: KafkaProducerContext;
  healthContext: HealthContext;
  healthServer: Server;
  checkpointStore: CheckpointStore;
  allowlist: CompiledAllowlist;
  adapters: SourceAdapter[];
  shutdownRequested: boolean;
  lastSeenCleanupAt: number;
}

async function initializeCollector(): Promise<CollectorContext> {
  // Load config first (validates env vars)
  const config = loadConfig();
  const logger = getLogger();

  logger.info({ service: config.SERVICE_NAME }, "Starting collector service");

  // Create health context
  const healthContext = createHealthContext();

  // Start health server early
  const healthServer = startHealthServer(healthContext, logger);

  // Initialize checkpoint store
  const checkpointStore = new CheckpointStore(
    config.CHECKPOINT_PATH,
    createChildLogger({ component: "checkpoint" })
  );
  await checkpointStore.initialize();
  healthContext.checkpointsHealthy = true;

  // Load topics allowlist
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

  // Connect to Kafka
  const kafkaContext = await createKafkaProducer(logger);
  healthContext.kafkaHealthy = true;

  // Initialize adapters based on config
  const adapters: SourceAdapter[] = [];

  if (config.RSS_ENABLED) {
    adapters.push(
      createRSSAdapter(
        config.FEEDS_CONFIG_PATH,
        config.RSS_POLL_INTERVAL_SECONDS * 1000,
        checkpointStore,
        createChildLogger({ adapter: "rss" })
      )
    );
  }

  if (config.HN_ENABLED) {
    adapters.push(
      createHackerNewsAdapter(
        config.HN_MODE,
        config.HN_POLL_INTERVAL_SECONDS * 1000,
        config.HN_MAX_ITEMS_PER_POLL,
        checkpointStore,
        createChildLogger({ adapter: "hackernews" })
      )
    );
  }

  if (config.LOBSTERS_ENABLED) {
    adapters.push(
      createLobstersAdapter(
        config.LOBSTERS_POLL_INTERVAL_SECONDS * 1000,
        config.LOBSTERS_MAX_ITEMS_PER_POLL,
        checkpointStore,
        createChildLogger({ adapter: "lobsters" })
      )
    );
  }

  // Initialize all adapters
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
  const { logger, kafkaContext, healthContext, checkpointStore, allowlist } = ctx;
  const adapterLogger = createChildLogger({ adapter: adapter.name });
  const backoff = new BackoffManager(adapter.name, adapterLogger);

  while (!ctx.shutdownRequested) {
    try {
      let batchCount = 0;
      let lastCheckpointKey: string | null = null;
      let lastCheckpointValue: string | null = null;

      for await (const { event, checkpointKey, checkpointValue } of adapter.fetch()) {
        if (ctx.shutdownRequested) break;

        // Always advance our local "last checkpoint" for handled items.
        // This prevents repeated reprocessing of duplicate/invalid items.
        lastCheckpointKey = checkpointKey;
        lastCheckpointValue = checkpointValue;

        // Check if already seen (dedup)
        if (checkpointStore.hasSeen(adapter.source, event.event_id)) {
          adapterLogger.debug({ eventId: event.event_id }, "Duplicate event skipped");
          continue;
        }

        // Extract topics
        const topics = extractTopics(
          { title: event.title, text: event.text },
          allowlist
        );
        event.tags = topics;

        // Validate event (basic checks)
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
          incrementEventsDlq(healthContext, adapter.source);
          continue;
        }

        // Publish to Kafka
        await publishEvent(
          kafkaContext.producer,
          TOPICS.RAW_EVENTS,
          event.event_id,
          serializeRawEvent(event),
          adapterLogger
        );

        // Mark as seen
        checkpointStore.markSeen(adapter.source, event.event_id);
        incrementEventsPublished(healthContext, adapter.source);
        healthContext.lastEventAt = new Date();

        batchCount++;
      }

      // Update checkpoint after successful batch
      if (lastCheckpointKey && lastCheckpointValue) {
        checkpointStore.setCheckpoint(
          adapter.name,
          lastCheckpointKey,
          lastCheckpointValue
        );
        incrementCheckpointsWritten(healthContext, adapter.source);
      }

      // Update health status
      healthContext.sourceHealth.set(adapter.name, {
        status: "healthy",
        last_poll_at: new Date().toISOString(),
        items_fetched: batchCount,
      });
      recordLastPoll(healthContext, adapter.source);

      // Periodically cleanup old seen-event entries to prevent unbounded growth.
      const now = Date.now();
      if (now - ctx.lastSeenCleanupAt > 60 * 60 * 1000) {
        checkpointStore.cleanupSeen("-7 days");
        ctx.lastSeenCleanupAt = now;
      }

      // Publish heartbeat
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

      // Wait for next poll interval
      if (ctx.shutdownRequested) break;
      await sleep(adapter.pollIntervalMs);

    } catch (error) {
      adapterLogger.error({ error }, "Adapter error");

      const lastSuccessfulPollAt = healthContext.sourceHealth.get(adapter.name)?.last_poll_at;

      // Update health status
      healthContext.sourceHealth.set(adapter.name, {
        status: "error",
        error_message: error instanceof Error ? error.message : String(error),
      });

      // Publish error heartbeat
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

      // Track error metrics
      if (ctx.shutdownRequested) break;
      if (isRateLimitError(error)) {
        incrementError(healthContext, adapter.source, "rate_limit");
        await backoff.waitRateLimit();
      } else if (isTransientError(error)) {
        incrementError(healthContext, adapter.source, "transient");
        await backoff.waitTransient();
      } else {
        incrementError(healthContext, adapter.source, "unknown");
        await backoff.waitTransient();
      }
    }
  }
}

async function gracefulShutdown(ctx: CollectorContext): Promise<void> {
  const { logger, kafkaContext, healthServer, checkpointStore, adapters } = ctx;

  logger.info("Initiating graceful shutdown");
  ctx.shutdownRequested = true;

  // Shutdown adapters
  for (const adapter of adapters) {
    try {
      await adapter.shutdown();
      logger.info({ adapter: adapter.name }, "Adapter shut down");
    } catch (error) {
      logger.error({ adapter: adapter.name, error }, "Adapter shutdown error");
    }
  }

  // Flush checkpoints
  checkpointStore.close();
  logger.info("Checkpoints flushed");

  // Disconnect Kafka
  await disconnectProducer(kafkaContext.producer, logger);

  // Close health server
  await new Promise<void>((resolve) => {
    healthServer.close(() => resolve());
  });
  logger.info("Health server closed");

  logger.info("Graceful shutdown complete");
}

async function main(): Promise<void> {
  let ctx: CollectorContext | null = null;

  try {
    ctx = await initializeCollector();

    // Setup signal handlers
    const shutdown = async () => {
      if (ctx) {
        await gracefulShutdown(ctx);
      }
      process.exit(0);
    };

    process.on("SIGTERM", shutdown);
    process.on("SIGINT", shutdown);

    // Run all adapters concurrently
    ctx.logger.info(
      { adapterCount: ctx.adapters.length },
      "Starting adapter loops"
    );

    await Promise.all(
      ctx.adapters.map((adapter) => runAdapter(ctx!, adapter))
    );

  } catch (error) {
    const logger = ctx?.logger ?? getLogger();
    logger.fatal({ error }, "Fatal error in collector");
    process.exit(1);
  }
}

main();
