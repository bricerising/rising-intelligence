import {
  createServiceBootstrap,
  runService,
  runShutdownSteps,
} from "@rising-intelligence/shared/lifecycle";
import { serializeError } from "@rising-intelligence/shared/errors";
import { closeServer } from "@rising-intelligence/shared/http";
import { BackoffManager, sleep } from "@rising-intelligence/shared/resilience";
import { getConfig } from "./config.js";
import { createCollectorIngestion } from "./collector-ingestion.js";
import {
  observePollDuration,
  observePollItemsCount,
  incrementCheckpointUpdated,
} from "./health.js";
import { createAdapterErrorPolicy } from "./adapter-error-policy.js";
import type { SourceAdapter, CollectorHeartbeat } from "./types.js";
import {
  createCollectorHeartbeatPublisher,
  createCollectorIngestionPublisher,
  createCollectorPublisher,
} from "./publishing-facade.js";
import {
  createCollectorRuntimeFactory,
  type CollectorRuntimeContext,
} from "./runtime-factory.js";

type CollectorContext = CollectorRuntimeContext;

const bootstrap = createServiceBootstrap(getConfig);
const runtimeFactory = createCollectorRuntimeFactory();
const SHUTDOWN_SLEEP_CHUNK_MS = 1_000;

async function sleepUntilNextPoll(
  ctx: CollectorContext,
  pollIntervalMs: number
): Promise<void> {
  let remainingMs = pollIntervalMs;

  while (remainingMs > 0 && !ctx.shutdownRequested) {
    const waitMs = Math.min(remainingMs, SHUTDOWN_SLEEP_CHUNK_MS);
    await sleep(waitMs);
    remainingMs -= waitMs;
  }
}

async function initializeCollector(): Promise<CollectorContext> {
  const config = bootstrap.getConfig();
  const logger = bootstrap.getLogger();
  logger.info({ service: config.SERVICE_NAME }, "Starting collector service");
  return runtimeFactory.createRuntime(config, logger);
}

async function runAdapter(
  ctx: CollectorContext,
  adapter: SourceAdapter
): Promise<void> {
  const { kafkaContext, healthContext, checkpointStore, allowlist } = ctx;
  const adapterLogger = ctx.logger.child({ adapter: adapter.name });
  const publisher = createCollectorPublisher({
    connection: kafkaContext.producer,
    logger: adapterLogger,
  });
  const ingestionPublisher = createCollectorIngestionPublisher(publisher);
  const heartbeatPublisher = createCollectorHeartbeatPublisher(publisher);
  const backoff = new BackoffManager(adapter.name, adapterLogger);
  const errorPolicy = createAdapterErrorPolicy();
  const ingestion = createCollectorIngestion({
    adapterName: adapter.name,
    adapterSource: adapter.source,
    allowlist,
    checkpointStore,
    healthContext,
    logger: adapterLogger,
    publisher: ingestionPublisher,
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

        const processingResult = await ingestion.ingest(event);
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
      await heartbeatPublisher.publishSourceHeartbeat(heartbeat);

      adapterLogger.info({ batchCount }, "Poll cycle complete");
      backoff.reset();

      if (ctx.shutdownRequested) break;
      await sleepUntilNextPoll(ctx, adapter.pollIntervalMs);

    } catch (error) {
      adapterLogger.error({ error: serializeError(error) }, "Adapter error");

      const lastSuccessfulPollAt = healthContext.sourceHealth.get(adapter.name)?.last_poll_at;

      healthContext.sourceHealth.set(adapter.name, {
        status: "error",
        last_poll_at: lastSuccessfulPollAt,
        items_fetched: 0,
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
        await heartbeatPublisher.publishSourceHeartbeat(heartbeat);
      } catch {
        // Ignore heartbeat publish errors
      }

      if (ctx.shutdownRequested) break;
      await errorPolicy.handle(error, {
        healthContext,
        adapterSource: adapter.source,
        backoff,
      });
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
      run: async () => kafkaContext.producer.disconnect(),
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

runService<CollectorContext>({
  name: bootstrap.getServiceName(),
  shutdownTimeoutMs: bootstrap.getShutdownTimeoutMs(),
  getLogger() {
    return bootstrap.getLogger();
  },
  async initialize() {
    const ctx = await initializeCollector();
    bootstrap.setRuntimeLogger(ctx.logger);
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
