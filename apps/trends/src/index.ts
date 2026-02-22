import type { EachBatchPayload } from "kafkajs";
import {
  createTopicBatchRouter,
  type BatchTopicHandler,
  closeServer,
  serializeError,
  runService,
  runShutdownSteps,
  createServiceBootstrap,
} from "@rising-intelligence/shared";
import { getConfig } from "./config.js";
import { incrementError } from "./health.js";
import { disconnectKafkaConsumer } from "./kafka/consumer.js";
import { disconnectKafkaProducer } from "./kafka/producer.js";
import { disconnectRedis } from "./redis.js";
import { processBatch, processCollectorHeartbeatBatch } from "./process.js";
import {
  createTrendsRuntimeFactory,
  type TrendsRuntimeContext,
} from "./runtime-factory.js";
import { publishSnapshots } from "./snapshot.js";

const bootstrap = createServiceBootstrap(getConfig);
const runtimeFactory = createTrendsRuntimeFactory();

async function initializeTrends(): Promise<TrendsRuntimeContext> {
  const config = bootstrap.getConfig();
  const logger = bootstrap.getLogger();
  logger.info({ service: config.SERVICE_NAME }, "Starting trends service");
  return runtimeFactory.createRuntime(config, logger);
}

async function runSnapshotLoop(ctx: TrendsRuntimeContext): Promise<void> {
  const run = async () => {
    if (ctx.snapshotInFlight) {
      return;
    }

    ctx.snapshotInFlight = true;
    try {
      const snapshots = await publishSnapshots({
        config: ctx.config,
        logger: ctx.logger.child({ component: "snapshot" }),
        redis: ctx.redis,
        producer: ctx.kafkaProducerContext.producer,
        prisma: ctx.prisma,
        allowlist: ctx.allowlist,
        healthContext: ctx.healthContext,
      });
      if (snapshots.length === 0) {
        ctx.logger.debug("No snapshots published in this interval");
      }
    } catch (error) {
      incrementError(ctx.healthContext, "snapshot_error");
      ctx.logger.error({ error: serializeError(error) }, "Snapshot publish failed");
    } finally {
      ctx.snapshotInFlight = false;
    }
  };

  await run();
  ctx.snapshotTimer = setInterval(() => {
    void run();
  }, ctx.config.SNAPSHOT_INTERVAL_SECONDS * 1000);
  ctx.snapshotTimer.unref();
}

function createBatchTopicHandlers(ctx: TrendsRuntimeContext): Map<string, BatchTopicHandler> {
  return new Map<string, BatchTopicHandler>([
    [ctx.config.KAFKA_TOPIC_RAW_EVENTS, async (payload) => processBatch(ctx, payload)],
    [
      ctx.config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT,
      async (payload) => processCollectorHeartbeatBatch(ctx, payload),
    ],
  ]);
}

async function runConsumer(ctx: TrendsRuntimeContext): Promise<void> {
  const topicBatchRouter = createTopicBatchRouter({
    logger: ctx.logger,
    handlers: createBatchTopicHandlers(ctx),
  });

  await ctx.kafkaConsumerContext.consumer.run({
    // Offsets are resolved manually; auto-commit must stay enabled so
    // commitOffsetsIfNecessary() records progress for restart recovery.
    autoCommit: true,
    eachBatchAutoResolve: false,
    eachBatch: async (payload: EachBatchPayload) => {
      await topicBatchRouter.handle(payload);
    },
  });
}

async function gracefulShutdown(ctx: TrendsRuntimeContext): Promise<void> {
  const logger = ctx.logger;

  if (ctx.snapshotTimer) {
    clearInterval(ctx.snapshotTimer);
    ctx.snapshotTimer = null;
  }

  await runShutdownSteps(logger, [
    {
      name: "kafka-consumer",
      run: async () => disconnectKafkaConsumer(ctx.kafkaConsumerContext.consumer, logger),
      errorMessage: "Kafka consumer disconnect failed",
      onSuccess: () => {
        ctx.healthContext.kafkaHealthy = false;
      },
    },
    {
      name: "kafka-producer",
      run: async () => disconnectKafkaProducer(ctx.kafkaProducerContext.producer, logger),
      errorMessage: "Kafka producer disconnect failed",
    },
    {
      name: "redis",
      run: async () => disconnectRedis(ctx.redis, logger),
      errorMessage: "Redis disconnect failed",
      onSuccess: () => {
        ctx.healthContext.redisHealthy = false;
      },
    },
    {
      name: "postgres",
      run: async () => ctx.prisma.$disconnect(),
      errorMessage: "Postgres disconnect failed",
      onSuccess: () => {
        ctx.healthContext.postgresHealthy = false;
      },
    },
    {
      name: "health-server",
      run: async () => closeServer(ctx.healthServer),
      errorMessage: "Health server close failed",
    },
  ]);
}

runService<TrendsRuntimeContext>({
  name: bootstrap.getServiceName(),
  shutdownTimeoutMs: bootstrap.getShutdownTimeoutMs(),
  getLogger() {
    return bootstrap.getLogger();
  },
  async initialize() {
    const ctx = await initializeTrends();
    bootstrap.setRuntimeLogger(ctx.logger);
    return ctx;
  },
  async run(ctx) {
    await runSnapshotLoop(ctx);
    await runConsumer(ctx);
  },
  async shutdown(ctx) {
    await gracefulShutdown(ctx);
  },
});
