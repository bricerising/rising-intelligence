import type { Server } from "node:http";
import { PrismaClient } from "@rising-intelligence/db";
import { getConfig } from "./config.js";
import { loadAllowlist, type CompiledAllowlist } from "./allowlist.js";
import { getLogger, createChildLogger } from "./logger.js";
import {
  createHealthContext,
  incrementError,
  startHealthServer,
  type HealthContext,
} from "./health.js";
import {
  createKafkaConsumer,
  disconnectKafkaConsumer,
  type KafkaConsumerContext,
} from "./kafka/consumer.js";
import {
  createKafkaProducer,
  disconnectKafkaProducer,
  type KafkaProducerContext,
} from "./kafka/producer.js";
import { createRedisClient, disconnectRedis } from "./redis.js";
import { processBatch, type TrendsContext } from "./process.js";
import { publishSnapshots } from "./snapshot.js";

interface RuntimeContext extends TrendsContext {
  healthServer: Server;
  kafkaConsumerContext: KafkaConsumerContext;
  kafkaProducerContext: KafkaProducerContext;
  snapshotTimer: NodeJS.Timeout | null;
  snapshotInFlight: boolean;
}

function serializeError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

async function closeServer(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

async function initializeAllowlist(
  allowlistPath: string,
  logger: ReturnType<typeof getLogger>,
  healthContext: HealthContext
): Promise<CompiledAllowlist> {
  const allowlist = loadAllowlist(allowlistPath);
  healthContext.allowlistHealthy = true;
  logger.info({ topicCount: allowlist.topics.length }, "Topics allowlist loaded");
  return allowlist;
}

async function initialize(): Promise<RuntimeContext> {
  const config = getConfig();
  const logger = getLogger();

  logger.info({ service: config.SERVICE_NAME }, "Starting trends service");

  const healthContext = createHealthContext();
  const healthServer = startHealthServer(healthContext, logger);

  const prisma = new PrismaClient({
    datasources: { db: { url: config.DATABASE_URL } },
    log: process.env.NODE_ENV === "development" ? ["query", "warn", "error"] : ["error"],
  });

  await prisma.$connect();
  healthContext.postgresHealthy = true;
  logger.info("Postgres connected");

  const redis = await createRedisClient(
    config.REDIS_URL,
    createChildLogger({ component: "redis" })
  );
  healthContext.redisHealthy = true;

  const allowlist = await initializeAllowlist(config.TOPICS_ALLOWLIST_PATH, logger, healthContext);

  const kafkaConsumerContext = await createKafkaConsumer(
    createChildLogger({ component: "kafka-consumer" })
  );
  await kafkaConsumerContext.consumer.subscribe({
    topic: config.KAFKA_TOPIC_RAW_EVENTS,
    fromBeginning: false,
  });

  const kafkaProducerContext = await createKafkaProducer(
    createChildLogger({ component: "kafka-producer" })
  );
  healthContext.kafkaHealthy = true;

  logger.info(
    {
      consumeTopic: config.KAFKA_TOPIC_RAW_EVENTS,
      publishTopic: config.KAFKA_TOPIC_TRENDS_SNAPSHOTS,
      windows: config.WINDOWS,
    },
    "Kafka subscriptions initialized"
  );

  return {
    config,
    logger,
    healthContext,
    prisma,
    redis,
    allowlist,
    lagWriteTimestamps: new Map(),
    healthServer,
    kafkaConsumerContext,
    kafkaProducerContext,
    snapshotTimer: null,
    snapshotInFlight: false,
  };
}

async function runSnapshotLoop(ctx: RuntimeContext): Promise<void> {
  const run = async () => {
    if (ctx.snapshotInFlight) {
      return;
    }

    ctx.snapshotInFlight = true;
    try {
      await publishSnapshots({
        config: ctx.config,
        logger: createChildLogger({ component: "snapshot" }),
        redis: ctx.redis,
        producer: ctx.kafkaProducerContext.producer,
        prisma: ctx.prisma,
        allowlist: ctx.allowlist,
        healthContext: ctx.healthContext,
      });
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

async function runConsumer(ctx: RuntimeContext): Promise<void> {
  await ctx.kafkaConsumerContext.consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async (payload) => {
      await processBatch(ctx, payload);
    },
  });
}

async function shutdown(ctx: RuntimeContext | null, exitCode: number): Promise<never> {
  if (!ctx) {
    process.exit(exitCode);
  }

  const logger = ctx.logger;
  logger.info("Shutting down trends service");

  if (ctx.snapshotTimer) {
    clearInterval(ctx.snapshotTimer);
    ctx.snapshotTimer = null;
  }

  const timeout = setTimeout(() => {
    logger.error({ timeoutMs: ctx.config.SHUTDOWN_TIMEOUT_MS }, "Shutdown timeout reached");
    process.exit(1);
  }, ctx.config.SHUTDOWN_TIMEOUT_MS);

  try {
    await disconnectKafkaConsumer(ctx.kafkaConsumerContext.consumer, logger);
    ctx.healthContext.kafkaHealthy = false;
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Kafka consumer disconnect failed");
  }

  try {
    await disconnectKafkaProducer(ctx.kafkaProducerContext.producer, logger);
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Kafka producer disconnect failed");
  }

  try {
    await disconnectRedis(ctx.redis, logger);
    ctx.healthContext.redisHealthy = false;
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Redis disconnect failed");
  }

  try {
    await ctx.prisma.$disconnect();
    ctx.healthContext.postgresHealthy = false;
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Postgres disconnect failed");
  }

  try {
    await closeServer(ctx.healthServer);
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Health server close failed");
  }

  clearTimeout(timeout);
  process.exit(exitCode);
}

let shuttingDown = false;

async function start(): Promise<void> {
  let context: RuntimeContext | null = null;

  const requestShutdown = async (exitCode: number) => {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    await shutdown(context, exitCode);
  };

  process.on("SIGTERM", () => {
    void requestShutdown(0);
  });

  process.on("SIGINT", () => {
    void requestShutdown(0);
  });

  process.on("uncaughtException", (error) => {
    getLogger().error({ error: serializeError(error) }, "Uncaught exception");
    void requestShutdown(1);
  });

  process.on("unhandledRejection", (reason) => {
    getLogger().error({ error: serializeError(reason) }, "Unhandled rejection");
    void requestShutdown(1);
  });

  try {
    context = await initialize();
    await runSnapshotLoop(context);
    await runConsumer(context);
  } catch (error) {
    getLogger().error({ error: serializeError(error) }, "Trends service crashed");
    await requestShutdown(1);
  }
}

void start();

export {};
