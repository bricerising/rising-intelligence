import { PrismaClient } from "@rising-intelligence/db";
import { getConfig } from "./config.js";
import { getLogger, createChildLogger } from "./logger.js";
import {
  createKafkaConsumer,
  disconnectKafkaConsumer,
} from "./kafka/consumer.js";
import {
  createHealthContext,
  startHealthServer,
} from "./health.js";
import { createRedisClient, disconnectRedis } from "./redis.js";
import { PostgresCircuitBreaker } from "./circuit-breaker.js";
import { processBatch, type PersisterContext } from "./process.js";
import { serializeError } from "./utils.js";

async function initialize(): Promise<PersisterContext> {
  const config = getConfig();
  const logger = getLogger();

  logger.info({ service: config.SERVICE_NAME }, "Starting persister service");

  const healthContext = createHealthContext();
  const healthServer = startHealthServer(healthContext, logger);

  const prisma = new PrismaClient({
    datasources: { db: { url: config.DATABASE_URL } },
    log: process.env.NODE_ENV === "development" ? ["query", "warn", "error"] : ["error"],
  });

  await prisma.$connect();
  healthContext.postgresHealthy = true;
  logger.info("Postgres connected");

  const redis = await createRedisClient(config, createChildLogger({ component: "redis" }));
  healthContext.redisHealthy = redis !== null;

  const kafkaContext = await createKafkaConsumer(createChildLogger({ component: "kafka" }));
  await kafkaContext.consumer.subscribe({
    topic: config.KAFKA_TOPIC_RAW_EVENTS,
    fromBeginning: false,
  });
  healthContext.kafkaHealthy = true;
  logger.info({ topic: config.KAFKA_TOPIC_RAW_EVENTS }, "Kafka consumer subscribed");

  return {
    config,
    logger,
    healthContext,
    healthServer,
    prisma,
    redis,
    kafkaContext,
    circuitBreaker: new PostgresCircuitBreaker(
      config.POSTGRES_CIRCUIT_FAILURE_THRESHOLD,
      config.POSTGRES_CIRCUIT_OPEN_MS
    ),
    lagWriteTimestamps: new Map(),
  };
}

async function runConsumer(ctx: PersisterContext): Promise<void> {
  const consumer = ctx.kafkaContext.consumer;

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async (payload) => {
      await processBatch(ctx, payload);
    },
  });
}

async function closeServer(server: import("node:http").Server): Promise<void> {
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

async function shutdown(ctx: PersisterContext | null, exitCode: number): Promise<never> {
  if (!ctx) {
    process.exit(exitCode);
  }

  const logger = ctx.logger;
  logger.info("Shutting down persister service");

  const timeout = setTimeout(() => {
    logger.error({ timeoutMs: ctx.config.SHUTDOWN_TIMEOUT_MS }, "Shutdown timeout reached");
    process.exit(1);
  }, ctx.config.SHUTDOWN_TIMEOUT_MS);

  try {
    await disconnectKafkaConsumer(ctx.kafkaContext.consumer, logger);
    ctx.healthContext.kafkaHealthy = false;
  } catch (error) {
    logger.warn({ error: serializeError(error).message }, "Kafka disconnect failed during shutdown");
  }

  try {
    await disconnectRedis(ctx.redis);
    ctx.healthContext.redisHealthy = false;
  } catch (error) {
    logger.warn({ error: serializeError(error).message }, "Redis disconnect failed during shutdown");
  }

  try {
    await ctx.prisma.$disconnect();
    ctx.healthContext.postgresHealthy = false;
  } catch (error) {
    logger.warn({ error: serializeError(error).message }, "Postgres disconnect failed during shutdown");
  }

  try {
    await closeServer(ctx.healthServer);
  } catch (error) {
    logger.warn({ error: serializeError(error).message }, "Health server close failed during shutdown");
  }

  clearTimeout(timeout);
  process.exit(exitCode);
}

let shuttingDown = false;

async function start(): Promise<void> {
  let context: PersisterContext | null = null;

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
    getLogger().error({ error: serializeError(error).message }, "Uncaught exception");
    void requestShutdown(1);
  });

  process.on("unhandledRejection", (reason) => {
    getLogger().error({ error: serializeError(reason).message }, "Unhandled rejection");
    void requestShutdown(1);
  });

  try {
    context = await initialize();
    await runConsumer(context);
  } catch (error) {
    getLogger().error({ error: serializeError(error).message }, "Persister crashed");
    await requestShutdown(1);
  }
}

void start();

export {};
