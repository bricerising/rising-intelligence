import { PrismaClient } from "@rising-intelligence/db";
import {
  closeServer,
  serializeError,
  runService,
  createServiceLogger,
} from "@rising-intelligence/shared";
import type pino from "pino";
import { getConfig } from "./config.js";
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

async function initialize(): Promise<PersisterContext> {
  const config = getConfig();
  const logger = createServiceLogger(config.SERVICE_NAME, config.LOG_LEVEL);

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

  const redis = await createRedisClient(config, logger.child({ component: "redis" }));
  healthContext.redisHealthy = true;

  const kafkaContext = await createKafkaConsumer(logger.child({ component: "kafka" }));
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

async function shutdown(ctx: PersisterContext): Promise<void> {
  const logger = ctx.logger;

  try {
    await disconnectKafkaConsumer(ctx.kafkaContext.consumer, logger);
    ctx.healthContext.kafkaHealthy = false;
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Kafka disconnect failed during shutdown");
  }

  try {
    await disconnectRedis(ctx.redis);
    ctx.healthContext.redisHealthy = false;
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Redis disconnect failed during shutdown");
  }

  try {
    await ctx.prisma.$disconnect();
    ctx.healthContext.postgresHealthy = false;
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Postgres disconnect failed during shutdown");
  }

  try {
    await closeServer(ctx.healthServer);
  } catch (error) {
    logger.warn({ error: serializeError(error) }, "Health server close failed during shutdown");
  }
}

let _logger: pino.Logger | null = null;

runService<PersisterContext>({
  name: "persister",
  shutdownTimeoutMs: getConfig().SHUTDOWN_TIMEOUT_MS,
  getLogger() {
    if (!_logger) {
      _logger = createServiceLogger("persister", "info");
    }
    return _logger;
  },
  async initialize() {
    const ctx = await initialize();
    _logger = ctx.logger;
    return ctx;
  },
  async run(ctx) {
    await runConsumer(ctx);
  },
  async shutdown(ctx) {
    await shutdown(ctx);
  },
});
