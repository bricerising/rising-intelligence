import type { Server } from "node:http";
import { PrismaClient } from "@rising-intelligence/db";
import {
  closeServer,
  serializeError,
  runService,
  createServiceLogger,
} from "@rising-intelligence/shared";
import type pino from "pino";
import { getConfig } from "./config.js";
import { loadAllowlist, type CompiledAllowlist } from "./allowlist.js";
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
import {
  processBatch,
  processCollectorHeartbeatBatch,
  type TrendsContext,
} from "./process.js";
import { publishSnapshots } from "./snapshot.js";

interface RuntimeContext extends TrendsContext {
  healthServer: Server;
  kafkaConsumerContext: KafkaConsumerContext;
  kafkaProducerContext: KafkaProducerContext;
  snapshotTimer: NodeJS.Timeout | null;
  snapshotInFlight: boolean;
}

async function initializeAllowlist(
  allowlistPath: string,
  logger: pino.Logger,
  healthContext: HealthContext
): Promise<CompiledAllowlist> {
  const allowlist = loadAllowlist(allowlistPath);
  healthContext.allowlistHealthy = true;
  logger.info({ topicCount: allowlist.topics.length }, "Topics allowlist loaded");
  return allowlist;
}

async function initializeTrends(): Promise<RuntimeContext> {
  const config = getConfig();
  const logger = createServiceLogger(config.SERVICE_NAME, config.LOG_LEVEL);

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
    logger.child({ component: "redis" })
  );
  healthContext.redisHealthy = true;

  const allowlist = await initializeAllowlist(config.TOPICS_ALLOWLIST_PATH, logger, healthContext);

  const kafkaConsumerContext = await createKafkaConsumer(
    logger.child({ component: "kafka-consumer" })
  );
  await kafkaConsumerContext.consumer.subscribe({
    topic: config.KAFKA_TOPIC_RAW_EVENTS,
    fromBeginning: false,
  });
  await kafkaConsumerContext.consumer.subscribe({
    topic: config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT,
    fromBeginning: false,
  });

  const kafkaProducerContext = await createKafkaProducer(
    logger.child({ component: "kafka-producer" })
  );
  healthContext.kafkaHealthy = true;

  logger.info(
    {
      consumeTopic: config.KAFKA_TOPIC_RAW_EVENTS,
      heartbeatTopic: config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT,
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

async function runConsumer(ctx: RuntimeContext): Promise<void> {
  await ctx.kafkaConsumerContext.consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async (payload) => {
      if (payload.batch.topic === ctx.config.KAFKA_TOPIC_RAW_EVENTS) {
        await processBatch(ctx, payload);
        return;
      }
      if (payload.batch.topic === ctx.config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT) {
        await processCollectorHeartbeatBatch(ctx, payload);
        return;
      }

      ctx.logger.warn(
        { topic: payload.batch.topic, partition: payload.batch.partition },
        "Received batch for unexpected topic; skipping"
      );
    },
  });
}

async function gracefulShutdown(ctx: RuntimeContext): Promise<void> {
  const logger = ctx.logger;

  if (ctx.snapshotTimer) {
    clearInterval(ctx.snapshotTimer);
    ctx.snapshotTimer = null;
  }

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
}

let _logger: pino.Logger | null = null;

runService<RuntimeContext>({
  name: "trends",
  shutdownTimeoutMs: getConfig().SHUTDOWN_TIMEOUT_MS,
  getLogger() {
    if (!_logger) {
      _logger = createServiceLogger("trends", "info");
    }
    return _logger;
  },
  async initialize() {
    const ctx = await initializeTrends();
    _logger = ctx.logger;
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
