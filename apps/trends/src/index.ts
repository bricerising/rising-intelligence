import type { Server } from "node:http";
import type { EachBatchPayload } from "kafkajs";
import { PrismaClient } from "@rising-intelligence/db";
import {
  closeServer,
  serializeError,
  runService,
  runShutdownSteps,
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

type BatchTopicHandler = (payload: EachBatchPayload) => Promise<void>;

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

function createBatchTopicHandlers(ctx: RuntimeContext): Map<string, BatchTopicHandler> {
  return new Map<string, BatchTopicHandler>([
    [ctx.config.KAFKA_TOPIC_RAW_EVENTS, async (payload) => processBatch(ctx, payload)],
    [
      ctx.config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT,
      async (payload) => processCollectorHeartbeatBatch(ctx, payload),
    ],
  ]);
}

async function runConsumer(ctx: RuntimeContext): Promise<void> {
  const batchTopicHandlers = createBatchTopicHandlers(ctx);

  await ctx.kafkaConsumerContext.consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async (payload: EachBatchPayload) => {
      const topicHandler = batchTopicHandlers.get(payload.batch.topic);
      if (topicHandler) {
        await topicHandler(payload);
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
