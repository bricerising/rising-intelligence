import type { Server } from "node:http";
import type { Redis } from "ioredis";
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
  createHealthContext,
  incrementGeneration,
  observeGenerationDuration,
  incrementError,
  setBudgetRemainingUsd,
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
import { deserializeSummaryRequest } from "./deserialize.js";
import { createRedisClient, disconnectRedis } from "./redis.js";
import { processSummaryRequest } from "./process.js";

interface RuntimeContext {
  config: ReturnType<typeof getConfig>;
  logger: pino.Logger;
  healthContext: HealthContext;
  healthServer: Server;
  kafkaConsumerContext: KafkaConsumerContext;
  kafkaProducerContext: KafkaProducerContext;
  prisma: PrismaClient;
  redis: Redis;
}

async function initialize(): Promise<RuntimeContext> {
  const config = getConfig();
  const logger = createServiceLogger(config.SERVICE_NAME, config.LOG_LEVEL);
  logger.info({ service: config.SERVICE_NAME }, "Starting brief service");

  const healthContext = createHealthContext(config.LLM_DAILY_BUDGET_USD);
  const healthServer = startHealthServer(healthContext, logger);
  setBudgetRemainingUsd(healthContext, config.LLM_DAILY_BUDGET_USD);

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

  const kafkaConsumerContext = await createKafkaConsumer(logger);
  await kafkaConsumerContext.consumer.subscribe({
    topic: config.KAFKA_TOPIC_SUMMARY_REQUESTS,
    fromBeginning: false,
  });
  const kafkaProducerContext = await createKafkaProducer(
    logger.child({ component: "kafka-producer" })
  );
  healthContext.kafkaHealthy = true;

  logger.info(
    {
      consumeTopic: config.KAFKA_TOPIC_SUMMARY_REQUESTS,
      publishTopic: config.KAFKA_TOPIC_SUMMARY_RESULTS,
    },
    "Kafka subscriptions initialized"
  );

  return {
    config,
    logger,
    healthContext,
    healthServer,
    kafkaConsumerContext,
    kafkaProducerContext,
    prisma,
    redis,
  };
}

async function runConsumer(ctx: RuntimeContext): Promise<void> {
  await ctx.kafkaConsumerContext.consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const startTime = Date.now();
      if (!message.value) {
        incrementError(ctx.healthContext, "parse_error");
        incrementGeneration(ctx.healthContext, "failure");
        observeGenerationDuration(ctx.healthContext, (Date.now() - startTime) / 1000);
        ctx.logger.warn(
          {
            kafkaTopic: topic,
            partition,
            offset: message.offset,
          },
          "Skipping message with empty value"
        );
        return;
      }

      try {
        const request = deserializeSummaryRequest(message.value);
        await processSummaryRequest(
          {
            config: ctx.config,
            logger: ctx.logger.child({
              kafkaTopic: topic,
              partition,
              offset: message.offset,
            }),
            healthContext: ctx.healthContext,
            prisma: ctx.prisma,
            redis: ctx.redis,
            producer: ctx.kafkaProducerContext.producer,
          },
          request
        );
      } catch (error) {
        incrementError(ctx.healthContext, "parse_error");
        incrementGeneration(ctx.healthContext, "failure");
        ctx.logger.warn(
          {
            kafkaTopic: topic,
            partition,
            offset: message.offset,
            error: serializeError(error),
          },
          "Failed to process summary request"
        );
      } finally {
        observeGenerationDuration(ctx.healthContext, (Date.now() - startTime) / 1000);
      }
    },
  });
}

async function gracefulShutdown(ctx: RuntimeContext): Promise<void> {
  try {
    await disconnectKafkaConsumer(ctx.kafkaConsumerContext.consumer, ctx.logger);
    ctx.healthContext.kafkaHealthy = false;
  } catch (error) {
    ctx.logger.warn({ error: serializeError(error) }, "Kafka consumer disconnect failed");
  }

  try {
    await disconnectKafkaProducer(ctx.kafkaProducerContext.producer, ctx.logger);
  } catch (error) {
    ctx.logger.warn({ error: serializeError(error) }, "Kafka producer disconnect failed");
  }

  try {
    await disconnectRedis(ctx.redis, ctx.logger);
    ctx.healthContext.redisHealthy = false;
  } catch (error) {
    ctx.logger.warn({ error: serializeError(error) }, "Redis disconnect failed");
  }

  try {
    await ctx.prisma.$disconnect();
    ctx.healthContext.postgresHealthy = false;
  } catch (error) {
    ctx.logger.warn({ error: serializeError(error) }, "Postgres disconnect failed");
  }

  try {
    await closeServer(ctx.healthServer);
  } catch (error) {
    ctx.logger.warn({ error: serializeError(error) }, "Health server close failed");
  }
}

let _logger: pino.Logger | null = null;

runService<RuntimeContext>({
  name: "brief",
  shutdownTimeoutMs: getConfig().SHUTDOWN_TIMEOUT_MS,
  getLogger() {
    if (!_logger) {
      _logger = createServiceLogger("brief", "info");
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
    await gracefulShutdown(ctx);
  },
});
