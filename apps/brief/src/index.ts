import type { Server } from "node:http";
import type { Redis } from "ioredis";
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
import { deserializeSummaryRequest } from "./deserialize.js";
import { createRedisClient, disconnectRedis } from "./redis.js";

interface RuntimeContext {
  config: ReturnType<typeof getConfig>;
  logger: pino.Logger;
  healthContext: HealthContext;
  healthServer: Server;
  kafkaContext: KafkaConsumerContext;
  redis: Redis;
}

async function initialize(): Promise<RuntimeContext> {
  const config = getConfig();
  const logger = createServiceLogger(config.SERVICE_NAME, config.LOG_LEVEL);
  logger.info({ service: config.SERVICE_NAME }, "Starting brief service");

  const healthContext = createHealthContext(config.LLM_DAILY_BUDGET_USD);
  const healthServer = startHealthServer(healthContext, logger);
  setBudgetRemainingUsd(healthContext, config.LLM_DAILY_BUDGET_USD);

  const redis = await createRedisClient(
    config.REDIS_URL,
    logger.child({ component: "redis" })
  );
  healthContext.redisHealthy = true;

  const kafkaContext = await createKafkaConsumer(logger);
  await kafkaContext.consumer.subscribe({
    topic: config.KAFKA_TOPIC_SUMMARY_REQUESTS,
    fromBeginning: false,
  });
  healthContext.kafkaHealthy = true;

  logger.info(
    { topic: config.KAFKA_TOPIC_SUMMARY_REQUESTS },
    "Kafka consumer subscribed"
  );

  return {
    config,
    logger,
    healthContext,
    healthServer,
    kafkaContext,
    redis,
  };
}

async function runConsumer(ctx: RuntimeContext): Promise<void> {
  await ctx.kafkaContext.consumer.run({
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
        incrementGeneration(ctx.healthContext, "skipped");
        observeGenerationDuration(ctx.healthContext, (Date.now() - startTime) / 1000);
        ctx.logger.info(
          {
            requestId: request.requestId,
            type: request.type,
            topicCount: request.topics.length,
            kafkaTopic: topic,
            partition,
            offset: message.offset,
          },
          "Summary request consumed"
        );
      } catch (error) {
        incrementError(ctx.healthContext, "parse_error");
        incrementGeneration(ctx.healthContext, "failure");
        observeGenerationDuration(ctx.healthContext, (Date.now() - startTime) / 1000);
        ctx.logger.warn(
          {
            kafkaTopic: topic,
            partition,
            offset: message.offset,
            error: serializeError(error),
          },
          "Failed to deserialize summary request"
        );
      }
    },
  });
}

async function gracefulShutdown(ctx: RuntimeContext): Promise<void> {
  try {
    await disconnectKafkaConsumer(ctx.kafkaContext.consumer, ctx.logger);
    ctx.healthContext.kafkaHealthy = false;
  } catch (error) {
    ctx.logger.warn({ error: serializeError(error) }, "Kafka consumer disconnect failed");
  }

  try {
    await disconnectRedis(ctx.redis, ctx.logger);
    ctx.healthContext.redisHealthy = false;
  } catch (error) {
    ctx.logger.warn({ error: serializeError(error) }, "Redis disconnect failed");
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
  shutdownTimeoutMs: 30000,
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
