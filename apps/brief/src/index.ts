import type { Server } from "node:http";
import type { Redis } from "ioredis";
import type { EachBatchPayload } from "kafkajs";
import { PrismaClient, Prisma, TrendWindow } from "@rising-intelligence/db";
import {
  closeServer,
  serializeError,
  runService,
  runShutdownSteps,
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
import { deserializeSummaryRequest, deserializeTrendSnapshot } from "./deserialize.js";
import { createRedisClient, disconnectRedis } from "./redis.js";
import { processSummaryRequest } from "./process.js";
import type { ParsedTrendSnapshot } from "./types.js";

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

const IN_FLIGHT_HEARTBEAT_INTERVAL_MS = 5_000;

interface TopicMessageHandlerInput {
  messageValue: Buffer;
  messageLogger: pino.Logger;
  heartbeat: () => Promise<void>;
}

type TopicMessageHandler = (input: TopicMessageHandlerInput) => Promise<void>;

function mapTrendWindowToEnum(window: number): TrendWindow {
  switch (window) {
    case 1:
      return TrendWindow.WINDOW_15M;
    case 2:
      return TrendWindow.WINDOW_60M;
    case 3:
      return TrendWindow.WINDOW_24H;
    default:
      throw new Error(`Unsupported trend window: ${window}`);
  }
}

async function persistTrendSnapshot(
  ctx: RuntimeContext,
  snapshot: ParsedTrendSnapshot
): Promise<void> {
  try {
    await ctx.prisma.briefTrendSnapshot.create({
      data: {
        generatedAt: snapshot.generatedAt,
        window: mapTrendWindowToEnum(snapshot.window),
        snapshot: snapshot.snapshot as Prisma.InputJsonValue,
      },
    });
    ctx.healthContext.postgresHealthy = true;
  } catch (error) {
    ctx.healthContext.postgresHealthy = false;
    throw error;
  }
}

async function runWithInFlightHeartbeats(
  heartbeat: () => Promise<void>,
  logger: pino.Logger,
  work: () => Promise<void>
): Promise<void> {
  const interval = setInterval(() => {
    void heartbeat().catch((error) => {
      logger.warn(
        { error: serializeError(error) },
        "Background Kafka heartbeat failed while processing summary request"
      );
    });
  }, IN_FLIGHT_HEARTBEAT_INTERVAL_MS);
  interval.unref();

  try {
    await work();
  } finally {
    clearInterval(interval);
  }
}

function createTopicMessageHandlers(ctx: RuntimeContext): Map<string, TopicMessageHandler> {
  const trendSnapshotHandler: TopicMessageHandler = async ({ messageValue, messageLogger }) => {
    try {
      const snapshot = deserializeTrendSnapshot(messageValue);
      await persistTrendSnapshot(ctx, snapshot);
    } catch (error) {
      messageLogger.warn(
        {
          error: serializeError(error),
        },
        "Failed to process trend snapshot"
      );
    }
  };

  const summaryRequestHandler: TopicMessageHandler = async ({
    messageValue,
    messageLogger,
    heartbeat,
  }) => {
    let request: ReturnType<typeof deserializeSummaryRequest>;
    try {
      request = deserializeSummaryRequest(messageValue);
    } catch (error) {
      incrementError(ctx.healthContext, "parse_error");
      incrementGeneration(ctx.healthContext, "failure");
      messageLogger.warn(
        {
          error: serializeError(error),
        },
        "Failed to deserialize summary request"
      );
      return;
    }

    const startTime = Date.now();
    try {
      await runWithInFlightHeartbeats(heartbeat, messageLogger, async () => {
        await processSummaryRequest(
          {
            config: ctx.config,
            logger: messageLogger,
            healthContext: ctx.healthContext,
            prisma: ctx.prisma,
            redis: ctx.redis,
            producer: ctx.kafkaProducerContext.producer,
          },
          request
        );
      });
    } catch (error) {
      messageLogger.warn(
        {
          error: serializeError(error),
        },
        "Failed to process summary request"
      );
      throw error;
    } finally {
      observeGenerationDuration(ctx.healthContext, (Date.now() - startTime) / 1000);
    }
  };

  return new Map<string, TopicMessageHandler>([
    [ctx.config.KAFKA_TOPIC_TREND_SNAPSHOTS, trendSnapshotHandler],
    [ctx.config.KAFKA_TOPIC_SUMMARY_REQUESTS, summaryRequestHandler],
  ]);
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
    topics: [config.KAFKA_TOPIC_SUMMARY_REQUESTS, config.KAFKA_TOPIC_TREND_SNAPSHOTS],
    fromBeginning: false,
  });
  const kafkaProducerContext = await createKafkaProducer(
    logger.child({ component: "kafka-producer" })
  );
  healthContext.kafkaHealthy = true;

  logger.info(
    {
      consumeTopics: [config.KAFKA_TOPIC_SUMMARY_REQUESTS, config.KAFKA_TOPIC_TREND_SNAPSHOTS],
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
  const topicHandlers = createTopicMessageHandlers(ctx);

  await ctx.kafkaConsumerContext.consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async (payload: EachBatchPayload) => {
      const { batch, isRunning, isStale, resolveOffset, commitOffsetsIfNecessary, heartbeat } = payload;
      if (!isRunning() || isStale()) {
        return;
      }

      let messagesSinceHeartbeat = 0;
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) {
          break;
        }

        if (!message.value) {
          ctx.logger.warn(
            {
              kafkaTopic: batch.topic,
              partition: batch.partition,
              offset: message.offset,
            },
            "Skipping message with empty value"
          );
          resolveOffset(message.offset);
          await commitOffsetsIfNecessary();
          continue;
        }

        const topicHandler = topicHandlers.get(batch.topic);
        const messageLogger = ctx.logger.child({
          kafkaTopic: batch.topic,
          partition: batch.partition,
          offset: message.offset,
        });

        if (!topicHandler) {
          messageLogger.warn(
            {
              topic: batch.topic,
            },
            "Unknown topic, skipping message"
          );
          resolveOffset(message.offset);
          await commitOffsetsIfNecessary();
          continue;
        }

        await topicHandler({
          messageValue: message.value,
          messageLogger,
          heartbeat,
        });
        resolveOffset(message.offset);
        await commitOffsetsIfNecessary();

        messagesSinceHeartbeat += 1;
        if (messagesSinceHeartbeat >= 20) {
          await heartbeat();
          messagesSinceHeartbeat = 0;
        }
      }

      await commitOffsetsIfNecessary();
      await heartbeat();
    },
  });
}

async function gracefulShutdown(ctx: RuntimeContext): Promise<void> {
  await runShutdownSteps(ctx.logger, [
    {
      name: "kafka-consumer",
      run: async () => disconnectKafkaConsumer(ctx.kafkaConsumerContext.consumer, ctx.logger),
      errorMessage: "Kafka consumer disconnect failed",
      onSuccess: () => {
        ctx.healthContext.kafkaHealthy = false;
      },
    },
    {
      name: "kafka-producer",
      run: async () => disconnectKafkaProducer(ctx.kafkaProducerContext.producer, ctx.logger),
      errorMessage: "Kafka producer disconnect failed",
    },
    {
      name: "redis",
      run: async () => disconnectRedis(ctx.redis, ctx.logger),
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
