import type { EachBatchPayload } from "kafkajs";
import { Prisma } from "@rising-intelligence/db";
import {
  createTopicBatchRouter,
  runKafkaMessageBatch,
  type BatchTopicHandler,
  closeServer,
  serializeError,
  runService,
  runShutdownSteps,
  createServiceBootstrap,
} from "@rising-intelligence/shared";
import { getConfig } from "./config.js";
import {
  incrementGeneration,
  observeGenerationDuration,
  incrementError,
} from "./health.js";
import { disconnectKafkaConsumer } from "./kafka/consumer.js";
import { disconnectKafkaProducer } from "./kafka/producer.js";
import { deserializeSummaryRequest, deserializeTrendSnapshot } from "./deserialize.js";
import { disconnectRedis } from "./redis.js";
import { processSummaryRequest } from "./process.js";
import {
  createBriefRuntimeFactory,
  type BriefRuntimeContext as RuntimeContext,
} from "./runtime-factory.js";
import type { ParsedTrendSnapshot } from "./types.js";
import {
  createTopicMessageHandlerMap,
  mapTrendWindowToEnum,
  runWithInFlightHeartbeats,
  type TopicMessageCommand,
  type TopicMessageHandler,
} from "./topic-message-handlers.js";

const bootstrap = createServiceBootstrap(getConfig);
const runtimeFactory = createBriefRuntimeFactory();

const IN_FLIGHT_HEARTBEAT_INTERVAL_MS = 5_000;
const LOOP_HEARTBEAT_INTERVAL_MESSAGES = 20;

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

function createTopicMessageHandlers(ctx: RuntimeContext): Map<string, TopicMessageHandler> {
  const commands: readonly TopicMessageCommand[] = [
    {
      name: "trend-snapshot",
      topic: ctx.config.KAFKA_TOPIC_TREND_SNAPSHOTS,
      async execute({ messageValue, messageLogger }): Promise<void> {
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
      },
    },
    {
      name: "summary-request",
      topic: ctx.config.KAFKA_TOPIC_SUMMARY_REQUESTS,
      async execute({ messageValue, messageLogger, heartbeat }): Promise<void> {
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
          await runWithInFlightHeartbeats(
            heartbeat,
            messageLogger,
            async () => {
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
            },
            IN_FLIGHT_HEARTBEAT_INTERVAL_MS
          );
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
      },
    },
  ];

  return createTopicMessageHandlerMap(commands);
}

function createTopicBatchHandler(
  ctx: RuntimeContext,
  topicHandler: TopicMessageHandler
): BatchTopicHandler {
  return async (payload: EachBatchPayload): Promise<void> => {
    const { heartbeat } = payload;

    await runKafkaMessageBatch({
      ctx,
      payload,
      heartbeatIntervalMessages: LOOP_HEARTBEAT_INTERVAL_MESSAGES,
      strategy: {
        deserialize: (value) => value,
        onEmptyValue(_ctx, messageContext): void {
          ctx.logger.warn(messageContext, "Skipping message with empty value");
        },
        async onMessage(_ctx, messageContext, messageValue): Promise<void> {
          await topicHandler({
            messageValue,
            messageLogger: ctx.logger.child(messageContext),
            heartbeat,
          });
        },
      },
      resolveOffsets: true,
    });
  };
}

function createBatchTopicHandlers(
  ctx: RuntimeContext
): Map<string, BatchTopicHandler> {
  const messageHandlers = createTopicMessageHandlers(ctx);
  return new Map<string, BatchTopicHandler>(
    [...messageHandlers.entries()].map(([topic, handler]) => [
      topic,
      createTopicBatchHandler(ctx, handler),
    ])
  );
}

async function createRuntime(): Promise<RuntimeContext> {
  const config = bootstrap.getConfig();
  const logger = bootstrap.getLogger();
  logger.info({ service: config.SERVICE_NAME }, "Starting brief service");
  return runtimeFactory.createRuntime(config, logger);
}

async function runConsumer(ctx: RuntimeContext): Promise<void> {
  const topicBatchRouter = createTopicBatchRouter({
    logger: ctx.logger,
    handlers: createBatchTopicHandlers(ctx),
  });

  await ctx.kafkaConsumerContext.consumer.run({
    // Offsets are resolved manually per message; keep auto-commit enabled so
    // commitOffsetsIfNecessary() persists progress and downtime messages replay.
    autoCommit: true,
    eachBatchAutoResolve: false,
    eachBatch: async (payload: EachBatchPayload) => {
      await topicBatchRouter.handle(payload);
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

runService<RuntimeContext>({
  name: bootstrap.getServiceName(),
  shutdownTimeoutMs: bootstrap.getShutdownTimeoutMs(),
  getLogger() {
    return bootstrap.getLogger();
  },
  async initialize() {
    const ctx = await createRuntime();
    bootstrap.setRuntimeLogger(ctx.logger);
    return ctx;
  },
  async run(ctx) {
    await runConsumer(ctx);
  },
  async shutdown(ctx) {
    await gracefulShutdown(ctx);
  },
});
