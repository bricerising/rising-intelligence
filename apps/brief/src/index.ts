import type { Server } from "node:http";
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
  incrementConsumed,
  incrementError,
  incrementMalformed,
  startHealthServer,
  type HealthContext,
} from "./health.js";
import {
  createKafkaConsumer,
  disconnectKafkaConsumer,
  type KafkaConsumerContext,
} from "./kafka/consumer.js";
import { deserializeSummaryRequest } from "./deserialize.js";

interface RuntimeContext {
  config: ReturnType<typeof getConfig>;
  logger: pino.Logger;
  healthContext: HealthContext;
  healthServer: Server;
  kafkaContext: KafkaConsumerContext;
}

async function initialize(): Promise<RuntimeContext> {
  const config = getConfig();
  const logger = createServiceLogger(config.SERVICE_NAME, config.LOG_LEVEL);
  logger.info({ service: config.SERVICE_NAME }, "Starting brief service");

  const healthContext = createHealthContext();
  const healthServer = startHealthServer(healthContext, logger);

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
  };
}

async function runConsumer(ctx: RuntimeContext): Promise<void> {
  await ctx.kafkaContext.consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) {
        incrementMalformed(ctx.healthContext);
        incrementError(ctx.healthContext, "parse_error");
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
        incrementConsumed(ctx.healthContext);
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
        incrementMalformed(ctx.healthContext);
        incrementError(ctx.healthContext, "parse_error");
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
