import type { Server } from "node:http";
import { getConfig } from "./config.js";
import { getLogger } from "./logger.js";
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
  logger: ReturnType<typeof getLogger>;
  healthContext: HealthContext;
  healthServer: Server;
  kafkaContext: KafkaConsumerContext;
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

async function initialize(): Promise<RuntimeContext> {
  const config = getConfig();
  const logger = getLogger();
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

async function shutdown(ctx: RuntimeContext | null, exitCode: number): Promise<never> {
  if (!ctx) {
    process.exit(exitCode);
  }

  const timeout = setTimeout(() => {
    ctx.logger.error({ timeoutMs: ctx.config.SHUTDOWN_TIMEOUT_MS }, "Shutdown timeout reached");
    process.exit(1);
  }, ctx.config.SHUTDOWN_TIMEOUT_MS);

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
    await runConsumer(context);
  } catch (error) {
    getLogger().error({ error: serializeError(error) }, "Brief service crashed");
    await requestShutdown(1);
  }
}

void start();

export {};
