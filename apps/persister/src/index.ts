import {
  createServiceBootstrap,
  runService,
  runShutdownSteps,
} from "@rising-intelligence/shared/lifecycle";
import { closeServer } from "@rising-intelligence/shared/http";
import { getConfig } from "./config.js";
import { disconnectRedis } from "./redis.js";
import { PERSISTER_BATCH_STRATEGY, type PersisterContext } from "./process.js";
import { createPersisterRuntimeFactory } from "./runtime-factory.js";

const bootstrap = createServiceBootstrap(getConfig);
const runtimeFactory = createPersisterRuntimeFactory();

async function initializePersister(): Promise<PersisterContext> {
  const config = bootstrap.getConfig();
  const logger = bootstrap.getLogger();

  logger.info({ service: config.SERVICE_NAME }, "Starting persister service");
  return runtimeFactory.createRuntime(config, logger);
}

async function runConsumer(ctx: PersisterContext): Promise<void> {
  await ctx.kafkaContext.consumer.consume({
    topics: ctx.config.KAFKA_TOPIC_RAW_EVENTS,
    ctx,
    strategy: PERSISTER_BATCH_STRATEGY,
    fromBeginning: false,
  });
}

async function gracefulShutdown(ctx: PersisterContext): Promise<void> {
  await runShutdownSteps(ctx.logger, [
    {
      name: "kafka-consumer",
      run: async () => ctx.kafkaContext.consumer.disconnect(),
      errorMessage: "Kafka disconnect failed during shutdown",
      onSuccess: () => {
        ctx.healthContext.kafkaHealthy = false;
      },
    },
    {
      name: "redis",
      run: async () => disconnectRedis(ctx.redis),
      errorMessage: "Redis disconnect failed during shutdown",
      onSuccess: () => {
        ctx.healthContext.redisHealthy = false;
      },
    },
    {
      name: "postgres",
      run: async () => ctx.prisma.$disconnect(),
      errorMessage: "Postgres disconnect failed during shutdown",
      onSuccess: () => {
        ctx.healthContext.postgresHealthy = false;
      },
    },
    {
      name: "health-server",
      run: async () => closeServer(ctx.healthServer),
      errorMessage: "Health server close failed during shutdown",
    },
  ]);
}

runService<PersisterContext>({
  name: bootstrap.getServiceName(),
  shutdownTimeoutMs: bootstrap.getShutdownTimeoutMs(),
  getLogger() {
    return bootstrap.getLogger();
  },
  async initialize() {
    const ctx = await initializePersister();
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
