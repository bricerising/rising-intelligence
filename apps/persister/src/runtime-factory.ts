import type { Server } from "node:http";
import {
  createConnectedPrismaClient,
  type PrismaClient,
} from "@rising-intelligence/db";
import {
  createFunctionDependencyFactory,
  closeServer,
  createStartupFacade,
  createStartupResourceConnector,
  type FunctionDependencyOverrides,
} from "@rising-intelligence/shared";
import type { Redis } from "ioredis";
import type pino from "pino";
import { PostgresCircuitBreaker } from "./circuit-breaker.js";
import type { Config } from "./config.js";
import {
  createHealthContext,
  startHealthServer,
  type HealthContext,
} from "./health.js";
import {
  createKafkaConsumer,
  disconnectKafkaConsumer,
  type KafkaConsumerContext,
} from "./kafka/consumer.js";
import { createRedisClient, disconnectRedis } from "./redis.js";
import type { PersisterContext } from "./process.js";

type KafkaConsumer = KafkaConsumerContext["consumer"];

export interface PersisterRuntimeFactoryDependencies {
  createHealthContext(): HealthContext;
  startHealthServer(ctx: HealthContext, logger: pino.Logger): Server;
  createPrismaClient(config: Config): Promise<PrismaClient>;
  createRedisClient(config: Config, logger: pino.Logger): Promise<Redis>;
  createKafkaConsumer(logger: pino.Logger): Promise<KafkaConsumerContext>;
  createCircuitBreaker(config: Config): PostgresCircuitBreaker;
  disconnectKafkaConsumer(consumer: KafkaConsumer, logger: pino.Logger): Promise<void>;
  disconnectRedis(redis: Redis | null): Promise<void>;
  closePrismaClient(prisma: PrismaClient): Promise<void>;
  closeHealthServer(server: Server): Promise<void>;
}

export interface PersisterRuntimeFactory {
  createRuntime(config: Config, logger: pino.Logger): Promise<PersisterContext>;
}

type DependencyOverrides = FunctionDependencyOverrides<PersisterRuntimeFactoryDependencies>;

const DEFAULT_DEPENDENCIES: PersisterRuntimeFactoryDependencies = {
  createHealthContext,
  startHealthServer,
  async createPrismaClient(config): Promise<PrismaClient> {
    return createConnectedPrismaClient({
      databaseUrl: config.DATABASE_URL,
    });
  },
  createRedisClient,
  createKafkaConsumer,
  createCircuitBreaker(config): PostgresCircuitBreaker {
    return new PostgresCircuitBreaker(
      config.POSTGRES_CIRCUIT_FAILURE_THRESHOLD,
      config.POSTGRES_CIRCUIT_OPEN_MS
    );
  },
  disconnectKafkaConsumer,
  disconnectRedis,
  closePrismaClient(prisma): Promise<void> {
    return prisma.$disconnect();
  },
  closeHealthServer: closeServer,
};

class DefaultPersisterRuntimeFactory implements PersisterRuntimeFactory {
  constructor(
    private readonly dependencies: PersisterRuntimeFactoryDependencies
  ) {}

  async createRuntime(config: Config, logger: pino.Logger): Promise<PersisterContext> {
    const startup = createStartupFacade(logger);
    const resources = createStartupResourceConnector(startup);

    return startup.run(async () => {
      const healthContext = this.dependencies.createHealthContext();
      const healthServer = await resources.connect({
        name: "health-server",
        connect: () => this.dependencies.startHealthServer(healthContext, logger),
        disconnect: (server) => this.dependencies.closeHealthServer(server),
        rollbackAction: "close",
      });

      const prisma = await resources.connect({
        name: "postgres",
        connect: () => this.dependencies.createPrismaClient(config),
        disconnect: (prismaClient) => this.dependencies.closePrismaClient(prismaClient),
        rollbackAction: "disconnect",
      });
      healthContext.postgresHealthy = true;
      logger.info("Postgres connected");

      const redis = await resources.connect({
        name: "redis",
        connect: () =>
          this.dependencies.createRedisClient(
            config,
            logger.child({ component: "redis" })
          ),
        disconnect: (redisClient) => this.dependencies.disconnectRedis(redisClient),
        rollbackAction: "disconnect",
      });
      healthContext.redisHealthy = true;

      const kafkaContext = await resources.connect({
        name: "kafka-consumer",
        connect: () =>
          this.dependencies.createKafkaConsumer(logger.child({ component: "kafka" })),
        disconnect: (consumerContext) =>
          this.dependencies.disconnectKafkaConsumer(consumerContext.consumer, logger),
        rollbackAction: "disconnect",
      });
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
        circuitBreaker: this.dependencies.createCircuitBreaker(config),
        lagWriteTimestamps: new Map(),
      };
    });
  }
}

export function createPersisterRuntimeFactory(
  overrides: DependencyOverrides = {}
): PersisterRuntimeFactory {
  return createFunctionDependencyFactory({
    targetName: "Persister runtime dependency",
    defaults: DEFAULT_DEPENDENCIES,
    overrides,
    create(dependencies): PersisterRuntimeFactory {
      return new DefaultPersisterRuntimeFactory(dependencies);
    },
  });
}
