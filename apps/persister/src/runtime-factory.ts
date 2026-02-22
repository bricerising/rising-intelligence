import type { Server } from "node:http";
import {
  createConnectedPrismaClient,
  type PrismaClient,
} from "@rising-intelligence/db";
import {
  createFunctionDependencyFactory,
  closeServer,
  createInitializationResourceBuilder,
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
    const resourceBuilder = createInitializationResourceBuilder();

    try {
      const healthContext = this.dependencies.createHealthContext();
      const healthServer = await resourceBuilder.create({
        name: "health-server",
        create: () => this.dependencies.startHealthServer(healthContext, logger),
        rollback: async (server) => this.dependencies.closeHealthServer(server),
        rollbackErrorMessage: "Health server close failed during initialization rollback",
      });

      const prisma = await resourceBuilder.create({
        name: "postgres",
        create: async () => this.dependencies.createPrismaClient(config),
        rollback: async (prismaClient) => this.dependencies.closePrismaClient(prismaClient),
        rollbackErrorMessage: "Postgres disconnect failed during initialization rollback",
      });
      healthContext.postgresHealthy = true;
      logger.info("Postgres connected");

      const redis = await resourceBuilder.create({
        name: "redis",
        create: async () =>
          this.dependencies.createRedisClient(
            config,
            logger.child({ component: "redis" })
          ),
        rollback: async (redisClient) => this.dependencies.disconnectRedis(redisClient),
        rollbackErrorMessage: "Redis disconnect failed during initialization rollback",
      });
      healthContext.redisHealthy = true;

      const kafkaContext = await resourceBuilder.create({
        name: "kafka-consumer",
        create: async () =>
          this.dependencies.createKafkaConsumer(logger.child({ component: "kafka" })),
        rollback: async (consumerContext) =>
          this.dependencies.disconnectKafkaConsumer(consumerContext.consumer, logger),
        rollbackErrorMessage: "Kafka disconnect failed during initialization rollback",
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
    } catch (error) {
      await resourceBuilder.rollback(logger);
      throw error;
    }
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
