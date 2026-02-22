import type { Server } from "node:http";
import {
  createConnectedPrismaClient,
  type PrismaClient,
} from "@rising-intelligence/db";
import {
  closeServer,
  createInitializationRollbackBuilder,
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

type DependencyOverrides = Partial<PersisterRuntimeFactoryDependencies>;

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
    const rollbackBuilder = createInitializationRollbackBuilder();

    try {
      const healthContext = this.dependencies.createHealthContext();
      const healthServer = this.dependencies.startHealthServer(healthContext, logger);
      rollbackBuilder.register({
        name: "health-server",
        run: async () => this.dependencies.closeHealthServer(healthServer),
        errorMessage: "Health server close failed during initialization rollback",
      });

      const prisma = await this.dependencies.createPrismaClient(config);
      rollbackBuilder.register({
        name: "postgres",
        run: async () => this.dependencies.closePrismaClient(prisma),
        errorMessage: "Postgres disconnect failed during initialization rollback",
      });
      healthContext.postgresHealthy = true;
      logger.info("Postgres connected");

      const redis = await this.dependencies.createRedisClient(
        config,
        logger.child({ component: "redis" })
      );
      rollbackBuilder.register({
        name: "redis",
        run: async () => this.dependencies.disconnectRedis(redis),
        errorMessage: "Redis disconnect failed during initialization rollback",
      });
      healthContext.redisHealthy = true;

      const kafkaContext = await this.dependencies.createKafkaConsumer(
        logger.child({ component: "kafka" })
      );
      rollbackBuilder.register({
        name: "kafka-consumer",
        run: async () =>
          this.dependencies.disconnectKafkaConsumer(kafkaContext.consumer, logger),
        errorMessage: "Kafka disconnect failed during initialization rollback",
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
      await rollbackBuilder.rollback(logger);
      throw error;
    }
  }
}

export function createPersisterRuntimeFactory(
  overrides: DependencyOverrides = {}
): PersisterRuntimeFactory {
  return new DefaultPersisterRuntimeFactory({
    ...DEFAULT_DEPENDENCIES,
    ...overrides,
  });
}
