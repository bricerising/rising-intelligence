import type { Server } from "node:http";
import {
  createPrismaRuntimeDependencies,
  type PrismaClient,
} from "@rising-intelligence/db";
import {
  createFunctionDependencyFactory,
  closeServer,
  createComponentLoggerFactory,
  createRuntimeCompositionRoot,
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
type RuntimeLoggerComponent = "redis" | "kafka";

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

const DEFAULT_PRISMA_RUNTIME_DEPENDENCIES =
  createPrismaRuntimeDependencies<Config>({
    getDatabaseUrl(config): string {
      return config.DATABASE_URL;
    },
  });

const DEFAULT_DEPENDENCIES: PersisterRuntimeFactoryDependencies = {
  ...DEFAULT_PRISMA_RUNTIME_DEPENDENCIES,
  createHealthContext,
  startHealthServer,
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
  closeHealthServer: closeServer,
};

class DefaultPersisterRuntimeFactory implements PersisterRuntimeFactory {
  constructor(
    private readonly dependencies: PersisterRuntimeFactoryDependencies
  ) {}

  async createRuntime(config: Config, logger: pino.Logger): Promise<PersisterContext> {
    const { startup, resources } = createRuntimeCompositionRoot(logger);
    const componentLoggers =
      createComponentLoggerFactory<RuntimeLoggerComponent>(logger);

    return startup.run(async () => {
      const healthContext = this.dependencies.createHealthContext();
      const healthServer = await resources.connectHealthServer(
        () => this.dependencies.startHealthServer(healthContext, logger),
        (server) => this.dependencies.closeHealthServer(server)
      );

      const prisma = await resources.connectPostgres(
        () => this.dependencies.createPrismaClient(config),
        (prismaClient) => this.dependencies.closePrismaClient(prismaClient)
      );
      healthContext.postgresHealthy = true;
      logger.info("Postgres connected");

      const redis = await resources.connectRedis(
        () =>
          this.dependencies.createRedisClient(
            config,
            componentLoggers.create("redis")
          ),
        (redisClient) => this.dependencies.disconnectRedis(redisClient)
      );
      healthContext.redisHealthy = true;

      const kafkaContext = await resources.connectKafkaConsumer(
        () =>
          this.dependencies.createKafkaConsumer(
            componentLoggers.create("kafka")
          ),
        (consumerContext) =>
          this.dependencies.disconnectKafkaConsumer(consumerContext.consumer, logger)
      );
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
