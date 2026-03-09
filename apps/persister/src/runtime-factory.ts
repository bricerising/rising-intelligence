import type { Server } from "node:http";
import {
  createConsumerConnection,
  type ConsumerConnection,
} from "@rising-intelligence/pipeline/transport";
import {
  createPrismaRuntimeDependencies,
  type PrismaClient,
} from "@rising-intelligence/db";
import {
  createFunctionDependencyFactory,
  createRuntimeCompositionRoot,
  healthServerSpec,
  kafkaConsumerSpec,
  postgresSpec,
  redisSpec,
  type FunctionDependencyOverrides,
} from "@rising-intelligence/shared/lifecycle";
import { closeServer } from "@rising-intelligence/shared/http";
import { createComponentLoggerFactory } from "@rising-intelligence/shared/logging";
import type { Redis } from "ioredis";
import type pino from "pino";
import { PostgresCircuitBreaker } from "./circuit-breaker.js";
import type { Config } from "./config.js";
import {
  createHealthContext,
  startHealthServer,
  type HealthContext,
} from "./health.js";
import { createRedisClient, disconnectRedis } from "./redis.js";
import type { PersisterContext } from "./process.js";

type KafkaConsumer = ConsumerConnection;
type RuntimeLoggerComponent = "redis" | "kafka";

export interface PersisterRuntimeFactoryDependencies {
  createHealthContext(): HealthContext;
  startHealthServer(ctx: HealthContext, logger: pino.Logger): Server;
  createPrismaClient(config: Config): Promise<PrismaClient>;
  createRedisClient(config: Config, logger: pino.Logger): Promise<Redis>;
  createKafkaConsumer(config: Config, logger: pino.Logger): Promise<KafkaConsumer>;
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
  async createKafkaConsumer(config, logger): Promise<KafkaConsumer> {
    return createConsumerConnection({
      brokers: config.KAFKA_BROKERS,
      clientId: config.KAFKA_CLIENT_ID,
      groupId: config.KAFKA_CONSUMER_GROUP,
      logger,
    });
  },
  createCircuitBreaker(config): PostgresCircuitBreaker {
    return new PostgresCircuitBreaker(
      config.POSTGRES_CIRCUIT_FAILURE_THRESHOLD,
      config.POSTGRES_CIRCUIT_OPEN_MS
    );
  },
  async disconnectKafkaConsumer(consumer, logger): Promise<void> {
    await consumer.disconnect();
    logger.info("Kafka consumer disconnected");
  },
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
      const healthServer = await resources.connect(healthServerSpec(
        () => this.dependencies.startHealthServer(healthContext, logger),
        (server) => this.dependencies.closeHealthServer(server)
      ));

      const prisma = await resources.connect(postgresSpec(
        () => this.dependencies.createPrismaClient(config),
        (prismaClient) => this.dependencies.closePrismaClient(prismaClient)
      ));
      healthContext.postgresHealthy = true;
      logger.info("Postgres connected");

      const redis = await resources.connect(redisSpec(
        () =>
          this.dependencies.createRedisClient(
            config,
            componentLoggers.create("redis")
          ),
        (redisClient) => this.dependencies.disconnectRedis(redisClient)
      ));
      healthContext.redisHealthy = true;

      const kafkaConsumerConnection = await resources.connect(kafkaConsumerSpec(
        () =>
          this.dependencies.createKafkaConsumer(
            config,
            componentLoggers.create("kafka")
          ),
        (consumerConnection) =>
          this.dependencies.disconnectKafkaConsumer(consumerConnection, logger)
      ));
      healthContext.kafkaHealthy = true;
      logger.info({ topic: config.KAFKA_TOPIC_RAW_EVENTS }, "Kafka consumer initialized");

      return {
        config,
        logger,
        healthContext,
        healthServer,
        prisma,
        redis,
        kafkaContext: {
          consumer: kafkaConsumerConnection,
        },
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
