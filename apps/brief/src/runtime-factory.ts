import type { Server } from "node:http";
import type { Redis } from "ioredis";
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
import type pino from "pino";
import type { Config } from "./config.js";
import {
  createHealthContext,
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
import {
  createRedisClient as createRedisConnection,
  disconnectRedis,
} from "./redis.js";

type KafkaConsumer = KafkaConsumerContext["consumer"];
type KafkaProducer = KafkaProducerContext["producer"];

export interface BriefRuntimeFactoryDependencies {
  createHealthContext(initialBudgetUsd: number): HealthContext;
  startHealthServer(ctx: HealthContext, logger: pino.Logger): Server;
  setBudgetRemainingUsd(ctx: HealthContext, amount: number): void;
  createPrismaClient(config: Config): Promise<PrismaClient>;
  createRedisClient(config: Config, logger: pino.Logger): Promise<Redis>;
  createKafkaConsumer(logger: pino.Logger): Promise<KafkaConsumerContext>;
  createKafkaProducer(logger: pino.Logger): Promise<KafkaProducerContext>;
  disconnectKafkaConsumer(consumer: KafkaConsumer, logger: pino.Logger): Promise<void>;
  disconnectKafkaProducer(producer: KafkaProducer, logger: pino.Logger): Promise<void>;
  disconnectRedis(redis: Redis | null, logger: pino.Logger): Promise<void>;
  closePrismaClient(prisma: PrismaClient): Promise<void>;
  closeHealthServer(server: Server): Promise<void>;
}

export interface BriefRuntimeContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
  healthServer: Server;
  kafkaConsumerContext: KafkaConsumerContext;
  kafkaProducerContext: KafkaProducerContext;
  prisma: PrismaClient;
  redis: Redis;
}

export interface BriefRuntimeFactory {
  createRuntime(config: Config, logger: pino.Logger): Promise<BriefRuntimeContext>;
}

type DependencyOverrides = FunctionDependencyOverrides<BriefRuntimeFactoryDependencies>;

const DEFAULT_DEPENDENCIES: BriefRuntimeFactoryDependencies = {
  createHealthContext,
  startHealthServer,
  setBudgetRemainingUsd,
  async createPrismaClient(config): Promise<PrismaClient> {
    return createConnectedPrismaClient({
      databaseUrl: config.DATABASE_URL,
    });
  },
  createRedisClient(config, logger): Promise<Redis> {
    return createRedisConnection(config.REDIS_URL, logger);
  },
  createKafkaConsumer,
  createKafkaProducer,
  disconnectKafkaConsumer,
  disconnectKafkaProducer,
  disconnectRedis,
  closePrismaClient(prisma): Promise<void> {
    return prisma.$disconnect();
  },
  closeHealthServer: closeServer,
};

class DefaultBriefRuntimeFactory implements BriefRuntimeFactory {
  constructor(private readonly dependencies: BriefRuntimeFactoryDependencies) {}

  async createRuntime(config: Config, logger: pino.Logger): Promise<BriefRuntimeContext> {
    const startup = createStartupFacade(logger);
    const resources = createStartupResourceConnector(startup);

    return startup.run(async () => {
      const healthContext = this.dependencies.createHealthContext(config.LLM_DAILY_BUDGET_USD);
      const healthServer = await resources.connect({
        name: "health-server",
        connect: () => this.dependencies.startHealthServer(healthContext, logger),
        disconnect: (server) => this.dependencies.closeHealthServer(server),
        rollbackAction: "close",
      });

      this.dependencies.setBudgetRemainingUsd(healthContext, config.LLM_DAILY_BUDGET_USD);

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
        disconnect: (redisClient) => this.dependencies.disconnectRedis(redisClient, logger),
        rollbackAction: "disconnect",
      });
      healthContext.redisHealthy = true;

      const kafkaConsumerContext = await resources.connect({
        name: "kafka-consumer",
        connect: () => this.dependencies.createKafkaConsumer(logger),
        disconnect: (consumerContext) =>
          this.dependencies.disconnectKafkaConsumer(consumerContext.consumer, logger),
        rollbackAction: "disconnect",
      });
      await kafkaConsumerContext.consumer.subscribe({
        topics: [config.KAFKA_TOPIC_SUMMARY_REQUESTS, config.KAFKA_TOPIC_TREND_SNAPSHOTS],
        fromBeginning: false,
      });

      const kafkaProducerContext = await resources.connect({
        name: "kafka-producer",
        connect: () =>
          this.dependencies.createKafkaProducer(
            logger.child({ component: "kafka-producer" })
          ),
        disconnect: (producerContext) =>
          this.dependencies.disconnectKafkaProducer(producerContext.producer, logger),
        rollbackAction: "disconnect",
      });
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
    });
  }
}

export function createBriefRuntimeFactory(
  overrides: DependencyOverrides = {}
): BriefRuntimeFactory {
  return createFunctionDependencyFactory({
    targetName: "Brief runtime dependency",
    defaults: DEFAULT_DEPENDENCIES,
    overrides,
    create(dependencies): BriefRuntimeFactory {
      return new DefaultBriefRuntimeFactory(dependencies);
    },
  });
}
