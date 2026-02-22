import type { Server } from "node:http";
import type { Redis } from "ioredis";
import {
  createConnectedPrismaClient,
  type PrismaClient,
} from "@rising-intelligence/db";
import {
  closeServer,
  createInitializationRollbackBuilder,
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

type DependencyOverrides = Partial<BriefRuntimeFactoryDependencies>;

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
    const rollbackBuilder = createInitializationRollbackBuilder();

    try {
      const healthContext = this.dependencies.createHealthContext(config.LLM_DAILY_BUDGET_USD);
      const healthServer = this.dependencies.startHealthServer(healthContext, logger);
      rollbackBuilder.register({
        name: "health-server",
        run: async () => this.dependencies.closeHealthServer(healthServer),
        errorMessage: "Health server close failed during initialization rollback",
      });

      this.dependencies.setBudgetRemainingUsd(healthContext, config.LLM_DAILY_BUDGET_USD);

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
        run: async () => this.dependencies.disconnectRedis(redis, logger),
        errorMessage: "Redis disconnect failed during initialization rollback",
      });
      healthContext.redisHealthy = true;

      const kafkaConsumerContext = await this.dependencies.createKafkaConsumer(logger);
      rollbackBuilder.register({
        name: "kafka-consumer",
        run: async () =>
          this.dependencies.disconnectKafkaConsumer(kafkaConsumerContext.consumer, logger),
        errorMessage: "Kafka consumer disconnect failed during initialization rollback",
      });
      await kafkaConsumerContext.consumer.subscribe({
        topics: [config.KAFKA_TOPIC_SUMMARY_REQUESTS, config.KAFKA_TOPIC_TREND_SNAPSHOTS],
        fromBeginning: false,
      });

      const kafkaProducerContext = await this.dependencies.createKafkaProducer(
        logger.child({ component: "kafka-producer" })
      );
      rollbackBuilder.register({
        name: "kafka-producer",
        run: async () =>
          this.dependencies.disconnectKafkaProducer(kafkaProducerContext.producer, logger),
        errorMessage: "Kafka producer disconnect failed during initialization rollback",
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
    } catch (error) {
      await rollbackBuilder.rollback(logger);
      throw error;
    }
  }
}

export function createBriefRuntimeFactory(
  overrides: DependencyOverrides = {}
): BriefRuntimeFactory {
  return new DefaultBriefRuntimeFactory({
    ...DEFAULT_DEPENDENCIES,
    ...overrides,
  });
}
