import type { Server } from "node:http";
import type { Redis } from "ioredis";
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
type RuntimeLoggerComponent = "redis" | "kafka-consumer" | "kafka-producer";

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

const DEFAULT_PRISMA_RUNTIME_DEPENDENCIES =
  createPrismaRuntimeDependencies<Config>({
    getDatabaseUrl(config): string {
      return config.DATABASE_URL;
    },
  });

const DEFAULT_DEPENDENCIES: BriefRuntimeFactoryDependencies = {
  ...DEFAULT_PRISMA_RUNTIME_DEPENDENCIES,
  createHealthContext,
  startHealthServer,
  setBudgetRemainingUsd,
  createRedisClient(config, logger): Promise<Redis> {
    return createRedisConnection(config.REDIS_URL, logger);
  },
  createKafkaConsumer,
  createKafkaProducer,
  disconnectKafkaConsumer,
  disconnectKafkaProducer,
  disconnectRedis,
  closeHealthServer: closeServer,
};

class DefaultBriefRuntimeFactory implements BriefRuntimeFactory {
  constructor(private readonly dependencies: BriefRuntimeFactoryDependencies) {}

  async createRuntime(config: Config, logger: pino.Logger): Promise<BriefRuntimeContext> {
    const { startup, resources } = createRuntimeCompositionRoot(logger);
    const componentLoggers =
      createComponentLoggerFactory<RuntimeLoggerComponent>(logger);

    return startup.run(async () => {
      const healthContext = this.dependencies.createHealthContext(config.LLM_DAILY_BUDGET_USD);
      const healthServer = await resources.connectHealthServer(
        () => this.dependencies.startHealthServer(healthContext, logger),
        (server) => this.dependencies.closeHealthServer(server)
      );

      this.dependencies.setBudgetRemainingUsd(healthContext, config.LLM_DAILY_BUDGET_USD);

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
        (redisClient) => this.dependencies.disconnectRedis(redisClient, logger)
      );
      healthContext.redisHealthy = true;

      const kafkaConsumerContext = await resources.connectKafkaConsumer(
        () =>
          this.dependencies.createKafkaConsumer(
            componentLoggers.create("kafka-consumer")
          ),
        (consumerContext) =>
          this.dependencies.disconnectKafkaConsumer(consumerContext.consumer, logger)
      );
      await kafkaConsumerContext.consumer.subscribe({
        topics: [config.KAFKA_TOPIC_SUMMARY_REQUESTS, config.KAFKA_TOPIC_TREND_SNAPSHOTS],
        fromBeginning: false,
      });

      const kafkaProducerContext = await resources.connectKafkaProducer(
        () =>
          this.dependencies.createKafkaProducer(
            componentLoggers.create("kafka-producer")
          ),
        (producerContext) =>
          this.dependencies.disconnectKafkaProducer(producerContext.producer, logger)
      );
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
