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
import { loadAllowlist, type CompiledAllowlist } from "./allowlist.js";
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
import {
  createKafkaProducer,
  disconnectKafkaProducer,
  type KafkaProducerContext,
} from "./kafka/producer.js";
import {
  createRedisClient as createRedisConnection,
  disconnectRedis,
} from "./redis.js";
import type { TrendsContext } from "./process.js";

type KafkaConsumer = KafkaConsumerContext["consumer"];
type KafkaProducer = KafkaProducerContext["producer"];

export interface TrendsRuntimeFactoryDependencies {
  createHealthContext(): HealthContext;
  startHealthServer(ctx: HealthContext, logger: pino.Logger): Server;
  createPrismaClient(config: Config): Promise<PrismaClient>;
  createRedisClient(config: Config, logger: pino.Logger): Promise<Redis>;
  loadAllowlist(path: string): CompiledAllowlist;
  createKafkaConsumer(logger: pino.Logger): Promise<KafkaConsumerContext>;
  createKafkaProducer(logger: pino.Logger): Promise<KafkaProducerContext>;
  disconnectKafkaConsumer(consumer: KafkaConsumer, logger: pino.Logger): Promise<void>;
  disconnectKafkaProducer(producer: KafkaProducer, logger: pino.Logger): Promise<void>;
  disconnectRedis(redis: Redis | null, logger: pino.Logger): Promise<void>;
  closePrismaClient(prisma: PrismaClient): Promise<void>;
  closeHealthServer(server: Server): Promise<void>;
}

export interface TrendsRuntimeContext extends TrendsContext {
  healthServer: Server;
  kafkaConsumerContext: KafkaConsumerContext;
  kafkaProducerContext: KafkaProducerContext;
  snapshotTimer: NodeJS.Timeout | null;
  snapshotInFlight: boolean;
}

export interface TrendsRuntimeFactory {
  createRuntime(config: Config, logger: pino.Logger): Promise<TrendsRuntimeContext>;
}

type DependencyOverrides = FunctionDependencyOverrides<TrendsRuntimeFactoryDependencies>;

const DEFAULT_DEPENDENCIES: TrendsRuntimeFactoryDependencies = {
  createHealthContext,
  startHealthServer,
  async createPrismaClient(config): Promise<PrismaClient> {
    return createConnectedPrismaClient({
      databaseUrl: config.DATABASE_URL,
    });
  },
  createRedisClient(config, logger): Promise<Redis> {
    return createRedisConnection(config.REDIS_URL, logger);
  },
  loadAllowlist,
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

class DefaultTrendsRuntimeFactory implements TrendsRuntimeFactory {
  constructor(
    private readonly dependencies: TrendsRuntimeFactoryDependencies
  ) {}

  async createRuntime(config: Config, logger: pino.Logger): Promise<TrendsRuntimeContext> {
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
        disconnect: (redisClient) => this.dependencies.disconnectRedis(redisClient, logger),
        rollbackAction: "disconnect",
      });
      healthContext.redisHealthy = true;

      const allowlist = this.dependencies.loadAllowlist(config.TOPICS_ALLOWLIST_PATH);
      healthContext.allowlistHealthy = true;
      logger.info({ topicCount: allowlist.topics.length }, "Topics allowlist loaded");

      const kafkaConsumerContext = await resources.connect({
        name: "kafka-consumer",
        connect: () =>
          this.dependencies.createKafkaConsumer(
            logger.child({ component: "kafka-consumer" })
          ),
        disconnect: (consumerContext) =>
          this.dependencies.disconnectKafkaConsumer(consumerContext.consumer, logger),
        rollbackAction: "disconnect",
      });
      await kafkaConsumerContext.consumer.subscribe({
        topic: config.KAFKA_TOPIC_RAW_EVENTS,
        fromBeginning: false,
      });
      await kafkaConsumerContext.consumer.subscribe({
        topic: config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT,
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
          consumeTopic: config.KAFKA_TOPIC_RAW_EVENTS,
          heartbeatTopic: config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT,
          publishTopic: config.KAFKA_TOPIC_TRENDS_SNAPSHOTS,
          windows: config.WINDOWS,
        },
        "Kafka subscriptions initialized"
      );

      return {
        config,
        logger,
        healthContext,
        prisma,
        redis,
        allowlist,
        lagWriteTimestamps: new Map(),
        healthServer,
        kafkaConsumerContext,
        kafkaProducerContext,
        snapshotTimer: null,
        snapshotInFlight: false,
      };
    });
  }
}

export function createTrendsRuntimeFactory(
  overrides: DependencyOverrides = {}
): TrendsRuntimeFactory {
  return createFunctionDependencyFactory({
    targetName: "Trends runtime dependency",
    defaults: DEFAULT_DEPENDENCIES,
    overrides,
    create(dependencies): TrendsRuntimeFactory {
      return new DefaultTrendsRuntimeFactory(dependencies);
    },
  });
}
