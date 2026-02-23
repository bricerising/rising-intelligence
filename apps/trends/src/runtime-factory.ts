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
type RuntimeLoggerComponent = "redis" | "kafka-consumer" | "kafka-producer";

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

const DEFAULT_PRISMA_RUNTIME_DEPENDENCIES =
  createPrismaRuntimeDependencies<Config>({
    getDatabaseUrl(config): string {
      return config.DATABASE_URL;
    },
  });

const DEFAULT_DEPENDENCIES: TrendsRuntimeFactoryDependencies = {
  ...DEFAULT_PRISMA_RUNTIME_DEPENDENCIES,
  createHealthContext,
  startHealthServer,
  createRedisClient(config, logger): Promise<Redis> {
    return createRedisConnection(config.REDIS_URL, logger);
  },
  loadAllowlist,
  createKafkaConsumer,
  createKafkaProducer,
  disconnectKafkaConsumer,
  disconnectKafkaProducer,
  disconnectRedis,
  closeHealthServer: closeServer,
};

class DefaultTrendsRuntimeFactory implements TrendsRuntimeFactory {
  constructor(
    private readonly dependencies: TrendsRuntimeFactoryDependencies
  ) {}

  async createRuntime(config: Config, logger: pino.Logger): Promise<TrendsRuntimeContext> {
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
        (redisClient) => this.dependencies.disconnectRedis(redisClient, logger)
      );
      healthContext.redisHealthy = true;

      const allowlist = this.dependencies.loadAllowlist(config.TOPICS_ALLOWLIST_PATH);
      healthContext.allowlistHealthy = true;
      logger.info({ topicCount: allowlist.topics.length }, "Topics allowlist loaded");

      const kafkaConsumerContext = await resources.connectKafkaConsumer(
        () =>
          this.dependencies.createKafkaConsumer(
            componentLoggers.create("kafka-consumer")
          ),
        (consumerContext) =>
          this.dependencies.disconnectKafkaConsumer(consumerContext.consumer, logger)
      );
      await kafkaConsumerContext.consumer.subscribe({
        topic: config.KAFKA_TOPIC_RAW_EVENTS,
        fromBeginning: false,
      });
      await kafkaConsumerContext.consumer.subscribe({
        topic: config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT,
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
