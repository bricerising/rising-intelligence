import type { Server } from "node:http";
import {
  createConsumerConnection,
  createProducerConnection,
  type ConsumerConnection,
  type ProducerConnection,
} from "@rising-intelligence/pipeline/transport";
import {
  createPrismaRuntimeDependencies,
  type PrismaClient,
} from "@rising-intelligence/db";
import {
  createFunctionDependencyFactory,
  createRuntimeCompositionRoot,
  type FunctionDependencyOverrides,
} from "@rising-intelligence/shared/lifecycle";
import { closeServer } from "@rising-intelligence/shared/http";
import { createComponentLoggerFactory } from "@rising-intelligence/shared/logging";
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
  createRedisClient as createRedisConnection,
  disconnectRedis,
} from "./redis.js";
import type { TrendsContext } from "./process.js";

export interface KafkaConsumerContext {
  consumer: ConsumerConnection;
}

export interface KafkaProducerContext {
  producer: ProducerConnection;
}

type KafkaConsumer = KafkaConsumerContext["consumer"];
type KafkaProducer = KafkaProducerContext["producer"];
type RuntimeLoggerComponent = "redis" | "kafka-consumer" | "kafka-producer";

export interface TrendsRuntimeFactoryDependencies {
  createHealthContext(): HealthContext;
  startHealthServer(ctx: HealthContext, logger: pino.Logger): Server;
  createPrismaClient(config: Config): Promise<PrismaClient>;
  createRedisClient(config: Config, logger: pino.Logger): Promise<Redis>;
  loadAllowlist(path: string): CompiledAllowlist;
  createKafkaConsumer(config: Config, logger: pino.Logger): Promise<KafkaConsumer>;
  createKafkaProducer(config: Config, logger: pino.Logger): Promise<KafkaProducer>;
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
  async createKafkaConsumer(config, logger): Promise<KafkaConsumer> {
    return createConsumerConnection({
      brokers: config.KAFKA_BROKERS,
      clientId: config.KAFKA_CLIENT_ID,
      groupId: config.KAFKA_CONSUMER_GROUP,
      logger,
    });
  },
  async createKafkaProducer(config, logger): Promise<KafkaProducer> {
    return createProducerConnection({
      brokers: config.KAFKA_BROKERS,
      clientId: config.KAFKA_CLIENT_ID,
      clientIdSuffix: "-producer",
      logger,
    });
  },
  async disconnectKafkaConsumer(consumer, logger): Promise<void> {
    await consumer.disconnect();
    logger.info("Kafka consumer disconnected");
  },
  async disconnectKafkaProducer(producer, logger): Promise<void> {
    await producer.disconnect();
    logger.info("Kafka producer disconnected");
  },
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
        async () => ({
          consumer: await this.dependencies.createKafkaConsumer(
            config,
            componentLoggers.create("kafka-consumer")
          ),
        }),
        (consumerContext) =>
          this.dependencies.disconnectKafkaConsumer(consumerContext.consumer, logger)
      );

      const kafkaProducerContext = await resources.connectKafkaProducer(
        async () => ({
          producer: await this.dependencies.createKafkaProducer(
            config,
            componentLoggers.create("kafka-producer")
          ),
        }),
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
