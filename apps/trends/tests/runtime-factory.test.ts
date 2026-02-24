import type { Server } from "node:http";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConsumerConnection, ProducerConnection } from "@rising-intelligence/pipeline/transport";
import type { CompiledAllowlist } from "../src/allowlist.js";
import type { Config } from "../src/config.js";
import { createHealthContext } from "../src/health.js";
import {
  createTrendsRuntimeFactory,
  type TrendsRuntimeFactoryDependencies,
} from "../src/runtime-factory.js";

function createConfig(overrides: Partial<Config> = {}): Config {
  return {
    SERVICE_NAME: "trends",
    PORT: 3000,
    LOG_LEVEL: "info",
    KAFKA_BROKERS: "localhost:9092",
    KAFKA_CLIENT_ID: "trends",
    KAFKA_CONSUMER_GROUP: "trends-processor",
    PERSISTER_CONSUMER_GROUP: "persister",
    KAFKA_TOPIC_RAW_EVENTS: "events.raw",
    KAFKA_TOPIC_COLLECTOR_HEARTBEAT: "collector.heartbeat",
    KAFKA_TOPIC_TRENDS_SNAPSHOTS: "trends.snapshots",
    KAFKA_TOPIC_SUMMARY_REQUESTS: "summary.requests",
    DATABASE_URL: "postgresql://localhost/rising_intelligence",
    POSTGRES_HOST: "localhost",
    POSTGRES_PORT: 5432,
    POSTGRES_DB: "rising_intelligence",
    POSTGRES_USER: "rising",
    POSTGRES_PASSWORD: undefined,
    REDIS_URL: "redis://localhost:6379",
    TOPICS_ALLOWLIST_PATH: "./config/topics.allowlist.yaml",
    WINDOWS: ["15m", "60m"],
    TOP_N_TOPICS: 10,
    MAX_EVIDENCE_PER_TOPIC: 10,
    SNAPSHOT_INTERVAL_SECONDS: 300,
    CONSUMER_LAG_UPDATE_INTERVAL_MS: 15000,
    DAILY_BRIEF_ENABLED: true,
    DAILY_BRIEF_UTC_HOUR: 1,
    DAILY_BRIEF_UTC_MINUTE: 0,
    MAX_LAG_MESSAGES: 100,
    MAX_LAG_AGE_MS: 300000,
    MAX_SOURCE_HEARTBEAT_AGE_MS: 300000,
    MIN_HEALTHY_SOURCES: 2,
    BRIEF_DAILY_BUDGET_USD: 5,
    BRIEF_MAX_TOPICS: 10,
    BRIEF_MAX_EVIDENCE_PER_TOPIC: 5,
    BRIEF_MAX_OUTPUT_TOKENS: 2000,
    SHUTDOWN_TIMEOUT_MS: 30000,
    ...overrides,
  };
}

function createLogger() {
  const createMockLogger = () =>
    ({
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
      child: vi.fn(),
    }) as any;

  const logger = createMockLogger();
  const childLoggers = new Map<string, any>();

  logger.child.mockImplementation((bindings: { component?: unknown }) => {
    const component = String(bindings.component);
    const existing = childLoggers.get(component);
    if (existing) {
      return existing;
    }

    const childLogger = createMockLogger();
    childLogger.child.mockReturnValue(childLogger);
    childLoggers.set(component, childLogger);
    return childLogger;
  });

  return Object.assign(logger, {
    childFor(component: string) {
      const childLogger = childLoggers.get(component);
      if (!childLogger) {
        throw new Error(`Expected logger child for component "${component}"`);
      }
      return childLogger;
    },
  });
}

function createAllowlist(): CompiledAllowlist {
  const topic = {
    key: "aws.bedrock",
    displayName: "AWS Bedrock",
    priority: 90,
  };

  return {
    topics: [topic],
    topicMap: new Map([[topic.key, topic]]),
    mutedTopics: new Set<string>(),
    maxTopicsPerEvent: 5,
  };
}

function createMockConsumerConnection(): ConsumerConnection {
  return {
    consume: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
  };
}

function createMockProducerConnection(): ProducerConnection {
  return {
    publish: vi.fn(async () => undefined),
    publishBatch: vi.fn(async () => false),
    disconnect: vi.fn(async () => undefined),
  };
}

function createDependencies(
  overrides: Partial<TrendsRuntimeFactoryDependencies> = {}
) {
  const healthContext = createHealthContext();
  const healthServer = {} as Server;
  const prisma = { $disconnect: vi.fn().mockResolvedValue(undefined) } as any;
  const redis = {} as any;
  const allowlist = createAllowlist();
  const kafkaConsumerConnection = createMockConsumerConnection();
  const kafkaProducerConnection = createMockProducerConnection();

  const dependencies: TrendsRuntimeFactoryDependencies = {
    createHealthContext: vi.fn(() => healthContext),
    startHealthServer: vi.fn(() => healthServer),
    createPrismaClient: vi.fn(async () => prisma),
    createRedisClient: vi.fn(async () => redis),
    loadAllowlist: vi.fn(() => allowlist),
    createKafkaConsumer: vi.fn(async () => kafkaConsumerConnection),
    createKafkaProducer: vi.fn(async () => kafkaProducerConnection),
    disconnectKafkaConsumer: vi.fn(async () => undefined),
    disconnectKafkaProducer: vi.fn(async () => undefined),
    disconnectRedis: vi.fn(async () => undefined),
    closePrismaClient: vi.fn(async () => undefined),
    closeHealthServer: vi.fn(async () => undefined),
    ...overrides,
  };

  return {
    dependencies,
    healthContext,
    healthServer,
    prisma,
    redis,
    allowlist,
    kafkaConsumerConnection,
    kafkaProducerConnection,
  };
}

describe("createTrendsRuntimeFactory", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("uses default dependencies when an override is explicitly undefined", async () => {
    const config = createConfig();
    const loggerHarness = createLogger();
    const setup = createDependencies();
    const factory = createTrendsRuntimeFactory({
      ...setup.dependencies,
      createHealthContext: undefined,
    });

    const ctx = await factory.createRuntime(config, loggerHarness);

    expect(ctx.healthContext).not.toBe(setup.healthContext);
    expect(setup.dependencies.createHealthContext).not.toHaveBeenCalled();
  });

  it("creates a runtime context with healthy dependencies", async () => {
    const config = createConfig();
    const loggerHarness = createLogger();
    const {
      dependencies,
      healthContext,
      healthServer,
      prisma,
      redis,
      allowlist,
      kafkaConsumerConnection,
      kafkaProducerConnection,
    } = createDependencies();
    const factory = createTrendsRuntimeFactory(dependencies);

    const ctx = await factory.createRuntime(config, loggerHarness);

    expect(ctx.config).toBe(config);
    expect(ctx.logger).toBe(loggerHarness);
    expect(ctx.healthContext).toBe(healthContext);
    expect(ctx.healthServer).toBe(healthServer);
    expect(ctx.prisma).toBe(prisma);
    expect(ctx.redis).toBe(redis);
    expect(ctx.allowlist).toBe(allowlist);
    expect(ctx.kafkaConsumerContext.consumer).toBe(kafkaConsumerConnection);
    expect(ctx.kafkaProducerContext.producer).toBe(kafkaProducerConnection);
    expect(ctx.lagWriteTimestamps.size).toBe(0);
    expect(ctx.snapshotTimer).toBeNull();
    expect(ctx.snapshotInFlight).toBe(false);

    expect(dependencies.createPrismaClient).toHaveBeenCalledWith(config);
    expect(dependencies.createRedisClient).toHaveBeenCalledWith(
      config,
      loggerHarness.childFor("redis")
    );
    expect(dependencies.loadAllowlist).toHaveBeenCalledWith(config.TOPICS_ALLOWLIST_PATH);
    expect(dependencies.createKafkaConsumer).toHaveBeenCalledWith(
      config,
      loggerHarness.childFor("kafka-consumer")
    );
    expect(dependencies.createKafkaProducer).toHaveBeenCalledWith(
      config,
      loggerHarness.childFor("kafka-producer")
    );

    expect(loggerHarness.child).toHaveBeenCalledWith({ component: "redis" });
    expect(loggerHarness.child).toHaveBeenCalledWith({ component: "kafka-consumer" });
    expect(loggerHarness.child).toHaveBeenCalledWith({ component: "kafka-producer" });

    expect(healthContext.postgresHealthy).toBe(true);
    expect(healthContext.redisHealthy).toBe(true);
    expect(healthContext.allowlistHealthy).toBe(true);
    expect(healthContext.kafkaHealthy).toBe(true);
  });

  it("rolls back already-created resources in reverse order when initialization fails", async () => {
    const config = createConfig();
    const loggerHarness = createLogger();
    const cleanupOrder: string[] = [];
    const setup = createDependencies({
      createKafkaProducer: vi.fn(async () => {
        throw new Error("producer failed");
      }),
      disconnectKafkaConsumer: vi.fn(async () => {
        cleanupOrder.push("kafka-consumer");
      }),
      disconnectRedis: vi.fn(async () => {
        cleanupOrder.push("redis");
      }),
      closePrismaClient: vi.fn(async () => {
        cleanupOrder.push("postgres");
      }),
      closeHealthServer: vi.fn(async () => {
        cleanupOrder.push("health-server");
      }),
    });
    const factory = createTrendsRuntimeFactory(setup.dependencies);

    await expect(factory.createRuntime(config, loggerHarness)).rejects.toThrow("producer failed");

    expect(cleanupOrder).toEqual([
      "kafka-consumer",
      "redis",
      "postgres",
      "health-server",
    ]);
  });

  it("fails fast when a dependency override is not a function", () => {
    expect(() =>
      createTrendsRuntimeFactory({
        createKafkaProducer: 123 as unknown as TrendsRuntimeFactoryDependencies["createKafkaProducer"],
      })
    ).toThrow('Trends runtime dependency override "createKafkaProducer" must be a function');
  });
});
