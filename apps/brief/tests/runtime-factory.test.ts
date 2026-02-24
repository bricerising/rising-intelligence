import type { Server } from "node:http";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConsumerConnection, ProducerConnection } from "@rising-intelligence/pipeline/transport";
import type { Config } from "../src/config.js";
import { createHealthContext } from "../src/health.js";
import {
  createBriefRuntimeFactory,
  type BriefRuntimeFactoryDependencies,
} from "../src/runtime-factory.js";

function createConfig(overrides: Partial<Config> = {}): Config {
  return {
    SERVICE_NAME: "brief",
    PORT: 3000,
    LOG_LEVEL: "info",
    KAFKA_BROKERS: "localhost:9092",
    KAFKA_CLIENT_ID: "brief",
    KAFKA_CONSUMER_GROUP: "brief-processor",
    KAFKA_TOPIC_SUMMARY_REQUESTS: "summary.requests",
    KAFKA_TOPIC_SUMMARY_RESULTS: "summary.results",
    KAFKA_TOPIC_TREND_SNAPSHOTS: "trends.snapshots",
    DATABASE_URL: "postgresql://localhost/rising_intelligence",
    POSTGRES_HOST: "localhost",
    POSTGRES_PORT: 5432,
    POSTGRES_DB: "rising_intelligence",
    POSTGRES_USER: "rising",
    POSTGRES_PASSWORD: undefined,
    REDIS_URL: "redis://localhost:6379",
    LLM_PROVIDER: "codex-cli",
    LLM_ENDPOINT_URL: "http://localhost:8088/v1/generate",
    LLM_TIMEOUT_MS: 300000,
    LLM_CODEX_CLI_COMMAND: "codex",
    LLM_CODEX_MODEL: "",
    LLM_CODEX_PROFILE: "",
    LLM_CODEX_TIMEOUT_MS: 300000,
    LLM_DAILY_BUDGET_USD: 5,
    BRIEF_DEFAULT_LOOKBACK_DAYS: 7,
    BRIEF_MAX_LOOKBACK_DAYS: 30,
    BRIEF_MAX_QUERY_EVENTS_PER_TOPIC: 25,
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

function createDependencies(
  overrides: Partial<BriefRuntimeFactoryDependencies> = {}
) {
  const healthContext = createHealthContext();
  const healthServer = {} as Server;
  const prisma = { $disconnect: vi.fn().mockResolvedValue(undefined) } as any;
  const redis = {} as any;
  const kafkaConsumerConnection: ConsumerConnection = {
    consume: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
  };
  const kafkaProducerConnection: ProducerConnection = {
    publish: vi.fn(async () => undefined),
    publishBatch: vi.fn(async () => false),
    disconnect: vi.fn(async () => undefined),
  };

  const dependencies: BriefRuntimeFactoryDependencies = {
    createHealthContext: vi.fn(() => healthContext),
    startHealthServer: vi.fn(() => healthServer),
    setBudgetRemainingUsd: vi.fn(() => undefined),
    createPrismaClient: vi.fn(async () => prisma),
    createRedisClient: vi.fn(async () => redis),
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
    kafkaConsumerConnection,
    kafkaProducerConnection,
  };
}

describe("createBriefRuntimeFactory", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("uses default dependencies when an override is explicitly undefined", async () => {
    const config = createConfig();
    const loggerHarness = createLogger();
    const setup = createDependencies();
    const factory = createBriefRuntimeFactory({
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
      kafkaConsumerConnection,
      kafkaProducerConnection,
    } = createDependencies();
    const factory = createBriefRuntimeFactory(dependencies);

    const ctx = await factory.createRuntime(config, loggerHarness);

    expect(ctx.config).toBe(config);
    expect(ctx.logger).toBe(loggerHarness);
    expect(ctx.healthContext).toBe(healthContext);
    expect(ctx.healthServer).toBe(healthServer);
    expect(ctx.prisma).toBe(prisma);
    expect(ctx.redis).toBe(redis);
    expect(ctx.kafkaConsumerContext.consumer).toBe(kafkaConsumerConnection);
    expect(ctx.kafkaProducerContext.producer).toBe(kafkaProducerConnection);

    expect(dependencies.createHealthContext).toHaveBeenCalledWith(config.LLM_DAILY_BUDGET_USD);
    expect(dependencies.startHealthServer).toHaveBeenCalledWith(healthContext, loggerHarness);
    expect(dependencies.setBudgetRemainingUsd).toHaveBeenCalledWith(
      healthContext,
      config.LLM_DAILY_BUDGET_USD
    );
    expect(dependencies.createPrismaClient).toHaveBeenCalledWith(config);
    expect(dependencies.createRedisClient).toHaveBeenCalledWith(
      config,
      loggerHarness.childFor("redis")
    );
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
    const factory = createBriefRuntimeFactory(setup.dependencies);

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
      createBriefRuntimeFactory({
        createKafkaConsumer: 123 as unknown as BriefRuntimeFactoryDependencies["createKafkaConsumer"],
      })
    ).toThrow('Brief runtime dependency override "createKafkaConsumer" must be a function');
  });
});
