import type { Server } from "node:http";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConsumerConnection } from "@rising-intelligence/pipeline/transport";
import { PostgresCircuitBreaker } from "../src/circuit-breaker.js";
import type { Config } from "../src/config.js";
import { createHealthContext } from "../src/health.js";
import {
  createPersisterRuntimeFactory,
  type PersisterRuntimeFactoryDependencies,
} from "../src/runtime-factory.js";

function createConfig(overrides: Partial<Config> = {}): Config {
  return {
    SERVICE_NAME: "persister",
    PORT: 3000,
    LOG_LEVEL: "info",
    KAFKA_BROKERS: "localhost:9092",
    KAFKA_CLIENT_ID: "persister",
    KAFKA_CONSUMER_GROUP: "persister",
    KAFKA_TOPIC_RAW_EVENTS: "events.raw",
    DATABASE_URL: "postgresql://localhost/rising_intelligence",
    POSTGRES_HOST: "localhost",
    POSTGRES_PORT: 5432,
    POSTGRES_DB: "rising_intelligence",
    POSTGRES_USER: "rising",
    POSTGRES_PASSWORD: undefined,
    REDIS_URL: "redis://localhost:6379",
    SEEN_TTL_SECONDS: 86400,
    CONSUMER_LAG_UPDATE_INTERVAL_MS: 15000,
    POSTGRES_CIRCUIT_FAILURE_THRESHOLD: 5,
    POSTGRES_CIRCUIT_OPEN_MS: 30000,
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

function createMockConsumerConnection(): ConsumerConnection {
  return {
    consume: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
  };
}

function createDependencies(
  overrides: Partial<PersisterRuntimeFactoryDependencies> = {}
) {
  const healthContext = createHealthContext();
  const healthServer = {} as Server;
  const prisma = { $disconnect: vi.fn().mockResolvedValue(undefined) } as any;
  const redis = {} as any;
  const kafkaConsumerConnection = createMockConsumerConnection();
  const circuitBreaker = new PostgresCircuitBreaker(2, 60000);

  const dependencies: PersisterRuntimeFactoryDependencies = {
    createHealthContext: vi.fn(() => healthContext),
    startHealthServer: vi.fn(() => healthServer),
    createPrismaClient: vi.fn(async () => prisma),
    createRedisClient: vi.fn(async () => redis),
    createKafkaConsumer: vi.fn(async () => kafkaConsumerConnection),
    createCircuitBreaker: vi.fn(() => circuitBreaker),
    disconnectKafkaConsumer: vi.fn(async () => undefined),
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
    circuitBreaker,
  };
}

describe("createPersisterRuntimeFactory", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("uses default dependencies when an override is explicitly undefined", async () => {
    const config = createConfig();
    const loggerHarness = createLogger();
    const setup = createDependencies();
    const factory = createPersisterRuntimeFactory({
      ...setup.dependencies,
      createHealthContext: undefined,
    });

    const ctx = await factory.createRuntime(config, loggerHarness);

    expect(ctx.healthContext).not.toBe(setup.healthContext);
    expect(setup.dependencies.createHealthContext).not.toHaveBeenCalled();
  });

  it("creates a runtime context with healthy dependencies and typed constructor wiring", async () => {
    const config = createConfig({
      POSTGRES_CIRCUIT_FAILURE_THRESHOLD: 3,
      POSTGRES_CIRCUIT_OPEN_MS: 120000,
    });
    const loggerHarness = createLogger();
    const {
      dependencies,
      healthContext,
      healthServer,
      prisma,
      redis,
      kafkaConsumerConnection,
      circuitBreaker,
    } = createDependencies();
    const factory = createPersisterRuntimeFactory(dependencies);

    const ctx = await factory.createRuntime(config, loggerHarness);

    expect(ctx.config).toBe(config);
    expect(ctx.logger).toBe(loggerHarness);
    expect(ctx.healthContext).toBe(healthContext);
    expect(ctx.healthServer).toBe(healthServer);
    expect(ctx.prisma).toBe(prisma);
    expect(ctx.redis).toBe(redis);
    expect(ctx.kafkaContext.consumer).toBe(kafkaConsumerConnection);
    expect(ctx.circuitBreaker).toBe(circuitBreaker);
    expect(ctx.lagWriteTimestamps.size).toBe(0);

    expect(dependencies.createPrismaClient).toHaveBeenCalledWith(config);
    expect(dependencies.createRedisClient).toHaveBeenCalledWith(
      config,
      loggerHarness.childFor("redis")
    );
    expect(dependencies.createKafkaConsumer).toHaveBeenCalledWith(
      config,
      loggerHarness.childFor("kafka")
    );
    expect(dependencies.createCircuitBreaker).toHaveBeenCalledWith(config);

    expect(loggerHarness.child).toHaveBeenCalledWith({ component: "redis" });
    expect(loggerHarness.child).toHaveBeenCalledWith({ component: "kafka" });

    expect(healthContext.postgresHealthy).toBe(true);
    expect(healthContext.redisHealthy).toBe(true);
    expect(healthContext.kafkaHealthy).toBe(true);
  });

  it("rolls back already-created resources in reverse order when initialization fails", async () => {
    const config = createConfig();
    const loggerHarness = createLogger();
    const cleanupOrder: string[] = [];
    const setup = createDependencies({
      createKafkaConsumer: vi.fn(async () => {
        throw new Error("consumer connect failed");
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
    const factory = createPersisterRuntimeFactory(setup.dependencies);

    await expect(factory.createRuntime(config, loggerHarness)).rejects.toThrow("consumer connect failed");

    expect(cleanupOrder).toEqual([
      "redis",
      "postgres",
      "health-server",
    ]);
  });

  it("fails fast when a dependency override is not a function", () => {
    expect(() =>
      createPersisterRuntimeFactory({
        createKafkaConsumer: 123 as unknown as PersisterRuntimeFactoryDependencies["createKafkaConsumer"],
      })
    ).toThrow('Persister runtime dependency override "createKafkaConsumer" must be a function');
  });
});
