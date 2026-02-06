import { beforeEach, describe, expect, it, vi } from "vitest";

const configMocks = vi.hoisted(() => ({
  getConfig: vi.fn(),
}));

const kafkaMocks = vi.hoisted(() => ({
  Kafka: vi.fn(),
  logLevel: {
    NOTHING: 0,
    ERROR: 1,
    WARN: 2,
    INFO: 4,
    DEBUG: 5,
  },
}));

vi.mock("../src/config.js", () => ({
  getConfig: configMocks.getConfig,
}));

vi.mock("kafkajs", () => ({
  Kafka: kafkaMocks.Kafka,
  logLevel: kafkaMocks.logLevel,
}));

describe("kafka consumer", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
  });

  it("creates and connects consumer with trimmed brokers", async () => {
    const connect = vi.fn().mockResolvedValue(undefined);
    const consumer = {
      connect,
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const consumerFactory = vi.fn().mockReturnValue(consumer);

    kafkaMocks.Kafka.mockImplementation(() => ({
      consumer: consumerFactory,
    }));

    configMocks.getConfig.mockReturnValue({
      KAFKA_BROKERS: "k1:9092,  k2:9092 ",
      KAFKA_CLIENT_ID: "persister-client",
      KAFKA_CONSUMER_GROUP: "persister-group",
    });

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
      silent: vi.fn(),
    } as any;

    const { createKafkaConsumer } = await import("../src/kafka/consumer.js");
    const result = await createKafkaConsumer(logger);

    expect(kafkaMocks.Kafka).toHaveBeenCalledWith(
      expect.objectContaining({
        clientId: "persister-client",
        brokers: ["k1:9092", "k2:9092"],
        logLevel: kafkaMocks.logLevel.WARN,
        logCreator: expect.any(Function),
      })
    );
    expect(consumerFactory).toHaveBeenCalledWith(
      expect.objectContaining({
        groupId: "persister-group",
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        allowAutoTopicCreation: false,
      })
    );
    expect(connect).toHaveBeenCalledOnce();
    expect(result.consumer).toBe(consumer);
    expect(logger.info).toHaveBeenCalledWith(
      { brokers: ["k1:9092", "k2:9092"], groupId: "persister-group" },
      "Kafka consumer connected"
    );
  });

  it("drops empty broker entries before creating Kafka client", async () => {
    const connect = vi.fn().mockResolvedValue(undefined);
    const consumer = {
      connect,
      disconnect: vi.fn().mockResolvedValue(undefined),
    };

    kafkaMocks.Kafka.mockImplementation(() => ({
      consumer: vi.fn().mockReturnValue(consumer),
    }));

    configMocks.getConfig.mockReturnValue({
      KAFKA_BROKERS: "k1:9092, ,  k2:9092,,",
      KAFKA_CLIENT_ID: "persister-client",
      KAFKA_CONSUMER_GROUP: "persister-group",
    });

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
    } as any;

    const { createKafkaConsumer } = await import("../src/kafka/consumer.js");
    await createKafkaConsumer(logger);

    expect(kafkaMocks.Kafka).toHaveBeenCalledWith(
      expect.objectContaining({
        brokers: ["k1:9092", "k2:9092"],
      })
    );
  });

  it("throws when no non-empty Kafka brokers are configured", async () => {
    configMocks.getConfig.mockReturnValue({
      KAFKA_BROKERS: " , , ",
      KAFKA_CLIENT_ID: "persister-client",
      KAFKA_CONSUMER_GROUP: "persister-group",
    });

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
    } as any;

    const { createKafkaConsumer } = await import("../src/kafka/consumer.js");
    await expect(createKafkaConsumer(logger)).rejects.toThrow(
      "KAFKA_BROKERS must include at least one broker"
    );
    expect(kafkaMocks.Kafka).not.toHaveBeenCalled();
  });

  it("bridges KafkaJS logs into the provided logger", async () => {
    const connect = vi.fn().mockResolvedValue(undefined);
    const consumer = {
      connect,
      disconnect: vi.fn().mockResolvedValue(undefined),
    };

    kafkaMocks.Kafka.mockImplementation((options: { logCreator: () => (entry: any) => void }) => {
      const forward = options.logCreator();
      forward({
        level: kafkaMocks.logLevel.WARN,
        log: { message: "warn-msg", namespace: "kafkajs" },
      });

      return {
        consumer: vi.fn().mockReturnValue(consumer),
      };
    });

    configMocks.getConfig.mockReturnValue({
      KAFKA_BROKERS: "k1:9092",
      KAFKA_CLIENT_ID: "persister-client",
      KAFKA_CONSUMER_GROUP: "persister-group",
    });

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
      silent: vi.fn(),
    } as any;

    const { createKafkaConsumer } = await import("../src/kafka/consumer.js");
    await createKafkaConsumer(logger);

    expect(logger.warn).toHaveBeenCalledWith(
      { namespace: "kafkajs", kafkajs: true },
      "warn-msg"
    );
  });

  it("ignores KafkaJS logs at NOTHING level", async () => {
    const connect = vi.fn().mockResolvedValue(undefined);
    const consumer = {
      connect,
      disconnect: vi.fn().mockResolvedValue(undefined),
    };

    kafkaMocks.Kafka.mockImplementation((options: { logCreator: () => (entry: any) => void }) => {
      const forward = options.logCreator();
      forward({
        level: kafkaMocks.logLevel.NOTHING,
        log: { message: "noop-msg", namespace: "kafkajs" },
      });

      return {
        consumer: vi.fn().mockReturnValue(consumer),
      };
    });

    configMocks.getConfig.mockReturnValue({
      KAFKA_BROKERS: "k1:9092",
      KAFKA_CLIENT_ID: "persister-client",
      KAFKA_CONSUMER_GROUP: "persister-group",
    });

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
    } as any;

    const { createKafkaConsumer } = await import("../src/kafka/consumer.js");
    await createKafkaConsumer(logger);

    expect(logger.warn).not.toHaveBeenCalled();
    expect(logger.error).not.toHaveBeenCalled();
    expect(logger.debug).not.toHaveBeenCalledWith(
      expect.objectContaining({ namespace: "kafkajs", kafkajs: true }),
      "noop-msg"
    );
  });

  it("falls back to debug for unknown KafkaJS log levels", async () => {
    const connect = vi.fn().mockResolvedValue(undefined);
    const consumer = {
      connect,
      disconnect: vi.fn().mockResolvedValue(undefined),
    };

    kafkaMocks.Kafka.mockImplementation((options: { logCreator: () => (entry: any) => void }) => {
      const forward = options.logCreator();
      forward({
        level: 999,
        log: { message: "odd-level-msg", namespace: "kafkajs" },
      });

      return {
        consumer: vi.fn().mockReturnValue(consumer),
      };
    });

    configMocks.getConfig.mockReturnValue({
      KAFKA_BROKERS: "k1:9092",
      KAFKA_CLIENT_ID: "persister-client",
      KAFKA_CONSUMER_GROUP: "persister-group",
    });

    const logger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
    } as any;

    const { createKafkaConsumer } = await import("../src/kafka/consumer.js");
    await createKafkaConsumer(logger);

    expect(logger.debug).toHaveBeenCalledWith(
      { level: 999, namespace: "kafkajs", kafkajs: true },
      "odd-level-msg"
    );
  });

  it("disconnects consumer and logs", async () => {
    const consumer = {
      disconnect: vi.fn().mockResolvedValue(undefined),
    } as any;
    const logger = {
      info: vi.fn(),
    } as any;

    const { disconnectKafkaConsumer } = await import("../src/kafka/consumer.js");
    await disconnectKafkaConsumer(consumer, logger);

    expect(consumer.disconnect).toHaveBeenCalledOnce();
    expect(logger.info).toHaveBeenCalledWith("Kafka consumer disconnected");
  });
});
