import { beforeEach, describe, expect, it, vi } from "vitest";

const kafkaMocks = vi.hoisted(() => {
  const producer = {
    connect: vi.fn(),
    send: vi.fn(),
    disconnect: vi.fn(),
  };
  const producerFactory = vi.fn(() => producer);
  const kafkaConstructor = vi.fn(() => ({
    producer: producerFactory,
  }));

  return {
    producer,
    producerFactory,
    kafkaConstructor,
  };
});

const configMocks = vi.hoisted(() => ({
  getConfig: vi.fn(() => ({
    KAFKA_BROKERS: "broker-1:9092, broker-2:9092",
    KAFKA_CLIENT_ID: "collector-test",
  })),
}));

vi.mock("../src/config.js", () => ({
  getConfig: configMocks.getConfig,
}));

vi.mock("kafkajs", () => ({
  Kafka: kafkaMocks.kafkaConstructor,
  CompressionTypes: { GZIP: 1 },
  logLevel: {
    ERROR: 0,
    WARN: 1,
    INFO: 2,
    DEBUG: 4,
    NOTHING: 5,
  },
}));

function createTestLogger() {
  return {
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    silent: vi.fn(),
  } as any;
}

describe("kafka producer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    kafkaMocks.producer.connect.mockResolvedValue(undefined);
    kafkaMocks.producer.send.mockResolvedValue(undefined);
    kafkaMocks.producer.disconnect.mockResolvedValue(undefined);
  });

  it("creates and connects producer with parsed brokers", async () => {
    const { createKafkaProducer } = await import("../src/kafka/producer.js");
    const logger = createTestLogger();

    const context = await createKafkaProducer(logger);

    expect(kafkaMocks.kafkaConstructor).toHaveBeenCalledWith(
      expect.objectContaining({
        clientId: "collector-test",
        brokers: ["broker-1:9092", "broker-2:9092"],
      })
    );
    expect(kafkaMocks.producerFactory).toHaveBeenCalledWith(
      expect.objectContaining({
        allowAutoTopicCreation: false,
        transactionTimeout: 30000,
      })
    );
    expect(kafkaMocks.producer.connect).toHaveBeenCalledOnce();
    expect(context.producer).toBe(kafkaMocks.producer);
    expect(logger.info).toHaveBeenCalledWith(
      { brokers: ["broker-1:9092", "broker-2:9092"] },
      "Kafka producer connected"
    );
  });

  it("publishes a single event with gzip compression", async () => {
    const { publishEvent } = await import("../src/kafka/producer.js");
    const logger = createTestLogger();
    const payload = Buffer.from("payload");

    await publishEvent(kafkaMocks.producer as any, "events.raw", "evt-1", payload, logger);

    expect(kafkaMocks.producer.send).toHaveBeenCalledWith({
      topic: "events.raw",
      compression: 1,
      messages: [{ key: "evt-1", value: payload }],
    });
    expect(logger.debug).toHaveBeenCalledWith(
      { topic: "events.raw", key: "evt-1" },
      "Event published to Kafka"
    );
  });

  it("skips publishBatch when there are no messages", async () => {
    const { publishBatch } = await import("../src/kafka/producer.js");

    await publishBatch(
      kafkaMocks.producer as any,
      "events.raw",
      [],
      createTestLogger()
    );

    expect(kafkaMocks.producer.send).not.toHaveBeenCalled();
  });

  it("publishes batched events with gzip compression", async () => {
    const { publishBatch } = await import("../src/kafka/producer.js");
    const logger = createTestLogger();
    const messages = [
      { key: "evt-1", value: Buffer.from("one") },
      { key: "evt-2", value: Buffer.from("two") },
    ];

    await publishBatch(
      kafkaMocks.producer as any,
      "events.raw",
      messages,
      logger
    );

    expect(kafkaMocks.producer.send).toHaveBeenCalledWith({
      topic: "events.raw",
      compression: 1,
      messages,
    });
    expect(logger.debug).toHaveBeenCalledWith(
      { topic: "events.raw", count: 2 },
      "Batch published to Kafka"
    );
  });

  it("disconnects producer cleanly", async () => {
    const { disconnectProducer } = await import("../src/kafka/producer.js");
    const logger = createTestLogger();

    await disconnectProducer(kafkaMocks.producer as any, logger);

    expect(kafkaMocks.producer.disconnect).toHaveBeenCalledOnce();
    expect(logger.info).toHaveBeenCalledWith("Kafka producer disconnected");
  });
});
