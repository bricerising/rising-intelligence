import { CompressionTypes } from "kafkajs";
import type { Kafka, Producer } from "kafkajs";
import { createKafkaProducerFactory } from "@rising-intelligence/shared";
import type { Logger } from "pino";
import { getConfig } from "../config.js";

const TOPICS = {
  RAW_EVENTS: "events.raw",
  DLQ: "events.raw.dlq",
  HEARTBEAT: "collector.heartbeat",
} as const;

export { TOPICS };

export interface KafkaProducerContext {
  producer: Producer;
  kafka: Kafka;
}

const createProducer = createKafkaProducerFactory({
  getConfig,
  allowAutoTopicCreation: false,
  onConnected: ({ connection, logger }) => {
    logger.info({ brokers: connection.brokers }, "Kafka producer connected");
  },
});

export async function createKafkaProducer(logger: Logger): Promise<KafkaProducerContext> {
  return createProducer(logger);
}

export async function publishEvent(
  producer: Producer,
  topic: string,
  key: string,
  value: Buffer,
  logger: Logger
): Promise<void> {
  await producer.send({
    topic,
    compression: CompressionTypes.GZIP,
    messages: [{ key, value }],
  });
  logger.debug({ topic, key }, "Event published to Kafka");
}

export async function publishBatch(
  producer: Producer,
  topic: string,
  messages: Array<{ key: string; value: Buffer }>,
  logger: Logger
): Promise<void> {
  if (messages.length === 0) return;

  await producer.send({
    topic,
    compression: CompressionTypes.GZIP,
    messages,
  });
  logger.debug({ topic, count: messages.length }, "Batch published to Kafka");
}

export async function disconnectProducer(
  producer: Producer,
  logger: Logger
): Promise<void> {
  await producer.disconnect();
  logger.info("Kafka producer disconnected");
}
