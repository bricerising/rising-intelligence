import type { Kafka, Producer } from "kafkajs";
import {
  createKafkaProducerFactory,
  publishKafkaTopicMessage,
} from "@rising-intelligence/shared";
import type { Logger } from "pino";
import { getConfig } from "../config.js";

export interface KafkaProducerContext {
  kafka: Kafka;
  producer: Producer;
}

const createProducer = createKafkaProducerFactory({
  getConfig,
  clientIdSuffix: "-producer",
  allowAutoTopicCreation: false,
  onConnected: ({ connection, logger }) => {
    logger.info({ brokers: connection.brokers }, "Kafka producer connected");
  },
});

export async function createKafkaProducer(logger: Logger): Promise<KafkaProducerContext> {
  return createProducer(logger);
}

export async function publishSnapshot(
  producer: Producer,
  topic: string,
  key: string,
  value: Buffer,
  logger: Logger
): Promise<void> {
  await publishKafkaTopicMessage(producer, topic, key, value, logger, {
    logMessage: "Trend snapshot published",
  });
}

export async function publishSummaryRequest(
  producer: Producer,
  topic: string,
  key: string,
  value: Buffer,
  logger: Logger
): Promise<void> {
  await publishKafkaTopicMessage(producer, topic, key, value, logger, {
    logMessage: "Summary request published",
  });
}

export async function disconnectKafkaProducer(producer: Producer, logger: Logger): Promise<void> {
  await producer.disconnect();
  logger.info("Kafka producer disconnected");
}
