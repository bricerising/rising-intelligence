import { CompressionTypes, Kafka, Producer } from "kafkajs";
import { connectKafkaProducer } from "@rising-intelligence/shared";
import type { Logger } from "pino";
import { getConfig } from "../config.js";

export interface KafkaProducerContext {
  kafka: Kafka;
  producer: Producer;
}

export async function createKafkaProducer(logger: Logger): Promise<KafkaProducerContext> {
  const config = getConfig();
  const connection = await connectKafkaProducer({
    brokers: config.KAFKA_BROKERS,
    clientId: config.KAFKA_CLIENT_ID,
    clientIdSuffix: "-producer",
    logger,
    allowAutoTopicCreation: false,
  });
  logger.info({ brokers: connection.brokers }, "Kafka producer connected");

  return {
    kafka: connection.kafka,
    producer: connection.producer,
  };
}

export async function publishBriefResult(
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
  logger.debug({ topic, key }, "Brief result published");
}

export async function disconnectKafkaProducer(producer: Producer, logger: Logger): Promise<void> {
  await producer.disconnect();
  logger.info("Kafka producer disconnected");
}
