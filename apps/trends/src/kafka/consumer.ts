import { Consumer, Kafka } from "kafkajs";
import { connectKafkaConsumer } from "@rising-intelligence/shared";
import type { Logger } from "pino";
import { getConfig } from "../config.js";

export interface KafkaConsumerContext {
  kafka: Kafka;
  consumer: Consumer;
}

export async function createKafkaConsumer(logger: Logger): Promise<KafkaConsumerContext> {
  const config = getConfig();
  const connection = await connectKafkaConsumer({
    brokers: config.KAFKA_BROKERS,
    clientId: config.KAFKA_CLIENT_ID,
    groupId: config.KAFKA_CONSUMER_GROUP,
    logger,
    allowAutoTopicCreation: false,
  });
  logger.info(
    { brokers: connection.brokers, groupId: config.KAFKA_CONSUMER_GROUP },
    "Kafka consumer connected"
  );

  return {
    kafka: connection.kafka,
    consumer: connection.consumer,
  };
}

export async function disconnectKafkaConsumer(consumer: Consumer, logger: Logger): Promise<void> {
  await consumer.disconnect();
  logger.info("Kafka consumer disconnected");
}
