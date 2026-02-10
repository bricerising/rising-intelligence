import type { Consumer, Kafka } from "kafkajs";
import { createKafkaConsumerFactory } from "@rising-intelligence/shared";
import type { Logger } from "pino";
import { getConfig } from "../config.js";

export interface KafkaConsumerContext {
  kafka: Kafka;
  consumer: Consumer;
}

const createConsumer = createKafkaConsumerFactory({
  getConfig,
  allowAutoTopicCreation: false,
  onConnected: ({ connection, config, logger }) => {
    logger.info(
      { brokers: connection.brokers, groupId: config.KAFKA_CONSUMER_GROUP },
      "Kafka consumer connected"
    );
  },
});

export async function createKafkaConsumer(logger: Logger): Promise<KafkaConsumerContext> {
  return createConsumer(logger);
}

export async function disconnectKafkaConsumer(consumer: Consumer, logger: Logger): Promise<void> {
  await consumer.disconnect();
  logger.info("Kafka consumer disconnected");
}
