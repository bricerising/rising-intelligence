import { Consumer, Kafka, logLevel } from "kafkajs";
import type { Logger } from "pino";
import { getConfig } from "../config.js";

export interface KafkaConsumerContext {
  kafka: Kafka;
  consumer: Consumer;
}

export async function createKafkaConsumer(logger: Logger): Promise<KafkaConsumerContext> {
  const config = getConfig();
  const brokers = config.KAFKA_BROKERS
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (brokers.length === 0) {
    throw new Error("KAFKA_BROKERS must include at least one broker");
  }

  const kafka = new Kafka({
    clientId: config.KAFKA_CLIENT_ID,
    brokers,
    logLevel: logLevel.WARN,
    logCreator: () => {
      return ({ level, log }) => {
        if (level === logLevel.NOTHING) {
          return;
        }

        const { message, ...extra } = log;
        const pinoLevel = {
          [logLevel.ERROR]: "error",
          [logLevel.WARN]: "warn",
          [logLevel.INFO]: "info",
          [logLevel.DEBUG]: "debug",
        }[level] as "error" | "warn" | "info" | "debug" | undefined;

        if (!pinoLevel) {
          logger.debug({ level, ...extra, kafkajs: true }, message);
          return;
        }

        logger[pinoLevel]({ ...extra, kafkajs: true }, message);
      };
    },
  });

  const consumer = kafka.consumer({
    groupId: config.KAFKA_CONSUMER_GROUP,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    allowAutoTopicCreation: false,
  });

  await consumer.connect();
  logger.info({ brokers, groupId: config.KAFKA_CONSUMER_GROUP }, "Kafka consumer connected");

  return {
    kafka,
    consumer,
  };
}

export async function disconnectKafkaConsumer(
  consumer: Consumer,
  logger: Logger
): Promise<void> {
  await consumer.disconnect();
  logger.info("Kafka consumer disconnected");
}
