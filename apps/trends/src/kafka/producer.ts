import { CompressionTypes, Kafka, Producer, logLevel } from "kafkajs";
import type { Logger } from "pino";
import { getConfig } from "../config.js";

export interface KafkaProducerContext {
  kafka: Kafka;
  producer: Producer;
}

export async function createKafkaProducer(logger: Logger): Promise<KafkaProducerContext> {
  const config = getConfig();
  const brokers = config.KAFKA_BROKERS
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (brokers.length === 0) {
    throw new Error("KAFKA_BROKERS must include at least one broker");
  }

  const kafka = new Kafka({
    clientId: `${config.KAFKA_CLIENT_ID}-producer`,
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

  const producer = kafka.producer({
    allowAutoTopicCreation: false,
    transactionTimeout: 30000,
  });

  await producer.connect();
  logger.info({ brokers }, "Kafka producer connected");

  return {
    kafka,
    producer,
  };
}

export async function publishSnapshot(
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
  logger.debug({ topic, key }, "Trend snapshot published");
}

export async function disconnectKafkaProducer(producer: Producer, logger: Logger): Promise<void> {
  await producer.disconnect();
  logger.info("Kafka producer disconnected");
}
