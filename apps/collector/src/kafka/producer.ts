import { Kafka, Producer, CompressionTypes, logLevel } from "kafkajs";
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

export async function createKafkaProducer(logger: Logger): Promise<KafkaProducerContext> {
  const config = getConfig();
  const brokers = config.KAFKA_BROKERS.split(",").map((b) => b.trim());

  const kafka = new Kafka({
    clientId: config.KAFKA_CLIENT_ID,
    brokers,
    logLevel: logLevel.WARN,
    logCreator: () => {
      return ({ level, log }) => {
        const { message, ...extra } = log;
        const pinoLevel = {
          [logLevel.ERROR]: "error",
          [logLevel.WARN]: "warn",
          [logLevel.INFO]: "info",
          [logLevel.DEBUG]: "debug",
          [logLevel.NOTHING]: "silent",
        }[level] as "error" | "warn" | "info" | "debug" | "silent";
        logger[pinoLevel]({ ...extra, kafkajs: true }, message);
      };
    },
  });

  const producer = kafka.producer({
    allowAutoTopicCreation: true,
    transactionTimeout: 30000,
  });

  await producer.connect();
  logger.info({ brokers }, "Kafka producer connected");

  return { producer, kafka };
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
