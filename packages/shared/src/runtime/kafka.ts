import { Kafka, logLevel, type Consumer, type Producer } from "kafkajs";

type LoggerMethod = (bindings: Record<string, unknown>, message?: string) => void;

export interface KafkaLogger {
  error: LoggerMethod;
  warn: LoggerMethod;
  info: LoggerMethod;
  debug: LoggerMethod;
}

type PinoKafkaLevel = "error" | "warn" | "info" | "debug";

interface KafkaLogEntry {
  level: number;
  log: {
    message: string;
    [key: string]: unknown;
  };
}

function getPinoLevel(level: number): PinoKafkaLevel | null {
  if (level === logLevel.ERROR) {
    return "error";
  }
  if (level === logLevel.WARN) {
    return "warn";
  }
  if (level === logLevel.INFO) {
    return "info";
  }
  if (level === logLevel.DEBUG) {
    return "debug";
  }

  return null;
}

function createKafkaLogCreator(logger: KafkaLogger) {
  return () => ({ level, log }: KafkaLogEntry) => {
    if (level === logLevel.NOTHING) {
      return;
    }

    const { message, ...extra } = log;
    const pinoLevel = getPinoLevel(level);
    if (!pinoLevel) {
      logger.debug({ level, ...extra, kafkajs: true }, message);
      return;
    }

    if (pinoLevel === "error") {
      logger.error({ ...extra, kafkajs: true }, message);
      return;
    }
    if (pinoLevel === "warn") {
      logger.warn({ ...extra, kafkajs: true }, message);
      return;
    }
    if (pinoLevel === "info") {
      logger.info({ ...extra, kafkajs: true }, message);
      return;
    }

    logger.debug({ ...extra, kafkajs: true }, message);
  };
}

export function parseKafkaBrokers(rawBrokers: string | string[]): string[] {
  const brokers = (Array.isArray(rawBrokers) ? rawBrokers : rawBrokers.split(","))
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (brokers.length === 0) {
    throw new Error("KAFKA_BROKERS must include at least one broker");
  }

  return brokers;
}

interface KafkaClientOptions {
  clientId: string;
  brokers: string | string[];
  logger: KafkaLogger;
}

function createKafkaClient(options: KafkaClientOptions): { kafka: Kafka; brokers: string[] } {
  const brokers = parseKafkaBrokers(options.brokers);
  const kafka = new Kafka({
    clientId: options.clientId,
    brokers,
    logLevel: logLevel.WARN,
    logCreator: createKafkaLogCreator(options.logger),
  });

  return { kafka, brokers };
}

export interface KafkaConsumerConnection {
  kafka: Kafka;
  consumer: Consumer;
  brokers: string[];
}

export interface KafkaProducerConnection {
  kafka: Kafka;
  producer: Producer;
  brokers: string[];
}

export interface ConnectKafkaConsumerOptions {
  brokers: string | string[];
  clientId: string;
  groupId: string;
  logger: KafkaLogger;
  sessionTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  allowAutoTopicCreation?: boolean;
}

export interface ConnectKafkaProducerOptions {
  brokers: string | string[];
  clientId: string;
  clientIdSuffix?: string;
  logger: KafkaLogger;
  allowAutoTopicCreation?: boolean;
  transactionTimeoutMs?: number;
}

export async function connectKafkaConsumer(
  options: ConnectKafkaConsumerOptions
): Promise<KafkaConsumerConnection> {
  const { kafka, brokers } = createKafkaClient({
    clientId: options.clientId,
    brokers: options.brokers,
    logger: options.logger,
  });
  const consumer = kafka.consumer({
    groupId: options.groupId,
    sessionTimeout: options.sessionTimeoutMs ?? 30_000,
    heartbeatInterval: options.heartbeatIntervalMs ?? 3_000,
    allowAutoTopicCreation: options.allowAutoTopicCreation ?? false,
  });

  await consumer.connect();

  return {
    kafka,
    consumer,
    brokers,
  };
}

export async function connectKafkaProducer(
  options: ConnectKafkaProducerOptions
): Promise<KafkaProducerConnection> {
  const clientId = `${options.clientId}${options.clientIdSuffix ?? ""}`;
  const { kafka, brokers } = createKafkaClient({
    clientId,
    brokers: options.brokers,
    logger: options.logger,
  });
  const producer = kafka.producer({
    allowAutoTopicCreation: options.allowAutoTopicCreation ?? false,
    transactionTimeout: options.transactionTimeoutMs ?? 30_000,
  });

  await producer.connect();

  return {
    kafka,
    producer,
    brokers,
  };
}
