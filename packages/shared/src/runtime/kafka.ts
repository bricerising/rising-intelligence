import { CompressionTypes, Kafka, logLevel, type Consumer, type Producer } from "kafkajs";

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

export interface PublishKafkaMessageInput {
  topic: string;
  key: string;
  value: Buffer;
  logMessage: string;
  logContext?: Record<string, unknown>;
}

export interface PublishKafkaBatchInput {
  topic: string;
  messages: Array<{ key: string; value: Buffer }>;
  logMessage: string;
  logContext?: Record<string, unknown>;
}

export interface KafkaProducerProxy {
  publishMessage(input: PublishKafkaMessageInput): Promise<void>;
  publishBatch(input: PublishKafkaBatchInput): Promise<boolean>;
}

export interface CreateKafkaProducerProxyInput {
  producer: Producer;
  logger: KafkaLogger;
  compressionType?: CompressionTypes;
}

/**
 * Proxy around KafkaJS producer sends so services share one publish contract
 * (compression, payload shape, and logging) while keeping service-level facades.
 */
export function createKafkaProducerProxy(
  input: CreateKafkaProducerProxyInput
): KafkaProducerProxy {
  const compressionType = input.compressionType ?? CompressionTypes.GZIP;

  return {
    async publishMessage(message): Promise<void> {
      await input.producer.send({
        topic: message.topic,
        compression: compressionType,
        messages: [{ key: message.key, value: message.value }],
      });

      input.logger.debug(
        { topic: message.topic, key: message.key, ...message.logContext },
        message.logMessage
      );
    },

    async publishBatch(message): Promise<boolean> {
      if (message.messages.length === 0) {
        return false;
      }

      await input.producer.send({
        topic: message.topic,
        compression: compressionType,
        messages: message.messages,
      });

      input.logger.debug(
        {
          topic: message.topic,
          count: message.messages.length,
          ...message.logContext,
        },
        message.logMessage
      );

      return true;
    },
  };
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

export interface KafkaConsumerServiceConfig {
  KAFKA_BROKERS: string | string[];
  KAFKA_CLIENT_ID: string;
  KAFKA_CONSUMER_GROUP: string;
}

export interface KafkaProducerServiceConfig {
  KAFKA_BROKERS: string | string[];
  KAFKA_CLIENT_ID: string;
}

export interface KafkaConsumerConnectedHookArgs<TConfig extends KafkaConsumerServiceConfig> {
  config: TConfig;
  connection: KafkaConsumerConnection;
  logger: KafkaLogger;
}

export interface KafkaProducerConnectedHookArgs<TConfig extends KafkaProducerServiceConfig> {
  config: TConfig;
  connection: KafkaProducerConnection;
  logger: KafkaLogger;
}

export interface CreateKafkaConsumerFactoryOptions<TConfig extends KafkaConsumerServiceConfig> {
  getConfig: () => TConfig;
  sessionTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  allowAutoTopicCreation?: boolean;
  onConnected?: (args: KafkaConsumerConnectedHookArgs<TConfig>) => void;
}

export interface CreateKafkaProducerFactoryOptions<TConfig extends KafkaProducerServiceConfig> {
  getConfig: () => TConfig;
  clientIdSuffix?: string;
  allowAutoTopicCreation?: boolean;
  transactionTimeoutMs?: number;
  onConnected?: (args: KafkaProducerConnectedHookArgs<TConfig>) => void;
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

export function createKafkaConsumerFactory<TConfig extends KafkaConsumerServiceConfig>(
  options: CreateKafkaConsumerFactoryOptions<TConfig>
): (logger: KafkaLogger) => Promise<{ kafka: Kafka; consumer: Consumer }> {
  return async (logger) => {
    const config = options.getConfig();
    const connection = await connectKafkaConsumer({
      brokers: config.KAFKA_BROKERS,
      clientId: config.KAFKA_CLIENT_ID,
      groupId: config.KAFKA_CONSUMER_GROUP,
      logger,
      sessionTimeoutMs: options.sessionTimeoutMs,
      heartbeatIntervalMs: options.heartbeatIntervalMs,
      allowAutoTopicCreation: options.allowAutoTopicCreation,
    });

    options.onConnected?.({
      config,
      connection,
      logger,
    });

    return {
      kafka: connection.kafka,
      consumer: connection.consumer,
    };
  };
}

export function createKafkaProducerFactory<TConfig extends KafkaProducerServiceConfig>(
  options: CreateKafkaProducerFactoryOptions<TConfig>
): (logger: KafkaLogger) => Promise<{ kafka: Kafka; producer: Producer }> {
  return async (logger) => {
    const config = options.getConfig();
    const connection = await connectKafkaProducer({
      brokers: config.KAFKA_BROKERS,
      clientId: config.KAFKA_CLIENT_ID,
      clientIdSuffix: options.clientIdSuffix,
      logger,
      allowAutoTopicCreation: options.allowAutoTopicCreation,
      transactionTimeoutMs: options.transactionTimeoutMs,
    });

    options.onConnected?.({
      config,
      connection,
      logger,
    });

    return {
      kafka: connection.kafka,
      producer: connection.producer,
    };
  };
}
