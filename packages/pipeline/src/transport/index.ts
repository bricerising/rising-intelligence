import {
  CompressionTypes,
  Kafka,
  logLevel,
  type Consumer,
  type EachBatchPayload,
  type Producer,
} from "kafkajs";
export * from "./topic-router.js";

type LoggerMethod = (bindings: Record<string, unknown>, message?: string) => void;

export interface PipelineLogger {
  error: LoggerMethod;
  warn: LoggerMethod;
  info: LoggerMethod;
  debug: LoggerMethod;
}

export interface ProducerConnection {
  publish(topic: string, key: string, value: Buffer): Promise<void>;
  publishBatch(topic: string, messages: Array<{ key: string; value: Buffer }>): Promise<boolean>;
  disconnect(): Promise<void>;
}

export interface CreateProducerConnectionOptions {
  brokers: string | string[];
  clientId: string;
  clientIdSuffix?: string;
  logger: PipelineLogger;
}

export interface TopicPublisher<TPayload> {
  publish(key: string, payload: TPayload): Promise<void>;
}

export interface KeyedTopicPublisher<TPayload> {
  publish(payload: TPayload): Promise<void>;
}

export interface CreateTopicPublisherOptions<TPayload> {
  connection: ProducerConnection;
  topic: string;
  serialize?: (payload: TPayload) => Buffer;
}

export interface CreateKeyedTopicPublisherOptions<TPayload>
  extends CreateTopicPublisherOptions<TPayload> {
  getKey(payload: TPayload): string;
}

export interface PipelineMessage {
  key: Buffer | null;
  value: Buffer | null;
  position: string;
  timestamp: string;
}

export interface BatchContext {
  topic: string;
  partition: number;
  highWatermark: string;
  isActive(): boolean;
  keepAlive(): Promise<void>;
  acknowledge(position: string): void;
  commit(): Promise<void>;
  pause(): () => void;
}

export interface BatchStrategy<TContext> {
  processBatch(
    ctx: TContext,
    batch: BatchContext,
    messages: readonly PipelineMessage[]
  ): Promise<void>;
}

export interface MessageContext {
  topic: string;
  partition: number;
  position: string;
  keepAlive(): Promise<void>;
}

export interface MessageStrategy<TContext, TMessage> {
  deserialize(value: Buffer): TMessage;
  onEmptyValue?(ctx: TContext, messageContext: MessageContext): Promise<void> | void;
  onDeserializeFailure?(
    ctx: TContext,
    messageContext: MessageContext,
    error: unknown
  ): Promise<void> | void;
  onMessage(
    ctx: TContext,
    messageContext: MessageContext,
    decoded: TMessage
  ): Promise<void> | void;
}

export interface CreateMessageBatchStrategyOptions<TContext, TMessage> {
  strategy: MessageStrategy<TContext, TMessage>;
  progressInterval?: number;
  acknowledge?: boolean;
  onBatchCompleted?(ctx: TContext): Promise<void> | void;
}

export interface ConsumeOptions<TContext> {
  topics: string | string[];
  ctx: TContext;
  strategy: BatchStrategy<TContext> | ReadonlyMap<string, BatchStrategy<TContext>>;
  fromBeginning?: boolean;
}

export interface ConsumerConnection {
  consume<TContext>(options: ConsumeOptions<TContext>): Promise<void>;
  disconnect(): Promise<void>;
}

export interface CreateConsumerConnectionOptions {
  brokers: string | string[];
  clientId: string;
  groupId: string;
  logger: PipelineLogger;
  sessionTimeoutMs?: number;
}

interface KafkaLogEntry {
  level: number;
  log: {
    message: string;
    [key: string]: unknown;
  };
}

type PipelineLogLevel = "error" | "warn" | "info" | "debug";

function getPipelineLogLevel(level: number): PipelineLogLevel | null {
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

function createKafkaLogCreator(logger: PipelineLogger) {
  return () => ({ level, log }: KafkaLogEntry) => {
    if (level === logLevel.NOTHING) {
      return;
    }

    const { message, ...extra } = log;
    const mappedLevel = getPipelineLogLevel(level);
    if (!mappedLevel) {
      logger.debug({ level, kafkajs: true, ...extra }, message);
      return;
    }

    if (mappedLevel === "error") {
      logger.error({ kafkajs: true, ...extra }, message);
      return;
    }
    if (mappedLevel === "warn") {
      logger.warn({ kafkajs: true, ...extra }, message);
      return;
    }
    if (mappedLevel === "info") {
      logger.info({ kafkajs: true, ...extra }, message);
      return;
    }

    logger.debug({ kafkajs: true, ...extra }, message);
  };
}

function parseKafkaBrokers(rawBrokers: string | string[]): string[] {
  const brokers = (Array.isArray(rawBrokers) ? rawBrokers : rawBrokers.split(","))
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (brokers.length === 0) {
    throw new Error("brokers must include at least one broker");
  }

  return brokers;
}

function createKafkaClient(
  brokers: string | string[],
  clientId: string,
  logger: PipelineLogger
): Kafka {
  return new Kafka({
    clientId,
    brokers: parseKafkaBrokers(brokers),
    logLevel: logLevel.WARN,
    logCreator: createKafkaLogCreator(logger),
  });
}

function serializeJsonPayload(payload: unknown): Buffer {
  return Buffer.from(JSON.stringify(payload), "utf-8");
}

export function createTopicPublisher<TPayload>(
  options: CreateTopicPublisherOptions<TPayload>
): TopicPublisher<TPayload> {
  const serialize = options.serialize ?? ((payload: TPayload) => serializeJsonPayload(payload));

  return {
    async publish(key: string, payload: TPayload): Promise<void> {
      await options.connection.publish(options.topic, key, serialize(payload));
    },
  };
}

export function createKeyedTopicPublisher<TPayload>(
  options: CreateKeyedTopicPublisherOptions<TPayload>
): KeyedTopicPublisher<TPayload> {
  const topicPublisher = createTopicPublisher(options);

  return {
    async publish(payload: TPayload): Promise<void> {
      await topicPublisher.publish(options.getKey(payload), payload);
    },
  };
}

export async function createProducerConnection(
  options: CreateProducerConnectionOptions
): Promise<ProducerConnection> {
  const clientId = `${options.clientId}${options.clientIdSuffix ?? ""}`;
  const kafka = createKafkaClient(options.brokers, clientId, options.logger);
  const producer = kafka.producer({
    allowAutoTopicCreation: false,
    transactionTimeout: 30_000,
  });
  await producer.connect();

  return {
    async publish(topic: string, key: string, value: Buffer): Promise<void> {
      await producer.send({
        topic,
        compression: CompressionTypes.GZIP,
        messages: [{ key, value }],
      });
      options.logger.debug({ topic, key }, "Published pipeline message");
    },
    async publishBatch(
      topic: string,
      messages: Array<{ key: string; value: Buffer }>
    ): Promise<boolean> {
      if (messages.length === 0) {
        return false;
      }

      await producer.send({
        topic,
        compression: CompressionTypes.GZIP,
        messages,
      });
      options.logger.debug({ topic, count: messages.length }, "Published pipeline batch");
      return true;
    },
    async disconnect(): Promise<void> {
      await producer.disconnect();
      options.logger.info({ clientId }, "Kafka producer disconnected");
    },
  };
}

function createBatchContext(payload: EachBatchPayload): BatchContext {
  return {
    topic: payload.batch.topic,
    partition: payload.batch.partition,
    highWatermark: payload.batch.highWatermark,
    isActive(): boolean {
      return payload.isRunning() && !payload.isStale();
    },
    async keepAlive(): Promise<void> {
      await payload.heartbeat();
    },
    acknowledge(position: string): void {
      payload.resolveOffset(position);
    },
    async commit(): Promise<void> {
      await payload.commitOffsetsIfNecessary();
    },
    pause(): () => void {
      return payload.pause();
    },
  };
}

function toPipelineMessages(payload: EachBatchPayload): PipelineMessage[] {
  return payload.batch.messages.map((message) => ({
    key: message.key,
    value: message.value,
    position: message.offset,
    timestamp: message.timestamp,
  }));
}

async function skipUnknownTopicBatch(payload: EachBatchPayload): Promise<void> {
  for (const message of payload.batch.messages) {
    payload.resolveOffset(message.offset);
  }
  await payload.commitOffsetsIfNecessary();
  await payload.heartbeat();
}

function resolveTopicStrategy<TContext>(
  strategy: BatchStrategy<TContext> | ReadonlyMap<string, BatchStrategy<TContext>>,
  topic: string
): BatchStrategy<TContext> | undefined {
  if ("processBatch" in strategy) {
    return strategy;
  }
  return strategy.get(topic);
}

export async function createConsumerConnection(
  options: CreateConsumerConnectionOptions
): Promise<ConsumerConnection> {
  const kafka = createKafkaClient(options.brokers, options.clientId, options.logger);
  const consumer = kafka.consumer({
    groupId: options.groupId,
    sessionTimeout: options.sessionTimeoutMs ?? 30_000,
    allowAutoTopicCreation: false,
  });
  await consumer.connect();

  return {
    async consume<TContext>(consumeOptions: ConsumeOptions<TContext>): Promise<void> {
      const topics = Array.isArray(consumeOptions.topics)
        ? consumeOptions.topics
        : [consumeOptions.topics];

      for (const topic of topics) {
        await consumer.subscribe({
          topic,
          fromBeginning: consumeOptions.fromBeginning ?? false,
        });
      }

      await consumer.run({
        autoCommit: true,
        eachBatchAutoResolve: false,
        eachBatch: async (payload: EachBatchPayload) => {
          const strategy = resolveTopicStrategy(consumeOptions.strategy, payload.batch.topic);
          if (!strategy) {
            options.logger.warn(
              { topic: payload.batch.topic, partition: payload.batch.partition },
              "Received batch for unexpected topic; skipping"
            );
            await skipUnknownTopicBatch(payload);
            return;
          }

          await strategy.processBatch(
            consumeOptions.ctx,
            createBatchContext(payload),
            toPipelineMessages(payload)
          );
        },
      });
    },
    async disconnect(): Promise<void> {
      await consumer.disconnect();
      options.logger.info({ groupId: options.groupId }, "Kafka consumer disconnected");
    },
  };
}

function assertPositiveInteger(value: number, fieldName: string): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${fieldName} must be a positive integer, received: ${value}`);
  }
}

async function runMessageCallback<T>(
  callback: (() => Promise<T> | T) | undefined
): Promise<void> {
  if (!callback) {
    return;
  }
  await callback();
}

export function createMessageBatchStrategy<TContext, TMessage>(
  options: CreateMessageBatchStrategyOptions<TContext, TMessage>
): BatchStrategy<TContext> {
  const progressInterval = options.progressInterval ?? 50;
  assertPositiveInteger(progressInterval, "progressInterval");
  const acknowledge = options.acknowledge ?? true;

  return {
    async processBatch(
      ctx: TContext,
      batch: BatchContext,
      messages: readonly PipelineMessage[]
    ): Promise<void> {
      if (!batch.isActive()) {
        return;
      }

      let completed = true;
      let handledCount = 0;

      for (const message of messages) {
        if (!batch.isActive()) {
          completed = false;
          break;
        }

        const messageContext: MessageContext = {
          topic: batch.topic,
          partition: batch.partition,
          position: message.position,
          keepAlive: async () => {
            await batch.keepAlive();
          },
        };

        if (!message.value) {
          await runMessageCallback(() => options.strategy.onEmptyValue?.(ctx, messageContext));
          if (acknowledge) {
            batch.acknowledge(message.position);
          }
          handledCount += 1;
          if (handledCount % progressInterval === 0) {
            await batch.keepAlive();
          }
          continue;
        }

        let decoded: TMessage;
        try {
          decoded = options.strategy.deserialize(message.value);
        } catch (error) {
          await runMessageCallback(() =>
            options.strategy.onDeserializeFailure?.(ctx, messageContext, error)
          );
          if (acknowledge) {
            batch.acknowledge(message.position);
          }
          handledCount += 1;
          if (handledCount % progressInterval === 0) {
            await batch.keepAlive();
          }
          continue;
        }

        await runMessageCallback(() => options.strategy.onMessage(ctx, messageContext, decoded));
        if (acknowledge) {
          batch.acknowledge(message.position);
        }

        handledCount += 1;
        if (handledCount % progressInterval === 0) {
          await batch.keepAlive();
        }
      }

      await batch.commit();

      if (!completed || !batch.isActive()) {
        return;
      }

      await batch.keepAlive();
      await runMessageCallback(() => options.onBatchCompleted?.(ctx));
    },
  };
}
