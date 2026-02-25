import type { EachBatchPayload } from "kafkajs";

export interface KafkaBatchLifecycle {
  shouldContinue(): boolean;
  onMessageHandled(): Promise<void>;
  flushHeartbeat(): Promise<void>;
}

export interface KafkaBatchMessageContext {
  kafkaTopic: string;
  partition: number;
  offset: string;
}

export interface KafkaBatchMessageStrategy<TContext, TMessage> {
  deserialize(value: Buffer): TMessage;
  onEmptyValue?(
    ctx: TContext,
    messageContext: KafkaBatchMessageContext
  ): Promise<void> | void;
  onDeserializeFailure?(
    ctx: TContext,
    messageContext: KafkaBatchMessageContext,
    error: unknown
  ): Promise<void> | void;
  onMessage(
    ctx: TContext,
    messageContext: KafkaBatchMessageContext,
    decoded: TMessage
  ): Promise<void> | void;
}

export interface ProcessKafkaBatchMessagesOptions {
  resolveOffsets?: boolean;
}

export interface ProcessKafkaBatchMessagesResult {
  completed: boolean;
}

export type KafkaMessageBatchPayload = Pick<
  EachBatchPayload,
  | "batch"
  | "resolveOffset"
  | "isRunning"
  | "isStale"
  | "heartbeat"
  | "commitOffsetsIfNecessary"
>;

export interface RunKafkaMessageBatchInput<TContext, TMessage> {
  ctx: TContext;
  payload: KafkaMessageBatchPayload;
  heartbeatIntervalMessages: number;
  strategy: KafkaBatchMessageStrategy<TContext, TMessage>;
  resolveOffsets?: boolean;
  onCompletedBatch?(
    ctx: TContext,
    payload: KafkaMessageBatchPayload
  ): Promise<void> | void;
}

/**
 * Proxy around KafkaJS batch lifecycle signals so consumers share consistent
 * heartbeat cadence and stale/running checks.
 */
export function createKafkaBatchLifecycle(
  input: Pick<EachBatchPayload, "isRunning" | "isStale" | "heartbeat">,
  heartbeatIntervalMessages: number
): KafkaBatchLifecycle {
  if (!Number.isInteger(heartbeatIntervalMessages) || heartbeatIntervalMessages <= 0) {
    throw new Error(
      `heartbeatIntervalMessages must be a positive integer, received: ${heartbeatIntervalMessages}`
    );
  }

  let messagesSinceHeartbeat = 0;

  return {
    shouldContinue(): boolean {
      return input.isRunning() && !input.isStale();
    },
    async onMessageHandled(): Promise<void> {
      messagesSinceHeartbeat += 1;
      if (messagesSinceHeartbeat < heartbeatIntervalMessages) {
        return;
      }

      await input.heartbeat();
      messagesSinceHeartbeat = 0;
    },
    async flushHeartbeat(): Promise<void> {
      await input.heartbeat();
      messagesSinceHeartbeat = 0;
    },
  };
}

export async function processKafkaBatchMessages<TContext, TMessage>(
  ctx: TContext,
  payload: Pick<EachBatchPayload, "batch" | "resolveOffset">,
  batchLifecycle: KafkaBatchLifecycle,
  strategy: KafkaBatchMessageStrategy<TContext, TMessage>,
  options: ProcessKafkaBatchMessagesOptions = {}
): Promise<ProcessKafkaBatchMessagesResult> {
  const resolveOffsets = options.resolveOffsets ?? false;
  let completed = true;

  for (const message of payload.batch.messages) {
    if (!batchLifecycle.shouldContinue()) {
      completed = false;
      break;
    }

    const messageContext: KafkaBatchMessageContext = {
      kafkaTopic: payload.batch.topic,
      partition: payload.batch.partition,
      offset: message.offset,
    };

    if (!message.value) {
      await strategy.onEmptyValue?.(ctx, messageContext);
      if (resolveOffsets) {
        payload.resolveOffset(message.offset);
      }
      await batchLifecycle.onMessageHandled();
      continue;
    }

    let decoded: TMessage;
    try {
      decoded = strategy.deserialize(message.value);
    } catch (error) {
      await strategy.onDeserializeFailure?.(ctx, messageContext, error);
      if (resolveOffsets) {
        payload.resolveOffset(message.offset);
      }
      await batchLifecycle.onMessageHandled();
      continue;
    }

    await strategy.onMessage(ctx, messageContext, decoded);
    if (resolveOffsets) {
      payload.resolveOffset(message.offset);
    }
    await batchLifecycle.onMessageHandled();
  }

  if (!batchLifecycle.shouldContinue()) {
    completed = false;
  }

  return { completed };
}

/**
 * Template-method runner for common Kafka batch flow:
 * gate by lifecycle -> process messages -> commit offsets -> flush heartbeat -> optional completion hook.
 */
export async function runKafkaMessageBatch<TContext, TMessage>(
  input: RunKafkaMessageBatchInput<TContext, TMessage>
): Promise<ProcessKafkaBatchMessagesResult> {
  const { ctx, payload, heartbeatIntervalMessages, strategy } = input;
  const batchLifecycle = createKafkaBatchLifecycle(
    {
      isRunning: payload.isRunning,
      isStale: payload.isStale,
      heartbeat: payload.heartbeat,
    },
    heartbeatIntervalMessages
  );

  if (!batchLifecycle.shouldContinue()) {
    return { completed: false };
  }

  const result = await processKafkaBatchMessages(
    ctx,
    payload,
    batchLifecycle,
    strategy,
    {
      resolveOffsets: input.resolveOffsets ?? false,
    }
  );

  await payload.commitOffsetsIfNecessary();

  if (!result.completed) {
    return result;
  }

  await batchLifecycle.flushHeartbeat();
  await input.onCompletedBatch?.(ctx, payload);

  return result;
}
