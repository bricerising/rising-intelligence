import { Server } from "node:http";
import { setTimeout as sleep } from "node:timers/promises";
import type { EachBatchPayload } from "kafkajs";
import type { Redis } from "ioredis";
import type { PrismaClient } from "@rising-intelligence/db";
import {
  createKafkaBatchLifecycle,
  processKafkaBatchMessages,
  runAsyncChain,
  serializeError,
  type AsyncChainStep,
  type KafkaBatchLifecycle,
  type KafkaBatchMessageContext,
  type KafkaBatchMessageStrategy,
} from "@rising-intelligence/shared";
import type pino from "pino";
import type { Config } from "./config.js";
import type { KafkaConsumerContext } from "./kafka/consumer.js";
import {
  incrementError,
  incrementEventsProcessed,
  incrementEventsSkipped,
  observeBatchSize,
  observePostgresWriteDuration,
  observeRedisWriteDuration,
  setConsumerLag,
  type HealthContext,
} from "./health.js";
import { deserializeRawEvent } from "./deserialize.js";
import { persistBatch, upsertConsumerLag } from "./persist.js";
import { markEventsSeen } from "./redis.js";
import type { PostgresCircuitBreaker } from "./circuit-breaker.js";
import type { ParsedRawEvent } from "./types.js";
import { nowSeconds, toBigInt } from "./utils.js";

const CIRCUIT_PAUSE_HEARTBEAT_INTERVAL_MS = 2000;
const LOOP_HEARTBEAT_INTERVAL_MESSAGES = 50;

class RedisWriteFailure extends Error {
  readonly cause: unknown;

  constructor(cause: unknown) {
    super("Failed to write seen keys to Redis");
    this.name = "RedisWriteFailure";
    this.cause = cause;
  }
}

type BatchMessageCollectorStrategy<TMessage> = Pick<
  KafkaBatchMessageStrategy<PersisterContext, TMessage>,
  "deserialize" | "onEmptyValue" | "onDeserializeFailure"
>;

const RAW_EVENT_BATCH_STRATEGY: BatchMessageCollectorStrategy<ParsedRawEvent> = {
  deserialize: deserializeRawEvent,
  onEmptyValue(ctx, messageContext): void {
    incrementEventsSkipped(ctx.healthContext, "malformed");
    incrementError(ctx.healthContext, "parse_error");
    ctx.logger.warn(messageContext, "Skipping message with empty value");
  },
  onDeserializeFailure(ctx, messageContext, error): void {
    incrementEventsSkipped(ctx.healthContext, "malformed");
    incrementError(ctx.healthContext, "parse_error");
    ctx.logger.warn(
      {
        ...messageContext,
        error: serializeError(error),
      },
      "Failed to deserialize event"
    );
  },
};

async function waitWithHeartbeats(
  waitMs: number,
  heartbeat: () => Promise<void>
): Promise<void> {
  let remainingMs = waitMs;

  while (remainingMs > 0) {
    const sleepMs = Math.min(remainingMs, CIRCUIT_PAUSE_HEARTBEAT_INTERVAL_MS);
    await sleep(sleepMs);
    remainingMs -= sleepMs;
    await heartbeat();
  }
}

async function collectMessagesWithStrategy<TMessage>(
  ctx: PersisterContext,
  payload: EachBatchPayload,
  batchLifecycle: KafkaBatchLifecycle,
  strategy: BatchMessageCollectorStrategy<TMessage>
): Promise<TMessage[] | null> {
  const events: TMessage[] = [];

  const { completed } = await processKafkaBatchMessages(
    ctx,
    payload,
    batchLifecycle,
    {
      ...strategy,
      onMessage(
        _ctx: PersisterContext,
        _messageContext: KafkaBatchMessageContext,
        decoded: TMessage
      ): void {
        events.push(decoded);
      },
    }
  );
  if (!completed) {
    return null;
  }

  return events;
}

async function pausePartitionWhenCircuitOpen(
  ctx: PersisterContext,
  payload: EachBatchPayload
): Promise<boolean> {
  if (!ctx.circuitBreaker.isOpen()) {
    return false;
  }

  const waitMs = ctx.circuitBreaker.timeUntilClose();
  ctx.healthContext.circuitOpen = true;

  const resume = payload.pause();
  ctx.logger.warn(
    {
      waitMs,
      kafkaTopic: payload.batch.topic,
      partition: payload.batch.partition,
    },
    "Postgres circuit open; pausing partition"
  );

  try {
    await waitWithHeartbeats(waitMs, payload.heartbeat);
  } finally {
    resume();
  }

  return true;
}

async function persistEventsWithCircuitHandling(
  ctx: PersisterContext,
  payload: EachBatchPayload,
  events: ParsedRawEvent[]
): Promise<void> {
  try {
    if (events.length > 0) {
      await persistAndMarkSeen(ctx, events);
      ctx.healthContext.lastEventAt = new Date();
      ctx.circuitBreaker.recordSuccess();
      ctx.healthContext.postgresHealthy = true;
    }

    ctx.healthContext.circuitOpen = ctx.circuitBreaker.isOpen();
  } catch (error) {
    if (error instanceof RedisWriteFailure) {
      // Postgres succeeded before Redis failed, so keep the circuit closed.
      ctx.circuitBreaker.recordSuccess();
      ctx.healthContext.postgresHealthy = true;
      ctx.healthContext.redisHealthy = false;
      ctx.healthContext.circuitOpen = ctx.circuitBreaker.isOpen();
      throw error;
    }

    const opened = ctx.circuitBreaker.recordFailure();
    ctx.healthContext.postgresHealthy = false;
    ctx.healthContext.circuitOpen = ctx.circuitBreaker.isOpen();
    incrementError(ctx.healthContext, "postgres_error");

    ctx.logger.error(
      {
        kafkaTopic: payload.batch.topic,
        partition: payload.batch.partition,
        batchSize: payload.batch.messages.length,
        openedCircuit: opened,
        error: serializeError(error),
      },
      "Failed to persist Kafka batch"
    );

    throw error;
  }
}

export interface PersisterContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
  healthServer: Server;
  prisma: PrismaClient;
  redis: Redis;
  kafkaContext: KafkaConsumerContext;
  circuitBreaker: PostgresCircuitBreaker;
  lagWriteTimestamps: Map<string, number>;
}

export async function persistAndMarkSeen(
  ctx: PersisterContext,
  events: ParsedRawEvent[]
): Promise<void> {
  const postgresStart = Date.now();
  const result = await persistBatch(ctx.prisma, events);
  observePostgresWriteDuration(ctx.healthContext, nowSeconds(postgresStart));

  for (const [source, insertedCount] of result.insertedBySource) {
    incrementEventsProcessed(ctx.healthContext, source, insertedCount);
  }

  if (result.duplicates > 0) {
    incrementEventsSkipped(ctx.healthContext, "duplicate", result.duplicates);
  }

  const redisStart = Date.now();
  try {
    await markEventsSeen(ctx.redis, events, ctx.config.SEEN_TTL_SECONDS);
    observeRedisWriteDuration(ctx.healthContext, nowSeconds(redisStart));
    ctx.healthContext.redisHealthy = true;
  } catch (error) {
    incrementError(ctx.healthContext, "redis_error");
    ctx.healthContext.redisHealthy = false;
    ctx.logger.error(
      { error: serializeError(error) },
      "Failed to write seen keys to Redis"
    );
    throw new RedisWriteFailure(error);
  }
}

export async function updateLag(
  ctx: PersisterContext,
  payload: EachBatchPayload,
  currentOffset: bigint,
  latestOffset: bigint,
  lag: bigint
): Promise<boolean> {
  setConsumerLag(ctx.healthContext, payload.batch.partition, lag);

  const partitionKey = `${payload.batch.topic}:${payload.batch.partition}`;
  const lastWrite = ctx.lagWriteTimestamps.get(partitionKey) ?? 0;
  const now = Date.now();

  if (now - lastWrite < ctx.config.CONSUMER_LAG_UPDATE_INTERVAL_MS) {
    return false;
  }

  await upsertConsumerLag(ctx.prisma, {
    consumerGroup: ctx.config.KAFKA_CONSUMER_GROUP,
    topic: payload.batch.topic,
    partition: payload.batch.partition,
    currentOffset,
    latestOffset,
    lagMessages: lag,
    observedAt: new Date(now),
  });

  ctx.lagWriteTimestamps.set(partitionKey, now);
  return true;
}

interface ProcessBatchState {
  batchLifecycle: KafkaBatchLifecycle;
  events: ParsedRawEvent[] | null;
}

interface ProcessBatchExecutionContext {
  ctx: PersisterContext;
  payload: EachBatchPayload;
  state: ProcessBatchState;
}

type ProcessBatchStep = AsyncChainStep<ProcessBatchExecutionContext, void>;

function createProcessBatchState(payload: EachBatchPayload): ProcessBatchState {
  return {
    batchLifecycle: createKafkaBatchLifecycle(
      {
        isRunning: payload.isRunning,
        isStale: payload.isStale,
        heartbeat: payload.heartbeat,
      },
      LOOP_HEARTBEAT_INTERVAL_MESSAGES
    ),
    events: null,
  };
}

function createBatchLifecycleGateStep(): ProcessBatchStep {
  return {
    name: "batch-lifecycle-gate",
    async execute({ state }, next): Promise<void> {
      if (!state.batchLifecycle.shouldContinue()) {
        return;
      }

      await next();
    },
  };
}

function createCircuitPauseStep(): ProcessBatchStep {
  return {
    name: "circuit-breaker-pause",
    async execute({ ctx, payload }, next): Promise<void> {
      if (await pausePartitionWhenCircuitOpen(ctx, payload)) {
        return;
      }

      await next();
    },
  };
}

function createObserveBatchSizeStep(): ProcessBatchStep {
  return {
    name: "observe-batch-size",
    async execute({ ctx, payload }, next): Promise<void> {
      observeBatchSize(ctx.healthContext, payload.batch.messages.length);
      await next();
    },
  };
}

function createCollectMessagesStep(): ProcessBatchStep {
  return {
    name: "collect-messages",
    async execute({ ctx, payload, state }, next): Promise<void> {
      const events = await collectMessagesWithStrategy(
        ctx,
        payload,
        state.batchLifecycle,
        RAW_EVENT_BATCH_STRATEGY
      );
      if (!events) {
        return;
      }

      state.events = events;
      await next();
    },
  };
}

function createPersistEventsStep(): ProcessBatchStep {
  return {
    name: "persist-events",
    async execute({ ctx, payload, state }, next): Promise<void> {
      if (state.events === null) {
        throw new Error("Persister process pipeline reached persistence without collected events");
      }

      await persistEventsWithCircuitHandling(ctx, payload, state.events);
      await next();
    },
  };
}

function createResolveOffsetsStep(): ProcessBatchStep {
  return {
    name: "resolve-offsets",
    async execute({ payload }, next): Promise<void> {
      for (const message of payload.batch.messages) {
        payload.resolveOffset(message.offset);
      }
      await next();
    },
  };
}

function createCommitAndHeartbeatStep(): ProcessBatchStep {
  return {
    name: "commit-and-heartbeat",
    async execute({ payload, state }, next): Promise<void> {
      await payload.commitOffsetsIfNecessary();
      if (!state.batchLifecycle.shouldContinue()) {
        return;
      }

      await state.batchLifecycle.flushHeartbeat();
      await next();
    },
  };
}

function createLagUpdateStep(): ProcessBatchStep {
  return {
    name: "update-consumer-lag",
    async execute({ ctx, payload }, next): Promise<void> {
      const lastMessage = payload.batch.messages.at(-1);
      if (!lastMessage) {
        await next();
        return;
      }

      const currentOffset = toBigInt(lastMessage.offset, 0n) + 1n;
      const latestOffset = toBigInt(payload.batch.highWatermark, currentOffset);
      const lag = latestOffset > currentOffset ? latestOffset - currentOffset : 0n;

      try {
        const wroteLag = await updateLag(
          ctx,
          payload,
          currentOffset,
          latestOffset,
          lag
        );
        if (wroteLag) {
          ctx.healthContext.postgresHealthy = true;
        }
      } catch (error) {
        incrementError(ctx.healthContext, "postgres_error");
        ctx.healthContext.postgresHealthy = false;
        ctx.logger.warn(
          {
            kafkaTopic: payload.batch.topic,
            partition: payload.batch.partition,
            error: serializeError(error),
          },
          "Failed to update consumer lag"
        );
      }

      await next();
    },
  };
}

const PROCESS_BATCH_STEPS: ReadonlyArray<ProcessBatchStep> = [
  createBatchLifecycleGateStep(),
  createCircuitPauseStep(),
  createObserveBatchSizeStep(),
  createCollectMessagesStep(),
  createPersistEventsStep(),
  createResolveOffsetsStep(),
  createCommitAndHeartbeatStep(),
  createLagUpdateStep(),
];

async function runProcessBatchPipeline(
  executionContext: ProcessBatchExecutionContext
): Promise<void> {
  await runAsyncChain(PROCESS_BATCH_STEPS, executionContext, {
    onEnd() {
      return;
    },
    duplicateNextError(stepName) {
      return new Error(
        `Persister process pipeline step "${stepName}" called next() multiple times`
      );
    },
  });
}

export async function processBatch(
  ctx: PersisterContext,
  payload: EachBatchPayload
): Promise<void> {
  await runProcessBatchPipeline({
    ctx,
    payload,
    state: createProcessBatchState(payload),
  });
}
