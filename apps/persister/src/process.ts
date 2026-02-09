import { Server } from "node:http";
import { setTimeout as sleep } from "node:timers/promises";
import type { EachBatchPayload } from "kafkajs";
import type { Redis } from "ioredis";
import type { PrismaClient } from "@rising-intelligence/db";
import { serializeError } from "@rising-intelligence/shared";
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

export async function processBatch(ctx: PersisterContext, payload: EachBatchPayload): Promise<void> {
  const { batch, isRunning, isStale, pause, resolveOffset, commitOffsetsIfNecessary, heartbeat } = payload;

  if (!isRunning() || isStale()) {
    return;
  }

  if (ctx.circuitBreaker.isOpen()) {
    const waitMs = ctx.circuitBreaker.timeUntilClose();
    ctx.healthContext.circuitOpen = true;

    const resume = pause();
    ctx.logger.warn(
      {
        waitMs,
        kafkaTopic: batch.topic,
        partition: batch.partition,
      },
      "Postgres circuit open; pausing partition"
    );

    try {
      await waitWithHeartbeats(waitMs, heartbeat);
    } finally {
      resume();
    }

    return;
  }

  observeBatchSize(ctx.healthContext, batch.messages.length);
  let messagesSinceHeartbeat = 0;
  const maybeHeartbeat = async () => {
    messagesSinceHeartbeat += 1;
    if (messagesSinceHeartbeat < LOOP_HEARTBEAT_INTERVAL_MESSAGES) {
      return;
    }
    await heartbeat();
    messagesSinceHeartbeat = 0;
  };

  const events: ParsedRawEvent[] = [];
  for (const message of batch.messages) {
    if (!message.value) {
      incrementEventsSkipped(ctx.healthContext, "malformed");
      incrementError(ctx.healthContext, "parse_error");
      ctx.logger.warn(
        {
          kafkaTopic: batch.topic,
          partition: batch.partition,
          offset: message.offset,
        },
        "Skipping message with empty value"
      );
      await maybeHeartbeat();
      continue;
    }

    try {
      const event = deserializeRawEvent(message.value);
      events.push(event);
    } catch (error) {
      incrementEventsSkipped(ctx.healthContext, "malformed");
      incrementError(ctx.healthContext, "parse_error");
      ctx.logger.warn(
        {
          kafkaTopic: batch.topic,
          partition: batch.partition,
          offset: message.offset,
          error: serializeError(error),
        },
        "Failed to deserialize event"
      );
    }
    await maybeHeartbeat();
  }

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
        kafkaTopic: batch.topic,
        partition: batch.partition,
        batchSize: batch.messages.length,
        openedCircuit: opened,
        error: serializeError(error),
      },
      "Failed to persist Kafka batch"
    );

    throw error;
  }

  for (const message of batch.messages) {
    resolveOffset(message.offset);
    await maybeHeartbeat();
  }

  await commitOffsetsIfNecessary();
  await heartbeat();

  const lastMessage = batch.messages.at(-1);
  if (!lastMessage) {
    return;
  }

  const currentOffset = toBigInt(lastMessage.offset, 0n) + 1n;
  const latestOffset = toBigInt(batch.highWatermark, currentOffset);
  const lag = latestOffset > currentOffset ? latestOffset - currentOffset : 0n;

  try {
    const wroteLag = await updateLag(ctx, payload, currentOffset, latestOffset, lag);
    if (wroteLag) {
      ctx.healthContext.postgresHealthy = true;
    }
  } catch (error) {
    incrementError(ctx.healthContext, "postgres_error");
    ctx.healthContext.postgresHealthy = false;
    ctx.logger.warn(
      {
        kafkaTopic: batch.topic,
        partition: batch.partition,
        error: serializeError(error),
      },
      "Failed to update consumer lag"
    );
  }
}
