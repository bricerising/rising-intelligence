import type { EachBatchPayload } from "kafkajs";
import type { Redis } from "ioredis";
import type { PrismaClient } from "@rising-intelligence/db";
import type pino from "pino";
import type { Config } from "./config.js";
import type { CompiledAllowlist } from "./allowlist.js";
import { filterTrackedTags } from "./allowlist.js";
import { deserializeRawEvent } from "./deserialize.js";
import {
  incrementDuplicatesSkipped,
  incrementError,
  incrementEventsProcessed,
  setConsumerLag,
  type HealthContext,
} from "./health.js";
import { applyEventToWindows } from "./redis.js";

function serializeError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function toBigInt(value: string | null | undefined, fallback = 0n): bigint {
  if (!value) {
    return fallback;
  }

  try {
    return BigInt(value);
  } catch {
    return fallback;
  }
}

async function upsertConsumerLag(
  prisma: PrismaClient,
  config: Config,
  topic: string,
  partition: number,
  currentOffset: bigint,
  latestOffset: bigint,
  lagMessages: bigint
): Promise<void> {
  await prisma.consumerLag.upsert({
    where: {
      consumerGroup_topic_partition: {
        consumerGroup: config.KAFKA_CONSUMER_GROUP,
        topic,
        partition,
      },
    },
    update: {
      currentOffset,
      latestOffset,
      lagMessages,
      updatedAt: new Date(),
    },
    create: {
      consumerGroup: config.KAFKA_CONSUMER_GROUP,
      topic,
      partition,
      currentOffset,
      latestOffset,
      lagMessages,
      updatedAt: new Date(),
    },
  });
}

export interface TrendsContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
  prisma: PrismaClient;
  redis: Redis;
  allowlist: CompiledAllowlist;
  lagWriteTimestamps: Map<string, number>;
}

export async function processBatch(
  ctx: TrendsContext,
  payload: EachBatchPayload
): Promise<void> {
  const { batch, isRunning, isStale, resolveOffset, commitOffsetsIfNecessary, heartbeat } = payload;

  if (!isRunning() || isStale()) {
    return;
  }

  for (const message of batch.messages) {
    if (!message.value) {
      incrementError(ctx.healthContext, "parse_error");
      ctx.logger.warn(
        {
          kafkaTopic: batch.topic,
          partition: batch.partition,
          offset: message.offset,
        },
        "Skipping message with empty value"
      );
      resolveOffset(message.offset);
      continue;
    }

    let event;
    try {
      event = deserializeRawEvent(message.value);
    } catch (error) {
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
      resolveOffset(message.offset);
      continue;
    }

    const trackedTopics = filterTrackedTags(event.tags, ctx.allowlist);
    if (trackedTopics.length === 0) {
      resolveOffset(message.offset);
      continue;
    }

    try {
      const result = await applyEventToWindows(
        ctx.redis,
        event,
        trackedTopics,
        ctx.config.WINDOWS,
        ctx.config.MAX_EVIDENCE_PER_TOPIC
      );

      if (result.duplicate) {
        incrementDuplicatesSkipped(ctx.healthContext);
      } else {
        incrementEventsProcessed(ctx.healthContext);
        ctx.healthContext.lastEventAt = new Date();
      }

      ctx.healthContext.redisHealthy = true;
    } catch (error) {
      incrementError(ctx.healthContext, "redis_error");
      ctx.healthContext.redisHealthy = false;
      ctx.logger.error(
        {
          kafkaTopic: batch.topic,
          partition: batch.partition,
          offset: message.offset,
          error: serializeError(error),
        },
        "Redis update failed while processing event"
      );
      throw error;
    }

    resolveOffset(message.offset);
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

  setConsumerLag(ctx.healthContext, batch.partition, lag);

  const partitionKey = `${batch.topic}:${batch.partition}`;
  const lastWriteAt = ctx.lagWriteTimestamps.get(partitionKey) ?? 0;
  const now = Date.now();

  if (now - lastWriteAt < ctx.config.CONSUMER_LAG_UPDATE_INTERVAL_MS) {
    return;
  }

  try {
    await upsertConsumerLag(
      ctx.prisma,
      ctx.config,
      batch.topic,
      batch.partition,
      currentOffset,
      latestOffset,
      lag
    );
    ctx.healthContext.postgresHealthy = true;
    ctx.lagWriteTimestamps.set(partitionKey, now);
  } catch (error) {
    ctx.healthContext.postgresHealthy = false;
    incrementError(ctx.healthContext, "postgres_error");
    ctx.logger.warn(
      {
        kafkaTopic: batch.topic,
        partition: batch.partition,
        error: serializeError(error),
      },
      "Failed to update consumer lag row"
    );
  }
}
