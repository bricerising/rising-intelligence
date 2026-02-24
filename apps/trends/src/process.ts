import type {
  BatchContext,
  BatchStrategy,
  MessageContext,
  MessageStrategy,
  PipelineMessage,
} from "@rising-intelligence/pipeline/transport";
import { createMessageBatchStrategy } from "@rising-intelligence/pipeline/transport";
import type { Redis } from "ioredis";
import { type PrismaClient, upsertConsumerLag } from "@rising-intelligence/db";
import { serializeError } from "@rising-intelligence/shared/errors";
import type pino from "pino";
import type { Config } from "./config.js";
import type { CompiledAllowlist } from "./allowlist.js";
import { filterTrackedTags } from "./allowlist.js";
import { deserializeCollectorHeartbeat } from "./collector-heartbeat-adapter.js";
import { recordCollectorHeartbeat } from "./collector-heartbeat-store.js";
import { deserializeRawEvent } from "./deserialize.js";
import type { ParsedRawEvent } from "./types.js";
import {
  type CollectorHeartbeatState,
  incrementDuplicatesSkipped,
  incrementError,
  incrementEventsProcessed,
  setConsumerLag,
  type HealthContext,
} from "./health.js";
import { applyEventToWindows } from "./redis.js";

const LOOP_HEARTBEAT_INTERVAL_MESSAGES = 50;

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

export interface TrendsContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
  prisma: PrismaClient;
  redis: Redis;
  allowlist: CompiledAllowlist;
  lagWriteTimestamps: Map<string, number>;
}

function onEmptyBatchValue(
  ctx: TrendsContext,
  messageContext: MessageContext,
  logMessage: string
): void {
  incrementError(ctx.healthContext, "parse_error");
  ctx.logger.warn(messageContext, logMessage);
}

function onBatchDeserializeFailure(
  ctx: TrendsContext,
  messageContext: MessageContext,
  error: unknown,
  logMessage: string
): void {
  incrementError(ctx.healthContext, "parse_error");
  ctx.logger.warn(
    {
      ...messageContext,
      error: serializeError(error),
    },
    logMessage
  );
}

const RAW_EVENT_MESSAGE_STRATEGY: MessageStrategy<TrendsContext, ParsedRawEvent> = {
  deserialize: deserializeRawEvent,
  onEmptyValue(ctx, messageContext): void {
    onEmptyBatchValue(ctx, messageContext, "Skipping message with empty value");
  },
  onDeserializeFailure(ctx, messageContext, error): void {
    onBatchDeserializeFailure(
      ctx,
      messageContext,
      error,
      "Failed to deserialize event"
    );
  },
  async onMessage(ctx, messageContext, event): Promise<void> {
    const trackedTopics = filterTrackedTags(event.tags, ctx.allowlist);
    if (trackedTopics.length === 0) {
      return;
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
          ...messageContext,
          error: serializeError(error),
        },
        "Redis update failed while processing event"
      );
      throw error;
    }
  },
};

const COLLECTOR_HEARTBEAT_MESSAGE_STRATEGY: MessageStrategy<
  TrendsContext,
  CollectorHeartbeatState
> = {
  deserialize: deserializeCollectorHeartbeat,
  onEmptyValue(ctx, messageContext): void {
    onEmptyBatchValue(
      ctx,
      messageContext,
      "Skipping collector heartbeat with empty value"
    );
  },
  onDeserializeFailure(ctx, messageContext, error): void {
    onBatchDeserializeFailure(
      ctx,
      messageContext,
      error,
      "Failed to deserialize collector heartbeat"
    );
  },
  async onMessage(ctx, messageContext, collectorHeartbeat): Promise<void> {
    const result = recordCollectorHeartbeat(
      ctx.healthContext.collectorHeartbeats,
      collectorHeartbeat
    );
    if (result === "ignored_stale") {
      ctx.logger.debug(
        {
          ...messageContext,
          source: collectorHeartbeat.source,
          incomingTimestamp: collectorHeartbeat.timestamp.toISOString(),
          currentTimestamp: ctx.healthContext.collectorHeartbeats
            .get(collectorHeartbeat.source)
            ?.timestamp.toISOString(),
        },
        "Ignoring stale collector heartbeat update"
      );
    }
  },
};

async function updateConsumerLag(
  ctx: TrendsContext,
  batch: BatchContext,
  messages: readonly PipelineMessage[]
): Promise<void> {
  const lastMessage = messages.at(-1);
  if (!lastMessage) {
    return;
  }

  const currentOffset = toBigInt(lastMessage.position, 0n) + 1n;
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
      {
        consumerGroup: ctx.config.KAFKA_CONSUMER_GROUP,
        topic: batch.topic,
        partition: batch.partition,
        currentOffset,
        latestOffset,
        lagMessages: lag,
        observedAt: new Date(now),
      }
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

const RAW_EVENT_BATCH_STRATEGY_BASE = createMessageBatchStrategy({
  strategy: RAW_EVENT_MESSAGE_STRATEGY,
  progressInterval: LOOP_HEARTBEAT_INTERVAL_MESSAGES,
});

const COLLECTOR_HEARTBEAT_BATCH_STRATEGY = createMessageBatchStrategy({
  strategy: COLLECTOR_HEARTBEAT_MESSAGE_STRATEGY,
  progressInterval: LOOP_HEARTBEAT_INTERVAL_MESSAGES,
});

function createRawEventBatchStrategy(): BatchStrategy<TrendsContext> {
  return {
    async processBatch(ctx, batch, messages): Promise<void> {
      await RAW_EVENT_BATCH_STRATEGY_BASE.processBatch(ctx, batch, messages);
      if (!batch.isActive()) {
        return;
      }
      await updateConsumerLag(ctx, batch, messages);
    },
  };
}

export function createBatchStrategies(
  ctx: TrendsContext
): ReadonlyMap<string, BatchStrategy<TrendsContext>> {
  return new Map<string, BatchStrategy<TrendsContext>>([
    [ctx.config.KAFKA_TOPIC_RAW_EVENTS, createRawEventBatchStrategy()],
    [ctx.config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT, COLLECTOR_HEARTBEAT_BATCH_STRATEGY],
  ]);
}
