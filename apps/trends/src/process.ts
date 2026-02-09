import type { EachBatchPayload } from "kafkajs";
import type { Redis } from "ioredis";
import { type PrismaClient, upsertConsumerLag } from "@rising-intelligence/db";
import { parseCanonicalSource, serializeError } from "@rising-intelligence/shared";
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

function parseIsoDate(value: unknown, field: string): Date {
  if (typeof value !== "string") {
    throw new Error(`collector heartbeat ${field} must be a string`);
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`collector heartbeat ${field} is invalid: ${value}`);
  }
  return parsed;
}

function parseCollectorStatus(value: unknown): "healthy" | "degraded" | "error" {
  if (typeof value === "number") {
    if (value === 1) {
      return "healthy";
    }
    if (value === 2) {
      return "degraded";
    }
    if (value === 3) {
      return "error";
    }
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "healthy" || normalized === "collector_status_healthy") {
      return "healthy";
    }
    if (normalized === "degraded" || normalized === "collector_status_degraded") {
      return "degraded";
    }
    if (normalized === "error" || normalized === "collector_status_error") {
      return "error";
    }
  }

  throw new Error(`Unsupported collector heartbeat status: ${String(value)}`);
}

function deserializeCollectorHeartbeat(
  messageValue: Buffer
): {
  source: string;
  status: "healthy" | "degraded" | "error";
  timestamp: Date;
  lastFetchAt: Date;
  itemsFetched: number;
  errorMessage?: string;
} {
  let decoded: unknown;
  try {
    decoded = JSON.parse(messageValue.toString("utf-8"));
  } catch (error) {
    throw new Error(`Invalid collector heartbeat JSON: ${(error as Error).message}`);
  }

  if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) {
    throw new Error("Collector heartbeat payload must be an object");
  }

  const heartbeat = decoded as Record<string, unknown>;
  const source = parseCanonicalSource(heartbeat.source as number | string);
  const status = parseCollectorStatus(heartbeat.status);
  const timestamp = parseIsoDate(heartbeat.timestamp, "timestamp");
  const lastFetchAt = parseIsoDate(heartbeat.last_fetch_at, "last_fetch_at");
  const itemsFetched = typeof heartbeat.items_fetched === "number" ? heartbeat.items_fetched : 0;
  const errorMessage =
    typeof heartbeat.error_message === "string" && heartbeat.error_message.trim().length > 0
      ? heartbeat.error_message
      : undefined;

  return {
    source,
    status,
    timestamp,
    lastFetchAt,
    itemsFetched,
    errorMessage,
  };
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

  let messagesSinceHeartbeat = 0;
  const maybeHeartbeat = async () => {
    messagesSinceHeartbeat += 1;
    if (messagesSinceHeartbeat < LOOP_HEARTBEAT_INTERVAL_MESSAGES) {
      return;
    }
    await heartbeat();
    messagesSinceHeartbeat = 0;
  };

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
      await maybeHeartbeat();
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
      await maybeHeartbeat();
      continue;
    }

    const trackedTopics = filterTrackedTags(event.tags, ctx.allowlist);
    if (trackedTopics.length === 0) {
      resolveOffset(message.offset);
      await maybeHeartbeat();
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

export async function processCollectorHeartbeatBatch(
  ctx: TrendsContext,
  payload: EachBatchPayload
): Promise<void> {
  const { batch, isRunning, isStale, resolveOffset, commitOffsetsIfNecessary, heartbeat } = payload;
  if (!isRunning() || isStale()) {
    return;
  }

  let messagesSinceHeartbeat = 0;
  const maybeHeartbeat = async () => {
    messagesSinceHeartbeat += 1;
    if (messagesSinceHeartbeat < LOOP_HEARTBEAT_INTERVAL_MESSAGES) {
      return;
    }
    await heartbeat();
    messagesSinceHeartbeat = 0;
  };

  for (const message of batch.messages) {
    if (!message.value) {
      incrementError(ctx.healthContext, "parse_error");
      ctx.logger.warn(
        {
          kafkaTopic: batch.topic,
          partition: batch.partition,
          offset: message.offset,
        },
        "Skipping collector heartbeat with empty value"
      );
      resolveOffset(message.offset);
      await maybeHeartbeat();
      continue;
    }

    try {
      const collectorHeartbeat = deserializeCollectorHeartbeat(message.value);
      ctx.healthContext.collectorHeartbeats.set(collectorHeartbeat.source, collectorHeartbeat);
    } catch (error) {
      incrementError(ctx.healthContext, "parse_error");
      ctx.logger.warn(
        {
          kafkaTopic: batch.topic,
          partition: batch.partition,
          offset: message.offset,
          error: serializeError(error),
        },
        "Failed to deserialize collector heartbeat"
      );
    }

    resolveOffset(message.offset);
    await maybeHeartbeat();
  }

  await commitOffsetsIfNecessary();
  await heartbeat();
}
