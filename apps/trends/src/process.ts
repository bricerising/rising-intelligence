import type { EachBatchPayload } from "kafkajs";
import type { Redis } from "ioredis";
import { type PrismaClient, upsertConsumerLag } from "@rising-intelligence/db";
import {
  createKafkaBatchLifecycle,
  parseCanonicalSource,
  serializeError,
} from "@rising-intelligence/shared";
import type pino from "pino";
import type { Config } from "./config.js";
import type { CompiledAllowlist } from "./allowlist.js";
import { filterTrackedTags } from "./allowlist.js";
import { deserializeRawEvent } from "./deserialize.js";
import type { ParsedRawEvent } from "./types.js";
import {
  incrementDuplicatesSkipped,
  incrementError,
  incrementEventsProcessed,
  setConsumerLag,
  type HealthContext,
} from "./health.js";
import { applyEventToWindows } from "./redis.js";

const LOOP_HEARTBEAT_INTERVAL_MESSAGES = 50;
const COLLECTOR_STATUS_BY_NUMBER = {
  1: "healthy",
  2: "degraded",
  3: "error",
} as const;
const COLLECTOR_STATUS_BY_STRING = {
  healthy: "healthy",
  collector_status_healthy: "healthy",
  degraded: "degraded",
  collector_status_degraded: "degraded",
  error: "error",
  collector_status_error: "error",
} as const;

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
    const status = COLLECTOR_STATUS_BY_NUMBER[value as keyof typeof COLLECTOR_STATUS_BY_NUMBER];
    if (status) {
      return status;
    }
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    const statusByName = COLLECTOR_STATUS_BY_STRING[normalized as keyof typeof COLLECTOR_STATUS_BY_STRING];
    if (statusByName) {
      return statusByName;
    }

    const asNumber = Number.parseInt(normalized, 10);
    if (`${asNumber}` === normalized) {
      const statusByNumber = COLLECTOR_STATUS_BY_NUMBER[asNumber as keyof typeof COLLECTOR_STATUS_BY_NUMBER];
      if (statusByNumber) {
        return statusByNumber;
      }
    }
  }

  throw new Error(`Unsupported collector heartbeat status: ${String(value)}`);
}

function parseNonNegativeInteger(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.max(0, Math.floor(value));
  }

  if (typeof value === "string") {
    const normalized = value.trim();
    if (/^\d+$/.test(normalized)) {
      return Number.parseInt(normalized, 10);
    }
  }

  return fallback;
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
  const itemsFetched = parseNonNegativeInteger(heartbeat.items_fetched, 0);
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

interface MessageContext {
  kafkaTopic: string;
  partition: number;
  offset: string;
}

interface BatchMessageStrategy<TMessage> {
  readonly emptyValueLogMessage: string;
  readonly deserializeFailureLogMessage: string;
  deserialize(value: Buffer): TMessage;
  handleMessage(
    ctx: TrendsContext,
    decoded: TMessage,
    messageContext: MessageContext
  ): Promise<void>;
}

async function processBatchWithStrategy<TMessage>(
  ctx: TrendsContext,
  payload: EachBatchPayload,
  strategy: BatchMessageStrategy<TMessage>
): Promise<boolean> {
  const { batch, isRunning, isStale, resolveOffset, commitOffsetsIfNecessary, heartbeat } = payload;
  const batchLifecycle = createKafkaBatchLifecycle(
    { isRunning, isStale, heartbeat },
    LOOP_HEARTBEAT_INTERVAL_MESSAGES
  );

  if (!batchLifecycle.shouldContinue()) {
    return false;
  }

  let completedBatch = true;

  for (const message of batch.messages) {
    if (!batchLifecycle.shouldContinue()) {
      completedBatch = false;
      break;
    }

    const messageContext: MessageContext = {
      kafkaTopic: batch.topic,
      partition: batch.partition,
      offset: message.offset,
    };

    if (!message.value) {
      incrementError(ctx.healthContext, "parse_error");
      ctx.logger.warn(messageContext, strategy.emptyValueLogMessage);
      resolveOffset(message.offset);
      await batchLifecycle.onMessageHandled();
      continue;
    }

    let decoded: TMessage;
    try {
      decoded = strategy.deserialize(message.value);
    } catch (error) {
      incrementError(ctx.healthContext, "parse_error");
      ctx.logger.warn(
        {
          ...messageContext,
          error: serializeError(error),
        },
        strategy.deserializeFailureLogMessage
      );
      resolveOffset(message.offset);
      await batchLifecycle.onMessageHandled();
      continue;
    }

    await strategy.handleMessage(ctx, decoded, messageContext);
    resolveOffset(message.offset);
    await batchLifecycle.onMessageHandled();
  }

  await commitOffsetsIfNecessary();
  if (!completedBatch || !batchLifecycle.shouldContinue()) {
    return false;
  }

  await batchLifecycle.flushHeartbeat();
  return true;
}

const RAW_EVENT_BATCH_STRATEGY: BatchMessageStrategy<ParsedRawEvent> = {
  emptyValueLogMessage: "Skipping message with empty value",
  deserializeFailureLogMessage: "Failed to deserialize event",
  deserialize: deserializeRawEvent,
  async handleMessage(ctx, event, messageContext): Promise<void> {
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

type CollectorHeartbeatState = ReturnType<typeof deserializeCollectorHeartbeat>;

const COLLECTOR_HEARTBEAT_BATCH_STRATEGY: BatchMessageStrategy<CollectorHeartbeatState> = {
  emptyValueLogMessage: "Skipping collector heartbeat with empty value",
  deserializeFailureLogMessage: "Failed to deserialize collector heartbeat",
  deserialize: deserializeCollectorHeartbeat,
  async handleMessage(ctx, collectorHeartbeat): Promise<void> {
    ctx.healthContext.collectorHeartbeats.set(collectorHeartbeat.source, collectorHeartbeat);
  },
};

async function updateConsumerLag(
  ctx: TrendsContext,
  payload: EachBatchPayload
): Promise<void> {
  const { batch } = payload;
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

export async function processBatch(
  ctx: TrendsContext,
  payload: EachBatchPayload
): Promise<void> {
  const processed = await processBatchWithStrategy(
    ctx,
    payload,
    RAW_EVENT_BATCH_STRATEGY
  );
  if (!processed) {
    return;
  }

  await updateConsumerLag(ctx, payload);
}

export async function processCollectorHeartbeatBatch(
  ctx: TrendsContext,
  payload: EachBatchPayload
): Promise<void> {
  await processBatchWithStrategy(ctx, payload, COLLECTOR_HEARTBEAT_BATCH_STRATEGY);
}
