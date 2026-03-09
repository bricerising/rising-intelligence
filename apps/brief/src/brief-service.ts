import { BriefStatus, Prisma } from "@rising-intelligence/db";
import { serializeError } from "@rising-intelligence/shared/errors";
import type pino from "pino";
import type { BriefRuntimeContext } from "./runtime-factory.js";
import { deserializeSummaryRequest, deserializeTrendSnapshot } from "./deserialize.js";
import {
  incrementGeneration,
  observeGenerationDuration,
  incrementError,
} from "./health.js";
import { type ProcessContext, processSummaryRequest } from "./process.js";
import { mapTrendWindowToEnum } from "./topic-message-handlers.js";
import type { ParsedTrendSnapshot } from "./types.js";

export interface BriefService {
  handleSummaryRequest(
    ctx: BriefRuntimeContext,
    messageValue: Buffer,
    messageLogger: pino.Logger,
  ): Promise<void>;
  handleTrendSnapshot(
    ctx: BriefRuntimeContext,
    messageValue: Buffer,
    messageLogger: pino.Logger,
  ): Promise<void>;
}

function buildProcessContext(
  ctx: BriefRuntimeContext,
  messageLogger: pino.Logger,
): ProcessContext {
  return {
    config: ctx.config,
    logger: messageLogger,
    healthContext: ctx.healthContext,
    prisma: ctx.prisma,
    redis: ctx.redis,
    producer: ctx.kafkaProducerContext.producer,
  };
}

async function persistTrendSnapshot(
  ctx: BriefRuntimeContext,
  snapshot: ParsedTrendSnapshot,
): Promise<void> {
  try {
    await ctx.prisma.briefTrendSnapshot.create({
      data: {
        generatedAt: snapshot.generatedAt,
        window: mapTrendWindowToEnum(snapshot.window),
        snapshot: snapshot.snapshot as Prisma.InputJsonValue,
      },
    });
    ctx.healthContext.postgresHealthy = true;
  } catch (error) {
    ctx.healthContext.postgresHealthy = false;
    throw error;
  }
}

export function createBriefService(): BriefService {
  return {
    async handleSummaryRequest(ctx, messageValue, messageLogger) {
      let request: ReturnType<typeof deserializeSummaryRequest>;
      try {
        request = deserializeSummaryRequest(messageValue);
      } catch (error) {
        incrementError(ctx.healthContext, "parse_error");
        incrementGeneration(ctx.healthContext, "failure");
        messageLogger.warn(
          { error: serializeError(error) },
          "Failed to deserialize summary request",
        );
        return;
      }

      const startTime = Date.now();
      try {
        await processSummaryRequest(
          buildProcessContext(ctx, messageLogger),
          request,
        );
      } finally {
        observeGenerationDuration(ctx.healthContext, (Date.now() - startTime) / 1000);
      }
    },

    async handleTrendSnapshot(ctx, messageValue, messageLogger) {
      try {
        const snapshot = deserializeTrendSnapshot(messageValue);
        await persistTrendSnapshot(ctx, snapshot);
      } catch (error) {
        messageLogger.warn(
          { error: serializeError(error) },
          "Failed to process trend snapshot",
        );
      }
    },
  };
}
