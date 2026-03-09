import { BriefStatus, Prisma } from "@rising-intelligence/db";
import { serializeError } from "@rising-intelligence/shared/errors";
import type pino from "pino";
import {
  type BriefRuntimeContext,
  type ProcessContext,
  type ParsedTrendSnapshot,
  createSummaryRequestProcessor,
  createSummaryRequestGroundingFacade,
  deserializeSummaryRequest,
  deserializeTrendSnapshot,
  incrementGeneration,
  incrementError,
  mapTrendWindowToEnum,
  observeGenerationDuration,
} from "./service.js";

const briefGroundingFacade = createSummaryRequestGroundingFacade();
const summaryRequestProcessor = createSummaryRequestProcessor();

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
    groundingFacade: briefGroundingFacade,
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
        await summaryRequestProcessor.processSummaryRequest(
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
