/**
 * Summary request failure handling.
 *
 * Implements a chain-of-responsibility pattern that classifies processing
 * errors (non-retryable, persisted-result, retryable LLM, unknown) and
 * emits appropriate failure results.
 */

import { BriefStatus, type PrismaClient } from "@rising-intelligence/db";
import { serializeError } from "@rising-intelligence/shared/errors";
import type { Redis } from "ioredis";
import type pino from "pino";
import type { Config } from "./config.js";
import type { HealthContext } from "./health.js";
import {
  LlmGenerationError,
  NonRetryableProcessingError,
  classifyRetryableFailureCode,
} from "./processing-errors.js";
import {
  incrementDuplicatesSkipped,
  incrementError,
  incrementGeneration,
} from "./health.js";
import { buildFailureBriefResultPayload } from "./result-payload-adapter.js";
import type { BriefResultStore } from "./result-store-facade.js";
import {
  type BriefResultPublisher,
} from "./publishing-facade.js";

// ── Context types ───────────────────────────────────────────────────────────

export interface FailureHandlingProcessContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
  prisma: PrismaClient;
  redis: Redis;
}

// ── Result emission ─────────────────────────────────────────────────────────

async function republishPersistedResult(
  resultStore: BriefResultStore,
  requestId: string,
  publisher: BriefResultPublisher
): Promise<BriefStatus | null> {
  const existing = await resultStore.load(requestId);
  if (!existing) {
    return null;
  }

  await publisher.publishResult(requestId, existing.payload);
  return existing.status;
}

export async function emitFailureResult(
  ctx: FailureHandlingProcessContext,
  resultStore: BriefResultStore,
  publisher: BriefResultPublisher,
  requestId: string,
  producedAt: Date,
  code: string,
  message: string,
  retryable: boolean
): Promise<void> {
  const failureResult = buildFailureBriefResultPayload(
    requestId,
    producedAt,
    code,
    message,
    retryable
  );
  const persisted = await resultStore.persist(failureResult, BriefStatus.failure);
  if (persisted === "duplicate") {
    incrementDuplicatesSkipped(ctx.healthContext);
    const republishedStatus = await republishPersistedResult(
      resultStore,
      requestId,
      publisher
    );
    if (!republishedStatus) {
      throw new Error(`Unable to republish existing failure result for request ${requestId}`);
    }
    return;
  }

  await publisher.publishResult(requestId, failureResult);
}

// ── Non-retryable failure helper ────────────────────────────────────────────

function mapNonRetryableFailureMetric(
  error: NonRetryableProcessingError
): "grounding_error" | "generation_error" {
  return error.code === "grounding_error" ? "grounding_error" : "generation_error";
}

export interface HandleNonRetryableFailureInput {
  ctx: FailureHandlingProcessContext;
  resultStore: BriefResultStore;
  publisher: BriefResultPublisher;
  requestId: string;
  producedAt: Date;
  error: NonRetryableProcessingError;
  logger: pino.Logger;
  logMessage: string;
}

export async function handleNonRetryableFailure(
  input: HandleNonRetryableFailureInput
): Promise<void> {
  const {
    ctx,
    resultStore,
    publisher,
    requestId,
    producedAt,
    error,
    logger,
    logMessage,
  } = input;

  incrementError(ctx.healthContext, mapNonRetryableFailureMetric(error));
  incrementGeneration(ctx.healthContext, "failure");
  logger.warn({ error: serializeError(error) }, logMessage);
  await emitFailureResult(
    ctx,
    resultStore,
    publisher,
    requestId,
    producedAt,
    error.code,
    error.message,
    false
  );
}

// ── Failure handler chain ───────────────────────────────────────────────────

export type SummaryRequestFailureHandlerOutcome = "handled" | "rethrow";

export interface SummaryRequestFailureHandlingContext {
  ctx: FailureHandlingProcessContext;
  resultStore: BriefResultStore;
  publisher: BriefResultPublisher;
  requestId: string;
  producedAt: Date;
  logger: pino.Logger;
  persistedCreated: boolean;
}

interface SummaryRequestFailureHandler {
  readonly name: string;
  canHandle(
    error: unknown,
    context: SummaryRequestFailureHandlingContext
  ): boolean;
  handle(
    error: unknown,
    context: SummaryRequestFailureHandlingContext
  ): Promise<SummaryRequestFailureHandlerOutcome>;
}

const NON_RETRYABLE_SUMMARY_REQUEST_FAILURE_HANDLER: SummaryRequestFailureHandler = {
  name: "non-retryable",
  canHandle(error): boolean {
    return error instanceof NonRetryableProcessingError;
  },
  async handle(error, context): Promise<SummaryRequestFailureHandlerOutcome> {
    if (!(error instanceof NonRetryableProcessingError)) {
      return "rethrow";
    }

    await handleNonRetryableFailure({
      ctx: context.ctx,
      resultStore: context.resultStore,
      publisher: context.publisher,
      requestId: context.requestId,
      producedAt: context.producedAt,
      error,
      logger: context.logger,
      logMessage: "Brief request failed non-retryable validation",
    });
    return "handled";
  },
};

const PERSISTED_RESULT_SUMMARY_REQUEST_FAILURE_HANDLER: SummaryRequestFailureHandler = {
  name: "persisted-result-publish",
  canHandle(_error, context): boolean {
    return context.persistedCreated;
  },
  async handle(error, context): Promise<SummaryRequestFailureHandlerOutcome> {
    incrementError(context.ctx.healthContext, "publish_error");
    context.logger.error(
      { error: serializeError(error) },
      "Persisted brief result but failed to publish; will retry from Kafka"
    );
    return "rethrow";
  },
};

const RETRYABLE_LLM_SUMMARY_REQUEST_FAILURE_HANDLER: SummaryRequestFailureHandler = {
  name: "retryable-llm",
  canHandle(error): boolean {
    return error instanceof LlmGenerationError;
  },
  async handle(error, context): Promise<SummaryRequestFailureHandlerOutcome> {
    if (!(error instanceof LlmGenerationError)) {
      return "rethrow";
    }

    const failureCode = classifyRetryableFailureCode(error);
    incrementError(context.ctx.healthContext, failureCode);
    incrementGeneration(context.ctx.healthContext, "failure");
    context.logger.error(
      { error: serializeError(error), failureCode },
      "Failed to process summary request due to retryable LLM error"
    );
    await emitFailureResult(
      context.ctx,
      context.resultStore,
      context.publisher,
      context.requestId,
      context.producedAt,
      failureCode,
      error.message,
      true
    );
    return "handled";
  },
};

const UNKNOWN_SUMMARY_REQUEST_FAILURE_HANDLER: SummaryRequestFailureHandler = {
  name: "unknown",
  canHandle(): boolean {
    return true;
  },
  async handle(_error, context): Promise<SummaryRequestFailureHandlerOutcome> {
    incrementError(
      context.ctx.healthContext,
      "generation_error"
    );
    incrementGeneration(context.ctx.healthContext, "failure");
    context.logger.error({ error: serializeError(_error) }, "Failed to process summary request");
    return "rethrow";
  },
};

const SUMMARY_REQUEST_FAILURE_HANDLERS: readonly SummaryRequestFailureHandler[] = [
  NON_RETRYABLE_SUMMARY_REQUEST_FAILURE_HANDLER,
  PERSISTED_RESULT_SUMMARY_REQUEST_FAILURE_HANDLER,
  RETRYABLE_LLM_SUMMARY_REQUEST_FAILURE_HANDLER,
  UNKNOWN_SUMMARY_REQUEST_FAILURE_HANDLER,
];

export async function handleSummaryRequestFailure(
  error: unknown,
  context: SummaryRequestFailureHandlingContext
): Promise<SummaryRequestFailureHandlerOutcome> {
  for (const handler of SUMMARY_REQUEST_FAILURE_HANDLERS) {
    if (!handler.canHandle(error, context)) {
      continue;
    }
    return handler.handle(error, context);
  }

  return "rethrow";
}
