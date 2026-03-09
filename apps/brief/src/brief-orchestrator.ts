/**
 * Brief orchestration boundary.
 *
 * Mediates between the summary-request processor and the brief service's
 * internal subsystems (budgeting, generation, persistence, publishing,
 * failure handling, metrics).  process.ts delegates all subsystem
 * interactions through this single interface, collapsing the multi-cluster
 * fan-out into one internal boundary.
 */

import { BriefStatus } from "@rising-intelligence/db";
import { serializeError } from "@rising-intelligence/shared/errors";
import type pino from "pino";
import type {
  BriefBudgetLedger,
} from "./budget-ledger.js";
import type { HealthContext } from "./health.js";
import {
  incrementBudgetExceeded,
  incrementDuplicatesSkipped,
  incrementError,
  incrementGeneration,
  incrementLlmCostUsd,
  incrementLlmTokens,
  observeCitationsCount,
  observeHighlightsCount,
  setBudgetRemainingUsd,
} from "./health.js";
import type { BriefResultPublisher } from "./publishing-facade.js";
import type { BriefResultStore, StoredBriefResult } from "./result-store-facade.js";
import type { SummaryRequestGenerationFacade, SuccessResult } from "./llm/generation-facade.js";
import { estimateRequestCostUsd, normalizeUsd, normalizeUsdDelta } from "./llm/generation-facade.js";
import {
  emitFailureResult,
  handleNonRetryableFailure,
  handleSummaryRequestFailure,
} from "./failure-handling.js";
import { NonRetryableProcessingError } from "./processing-errors.js";
import type { QueryModeRequestResolver } from "./query-mode-request-facade.js";
import type { Config } from "./config.js";
import type { ParsedSummaryRequest } from "./types.js";
import type { PrismaClient } from "@rising-intelligence/db";
import type { ProducerConnection } from "@rising-intelligence/pipeline/transport";
import type { Redis } from "ioredis";

// ── Context shared with the orchestrator ────────────────────────────────────

export interface OrchestratorContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
  prisma: PrismaClient;
  redis: Redis;
  producer: ProducerConnection;
}

// ── Runtime collaborators built per-request ─────────────────────────────────

export interface OrchestratorRuntime {
  budgetLedger: BriefBudgetLedger;
  publisher: BriefResultPublisher;
  resultStore: BriefResultStore;
  queryModeRequestResolver: QueryModeRequestResolver;
  generationFacade: SummaryRequestGenerationFacade;
  producedAt: Date;
}

// ── Orchestrator interface ──────────────────────────────────────────────────

export interface BriefOrchestrator {
  /**
   * Runs the full request lifecycle: idempotency → query resolution →
   * budget → generation → persistence → publishing → metrics.
   */
  execute(
    ctx: OrchestratorContext,
    runtime: OrchestratorRuntime,
    request: ParsedSummaryRequest,
    logger: pino.Logger
  ): Promise<void>;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function getBudgetDateKey(date: Date): string {
  return date.toISOString().slice(0, 10);
}

async function republishPersistedResult(
  resultStore: BriefResultStore,
  requestId: string,
  publisher: BriefResultPublisher
): Promise<StoredBriefResult["status"] | null> {
  const existing = await resultStore.load(requestId);
  if (!existing) {
    return null;
  }

  await publisher.publishResult(requestId, existing.payload);
  return existing.status;
}

async function rollbackBudgetReservation(
  ctx: OrchestratorContext,
  budgetLedger: BriefBudgetLedger,
  logger: pino.Logger,
  dateKey: string,
  reservedAmountUsd: number,
  dailyBudgetUsd: number
): Promise<void> {
  const spentBudgetUsd = await budgetLedger.release({
    dateKey,
    amountUsd: reservedAmountUsd,
  });
  ctx.healthContext.redisHealthy = true;
  setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - spentBudgetUsd));
  logger.info({ spentBudgetUsd }, "Rolled back brief budget reservation");
}

function emitSuccessMetrics(
  healthContext: HealthContext,
  result: SuccessResult
): void {
  incrementGeneration(healthContext, "success");
  incrementLlmCostUsd(healthContext, result.metrics.costUsd);
  incrementLlmTokens(healthContext, "input", result.metrics.inputTokens);
  incrementLlmTokens(healthContext, "output", result.metrics.outputTokens);
  observeHighlightsCount(healthContext, result.metrics.highlightsCount);
  observeCitationsCount(healthContext, result.metrics.citationsCount);
}

// ── Default implementation ──────────────────────────────────────────────────

class DefaultBriefOrchestrator implements BriefOrchestrator {
  async execute(
    ctx: OrchestratorContext,
    runtime: OrchestratorRuntime,
    request: ParsedSummaryRequest,
    logger: pino.Logger
  ): Promise<void> {
    const {
      budgetLedger,
      publisher,
      resultStore,
      queryModeRequestResolver,
      generationFacade,
      producedAt,
    } = runtime;

    // ── Phase 1: Idempotency check ──────────────────────────────────────
    let existingResult: StoredBriefResult | null = null;

    try {
      existingResult = await resultStore.load(request.requestId);
    } catch (error) {
      incrementError(ctx.healthContext, "idempotency_error");
      logger.error({ error: serializeError(error) }, "Failed to load persisted brief result");
      throw error;
    }

    if (existingResult) {
      incrementDuplicatesSkipped(ctx.healthContext);
      incrementGeneration(ctx.healthContext, "skipped");
      try {
        await publisher.publishResult(request.requestId, existingResult.payload);
        logger.info({ status: existingResult.status }, "Republished persisted brief result for duplicate request");
        return;
      } catch (error) {
        incrementError(ctx.healthContext, "publish_error");
        logger.error(
          { error: serializeError(error) },
          "Failed to republish persisted brief result for duplicate request"
        );
        throw error;
      }
    }

    // ── Phase 2: Query-mode resolution ──────────────────────────────────
    let requestForGeneration: ParsedSummaryRequest;
    try {
      requestForGeneration = await queryModeRequestResolver.resolve(ctx, request, logger);
    } catch (error) {
      if (error instanceof NonRetryableProcessingError) {
        await handleNonRetryableFailure({
          ctx,
          resultStore,
          publisher,
          requestId: request.requestId,
          producedAt,
          error,
          logger,
          logMessage: "Summary request failed non-retryable pre-processing",
        });
        return;
      }

      incrementError(ctx.healthContext, "generation_error");
      incrementGeneration(ctx.healthContext, "failure");
      logger.error({ error: serializeError(error) }, "Failed to resolve summary request");
      throw error;
    }

    // ── Phase 3: Budget reservation ─────────────────────────────────────
    const dateKey = getBudgetDateKey(producedAt);
    const dailyBudgetUsd =
      requestForGeneration.budget?.dailyBudgetUsd ?? ctx.config.LLM_DAILY_BUDGET_USD;
    const estimatedCostUsd = normalizeUsd(estimateRequestCostUsd(requestForGeneration));
    let budgetReserved = false;
    let spentBudgetUsd = 0;
    let reservedCostUsd = estimatedCostUsd;

    try {
      const reservation = await budgetLedger.reserve({
        dateKey,
        dailyBudgetUsd,
        amountUsd: reservedCostUsd,
      });
      budgetReserved = reservation.reserved;
      spentBudgetUsd = reservation.spentUsd;
      ctx.healthContext.redisHealthy = true;
      setBudgetRemainingUsd(
        ctx.healthContext,
        Math.max(0, dailyBudgetUsd - spentBudgetUsd)
      );
    } catch (error) {
      ctx.healthContext.redisHealthy = false;
      incrementError(ctx.healthContext, "redis_error");
      logger.error({ error: serializeError(error) }, "Failed to reserve daily budget");
      throw error;
    }

    if (!budgetReserved) {
      incrementBudgetExceeded(ctx.healthContext);
      incrementGeneration(ctx.healthContext, "skipped");
      await emitFailureResult(
        ctx,
        resultStore,
        publisher,
        request.requestId,
        producedAt,
        "budget_exceeded",
        "Daily brief budget exceeded",
        false
      );
      logger.info(
        {
          spentBudgetUsd,
          reservedCostUsd,
          dailyBudgetUsd,
        },
        "Skipped summary request due to budget limit"
      );
      return;
    }

    // ── Phase 4: Generation → Persistence → Publishing ──────────────────
    let persistedCreated = false;
    try {
      const successResult = await generationFacade.buildSuccessResult({
        ctx,
        request: requestForGeneration,
        producedAt,
        estimatedCostUsd,
      });
      const persisted = await resultStore.persist(successResult.payload, BriefStatus.success);
      if (persisted === "duplicate") {
        await rollbackBudgetReservation(
          ctx,
          budgetLedger,
          logger,
          dateKey,
          reservedCostUsd,
          dailyBudgetUsd
        );
        budgetReserved = false;
        incrementDuplicatesSkipped(ctx.healthContext);
        incrementGeneration(ctx.healthContext, "skipped");
        const republishedStatus = await republishPersistedResult(
          resultStore,
          request.requestId,
          publisher
        );
        if (!republishedStatus) {
          throw new Error(`Persisted result missing after duplicate insert for request ${request.requestId}`);
        }
        logger.info({ status: republishedStatus }, "Detected duplicate during persist and republished stored result");
        return;
      }
      persistedCreated = true;

      // ── Phase 5: Budget settlement ────────────────────────────────────
      const costDeltaUsd = normalizeUsdDelta(successResult.metrics.costUsd - reservedCostUsd);
      if (costDeltaUsd !== 0) {
        try {
          spentBudgetUsd = await budgetLedger.settle({
            dateKey,
            deltaUsd: costDeltaUsd,
          });
          reservedCostUsd = normalizeUsd(successResult.metrics.costUsd);
          ctx.healthContext.redisHealthy = true;
        } catch (error) {
          ctx.healthContext.redisHealthy = false;
          incrementError(ctx.healthContext, "redis_error");
          logger.warn(
            {
              costDeltaUsd,
              error: serializeError(error),
            },
            "Failed to settle reserved budget to final brief cost"
          );
        }
      }

      await publisher.publishResult(request.requestId, successResult.payload);

      // ── Phase 6: Metrics ──────────────────────────────────────────────
      emitSuccessMetrics(ctx.healthContext, successResult);

      setBudgetRemainingUsd(
        ctx.healthContext,
        Math.max(0, dailyBudgetUsd - spentBudgetUsd)
      );
      logger.info(
        {
          topicCount: successResult.metrics.highlightsCount,
          citationsCount: successResult.metrics.citationsCount,
          costUsd: successResult.metrics.costUsd,
        },
        "Summary request processed"
      );
    } catch (error) {
      // ── Phase 7: Failure handling ─────────────────────────────────────
      if (budgetReserved && !persistedCreated) {
        try {
          await rollbackBudgetReservation(
            ctx,
            budgetLedger,
            logger,
            dateKey,
            reservedCostUsd,
            dailyBudgetUsd
          );
          budgetReserved = false;
        } catch (rollbackError) {
          ctx.healthContext.redisHealthy = false;
          incrementError(ctx.healthContext, "redis_error");
          logger.error(
            { error: serializeError(rollbackError) },
            "Failed to roll back reserved brief budget"
          );
          throw rollbackError;
        }
      }

      const failureOutcome = await handleSummaryRequestFailure(error, {
        ctx,
        resultStore,
        publisher,
        requestId: request.requestId,
        producedAt,
        logger,
        persistedCreated,
      });
      if (failureOutcome === "handled") {
        return;
      }

      throw error;
    }
  }
}

// ── Factory ─────────────────────────────────────────────────────────────────

export function createBriefOrchestrator(): BriefOrchestrator {
  return new DefaultBriefOrchestrator();
}
