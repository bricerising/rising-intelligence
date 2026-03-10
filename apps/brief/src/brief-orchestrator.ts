/**
 * Brief orchestration boundary.
 *
 * Mediates between prepared brief execution jobs and the brief
 * service's internal subsystems (budgeting, generation, persistence,
 * publishing, failure handling, metrics). Trigger assembly, query resolution,
 * and source ingestion stay outside this boundary.
 */

import { BriefStatus } from "@rising-intelligence/db";
import { serializeError } from "@rising-intelligence/shared/errors";
import type pino from "pino";
import type {
  BriefBudgetDecision,
  BriefBudgetGovernor,
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
  handleSummaryRequestFailure,
} from "./failure-handling.js";
import type { Config } from "./config.js";
import {
  createBriefOrchestrationRequest,
  type BriefOrchestrationRequest,
} from "./types.js";
import type { PrismaClient } from "@rising-intelligence/db";
import type { Redis } from "ioredis";

export interface BriefExecutionJob {
  request: BriefOrchestrationRequest;
  environment: {
    config: Config;
    logger: pino.Logger;
    healthContext: HealthContext;
    prisma: PrismaClient;
    redis: Redis;
  };
  services: {
    budgetGovernor: BriefBudgetGovernor;
    publisher: BriefResultPublisher;
    resultStore: BriefResultStore;
    generationFacade: SummaryRequestGenerationFacade;
    producedAt: Date;
  };
}

export type BriefExecutionInput = BriefExecutionJob;

type OrchestratorContext = BriefExecutionJob["environment"];
type OrchestratorRuntime = BriefExecutionJob["services"];

export function createBriefExecutionJob(
  input: BriefExecutionJob
): BriefExecutionJob {
  return {
    request: createBriefOrchestrationRequest(input.request),
    environment: {
      ...input.environment,
    },
    services: {
      ...input.services,
      producedAt: new Date(input.services.producedAt.getTime()),
    },
  };
}

export function createBriefExecutionInput(
  input: BriefExecutionInput
): BriefExecutionInput {
  return createBriefExecutionJob(input);
}

export interface BriefOrchestrator {
  /**
   * Runs the brief orchestration lifecycle:
   * idempotency → budget → generation → persistence → publishing → metrics.
   */
  execute(job: BriefExecutionJob): Promise<void>;
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

function requireBudgetDecision(
  decision: BriefBudgetDecision | null
): BriefBudgetDecision {
  if (!decision) {
    throw new Error("Budget decision is required");
  }

  return decision;
}

async function rollbackBudgetReservation(
  ctx: OrchestratorContext,
  budgetGovernor: BriefBudgetGovernor,
  logger: pino.Logger,
  decision: BriefBudgetDecision
): Promise<void> {
  const spentBudgetUsd = await budgetGovernor.rollback(decision);
  ctx.healthContext.redisHealthy = true;
  setBudgetRemainingUsd(
    ctx.healthContext,
    Math.max(0, decision.dailyBudgetUsd - spentBudgetUsd)
  );
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
  async execute(job: BriefExecutionJob): Promise<void> {
    const { environment: ctx, services: runtime, request } = job;
    const {
      budgetGovernor,
      publisher,
      resultStore,
      generationFacade,
      producedAt,
    } = runtime;
    const logger = ctx.logger;

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

    // ── Phase 2: Budget reservation ─────────────────────────────────────
    const dateKey = getBudgetDateKey(producedAt);
    const dailyBudgetUsd =
      request.budget?.dailyBudgetUsd ?? ctx.config.LLM_DAILY_BUDGET_USD;
    const estimatedCostUsd = normalizeUsd(estimateRequestCostUsd(request));
    let budgetDecision: BriefBudgetDecision | null = null;
    let spentBudgetUsd = 0;

    try {
      budgetDecision = await budgetGovernor.authorize({
        dateKey,
        dailyBudgetUsd,
        estimatedCostUsd,
      });
      spentBudgetUsd = budgetDecision.spentUsd;
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

    const currentBudgetDecision = requireBudgetDecision(budgetDecision);

    if (!currentBudgetDecision.authorized) {
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
          reservedCostUsd: currentBudgetDecision.reservedCostUsd,
          dailyBudgetUsd,
        },
        "Skipped summary request due to budget limit"
      );
      return;
    }

    // ── Phase 3: Generation → Persistence → Publishing ──────────────────
    let persistedCreated = false;
    try {
      const successResult = await generationFacade.buildSuccessResult({
        ctx,
        request,
        producedAt,
        estimatedCostUsd,
      });
      const persisted = await resultStore.persist(successResult.payload, BriefStatus.success);
      if (persisted === "duplicate") {
        await rollbackBudgetReservation(
          ctx,
          budgetGovernor,
          logger,
          requireBudgetDecision(budgetDecision)
        );
        budgetDecision = null;
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

      // ── Phase 4: Budget settlement ────────────────────────────────────
      const costDeltaUsd = normalizeUsdDelta(
        successResult.metrics.costUsd - requireBudgetDecision(budgetDecision).reservedCostUsd
      );
      if (costDeltaUsd !== 0) {
        try {
          spentBudgetUsd = await budgetGovernor.settle({
            decision: requireBudgetDecision(budgetDecision),
            actualCostUsd: successResult.metrics.costUsd,
          });
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

      // ── Phase 5: Metrics ──────────────────────────────────────────────
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
      // ── Phase 6: Failure handling ─────────────────────────────────────
      if (budgetDecision?.authorized && !persistedCreated) {
        try {
          await rollbackBudgetReservation(
            ctx,
            budgetGovernor,
            logger,
            requireBudgetDecision(budgetDecision)
          );
          budgetDecision = null;
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
