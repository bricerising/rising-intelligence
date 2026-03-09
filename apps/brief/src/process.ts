/**
 * Summary request processing orchestrator.
 *
 * Coordinates the end-to-end lifecycle of a summary request:
 * duplicate detection → query-mode resolution → budget reservation →
 * LLM generation → persistence → publishing → metric emission.
 *
 * Domain concerns (evidence scoring, grounding enforcement, LLM provider
 * strategies, failure handling) are delegated to focused internal modules
 * behind the internals barrel.
 */

import { BriefStatus, type PrismaClient } from "@rising-intelligence/db";
import type { ProducerConnection } from "@rising-intelligence/pipeline/transport";
import type { Redis } from "ioredis";
import {
  buildFunctionDependencies,
  type FunctionDependencyOverrides,
} from "@rising-intelligence/shared/lifecycle";
import { serializeError } from "@rising-intelligence/shared/errors";
import type pino from "pino";
import {
  createBriefBudgetLedger,
  type BriefBudgetLedger,
  type CreateBriefBudgetLedgerInput,
  type Config,
  incrementBudgetExceeded,
  incrementDuplicatesSkipped,
  incrementError,
  incrementGeneration,
  incrementLlmCostUsd,
  incrementLlmTokens,
  observeCitationsCount,
  observeHighlightsCount,
  setBudgetRemainingUsd,
  type HealthContext,
  createSummaryRequestGroundingFacade,
  type SummaryRequestGroundingFacade,
  NonRetryableProcessingError,
  createBriefResultPublisher,
  type CreateBriefResultPublisherInput,
  type BriefResultPublisher,
  createQueryModeRequestResolver,
  type QueryModeRequestResolver,
  type BriefResultPayload,
  createBriefResultStore,
  type BriefResultStore,
  type StoredBriefResult,
  type ParsedSummaryRequest,
  // LLM generation facade
  createSummaryRequestGenerationFacade,
  type SummaryRequestGenerationFacade,
  estimateRequestCostUsd,
  normalizeUsd,
  normalizeUsdDelta,
  // Failure handling
  emitFailureResult,
  handleNonRetryableFailure,
  handleSummaryRequestFailure,
} from "./internals.js";

export interface ProcessContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
  prisma: PrismaClient;
  redis: Redis;
  producer: ProducerConnection;
  /** Grounding facade scoped to this processing context. Falls back to a shared default when omitted. */
  groundingFacade?: SummaryRequestGroundingFacade;
}

function resolveGroundingFacade(ctx: ProcessContext): SummaryRequestGroundingFacade {
  return ctx.groundingFacade ?? DEFAULT_GROUNDING_FACADE;
}

interface RequestScopedProcessContext extends ProcessContext {
  logger: pino.Logger;
}

function createRequestScopedProcessContext(
  ctx: ProcessContext,
  requestId: string
): RequestScopedProcessContext {
  return {
    ...ctx,
    logger: ctx.logger.child({ requestId }),
  };
}

const DEFAULT_GROUNDING_FACADE = createSummaryRequestGroundingFacade();

export interface SummaryRequestProcessor {
  processSummaryRequest(ctx: ProcessContext, request: ParsedSummaryRequest): Promise<void>;
}

interface SummaryRequestProcessorDependencies {
  createBriefBudgetLedger(input: CreateBriefBudgetLedgerInput): BriefBudgetLedger;
  createBriefResultPublisher(
    input: CreateBriefResultPublisherInput
  ): BriefResultPublisher<BriefResultPayload>;
  createBriefResultStore(
    prisma: PrismaClient,
    healthContext: HealthContext
  ): BriefResultStore;
  createQueryModeRequestResolver(): QueryModeRequestResolver;
}

type SummaryRequestProcessorDependencyOverrides = FunctionDependencyOverrides<
  SummaryRequestProcessorDependencies
>;

interface SummaryRequestRuntime {
  requestContext: RequestScopedProcessContext;
  logger: pino.Logger;
  budgetLedger: BriefBudgetLedger;
  publisher: BriefResultPublisher<BriefResultPayload>;
  resultStore: BriefResultStore;
  queryModeRequestResolver: QueryModeRequestResolver;
  generationFacade: SummaryRequestGenerationFacade;
  producedAt: Date;
}

const DEFAULT_SUMMARY_REQUEST_PROCESSOR_DEPENDENCIES: SummaryRequestProcessorDependencies = {
  createBriefBudgetLedger,
  createBriefResultPublisher(input): BriefResultPublisher<BriefResultPayload> {
    return createBriefResultPublisher<BriefResultPayload>(input);
  },
  createBriefResultStore,
  createQueryModeRequestResolver,
};

class SummaryRequestRuntimeFactory {
  private readonly queryModeRequestResolver: QueryModeRequestResolver;

  constructor(
    private readonly dependencies: SummaryRequestProcessorDependencies
  ) {
    this.queryModeRequestResolver = dependencies.createQueryModeRequestResolver();
  }

  create(ctx: ProcessContext, request: ParsedSummaryRequest): SummaryRequestRuntime {
    const requestContext = createRequestScopedProcessContext(ctx, request.requestId);
    const logger = requestContext.logger;
    const groundingFacade = resolveGroundingFacade(ctx);
    return {
      requestContext,
      logger,
      budgetLedger: this.dependencies.createBriefBudgetLedger({
        prisma: requestContext.prisma,
        redis: requestContext.redis,
        logger,
      }),
      publisher: this.dependencies.createBriefResultPublisher({
        connection: requestContext.producer,
        logger,
        topic: requestContext.config.KAFKA_TOPIC_SUMMARY_RESULTS,
      }),
      resultStore: this.dependencies.createBriefResultStore(
        requestContext.prisma,
        requestContext.healthContext
      ),
      queryModeRequestResolver: this.queryModeRequestResolver,
      generationFacade: createSummaryRequestGenerationFacade(groundingFacade),
      producedAt: new Date(),
    };
  }
}

function getBudgetDateKey(date: Date): string {
  return date.toISOString().slice(0, 10);
}

async function republishPersistedResult(
  resultStore: BriefResultStore,
  requestId: string,
  publisher: BriefResultPublisher<BriefResultPayload>
): Promise<BriefStatus | null> {
  const existing = await resultStore.load(requestId);
  if (!existing) {
    return null;
  }

  await publisher.publishResult(requestId, existing.payload);
  return existing.status;
}

async function rollbackBudgetReservation(
  ctx: ProcessContext,
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

class DefaultSummaryRequestProcessor implements SummaryRequestProcessor {
  private readonly runtimeFactory: SummaryRequestRuntimeFactory;

  constructor(dependencies: SummaryRequestProcessorDependencies) {
    this.runtimeFactory = new SummaryRequestRuntimeFactory(dependencies);
  }

  async processSummaryRequest(
    ctx: ProcessContext,
    request: ParsedSummaryRequest
  ): Promise<void> {
    const runtime = this.runtimeFactory.create(ctx, request);
    const {
      requestContext,
      logger,
      budgetLedger,
      publisher,
      resultStore,
      queryModeRequestResolver,
      generationFacade,
      producedAt,
    } = runtime;
    let existingResult: StoredBriefResult | null = null;

    try {
      existingResult = await resultStore.load(request.requestId);
    } catch (error) {
      incrementError(requestContext.healthContext, "idempotency_error");
      logger.error({ error: serializeError(error) }, "Failed to load persisted brief result");
      throw error;
    }

    if (existingResult) {
      incrementDuplicatesSkipped(requestContext.healthContext);
      incrementGeneration(requestContext.healthContext, "skipped");
      try {
        await publisher.publishResult(request.requestId, existingResult.payload);
        logger.info({ status: existingResult.status }, "Republished persisted brief result for duplicate request");
        return;
      } catch (error) {
        incrementError(requestContext.healthContext, "publish_error");
        logger.error(
          { error: serializeError(error) },
          "Failed to republish persisted brief result for duplicate request"
        );
        throw error;
      }
    }

    let requestForGeneration: ParsedSummaryRequest;
    try {
      requestForGeneration = await queryModeRequestResolver.resolve(requestContext, request, logger);
    } catch (error) {
      if (error instanceof NonRetryableProcessingError) {
        await handleNonRetryableFailure({
          ctx: requestContext,
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

      incrementError(requestContext.healthContext, "generation_error");
      incrementGeneration(requestContext.healthContext, "failure");
      logger.error({ error: serializeError(error) }, "Failed to resolve summary request");
      throw error;
    }

    const dateKey = getBudgetDateKey(producedAt);
    const dailyBudgetUsd =
      requestForGeneration.budget?.dailyBudgetUsd ?? requestContext.config.LLM_DAILY_BUDGET_USD;
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
      requestContext.healthContext.redisHealthy = true;
      setBudgetRemainingUsd(
        requestContext.healthContext,
        Math.max(0, dailyBudgetUsd - spentBudgetUsd)
      );
    } catch (error) {
      requestContext.healthContext.redisHealthy = false;
      incrementError(requestContext.healthContext, "redis_error");
      logger.error({ error: serializeError(error) }, "Failed to reserve daily budget");
      throw error;
    }

    if (!budgetReserved) {
      incrementBudgetExceeded(requestContext.healthContext);
      incrementGeneration(requestContext.healthContext, "skipped");
      await emitFailureResult(
        requestContext,
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

    let persistedCreated = false;
    try {
      const successResult = await generationFacade.buildSuccessResult({
        ctx: requestContext,
        request: requestForGeneration,
        producedAt,
        estimatedCostUsd,
      });
      const persisted = await resultStore.persist(successResult.payload, BriefStatus.success);
      if (persisted === "duplicate") {
        await rollbackBudgetReservation(
          requestContext,
          budgetLedger,
          logger,
          dateKey,
          reservedCostUsd,
          dailyBudgetUsd
        );
        budgetReserved = false;
        incrementDuplicatesSkipped(requestContext.healthContext);
        incrementGeneration(requestContext.healthContext, "skipped");
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

      const costDeltaUsd = normalizeUsdDelta(successResult.metrics.costUsd - reservedCostUsd);
      if (costDeltaUsd !== 0) {
        try {
          spentBudgetUsd = await budgetLedger.settle({
            dateKey,
            deltaUsd: costDeltaUsd,
          });
          reservedCostUsd = normalizeUsd(successResult.metrics.costUsd);
          requestContext.healthContext.redisHealthy = true;
        } catch (error) {
          requestContext.healthContext.redisHealthy = false;
          incrementError(requestContext.healthContext, "redis_error");
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

      incrementGeneration(requestContext.healthContext, "success");
      incrementLlmCostUsd(requestContext.healthContext, successResult.metrics.costUsd);
      incrementLlmTokens(requestContext.healthContext, "input", successResult.metrics.inputTokens);
      incrementLlmTokens(requestContext.healthContext, "output", successResult.metrics.outputTokens);
      observeHighlightsCount(requestContext.healthContext, successResult.metrics.highlightsCount);
      observeCitationsCount(requestContext.healthContext, successResult.metrics.citationsCount);

      setBudgetRemainingUsd(
        requestContext.healthContext,
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
      if (budgetReserved && !persistedCreated) {
        try {
          await rollbackBudgetReservation(
            requestContext,
            budgetLedger,
            logger,
            dateKey,
            reservedCostUsd,
            dailyBudgetUsd
          );
          budgetReserved = false;
        } catch (rollbackError) {
          requestContext.healthContext.redisHealthy = false;
          incrementError(requestContext.healthContext, "redis_error");
          logger.error(
            { error: serializeError(rollbackError) },
            "Failed to roll back reserved brief budget"
          );
          throw rollbackError;
        }
      }

      const failureOutcome = await handleSummaryRequestFailure(error, {
        ctx: requestContext,
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

export function createSummaryRequestProcessor(
  overrides: SummaryRequestProcessorDependencyOverrides = {}
): SummaryRequestProcessor {
  const dependencies = buildFunctionDependencies(
    "Summary request processor dependency",
    DEFAULT_SUMMARY_REQUEST_PROCESSOR_DEPENDENCIES,
    overrides
  );
  return new DefaultSummaryRequestProcessor(dependencies);
}

const DEFAULT_SUMMARY_REQUEST_PROCESSOR = createSummaryRequestProcessor();

export async function processSummaryRequest(
  ctx: ProcessContext,
  request: ParsedSummaryRequest
): Promise<void> {
  await DEFAULT_SUMMARY_REQUEST_PROCESSOR.processSummaryRequest(ctx, request);
}
