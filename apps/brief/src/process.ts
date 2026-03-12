/**
 * Summary request processing orchestrator.
 *
 * Coordinates the end-to-end lifecycle of a summary request:
 * request-scoped logging → query-mode resolution → brief orchestration execution.
 *
 * Domain concerns (evidence scoring, grounding enforcement, LLM provider
 * strategies, failure handling) are delegated to focused internal modules
 * behind the internals barrel.
 *
 * Once request translation is complete, a prepared brief execution job is
 * handed to the BriefOrchestrator boundary for budgeting, generation,
 * persistence, publishing, failure handling, and metrics.
 */

import type { PrismaClient } from "@rising-intelligence/db";
import type { ProducerConnection } from "@rising-intelligence/pipeline/transport";
import type { Redis } from "ioredis";
import {
  buildFunctionDependencies,
  type FunctionDependencyOverrides,
} from "@rising-intelligence/shared/runtime";
import type pino from "pino";
import {
  createBriefBudgetGovernor,
  type BriefBudgetGovernor,
  type CreateBriefBudgetLedgerInput,
  type Config,
  type HealthContext,
  incrementError,
  incrementGeneration,
  createSummaryRequestGroundingFacade,
  type SummaryRequestGroundingFacade,
  createBriefResultPublisher,
  type CreateBriefResultPublisherInput,
  type BriefResultPublisher,
  createQueryModeRequestResolver,
  type QueryModeRequestResolver,
  createBriefResultStore,
  type BriefResultStore,
  type ParsedSummaryRequest,
  handleNonRetryableFailure,
  NonRetryableProcessingError,
  // LLM generation facade
  createSummaryRequestGenerationFacade,
} from "./internals.js";
import {
  createBriefExecutionJob,
  createBriefOrchestrator,
  type BriefOrchestrator,
  type BriefExecutionJob,
} from "./brief-orchestrator.js";
import { serializeError } from "@rising-intelligence/shared/errors";

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
  return ctx.groundingFacade ?? createSummaryRequestGroundingFacade();
}

export interface SummaryRequestProcessor {
  processSummaryRequest(ctx: ProcessContext, request: ParsedSummaryRequest): Promise<void>;
}

interface SummaryRequestProcessorDependencies {
  createBriefBudgetGovernor(input: CreateBriefBudgetLedgerInput): BriefBudgetGovernor;
  createBriefResultPublisher(input: CreateBriefResultPublisherInput): BriefResultPublisher;
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
  job: BriefExecutionJob;
}

const DEFAULT_SUMMARY_REQUEST_PROCESSOR_DEPENDENCIES: SummaryRequestProcessorDependencies = {
  createBriefBudgetGovernor,
  createBriefResultPublisher(input): BriefResultPublisher {
    return createBriefResultPublisher(input);
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

  async create(
    ctx: ProcessContext,
    request: ParsedSummaryRequest
  ): Promise<SummaryRequestRuntime | null> {
    const logger = ctx.logger.child({ requestId: request.requestId });
    const groundingFacade = resolveGroundingFacade(ctx);

    const environment: BriefExecutionJob["environment"] = {
      config: ctx.config,
      logger,
      healthContext: ctx.healthContext,
      prisma: ctx.prisma,
      redis: ctx.redis,
    };

    const services: BriefExecutionJob["services"] = {
      budgetGovernor: this.dependencies.createBriefBudgetGovernor({
        prisma: ctx.prisma,
        redis: ctx.redis,
        logger,
      }),
      publisher: this.dependencies.createBriefResultPublisher({
        connection: ctx.producer,
        logger,
        topic: ctx.config.KAFKA_TOPIC_SUMMARY_RESULTS,
      }),
      resultStore: this.dependencies.createBriefResultStore(
        ctx.prisma,
        ctx.healthContext
      ),
      generationFacade: createSummaryRequestGenerationFacade(groundingFacade),
      producedAt: new Date(),
    };

    let resolvedRequest: ParsedSummaryRequest;
    try {
      resolvedRequest = await this.queryModeRequestResolver.resolve(
        environment,
        request,
        logger
      );
    } catch (error) {
      if (error instanceof NonRetryableProcessingError) {
        await handleNonRetryableFailure({
          ctx: environment,
          resultStore: services.resultStore,
          publisher: services.publisher,
          requestId: request.requestId,
          producedAt: services.producedAt,
          error,
          logger,
          logMessage: "Summary request failed non-retryable pre-processing",
        });
        return null;
      }

      incrementError(environment.healthContext, "generation_error");
      incrementGeneration(environment.healthContext, "failure");
      logger.error(
        { error: serializeError(error) },
        "Failed to resolve summary request into a brief execution job"
      );
      throw error;
    }

    return {
      job: createBriefExecutionJob({
        request: resolvedRequest,
        environment,
        services,
      }),
    };
  }
}

class DefaultSummaryRequestProcessor implements SummaryRequestProcessor {
  private readonly runtimeFactory: SummaryRequestRuntimeFactory;
  private readonly orchestrator: BriefOrchestrator;

  constructor(dependencies: SummaryRequestProcessorDependencies) {
    this.runtimeFactory = new SummaryRequestRuntimeFactory(dependencies);
    this.orchestrator = createBriefOrchestrator();
  }

  async processSummaryRequest(
    ctx: ProcessContext,
    request: ParsedSummaryRequest
  ): Promise<void> {
    const runtime = await this.runtimeFactory.create(ctx, request);
    if (!runtime) {
      return;
    }

    await this.orchestrator.execute(runtime.job);
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

export async function processSummaryRequest(
  ctx: ProcessContext,
  request: ParsedSummaryRequest
): Promise<void> {
  const processor = createSummaryRequestProcessor();
  await processor.processSummaryRequest(ctx, request);
}
