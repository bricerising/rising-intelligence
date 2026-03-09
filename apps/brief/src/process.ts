/**
 * Summary request processing orchestrator.
 *
 * Coordinates the end-to-end lifecycle of a summary request:
 * request-scoped logging → query-mode resolution → prepared briefing execution.
 *
 * Domain concerns (evidence scoring, grounding enforcement, LLM provider
 * strategies, failure handling) are delegated to focused internal modules
 * behind the internals barrel.
 *
 * Once request preparation is complete, budgeting, generation, persistence,
 * publishing, failure handling, and metrics are mediated through the
 * BriefOrchestrator boundary.
 */

import type { PrismaClient } from "@rising-intelligence/db";
import type { ProducerConnection } from "@rising-intelligence/pipeline/transport";
import type { Redis } from "ioredis";
import {
  buildFunctionDependencies,
  type FunctionDependencyOverrides,
} from "@rising-intelligence/shared/lifecycle";
import type pino from "pino";
import {
  createBriefBudgetLedger,
  type BriefBudgetLedger,
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
  type SummaryRequestGenerationFacade,
} from "./internals.js";
import {
  createBriefingInput,
  createBriefOrchestrator,
  type BriefingInput,
  type BriefOrchestrator,
  type OrchestratorContext,
  type OrchestratorRuntime,
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
  return ctx.groundingFacade ?? DEFAULT_GROUNDING_FACADE;
}

const DEFAULT_GROUNDING_FACADE = createSummaryRequestGroundingFacade();

export interface SummaryRequestProcessor {
  processSummaryRequest(ctx: ProcessContext, request: ParsedSummaryRequest): Promise<void>;
}

interface SummaryRequestProcessorDependencies {
  createBriefBudgetLedger(input: CreateBriefBudgetLedgerInput): BriefBudgetLedger;
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
  briefingInput: BriefingInput;
  orchestratorContext: OrchestratorContext;
  orchestratorRuntime: OrchestratorRuntime;
}

const DEFAULT_SUMMARY_REQUEST_PROCESSOR_DEPENDENCIES: SummaryRequestProcessorDependencies = {
  createBriefBudgetLedger,
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

    const orchestratorContext: OrchestratorContext = {
      config: ctx.config,
      logger,
      healthContext: ctx.healthContext,
      prisma: ctx.prisma,
      redis: ctx.redis,
      producer: ctx.producer,
    };

    const orchestratorRuntime: OrchestratorRuntime = {
      budgetLedger: this.dependencies.createBriefBudgetLedger({
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
        orchestratorContext,
        request,
        logger
      );
    } catch (error) {
      if (error instanceof NonRetryableProcessingError) {
        await handleNonRetryableFailure({
          ctx: orchestratorContext,
          resultStore: orchestratorRuntime.resultStore,
          publisher: orchestratorRuntime.publisher,
          requestId: request.requestId,
          producedAt: orchestratorRuntime.producedAt,
          error,
          logger,
          logMessage: "Summary request failed non-retryable pre-processing",
        });
        return null;
      }

      incrementError(orchestratorContext.healthContext, "generation_error");
      incrementGeneration(orchestratorContext.healthContext, "failure");
      logger.error(
        { error: serializeError(error) },
        "Failed to resolve summary request into briefing input"
      );
      throw error;
    }

    return {
      briefingInput: createBriefingInput(resolvedRequest),
      orchestratorContext,
      orchestratorRuntime,
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

    const { briefingInput, orchestratorContext, orchestratorRuntime } = runtime;

    await this.orchestrator.execute(
      orchestratorContext,
      orchestratorRuntime,
      briefingInput
    );
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
