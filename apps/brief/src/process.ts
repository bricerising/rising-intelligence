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
 *
 * All subsystem interactions (budgeting, generation, persistence, publishing,
 * failure handling, metrics) are mediated through the BriefOrchestrator
 * boundary, collapsing process.ts's multi-cluster fan-out into a single
 * internal dependency.
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
  // LLM generation facade
  createSummaryRequestGenerationFacade,
  type SummaryRequestGenerationFacade,
} from "./internals.js";
import {
  createBriefOrchestrator,
  type BriefOrchestrator,
  type OrchestratorContext,
  type OrchestratorRuntime,
} from "./brief-orchestrator.js";

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
  orchestratorContext: OrchestratorContext;
  orchestratorRuntime: OrchestratorRuntime;
  logger: pino.Logger;
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

  create(ctx: ProcessContext, request: ParsedSummaryRequest): SummaryRequestRuntime {
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
      queryModeRequestResolver: this.queryModeRequestResolver,
      generationFacade: createSummaryRequestGenerationFacade(groundingFacade),
      producedAt: new Date(),
    };

    return { orchestratorContext, orchestratorRuntime, logger };
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
    const { orchestratorContext, orchestratorRuntime, logger } =
      this.runtimeFactory.create(ctx, request);

    await this.orchestrator.execute(
      orchestratorContext,
      orchestratorRuntime,
      request,
      logger
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
