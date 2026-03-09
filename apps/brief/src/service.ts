/**
 * Brief service boundary.
 *
 * This barrel consolidates all brief-service concerns — process orchestration,
 * grounding, budget ledger, config, health, and LLM integration — behind a
 * single ownership surface.  Consumers (index.ts, brief-service.ts) import
 * from here rather than reaching into individual internal modules.
 *
 * Internally the service is organized in two layers:
 *
 *   internals  — foundation modules (config, health, types, errors,
 *                grounding, budget, persistence, publishing, query-mode,
 *                deserialization, topic handlers, redis, LLM adapters)
 *
 *   process / runtime-factory — orchestration that depends on the internals
 *
 * This file re-exports both layers as the full public API.
 */

// --- Foundation (internals barrel) ---
export {
  // Config
  getConfig,
  loadConfig,
  type Config,

  // Health / metrics
  createHealthContext,
  startHealthServer,
  getHealthStatus,
  formatMetrics,
  createHandlers,
  setBudgetRemainingUsd,
  incrementGeneration,
  incrementError,
  incrementDuplicatesSkipped,
  incrementBudgetExceeded,
  incrementSuspiciousContent,
  incrementLlmCostUsd,
  incrementLlmTokens,
  observeHighlightsCount,
  observeCitationsCount,
  observeGenerationDuration,
  type HealthContext,
  type HealthStatus,
  type BriefGenerationStatus,
  type TokenDirection,
  type Metrics,

  // Postgres health proxy
  createPostgresHealthProxy,

  // Grounding facade
  createSummaryRequestGroundingFacade,
  EVIDENCE_EXCERPT_MAX_LENGTH,
  type SummaryRequestGroundingFacade,
  type SummaryRequestPayloadOptions,

  // Budget ledger
  createBriefBudgetLedger,
  type BriefBudgetLedger,
  type CreateBriefBudgetLedgerInput,
  type BudgetReservationResult,
  type ReserveBudgetInput,
  type ReleaseBudgetInput,
  type SettleBudgetInput,

  // Domain types
  type ParsedSummaryRequest,
  type ParsedSummaryEvidence,
  type ParsedSummaryTopic,
  type ParsedSummaryMetric,
  type ParsedSummaryQuery,
  type ParsedSummaryReport,
  type ParsedTrendSnapshot,
  type SummaryRequestType,
  type EvidenceStrategy,
  type LlmProvider,

  // Processing errors
  LlmGenerationError,
  NonRetryableProcessingError,
  toGroundingError,
  toNoCoverageError,
  classifyRetryableFailureCode,

  // Result payload / store
  parseBriefResultPayload,
  buildFailureBriefResultPayload,
  type BriefResultPayload,
  createBriefResultStore,
  type BriefResultStore,
  type StoredBriefResult,
  type PersistBriefResultOutcome,

  // Publishing
  createBriefResultPublisher,
  type BriefResultPublisher,
  type CreateBriefResultPublisherInput,

  // Query-mode resolution
  createQueryModeRequestResolver,
  type QueryModeRequestResolver,
  type QueryModeRequestResolverContext,

  // Deserialisation
  deserializeSummaryRequest,
  deserializeTrendSnapshot,

  // Topic message handlers
  createTopicMessageHandlerMap,
  runWithInFlightHeartbeats,
  mapTrendWindowToEnum,
  type TopicMessageCommand,
  type TopicMessageHandler,

  // Redis
  createRedisClient,
  disconnectRedis,
} from "./internals.js";

// --- Process orchestration ---
export {
  processSummaryRequest,
  createSummaryRequestProcessor,
  type ProcessContext,
  type SummaryRequestProcessor,
} from "./process.js";

// --- Runtime factory ---
export {
  createBriefRuntimeFactory,
  type BriefRuntimeContext,
  type BriefRuntimeFactory,
  type BriefRuntimeFactoryDependencies,
  type KafkaConsumerContext,
  type KafkaProducerContext,
} from "./runtime-factory.js";
