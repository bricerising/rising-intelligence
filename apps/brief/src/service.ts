/**
 * Brief service boundary.
 *
 * This barrel consolidates all brief-service concerns — process orchestration,
 * grounding, budget ledger, config, health, and LLM integration — behind a
 * single ownership surface.  Consumers (index.ts, brief-service.ts) import
 * from here rather than reaching into individual internal modules.
 */

// --- Config ---
export { getConfig, loadConfig, type Config } from "./config.js";

// --- Health / metrics ---
export {
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
} from "./health.js";

// --- Postgres health proxy ---
export { createPostgresHealthProxy } from "./postgres-health-proxy.js";

// --- Grounding facade ---
export {
  createSummaryRequestGroundingFacade,
  EVIDENCE_EXCERPT_MAX_LENGTH,
  type SummaryRequestGroundingFacade,
  type SummaryRequestPayloadOptions,
} from "./grounding-facade.js";

// --- Budget ledger ---
export {
  createBriefBudgetLedger,
  type BriefBudgetLedger,
  type CreateBriefBudgetLedgerInput,
  type BudgetReservationResult,
  type ReserveBudgetInput,
  type ReleaseBudgetInput,
  type SettleBudgetInput,
} from "./budget-ledger.js";

// --- Process orchestration ---
export {
  processSummaryRequest,
  createSummaryRequestProcessor,
  type ProcessContext,
  type SummaryRequestProcessor,
} from "./process.js";

// --- Domain types ---
export type {
  ParsedSummaryRequest,
  ParsedSummaryEvidence,
  ParsedSummaryTopic,
  ParsedSummaryMetric,
  ParsedSummaryQuery,
  ParsedSummaryReport,
  ParsedTrendSnapshot,
  SummaryRequestType,
  EvidenceStrategy,
  LlmProvider,
} from "./types.js";

// --- Processing errors ---
export {
  LlmGenerationError,
  NonRetryableProcessingError,
  toGroundingError,
  toNoCoverageError,
  classifyRetryableFailureCode,
} from "./processing-errors.js";

// --- Result payload / store ---
export {
  parseBriefResultPayload,
  buildFailureBriefResultPayload,
  type BriefResultPayload,
} from "./result-payload-adapter.js";
export {
  createBriefResultStore,
  type BriefResultStore,
  type StoredBriefResult,
  type PersistBriefResultOutcome,
} from "./result-store-facade.js";

// --- Publishing ---
export {
  createBriefResultPublisher,
  type BriefResultPublisher,
  type CreateBriefResultPublisherInput,
} from "./publishing-facade.js";

// --- Query-mode resolution ---
export {
  createQueryModeRequestResolver,
  type QueryModeRequestResolver,
  type QueryModeRequestResolverContext,
} from "./query-mode-request-facade.js";

// --- Deserialisation ---
export {
  deserializeSummaryRequest,
  deserializeTrendSnapshot,
} from "./deserialize.js";

// --- Topic message handlers ---
export {
  createTopicMessageHandlerMap,
  runWithInFlightHeartbeats,
  mapTrendWindowToEnum,
  type TopicMessageCommand,
  type TopicMessageHandler,
} from "./topic-message-handlers.js";

// --- Redis ---
export { createRedisClient, disconnectRedis } from "./redis.js";

// --- Runtime factory ---
export {
  createBriefRuntimeFactory,
  type BriefRuntimeContext,
  type BriefRuntimeFactory,
  type BriefRuntimeFactoryDependencies,
  type KafkaConsumerContext,
  type KafkaProducerContext,
} from "./runtime-factory.js";
