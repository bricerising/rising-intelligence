/**
 * Brief service internal barrel.
 *
 * Consolidates all foundation modules behind a single import surface.
 * Higher-level orchestrators (process.ts, runtime-factory.ts) import from
 * here rather than reaching into individual internal modules.
 *
 * The outer service barrel (service.ts) re-exports a curated subset of this
 * surface to form the public API visible to the entrypoint and message handler.
 *
 * Internal layering (bottom → top):
 *
 *   Layer 1 — Config & types        (config, types, processing-errors)
 *   Layer 2 — Health & monitoring    (health, postgres-health-proxy)
 *   Layer 3 — Infrastructure adapters (redis, budget-mirror, budget-ledger,
 *             deserialize, topic-message-handlers, topic-glob,
 *             result-payload-adapter, result-store-facade,
 *             publishing-facade, llm/codex-cli)
 *   Layer 4 — Domain logic           (grounding-facade, query-mode-selection,
 *             internal-highlight-strategy, query-mode-request-facade)
 */

// ─── Layer 1: Config & types ────────────────────────────────────────────────

export { getConfig, loadConfig, type Config } from "./config.js";

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

export {
  LlmGenerationError,
  NonRetryableProcessingError,
  toGroundingError,
  toNoCoverageError,
  classifyRetryableFailureCode,
} from "./processing-errors.js";

// ─── Layer 2: Health & monitoring ───────────────────────────────────────────

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

export { createPostgresHealthProxy } from "./postgres-health-proxy.js";

// ─── Layer 3: Infrastructure adapters ───────────────────────────────────────

export { createRedisClient, disconnectRedis } from "./redis.js";

export {
  createBriefBudgetLedger,
  type BriefBudgetLedger,
  type CreateBriefBudgetLedgerInput,
  type BudgetReservationResult,
  type ReserveBudgetInput,
  type ReleaseBudgetInput,
  type SettleBudgetInput,
} from "./budget-ledger.js";

export {
  deserializeSummaryRequest,
  deserializeTrendSnapshot,
} from "./deserialize.js";

export {
  createTopicMessageHandlerMap,
  runWithInFlightHeartbeats,
  mapTrendWindowToEnum,
  type TopicMessageCommand,
  type TopicMessageHandler,
} from "./topic-message-handlers.js";

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

export {
  createBriefResultPublisher,
  type BriefResultPublisher,
  type CreateBriefResultPublisherInput,
} from "./publishing-facade.js";

export { executeCodexCli } from "./llm/codex-cli.js";

// ─── Layer 4: Domain logic ──────────────────────────────────────────────────

export {
  createSummaryRequestGroundingFacade,
  EVIDENCE_EXCERPT_MAX_LENGTH,
  type SummaryRequestGroundingFacade,
  type SummaryRequestPayloadOptions,
} from "./grounding-facade.js";

export {
  countTopicRelevanceTermMatches,
  getTopLevelTopicGroup,
} from "./query-mode-selection.js";

export {
  buildInternalSuggestedAction,
  buildInternalWhyItMatters,
  detectSignalCategories,
  type SignalCategory,
} from "./internal-highlight-strategy.js";

export {
  createQueryModeRequestResolver,
  type QueryModeRequestResolver,
  type QueryModeRequestResolverContext,
} from "./query-mode-request-facade.js";
