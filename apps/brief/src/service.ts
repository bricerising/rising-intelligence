/**
 * Brief service boundary.
 *
 * This barrel is the single public API of the brief service.  The entrypoint
 * (index.ts) and message handler (brief-service.ts) import exclusively from
 * here.  Internal orchestrators (process.ts, runtime-factory.ts) import from
 * the narrower internals barrel instead.
 *
 * Layering (entrypoint → orchestration → domain → infrastructure):
 *
 *   service.ts          ← public boundary (this file)
 *     ├─ process.ts          ← orchestration
 *     ├─ runtime-factory.ts  ← orchestration / composition root
 *     └─ internals.ts        ← foundation barrel
 *          ├─ config, types, processing-errors     (config & types)
 *          ├─ health, postgres-health-proxy         (health / metrics)
 *          ├─ redis, budget-ledger, deserialize,    (infrastructure adapters)
 *          │  topic-message-handlers, result-*,
 *          │  publishing-facade, llm/codex-cli
 *          └─ grounding-facade, query-mode-*,       (domain logic)
 *             internal-highlight-strategy
 *
 * Only symbols required by the entrypoint and message handler are re-exported
 * here.  Orchestration-internal symbols (e.g. executeCodexCli,
 * countTopicRelevanceTermMatches, buildInternalSuggestedAction) remain
 * accessible only through internals.ts.
 */

// ── Config & types (from internals) ─────────────────────────────────────────

export {
  getConfig,
  loadConfig,
  type Config,
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
  LlmGenerationError,
  NonRetryableProcessingError,
  toGroundingError,
  toNoCoverageError,
  classifyRetryableFailureCode,
} from "./internals.js";

// ── Health & monitoring (from internals) ────────────────────────────────────

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
  createPostgresHealthProxy,
} from "./internals.js";

// ── Infrastructure adapters (from internals) ────────────────────────────────

export {
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

// ── Process orchestration ───────────────────────────────────────────────────

export {
  processSummaryRequest,
  createSummaryRequestProcessor,
  type ProcessContext,
  type SummaryRequestProcessor,
} from "./process.js";

// ── Runtime factory ─────────────────────────────────────────────────────────

export {
  createBriefRuntimeFactory,
  type BriefRuntimeContext,
  type BriefRuntimeFactory,
  type BriefRuntimeFactoryDependencies,
  type KafkaConsumerContext,
  type KafkaProducerContext,
} from "./runtime-factory.js";
