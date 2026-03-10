import {
  createBriefingJobPayload,
  type CanonicalSource,
} from "@rising-intelligence/pipeline";
import {
  BRIEF_CONFIG_DEFAULTS,
  getConfig,
  loadConfig,
  type Config,
} from "./config.js";
import {
  createHealthContext,
  type HealthContext,
} from "./health.js";
import {
  createSummaryRequestGroundingFacade,
  EVIDENCE_EXCERPT_MAX_LENGTH,
  type SummaryRequestGroundingFacade,
} from "./grounding-facade.js";
import {
  createBriefBudgetLedger,
  createBriefBudgetGovernor,
  type AuthorizeBudgetInput,
  type BriefBudgetDecision,
  type BriefBudgetGovernor,
  type BriefBudgetLedger,
  type CreateBriefBudgetLedgerInput,
  type SettleBudgetDecisionInput,
} from "./budget-ledger.js";
import {
  createSummaryRequestProcessor,
  processSummaryRequest,
  type ProcessContext,
  type SummaryRequestProcessor,
} from "./process.js";
import {
  createBriefRuntimeFactory,
  type BriefRuntimeContext,
  type BriefRuntimeFactory,
  type BriefRuntimeFactoryDependencies,
  type KafkaConsumerContext,
  type KafkaProducerContext,
} from "./runtime-factory.js";
import { createServiceBootstrap } from "./service-runtime.js";
import type {
  EvidenceStrategy,
  LlmProvider,
  ParsedSummaryRequest,
  SummaryRequestType,
} from "./types.js";

export {
  BRIEF_CONFIG_DEFAULTS,
  createHealthContext,
  createSummaryRequestGroundingFacade,
  EVIDENCE_EXCERPT_MAX_LENGTH,
  createBriefBudgetLedger,
  createBriefBudgetGovernor,
  createSummaryRequestProcessor,
  processSummaryRequest,
  createBriefRuntimeFactory,
  createServiceBootstrap,
  loadConfig,
  getConfig,
};

export type {
  AuthorizeBudgetInput,
  BriefBudgetDecision,
  BriefBudgetGovernor,
  BriefBudgetLedger,
  BriefRuntimeContext,
  BriefRuntimeFactory,
  BriefRuntimeFactoryDependencies,
  Config,
  CreateBriefBudgetLedgerInput,
  EvidenceStrategy,
  HealthContext,
  KafkaConsumerContext,
  KafkaProducerContext,
  LlmProvider,
  ParsedSummaryRequest,
  ProcessContext,
  SettleBudgetDecisionInput,
  SummaryRequestGroundingFacade,
  SummaryRequestProcessor,
  SummaryRequestType,
};

export const BRIEF_KAFKA_TOPICS = {
  summaryRequests: BRIEF_CONFIG_DEFAULTS.kafkaTopicSummaryRequests,
  summaryResults: BRIEF_CONFIG_DEFAULTS.kafkaTopicSummaryResults,
  trendSnapshots: BRIEF_CONFIG_DEFAULTS.kafkaTopicTrendSnapshots,
} as const;

export const BRIEF_TRIGGER_DEFAULTS = {
  requestType: "daily",
  windows: [2],
  dailyBudgetUsd: BRIEF_CONFIG_DEFAULTS.llmDailyBudgetUsd,
  maxTopics: 5,
  maxEvidencePerTopic: 3,
  maxOutputTokens: 1200,
  queryEvidenceStrategy: "diversity",
} as const satisfies {
  requestType: SummaryRequestType;
  windows: readonly number[];
  dailyBudgetUsd: number;
  maxTopics: number;
  maxEvidencePerTopic: number;
  maxOutputTokens: number;
  queryEvidenceStrategy: EvidenceStrategy;
};

export interface BriefSummaryRequestBudgetInput {
  dailyBudgetUsd: number;
  maxTopics: number;
  maxEvidencePerTopic: number;
  maxOutputTokens: number;
}

export interface BriefSummaryRequestReportInput {
  timezone?: string;
  startAt?: string;
  endAt?: string;
}

export interface BriefSummaryRequestQueryInput {
  lookbackDays: number;
  topicGlobs: string[];
  maxEventsPerTopic: number;
  evidenceStrategy: EvidenceStrategy;
}

export interface BriefSummaryRequestMetricInput {
  topic: string;
  window: number;
  score: number;
  volume: number;
  acceleration: number;
}

export interface BriefSummaryRequestEvidenceInput {
  eventId: string;
  source: CanonicalSource;
  url: string;
  title: string;
  publishedAt: string;
  fetchedAt: string;
  textExcerpt: string;
}

export interface BriefSummaryRequestTopicInput {
  topic: string;
  metrics: BriefSummaryRequestMetricInput[];
  evidence: BriefSummaryRequestEvidenceInput[];
}

export interface CreateBriefSummaryRequestInput {
  requestId: string;
  requestedAt: string;
  type: SummaryRequestType;
  windows: number[];
  budget?: BriefSummaryRequestBudgetInput;
  query?: BriefSummaryRequestQueryInput;
  report?: BriefSummaryRequestReportInput;
  llmProvider?: LlmProvider;
  topics: BriefSummaryRequestTopicInput[];
}

export interface BriefSummaryRequestPayload {
  request_id: string;
  requested_at: string;
  type: SummaryRequestType;
  windows: number[];
  budget?: {
    daily_budget_usd: number;
    max_topics: number;
    max_evidence_per_topic: number;
    max_output_tokens: number;
  };
  query?: {
    lookback_days: number;
    topic_globs: string[];
    max_events_per_topic: number;
    evidence_strategy: EvidenceStrategy;
  };
  report?: {
    timezone?: string;
    start_at?: string;
    end_at?: string;
  };
  llm_provider?: LlmProvider;
  topics: Array<{
    topic: string;
    metrics: Array<{
      topic: string;
      window: number;
      score: number;
      volume: number;
      acceleration: number;
    }>;
    evidence: Array<{
      event_id: string;
      source: CanonicalSource;
      url: string;
      title: string;
      published_at: string;
      fetched_at: string;
      text_excerpt: string;
    }>;
  }>;
}

export function createBriefSummaryRequest(
  input: CreateBriefSummaryRequestInput
): BriefSummaryRequestPayload {
  return createBriefingJobPayload(input) as BriefSummaryRequestPayload;
}
