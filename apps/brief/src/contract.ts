import type { CanonicalSource } from "@rising-intelligence/pipeline";
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

function buildReport(
  report: BriefSummaryRequestReportInput | undefined
): BriefSummaryRequestPayload["report"] | undefined {
  if (!report) {
    return undefined;
  }

  const payload = {
    ...(report.timezone ? { timezone: report.timezone } : {}),
    ...(report.startAt ? { start_at: report.startAt } : {}),
    ...(report.endAt ? { end_at: report.endAt } : {}),
  };

  return Object.keys(payload).length > 0 ? payload : undefined;
}

export function createBriefSummaryRequest(
  input: CreateBriefSummaryRequestInput
): BriefSummaryRequestPayload {
  const report = buildReport(input.report);

  return {
    request_id: input.requestId,
    requested_at: input.requestedAt,
    type: input.type,
    windows: [...input.windows],
    ...(input.budget
      ? {
          budget: {
            daily_budget_usd: input.budget.dailyBudgetUsd,
            max_topics: input.budget.maxTopics,
            max_evidence_per_topic: input.budget.maxEvidencePerTopic,
            max_output_tokens: input.budget.maxOutputTokens,
          },
        }
      : {}),
    ...(input.query
      ? {
          query: {
            lookback_days: input.query.lookbackDays,
            topic_globs: [...input.query.topicGlobs],
            max_events_per_topic: input.query.maxEventsPerTopic,
            evidence_strategy: input.query.evidenceStrategy,
          },
        }
      : {}),
    ...(report ? { report } : {}),
    ...(input.llmProvider ? { llm_provider: input.llmProvider } : {}),
    topics: input.topics.map((topic) => ({
      topic: topic.topic,
      metrics: topic.metrics.map((metric) => ({
        topic: metric.topic,
        window: metric.window,
        score: metric.score,
        volume: metric.volume,
        acceleration: metric.acceleration,
      })),
      evidence: topic.evidence.map((evidence) => ({
        event_id: evidence.eventId,
        source: evidence.source,
        url: evidence.url,
        title: evidence.title,
        published_at: evidence.publishedAt,
        fetched_at: evidence.fetchedAt,
        text_excerpt: evidence.textExcerpt,
      })),
    })),
  };
}
