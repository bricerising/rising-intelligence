import {
  createBriefingJobPayload,
  type CanonicalSource,
} from "@rising-intelligence/pipeline";
import { BRIEF_CONFIG_DEFAULTS } from "./config.js";
import type {
  EvidenceStrategy,
  LlmProvider,
  SummaryRequestType,
} from "./types.js";

export type {
  CanonicalSource,
  EvidenceStrategy,
  LlmProvider,
  SummaryRequestType,
};

export const BRIEF_SUPPORTED_WINDOWS = [1, 2, 3] as const;

export const BRIEF_QUERY_MODE_WINDOWS = [2] as const;

export const BRIEF_KAFKA_TOPICS = {
  summaryRequests: BRIEF_CONFIG_DEFAULTS.kafkaTopicSummaryRequests,
  summaryResults: BRIEF_CONFIG_DEFAULTS.kafkaTopicSummaryResults,
  trendSnapshots: BRIEF_CONFIG_DEFAULTS.kafkaTopicTrendSnapshots,
} as const;

export const BRIEF_OPERATION_DEFAULTS = {
  kafkaBrokers: BRIEF_CONFIG_DEFAULTS.kafkaBrokers,
  llmProvider: BRIEF_CONFIG_DEFAULTS.llmProvider,
  defaultLookbackDays: BRIEF_CONFIG_DEFAULTS.briefDefaultLookbackDays,
  maxLookbackDays: BRIEF_CONFIG_DEFAULTS.briefMaxLookbackDays,
  maxQueryEventsPerTopic: BRIEF_CONFIG_DEFAULTS.briefMaxQueryEventsPerTopic,
} as const;

export const BRIEF_TRIGGER_DEFAULTS = {
  requestType: "daily",
  windows: [...BRIEF_QUERY_MODE_WINDOWS],
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
