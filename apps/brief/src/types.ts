import type { BriefEvidenceRecord } from "@rising-intelligence/pipeline";

export type SummaryRequestType = "daily" | "threshold";

export interface ParsedSummaryMetric {
  topic: string;
  window: number;
  score: number;
  volume: number;
  acceleration: number;
}

export type ParsedSummaryEvidence = BriefEvidenceRecord;

export interface ParsedSummaryTopic {
  topic: string;
  metrics: ParsedSummaryMetric[];
  evidence: ParsedSummaryEvidence[];
}

export type EvidenceStrategy = "diversity" | "recency" | "engagement";
export type LlmProvider = "internal" | "http" | "codex-cli";

export interface ParsedSummaryQuery {
  lookbackDays?: number;
  topicGlobs?: string[];
  maxEventsPerTopic?: number;
  evidenceStrategy?: EvidenceStrategy;
}

export interface ParsedSummaryReport {
  timezone?: string;
  startAt?: Date;
  endAt?: Date;
}

export interface ParsedSummaryRequest {
  requestId: string;
  requestedAt: Date;
  type: SummaryRequestType;
  windows: number[];
  budget:
    | {
        dailyBudgetUsd?: number;
        maxTopics?: number;
        maxEvidencePerTopic?: number;
        maxOutputTokens?: number;
      }
    | null;
  query: ParsedSummaryQuery | null;
  report: ParsedSummaryReport | null;
  topics: ParsedSummaryTopic[];
  llmProvider?: LlmProvider; // Override LLM provider for this request
  coverageWarnings?: string[]; // Query-mode warnings about incomplete data
}

export interface BriefOrchestrationBudget {
  dailyBudgetUsd?: number;
  maxTopics?: number;
  maxEvidencePerTopic?: number;
  maxOutputTokens?: number;
}

export interface BriefOrchestrationQuery {
  lookbackDays?: number;
  topicGlobs?: string[];
  maxEventsPerTopic?: number;
  evidenceStrategy?: EvidenceStrategy;
}

export interface BriefOrchestrationReport {
  timezone?: string;
  startAt?: Date;
  endAt?: Date;
}

export interface BriefOrchestrationRequest {
  requestId: string;
  requestedAt: Date;
  type: SummaryRequestType;
  windows: number[];
  budget: BriefOrchestrationBudget | null;
  query: BriefOrchestrationQuery | null;
  report: BriefOrchestrationReport | null;
  topics: ParsedSummaryTopic[];
  llmProvider?: LlmProvider;
  coverageWarnings?: string[];
}

function cloneDate(value: Date | null | undefined): Date | undefined {
  if (!value) {
    return undefined;
  }
  return new Date(value.getTime());
}

function cloneSummaryEvidence(
  evidence: ParsedSummaryEvidence
): ParsedSummaryEvidence {
  return {
    ...evidence,
    publishedAt: cloneDate(evidence.publishedAt) ?? null,
    fetchedAt: cloneDate(evidence.fetchedAt) ?? null,
  };
}

function cloneSummaryTopic(topic: ParsedSummaryTopic): ParsedSummaryTopic {
  return {
    topic: topic.topic,
    metrics: topic.metrics.map((metric) => ({ ...metric })),
    evidence: topic.evidence.map(cloneSummaryEvidence),
  };
}

export function createBriefOrchestrationRequest(
  request: ParsedSummaryRequest
): BriefOrchestrationRequest {
  return {
    requestId: request.requestId,
    requestedAt: new Date(request.requestedAt.getTime()),
    type: request.type,
    windows: [...request.windows],
    budget: request.budget ? { ...request.budget } : null,
    query: request.query
      ? {
          ...request.query,
          topicGlobs: request.query.topicGlobs
            ? [...request.query.topicGlobs]
            : undefined,
        }
      : null,
    report: request.report
      ? {
          timezone: request.report.timezone,
          startAt: cloneDate(request.report.startAt),
          endAt: cloneDate(request.report.endAt),
        }
      : null,
    topics: request.topics.map(cloneSummaryTopic),
    llmProvider: request.llmProvider,
    coverageWarnings: request.coverageWarnings
      ? [...request.coverageWarnings]
      : undefined,
  };
}

export type PreparedBriefingRequest = BriefOrchestrationRequest;
export const prepareBriefingRequest = createBriefOrchestrationRequest;

export interface ParsedTrendSnapshot {
  generatedAt: Date;
  window: number;
  snapshot: unknown; // Raw JSON to be stored in Postgres
}
