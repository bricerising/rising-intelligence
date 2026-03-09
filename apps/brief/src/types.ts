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

export interface PreparedBriefingRequest
  extends Pick<
    ParsedSummaryRequest,
    | "requestId"
    | "requestedAt"
    | "type"
    | "windows"
    | "budget"
    | "query"
    | "report"
    | "topics"
    | "llmProvider"
    | "coverageWarnings"
  > {}

export function prepareBriefingRequest(
  request: ParsedSummaryRequest
): PreparedBriefingRequest {
  return {
    requestId: request.requestId,
    requestedAt: request.requestedAt,
    type: request.type,
    windows: request.windows,
    budget: request.budget,
    query: request.query,
    report: request.report,
    topics: request.topics,
    llmProvider: request.llmProvider,
    coverageWarnings: request.coverageWarnings,
  };
}

export interface ParsedTrendSnapshot {
  generatedAt: Date;
  window: number;
  snapshot: unknown; // Raw JSON to be stored in Postgres
}
