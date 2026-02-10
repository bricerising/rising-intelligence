export type SummaryRequestType = "daily" | "threshold";

export interface ParsedSummaryMetric {
  topic: string;
  window: number;
  score: number;
  volume: number;
  acceleration: number;
}

export interface ParsedSummaryEvidence {
  eventId: string;
  source: string;
  url: string | null;
  title: string | null;
  publishedAt: Date | null;
  fetchedAt: Date | null;
  textExcerpt: string | null;
}

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

export interface ParsedTrendSnapshot {
  generatedAt: Date;
  window: number;
  snapshot: unknown; // Raw JSON to be stored in Postgres
}
