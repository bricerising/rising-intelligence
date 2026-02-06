export type SummaryRequestType = "daily" | "threshold";

export interface ParsedSummaryTopic {
  topic: string;
  evidenceCount: number;
}

export interface ParsedSummaryRequest {
  requestId: string;
  requestedAt: Date;
  type: SummaryRequestType;
  windows: number[];
  topics: ParsedSummaryTopic[];
}
