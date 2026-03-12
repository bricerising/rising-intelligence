import type { CanonicalSource } from "@rising-intelligence/pipeline";

export type Source = CanonicalSource;

export type TrendWindow = "15m" | "60m";

export interface ParsedRawEvent {
  eventId: string;
  source: Source;
  fetchedAt: Date;
  publishedAt: Date | null;
  url: string | null;
  title: string | null;
  text: string;
  tags: string[];
  engagementScore: number;
  feedPriority: number;
}

export interface TopicSnapshotMetric {
  topic: string;
  window: TrendWindow;
  volume: number;
  prevVolume: number;
  acceleration: number;
  baselineVolume: number;
  baselineDelta: number;
  score: number;
  evidenceEventIds: string[];
}
