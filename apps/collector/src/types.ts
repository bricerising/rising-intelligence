import type { CanonicalSource } from "@rising-intelligence/pipeline";

/**
 * Source values accepted by collector adapters.
 * Extends the shared CanonicalSource contract with "lobsters", a collector-
 * internal alias that the serializer resolves to "news" on the wire.
 */
export type Source = CanonicalSource | "lobsters";

export interface Author {
  id?: string;
  handle?: string;
  display_name?: string;
}

export interface Engagement {
  score?: number;
  comments?: number;
  likes?: number;
  shares?: number;
}

export interface Extracted {
  hashtags?: string[];
  urls?: string[];
}

/**
 * Canonical RawEvent schema matching the proto contract.
 * All ingested items are normalized to this format before publishing to Kafka.
 */
export interface RawEvent {
  event_id: string;
  source: Source;
  fetched_at: string; // ISO8601
  published_at?: string; // ISO8601

  url?: string;
  title?: string;
  text: string;

  author?: Author;
  engagement?: Engagement;

  lang?: string;
  tags?: string[]; // Canonical topic keys populated by topic extraction
  extracted?: Extracted;

  source_meta?: Record<string, unknown>;
}

/**
 * Dead letter event for failed parse/normalize.
 */
export interface DeadLetterEvent {
  dlq_id: string;
  occurred_at: string; // ISO8601
  source: string;
  error_code: string;
  error_message: string;
  raw_reference?: string;
  raw_payload_excerpt?: string;
}

/**
 * Collector heartbeat for data freshness validation.
 */
export type CollectorStatus = "healthy" | "degraded" | "error";

export interface CollectorHeartbeat {
  source: Source;
  timestamp: string; // ISO8601
  last_fetch_at: string; // ISO8601
  items_fetched: number;
  status: CollectorStatus;
  error_message?: string;
}

/**
 * Adapter interface for source-specific ingestion logic.
 */
export interface SourceAdapter {
  /** Unique name for this adapter (used in logs and metrics) */
  readonly name: string;

  /** Source type for RawEvent */
  readonly source: Source;

  /** Poll interval in milliseconds */
  readonly pollIntervalMs: number;

  /** Initialize the adapter (called once on startup) */
  initialize(): Promise<void>;

  /** Fetch new items and yield them with checkpoint data */
  fetch(): AsyncIterable<FetchResult>;

  /** Clean up resources on shutdown */
  shutdown(): Promise<void>;
}

export interface FetchResult {
  event: RawEvent;
  checkpointKey: string;
  checkpointValue: string;
}
