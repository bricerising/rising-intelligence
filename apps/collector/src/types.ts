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

export interface RawEventAuthorInput {
  id?: string | null;
  handle?: string | null;
  display_name?: string | null;
}

export interface RawEventEngagementInput {
  score?: number | null;
  comments?: number | null;
  likes?: number | null;
  shares?: number | null;
}

export interface RawEventExtractedInput {
  hashtags?: Array<string | null | undefined> | null;
  urls?: Array<string | null | undefined> | null;
}

export interface CreateRawEventInput {
  event_id: string;
  source: Source;
  fetched_at: string;
  published_at?: string | null;
  url?: string | null;
  title?: string | null;
  text: string;
  author?: RawEventAuthorInput | null;
  engagement?: RawEventEngagementInput | null;
  lang?: string | null;
  tags?: Array<string | null | undefined> | null;
  extracted?: RawEventExtractedInput | null;
  source_meta?: Record<string, unknown> | null;
}

function normalizeOptionalString(value: string | null | undefined): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized.length > 0 ? normalized : undefined;
}

function normalizeStringArray(
  values: Array<string | null | undefined> | null | undefined
): string[] | undefined {
  if (!values || values.length === 0) {
    return undefined;
  }

  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const trimmed = normalizeOptionalString(value);
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    normalized.push(trimmed);
  }

  return normalized.length > 0 ? normalized : undefined;
}

function normalizeAuthor(
  author: RawEventAuthorInput | null | undefined
): Author | undefined {
  if (!author) {
    return undefined;
  }

  const normalized: Author = {};
  const id = normalizeOptionalString(author.id);
  const handle = normalizeOptionalString(author.handle);
  const displayName = normalizeOptionalString(author.display_name);

  if (id) {
    normalized.id = id;
  }
  if (handle) {
    normalized.handle = handle;
  }
  if (displayName) {
    normalized.display_name = displayName;
  }

  return Object.keys(normalized).length > 0 ? normalized : undefined;
}

function normalizeEngagement(
  engagement: RawEventEngagementInput | null | undefined
): Engagement | undefined {
  if (!engagement) {
    return undefined;
  }

  const normalized: Engagement = {};
  if (typeof engagement.score === "number" && Number.isFinite(engagement.score)) {
    normalized.score = engagement.score;
  }
  if (
    typeof engagement.comments === "number" &&
    Number.isFinite(engagement.comments)
  ) {
    normalized.comments = engagement.comments;
  }
  if (typeof engagement.likes === "number" && Number.isFinite(engagement.likes)) {
    normalized.likes = engagement.likes;
  }
  if (
    typeof engagement.shares === "number" &&
    Number.isFinite(engagement.shares)
  ) {
    normalized.shares = engagement.shares;
  }

  return Object.keys(normalized).length > 0 ? normalized : undefined;
}

function normalizeExtracted(
  extracted: RawEventExtractedInput | null | undefined
): Extracted | undefined {
  if (!extracted) {
    return undefined;
  }

  const hashtags = normalizeStringArray(extracted.hashtags);
  const urls = normalizeStringArray(extracted.urls);
  if (!hashtags && !urls) {
    return undefined;
  }

  return {
    hashtags,
    urls,
  };
}

export function createRawEvent(input: CreateRawEventInput): RawEvent {
  const publishedAt = normalizeOptionalString(input.published_at);
  const url = normalizeOptionalString(input.url);
  const title = normalizeOptionalString(input.title);
  const lang = normalizeOptionalString(input.lang);
  const tags = normalizeStringArray(input.tags);
  const extracted = normalizeExtracted(input.extracted);
  const author = normalizeAuthor(input.author);
  const engagement = normalizeEngagement(input.engagement);

  return {
    event_id: input.event_id.trim(),
    source: input.source,
    fetched_at: input.fetched_at.trim(),
    published_at: publishedAt,
    url,
    title,
    text: input.text.trim(),
    author,
    engagement,
    lang,
    tags,
    extracted,
    source_meta: input.source_meta ?? undefined,
  };
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
