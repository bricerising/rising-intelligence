import {
  parseCanonicalSource,
  type CanonicalSource,
} from "@rising-intelligence/pipeline";

/**
 * Source values accepted by collector adapters.
 * Extends the shared CanonicalSource contract with "lobsters", a collector-
 * internal alias that collector normalization resolves to "news" on the wire.
 */
export type Source = CanonicalSource | "lobsters";
export type RawEventSource = CanonicalSource;

export interface CollectionIngestionAuthor {
  id?: string;
  handle?: string;
  displayName?: string;
}

export type CollectedContentAuthor = CollectionIngestionAuthor;

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
 * Published collection-ingestion contract used between source adaptation,
 * downstream consumers, and collector publication. The raw-event wire shape
 * is derived from this contract at the publication boundary.
 */
export interface CollectionIngestion {
  eventId: string;
  source: RawEventSource;
  fetchedAt: string; // ISO8601
  publishedAt?: string; // ISO8601

  url?: string;
  title?: string;
  text: string;

  author?: CollectionIngestionAuthor;
  engagement?: Engagement;

  lang?: string;
  tags?: string[];
  extracted?: Extracted;

  sourceMeta?: Record<string, unknown>;
}

export type CollectedContent = CollectionIngestion;

/**
 * Collector-owned ingestion contract consumed by runtime orchestration.
 * Source adapters emit this shape; RawEvent materialization stays at the
 * collector publication boundary.
 */
export type CollectorIngestionEvent = CollectionIngestion;

/**
 * Canonical RawEvent schema matching the proto contract.
 * All ingested items are normalized to this format before publishing to Kafka.
 */
export interface RawEvent {
  event_id: string;
  source: RawEventSource;
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

export interface CollectionIngestionAuthorInput {
  id?: string | null;
  handle?: string | null;
  displayName?: string | null;
}

export type CollectedContentAuthorInput = CollectionIngestionAuthorInput;

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

export interface CreateCollectionIngestionInput {
  eventId: string;
  source: Source;
  fetchedAt: string;
  publishedAt?: string | null;
  url?: string | null;
  title?: string | null;
  text: string;
  author?: CollectionIngestionAuthorInput | null;
  engagement?: RawEventEngagementInput | null;
  lang?: string | null;
  tags?: Array<string | null | undefined> | null;
  extracted?: RawEventExtractedInput | null;
  sourceMeta?: Record<string, unknown> | null;
}

export type CreateCollectedContentInput = CreateCollectionIngestionInput;

export function normalizeRawEventSource(source: Source): RawEventSource {
  return parseCanonicalSource(source);
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

function normalizeCollectionIngestionAuthor(
  author: CollectionIngestionAuthorInput | null | undefined
): CollectionIngestionAuthor | undefined {
  if (!author) {
    return undefined;
  }

  const normalized: CollectionIngestionAuthor = {};
  const id = normalizeOptionalString(author.id);
  const handle = normalizeOptionalString(author.handle);
  const displayName = normalizeOptionalString(author.displayName);

  if (id) {
    normalized.id = id;
  }
  if (handle) {
    normalized.handle = handle;
  }
  if (displayName) {
    normalized.displayName = displayName;
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

export function createCollectionIngestion(
  input: CreateCollectionIngestionInput
): CollectionIngestion {
  const publishedAt = normalizeOptionalString(input.publishedAt);
  const url = normalizeOptionalString(input.url);
  const title = normalizeOptionalString(input.title);
  const lang = normalizeOptionalString(input.lang);
  const tags = normalizeStringArray(input.tags);
  const extracted = normalizeExtracted(input.extracted);
  const author = normalizeCollectionIngestionAuthor(input.author);
  const engagement = normalizeEngagement(input.engagement);

  return {
    eventId: input.eventId.trim(),
    source: normalizeRawEventSource(input.source),
    fetchedAt: input.fetchedAt.trim(),
    publishedAt,
    url,
    title,
    text: input.text.trim(),
    author,
    engagement,
    lang,
    tags,
    extracted,
    sourceMeta: input.sourceMeta ?? undefined,
  };
}

export function normalizeCollectionIngestion(
  content: CollectionIngestion
): CollectionIngestion {
  return createCollectionIngestion({
    eventId: content.eventId,
    source: content.source,
    fetchedAt: content.fetchedAt,
    publishedAt: content.publishedAt,
    url: content.url,
    title: content.title,
    text: content.text,
    author: content.author
      ? {
          id: content.author.id,
          handle: content.author.handle,
          displayName: content.author.displayName,
        }
      : undefined,
    engagement: content.engagement,
    lang: content.lang,
    tags: content.tags,
    extracted: content.extracted,
    sourceMeta: content.sourceMeta,
  });
}

export function toRawEvent(content: CollectionIngestion): RawEvent {
  const normalized = normalizeCollectionIngestion(content);

  return {
    event_id: normalized.eventId,
    source: normalized.source,
    fetched_at: normalized.fetchedAt,
    published_at: normalized.publishedAt,
    url: normalized.url,
    title: normalized.title,
    text: normalized.text,
    author: normalized.author
      ? {
          id: normalized.author.id,
          handle: normalized.author.handle,
          display_name: normalized.author.displayName,
        }
      : undefined,
    engagement: normalized.engagement,
    lang: normalized.lang,
    tags: normalized.tags,
    extracted: normalized.extracted,
    source_meta: normalized.sourceMeta,
  };
}

export function toCollectionIngestion(event: RawEvent): CollectionIngestion {
  return createCollectionIngestion({
    eventId: event.event_id,
    source: event.source,
    fetchedAt: event.fetched_at,
    publishedAt: event.published_at,
    url: event.url,
    title: event.title,
    text: event.text,
    author: event.author
      ? {
          id: event.author.id,
          handle: event.author.handle,
          displayName: event.author.display_name,
        }
      : undefined,
    engagement: event.engagement,
    lang: event.lang,
    tags: event.tags,
    extracted: event.extracted,
    sourceMeta: event.source_meta,
  });
}

export type CollectorAcceptedEvent = CollectorIngestionEvent | RawEvent;

export function isRawEvent(event: CollectorAcceptedEvent): event is RawEvent {
  return "event_id" in event;
}

export function normalizeCollectorIngestionEvent(
  event: CollectorAcceptedEvent
): CollectorIngestionEvent {
  return isRawEvent(event)
    ? toCollectionIngestion(event)
    : normalizeCollectionIngestion(event);
}

export function createRawEvent(input: CreateRawEventInput): RawEvent {
  return toRawEvent(
    createCollectionIngestion({
      eventId: input.event_id,
      source: input.source,
      fetchedAt: input.fetched_at,
      publishedAt: input.published_at,
      url: input.url,
      title: input.title,
      text: input.text,
      author: input.author
        ? {
            id: input.author.id,
            handle: input.author.handle,
            displayName: input.author.display_name,
          }
        : undefined,
      engagement: input.engagement,
      lang: input.lang,
      tags: input.tags,
      extracted: input.extracted,
      sourceMeta: input.source_meta,
    })
  );
}

export const createCollectedContent = createCollectionIngestion;
export const normalizeCollectedContent = normalizeCollectionIngestion;
export const toCollectedContent = toCollectionIngestion;

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
export interface CollectorSourceAdapter {
  /** Unique name for this adapter (used in logs and metrics) */
  readonly name: string;

  /** Source identity reported by this adapter */
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

export type SourceAdapter = CollectorSourceAdapter;

export interface CollectorSourceRecord {
  content: CollectorIngestionEvent;
  checkpointKey: string;
  checkpointValue: string;
}

export interface FetchResult extends CollectorSourceRecord {
  /**
   * @deprecated Use `content`; this alias remains for callers migrating off
   * RawEvent internals.
   */
  event: RawEvent;
}

export interface CreateCollectorSourceRecordInput extends CollectorSourceRecord {}

export function createCollectorSourceRecord(
  input: CreateCollectorSourceRecordInput
): FetchResult {
  const content = normalizeCollectorIngestionEvent(input.content);

  return {
    content,
    event: toRawEvent(content),
    checkpointKey: input.checkpointKey,
    checkpointValue: input.checkpointValue,
  };
}
