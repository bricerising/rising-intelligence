import {
  SOURCE_KEY_TO_ENUM,
  sourceToProtoEnum,
} from "@rising-intelligence/pipeline";
import {
  normalizeCollectedContent,
  toCollectedContent,
  type CollectedContent,
  type RawEvent,
  type DeadLetterEvent,
  type CollectorHeartbeat,
  type Source,
} from "./types.js";

/**
 * Map CollectorStatus to proto enum value.
 */
const STATUS_TO_PROTO: Record<string, number> = {
  healthy: 1,
  degraded: 2,
  error: 3,
};

interface RawEventWirePayload {
  event_id: string;
  source: number;
  fetched_at: string;
  published_at: string;
  url: string;
  title: string;
  text: string;
  author?:
    | {
        id: string;
        handle: string;
        display_name: string;
      }
    | undefined;
  engagement?:
    | {
        score: number;
        comments: number;
        likes: number;
        shares: number;
      }
    | undefined;
  lang: string;
  tags: string[];
  extracted?:
    | {
        hashtags: string[];
        urls: string[];
      }
    | undefined;
  source_meta_json: string;
}

export function toRawEventWirePayload(
  content: CollectedContent
): RawEventWirePayload {
  const normalizedContent = normalizeCollectedContent(content);

  return {
    event_id: normalizedContent.eventId,
    source: SOURCE_KEY_TO_ENUM[normalizedContent.source],
    fetched_at: normalizedContent.fetchedAt,
    published_at: normalizedContent.publishedAt ?? "",
    url: normalizedContent.url ?? "",
    title: normalizedContent.title ?? "",
    text: normalizedContent.text,
    author: normalizedContent.author
      ? {
          id: normalizedContent.author.id ?? "",
          handle: normalizedContent.author.handle ?? "",
          display_name: normalizedContent.author.displayName ?? "",
        }
      : undefined,
    engagement: normalizedContent.engagement
      ? {
          score: normalizedContent.engagement.score ?? 0,
          comments: normalizedContent.engagement.comments ?? 0,
          likes: normalizedContent.engagement.likes ?? 0,
          shares: normalizedContent.engagement.shares ?? 0,
        }
      : undefined,
    lang: normalizedContent.lang ?? "",
    tags: normalizedContent.tags ?? [],
    extracted: normalizedContent.extracted
      ? {
          hashtags: normalizedContent.extracted.hashtags ?? [],
          urls: normalizedContent.extracted.urls ?? [],
        }
      : undefined,
    source_meta_json: normalizedContent.sourceMeta
      ? JSON.stringify(normalizedContent.sourceMeta)
      : "",
  };
}

export function serializeCollectedContent(content: CollectedContent): Buffer {
  return Buffer.from(JSON.stringify(toRawEventWirePayload(content)));
}

/**
 * Serialize RawEvent to JSON for Kafka.
 * In MVP, we use JSON encoding. Can switch to protobuf binary later.
 */
export function serializeRawEvent(event: RawEvent): Buffer {
  return serializeCollectedContent(toCollectedContent(event));
}

/**
 * Serialize DeadLetterEvent to JSON for Kafka.
 */
export function serializeDeadLetterEvent(event: DeadLetterEvent): Buffer {
  return Buffer.from(JSON.stringify(event));
}

/**
 * Serialize CollectorHeartbeat to JSON for Kafka.
 */
export function serializeHeartbeat(heartbeat: CollectorHeartbeat): Buffer {
  const protoHeartbeat = {
    source: sourceToProtoEnum(heartbeat.source),
    timestamp: heartbeat.timestamp,
    last_fetch_at: heartbeat.last_fetch_at,
    items_fetched: heartbeat.items_fetched,
    status: STATUS_TO_PROTO[heartbeat.status] ?? 0,
    error_message: heartbeat.error_message ?? "",
  };

  return Buffer.from(JSON.stringify(protoHeartbeat));
}

/**
 * Generate a stable event ID from source and unique identifier.
 */
export function generateEventId(source: Source, uniqueId: string): string {
  return `${source}:${uniqueId}`;
}

/**
 * Generate a DLQ ID.
 */
export function generateDlqId(): string {
  return `dlq:${Date.now()}:${Math.random().toString(36).substring(2, 10)}`;
}
