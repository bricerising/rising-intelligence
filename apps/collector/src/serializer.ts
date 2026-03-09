import {
  SOURCE_KEY_TO_ENUM,
  sourceToProtoEnum,
} from "@rising-intelligence/pipeline";
import {
  createRawEvent,
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

export function toRawEventWirePayload(event: RawEvent): RawEventWirePayload {
  const normalizedEvent = createRawEvent(event);

  return {
    event_id: normalizedEvent.event_id,
    source: SOURCE_KEY_TO_ENUM[normalizedEvent.source],
    fetched_at: normalizedEvent.fetched_at,
    published_at: normalizedEvent.published_at ?? "",
    url: normalizedEvent.url ?? "",
    title: normalizedEvent.title ?? "",
    text: normalizedEvent.text,
    author: normalizedEvent.author
      ? {
          id: normalizedEvent.author.id ?? "",
          handle: normalizedEvent.author.handle ?? "",
          display_name: normalizedEvent.author.display_name ?? "",
        }
      : undefined,
    engagement: normalizedEvent.engagement
      ? {
          score: normalizedEvent.engagement.score ?? 0,
          comments: normalizedEvent.engagement.comments ?? 0,
          likes: normalizedEvent.engagement.likes ?? 0,
          shares: normalizedEvent.engagement.shares ?? 0,
        }
      : undefined,
    lang: normalizedEvent.lang ?? "",
    tags: normalizedEvent.tags ?? [],
    extracted: normalizedEvent.extracted
      ? {
          hashtags: normalizedEvent.extracted.hashtags ?? [],
          urls: normalizedEvent.extracted.urls ?? [],
        }
      : undefined,
    source_meta_json: normalizedEvent.source_meta
      ? JSON.stringify(normalizedEvent.source_meta)
      : "",
  };
}

/**
 * Serialize RawEvent to JSON for Kafka.
 * In MVP, we use JSON encoding. Can switch to protobuf binary later.
 */
export function serializeRawEvent(event: RawEvent): Buffer {
  return Buffer.from(JSON.stringify(toRawEventWirePayload(event)));
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
