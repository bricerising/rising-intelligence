import { sourceToProtoEnum } from "@rising-intelligence/pipeline";
import type { RawEvent, DeadLetterEvent, CollectorHeartbeat, Source } from "./types.js";

/**
 * Map CollectorStatus to proto enum value.
 */
const STATUS_TO_PROTO: Record<string, number> = {
  healthy: 1,
  degraded: 2,
  error: 3,
};

/**
 * Serialize RawEvent to JSON for Kafka.
 * In MVP, we use JSON encoding. Can switch to protobuf binary later.
 */
export function serializeRawEvent(event: RawEvent): Buffer {
  const protoEvent = {
    event_id: event.event_id,
    source: sourceToProtoEnum(event.source),
    fetched_at: event.fetched_at,
    published_at: event.published_at ?? "",
    url: event.url ?? "",
    title: event.title ?? "",
    text: event.text,
    author: event.author
      ? {
          id: event.author.id ?? "",
          handle: event.author.handle ?? "",
          display_name: event.author.display_name ?? "",
        }
      : undefined,
    engagement: event.engagement
      ? {
          score: event.engagement.score ?? 0,
          comments: event.engagement.comments ?? 0,
          likes: event.engagement.likes ?? 0,
          shares: event.engagement.shares ?? 0,
        }
      : undefined,
    lang: event.lang ?? "",
    tags: event.tags ?? [],
    extracted: event.extracted
      ? {
          hashtags: event.extracted.hashtags ?? [],
          urls: event.extracted.urls ?? [],
        }
      : undefined,
    source_meta_json: event.source_meta
      ? JSON.stringify(event.source_meta)
      : "",
  };

  return Buffer.from(JSON.stringify(protoEvent));
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
