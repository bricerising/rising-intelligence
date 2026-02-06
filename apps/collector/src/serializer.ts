import type { RawEvent, DeadLetterEvent, CollectorHeartbeat, Source } from "./types.js";

/**
 * Map Source string to proto enum value.
 * Must match rising_intelligence.v1.Source enum.
 */
const SOURCE_TO_PROTO: Record<Source, number> = {
  rss: 1,
  news: 2,
  hackernews: 3,
  reddit: 4,
  github: 5,
  // twitter is reserved (6) - not used
  bluesky: 7,
  mastodon: 8,
  lobsters: 2, // Lobsters is mapped to NEWS in proto
};

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
    source: SOURCE_TO_PROTO[event.source] ?? 0,
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
    source: SOURCE_TO_PROTO[heartbeat.source] ?? 0,
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
