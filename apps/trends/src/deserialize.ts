import { deserializeRawEvent as sharedDeserialize } from "@rising-intelligence/pipeline";
import type { ParsedRawEvent, Source } from "./types.js";

export { parseCanonicalSource as parseSource } from "@rising-intelligence/pipeline";

function extractFeedPriority(sourceMeta: Record<string, unknown> | null): number {
  if (!sourceMeta) {
    return 50;
  }
  const raw = sourceMeta.feed_priority;
  if (typeof raw === "number" && Number.isFinite(raw) && raw >= 1) {
    return raw;
  }
  return 50;
}

export function deserializeRawEvent(messageValue: Buffer): ParsedRawEvent {
  const shared = sharedDeserialize(messageValue);
  return {
    eventId: shared.eventId,
    source: shared.source as Source,
    fetchedAt: shared.fetchedAt,
    publishedAt: shared.publishedAt,
    url: shared.url,
    title: shared.title,
    text: shared.text,
    tags: shared.tags,
    engagementScore: shared.engagementScore ?? 0,
    feedPriority: extractFeedPriority(shared.sourceMeta),
  };
}
