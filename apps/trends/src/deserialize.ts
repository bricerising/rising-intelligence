import { deserializeRawEvent as sharedDeserialize } from "@rising-intelligence/pipeline";
import type { ParsedRawEvent, Source } from "./types.js";

export { parseCanonicalSource as parseSource } from "@rising-intelligence/pipeline";

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
  };
}
