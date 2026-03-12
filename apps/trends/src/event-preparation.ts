import { prepareRawEventForPersistence } from "@rising-intelligence/pipeline/hydration";
import { filterTrackedTags, type CompiledAllowlist } from "./allowlist.js";
import type { ParsedRawEvent, PreparedTrendEvent } from "./types.js";

function toPersistenceInput(event: ParsedRawEvent) {
  return {
    eventId: event.eventId,
    source: event.source,
    fetchedAt: event.fetchedAt,
    publishedAt: event.publishedAt,
    url: event.url,
    title: event.title,
    text: event.text,
    authorId: null,
    authorHandle: null,
    authorDisplayName: null,
    engagementScore: event.engagementScore,
    engagementComments: null,
    engagementLikes: null,
    engagementShares: null,
    lang: event.lang,
    tags: event.tags,
    extractedHashtags: [],
    extractedUrls: event.extractedUrls,
    sourceMeta: event.sourceMeta,
  };
}

export function prepareTrendEvent(
  event: ParsedRawEvent,
  allowlist: CompiledAllowlist
): PreparedTrendEvent {
  const prepared = prepareRawEventForPersistence(toPersistenceInput(event));

  return {
    eventId: prepared.eventId,
    source: prepared.source,
    fetchedAt: prepared.fetchedAt,
    publishedAt: prepared.publishedAt,
    url: prepared.url,
    title: prepared.title,
    text: prepared.text,
    lang: prepared.lang,
    tags: prepared.tags,
    extractedUrls: prepared.extractedUrls,
    sourceMeta: prepared.sourceMeta,
    engagementScore: prepared.engagementScore ?? event.engagementScore,
    feedPriority: event.feedPriority,
    topics: filterTrackedTags(prepared.topics, allowlist),
  };
}

export function isEventFreshEnoughForTracking(
  event: Pick<PreparedTrendEvent, "fetchedAt" | "publishedAt">,
  maxTrackedEventAgeMs: number
): boolean {
  if (!event.publishedAt) {
    return true;
  }

  const ageMs = event.fetchedAt.getTime() - event.publishedAt.getTime();
  if (ageMs <= 0) {
    return true;
  }

  return ageMs <= maxTrackedEventAgeMs;
}
