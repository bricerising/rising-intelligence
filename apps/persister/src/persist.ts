import {
  PrismaClient,
  Source,
  type Prisma,
  type ConsumerLagUpdate,
  upsertConsumerLag as upsertSharedConsumerLag,
} from "@rising-intelligence/db";
import type { ParsedRawEvent } from "./types.js";

export interface PersistBatchResult {
  attempted: number;
  inserted: number;
  duplicates: number;
  insertedBySource: Map<Source, number>;
}

function groupBySource(events: ParsedRawEvent[]): Map<Source, ParsedRawEvent[]> {
  const groups = new Map<Source, ParsedRawEvent[]>();

  for (const event of events) {
    const existing = groups.get(event.source);
    if (existing) {
      existing.push(event);
    } else {
      groups.set(event.source, [event]);
    }
  }

  return groups;
}

function toCreateManyInput(event: ParsedRawEvent) {
  const sourceMeta = event.sourceMeta === null
    ? undefined
    : (event.sourceMeta as Prisma.InputJsonValue);

  return {
    eventId: event.eventId,
    source: event.source,
    fetchedAt: event.fetchedAt,
    publishedAt: event.publishedAt,
    url: event.url,
    title: event.title,
    text: event.text,
    authorId: event.authorId,
    authorHandle: event.authorHandle,
    authorDisplayName: event.authorDisplayName,
    engagementScore: event.engagementScore,
    engagementComments: event.engagementComments,
    engagementLikes: event.engagementLikes,
    engagementShares: event.engagementShares,
    lang: event.lang,
    tags: event.tags,
    extractedHashtags: event.extractedHashtags,
    extractedUrls: event.extractedUrls,
    topics: event.tags,
    sourceMeta,
  };
}

export async function persistBatch(
  prisma: PrismaClient,
  events: ParsedRawEvent[]
): Promise<PersistBatchResult> {
  if (events.length === 0) {
    return {
      attempted: 0,
      inserted: 0,
      duplicates: 0,
      insertedBySource: new Map(),
    };
  }

  const insertedBySource = new Map<Source, number>();
  let inserted = 0;
  let duplicates = 0;

  const grouped = groupBySource(events);
  for (const [source, sourceEvents] of grouped) {
    const createResult = await prisma.rawEvent.createMany({
      data: sourceEvents.map(toCreateManyInput),
      skipDuplicates: true,
    });

    insertedBySource.set(source, createResult.count);
    inserted += createResult.count;
    duplicates += sourceEvents.length - createResult.count;
  }

  return {
    attempted: events.length,
    inserted,
    duplicates,
    insertedBySource,
  };
}

export { type ConsumerLagUpdate };

export const upsertConsumerLag = upsertSharedConsumerLag;
