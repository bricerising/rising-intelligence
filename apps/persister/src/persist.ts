import {
  PrismaClient,
  Source,
  type Prisma,
  type ConsumerLagUpdate,
  upsertConsumerLag as upsertSharedConsumerLag,
} from "@rising-intelligence/db";
import type { ParsedRawEvent } from "./types.js";
import { prepareRawEventForPersistence } from "./enrich.js";

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
  const prepared = prepareRawEventForPersistence(event);
  const sourceMeta = prepared.sourceMeta as Prisma.InputJsonValue;

  return {
    eventId: prepared.eventId,
    source: prepared.source,
    fetchedAt: prepared.fetchedAt,
    publishedAt: prepared.publishedAt,
    url: prepared.url,
    title: prepared.title,
    text: prepared.text,
    authorId: prepared.authorId,
    authorHandle: prepared.authorHandle,
    authorDisplayName: prepared.authorDisplayName,
    engagementScore: prepared.engagementScore,
    engagementComments: prepared.engagementComments,
    engagementLikes: prepared.engagementLikes,
    engagementShares: prepared.engagementShares,
    lang: prepared.lang,
    tags: prepared.tags,
    extractedHashtags: prepared.extractedHashtags,
    extractedUrls: prepared.extractedUrls,
    topics: prepared.topics,
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
