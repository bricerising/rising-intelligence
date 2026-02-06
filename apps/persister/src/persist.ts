import { PrismaClient, Source } from "@rising-intelligence/db";
import type { ParsedRawEvent } from "./types.js";

export interface PersistBatchResult {
  attempted: number;
  inserted: number;
  duplicates: number;
  insertedBySource: Map<Source, number>;
}

export interface ConsumerLagUpdate {
  consumerGroup: string;
  topic: string;
  partition: number;
  currentOffset: bigint;
  latestOffset: bigint;
  lagMessages: bigint;
  observedAt: Date;
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
    sourceMeta: (event.sourceMeta ?? undefined) as any,
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

export async function upsertConsumerLag(
  prisma: PrismaClient,
  update: ConsumerLagUpdate
): Promise<void> {
  await prisma.consumerLag.upsert({
    where: {
      consumerGroup_topic_partition: {
        consumerGroup: update.consumerGroup,
        topic: update.topic,
        partition: update.partition,
      },
    },
    update: {
      currentOffset: update.currentOffset,
      latestOffset: update.latestOffset,
      lagMessages: update.lagMessages,
      updatedAt: update.observedAt,
    },
    create: {
      consumerGroup: update.consumerGroup,
      topic: update.topic,
      partition: update.partition,
      currentOffset: update.currentOffset,
      latestOffset: update.latestOffset,
      lagMessages: update.lagMessages,
      updatedAt: update.observedAt,
    },
  });
}
