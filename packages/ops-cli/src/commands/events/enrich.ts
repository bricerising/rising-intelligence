import { isDeepStrictEqual } from "node:util";
import { existsSync } from "node:fs";
import {
  PrismaClient,
  Source,
  Prisma,
} from "@rising-intelligence/db";
import {
  extractTopics,
  loadAllowlist,
  prepareRawEventForPersistence,
} from "@rising-intelligence/shared";
import type { CliFlags } from "../../lib/args.js";
import { getBooleanFlag, getStringFlag } from "../../lib/flags.js";
import {
  parseNonNegativeIntegerStrict,
  parsePositiveIntegerStrict,
} from "../../lib/number.js";
import { resolveTopicsDatabaseUrl } from "../topics/database-url.js";
import {
  parseSourceFlag,
  resolveAllowlistPath,
  stringArraysEqual,
} from "../topics/retag.js";

const DEFAULT_BATCH_SIZE = 200;
const DEFAULT_PREVIEW_LIMIT = 20;
const DEFAULT_STEPS = ["retag", "quality"] as const;
const ALLOWED_STEPS = new Set<string>(DEFAULT_STEPS);

export type EnrichStepName = "retag" | "quality";

interface EnrichStep {
  name: EnrichStepName;
  apply(event: EnrichMutableEvent, ctx: EnrichContext): EnrichMutableEvent;
}

interface EnrichContext {
  allowlist: ReturnType<typeof loadAllowlist> | null;
}

interface EnrichStats {
  scanned: number;
  changed: number;
  unchanged: number;
  retagStepChanged: number;
  qualityStepChanged: number;
}

interface EnrichPreviewRow {
  eventId: string;
  source: Source;
  stepChanges: EnrichStepName[];
  changedFields: string[];
  previousTopics: string[];
  nextTopics: string[];
}

interface EnrichTargetRow {
  id: bigint;
  eventId: string;
  source: Source;
  fetchedAt: Date;
  publishedAt: Date | null;
  url: string | null;
  title: string | null;
  text: string;
  authorId: string | null;
  authorHandle: string | null;
  authorDisplayName: string | null;
  engagementScore: number | null;
  engagementComments: number | null;
  engagementLikes: number | null;
  engagementShares: number | null;
  lang: string | null;
  tags: string[];
  topics: string[];
  extractedHashtags: string[];
  extractedUrls: string[];
  sourceMeta: Prisma.JsonValue | null;
}

interface EnrichMutableEvent {
  id: bigint;
  eventId: string;
  source: Source;
  fetchedAt: Date;
  publishedAt: Date | null;
  url: string | null;
  title: string | null;
  text: string;
  authorId: string | null;
  authorHandle: string | null;
  authorDisplayName: string | null;
  engagementScore: number | null;
  engagementComments: number | null;
  engagementLikes: number | null;
  engagementShares: number | null;
  lang: string | null;
  tags: string[];
  topics: string[];
  extractedHashtags: string[];
  extractedUrls: string[];
  sourceMeta: Record<string, unknown> | null;
}

function createStats(): EnrichStats {
  return {
    scanned: 0,
    changed: 0,
    unchanged: 0,
    retagStepChanged: 0,
    qualityStepChanged: 0,
  };
}

export function parseEnrichSteps(rawSteps: string | undefined): EnrichStepName[] {
  if (!rawSteps || rawSteps.trim().length === 0) {
    return [...DEFAULT_STEPS];
  }

  const parsed = rawSteps
    .split(",")
    .map((step) => step.trim().toLowerCase())
    .filter((step) => step.length > 0);

  if (parsed.length === 0) {
    throw new Error("--steps must include at least one step");
  }

  for (const step of parsed) {
    if (!ALLOWED_STEPS.has(step)) {
      throw new Error(
        `Invalid --steps value '${step}'. Allowed steps: ${[...DEFAULT_STEPS].join(", ")}`
      );
    }
  }

  return [...new Set(parsed)] as EnrichStepName[];
}

export function buildWhereClause(
  missingOnly: boolean,
  source: Source | undefined
): Prisma.RawEventWhereInput {
  const where: Prisma.RawEventWhereInput = {};

  if (missingOnly) {
    where.OR = [{ topics: { isEmpty: true } }, { tags: { isEmpty: true } }];
  }

  if (source) {
    where.source = source;
  }

  return where;
}

function toSourceMetaRecord(value: Prisma.JsonValue | null): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  return value as Record<string, unknown>;
}

function toMutableEvent(row: EnrichTargetRow): EnrichMutableEvent {
  return {
    ...row,
    tags: [...row.tags],
    topics: [...row.topics],
    extractedHashtags: [...row.extractedHashtags],
    extractedUrls: [...row.extractedUrls],
    sourceMeta: toSourceMetaRecord(row.sourceMeta),
  };
}

function hasManagedChanges(previous: EnrichMutableEvent, next: EnrichMutableEvent): boolean {
  return (
    !stringArraysEqual(previous.tags, next.tags)
    || !stringArraysEqual(previous.topics, next.topics)
    || previous.url !== next.url
    || previous.text !== next.text
    || previous.lang !== next.lang
    || !isDeepStrictEqual(previous.sourceMeta, next.sourceMeta)
  );
}

function diffManagedFields(previous: EnrichMutableEvent, next: EnrichMutableEvent): string[] {
  const fields: string[] = [];
  if (!stringArraysEqual(previous.tags, next.tags)) {
    fields.push("tags");
  }
  if (!stringArraysEqual(previous.topics, next.topics)) {
    fields.push("topics");
  }
  if (previous.url !== next.url) {
    fields.push("url");
  }
  if (previous.text !== next.text) {
    fields.push("text");
  }
  if (previous.lang !== next.lang) {
    fields.push("lang");
  }
  if (!isDeepStrictEqual(previous.sourceMeta, next.sourceMeta)) {
    fields.push("source_meta");
  }

  return fields;
}

function buildUpdateData(
  previous: EnrichMutableEvent,
  next: EnrichMutableEvent
): Prisma.RawEventUpdateInput | null {
  const changedFields = diffManagedFields(previous, next);
  if (changedFields.length === 0) {
    return null;
  }

  const data: Prisma.RawEventUpdateInput = {};

  if (!stringArraysEqual(previous.tags, next.tags)) {
    data.tags = [...next.tags];
  }
  if (!stringArraysEqual(previous.topics, next.topics)) {
    data.topics = [...next.topics];
  }
  if (previous.url !== next.url) {
    data.url = next.url;
  }
  if (previous.text !== next.text) {
    data.text = next.text;
  }
  if (previous.lang !== next.lang) {
    data.lang = next.lang;
  }
  if (!isDeepStrictEqual(previous.sourceMeta, next.sourceMeta)) {
    data.sourceMeta = next.sourceMeta === null
      ? Prisma.JsonNull
      : (next.sourceMeta as Prisma.InputJsonValue);
  }

  return data;
}

const RETAG_STEP: EnrichStep = {
  name: "retag",
  apply(event, ctx) {
    if (!ctx.allowlist) {
      return event;
    }

    const nextTopics = extractTopics(
      {
        title: event.title ?? undefined,
        text: event.text,
      },
      ctx.allowlist
    );

    return {
      ...event,
      tags: [...nextTopics],
      topics: [...nextTopics],
    };
  },
};

const QUALITY_STEP: EnrichStep = {
  name: "quality",
  apply(event) {
    const prepared = prepareRawEventForPersistence({
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
      sourceMeta: event.sourceMeta,
    });

    return {
      ...event,
      ...prepared,
      tags: [...prepared.tags],
      topics: [...prepared.topics],
      sourceMeta: prepared.sourceMeta,
    };
  },
};

const STEP_REGISTRY: Record<EnrichStepName, EnrichStep> = {
  retag: RETAG_STEP,
  quality: QUALITY_STEP,
};

function buildStepPipeline(stepNames: readonly EnrichStepName[]): EnrichStep[] {
  return stepNames.map((stepName) => STEP_REGISTRY[stepName]);
}

function printSummary(
  stats: EnrichStats,
  updated: number,
  dryRun: boolean,
  allowlistPath: string | null,
  steps: readonly EnrichStepName[],
  missingOnly: boolean,
  source: Source | undefined
): void {
  console.log("\nEvent enrichment complete.\n");
  console.log(`  mode: ${missingOnly ? "missing tags/topics only" : "all events"}`);
  console.log(`  steps: ${steps.join(", ")}`);
  if (allowlistPath) {
    console.log(`  allowlist: ${allowlistPath}`);
  }
  if (source) {
    console.log(`  source filter: ${source}`);
  }
  console.log(`  dry run: ${dryRun ? "yes" : "no"}`);
  console.log("");
  console.log(`  scanned: ${stats.scanned}`);
  console.log(`  unchanged: ${stats.unchanged}`);
  if (dryRun) {
    console.log(`  would update: ${stats.changed}`);
  } else {
    console.log(`  updated: ${updated}`);
  }
  if (steps.includes("retag")) {
    console.log(`  rows changed by retag step: ${stats.retagStepChanged}`);
  }
  if (steps.includes("quality")) {
    console.log(`  rows changed by quality step: ${stats.qualityStepChanged}`);
  }
  console.log("");
}

function printPreviewRows(rows: readonly EnrichPreviewRow[], dryRun: boolean): void {
  if (rows.length === 0) {
    return;
  }

  console.log(
    `Sample changes (${rows.length}, ${dryRun ? "would be applied" : "applied"}):\n`
  );

  for (const row of rows) {
    console.log(`  ${row.eventId} (${row.source})`);
    console.log(`    steps: ${row.stepChanges.join(", ")}`);
    console.log(`    fields: ${row.changedFields.join(", ")}`);
    if (
      !stringArraysEqual(row.previousTopics, row.nextTopics)
    ) {
      const previous = row.previousTopics.length === 0
        ? "[]"
        : `[${row.previousTopics.join(", ")}]`;
      const next = row.nextTopics.length === 0
        ? "[]"
        : `[${row.nextTopics.join(", ")}]`;
      console.log(`    topics: ${previous} -> ${next}`);
    }
  }
}

export async function eventsEnrich(flags: CliFlags): Promise<void> {
  const databaseUrl = resolveTopicsDatabaseUrl(flags);
  const steps = parseEnrichSteps(getStringFlag(flags, "steps"));
  const stepPipeline = buildStepPipeline(steps);
  const dryRun = getBooleanFlag(flags, "dry-run");
  const missingOnly = getBooleanFlag(flags, "missing-only");
  const source = parseSourceFlag(getStringFlag(flags, "source"));
  const limitRaw = getStringFlag(flags, "limit");
  const limit = limitRaw
    ? parseNonNegativeIntegerStrict(limitRaw, "--limit")
    : undefined;

  const batchSizeRaw = getStringFlag(flags, "batch-size");
  const batchSize = batchSizeRaw
    ? parsePositiveIntegerStrict(batchSizeRaw, "--batch-size")
    : DEFAULT_BATCH_SIZE;

  const needsAllowlist = steps.includes("retag");
  const allowlistPath = needsAllowlist
    ? resolveAllowlistPath(getStringFlag(flags, "allowlist-path"))
    : null;

  if (allowlistPath && !existsSync(allowlistPath)) {
    throw new Error(
      `Allowlist file not found at: ${allowlistPath}. Pass --allowlist-path to specify the correct file.`
    );
  }

  const allowlist = allowlistPath ? loadAllowlist(allowlistPath) : null;
  const context: EnrichContext = { allowlist };
  const where = buildWhereClause(missingOnly, source);

  const prisma = new PrismaClient({
    datasources: {
      db: {
        url: databaseUrl,
      },
    },
  });

  const stats = createStats();
  const previewRows: EnrichPreviewRow[] = [];
  let updated = 0;
  let cursorId: bigint | null = null;
  let remaining = limit;

  try {
    while (true) {
      if (remaining !== undefined && remaining <= 0) {
        break;
      }

      const take = remaining === undefined
        ? batchSize
        : Math.min(batchSize, remaining);

      const batchWhere: Prisma.RawEventWhereInput = cursorId === null
        ? where
        : { AND: [where, { id: { gt: cursorId } }] };

      const events = await prisma.rawEvent.findMany({
        where: batchWhere,
        orderBy: { id: "asc" },
        take,
        select: {
          id: true,
          eventId: true,
          source: true,
          fetchedAt: true,
          publishedAt: true,
          url: true,
          title: true,
          text: true,
          authorId: true,
          authorHandle: true,
          authorDisplayName: true,
          engagementScore: true,
          engagementComments: true,
          engagementLikes: true,
          engagementShares: true,
          lang: true,
          tags: true,
          topics: true,
          extractedHashtags: true,
          extractedUrls: true,
          sourceMeta: true,
        },
      }) as EnrichTargetRow[];

      if (events.length === 0) {
        break;
      }

      cursorId = events[events.length - 1].id;
      if (remaining !== undefined) {
        remaining -= events.length;
      }

      stats.scanned += events.length;
      const updates: Prisma.PrismaPromise<unknown>[] = [];

      for (const row of events) {
        const originalEvent = toMutableEvent(row);
        let transformedEvent = originalEvent;
        const stepChanges: EnrichStepName[] = [];

        for (const step of stepPipeline) {
          const stepResult = step.apply(transformedEvent, context);
          if (hasManagedChanges(transformedEvent, stepResult)) {
            stepChanges.push(step.name);
            if (step.name === "retag") {
              stats.retagStepChanged += 1;
            } else {
              stats.qualityStepChanged += 1;
            }
          }
          transformedEvent = stepResult;
        }

        const changedFields = diffManagedFields(originalEvent, transformedEvent);
        if (changedFields.length === 0) {
          stats.unchanged += 1;
          continue;
        }

        stats.changed += 1;
        if (previewRows.length < DEFAULT_PREVIEW_LIMIT) {
          previewRows.push({
            eventId: row.eventId,
            source: row.source,
            stepChanges,
            changedFields,
            previousTopics: originalEvent.topics,
            nextTopics: transformedEvent.topics,
          });
        }

        if (!dryRun) {
          const updateData = buildUpdateData(originalEvent, transformedEvent);
          if (updateData) {
            updates.push(
              prisma.rawEvent.update({
                where: { id: row.id },
                data: updateData,
              })
            );
          }
        }
      }

      if (!dryRun && updates.length > 0) {
        await prisma.$transaction(updates);
        updated += updates.length;
      }
    }
  } finally {
    await prisma.$disconnect();
  }

  printSummary(stats, updated, dryRun, allowlistPath, steps, missingOnly, source);
  printPreviewRows(previewRows, dryRun);
}
