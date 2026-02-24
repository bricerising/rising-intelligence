import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { createPrismaClient, Source, type Prisma } from "@rising-intelligence/db";
import { extractTopics, loadAllowlist } from "@rising-intelligence/pipeline";
import { REPO_ROOT } from "@rising-intelligence/shared/config";
import type { CliFlags } from "../../lib/args.js";
import { getBooleanFlag, getStringFlag } from "../../lib/flags.js";
import {
  parseNonNegativeIntegerStrict,
  parsePositiveIntegerStrict,
} from "../../lib/number.js";
import { resolveTopicsDatabaseUrl } from "./database-url.js";

const DEFAULT_BATCH_SIZE = 200;
const DEFAULT_PREVIEW_LIMIT = 20;
const DEFAULT_ALLOWLIST_CANDIDATES = [
  resolve(REPO_ROOT, "infra", "config", "topics.allowlist.yaml"),
  resolve(process.cwd(), "infra", "config", "topics.allowlist.yaml"),
] as const;

interface RetagPreviewRow {
  eventId: string;
  source: Source;
  previousTopics: string[];
  nextTopics: string[];
}

interface RetagStats {
  scanned: number;
  changed: number;
  unchanged: number;
  retaggedFromEmpty: number;
  retaggedToEmpty: number;
  retaggedChangedNonEmpty: number;
  syncedTagsOnly: number;
}

function createRetagStats(): RetagStats {
  return {
    scanned: 0,
    changed: 0,
    unchanged: 0,
    retaggedFromEmpty: 0,
    retaggedToEmpty: 0,
    retaggedChangedNonEmpty: 0,
    syncedTagsOnly: 0,
  };
}

export function stringArraysEqual(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length) {
    return false;
  }

  for (let i = 0; i < left.length; i += 1) {
    if (left[i] !== right[i]) {
      return false;
    }
  }

  return true;
}

export function resolveAllowlistPath(rawPath: string | undefined): string {
  if (!rawPath || rawPath.trim().length === 0) {
    for (const candidate of DEFAULT_ALLOWLIST_CANDIDATES) {
      if (existsSync(candidate)) {
        return candidate;
      }
    }
    return DEFAULT_ALLOWLIST_CANDIDATES[0];
  }

  return resolve(rawPath);
}

export function parseSourceFlag(rawValue: string | undefined): Source | undefined {
  if (!rawValue) {
    return undefined;
  }

  const normalized = rawValue.trim().toLowerCase();
  if (normalized.length === 0) {
    return undefined;
  }

  const allowed = Object.values(Source);
  if (!allowed.includes(normalized as Source)) {
    throw new Error(
      `Invalid --source value: ${rawValue}. Expected one of: ${allowed.join(", ")}`
    );
  }

  return normalized as Source;
}

function formatTopics(topics: readonly string[]): string {
  return topics.length === 0 ? "[]" : `[${topics.join(", ")}]`;
}

function printPreviewRows(rows: readonly RetagPreviewRow[], dryRun: boolean): void {
  if (rows.length === 0) {
    return;
  }

  console.log(
    `\nSample changes (${rows.length}, ${dryRun ? "would be applied" : "applied"}):\n`
  );

  for (const row of rows) {
    console.log(`  ${row.eventId} (${row.source})`);
    console.log(`    old: ${formatTopics(row.previousTopics)}`);
    console.log(`    new: ${formatTopics(row.nextTopics)}`);
  }
}

function printSummary(
  stats: RetagStats,
  updated: number,
  dryRun: boolean,
  allowlistPath: string,
  retagAll: boolean,
  source: Source | undefined
): void {
  console.log("\nTopic retag complete.\n");
  console.log(`  allowlist: ${allowlistPath}`);
  console.log(`  mode: ${retagAll ? "all events" : "missing tags/topics only"}`);
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
  console.log(`  empty -> tagged: ${stats.retaggedFromEmpty}`);
  console.log(`  tagged -> empty: ${stats.retaggedToEmpty}`);
  console.log(`  tagged -> different tags: ${stats.retaggedChangedNonEmpty}`);
  console.log(`  synced tags/topics only: ${stats.syncedTagsOnly}`);
  console.log("");
}

function buildWhereClause(retagAll: boolean, source: Source | undefined): Prisma.RawEventWhereInput {
  const where: Prisma.RawEventWhereInput = {};
  if (!retagAll) {
    where.OR = [{ topics: { isEmpty: true } }, { tags: { isEmpty: true } }];
  }
  if (source) {
    where.source = source;
  }

  return where;
}

interface RetagTarget {
  id: bigint;
  eventId: string;
  source: Source;
  url: string | null;
  title: string | null;
  text: string;
  tags: string[];
  topics: string[];
}

export async function topicsRetag(flags: CliFlags): Promise<void> {
  const databaseUrl = resolveTopicsDatabaseUrl(flags);
  const allowlistPath = resolveAllowlistPath(getStringFlag(flags, "allowlist-path"));
  const dryRun = getBooleanFlag(flags, "dry-run");
  const retagAll = getBooleanFlag(flags, "all");
  const source = parseSourceFlag(getStringFlag(flags, "source"));
  const limitRaw = getStringFlag(flags, "limit");
  const limit = limitRaw
    ? parseNonNegativeIntegerStrict(limitRaw, "--limit")
    : undefined;

  const batchSizeRaw = getStringFlag(flags, "batch-size");
  const batchSize = batchSizeRaw
    ? parsePositiveIntegerStrict(batchSizeRaw, "--batch-size")
    : DEFAULT_BATCH_SIZE;

  if (!existsSync(allowlistPath)) {
    throw new Error(
      `Allowlist file not found at: ${allowlistPath}. Pass --allowlist-path to specify the correct file.`
    );
  }

  const allowlist = loadAllowlist(allowlistPath);
  const where = buildWhereClause(retagAll, source);

  const prisma = createPrismaClient({
    databaseUrl,
  });

  const stats = createRetagStats();
  const previewRows: RetagPreviewRow[] = [];
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
          url: true,
          title: true,
          text: true,
          tags: true,
          topics: true,
        },
      }) as RetagTarget[];

      if (events.length === 0) {
        break;
      }

      cursorId = events[events.length - 1].id;
      if (remaining !== undefined) {
        remaining -= events.length;
      }

      stats.scanned += events.length;
      const updates: Prisma.PrismaPromise<unknown>[] = [];

      for (const event of events) {
        const nextTopics = extractTopics(
          { title: event.title ?? undefined, text: event.text, url: event.url ?? undefined },
          allowlist
        );

        const topicsChanged = !stringArraysEqual(event.topics, nextTopics);
        const tagsChanged = !stringArraysEqual(event.tags, nextTopics);
        const changed = topicsChanged || tagsChanged;

        if (!changed) {
          stats.unchanged += 1;
          continue;
        }

        stats.changed += 1;

        if (!topicsChanged && tagsChanged) {
          stats.syncedTagsOnly += 1;
        } else if (event.topics.length === 0 && nextTopics.length > 0) {
          stats.retaggedFromEmpty += 1;
        } else if (event.topics.length > 0 && nextTopics.length === 0) {
          stats.retaggedToEmpty += 1;
        } else {
          stats.retaggedChangedNonEmpty += 1;
        }

        if (previewRows.length < DEFAULT_PREVIEW_LIMIT) {
          previewRows.push({
            eventId: event.eventId,
            source: event.source,
            previousTopics: event.topics,
            nextTopics,
          });
        }

        if (!dryRun) {
          updates.push(
            prisma.rawEvent.update({
              where: { id: event.id },
              data: {
                topics: [...nextTopics],
                tags: [...nextTopics],
              },
            })
          );
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

  printSummary(stats, updated, dryRun, allowlistPath, retagAll, source);
  printPreviewRows(previewRows, dryRun);
}
