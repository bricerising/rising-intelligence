import { Source } from "@rising-intelligence/db";
import { z } from "zod";
import type { ParsedRawEvent } from "./types.js";

const RawEventWireSchema = z.object({
  event_id: z.string().min(1),
  source: z.union([z.number().int(), z.string().min(1)]),
  fetched_at: z.string().min(1),
  published_at: z.string().optional(),
  url: z.string().optional(),
  title: z.string().optional(),
  text: z.string().min(1),
  author: z
    .object({
      id: z.string().optional(),
      handle: z.string().optional(),
      display_name: z.string().optional(),
    })
    .optional(),
  engagement: z
    .object({
      score: z.number().optional(),
      comments: z.number().optional(),
      likes: z.number().optional(),
      shares: z.number().optional(),
    })
    .optional(),
  lang: z.string().optional(),
  tags: z.array(z.string()).optional(),
  extracted: z
    .object({
      hashtags: z.array(z.string()).optional(),
      urls: z.array(z.string()).optional(),
    })
    .optional(),
  source_meta_json: z.string().optional(),
});

function parseDate(value: string, field: string): Date {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid ${field}: ${value}`);
  }
  return parsed;
}

function normalizeOptionalString(value: string | undefined): string | null {
  if (value === undefined) {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function parseSourceFromNumber(source: number): Source {
  switch (source) {
    case 1:
      return Source.rss;
    case 2:
      return Source.news;
    case 3:
      return Source.hackernews;
    case 4:
      return Source.reddit;
    case 5:
      return Source.github;
    case 7:
      return Source.bluesky;
    case 8:
      return Source.mastodon;
    default:
      throw new Error(`Unsupported source enum: ${source}`);
  }
}

export function parseSource(value: number | string): Source {
  if (typeof value === "number") {
    return parseSourceFromNumber(value);
  }

  const trimmed = value.trim();
  const asNumber = Number.parseInt(trimmed, 10);
  if (!Number.isNaN(asNumber) && `${asNumber}` === trimmed) {
    return parseSourceFromNumber(asNumber);
  }

  const normalized = trimmed.toLowerCase();
  switch (normalized) {
    case "source_rss":
    case "rss":
      return Source.rss;
    case "source_news":
    case "news":
    case "lobsters":
      return Source.news;
    case "source_hackernews":
    case "hackernews":
    case "hacker_news":
      return Source.hackernews;
    case "source_reddit":
    case "reddit":
      return Source.reddit;
    case "source_github":
    case "github":
      return Source.github;
    case "source_bluesky":
    case "bluesky":
      return Source.bluesky;
    case "source_mastodon":
    case "mastodon":
      return Source.mastodon;
    default:
      throw new Error(`Unsupported source value: ${value}`);
  }
}

function parseSourceMeta(sourceMetaJson: string | undefined): Record<string, unknown> | null {
  const normalized = normalizeOptionalString(sourceMetaJson);
  if (!normalized) {
    return null;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(normalized);
  } catch (error) {
    throw new Error(`Invalid source_meta_json: ${(error as Error).message}`);
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Invalid source_meta_json: expected JSON object");
  }

  return parsed as Record<string, unknown>;
}

export function deserializeRawEvent(messageValue: Buffer): ParsedRawEvent {
  let decoded: unknown;
  try {
    decoded = JSON.parse(messageValue.toString("utf-8"));
  } catch (error) {
    throw new Error(`Invalid JSON payload: ${(error as Error).message}`);
  }

  const wire = RawEventWireSchema.parse(decoded);

  return {
    eventId: wire.event_id,
    source: parseSource(wire.source),
    fetchedAt: parseDate(wire.fetched_at, "fetched_at"),
    publishedAt: wire.published_at ? parseDate(wire.published_at, "published_at") : null,
    url: normalizeOptionalString(wire.url),
    title: normalizeOptionalString(wire.title),
    text: wire.text,
    authorId: normalizeOptionalString(wire.author?.id),
    authorHandle: normalizeOptionalString(wire.author?.handle),
    authorDisplayName: normalizeOptionalString(wire.author?.display_name),
    engagementScore: wire.engagement?.score ?? null,
    engagementComments: wire.engagement?.comments ?? null,
    engagementLikes: wire.engagement?.likes ?? null,
    engagementShares: wire.engagement?.shares ?? null,
    lang: normalizeOptionalString(wire.lang),
    tags: wire.tags ?? [],
    extractedHashtags: wire.extracted?.hashtags ?? [],
    extractedUrls: wire.extracted?.urls ?? [],
    sourceMeta: parseSourceMeta(wire.source_meta_json),
  };
}
