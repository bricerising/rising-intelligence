import { z } from "zod";
import type { ParsedRawEvent, Source } from "./types.js";

const RawEventWireSchema = z.object({
  event_id: z.string().min(1),
  source: z.union([z.number().int(), z.string().min(1)]),
  fetched_at: z.string().min(1),
  published_at: z.string().optional(),
  url: z.string().optional(),
  title: z.string().optional(),
  text: z.string().min(1),
  tags: z.array(z.string()).optional(),
  engagement: z
    .object({
      score: z.number().optional(),
      comments: z.number().optional(),
      likes: z.number().optional(),
      shares: z.number().optional(),
    })
    .optional(),
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
      return "rss";
    case 2:
      return "news";
    case 3:
      return "hackernews";
    case 4:
      return "reddit";
    case 5:
      return "github";
    case 7:
      return "bluesky";
    case 8:
      return "mastodon";
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
      return "rss";
    case "source_news":
    case "news":
    case "lobsters":
      return "news";
    case "source_hackernews":
    case "hackernews":
    case "hacker_news":
      return "hackernews";
    case "source_reddit":
    case "reddit":
      return "reddit";
    case "source_github":
    case "github":
      return "github";
    case "source_bluesky":
    case "bluesky":
      return "bluesky";
    case "source_mastodon":
    case "mastodon":
      return "mastodon";
    default:
      throw new Error(`Unsupported source value: ${value}`);
  }
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
    tags: wire.tags ?? [],
    engagementScore: wire.engagement?.score ?? 0,
  };
}
