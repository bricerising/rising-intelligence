import { z } from "zod";
import { parseCanonicalSource, type CanonicalSource } from "./source.js";

export const RawEventWireSchema = z.object({
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

export interface ParsedRawEvent {
  eventId: string;
  source: CanonicalSource;
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
  extractedHashtags: string[];
  extractedUrls: string[];
  sourceMeta: Record<string, unknown> | null;
}

export function parseDate(value: string, field: string): Date {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid ${field}: ${value}`);
  }
  return parsed;
}

export function normalizeOptionalString(value: string | undefined): string | null {
  if (value === undefined) {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
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
    source: parseCanonicalSource(wire.source),
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
