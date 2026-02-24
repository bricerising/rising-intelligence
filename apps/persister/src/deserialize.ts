import {
  deserializeRawEvent as sharedDeserialize,
  parseCanonicalSource,
  type CanonicalSource,
} from "@rising-intelligence/pipeline";
import { Source } from "@rising-intelligence/db";
import type { ParsedRawEvent } from "./types.js";

const CANONICAL_SOURCE_TO_DB_SOURCE: Readonly<Record<CanonicalSource, Source>> = {
  rss: Source.rss,
  news: Source.news,
  hackernews: Source.hackernews,
  reddit: Source.reddit,
  github: Source.github,
  bluesky: Source.bluesky,
  mastodon: Source.mastodon,
};

export function parseSource(value: number | string): Source {
  const canonical = parseCanonicalSource(value);
  return CANONICAL_SOURCE_TO_DB_SOURCE[canonical];
}

export function deserializeRawEvent(messageValue: Buffer): ParsedRawEvent {
  const shared = sharedDeserialize(messageValue);
  return {
    ...shared,
    source: parseSource(shared.source),
  };
}
