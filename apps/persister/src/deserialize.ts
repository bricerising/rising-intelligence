import { deserializeRawEvent as sharedDeserialize, parseCanonicalSource } from "@rising-intelligence/shared";
import { Source } from "@rising-intelligence/db";
import type { ParsedRawEvent } from "./types.js";

export function parseSource(value: number | string): Source {
  const canonical = parseCanonicalSource(value);
  if (canonical === "rss") {
    return Source.rss;
  }
  if (canonical === "news") {
    return Source.news;
  }
  if (canonical === "hackernews") {
    return Source.hackernews;
  }
  if (canonical === "reddit") {
    return Source.reddit;
  }
  if (canonical === "github") {
    return Source.github;
  }
  if (canonical === "bluesky") {
    return Source.bluesky;
  }

  return Source.mastodon;
}

export function deserializeRawEvent(messageValue: Buffer): ParsedRawEvent {
  const shared = sharedDeserialize(messageValue);
  return {
    ...shared,
    source: parseSource(shared.source),
  };
}
