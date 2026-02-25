export const SOURCE_ENUM_TO_KEY = {
  1: "rss",
  2: "news",
  3: "hackernews",
  4: "reddit",
  5: "github",
  7: "bluesky",
  8: "mastodon",
} as const;

export type CanonicalSource = (typeof SOURCE_ENUM_TO_KEY)[keyof typeof SOURCE_ENUM_TO_KEY];

const SOURCE_ALIAS_TO_KEY: Readonly<Record<string, CanonicalSource>> = {
  source_rss: "rss",
  rss: "rss",
  source_news: "news",
  news: "news",
  lobsters: "news",
  source_hackernews: "hackernews",
  hackernews: "hackernews",
  hacker_news: "hackernews",
  source_reddit: "reddit",
  reddit: "reddit",
  source_github: "github",
  github: "github",
  source_bluesky: "bluesky",
  bluesky: "bluesky",
  source_mastodon: "mastodon",
  mastodon: "mastodon",
};

function parseSourceNumber(source: number): CanonicalSource {
  const parsed = SOURCE_ENUM_TO_KEY[source as keyof typeof SOURCE_ENUM_TO_KEY];
  if (!parsed) {
    throw new Error(`Unsupported source enum: ${source}`);
  }

  return parsed;
}

export function parseCanonicalSource(value: number | string): CanonicalSource {
  if (typeof value === "number") {
    return parseSourceNumber(value);
  }

  const trimmed = value.trim();
  const asNumber = Number.parseInt(trimmed, 10);
  if (!Number.isNaN(asNumber) && `${asNumber}` === trimmed) {
    return parseSourceNumber(asNumber);
  }

  const normalized = trimmed.toLowerCase();
  const parsed = SOURCE_ALIAS_TO_KEY[normalized];
  if (!parsed) {
    throw new Error(`Unsupported source value: ${value}`);
  }

  return parsed;
}
