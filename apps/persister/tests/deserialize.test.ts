import { describe, expect, it } from "vitest";
import { Source } from "@rising-intelligence/db";
import { deserializeRawEvent, parseSource } from "../src/deserialize.js";

function makeBuffer(payload: unknown): Buffer {
  return Buffer.from(JSON.stringify(payload), "utf-8");
}

describe("deserializeRawEvent", () => {
  it("deserializes proto-style numeric source payload", () => {
    const payload = {
      event_id: "rss:123",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      published_at: "2026-02-06T09:59:00.000Z",
      url: "https://example.com/post",
      title: "Example",
      text: "hello world",
      author: {
        id: "a1",
        handle: "author",
        display_name: "Author",
      },
      engagement: {
        score: 42,
        comments: 10,
        likes: 20,
        shares: 12,
      },
      tags: ["aws.bedrock"],
      extracted: {
        hashtags: ["aws"],
        urls: ["https://example.com/post"],
      },
      source_meta_json: JSON.stringify({ feed: "main" }),
    };

    const parsed = deserializeRawEvent(makeBuffer(payload));

    expect(parsed.eventId).toBe("rss:123");
    expect(parsed.source).toBe(Source.rss);
    expect(parsed.fetchedAt.toISOString()).toBe("2026-02-06T10:00:00.000Z");
    expect(parsed.publishedAt?.toISOString()).toBe("2026-02-06T09:59:00.000Z");
    expect(parsed.tags).toEqual(["aws.bedrock"]);
    expect(parsed.sourceMeta).toEqual({ feed: "main" });
  });

  it("handles blank optional strings as nulls", () => {
    const payload = {
      event_id: "hn:1",
      source: "hackernews",
      fetched_at: "2026-02-06T10:00:00.000Z",
      published_at: "",
      url: "",
      title: "",
      text: "item text",
      lang: "",
      source_meta_json: "",
    };

    const parsed = deserializeRawEvent(makeBuffer(payload));

    expect(parsed.source).toBe(Source.hackernews);
    expect(parsed.publishedAt).toBeNull();
    expect(parsed.url).toBeNull();
    expect(parsed.title).toBeNull();
    expect(parsed.lang).toBeNull();
    expect(parsed.sourceMeta).toBeNull();
  });

  it("throws for invalid JSON", () => {
    expect(() => deserializeRawEvent(Buffer.from("{not-json", "utf-8"))).toThrow(
      "Invalid JSON payload"
    );
  });

  it("throws for invalid source_meta_json", () => {
    const payload = {
      event_id: "rss:123",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      text: "text",
      source_meta_json: "[1,2,3]",
    };

    expect(() => deserializeRawEvent(makeBuffer(payload))).toThrow(
      "Invalid source_meta_json"
    );
  });

  it("throws for unsupported source", () => {
    expect(() => parseSource(999)).toThrow("Unsupported source enum: 999");
    expect(() => parseSource("unknown")).toThrow("Unsupported source value");
  });

  it("maps lobsters source to news", () => {
    expect(parseSource("lobsters")).toBe(Source.news);
    expect(parseSource(2)).toBe(Source.news);
  });

  it("parses all string source names", () => {
    expect(parseSource("rss")).toBe(Source.rss);
    expect(parseSource("source_rss")).toBe(Source.rss);
    expect(parseSource("news")).toBe(Source.news);
    expect(parseSource("source_news")).toBe(Source.news);
    expect(parseSource("hackernews")).toBe(Source.hackernews);
    expect(parseSource("hacker_news")).toBe(Source.hackernews);
    expect(parseSource("source_hackernews")).toBe(Source.hackernews);
    expect(parseSource("reddit")).toBe(Source.reddit);
    expect(parseSource("source_reddit")).toBe(Source.reddit);
    expect(parseSource("github")).toBe(Source.github);
    expect(parseSource("source_github")).toBe(Source.github);
    expect(parseSource("bluesky")).toBe(Source.bluesky);
    expect(parseSource("source_bluesky")).toBe(Source.bluesky);
    expect(parseSource("mastodon")).toBe(Source.mastodon);
    expect(parseSource("source_mastodon")).toBe(Source.mastodon);
  });

  it("parses all numeric source values", () => {
    expect(parseSource(1)).toBe(Source.rss);
    expect(parseSource(2)).toBe(Source.news);
    expect(parseSource(3)).toBe(Source.hackernews);
    expect(parseSource(4)).toBe(Source.reddit);
    expect(parseSource(5)).toBe(Source.github);
    expect(parseSource(7)).toBe(Source.bluesky);
    expect(parseSource(8)).toBe(Source.mastodon);
  });

  it("parses numeric strings as source enums", () => {
    expect(parseSource("1")).toBe(Source.rss);
    expect(parseSource("3")).toBe(Source.hackernews);
    expect(parseSource("7")).toBe(Source.bluesky);
  });

  it("is case-insensitive for string sources", () => {
    expect(parseSource("RSS")).toBe(Source.rss);
    expect(parseSource("HackerNews")).toBe(Source.hackernews);
    expect(parseSource("SOURCE_BLUESKY")).toBe(Source.bluesky);
  });

  it("throws for reserved source enum 6", () => {
    expect(() => parseSource(6)).toThrow("Unsupported source enum: 6");
  });

  it("throws for invalid date in fetched_at", () => {
    const payload = {
      event_id: "rss:1",
      source: 1,
      fetched_at: "not-a-date",
      text: "text",
    };

    expect(() => deserializeRawEvent(makeBuffer(payload))).toThrow("Invalid fetched_at");
  });

  it("throws for invalid date in published_at", () => {
    const payload = {
      event_id: "rss:1",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      published_at: "garbage",
      text: "text",
    };

    expect(() => deserializeRawEvent(makeBuffer(payload))).toThrow("Invalid published_at");
  });

  it("rejects empty event_id", () => {
    const payload = {
      event_id: "",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      text: "text",
    };

    expect(() => deserializeRawEvent(makeBuffer(payload))).toThrow();
  });

  it("rejects empty text", () => {
    const payload = {
      event_id: "rss:1",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      text: "",
    };

    expect(() => deserializeRawEvent(makeBuffer(payload))).toThrow();
  });

  it("rejects missing required fields", () => {
    const payload = { source: 1, text: "text" };

    expect(() => deserializeRawEvent(makeBuffer(payload))).toThrow();
  });

  it("handles valid source_meta_json object", () => {
    const payload = {
      event_id: "rss:1",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      text: "text",
      source_meta_json: JSON.stringify({ subreddit: "aws", score: 42 }),
    };

    const parsed = deserializeRawEvent(makeBuffer(payload));

    expect(parsed.sourceMeta).toEqual({ subreddit: "aws", score: 42 });
  });

  it("rejects source_meta_json that is a primitive", () => {
    const payload = {
      event_id: "rss:1",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      text: "text",
      source_meta_json: '"just a string"',
    };

    expect(() => deserializeRawEvent(makeBuffer(payload))).toThrow("Invalid source_meta_json");
  });

  it("defaults optional arrays to empty", () => {
    const payload = {
      event_id: "rss:1",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      text: "text",
    };

    const parsed = deserializeRawEvent(makeBuffer(payload));

    expect(parsed.tags).toEqual([]);
    expect(parsed.extractedHashtags).toEqual([]);
    expect(parsed.extractedUrls).toEqual([]);
  });
});
