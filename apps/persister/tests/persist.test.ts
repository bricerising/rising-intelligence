import { describe, expect, it, vi } from "vitest";
import { Source } from "@rising-intelligence/db";
import { persistBatch, upsertConsumerLag } from "../src/persist.js";
import type { ParsedRawEvent } from "../src/types.js";

function createEvent(eventId: string, source: Source): ParsedRawEvent {
  return {
    eventId,
    source,
    fetchedAt: new Date("2026-02-06T10:00:00.000Z"),
    publishedAt: null,
    url: "https://example.com",
    title: "Example",
    text: "hello",
    authorId: null,
    authorHandle: null,
    authorDisplayName: null,
    engagementScore: null,
    engagementComments: null,
    engagementLikes: null,
    engagementShares: null,
    lang: null,
    tags: ["aws.bedrock"],
    extractedHashtags: [],
    extractedUrls: [],
    sourceMeta: null,
  };
}

describe("persistBatch", () => {
  it("groups by source and calculates duplicates", async () => {
    const createMany = vi
      .fn()
      .mockResolvedValueOnce({ count: 1 })
      .mockResolvedValueOnce({ count: 1 });

    const prisma = {
      rawEvent: {
        createMany,
      },
    } as any;

    const events = [
      createEvent("rss:1", Source.rss),
      createEvent("rss:2", Source.rss),
      createEvent("reddit:1", Source.reddit),
    ];

    const result = await persistBatch(prisma, events);

    expect(createMany).toHaveBeenCalledTimes(2);
    expect(result.attempted).toBe(3);
    expect(result.inserted).toBe(2);
    expect(result.duplicates).toBe(1);
    expect(result.insertedBySource.get(Source.rss)).toBe(1);
    expect(result.insertedBySource.get(Source.reddit)).toBe(1);
  });

  it("returns empty result for empty input", async () => {
    const prisma = {
      rawEvent: {
        createMany: vi.fn(),
      },
    } as any;

    const result = await persistBatch(prisma, []);

    expect(result).toMatchObject({
      attempted: 0,
      inserted: 0,
      duplicates: 0,
    });
    expect(prisma.rawEvent.createMany).not.toHaveBeenCalled();
  });

  it("maps tags to both tags and topics fields", async () => {
    const createMany = vi.fn().mockResolvedValue({ count: 1 });

    const prisma = {
      rawEvent: { createMany },
    } as any;

    const event = createEvent("rss:tagged", Source.rss);
    event.tags = ["aws.bedrock", "ai.agents"];

    await persistBatch(prisma, [event]);

    const createManyData = createMany.mock.calls[0][0].data[0];
    expect(createManyData.tags).toEqual(["aws.bedrock", "ai.agents"]);
    expect(createManyData.topics).toEqual(["aws.bedrock", "ai.agents"]);
  });

  it("adds quality metadata when sourceMeta is null", async () => {
    const createMany = vi.fn().mockResolvedValue({ count: 1 });

    const prisma = {
      rawEvent: { createMany },
    } as any;

    const event = createEvent("rss:null-meta", Source.rss);
    event.sourceMeta = null;

    await persistBatch(prisma, [event]);

    const createManyData = createMany.mock.calls[0][0].data[0];
    expect(createManyData.sourceMeta).toMatchObject({
      ri_quality: expect.objectContaining({
        schema_version: 1,
      }),
    });
  });

  it("passes through non-null sourceMeta", async () => {
    const createMany = vi.fn().mockResolvedValue({ count: 1 });

    const prisma = {
      rawEvent: { createMany },
    } as any;

    const event = createEvent("rss:meta", Source.rss);
    event.sourceMeta = { subreddit: "aws", score: 42 };

    await persistBatch(prisma, [event]);

    const createManyData = createMany.mock.calls[0][0].data[0];
    expect(createManyData.sourceMeta).toMatchObject({ subreddit: "aws", score: 42 });
    expect(createManyData.sourceMeta.ri_quality).toBeDefined();
  });

  it("propagates Postgres errors", async () => {
    const createMany = vi.fn().mockRejectedValue(new Error("connection refused"));

    const prisma = {
      rawEvent: { createMany },
    } as any;

    const events = [createEvent("rss:1", Source.rss)];

    await expect(persistBatch(prisma, events)).rejects.toThrow("connection refused");
  });

  it("maps all ParsedRawEvent fields to createMany input", async () => {
    const createMany = vi.fn().mockResolvedValue({ count: 1 });

    const prisma = {
      rawEvent: { createMany },
    } as any;

    const event: ParsedRawEvent = {
      eventId: "rss:full",
      source: Source.rss,
      fetchedAt: new Date("2026-02-06T10:00:00.000Z"),
      publishedAt: new Date("2026-02-06T09:00:00.000Z"),
      url: "https://example.com/post",
      title: "Full Event",
      text: "full text",
      authorId: "author-1",
      authorHandle: "handle",
      authorDisplayName: "Author Name",
      engagementScore: 100,
      engagementComments: 10,
      engagementLikes: 80,
      engagementShares: 10,
      lang: "en",
      tags: ["aws.bedrock"],
      extractedHashtags: ["#aws"],
      extractedUrls: ["https://example.com"],
      sourceMeta: { key: "value" },
    };

    await persistBatch(prisma, [event]);

    const data = createMany.mock.calls[0][0].data[0];
    expect(data.eventId).toBe("rss:full");
    expect(data.source).toBe(Source.rss);
    expect(data.fetchedAt).toEqual(new Date("2026-02-06T10:00:00.000Z"));
    expect(data.publishedAt).toEqual(new Date("2026-02-06T09:00:00.000Z"));
    expect(data.url).toBe("https://example.com/post");
    expect(data.title).toBe("Full Event");
    expect(data.text).toBe("full text");
    expect(data.authorId).toBe("author-1");
    expect(data.authorHandle).toBe("handle");
    expect(data.authorDisplayName).toBe("Author Name");
    expect(data.engagementScore).toBe(100);
    expect(data.engagementComments).toBe(10);
    expect(data.engagementLikes).toBe(80);
    expect(data.engagementShares).toBe(10);
    expect(data.lang).toBe("en");
    expect(data.extractedHashtags).toEqual(["#aws"]);
    expect(data.extractedUrls).toEqual(["https://example.com"]);
    expect(data.sourceMeta.ri_quality).toBeDefined();
  });

  it("normalizes malformed URLs and strips tracking query params", async () => {
    const createMany = vi.fn().mockResolvedValue({ count: 1 });
    const prisma = {
      rawEvent: { createMany },
    } as any;

    const event = createEvent("rss:url-normalize", Source.rss);
    event.url =
      "https://aws.amazon.comabout-aws/whats-new/2026/02/test/?utm_source=feed&utm_medium=rss&id=42#section";

    await persistBatch(prisma, [event]);

    const data = createMany.mock.calls[0][0].data[0];
    expect(data.url).toBe("https://aws.amazon.com/about-aws/whats-new/2026/02/test?id=42");
    expect(data.sourceMeta.ri_quality).toMatchObject({
      normalized_url: true,
      invalid_url: false,
      url_issue_codes: expect.arrayContaining([
        "repaired_known_url_typo",
        "removed_tracking_query_params",
        "removed_fragment",
      ]),
    });
  });

  it("flags stale events when fetchedAt is over 30 days after publishedAt", async () => {
    const createMany = vi.fn().mockResolvedValue({ count: 1 });
    const prisma = {
      rawEvent: { createMany },
    } as any;

    const event = createEvent("rss:stale", Source.rss);
    event.fetchedAt = new Date("2026-02-15T10:00:00.000Z");
    event.publishedAt = new Date("2025-12-01T10:00:00.000Z");

    await persistBatch(prisma, [event]);

    const data = createMany.mock.calls[0][0].data[0];
    expect(data.sourceMeta.ri_quality).toMatchObject({
      stale_event: true,
      stale_age_hours: expect.any(Number),
      published_in_future: false,
    });
  });

  it("infers tags/topics for untagged events from content", async () => {
    const createMany = vi.fn().mockResolvedValue({ count: 1 });
    const prisma = {
      rawEvent: { createMany },
    } as any;

    const event = createEvent("rss:infer-topics", Source.rss);
    event.tags = [];
    event.title = "OpenAI and Anthropic launch new LLM agents";
    event.text = "OpenAI announced GPT updates while Anthropic shipped Claude agents for developers.";

    await persistBatch(prisma, [event]);

    const data = createMany.mock.calls[0][0].data[0];
    expect(data.tags).toEqual(
      expect.arrayContaining(["ai.openai", "ai.anthropic", "ai.llm", "ai.agents"])
    );
    expect(data.topics).toEqual(data.tags);
    expect(data.sourceMeta.ri_quality).toMatchObject({
      inferred_topics: true,
      inferred_topic_count: data.tags.length,
    });
  });

  it("infers English language for missing lang values", async () => {
    const createMany = vi.fn().mockResolvedValue({ count: 1 });
    const prisma = {
      rawEvent: { createMany },
    } as any;

    const event = createEvent("rss:infer-lang", Source.rss);
    event.lang = null;
    event.title = "New cloud deployment guidance";
    event.text = "The team released a new guide for cloud migration and security with better performance.";

    await persistBatch(prisma, [event]);

    const data = createMany.mock.calls[0][0].data[0];
    expect(data.lang).toBe("en");
    expect(data.sourceMeta.ri_quality).toMatchObject({
      inferred_lang: true,
      lang_inference_method: "heuristic_en",
    });
  });

  it("infers Apple ecosystem topics for untagged Apple stories", async () => {
    const createMany = vi.fn().mockResolvedValue({ count: 1 });
    const prisma = {
      rawEvent: { createMany },
    } as any;

    const event = createEvent("rss:infer-apple", Source.rss);
    event.tags = [];
    event.title = "Apple launches new Hello Apple Instagram account";
    event.text = "Apple is expanding its ecosystem marketing with a new Hello Apple account.";

    await persistBatch(prisma, [event]);

    const data = createMany.mock.calls[0][0].data[0];
    expect(data.tags).toEqual(expect.arrayContaining(["apple.ecosystem"]));
    expect(data.topics).toEqual(data.tags);
    expect(data.sourceMeta.ri_quality).toMatchObject({
      inferred_topics: true,
    });
  });

  it("replaces placeholder comments text with title when available", async () => {
    const createMany = vi.fn().mockResolvedValue({ count: 1 });
    const prisma = {
      rawEvent: { createMany },
    } as any;

    const event = createEvent("news:comments-placeholder", Source.news);
    event.title = "A detailed incident write-up";
    event.text = "Comments";

    await persistBatch(prisma, [event]);

    const data = createMany.mock.calls[0][0].data[0];
    expect(data.text).toBe("A detailed incident write-up");
    expect(data.sourceMeta.ri_quality).toMatchObject({
      low_information_text: true,
      text_issue_codes: expect.arrayContaining(["comments_placeholder_text"]),
    });
  });
});

describe("upsertConsumerLag", () => {
  it("writes lag via composite key upsert", async () => {
    const upsert = vi.fn().mockResolvedValue(undefined);

    const prisma = {
      consumerLag: {
        upsert,
      },
    } as any;

    await upsertConsumerLag(prisma, {
      consumerGroup: "persister",
      topic: "events.raw",
      partition: 0,
      currentOffset: 10n,
      latestOffset: 15n,
      lagMessages: 5n,
      observedAt: new Date("2026-02-06T10:00:00.000Z"),
    });

    expect(upsert).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          consumerGroup_topic_partition: {
            consumerGroup: "persister",
            topic: "events.raw",
            partition: 0,
          },
        },
      })
    );
  });

  it("passes all lag fields to both create and update", async () => {
    const upsert = vi.fn().mockResolvedValue(undefined);
    const prisma = { consumerLag: { upsert } } as any;

    const observedAt = new Date("2026-02-06T10:00:00.000Z");

    await upsertConsumerLag(prisma, {
      consumerGroup: "persister",
      topic: "events.raw",
      partition: 2,
      currentOffset: 100n,
      latestOffset: 150n,
      lagMessages: 50n,
      observedAt,
    });

    const call = upsert.mock.calls[0][0];
    expect(call.update).toEqual({
      currentOffset: 100n,
      latestOffset: 150n,
      lagMessages: 50n,
      updatedAt: observedAt,
    });
    expect(call.create).toMatchObject({
      consumerGroup: "persister",
      topic: "events.raw",
      partition: 2,
      currentOffset: 100n,
      latestOffset: 150n,
      lagMessages: 50n,
    });
  });
});
