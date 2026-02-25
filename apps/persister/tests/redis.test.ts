import { describe, expect, it, vi } from "vitest";
import { Source } from "@rising-intelligence/db";
import { markEventsSeen, disconnectRedis } from "../src/redis.js";
import type { ParsedRawEvent } from "../src/types.js";

function createEvent(eventId: string, source: Source): ParsedRawEvent {
  return {
    eventId,
    source,
    fetchedAt: new Date("2026-02-06T10:00:00.000Z"),
    publishedAt: null,
    url: null,
    title: null,
    text: "text",
    authorId: null,
    authorHandle: null,
    authorDisplayName: null,
    engagementScore: null,
    engagementComments: null,
    engagementLikes: null,
    engagementShares: null,
    lang: null,
    tags: [],
    extractedHashtags: [],
    extractedUrls: [],
    sourceMeta: null,
  };
}

function createMockPipeline() {
  const commands: Array<{ key: string; value: string; ex: string; ttl: number; nx?: string }> = [];
  return {
    commands,
    set(key: string, value: string, ex: string, ttl: number, nx?: string) {
      commands.push({ key, value, ex, ttl, nx });
    },
    exec: vi.fn().mockResolvedValue(commands.map(() => [null, "OK"])),
  };
}

function createMockRedis(pipeline: ReturnType<typeof createMockPipeline>) {
  return {
    pipeline: () => pipeline,
    quit: vi.fn().mockResolvedValue("OK"),
  } as any;
}

describe("markEventsSeen", () => {
  it("sets seen keys with correct key format and TTL", async () => {
    const pipeline = createMockPipeline();
    const redis = createMockRedis(pipeline);

    const events = [
      createEvent("rss:abc", Source.rss),
      createEvent("hn:123", Source.hackernews),
    ];

    await markEventsSeen(redis, events, 3600);

    expect(pipeline.commands).toHaveLength(2);
    expect(pipeline.commands[0]).toEqual({
      key: `seen:${Source.rss}:rss:abc`,
      value: "1",
      ex: "EX",
      ttl: 3600,
      nx: "NX",
    });
    expect(pipeline.commands[1]).toEqual({
      key: `seen:${Source.hackernews}:hn:123`,
      value: "1",
      ex: "EX",
      ttl: 3600,
      nx: "NX",
    });
    expect(pipeline.exec).toHaveBeenCalledOnce();
  });

  it("skips pipeline for empty events array", async () => {
    const pipeline = createMockPipeline();
    const redis = createMockRedis(pipeline);

    await markEventsSeen(redis, [], 3600);

    expect(pipeline.exec).not.toHaveBeenCalled();
  });

  it("throws when pipeline returns null", async () => {
    const pipeline = createMockPipeline();
    pipeline.exec.mockResolvedValue(null);
    const redis = createMockRedis(pipeline);

    const events = [createEvent("rss:1", Source.rss)];

    await expect(markEventsSeen(redis, events, 3600)).rejects.toThrow(
      "Redis pipeline execution returned null"
    );
  });

  it("throws when any pipeline command errors", async () => {
    const pipeline = createMockPipeline();
    const redisError = new Error("READONLY");
    pipeline.exec.mockResolvedValue([[redisError, null]]);
    const redis = createMockRedis(pipeline);

    const events = [createEvent("rss:1", Source.rss)];

    await expect(markEventsSeen(redis, events, 3600)).rejects.toThrow("READONLY");
  });
});

describe("disconnectRedis", () => {
  it("calls quit on redis client", async () => {
    const redis = { quit: vi.fn().mockResolvedValue("OK") } as any;

    await disconnectRedis(redis);

    expect(redis.quit).toHaveBeenCalledOnce();
  });

  it("does nothing for null client", async () => {
    await disconnectRedis(null);
    // no throw
  });
});
