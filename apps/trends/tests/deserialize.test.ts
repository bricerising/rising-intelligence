import { describe, expect, it } from "vitest";
import { deserializeRawEvent, parseSource } from "../src/deserialize.js";

function makeBuffer(payload: unknown): Buffer {
  return Buffer.from(JSON.stringify(payload), "utf-8");
}

describe("trends deserializeRawEvent", () => {
  it("deserializes numeric source payloads", () => {
    const payload = {
      event_id: "rss:123",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      published_at: "2026-02-06T09:59:00.000Z",
      url: "https://example.com/post",
      title: "Example",
      text: "hello world",
      tags: ["aws.bedrock"],
      engagement: {
        score: 42,
      },
    };

    const parsed = deserializeRawEvent(makeBuffer(payload));

    expect(parsed.eventId).toBe("rss:123");
    expect(parsed.source).toBe("rss");
    expect(parsed.fetchedAt.toISOString()).toBe("2026-02-06T10:00:00.000Z");
    expect(parsed.publishedAt?.toISOString()).toBe("2026-02-06T09:59:00.000Z");
    expect(parsed.tags).toEqual(["aws.bedrock"]);
    expect(parsed.engagementScore).toBe(42);
  });

  it("maps lobsters to news", () => {
    expect(parseSource("lobsters")).toBe("news");
    expect(parseSource(2)).toBe("news");
  });

  it("throws for invalid JSON", () => {
    expect(() => deserializeRawEvent(Buffer.from("{not-json", "utf-8"))).toThrow(
      "Invalid JSON payload"
    );
  });

  it("throws for unsupported source", () => {
    expect(() => parseSource(999)).toThrow("Unsupported source enum: 999");
    expect(() => parseSource("unknown")).toThrow("Unsupported source value");
  });

  it("throws for invalid fetched_at", () => {
    const payload = {
      event_id: "rss:1",
      source: 1,
      fetched_at: "not-a-date",
      text: "text",
    };
    expect(() => deserializeRawEvent(makeBuffer(payload))).toThrow("Invalid fetched_at");
  });

  it("throws for invalid published_at", () => {
    const payload = {
      event_id: "rss:1",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      published_at: "not-a-date",
      text: "text",
    };
    expect(() => deserializeRawEvent(makeBuffer(payload))).toThrow("Invalid published_at");
  });

  it("rejects empty required fields", () => {
    const payload = {
      event_id: "",
      source: 1,
      fetched_at: "2026-02-06T10:00:00.000Z",
      text: "",
    };
    expect(() => deserializeRawEvent(makeBuffer(payload))).toThrow();
  });
});
