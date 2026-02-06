import { describe, it, expect } from "vitest";
import {
  serializeDeadLetterEvent,
  serializeRawEvent,
  serializeHeartbeat,
  generateEventId,
  generateDlqId,
} from "../src/serializer.js";

describe("serializer", () => {
  it("serializes RawEvent with proto-compatible defaults", () => {
    const buf = serializeRawEvent({
      event_id: "rss:abc",
      source: "rss",
      fetched_at: "2026-02-06T00:00:00.000Z",
      text: "hello",
    });

    const obj = JSON.parse(buf.toString("utf-8")) as Record<string, unknown>;
    expect(obj.event_id).toBe("rss:abc");
    expect(obj.source).toBe(1);
    expect(obj.fetched_at).toBe("2026-02-06T00:00:00.000Z");
    expect(obj.published_at).toBe("");
    expect(obj.url).toBe("");
    expect(obj.title).toBe("");
    expect(obj.text).toBe("hello");
    expect(obj.tags).toEqual([]);
    expect(obj.source_meta_json).toBe("");

    // Undefined optional nested fields should be omitted in JSON.
    expect("author" in obj).toBe(false);
    expect("engagement" in obj).toBe(false);
    expect("extracted" in obj).toBe(false);
  });

  it("serializes source_meta_json when provided", () => {
    const buf = serializeRawEvent({
      event_id: "rss:abc",
      source: "rss",
      fetched_at: "2026-02-06T00:00:00.000Z",
      text: "hello",
      source_meta: { feed_name: "Test", n: 1 },
    });

    const obj = JSON.parse(buf.toString("utf-8")) as Record<string, unknown>;
    expect(obj.source_meta_json).toBe(JSON.stringify({ feed_name: "Test", n: 1 }));
  });

  it("maps Lobsters source to proto NEWS enum", () => {
    const buf = serializeRawEvent({
      event_id: "lobsters:abc",
      source: "lobsters",
      fetched_at: "2026-02-06T00:00:00.000Z",
      text: "hello",
    });

    const obj = JSON.parse(buf.toString("utf-8")) as Record<string, unknown>;
    expect(obj.source).toBe(2);
  });

  it("serializes CollectorHeartbeat with enum mappings", () => {
    const buf = serializeHeartbeat({
      source: "hackernews",
      timestamp: "2026-02-06T00:00:00.000Z",
      last_fetch_at: "2026-02-06T00:00:00.000Z",
      items_fetched: 12,
      status: "healthy",
    });

    const obj = JSON.parse(buf.toString("utf-8")) as Record<string, unknown>;
    expect(obj.source).toBe(3);
    expect(obj.status).toBe(1);
    expect(obj.items_fetched).toBe(12);
    expect(obj.error_message).toBe("");
  });

  it("serializes CollectorHeartbeat error status", () => {
    const buf = serializeHeartbeat({
      source: "rss",
      timestamp: "2026-02-06T00:00:00.000Z",
      last_fetch_at: "2026-02-06T00:00:00.000Z",
      items_fetched: 0,
      status: "error",
      error_message: "timeout",
    });

    const obj = JSON.parse(buf.toString("utf-8")) as Record<string, unknown>;
    expect(obj.source).toBe(1);
    expect(obj.status).toBe(3);
    expect(obj.error_message).toBe("timeout");
  });

  it("serializes DeadLetterEvent payloads", () => {
    const buf = serializeDeadLetterEvent({
      dlq_id: "dlq:1:abcdef12",
      occurred_at: "2026-02-06T00:00:00.000Z",
      source: "rss",
      error_code: "VALIDATION_FAILED",
      error_message: "missing text",
      raw_reference: "https://example.com/post",
    });

    const obj = JSON.parse(buf.toString("utf-8")) as Record<string, unknown>;
    expect(obj).toMatchObject({
      dlq_id: "dlq:1:abcdef12",
      source: "rss",
      error_code: "VALIDATION_FAILED",
      error_message: "missing text",
    });
  });

  it("generates IDs", () => {
    expect(generateEventId("rss", "123")).toBe("rss:123");

    const dlqId = generateDlqId();
    expect(dlqId).toMatch(/^dlq:\d+:[a-z0-9]{8}$/);
  });
});
