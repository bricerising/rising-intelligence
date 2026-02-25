import { describe, expect, it } from "vitest";
import { deserializeCollectorHeartbeat } from "../src/collector-heartbeat-adapter.js";

function encode(value: unknown): Buffer {
  return Buffer.from(JSON.stringify(value), "utf-8");
}

describe("collector heartbeat adapter", () => {
  it("deserializes numeric source and status payloads", () => {
    const heartbeat = deserializeCollectorHeartbeat(
      encode({
        source: 1,
        status: 1,
        timestamp: "2026-02-06T10:00:00.000Z",
        last_fetch_at: "2026-02-06T09:59:30.000Z",
        items_fetched: 12,
        error_message: "",
      })
    );

    expect(heartbeat).toMatchObject({
      source: "rss",
      status: "healthy",
      itemsFetched: 12,
      errorMessage: undefined,
    });
  });

  it("accepts named status aliases and numeric-string fields", () => {
    const heartbeat = deserializeCollectorHeartbeat(
      encode({
        source: "rss",
        status: "collector_status_degraded",
        timestamp: "2026-02-06T10:00:00.000Z",
        last_fetch_at: "2026-02-06T09:59:30.000Z",
        items_fetched: "7",
        error_message: "rate limited",
      })
    );

    expect(heartbeat).toMatchObject({
      source: "rss",
      status: "degraded",
      itemsFetched: 7,
      errorMessage: "rate limited",
    });
  });

  it("throws for unsupported heartbeat statuses", () => {
    expect(() =>
      deserializeCollectorHeartbeat(
        encode({
          source: 1,
          status: "unknown",
          timestamp: "2026-02-06T10:00:00.000Z",
          last_fetch_at: "2026-02-06T09:59:30.000Z",
          items_fetched: 7,
        })
      )
    ).toThrow("Unsupported collector heartbeat status");
  });

  it("throws for malformed JSON", () => {
    expect(() => deserializeCollectorHeartbeat(Buffer.from("{", "utf-8"))).toThrow(
      "Invalid collector heartbeat JSON"
    );
  });
});
