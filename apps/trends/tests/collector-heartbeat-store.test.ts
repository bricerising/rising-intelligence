import { describe, expect, it } from "vitest";
import {
  recordCollectorHeartbeat,
  type RecordCollectorHeartbeatResult,
} from "../src/collector-heartbeat-store.js";
import type { CollectorHeartbeatState } from "../src/health.js";

function createHeartbeat(
  overrides: Partial<CollectorHeartbeatState> = {}
): CollectorHeartbeatState {
  return {
    source: "rss",
    status: "healthy",
    timestamp: new Date("2026-02-06T10:00:00.000Z"),
    lastFetchAt: new Date("2026-02-06T09:59:30.000Z"),
    itemsFetched: 10,
    ...overrides,
  };
}

describe("collector heartbeat store", () => {
  it("stores heartbeat when source has no prior state", () => {
    const state = new Map<string, CollectorHeartbeatState>();
    const result = recordCollectorHeartbeat(state, createHeartbeat());

    expect(result).toBe<RecordCollectorHeartbeatResult>("stored");
    expect(state.get("rss")).toMatchObject({
      source: "rss",
      status: "healthy",
      itemsFetched: 10,
    });
  });

  it("ignores stale heartbeat updates", () => {
    const state = new Map<string, CollectorHeartbeatState>([
      [
        "rss",
        createHeartbeat({
          status: "healthy",
          timestamp: new Date("2026-02-06T10:00:00.000Z"),
        }),
      ],
    ]);

    const result = recordCollectorHeartbeat(
      state,
      createHeartbeat({
        status: "error",
        timestamp: new Date("2026-02-06T09:59:00.000Z"),
      })
    );

    expect(result).toBe<RecordCollectorHeartbeatResult>("ignored_stale");
    expect(state.get("rss")?.status).toBe("healthy");
  });

  it("stores newer heartbeat updates", () => {
    const state = new Map<string, CollectorHeartbeatState>([
      [
        "rss",
        createHeartbeat({
          status: "degraded",
          timestamp: new Date("2026-02-06T10:00:00.000Z"),
        }),
      ],
    ]);

    const result = recordCollectorHeartbeat(
      state,
      createHeartbeat({
        status: "error",
        timestamp: new Date("2026-02-06T10:01:00.000Z"),
      })
    );

    expect(result).toBe<RecordCollectorHeartbeatResult>("stored");
    expect(state.get("rss")?.status).toBe("error");
  });
});
