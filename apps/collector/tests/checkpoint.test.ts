import { describe, it, expect, vi } from "vitest";
import { mkdtempSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import Database from "better-sqlite3";
import { CheckpointStore } from "../src/checkpoint.js";

function createTestLogger() {
  return {
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  } as any;
}

describe("CheckpointStore", () => {
  it("throws if used before initialize", () => {
    const dir = mkdtempSync(join(tmpdir(), "ri-checkpoints-"));
    const dbPath = join(dir, "data", "checkpoints.db");
    const store = new CheckpointStore(dbPath, createTestLogger());

    expect(() => store.getCheckpoint("rss", "k")).toThrow(/not initialized/i);
    expect(() => store.setCheckpoint("rss", "k", "v")).toThrow(/not initialized/i);
    expect(() => store.hasSeen("rss", "e1")).toThrow(/not initialized/i);
    expect(() => store.markSeen("rss", "e1")).toThrow(/not initialized/i);
  });

  it("persists checkpoints and seen events", async () => {
    const dir = mkdtempSync(join(tmpdir(), "ri-checkpoints-"));
    const dbPath = join(dir, "nested", "checkpoints.db");
    const store = new CheckpointStore(dbPath, createTestLogger());
    await store.initialize();

    expect(existsSync(join(dir, "nested"))).toBe(true);

    store.setCheckpoint("rss", "last_guid_feed1", "guid-123");
    expect(store.getCheckpoint("rss", "last_guid_feed1")).toBe("guid-123");
    expect(store.getCheckpoint("rss", "missing")).toBeUndefined();

    store.markSeen("rss", "event-1");
    expect(store.hasSeen("rss", "event-1")).toBe(true);
    expect(store.hasSeen("rss", "event-2")).toBe(false);

    store.setCheckpoint("hackernews", "last_max_id_top", "42");
    const checkpoints = store.listCheckpoints("r");
    expect(checkpoints).toEqual({ rss: { last_guid_feed1: "guid-123" } });

    store.close();
  });

  it("cleans up old seen events", async () => {
    const dir = mkdtempSync(join(tmpdir(), "ri-checkpoints-"));
    const dbPath = join(dir, "checkpoints.db");
    const store = new CheckpointStore(dbPath, createTestLogger());
    await store.initialize();

    store.markSeen("rss", "event-old");
    expect(store.hasSeen("rss", "event-old")).toBe(true);

    // Force the row to be "old" via a second DB connection.
    const db = new Database(dbPath);
    db.prepare(`UPDATE seen_events SET seen_at = '2000-01-01 00:00:00' WHERE event_id = ?`).run(
      "event-old"
    );
    db.close();

    const deleted = store.cleanupSeen("-7 days");
    expect(deleted).toBe(1);
    expect(store.hasSeen("rss", "event-old")).toBe(false);

    store.close();
  });
});
