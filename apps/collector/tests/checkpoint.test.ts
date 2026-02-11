import { describe, it, expect, vi } from "vitest";
import { mkdtempSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { CheckpointStore } from "../src/checkpoint.js";

const sqliteMock = vi.hoisted(() => {
  interface CheckpointRow {
    source: string;
    checkpointKey: string;
    checkpointValue: string;
  }

  interface SeenRow {
    source: string;
    eventId: string;
    seenAtMs: number;
  }

  function parseIntervalToMs(interval: string): number {
    const match = interval
      .trim()
      .match(/^([+-])(\d+)\s*(day|days|hour|hours|minute|minutes|second|seconds)$/i);
    if (!match) {
      return 0;
    }

    const [, sign, amountRaw, unit] = match;
    const amount = Number.parseInt(amountRaw, 10);
    const unitMs =
      unit.toLowerCase().startsWith("day") ? 86_400_000
        : unit.toLowerCase().startsWith("hour") ? 3_600_000
          : unit.toLowerCase().startsWith("minute") ? 60_000
            : 1_000;
    const direction = sign === "-" ? -1 : 1;
    return direction * amount * unitMs;
  }

  function normalizeSql(sql: string): string {
    return sql.replace(/\s+/g, " ").trim().toLowerCase();
  }

  class MockDatabase {
    private checkpoints = new Map<string, CheckpointRow>();
    private seenEvents = new Map<string, SeenRow>();

    constructor(_path: string) {}

    pragma(_sql: string): void {}

    exec(_sql: string): void {}

    prepare(sql: string): { get?: (...args: unknown[]) => unknown; run?: (...args: unknown[]) => { changes: number }; all?: (...args: unknown[]) => unknown[] } {
      const normalized = normalizeSql(sql);

      if (normalized.includes("select checkpoint_value") && normalized.includes("from checkpoints")) {
        return {
          get: (source: unknown, checkpointKey: unknown) => {
            const key = `${String(source)}:${String(checkpointKey)}`;
            const row = this.checkpoints.get(key);
            return row ? { checkpoint_value: row.checkpointValue } : undefined;
          },
        };
      }

      if (normalized.includes("insert or replace into checkpoints")) {
        return {
          run: (source: unknown, checkpointKey: unknown, checkpointValue: unknown) => {
            const row: CheckpointRow = {
              source: String(source),
              checkpointKey: String(checkpointKey),
              checkpointValue: String(checkpointValue),
            };
            this.checkpoints.set(`${row.source}:${row.checkpointKey}`, row);
            return { changes: 1 };
          },
        };
      }

      if (normalized.includes("select 1 as present") && normalized.includes("from seen_events")) {
        return {
          get: (source: unknown, eventId: unknown) => {
            const key = `${String(source)}:${String(eventId)}`;
            return this.seenEvents.has(key) ? { present: 1 } : undefined;
          },
        };
      }

      if (normalized.includes("insert or ignore into seen_events")) {
        return {
          run: (source: unknown, eventId: unknown) => {
            const key = `${String(source)}:${String(eventId)}`;
            if (this.seenEvents.has(key)) {
              return { changes: 0 };
            }

            this.seenEvents.set(key, {
              source: String(source),
              eventId: String(eventId),
              seenAtMs: Date.now(),
            });
            return { changes: 1 };
          },
        };
      }

      if (normalized.includes("delete from seen_events")) {
        return {
          run: (olderThan: unknown) => {
            const intervalMs = parseIntervalToMs(String(olderThan));
            const cutoff = Date.now() + intervalMs;
            let deleted = 0;

            for (const [key, row] of this.seenEvents) {
              if (row.seenAtMs < cutoff) {
                this.seenEvents.delete(key);
                deleted += 1;
              }
            }

            return { changes: deleted };
          },
        };
      }

      if (normalized.includes("select source, checkpoint_key, checkpoint_value")) {
        return {
          all: (sourcePattern: unknown) => {
            const pattern = String(sourcePattern);
            const prefix = pattern.endsWith("%") ? pattern.slice(0, -1) : pattern;

            return [...this.checkpoints.values()]
              .filter((row) => row.source.startsWith(prefix))
              .map((row) => ({
                source: row.source,
                checkpoint_key: row.checkpointKey,
                checkpoint_value: row.checkpointValue,
              }));
          },
        };
      }

      return {
        run: () => ({ changes: 0 }),
      };
    }

    close(): void {}
  }

  return { MockDatabase };
});

vi.mock("better-sqlite3", () => ({
  default: sqliteMock.MockDatabase,
}));

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

    // A forward interval makes all current rows older than the cutoff.
    const deleted = store.cleanupSeen("+1 day");
    expect(deleted).toBe(1);
    expect(store.hasSeen("rss", "event-old")).toBe(false);

    store.close();
  });
});
