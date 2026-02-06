import { describe, expect, it, vi } from "vitest";
import {
  applyEventToWindows,
  getBucketStart,
  getCounterKey,
  getDedupKey,
  getEvidenceKey,
  getPreviousCounterKey,
  writePreviousWindowCounts,
} from "../src/redis.js";

describe("trends redis key helpers", () => {
  describe("getBucketStart", () => {
    it("aligns 15m window to 15-minute boundary", () => {
      // 10:07 should align to 10:00
      const ts = new Date("2026-02-06T10:07:30.123Z");
      const bucket = getBucketStart(ts, "15m");
      expect(bucket).toBe("2026-02-06T10:00:00.000Z");
    });

    it("aligns 15m window at exact boundary", () => {
      const ts = new Date("2026-02-06T10:15:00.000Z");
      const bucket = getBucketStart(ts, "15m");
      expect(bucket).toBe("2026-02-06T10:15:00.000Z");
    });

    it("aligns 60m window to hour boundary", () => {
      const ts = new Date("2026-02-06T10:45:00.000Z");
      const bucket = getBucketStart(ts, "60m");
      expect(bucket).toBe("2026-02-06T10:00:00.000Z");
    });

    it("aligns 60m window at exact boundary", () => {
      const ts = new Date("2026-02-06T11:00:00.000Z");
      const bucket = getBucketStart(ts, "60m");
      expect(bucket).toBe("2026-02-06T11:00:00.000Z");
    });

    it("handles midnight rollover for 15m", () => {
      const ts = new Date("2026-02-06T23:59:59.999Z");
      const bucket = getBucketStart(ts, "15m");
      expect(bucket).toBe("2026-02-06T23:45:00.000Z");
    });

    it("handles midnight rollover for 60m", () => {
      const ts = new Date("2026-02-06T23:59:59.999Z");
      const bucket = getBucketStart(ts, "60m");
      expect(bucket).toBe("2026-02-06T23:00:00.000Z");
    });
  });

  describe("getCounterKey", () => {
    it("generates deterministic counter key", () => {
      const key = getCounterKey("15m", "aws.bedrock", "2026-02-06T10:00:00.000Z");
      expect(key).toBe("window:15m:aws.bedrock:2026-02-06T10:00:00.000Z");
    });

    it("includes all components", () => {
      const key = getCounterKey("60m", "ai.openai", "2026-02-06T11:00:00.000Z");
      expect(key).toContain("60m");
      expect(key).toContain("ai.openai");
      expect(key).toContain("2026-02-06T11:00:00.000Z");
    });
  });

  describe("getPreviousCounterKey", () => {
    it("generates previous counter key", () => {
      const key = getPreviousCounterKey("15m", "aws.bedrock");
      expect(key).toBe("prev:15m:aws.bedrock");
    });
  });

  describe("getEvidenceKey", () => {
    it("generates evidence key", () => {
      const key = getEvidenceKey("60m", "ai.openai");
      expect(key).toBe("evidence:60m:ai.openai");
    });
  });

  describe("getDedupKey", () => {
    it("generates dedup key", () => {
      const key = getDedupKey("60m", "2026-02-06T10:00:00.000Z");
      expect(key).toBe("dedup:60m:2026-02-06T10:00:00.000Z");
    });
  });

  describe("applyEventToWindows", () => {
    it("returns duplicate when event already exists in dedup set", async () => {
      const redis = {
        eval: vi.fn().mockResolvedValue(0),
      };

      const result = await applyEventToWindows(
        redis as any,
        {
          eventId: "rss:1",
          source: "rss",
          fetchedAt: new Date("2026-02-06T10:07:30.123Z"),
          publishedAt: null,
          url: null,
          title: null,
          text: "hello",
          tags: ["aws.bedrock"],
          engagementScore: 7,
        },
        ["aws.bedrock"],
        ["15m", "60m"],
        10
      );

      expect(result.duplicate).toBe(true);
      expect(redis.eval).toHaveBeenCalledWith(
        expect.any(String),
        1,
        "dedup:60m:2026-02-06T10:00:00.000Z",
        "rss:1",
        10800,
        10,
        7,
        2,
        "window:15m:aws.bedrock:2026-02-06T10:00:00.000Z",
        2700,
        "evidence:15m:aws.bedrock",
        1800,
        "window:60m:aws.bedrock:2026-02-06T10:00:00.000Z",
        10800,
        "evidence:60m:aws.bedrock",
        7200
      );
    });

    it("updates counters and evidence for non-duplicate events", async () => {
      const redis = {
        eval: vi.fn().mockResolvedValue(1),
      };

      const result = await applyEventToWindows(
        redis as any,
        {
          eventId: "rss:2",
          source: "rss",
          fetchedAt: new Date("2026-02-06T10:07:30.123Z"),
          publishedAt: null,
          url: null,
          title: null,
          text: "hello",
          tags: ["aws.bedrock", "ai.openai"],
          engagementScore: 11,
        },
        ["aws.bedrock", "ai.openai"],
        ["15m"],
        2
      );

      expect(result.duplicate).toBe(false);
      expect(redis.eval).toHaveBeenCalledWith(
        expect.any(String),
        1,
        "dedup:15m:2026-02-06T10:00:00.000Z",
        "rss:2",
        2700,
        2,
        11,
        2,
        "window:15m:aws.bedrock:2026-02-06T10:00:00.000Z",
        2700,
        "evidence:15m:aws.bedrock",
        1800,
        "window:15m:ai.openai:2026-02-06T10:00:00.000Z",
        2700,
        "evidence:15m:ai.openai",
        1800
      );
    });
  });

  describe("writePreviousWindowCounts", () => {
    it("writes previous counts with a 2x window TTL", async () => {
      const pipeline = {
        set: vi.fn().mockReturnThis(),
        expire: vi.fn().mockReturnThis(),
        exec: vi.fn().mockResolvedValue([]),
      };
      const redis = {
        pipeline: vi.fn(() => pipeline),
      };

      await writePreviousWindowCounts(
        redis as any,
        "60m",
        new Map([
          ["aws.bedrock", 12],
          ["ai.openai", 5],
        ])
      );

      expect(pipeline.set).toHaveBeenCalledWith("prev:60m:aws.bedrock", "12");
      expect(pipeline.set).toHaveBeenCalledWith("prev:60m:ai.openai", "5");
      expect(pipeline.expire).toHaveBeenCalledWith("prev:60m:aws.bedrock", 7200);
      expect(pipeline.expire).toHaveBeenCalledWith("prev:60m:ai.openai", 7200);
      expect(pipeline.exec).toHaveBeenCalledOnce();
    });
  });
});
