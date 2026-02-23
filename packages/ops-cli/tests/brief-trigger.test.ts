import { afterEach, describe, expect, it, vi } from "vitest";
import {
  briefTrigger,
  evaluateConsumerLagFreshness,
} from "../src/commands/brief/trigger.js";

function findJsonPayload(logCalls: Array<unknown[]>): Record<string, unknown> {
  for (const call of logCalls) {
    const [firstArg] = call;
    if (typeof firstArg !== "string") {
      continue;
    }
    if (!firstArg.trim().startsWith("{")) {
      continue;
    }
    return JSON.parse(firstArg) as Record<string, unknown>;
  }

  throw new Error("Unable to find JSON payload in console output");
}

describe("brief trigger mode handling", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete process.env.DATABASE_URL;
  });

  it("forces query mode to TREND_WINDOW_60M", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});

    await briefTrigger({
      "dry-run": true,
      "request-id": "query-window-test",
      windows: "1,2,3",
    });

    const payload = findJsonPayload(logSpy.mock.calls);
    const body = payload.payload as Record<string, unknown>;

    expect(payload.mode).toBe("query");
    expect(body.windows).toEqual([2]);
    expect(body.query).toMatchObject({
      lookback_days: 7,
      topic_globs: ["*"],
      max_events_per_topic: 3,
      evidence_strategy: "diversity",
    });
  });

  it("keeps dry-run mode side-effect free from freshness checks", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    await briefTrigger({
      "dry-run": true,
      "request-id": "query-dry-run-side-effect-free",
    });

    expect(findJsonPayload(logSpy.mock.calls).mode).toBe("query");
    expect(warnSpy).not.toHaveBeenCalledWith("⚠️  Data freshness warnings:");
  });

  it("rejects query mode when window 2 is not requested", async () => {
    await expect(
      briefTrigger({
        "dry-run": true,
        windows: "1",
      })
    ).rejects.toThrow(/TREND_WINDOW_60M/);
  });

  it("builds explicit mode payload without query fields", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});

    await briefTrigger({
      "dry-run": true,
      "request-id": "explicit-mode-test",
      windows: "1,3",
      "topic-key": "ai.openai",
      "evidence-url": "https://example.com/article",
      score: "9.1",
      volume: "120",
      acceleration: "0.6",
      "evidence-title": "Manual trigger",
      "evidence-excerpt": "Manual excerpt",
    });

    const payload = findJsonPayload(logSpy.mock.calls);
    const body = payload.payload as Record<string, unknown>;
    const topics = body.topics as Array<Record<string, unknown>>;

    expect(payload.mode).toBe("explicit");
    expect(body.query).toBeUndefined();
    expect(body.windows).toEqual([1, 3]);
    expect(topics).toHaveLength(1);

    const metrics = topics[0].metrics as Array<Record<string, unknown>>;
    expect(metrics[0]).toMatchObject({
      topic: "ai.openai",
      window: 1,
      score: 9.1,
      volume: 120,
      acceleration: 0.6,
    });

    const evidence = topics[0].evidence as Array<Record<string, unknown>>;
    expect(evidence[0]).toMatchObject({
      source: "rss",
      url: "https://example.com/article",
      title: "Manual trigger",
      text_excerpt: "Manual excerpt",
    });
  });

  it("does not require query feed-config inputs in explicit mode", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});

    await briefTrigger({
      "dry-run": true,
      "feed-config": "/tmp/missing-feed-config.yaml",
      "topic-key": "cloud.azure",
      "evidence-url": "https://example.com/azure",
    });

    const payload = findJsonPayload(logSpy.mock.calls);
    expect(payload.mode).toBe("explicit");
  });

  it("rejects non-positive timeout values", async () => {
    await expect(
      briefTrigger({
        "dry-run": true,
        timeout: "0",
      })
    ).rejects.toThrow(/--timeout/);
  });
});

describe("brief trigger freshness checks", () => {
  it("reports missing consumer lag rows", () => {
    expect(evaluateConsumerLagFreshness([], 1_700_000_000_000)).toEqual([
      {
        category: "consumer_lag",
        message: "No consumer lag records found for events.raw",
      },
    ]);
  });

  it("reports stale rows, missing groups, and lag over threshold", () => {
    const nowMs = 1_700_000_000_000;
    const issues = evaluateConsumerLagFreshness(
      [
        {
          consumerGroup: "trends-processor",
          lagMessages: 250,
          updatedAt: new Date(nowMs - 301_000),
        },
      ],
      nowMs
    );

    expect(issues).toEqual([
      {
        category: "consumer_lag",
        message: "Consumer lag records are stale (>5 min old) for: trends-processor",
      },
      {
        category: "consumer_lag",
        message: "trends-processor lag is 250 messages (threshold: 100)",
      },
      {
        category: "consumer_lag",
        message: "Missing consumer lag records for persister",
      },
    ]);
  });
});
