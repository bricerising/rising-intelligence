import { describe, expect, it, vi } from "vitest";
import type { CheckpointStore } from "../src/checkpoint.js";
import {
  createCollectorIngestion,
  type CollectorIngestion,
  type CollectorIngestionResult,
} from "../src/collector-ingestion.js";
import { createHealthContext } from "../src/health.js";
import type { CompiledAllowlist } from "@rising-intelligence/pipeline";
import type {
  CollectorIngestionEvent,
  DeadLetterEvent,
  Source,
} from "../src/types.js";

function createLogger() {
  return {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    child: vi.fn(),
  } as any;
}

function createAllowlist(): CompiledAllowlist {
  return {
    topics: [
      {
        key: "aws",
        displayName: "AWS",
        priority: 100,
        matchers: [{ type: "keyword", value: "aws" }],
      },
    ],
    maxTopicsPerEvent: 5,
    defaultPriority: 50,
    mutedTopics: new Set<string>(),
  };
}

function createEvent(
  overrides: Partial<CollectorIngestionEvent> = {}
): CollectorIngestionEvent {
  return {
    eventId: "evt-1",
    source: "rss",
    fetchedAt: "2026-02-10T00:00:00.000Z",
    title: "AWS update",
    text: "aws launched a new feature",
    sourceMeta: {
      feed_name: "AWS Blog",
      feed_url: "https://aws.amazon.com/blogs/aws/feed/",
    },
    ...overrides,
  };
}

function createCheckpointStore(
  hasSeen = false
): Pick<CheckpointStore, "hasSeen" | "markSeen"> {
  return {
    hasSeen: vi.fn(() => hasSeen),
    markSeen: vi.fn(),
  };
}

interface TestHarness {
  ingestion: CollectorIngestion;
  healthContext: ReturnType<typeof createHealthContext>;
  checkpointStore: Pick<CheckpointStore, "hasSeen" | "markSeen">;
  publishAcceptedEvent: ReturnType<typeof vi.fn>;
  publishRejectedEvent: ReturnType<typeof vi.fn>;
}

interface CreateHarnessOptions {
  hasSeen?: boolean;
  adapterName?: string;
  adapterSource?: Source;
}

function createHarness(options: CreateHarnessOptions = {}): TestHarness {
  const hasSeen = options.hasSeen ?? false;
  const adapterName = options.adapterName ?? "rss";
  const adapterSource = options.adapterSource ?? "rss";

  const checkpointStore = createCheckpointStore(hasSeen);
  const healthContext = createHealthContext();
  const publishAcceptedEvent = vi.fn(
    async (_event: CollectorIngestionEvent) => undefined
  );
  const publishRejectedEvent = vi.fn(async (_event: DeadLetterEvent) => undefined);

  const fixedNow = new Date("2026-02-10T12:00:00.000Z");
  const ingestion = createCollectorIngestion({
    adapterName,
    adapterSource,
    allowlist: createAllowlist(),
    checkpointStore,
    healthContext,
    logger: createLogger(),
    publisher: {
      publishAcceptedEvent,
      publishRejectedEvent,
    },
    now: () => fixedNow,
    generateDlqId: () => "dlq:test",
  });

  return {
    ingestion,
    healthContext,
    checkpointStore,
    publishAcceptedEvent,
    publishRejectedEvent,
  };
}

describe("collector ingestion pipeline", () => {
  it("ingests valid events through the full handler chain", async () => {
    const {
      ingestion,
      healthContext,
      checkpointStore,
      publishAcceptedEvent,
      publishRejectedEvent,
    } = createHarness();

    const event = createEvent();
    const result = await ingestion.ingest(event);

    expect(result).toEqual<CollectorIngestionResult>({
      status: "ingested",
      topics: ["aws"],
    });
    expect(publishAcceptedEvent).toHaveBeenCalledWith({
      ...event,
      tags: ["aws"],
    });
    expect(publishRejectedEvent).not.toHaveBeenCalled();
    expect(checkpointStore.markSeen).toHaveBeenCalledWith("rss", "evt-1");
    expect(event.tags).toBeUndefined();
    expect(healthContext.metrics.eventsIngested.get("rss")).toBe(1);
    expect(healthContext.metrics.topicsExtracted.get("aws")).toBe(1);
    expect(healthContext.lastEventAt?.toISOString()).toBe(
      "2026-02-10T12:00:00.000Z"
    );
  });

  it("preserves existing market tags while adding canonical topics", async () => {
    const {
      ingestion,
      publishAcceptedEvent,
      publishRejectedEvent,
    } = createHarness();

    const event = createEvent({
      tags: ["market.pos"],
    });
    const result = await ingestion.ingest(event);

    expect(result).toEqual<CollectorIngestionResult>({
      status: "ingested",
      topics: ["aws"],
    });
    expect(event.tags).toEqual(["market.pos"]);
    expect(publishAcceptedEvent).toHaveBeenCalledWith({
      ...event,
      tags: ["market.pos", "aws"],
    });
    expect(publishRejectedEvent).not.toHaveBeenCalled();
  });

  it("normalizes and deduplicates existing tags before publishing", async () => {
    const {
      ingestion,
      publishAcceptedEvent,
      publishRejectedEvent,
    } = createHarness();

    const event = createEvent({
      tags: [" market.pos ", "aws", "market.pos", ""],
    });
    const result = await ingestion.ingest(event);

    expect(result).toEqual<CollectorIngestionResult>({
      status: "ingested",
      topics: ["aws"],
    });
    expect(event.tags).toEqual([" market.pos ", "aws", "market.pos", ""]);
    expect(publishAcceptedEvent).toHaveBeenCalledWith({
      ...event,
      tags: ["market.pos", "aws"],
    });
    expect(publishRejectedEvent).not.toHaveBeenCalled();
  });

  it("short-circuits duplicates before validation and publishing", async () => {
    const {
      ingestion,
      healthContext,
      checkpointStore,
      publishAcceptedEvent,
      publishRejectedEvent,
    } = createHarness({ hasSeen: true });

    const result = await ingestion.ingest(createEvent());

    expect(result).toEqual<CollectorIngestionResult>({ status: "duplicate" });
    expect(checkpointStore.hasSeen).toHaveBeenCalledWith("rss", "evt-1");
    expect(checkpointStore.markSeen).not.toHaveBeenCalled();
    expect(publishAcceptedEvent).not.toHaveBeenCalled();
    expect(publishRejectedEvent).not.toHaveBeenCalled();
    expect(healthContext.metrics.eventsIngested.size).toBe(0);
    expect(healthContext.metrics.eventsFailed.size).toBe(0);
    expect(healthContext.metrics.topicsExtracted.size).toBe(0);
    expect(healthContext.lastEventAt).toBeUndefined();
  });

  it("routes invalid events to DLQ before topic extraction", async () => {
    const {
      ingestion,
      healthContext,
      checkpointStore,
      publishAcceptedEvent,
      publishRejectedEvent,
    } = createHarness();

    const invalidEvent = createEvent({
      text: "",
      title: "AWS post with missing body",
      url: "https://example.com/broken",
    });

    const result = await ingestion.ingest(invalidEvent);

    expect(result).toEqual<CollectorIngestionResult>({
      status: "invalid",
      errorCode: "VALIDATION_FAILED",
    });
    expect(publishAcceptedEvent).not.toHaveBeenCalled();
    expect(checkpointStore.markSeen).not.toHaveBeenCalled();
    expect(publishRejectedEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        dlq_id: "dlq:test",
        source: "rss",
        error_code: "VALIDATION_FAILED",
        raw_reference: "https://example.com/broken",
      })
    );
    expect(healthContext.metrics.eventsFailed.get("rss")?.get("parse_error")).toBe(1);
    expect([...healthContext.metrics.rssFeedErrors.values()]).toEqual([
      expect.objectContaining({
        feed: "AWS Blog",
        feedUrl: "https://aws.amazon.com/blogs/aws/feed/",
        errorType: "parse_error",
        count: 1,
      }),
    ]);
    expect(healthContext.metrics.topicsExtracted.size).toBe(0);
    expect(invalidEvent.tags).toBeUndefined();
    expect(healthContext.lastEventAt).toBeUndefined();
  });

  it("applies source-specific validation strategy for non-rss adapters", async () => {
    const {
      ingestion,
      healthContext,
      checkpointStore,
      publishAcceptedEvent,
      publishRejectedEvent,
    } = createHarness({
      adapterName: "hackernews",
      adapterSource: "hackernews",
    });

    const invalidEvent = createEvent({
      source: "hackernews",
      text: "",
      url: "https://news.ycombinator.com/item?id=1",
      sourceMeta: {
        feed_name: "Should not be counted for non-rss",
        feed_url: "https://example.com/ignored-feed",
      },
    });
    const result = await ingestion.ingest(invalidEvent);

    expect(result).toEqual<CollectorIngestionResult>({
      status: "invalid",
      errorCode: "VALIDATION_FAILED",
    });
    expect(publishAcceptedEvent).not.toHaveBeenCalled();
    expect(checkpointStore.markSeen).not.toHaveBeenCalled();
    expect(publishRejectedEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        source: "hackernews",
        error_code: "VALIDATION_FAILED",
      })
    );
    expect(healthContext.metrics.eventsFailed.get("hackernews")?.get("parse_error")).toBe(1);
    expect([...healthContext.metrics.rssFeedErrors.values()]).toEqual([]);
    expect(healthContext.lastEventAt).toBeUndefined();
  });

  it.each([
    {
      label: "eventId",
      event: createEvent({
        eventId: "   ",
        url: "https://example.com/missing-id",
      }),
    },
    {
      label: "text",
      event: createEvent({
        text: "   ",
        url: "https://example.com/missing-text",
      }),
    },
  ])("treats whitespace-only $label as invalid", async ({ event }) => {
    const {
      ingestion,
      healthContext,
      checkpointStore,
      publishAcceptedEvent,
      publishRejectedEvent,
    } = createHarness();

    const result = await ingestion.ingest(event);

    expect(result).toEqual<CollectorIngestionResult>({
      status: "invalid",
      errorCode: "VALIDATION_FAILED",
    });
    expect(publishAcceptedEvent).not.toHaveBeenCalled();
    expect(checkpointStore.markSeen).not.toHaveBeenCalled();
    expect(publishRejectedEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        dlq_id: "dlq:test",
        source: "rss",
        error_code: "VALIDATION_FAILED",
        raw_reference: event.url,
      })
    );
    expect(healthContext.metrics.eventsFailed.get("rss")?.get("parse_error")).toBe(1);
    expect([...healthContext.metrics.rssFeedErrors.values()]).toEqual([
      expect.objectContaining({
        feed: "AWS Blog",
        feedUrl: "https://aws.amazon.com/blogs/aws/feed/",
        errorType: "parse_error",
        count: 1,
      }),
    ]);
    expect(healthContext.lastEventAt).toBeUndefined();
  });
});
