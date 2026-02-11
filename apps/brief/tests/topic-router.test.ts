import { describe, expect, it, vi } from "vitest";

async function loadTopicBatchRouter() {
  const actual = await vi.importActual<typeof import("@rising-intelligence/shared")>(
    "@rising-intelligence/shared"
  );
  return actual.createTopicBatchRouter;
}

function createBatchPayload(topic: string, offsets: string[] = ["1", "2"]) {
  return {
    batch: {
      topic,
      partition: 3,
      messages: offsets.map((offset) => ({
        offset,
      })),
    },
    resolveOffset: vi.fn(),
    commitOffsetsIfNecessary: vi.fn().mockResolvedValue(undefined),
    heartbeat: vi.fn().mockResolvedValue(undefined),
  } as any;
}

describe("shared topic batch router", () => {
  it("routes known topics to the registered handler", async () => {
    const createTopicBatchRouter = await loadTopicBatchRouter();
    const logger = { warn: vi.fn() } as any;
    const handler = vi.fn().mockResolvedValue(undefined);
    const payload = createBatchPayload("summary.requests");

    const router = createTopicBatchRouter({
      logger,
      handlers: new Map([["summary.requests", handler]]),
    });

    await router.handle(payload);

    expect(handler).toHaveBeenCalledOnce();
    expect(handler).toHaveBeenCalledWith(payload);
    expect(logger.warn).not.toHaveBeenCalled();
    expect(payload.resolveOffset).not.toHaveBeenCalled();
    expect(payload.commitOffsetsIfNecessary).not.toHaveBeenCalled();
    expect(payload.heartbeat).not.toHaveBeenCalled();
  });

  it("skips unknown topics by resolving offsets, committing, and heartbeating", async () => {
    const createTopicBatchRouter = await loadTopicBatchRouter();
    const logger = { warn: vi.fn() } as any;
    const payload = createBatchPayload("unknown.topic", ["9", "10"]);

    const router = createTopicBatchRouter({
      logger,
      handlers: new Map(),
    });

    await router.handle(payload);

    expect(logger.warn).toHaveBeenCalledOnce();
    expect(logger.warn).toHaveBeenCalledWith(
      { topic: "unknown.topic", partition: 3 },
      "Received batch for unexpected topic; skipping"
    );
    expect(payload.resolveOffset).toHaveBeenCalledTimes(2);
    expect(payload.resolveOffset).toHaveBeenNthCalledWith(1, "9");
    expect(payload.resolveOffset).toHaveBeenNthCalledWith(2, "10");
    expect(payload.commitOffsetsIfNecessary).toHaveBeenCalledOnce();
    expect(payload.heartbeat).toHaveBeenCalledOnce();
  });
});
