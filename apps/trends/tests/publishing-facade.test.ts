import { describe, expect, it, vi } from "vitest";
import type { ProducerConnection } from "@rising-intelligence/pipeline/transport";
import { createTrendsSnapshotPublisher } from "../src/publishing-facade.js";

describe("trends publishing facade", () => {
  it("serializes snapshots and publishes to configured topic", async () => {
    const connection: ProducerConnection = {
      publish: vi.fn().mockResolvedValue(undefined),
      publishBatch: vi.fn(async () => false),
      disconnect: vi.fn(async () => undefined),
    };
    const logger = {} as any;

    const publisher = createTrendsSnapshotPublisher({
      connection,
      logger,
      topic: "trends.snapshots",
    });

    const payload = {
      generated_at: "2026-02-11T00:00:00.000Z",
      window: 1,
      topics: [{ topic: "aws.bedrock", score: 12.5 }],
    };

    await publisher.publishSnapshot("15m:2026-02-11T00:00:00.000Z", payload);

    expect(connection.publish).toHaveBeenCalledOnce();
    const [topic, key, value] = (connection.publish as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(topic).toBe("trends.snapshots");
    expect(key).toBe("15m:2026-02-11T00:00:00.000Z");
    expect(JSON.parse((value as Buffer).toString("utf-8"))).toEqual(payload);
  });
});
