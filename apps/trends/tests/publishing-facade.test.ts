import { describe, expect, it, vi } from "vitest";
import { createTrendsSnapshotPublisher } from "../src/publishing-facade.js";

describe("trends publishing facade", () => {
  it("serializes snapshots and publishes to configured topic", async () => {
    const producer = {} as any;
    const logger = {} as any;
    const publish = vi.fn().mockResolvedValue(undefined);

    const publisher = createTrendsSnapshotPublisher({
      producer,
      logger,
      topic: "trends.snapshots",
      publish,
    });

    const payload = {
      generated_at: "2026-02-11T00:00:00.000Z",
      window: 1,
      topics: [{ topic: "aws.bedrock", score: 12.5 }],
    };

    await publisher.publishSnapshot("15m:2026-02-11T00:00:00.000Z", payload);

    expect(publish).toHaveBeenCalledOnce();
    expect(publish).toHaveBeenCalledWith(
      producer,
      "trends.snapshots",
      "15m:2026-02-11T00:00:00.000Z",
      expect.any(Buffer),
      logger
    );

    const encodedPayload = publish.mock.calls[0][3] as Buffer;
    expect(JSON.parse(encodedPayload.toString("utf-8"))).toEqual(payload);
  });
});
