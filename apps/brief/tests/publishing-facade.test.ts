import { describe, expect, it, vi } from "vitest";
import { createBriefResultPublisher } from "../src/publishing-facade.js";

describe("brief publishing facade", () => {
  it("serializes payload and publishes to configured topic", async () => {
    const producer = {} as any;
    const logger = {} as any;
    const publish = vi.fn().mockResolvedValue(undefined);

    const publisher = createBriefResultPublisher({
      producer,
      logger,
      topic: "summary.results",
      publish,
    });

    const payload = {
      request_id: "req-1",
      produced_at: "2026-02-11T00:00:00.000Z",
      brief: {
        brief_id: "brief:req-1",
      },
    };

    await publisher.publishResult("req-1", payload);

    expect(publish).toHaveBeenCalledOnce();
    expect(publish).toHaveBeenCalledWith(
      producer,
      "summary.results",
      "req-1",
      expect.any(Buffer),
      logger
    );

    const encodedPayload = publish.mock.calls[0][3] as Buffer;
    expect(JSON.parse(encodedPayload.toString("utf-8"))).toEqual(payload);
  });

  it("supports repeated publishes with the same facade instance", async () => {
    const publish = vi.fn().mockResolvedValue(undefined);
    const publisher = createBriefResultPublisher({
      producer: {} as any,
      logger: {} as any,
      topic: "summary.results",
      publish,
    });

    await publisher.publishResult("req-1", { request_id: "req-1", produced_at: "now" });
    await publisher.publishResult("req-2", { request_id: "req-2", produced_at: "later" });

    expect(publish).toHaveBeenCalledTimes(2);
    expect(publish.mock.calls[0][2]).toBe("req-1");
    expect(publish.mock.calls[1][2]).toBe("req-2");
  });
});
