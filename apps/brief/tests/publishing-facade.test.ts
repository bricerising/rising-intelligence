import { describe, expect, it, vi } from "vitest";
import type { ProducerConnection } from "@rising-intelligence/pipeline/transport";
import { createBriefResultPublisher } from "../src/publishing-facade.js";

describe("brief publishing facade", () => {
  it("serializes payload and publishes to configured topic", async () => {
    const connection: ProducerConnection = {
      publish: vi.fn().mockResolvedValue(undefined),
      publishBatch: vi.fn(async () => false),
      disconnect: vi.fn(async () => undefined),
    };
    const logger = {} as any;

    const publisher = createBriefResultPublisher({
      connection,
      logger,
      topic: "summary.results",
    });

    const payload = {
      request_id: "req-1",
      produced_at: "2026-02-11T00:00:00.000Z",
      brief: {
        brief_id: "brief:req-1",
      },
    };

    await publisher.publishResult("req-1", payload);

    expect(connection.publish).toHaveBeenCalledOnce();
    const [topic, key, value] = (connection.publish as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(topic).toBe("summary.results");
    expect(key).toBe("req-1");
    expect(JSON.parse((value as Buffer).toString("utf-8"))).toEqual(payload);
  });

  it("supports repeated publishes with the same facade instance", async () => {
    const connection: ProducerConnection = {
      publish: vi.fn().mockResolvedValue(undefined),
      publishBatch: vi.fn(async () => false),
      disconnect: vi.fn(async () => undefined),
    };
    const publisher = createBriefResultPublisher({
      connection,
      logger: {} as any,
      topic: "summary.results",
    });

    await publisher.publishResult("req-1", {
      request_id: "req-1",
      produced_at: "2026-02-11T00:00:00.000Z",
    });
    await publisher.publishResult("req-2", {
      request_id: "req-2",
      produced_at: "2026-02-11T00:05:00.000Z",
    });

    expect(connection.publish).toHaveBeenCalledTimes(2);
    const calls = (connection.publish as ReturnType<typeof vi.fn>).mock.calls;
    expect(calls[0][1]).toBe("req-1");
    expect(calls[1][1]).toBe("req-2");
  });
});
