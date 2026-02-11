import { describe, expect, it, vi } from "vitest";
import type { Producer } from "kafkajs";
import { TOPICS } from "../src/kafka/producer.js";
import { createCollectorPublisher } from "../src/publishing-facade.js";
import type { CollectorHeartbeat, DeadLetterEvent, RawEvent } from "../src/types.js";

function createTestLogger() {
  return {
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  } as any;
}

describe("collector publishing facade", () => {
  it("publishes raw events with canonical topic and serialized payload", async () => {
    const publish = vi.fn(async () => undefined);
    const logger = createTestLogger();
    const producer = {} as Producer;
    const publisher = createCollectorPublisher({
      producer,
      logger,
      publish,
    });

    const event: RawEvent = {
      event_id: "evt-1",
      source: "rss",
      fetched_at: "2026-02-10T00:00:00.000Z",
      text: "AWS launch update",
      title: "New AWS feature",
    };

    await publisher.publishRawEvent(event);

    expect(publish).toHaveBeenCalledTimes(1);
    const [sentProducer, topic, key, payload, sentLogger] = publish.mock.calls[0];
    expect(sentProducer).toBe(producer);
    expect(topic).toBe(TOPICS.RAW_EVENTS);
    expect(key).toBe("evt-1");
    expect(sentLogger).toBe(logger);

    const decoded = JSON.parse((payload as Buffer).toString("utf-8"));
    expect(decoded).toMatchObject({
      event_id: "evt-1",
      source: 1,
      title: "New AWS feature",
      text: "AWS launch update",
    });
  });

  it("publishes dead letter events to DLQ topic keyed by dlq_id", async () => {
    const publish = vi.fn(async () => undefined);
    const logger = createTestLogger();
    const producer = {} as Producer;
    const publisher = createCollectorPublisher({
      producer,
      logger,
      publish,
    });

    const dlqEvent: DeadLetterEvent = {
      dlq_id: "dlq:1",
      occurred_at: "2026-02-10T00:00:00.000Z",
      source: "rss",
      error_code: "VALIDATION_FAILED",
      error_message: "missing field",
    };

    await publisher.publishDeadLetterEvent(dlqEvent);

    expect(publish).toHaveBeenCalledWith(
      producer,
      TOPICS.DLQ,
      "dlq:1",
      expect.any(Buffer),
      logger
    );
  });

  it("publishes collector heartbeats to heartbeat topic keyed by source", async () => {
    const publish = vi.fn(async () => undefined);
    const logger = createTestLogger();
    const producer = {} as Producer;
    const publisher = createCollectorPublisher({
      producer,
      logger,
      publish,
    });

    const heartbeat: CollectorHeartbeat = {
      source: "rss",
      timestamp: "2026-02-10T00:00:00.000Z",
      last_fetch_at: "2026-02-10T00:00:00.000Z",
      items_fetched: 2,
      status: "healthy",
    };

    await publisher.publishHeartbeat(heartbeat);

    expect(publish).toHaveBeenCalledWith(
      producer,
      TOPICS.HEARTBEAT,
      "rss",
      expect.any(Buffer),
      logger
    );

    const payload = publish.mock.calls[0][3] as Buffer;
    const decoded = JSON.parse(payload.toString("utf-8"));
    expect(decoded).toMatchObject({
      source: 1,
      status: 1,
      items_fetched: 2,
    });
  });
});
