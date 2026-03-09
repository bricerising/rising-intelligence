import { describe, expect, it, vi } from "vitest";
import type { ProducerConnection } from "@rising-intelligence/pipeline/transport";
import {
  TOPICS,
  createCollectorHeartbeatPublisher,
  createCollectorIngestionPublisher,
  createCollectorPublisher,
} from "../src/publishing-facade.js";
import type { CollectorHeartbeat, DeadLetterEvent, RawEvent } from "../src/types.js";

function createTestLogger() {
  return {
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  } as any;
}

function createMockConnection(): ProducerConnection {
  return {
    publish: vi.fn(async () => undefined),
    publishBatch: vi.fn(async () => false),
    disconnect: vi.fn(async () => undefined),
  };
}

describe("collector publishing facade", () => {
  it("publishes raw events with canonical topic and serialized payload", async () => {
    const logger = createTestLogger();
    const connection = createMockConnection();
    const publisher = createCollectorPublisher({ connection, logger });

    const event: RawEvent = {
      event_id: "evt-1",
      source: "rss",
      fetched_at: "2026-02-10T00:00:00.000Z",
      text: "AWS launch update",
      title: "New AWS feature",
    };

    await publisher.publishRawEvent(event);

    expect(connection.publish).toHaveBeenCalledTimes(1);
    const [topic, key, value] = (connection.publish as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(topic).toBe(TOPICS.RAW_EVENTS);
    expect(key).toBe("evt-1");

    const decoded = JSON.parse((value as Buffer).toString("utf-8"));
    expect(decoded).toMatchObject({
      event_id: "evt-1",
      source: 1,
      title: "New AWS feature",
      text: "AWS launch update",
    });
  });

  it("publishes dead letter events to DLQ topic keyed by dlq_id", async () => {
    const logger = createTestLogger();
    const connection = createMockConnection();
    const publisher = createCollectorPublisher({ connection, logger });

    const dlqEvent: DeadLetterEvent = {
      dlq_id: "dlq:1",
      occurred_at: "2026-02-10T00:00:00.000Z",
      source: "rss",
      error_code: "VALIDATION_FAILED",
      error_message: "missing field",
    };

    await publisher.publishDeadLetterEvent(dlqEvent);

    expect(connection.publish).toHaveBeenCalledWith(
      TOPICS.DLQ,
      "dlq:1",
      expect.any(Buffer)
    );
  });

  it("publishes collector heartbeats to heartbeat topic keyed by source", async () => {
    const logger = createTestLogger();
    const connection = createMockConnection();
    const publisher = createCollectorPublisher({ connection, logger });

    const heartbeat: CollectorHeartbeat = {
      source: "rss",
      timestamp: "2026-02-10T00:00:00.000Z",
      last_fetch_at: "2026-02-10T00:00:00.000Z",
      items_fetched: 2,
      status: "healthy",
    };

    await publisher.publishHeartbeat(heartbeat);

    expect(connection.publish).toHaveBeenCalledWith(
      TOPICS.HEARTBEAT,
      "rss",
      expect.any(Buffer)
    );

    const value = (connection.publish as ReturnType<typeof vi.fn>).mock.calls[0][2] as Buffer;
    const decoded = JSON.parse(value.toString("utf-8"));
    expect(decoded).toMatchObject({
      source: 1,
      status: 1,
      items_fetched: 2,
    });
  });

  it("narrows to an ingestion publisher contract", async () => {
    const logger = createTestLogger();
    const connection = createMockConnection();
    const publisher = createCollectorPublisher({ connection, logger });
    const ingestionPublisher = createCollectorIngestionPublisher(publisher);

    await ingestionPublisher.publishAcceptedEvent({
      event_id: "evt-2",
      source: "rss",
      fetched_at: "2026-02-10T00:00:00.000Z",
      text: "Ingested payload",
    });
    await ingestionPublisher.publishRejectedEvent({
      dlq_id: "dlq:2",
      occurred_at: "2026-02-10T00:00:00.000Z",
      source: "rss",
      error_code: "VALIDATION_FAILED",
      error_message: "invalid",
    });

    expect(connection.publish).toHaveBeenNthCalledWith(
      1,
      TOPICS.RAW_EVENTS,
      "evt-2",
      expect.any(Buffer)
    );
    expect(connection.publish).toHaveBeenNthCalledWith(
      2,
      TOPICS.DLQ,
      "dlq:2",
      expect.any(Buffer)
    );
  });

  it("narrows to a heartbeat publisher contract", async () => {
    const logger = createTestLogger();
    const connection = createMockConnection();
    const publisher = createCollectorPublisher({ connection, logger });
    const heartbeatPublisher = createCollectorHeartbeatPublisher(publisher);

    await heartbeatPublisher.publishSourceHeartbeat({
      source: "rss",
      timestamp: "2026-02-10T00:00:00.000Z",
      last_fetch_at: "2026-02-10T00:00:00.000Z",
      items_fetched: 1,
      status: "healthy",
    });

    expect(connection.publish).toHaveBeenCalledWith(
      TOPICS.HEARTBEAT,
      "rss",
      expect.any(Buffer)
    );
  });
});
