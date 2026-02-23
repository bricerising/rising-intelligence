import type { Kafka } from "kafkajs";
import { afterEach, describe, expect, it, vi } from "vitest";
import { setupRequestResultWaiter } from "../src/commands/brief/result-waiter.js";

interface TestResultPayload {
  request_id: string;
  produced_at: string;
}

type EachMessageHandler = (payload: {
  message: { value: Buffer | null };
}) => Promise<void>;

class FakeConsumer {
  private eachMessage: EachMessageHandler | null = null;

  readonly connect = vi.fn(async () => undefined);
  readonly subscribe = vi.fn(async () => undefined);
  readonly stop = vi.fn(async () => undefined);
  readonly disconnect = vi.fn(async () => undefined);

  readonly run = vi.fn(
    async (input: { eachMessage(payload: { message: { value: Buffer | null } }): Promise<void> }) => {
      this.eachMessage = input.eachMessage;
    }
  );

  async emitRaw(rawValue: string | null): Promise<void> {
    if (!this.eachMessage) {
      throw new Error("Consumer has not started");
    }
    await this.eachMessage({
      message: {
        value: rawValue === null ? null : Buffer.from(rawValue, "utf-8"),
      },
    });
  }
}

function createKafkaStub(consumer: unknown): Kafka {
  return {
    consumer: vi.fn(() => consumer),
  } as unknown as Kafka;
}

function parseTestResult(rawValue: string): TestResultPayload | null {
  return JSON.parse(rawValue) as TestResultPayload;
}

describe("setupRequestResultWaiter", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("resolves the first matching request result and ignores non-matching payloads", async () => {
    const fakeConsumer = new FakeConsumer();
    const waiter = await setupRequestResultWaiter<TestResultPayload>({
      kafka: createKafkaStub(fakeConsumer),
      groupId: "test-group",
      topic: "summary.results",
      requestId: "target-request",
      timeoutSeconds: 5,
      timeoutErrorMessage: "timeout",
      parseResult: parseTestResult,
    });

    const waitPromise = waiter.waitForResult();

    await fakeConsumer.emitRaw(JSON.stringify({ request_id: "other-request", produced_at: "now" }));
    await fakeConsumer.emitRaw("not-json");
    await fakeConsumer.emitRaw(JSON.stringify({ request_id: "target-request", produced_at: "now" }));

    await expect(waitPromise).resolves.toEqual({
      request_id: "target-request",
      produced_at: "now",
    });
    expect(fakeConsumer.stop).toHaveBeenCalledTimes(1);

    await waiter.disconnect();
    expect(fakeConsumer.disconnect).toHaveBeenCalledTimes(1);
  });

  it("rejects on timeout and stops the consumer", async () => {
    vi.useFakeTimers();

    const fakeConsumer = new FakeConsumer();
    const waiter = await setupRequestResultWaiter<TestResultPayload>({
      kafka: createKafkaStub(fakeConsumer),
      groupId: "test-group",
      topic: "summary.results",
      requestId: "target-request",
      timeoutSeconds: 1,
      timeoutErrorMessage: "Timed out waiting for summary result",
      parseResult: parseTestResult,
    });

    const timedOut = expect(waiter.waitForResult()).rejects.toThrow(
      "Timed out waiting for summary result"
    );
    await vi.advanceTimersByTimeAsync(1_000);

    await timedOut;
    expect(fakeConsumer.stop).toHaveBeenCalledTimes(1);
    await waiter.disconnect();
  });

  it("propagates consumer run failures", async () => {
    const runError = new Error("consumer run failed");
    const failingConsumer = {
      connect: vi.fn(async () => undefined),
      subscribe: vi.fn(async () => undefined),
      run: vi.fn(async () => {
        throw runError;
      }),
      stop: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
    };

    const waiter = await setupRequestResultWaiter<TestResultPayload>({
      kafka: createKafkaStub(failingConsumer),
      groupId: "test-group",
      topic: "summary.results",
      requestId: "target-request",
      timeoutSeconds: 10,
      timeoutErrorMessage: "timeout",
      parseResult: parseTestResult,
    });

    await expect(waiter.waitForResult()).rejects.toThrow("consumer run failed");
  });
});
