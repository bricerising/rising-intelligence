import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@rising-intelligence/pipeline/transport", async (importOriginal) => {
  const original = await importOriginal<typeof import("@rising-intelligence/pipeline/transport")>();
  return {
    ...original,
    createConsumerConnection: vi.fn(),
  };
});

import type {
  BatchContext,
  BatchStrategy,
  ConsumerConnection,
  PipelineMessage,
} from "@rising-intelligence/pipeline/transport";
import { createConsumerConnection } from "@rising-intelligence/pipeline/transport";
import { setupRequestResultWaiter } from "../src/commands/brief/result-waiter.js";

interface TestResultPayload {
  request_id: string;
  produced_at: string;
}

function parseTestResult(rawValue: string): TestResultPayload | null {
  return JSON.parse(rawValue) as TestResultPayload;
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeoutHandle = setTimeout(() => {
      reject(new Error(`Promise did not settle within ${timeoutMs}ms`));
    }, timeoutMs);

    promise.then(
      (value) => {
        clearTimeout(timeoutHandle);
        resolve(value);
      },
      (error) => {
        clearTimeout(timeoutHandle);
        reject(error);
      }
    );
  });
}

function createFakeBatchContext(): BatchContext {
  return {
    topic: "summary.results",
    partition: 0,
    highWatermark: "0",
    isActive: () => true,
    keepAlive: vi.fn(async () => undefined),
    acknowledge: vi.fn(),
    commit: vi.fn(async () => undefined),
    pause: () => () => undefined,
  };
}

function createPipelineMessage(rawValue: string | null): PipelineMessage {
  return {
    key: null,
    value: rawValue === null ? null : Buffer.from(rawValue, "utf-8"),
    position: "0",
    timestamp: Date.now().toString(),
  };
}

/**
 * Fake ConsumerConnection that captures the BatchStrategy from consume()
 * so tests can feed messages through it via emitMessage().
 */
class FakeConsumerConnection {
  private strategy: BatchStrategy<any> | null = null;
  private ctx: any = null;

  readonly disconnect = vi.fn(async () => undefined);

  readonly consume = vi.fn(async (options: any) => {
    this.strategy = options.strategy;
    this.ctx = options.ctx;
    // Return a never-resolving promise to simulate a running consumer
    return new Promise<void>(() => {});
  });

  async emitMessage(rawValue: string | null): Promise<void> {
    if (!this.strategy) {
      throw new Error("Consumer has not started");
    }
    await this.strategy.processBatch(
      this.ctx,
      createFakeBatchContext(),
      [createPipelineMessage(rawValue)]
    );
  }
}

/**
 * Variant that simulates disconnect() blocking until the message handler returns,
 * verifying the waiter does not deadlock in that scenario.
 */
class StopWithinHandlerConnection {
  private strategy: BatchStrategy<any> | null = null;
  private ctx: any = null;
  private handlingMessage = false;
  private releaseDisconnect: (() => void) | null = null;

  readonly disconnect = vi.fn(async () => {
    if (!this.handlingMessage) {
      return;
    }
    await new Promise<void>((resolve) => {
      this.releaseDisconnect = resolve;
    });
  });

  readonly consume = vi.fn(async (options: any) => {
    this.strategy = options.strategy;
    this.ctx = options.ctx;
    return new Promise<void>(() => {});
  });

  async emitMessage(rawValue: string | null): Promise<void> {
    if (!this.strategy) {
      throw new Error("Consumer has not started");
    }

    this.handlingMessage = true;
    try {
      await this.strategy.processBatch(
        this.ctx,
        createFakeBatchContext(),
        [createPipelineMessage(rawValue)]
      );
    } finally {
      this.handlingMessage = false;
      this.releaseDisconnect?.();
      this.releaseDisconnect = null;
    }
  }
}

const mockedCreateConsumerConnection = vi.mocked(createConsumerConnection);

describe("setupRequestResultWaiter", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it("resolves the first matching request result and ignores non-matching payloads", async () => {
    const fakeConnection = new FakeConsumerConnection();
    mockedCreateConsumerConnection.mockResolvedValue(fakeConnection);

    const waiter = await setupRequestResultWaiter<TestResultPayload>({
      kafkaBrokers: ["localhost:9092"],
      kafkaClientId: "test-client",
      groupId: "test-group",
      topic: "summary.results",
      requestId: "target-request",
      timeoutSeconds: 5,
      timeoutErrorMessage: "timeout",
      parseResult: parseTestResult,
    });

    const waitPromise = waiter.waitForResult();

    await fakeConnection.emitMessage(JSON.stringify({ request_id: "other-request", produced_at: "now" }));
    await fakeConnection.emitMessage("not-json");
    await fakeConnection.emitMessage(JSON.stringify({ request_id: "target-request", produced_at: "now" }));

    await expect(waitPromise).resolves.toEqual({
      request_id: "target-request",
      produced_at: "now",
    });

    await waiter.disconnect();
    expect(fakeConnection.disconnect).toHaveBeenCalledTimes(1);
  });

  it("rejects on timeout and stops the consumer", async () => {
    vi.useFakeTimers();

    const fakeConnection = new FakeConsumerConnection();
    mockedCreateConsumerConnection.mockResolvedValue(fakeConnection);

    const waiter = await setupRequestResultWaiter<TestResultPayload>({
      kafkaBrokers: ["localhost:9092"],
      kafkaClientId: "test-client",
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
    await waiter.disconnect();
    expect(fakeConnection.disconnect).toHaveBeenCalled();
  });

  it("propagates consumer run failures", async () => {
    const failingConnection: ConsumerConnection = {
      consume: vi.fn(async () => {
        throw new Error("consumer run failed");
      }),
      disconnect: vi.fn(async () => undefined),
    };
    mockedCreateConsumerConnection.mockResolvedValue(failingConnection);

    const waiter = await setupRequestResultWaiter<TestResultPayload>({
      kafkaBrokers: ["localhost:9092"],
      kafkaClientId: "test-client",
      groupId: "test-group",
      topic: "summary.results",
      requestId: "target-request",
      timeoutSeconds: 10,
      timeoutErrorMessage: "timeout",
      parseResult: parseTestResult,
    });

    await expect(waiter.waitForResult()).rejects.toThrow("consumer run failed");
  });

  it("does not hang when disconnect() waits for message handler to return", async () => {
    const connection = new StopWithinHandlerConnection();
    mockedCreateConsumerConnection.mockResolvedValue(connection);

    const waiter = await setupRequestResultWaiter<TestResultPayload>({
      kafkaBrokers: ["localhost:9092"],
      kafkaClientId: "test-client",
      groupId: "test-group",
      topic: "summary.results",
      requestId: "target-request",
      timeoutSeconds: 5,
      timeoutErrorMessage: "timeout",
      parseResult: parseTestResult,
    });

    const waitPromise = waiter.waitForResult();
    const emitPromise = connection.emitMessage(
      JSON.stringify({ request_id: "target-request", produced_at: "now" })
    );

    await expect(waitPromise).resolves.toEqual({
      request_id: "target-request",
      produced_at: "now",
    });
    await expect(withTimeout(emitPromise, 200)).resolves.toBeUndefined();

    await waiter.disconnect();
    expect(connection.disconnect).toHaveBeenCalledTimes(1);
  });
});
