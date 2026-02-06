import { describe, it, expect, vi, afterEach } from "vitest";
import { BackoffManager, isRateLimitError, isTransientError } from "../src/backoff.js";

function createTestLogger() {
  return {
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  } as any;
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("BackoffManager", () => {
  it("uses exponential backoff with caps and increments attempts", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0);

    const logger = createTestLogger();
    const backoff = new BackoffManager(
      "test",
      logger,
      undefined,
      { baseDelayMs: 10, maxDelayMs: 25, jitterFactor: 0 }
    );

    const p1 = backoff.waitTransient();
    await vi.advanceTimersByTimeAsync(10);
    await p1;
    expect(backoff.getAttempts()).toBe(1);
    expect(logger.info).toHaveBeenCalledWith(
      expect.objectContaining({ delayMs: 10, attempt: 0, reason: "transient" }),
      "Backing off"
    );

    const p2 = backoff.waitTransient();
    await vi.advanceTimersByTimeAsync(20);
    await p2;
    expect(backoff.getAttempts()).toBe(2);

    const p3 = backoff.waitTransient();
    await vi.advanceTimersByTimeAsync(25);
    await p3;
    expect(backoff.getAttempts()).toBe(3);

    backoff.reset();
    expect(backoff.getAttempts()).toBe(0);
  });
});

describe("Backoff error classification", () => {
  it("detects rate limit errors", () => {
    expect(isRateLimitError(new Error("HTTP 429"))).toBe(true);
    expect(isRateLimitError({ status: 429 })).toBe(true);

    const nested = new Error("outer", { cause: new Error("rate limit") });
    expect(isRateLimitError(nested)).toBe(true);
  });

  it("detects transient errors", () => {
    expect(isTransientError(new Error("ETIMEDOUT"))).toBe(true);
    expect(isTransientError({ status: 503 })).toBe(true);

    const nested = new Error("outer", { cause: new Error("socket hang up") });
    expect(isTransientError(nested)).toBe(true);
  });
});

