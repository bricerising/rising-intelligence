import { describe, expect, it } from "vitest";
import { PostgresCircuitBreaker } from "../src/circuit-breaker.js";

describe("PostgresCircuitBreaker", () => {
  it("starts closed", () => {
    const cb = new PostgresCircuitBreaker(3, 5000);

    expect(cb.isOpen()).toBe(false);
    expect(cb.timeUntilClose()).toBe(0);
  });

  it("stays closed below failure threshold", () => {
    const cb = new PostgresCircuitBreaker(3, 5000);
    const now = 1000;

    cb.recordFailure(now);
    cb.recordFailure(now);

    expect(cb.isOpen(now)).toBe(false);
  });

  it("opens after reaching failure threshold", () => {
    const cb = new PostgresCircuitBreaker(3, 5000);
    const now = 1000;

    cb.recordFailure(now);
    cb.recordFailure(now);
    const opened = cb.recordFailure(now);

    expect(opened).toBe(true);
    expect(cb.isOpen(now)).toBe(true);
  });

  it("returns true from recordFailure only when circuit transitions to open", () => {
    const cb = new PostgresCircuitBreaker(3, 5000);
    const now = 1000;

    expect(cb.recordFailure(now)).toBe(false);
    expect(cb.recordFailure(now)).toBe(false);
    expect(cb.recordFailure(now)).toBe(true); // opens here
  });

  it("reports correct time until close", () => {
    const cb = new PostgresCircuitBreaker(2, 5000);
    const now = 1000;

    cb.recordFailure(now);
    cb.recordFailure(now);

    expect(cb.timeUntilClose(now)).toBe(5000);
    expect(cb.timeUntilClose(now + 2000)).toBe(3000);
    expect(cb.timeUntilClose(now + 5000)).toBe(0);
    expect(cb.timeUntilClose(now + 6000)).toBe(0);
  });

  it("closes after openMs elapses", () => {
    const cb = new PostgresCircuitBreaker(2, 5000);
    const now = 1000;

    cb.recordFailure(now);
    cb.recordFailure(now);

    expect(cb.isOpen(now)).toBe(true);
    expect(cb.isOpen(now + 4999)).toBe(true);
    expect(cb.isOpen(now + 5000)).toBe(false);
    expect(cb.isOpen(now + 6000)).toBe(false);
  });

  it("resets failure count on success", () => {
    const cb = new PostgresCircuitBreaker(3, 5000);
    const now = 1000;

    cb.recordFailure(now);
    cb.recordFailure(now);
    cb.recordSuccess();

    // After reset, needs 3 more failures to open
    expect(cb.recordFailure(now)).toBe(false);
    expect(cb.recordFailure(now)).toBe(false);
    expect(cb.recordFailure(now)).toBe(true);
  });

  it("resets failure counter when circuit opens", () => {
    const cb = new PostgresCircuitBreaker(2, 5000);
    const openTime = 1000;

    cb.recordFailure(openTime);
    cb.recordFailure(openTime); // opens, counter resets

    // After circuit closes, need full threshold again
    const afterClose = openTime + 6000;
    expect(cb.isOpen(afterClose)).toBe(false);
    expect(cb.recordFailure(afterClose)).toBe(false); // 1 of 2
    expect(cb.recordFailure(afterClose)).toBe(true);  // 2 of 2 - opens again
  });

  it("handles threshold of 1", () => {
    const cb = new PostgresCircuitBreaker(1, 1000);

    const opened = cb.recordFailure(0);

    expect(opened).toBe(true);
    expect(cb.isOpen(0)).toBe(true);
    expect(cb.isOpen(1000)).toBe(false);
  });
});
