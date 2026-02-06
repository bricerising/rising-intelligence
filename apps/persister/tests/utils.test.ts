import { describe, expect, it } from "vitest";
import { toBigInt, nowSeconds } from "../src/utils.js";

describe("toBigInt", () => {
  it("converts valid numeric string", () => {
    expect(toBigInt("42", 0n)).toBe(42n);
  });

  it("converts large offset string", () => {
    expect(toBigInt("9007199254740993", 0n)).toBe(9007199254740993n);
  });

  it("returns fallback for non-numeric string", () => {
    expect(toBigInt("not-a-number", 99n)).toBe(99n);
  });

  it("returns fallback for empty string", () => {
    expect(toBigInt("", 0n)).toBe(0n);
  });

  it("converts zero", () => {
    expect(toBigInt("0", 99n)).toBe(0n);
  });

  it("converts negative values", () => {
    expect(toBigInt("-1", 0n)).toBe(-1n);
  });
});

describe("nowSeconds", () => {
  it("returns elapsed time in seconds", () => {
    const start = Date.now() - 5000;
    const elapsed = nowSeconds(start);

    expect(elapsed).toBeGreaterThanOrEqual(4.9);
    expect(elapsed).toBeLessThanOrEqual(5.5);
  });

  it("returns approximately zero for recent start", () => {
    const start = Date.now();
    const elapsed = nowSeconds(start);

    expect(elapsed).toBeGreaterThanOrEqual(0);
    expect(elapsed).toBeLessThan(0.1);
  });
});
