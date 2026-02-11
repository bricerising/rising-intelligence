import { describe, expect, it } from "vitest";
import {
  parseNonNegativeIntegerStrict,
  parsePositiveIntegerStrict,
} from "../src/lib/number.js";

describe("number parsing helpers", () => {
  it("parses positive integers exactly", () => {
    expect(parsePositiveIntegerStrict("42", "--count")).toBe(42);
    expect(parsePositiveIntegerStrict(" 7 ", "--count")).toBe(7);
  });

  it("rejects malformed integer strings", () => {
    expect(() => parsePositiveIntegerStrict("10abc", "--count")).toThrow(
      /invalid integer/i
    );
    expect(() => parsePositiveIntegerStrict("1.5", "--count")).toThrow(
      /invalid integer/i
    );
  });

  it("enforces non-negative constraints", () => {
    expect(parseNonNegativeIntegerStrict("0", "--min-count")).toBe(0);
    expect(() => parseNonNegativeIntegerStrict("-1", "--min-count")).toThrow(
      /non-negative/i
    );
  });
});
