import { Source } from "@rising-intelligence/db";
import { describe, expect, it } from "vitest";
import {
  parseSourceFlag,
  resolveAllowlistPath,
  stringArraysEqual,
} from "../src/commands/topics/retag.js";

describe("topics retag helpers", () => {
  it("resolves default allowlist path when flag is unset", () => {
    const path = resolveAllowlistPath(undefined);
    expect(path.endsWith("/infra/config/topics.allowlist.yaml")).toBe(true);
  });

  it("resolves custom allowlist path from flag", () => {
    const path = resolveAllowlistPath("./infra/config/topics.allowlist.yaml");
    expect(path.endsWith("/infra/config/topics.allowlist.yaml")).toBe(true);
  });

  it("parses --source into Source enum values", () => {
    expect(parseSourceFlag("rss")).toBe(Source.rss);
    expect(parseSourceFlag("HACKERNEWS")).toBe(Source.hackernews);
    expect(parseSourceFlag(undefined)).toBeUndefined();
  });

  it("throws for invalid --source values", () => {
    expect(() => parseSourceFlag("invalid-source")).toThrow(/invalid --source value/i);
  });

  it("compares topic arrays deterministically", () => {
    expect(stringArraysEqual([], [])).toBe(true);
    expect(stringArraysEqual(["ai.openai"], ["ai.openai"])).toBe(true);
    expect(stringArraysEqual(["ai.openai"], ["ai.anthropic"])).toBe(false);
    expect(stringArraysEqual(["ai.openai", "ai.agents"], ["ai.agents", "ai.openai"])).toBe(
      false
    );
  });
});
