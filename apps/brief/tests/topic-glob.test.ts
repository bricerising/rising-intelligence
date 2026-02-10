import { describe, expect, it } from "vitest";
import {
  compileTopicGlobMatchers,
  matchesAnyTopicGlob,
  normalizeTopicGlobs,
} from "../src/topic-glob.js";

describe("topic glob matcher", () => {
  it("matches wildcard and single-character patterns", () => {
    const matchers = compileTopicGlobMatchers(["aws.*", "ai.?"]);
    expect(matchesAnyTopicGlob("aws.bedrock", matchers)).toBe(true);
    expect(matchesAnyTopicGlob("ai.x", matchers)).toBe(true);
    expect(matchesAnyTopicGlob("ai.openai", matchers)).toBe(false);
  });

  it("defaults to '*' when glob list is missing", () => {
    const matchers = compileTopicGlobMatchers(undefined);
    expect(matchesAnyTopicGlob("aws.bedrock", matchers)).toBe(true);
    expect(matchesAnyTopicGlob("anything", matchers)).toBe(true);
  });

  it("deduplicates normalized glob patterns", () => {
    expect(normalizeTopicGlobs(["aws.*", "aws.*", "ai.*"])).toEqual(["aws.*", "ai.*"]);
  });

  it("rejects unsupported characters", () => {
    expect(() => compileTopicGlobMatchers(["aws.[*]"])).toThrow("Unsupported topic glob pattern");
  });
});
