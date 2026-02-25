import { Source } from "@rising-intelligence/db";
import { describe, expect, it } from "vitest";
import {
  buildWhereClause,
  parseEnrichSteps,
} from "../src/commands/events/enrich.js";

describe("events enrich helpers", () => {
  it("defaults to retag and quality steps", () => {
    expect(parseEnrichSteps(undefined)).toEqual(["retag", "quality"]);
  });

  it("parses and deduplicates custom step order", () => {
    expect(parseEnrichSteps("quality, retag, quality")).toEqual(["quality", "retag"]);
  });

  it("throws for invalid step names", () => {
    expect(() => parseEnrichSteps("retag,unknown")).toThrow(/invalid --steps value/i);
  });

  it("builds missing-only where clauses", () => {
    expect(buildWhereClause(true, Source.rss)).toEqual({
      OR: [{ topics: { isEmpty: true } }, { tags: { isEmpty: true } }],
      source: Source.rss,
    });
  });

  it("builds all-rows where clauses when missing-only is disabled", () => {
    expect(buildWhereClause(false, undefined)).toEqual({});
  });
});
