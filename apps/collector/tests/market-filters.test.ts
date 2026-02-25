import { describe, it, expect } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  evaluateMarketFilters,
  loadMarketFilterProfiles,
  matchEntityTerms,
} from "../src/market-filters.js";

describe("market filter profiles", () => {
  it("loads profile YAML files and evaluates keyword matches", () => {
    const tempDir = mkdtempSync(join(tmpdir(), "ri-market-filters-"));
    try {
      writeFileSync(
        join(tempDir, "pos.yaml"),
        `
profile: pos
keywords:
  - point of sale
matchers:
  - type: regex
    pattern: "\\\\bmerchant payment\\\\b"
`
      );

      const profiles = loadMarketFilterProfiles(tempDir);
      expect(profiles).toHaveLength(1);
      expect(profiles[0].key).toBe("pos");

      const evaluation = evaluateMarketFilters(
        "Merchant payment processing and point of sale terminals",
        profiles
      );
      expect(evaluation.marketProfiles).toEqual(["pos"]);
      expect(evaluation.matchReasons).toEqual(
        expect.arrayContaining([
          "pos:keyword:point of sale",
          "pos:regex:\\bmerchant payment\\b",
        ])
      );
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("fails loudly for invalid profile regex", () => {
    const tempDir = mkdtempSync(join(tmpdir(), "ri-market-filters-"));
    try {
      writeFileSync(
        join(tempDir, "bad.yaml"),
        `
profile: pos
matchers:
  - type: regex
    pattern: "[invalid"
`
      );

      expect(() => loadMarketFilterProfiles(tempDir)).toThrow(
        /invalid market filter profile/i
      );
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("matches watchlist entity terms using word boundaries", () => {
    const entityMatch = matchEntityTerms(
      "Adyen expands payment platform while Block updates terminals",
      ["adyen", "block", "fiserv"]
    );

    expect(entityMatch.matched).toBe(true);
    expect(entityMatch.matchedTerms).toEqual(["adyen", "block"]);
  });

  it("rejects entity terms that only appear as substrings of other words", () => {
    const entityMatch = matchEntityTerms(
      "Blockchain technology blocked by roadblock in targeting pinstriped shirts",
      ["block", "target", "stripe"]
    );

    expect(entityMatch.matched).toBe(false);
    expect(entityMatch.matchedTerms).toEqual([]);
  });
});
