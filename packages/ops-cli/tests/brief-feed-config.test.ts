import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  deriveTopicGlobsFromFeedConfigs,
  getRepeatedStringFlag,
  normalizeTopicGlobs,
} from "../src/commands/brief/feed-config.js";

describe("brief feed-config topic derivation", () => {
  it("derives topic globs from all sections including disabled entries", () => {
    const tempDir = mkdtempSync(join(tmpdir(), "ri-feed-config-"));
    try {
      const feedsPath = join(tempDir, "feeds.pos.yaml");
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: One
    url: https://example.com/one
    topics: ["market.pos", "payments.*"]
    enabled: false
wire_feeds:
  - name: Two
    url: https://example.com/two
    topics: ["market.pos", "wire.*"]
`
      );

      const derived = deriveTopicGlobsFromFeedConfigs([feedsPath]);
      expect(derived.topicGlobs).toEqual(["market.pos", "payments.*", "wire.*"]);
      expect(derived.warnings).toEqual([]);
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("warns on empty topics arrays", () => {
    const tempDir = mkdtempSync(join(tmpdir(), "ri-feed-config-"));
    try {
      const feedsPath = join(tempDir, "feeds.empty.yaml");
      writeFileSync(
        feedsPath,
        `
defaults:
  poll_interval_seconds: 900
sections:
  - name: Empty
    url: https://example.com/empty
    topics: []
`
      );

      const derived = deriveTopicGlobsFromFeedConfigs([feedsPath]);
      expect(derived.topicGlobs).toEqual([]);
      expect(derived.warnings).toEqual([
        expect.stringContaining("Ignoring empty topics array"),
      ]);
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("fails with actionable error when feed-config path is missing", () => {
    expect(() =>
      deriveTopicGlobsFromFeedConfigs(["/tmp/does-not-exist-feeds.yaml"])
    ).toThrow(/--feed-config path is missing or unreadable/i);
  });

  it("normalizes and validates topic globs", () => {
    expect(normalizeTopicGlobs(["market.pos", " market.pos ", "payments.*"])).toEqual([
      "market.pos",
      "payments.*",
    ]);
    expect(() => normalizeTopicGlobs(["market pos"])).toThrow(/invalid topic glob pattern/i);
  });

  it("extracts repeated string flags", () => {
    expect(getRepeatedStringFlag({ "feed-config": "a.yaml" }, "feed-config")).toEqual(["a.yaml"]);
    expect(getRepeatedStringFlag({ "feed-config": ["a.yaml", "b.yaml"] }, "feed-config")).toEqual([
      "a.yaml",
      "b.yaml",
    ]);
    expect(getRepeatedStringFlag({ "feed-config": true }, "feed-config")).toEqual([]);
  });
});
