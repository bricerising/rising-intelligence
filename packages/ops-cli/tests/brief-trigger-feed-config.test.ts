import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { briefTrigger } from "../src/commands/brief/trigger.js";

function findJsonPayload(logCalls: Array<unknown[]>): Record<string, unknown> {
  for (const call of logCalls) {
    const [firstArg] = call;
    if (typeof firstArg !== "string") {
      continue;
    }
    if (!firstArg.trim().startsWith("{")) {
      continue;
    }
    return JSON.parse(firstArg) as Record<string, unknown>;
  }
  throw new Error("Unable to find JSON payload in console output");
}

describe("brief trigger feed-config integration", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete process.env.DATABASE_URL;
  });

  it("derives and merges topic globs in dry-run mode", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "ri-brief-trigger-"));
    try {
      const feedConfigPath = join(tempDir, "feeds.pos.yaml");
      writeFileSync(
        feedConfigPath,
        `
wire:
  - name: PRN
    url: https://example.com/prn
    topics: ["market.pos", "payments.*"]
`
      );

      const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      await briefTrigger({
        "feed-config": feedConfigPath,
        "topic-globs": "custom.*",
        "dry-run": true,
      });

      const payload = findJsonPayload(logSpy.mock.calls);
      const body = payload.payload as Record<string, unknown>;
      const query = body.query as Record<string, unknown>;
      expect(query.topic_globs).toEqual(["market.pos", "payments.*", "custom.*"]);
      expect(warnSpy).toHaveBeenCalled();
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("defaults query topic globs to wildcard when feed-derived and explicit globs are absent", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "ri-brief-trigger-"));
    try {
      const feedConfigPath = join(tempDir, "feeds.empty.yaml");
      writeFileSync(
        feedConfigPath,
        `
wire:
  - name: Empty
    url: https://example.com/empty
    topics: []
`
      );

      const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
      vi.spyOn(console, "warn").mockImplementation(() => {});

      await briefTrigger({
        "feed-config": feedConfigPath,
        "dry-run": true,
      });

      const payload = findJsonPayload(logSpy.mock.calls);
      const body = payload.payload as Record<string, unknown>;
      const query = body.query as Record<string, unknown>;
      expect(query.topic_globs).toEqual(["*"]);
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("fails fast when a referenced --feed-config file does not exist", async () => {
    await expect(
      briefTrigger({
        "feed-config": "/tmp/missing-feed-config.yaml",
        "dry-run": true,
      })
    ).rejects.toThrow(/--feed-config path is missing or unreadable/i);
  });
});
