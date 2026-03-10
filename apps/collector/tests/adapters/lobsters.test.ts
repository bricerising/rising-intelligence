import { describe, it, expect, vi, beforeEach } from "vitest";
import { LobstersAdapter, createLobstersAdapter } from "../../src/adapters/lobsters.js";

// Mock rss-parser
vi.mock("rss-parser", () => {
  return {
    default: vi.fn().mockImplementation(() => ({
      parseURL: vi.fn(),
    })),
  };
});

import Parser from "rss-parser";

function createTestLogger() {
  return {
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    child: vi.fn(() => createTestLogger()),
  } as any;
}

function createMockCheckpoints() {
  const store = new Map<string, string>();
  return {
    getCheckpoint: vi.fn((source: string, key: string) => store.get(`${source}:${key}`)),
    setCheckpoint: vi.fn((source: string, key: string, value: string) => {
      store.set(`${source}:${key}`, value);
    }),
    hasSeen: vi.fn(() => false),
    markSeen: vi.fn(),
  } as any;
}

describe("LobstersAdapter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("initialization", () => {
    it("initializes with correct properties", async () => {
      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), createTestLogger());

      expect(adapter.name).toBe("lobsters");
      expect(adapter.source).toBe("lobsters");
      expect(adapter.pollIntervalMs).toBe(600000);

      await adapter.initialize();
      // Should not throw
    });
  });

  describe("fetch()", () => {
    it("fetches and yields events from Lobsters RSS", async () => {
      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "https://lobste.rs/s/abc123",
              title: "Interesting Rust Article",
              link: "https://example.com/rust-article",
              contentSnippet: "A great article about Rust programming",
              pubDate: "2024-01-15T10:00:00Z",
              creator: "rustdev",
              categories: ["rust", "programming"],
            },
            {
              guid: "https://lobste.rs/s/def456",
              title: "Go Performance Tips",
              link: "https://example.com/go-tips",
              contentSnippet: "Improve your Go code performance",
              pubDate: "2024-01-14T10:00:00Z",
              creator: "gomaster",
              categories: ["go", "performance"],
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const checkpoints = createMockCheckpoints();
      const adapter = new LobstersAdapter(600000, 25, checkpoints, createTestLogger());
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Items yielded in reverse order (oldest first)
      expect(results).toHaveLength(2);
      expect(results[0].event.title).toBe("Go Performance Tips");
      expect(results[1].event.title).toBe("Interesting Rust Article");

      // Verify event structure
      expect(results[1].event).toMatchObject({
        source: "news",
        title: "Interesting Rust Article",
        text: "A great article about Rust programming",
        url: "https://example.com/rust-article",
        author: { handle: "rustdev", display_name: "rustdev" },
      });
      expect(results[1].event.event_id).toMatch(/^lobsters:[a-f0-9]+$/);
      expect(results[1].event.published_at).toBe("2024-01-15T10:00:00.000Z");

      expect(results[1].event.source_meta).toMatchObject({
        collected_from: "lobsters",
        community_tags: ["rust", "programming"],
      });
    });

    it("respects checkpoint and only yields new items", async () => {
      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            { guid: "guid-3", title: "New Article", contentSnippet: "New" },
            { guid: "guid-2", title: "Checkpoint Article", contentSnippet: "Checkpoint" }, // checkpoint
            { guid: "guid-1", title: "Old Article", contentSnippet: "Old" },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const checkpoints = createMockCheckpoints();
      checkpoints.getCheckpoint = vi.fn().mockReturnValue("guid-2");

      const adapter = new LobstersAdapter(600000, 25, checkpoints, createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Should only yield items before checkpoint (guid-3)
      expect(results).toHaveLength(1);
      expect(results[0].event.title).toBe("New Article");
      expect(results[0].checkpointValue).toBe("guid-3");
    });

    it("limits items when checkpoint is stale (not found in feed)", async () => {
      // Generate 20 items
      const items = Array.from({ length: 20 }, (_, i) => ({
        guid: `guid-${i}`,
        title: `Article ${i}`,
        contentSnippet: `Content ${i}`,
      }));

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({ items }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const checkpoints = createMockCheckpoints();
      // Checkpoint exists but not in current feed
      checkpoints.getCheckpoint = vi.fn().mockReturnValue("stale-guid-not-in-feed");

      const adapter = new LobstersAdapter(600000, 25, checkpoints, createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Should limit to min(maxItems, 10) = 10 when checkpoint not found
      expect(results).toHaveLength(10);
    });

    it("respects maxItems when checkpoint is found in feed", async () => {
      // Generate 30 items, with checkpoint at position 10
      const items = Array.from({ length: 30 }, (_, i) => ({
        guid: `guid-${30 - i}`,
        title: `Article ${30 - i}`,
        contentSnippet: `Content ${30 - i}`,
      }));

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({ items }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const checkpoints = createMockCheckpoints();
      // Checkpoint at guid-20 (position 10 in the reversed list)
      // Items are guid-30, guid-29, ..., guid-20, guid-19, ...
      // So 10 items (guid-30 to guid-21) are "new" before hitting checkpoint
      checkpoints.getCheckpoint = vi.fn().mockReturnValue("guid-20");

      // maxItems = 15, but only 10 new items exist before checkpoint
      const adapter = new LobstersAdapter(600000, 15, checkpoints, createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Should get 10 items (those before the checkpoint)
      expect(results).toHaveLength(10);
    });

    it("handles empty feed gracefully", async () => {
      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({ items: [] }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(0);
    });

    it("throws when Lobsters RSS fetch fails", async () => {
      const mockParser = {
        parseURL: vi.fn().mockRejectedValue(new Error("Network error")),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const logger = createTestLogger();
      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), logger);

      await expect(async () => {
        for await (const _result of adapter.fetch()) {
          // No-op
        }
      }).rejects.toThrow("Network error");
      expect(logger.error).toHaveBeenCalled();
    });

    it("continues processing when one item normalization fails", async () => {
      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "guid-2",
              title: "Second item",
              contentSnippet: "Second content",
            },
            {
              guid: "guid-1",
              title: "First item",
              contentSnippet: "First content",
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const logger = createTestLogger();
      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), logger);
      vi.spyOn(adapter as any, "itemToCollectedContent").mockImplementationOnce(async () => {
        throw new Error("bad item");
      });

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(1);
      expect(results[0].event.title).toBe("Second item");
      expect(logger.warn).toHaveBeenCalledWith(
        expect.objectContaining({ guid: "guid-1" }),
        "Failed to normalize Lobsters item"
      );
    });

    it("fetches from correct Lobsters RSS URL", async () => {
      const mockParseURL = vi.fn().mockResolvedValue({ items: [] });
      const mockParser = { parseURL: mockParseURL };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(mockParseURL).toHaveBeenCalledWith("https://lobste.rs/rss");
    });

    it("uses link as guid fallback", async () => {
      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              // No guid field
              link: "https://lobste.rs/s/xyz789",
              title: "Article without GUID",
              contentSnippet: "Content",
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(1);
      expect(results[0].checkpointValue).toBe("https://lobste.rs/s/xyz789");
    });

    it("skips items without guid or link", async () => {
      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            { title: "No identifier", contentSnippet: "Content" }, // No guid or link
            { guid: "valid-guid", title: "Valid item", contentSnippet: "Content" },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(1);
      expect(results[0].event.title).toBe("Valid item");
    });
  });

  describe("event construction", () => {
    it("extracts URLs and hashtags from content", async () => {
      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "guid-1",
              title: "Check out #Rust and #WebAssembly",
              contentSnippet: "Learn more at https://rust-lang.org and https://webassembly.org",
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Note: extractHashtags returns lowercase without # prefix
      expect(results[0].event.extracted).toEqual({
        urls: ["https://rust-lang.org", "https://webassembly.org"],
        hashtags: ["rust", "webassembly"],
      });
    });

    it("includes guid and community tags in source_meta", async () => {
      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "https://lobste.rs/s/abc123",
              title: "Test",
              contentSnippet: "Content",
              categories: ["security", "crypto"],
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results[0].event.source_meta).toMatchObject({
        collected_from: "lobsters",
        guid: "https://lobste.rs/s/abc123",
        community_tags: ["security", "crypto"],
      });
    });

    it("handles missing optional fields", async () => {
      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "guid-1",
              // No title, contentSnippet, creator, categories, pubDate
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(1);
      expect(results[0].event.title).toBeUndefined();
      expect(results[0].event.text).toBe("");
      expect(results[0].event.author).toBeUndefined();
      expect(results[0].event.published_at).toBeUndefined();
    });

    it("handles non-string categories", async () => {
      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "guid-1",
              title: "Test",
              contentSnippet: "Content",
              categories: [{ _: "category-obj" }, "valid-tag", 123, null],
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Should convert non-strings and filter empties
      expect(results[0].event.source_meta.community_tags).toContain("valid-tag");
    });
  });

  describe("factory function", () => {
    it("createLobstersAdapter returns a valid SourceAdapter", () => {
      const adapter = createLobstersAdapter({
        pollIntervalMs: 600000,
        maxItems: 25,
        checkpoints: createMockCheckpoints(),
        logger: createTestLogger(),
      });

      expect(adapter.name).toBe("lobsters");
      expect(adapter.source).toBe("lobsters");
      expect(adapter.pollIntervalMs).toBe(600000);
      expect(typeof adapter.initialize).toBe("function");
      expect(typeof adapter.fetch).toBe("function");
      expect(typeof adapter.shutdown).toBe("function");
    });
  });

  describe("shutdown", () => {
    it("shutdown completes without error", async () => {
      const adapter = new LobstersAdapter(600000, 25, createMockCheckpoints(), createTestLogger());
      await adapter.shutdown();
      // Should not throw
    });
  });
});
