import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { RSSAdapter, createRSSAdapter } from "../../src/adapters/rss.js";
import { CheckpointStore } from "../../src/checkpoint.js";

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

function createMockCheckpoints(): CheckpointStore {
  const store = new Map<string, string>();
  return {
    getCheckpoint: vi.fn((source: string, key: string) => store.get(`${source}:${key}`)),
    setCheckpoint: vi.fn((source: string, key: string, value: string) => {
      store.set(`${source}:${key}`, value);
    }),
    hasSeen: vi.fn(() => false),
    markSeen: vi.fn(),
    listCheckpoints: vi.fn(() => ({})),
    initialize: vi.fn(),
    close: vi.fn(),
    cleanupSeen: vi.fn(() => 0),
  } as any;
}

describe("RSSAdapter", () => {
  let tmpDir: string;
  let feedsPath: string;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "ri-rss-test-"));
    feedsPath = join(tmpDir, "feeds.yaml");
    vi.clearAllMocks();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  describe("configuration loading", () => {
    it("loads feeds from YAML and applies defaults", () => {
      writeFileSync(
        feedsPath,
        `
defaults:
  poll_interval_seconds: 600
  priority: 50
  enabled: true

official_blogs:
  - name: AWS Blog
    url: https://aws.amazon.com/blogs/aws/feed/
    priority: 90
    category: cloud

ai_research:
  - name: OpenAI Blog
    url: https://openai.com/blog/rss.xml
    enabled: false
`
      );

      const logger = createTestLogger();
      const checkpoints = createMockCheckpoints();
      const adapter = new RSSAdapter(feedsPath, 300000, checkpoints, logger);

      // Only enabled feeds should be loaded
      expect((adapter as any).feeds).toHaveLength(1);
      expect((adapter as any).feeds[0]).toMatchObject({
        name: "AWS Blog",
        url: "https://aws.amazon.com/blogs/aws/feed/",
        priority: 90,
        poll_interval_seconds: 600,
        category: "cloud",
      });
    });

    it("handles empty sections gracefully", () => {
      writeFileSync(
        feedsPath,
        `
defaults:
  enabled: true
official_blogs:
  - name: Test Feed
    url: https://example.com/feed
`
      );

      const adapter = new RSSAdapter(feedsPath, 300000, createMockCheckpoints(), createTestLogger());
      expect((adapter as any).feeds).toHaveLength(1);
    });
  });

  describe("fetch()", () => {
    it("yields events for new feed items", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Test Feed
    url: https://example.com/feed
`
      );

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "guid-1",
              title: "Article One",
              link: "https://example.com/article-1",
              contentSnippet: "Content for article one",
              pubDate: "2024-01-15T10:00:00Z",
              creator: "John Doe",
            },
            {
              guid: "guid-2",
              title: "Article Two",
              link: "https://example.com/article-2",
              contentSnippet: "Content for article two",
              pubDate: "2024-01-14T10:00:00Z",
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const checkpoints = createMockCheckpoints();
      const adapter = new RSSAdapter(feedsPath, 300000, checkpoints, createTestLogger());
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Should yield both items in reverse order (oldest first)
      expect(results).toHaveLength(2);
      expect(results[0].event.title).toBe("Article Two");
      expect(results[1].event.title).toBe("Article One");

      // Verify event structure
      expect(results[1].event).toMatchObject({
        source: "rss",
        title: "Article One",
        text: "Content for article one",
        url: "https://example.com/article-1",
        author: { display_name: "John Doe" },
      });
      expect(results[1].event.event_id).toMatch(/^rss:[a-f0-9]+:[a-f0-9]+$/);
      expect(results[1].event.published_at).toBe("2024-01-15T10:00:00.000Z");
    });

    it("respects checkpoint and only yields new items", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Test Feed
    url: https://example.com/feed
`
      );

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            { guid: "guid-3", title: "New Article", contentSnippet: "New content" },
            { guid: "guid-2", title: "Old Article", contentSnippet: "Old content" }, // checkpoint
            { guid: "guid-1", title: "Older Article", contentSnippet: "Older content" },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const checkpoints = createMockCheckpoints();
      // Set checkpoint to guid-2
      checkpoints.getCheckpoint = vi.fn().mockReturnValue("guid-2");

      const adapter = new RSSAdapter(feedsPath, 300000, checkpoints, createTestLogger());
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Should only yield the new item (guid-3)
      expect(results).toHaveLength(1);
      expect(results[0].event.title).toBe("New Article");
      expect(results[0].checkpointValue).toBe("guid-3");
    });

    it("limits items when checkpoint is stale (not found in feed)", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Test Feed
    url: https://example.com/feed
`
      );

      // Generate 15 items
      const items = Array.from({ length: 15 }, (_, i) => ({
        guid: `guid-${i}`,
        title: `Article ${i}`,
        contentSnippet: `Content ${i}`,
      }));

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({ items }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const checkpoints = createMockCheckpoints();
      // Checkpoint exists but is not in the current feed (stale)
      checkpoints.getCheckpoint = vi.fn().mockReturnValue("guid-not-in-feed");

      const adapter = new RSSAdapter(feedsPath, 300000, checkpoints, createTestLogger());
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Should limit to 10 items when checkpoint exists but not found in feed
      expect(results).toHaveLength(10);
    });

    it("handles empty feed gracefully", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Empty Feed
    url: https://example.com/empty-feed
`
      );

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({ items: [] }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new RSSAdapter(feedsPath, 300000, createMockCheckpoints(), createTestLogger());
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(0);
    });

    it("continues processing other feeds when one fails", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Failing Feed
    url: https://example.com/failing
  - name: Working Feed
    url: https://example.com/working
`
      );

      const mockParser = {
        parseURL: vi.fn().mockImplementation((url: string) => {
          if (url.includes("failing")) {
            throw new Error("Network error");
          }
          return Promise.resolve({
            items: [{ guid: "guid-1", title: "Working Article", contentSnippet: "Content" }],
          });
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const logger = createTestLogger();
      const adapter = new RSSAdapter(feedsPath, 300000, createMockCheckpoints(), logger);
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Should still get the working feed
      expect(results).toHaveLength(1);
      expect(results[0].event.title).toBe("Working Article");
      // Parse failures are logged at warn level, not error
      expect(logger.warn).toHaveBeenCalled();
    });

    it("throws when all configured feeds fail", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Feed One
    url: https://example.com/feed-one
  - name: Feed Two
    url: https://example.com/feed-two
`
      );

      const mockParser = {
        parseURL: vi.fn().mockRejectedValue(new Error("Network error")),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new RSSAdapter(feedsPath, 300000, createMockCheckpoints(), createTestLogger());
      await adapter.initialize();

      await expect(async () => {
        for await (const _result of adapter.fetch()) {
          // No-op
        }
      }).rejects.toThrow("All configured RSS feeds failed during poll cycle");
    });

    it("extracts URLs and hashtags from content", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Test Feed
    url: https://example.com/feed
`
      );

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "guid-1",
              title: "Check out #AI trends",
              contentSnippet: "Visit https://example.com for more #MachineLearning content",
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new RSSAdapter(feedsPath, 300000, createMockCheckpoints(), createTestLogger());
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      // Note: extractHashtags returns lowercase without # prefix
      expect(results[0].event.extracted).toEqual({
        urls: ["https://example.com"],
        hashtags: ["ai", "machinelearning"],
      });
    });

    it("uses link as GUID fallback when guid is missing", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Test Feed
    url: https://example.com/feed
`
      );

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              // No guid field
              link: "https://example.com/article-1",
              title: "Article without GUID",
              contentSnippet: "Content",
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new RSSAdapter(feedsPath, 300000, createMockCheckpoints(), createTestLogger());
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(1);
      expect(results[0].checkpointValue).toBe("https://example.com/article-1");
    });
  });

  describe("factory function", () => {
    it("createRSSAdapter returns a valid SourceAdapter", () => {
      writeFileSync(feedsPath, "official_blogs: []");
      const adapter = createRSSAdapter(feedsPath, 300000, createMockCheckpoints(), createTestLogger());

      expect(adapter.name).toBe("rss");
      expect(adapter.source).toBe("rss");
      expect(adapter.pollIntervalMs).toBe(300000);
      expect(typeof adapter.initialize).toBe("function");
      expect(typeof adapter.fetch).toBe("function");
      expect(typeof adapter.shutdown).toBe("function");
    });
  });
});
