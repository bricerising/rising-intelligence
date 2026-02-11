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

    it("propagates market_gate and signal_tier from defaults to feeds", () => {
      writeFileSync(
        feedsPath,
        `
defaults:
  market_gate: true
  signal_tier: low_volume

policy_feeds:
  - name: SEC Press Releases
    url: https://www.sec.gov/news/pressreleases.rss
  - name: PR Newswire
    url: https://www.prnewswire.com/rss/news-releases-list.rss
    signal_tier: high_volume
`
      );

      const adapter = new RSSAdapter(feedsPath, 300000, createMockCheckpoints(), createTestLogger());
      const feeds = (adapter as any).feeds as any[];

      expect(feeds).toHaveLength(2);
      // SEC feed inherits defaults
      expect(feeds[0]).toMatchObject({
        name: "SEC Press Releases",
        market_gate: true,
        signal_tier: "low_volume",
      });
      // PR Newswire overrides signal_tier but inherits market_gate
      expect(feeds[1]).toMatchObject({
        name: "PR Newswire",
        market_gate: true,
        signal_tier: "high_volume",
      });
    });

    it("warns when high-volume strict gating has no EDGAR watchlist entity terms", () => {
      writeFileSync(
        feedsPath,
        `
wire_feeds:
  - name: PR Newswire
    url: https://example.com/pr-newswire
    market_gate: true
    signal_tier: high_volume
`
      );

      const logger = createTestLogger();
      new RSSAdapter(feedsPath, 300000, createMockCheckpoints(), logger);

      expect(logger.warn).toHaveBeenCalledWith(
        expect.objectContaining({
          feeds: ["PR Newswire"],
        }),
        expect.stringContaining(
          "High-volume market-gated feeds are configured without effective entity_terms"
        )
      );
    });

    it("does not warn when high-volume feeds provide their own entity terms", () => {
      writeFileSync(
        feedsPath,
        `
wire_feeds:
  - name: PR Newswire
    url: https://example.com/pr-newswire
    market_gate: true
    signal_tier: high_volume
    entity_terms: ["adyen"]
`
      );

      const logger = createTestLogger();
      new RSSAdapter(feedsPath, 300000, createMockCheckpoints(), logger);

      expect(logger.warn).not.toHaveBeenCalled();
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

    it("configures parser with a permissive Accept header", () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Test Feed
    url: https://example.com/feed
`
      );

      new RSSAdapter(feedsPath, 300000, createMockCheckpoints(), createTestLogger());

      expect(Parser).toHaveBeenCalledWith(
        expect.objectContaining({
          timeout: 30000,
          headers: expect.objectContaining({
            Accept: "*/*",
          }),
        })
      );
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
      const onFeedError = vi.fn();
      const adapter = new RSSAdapter(
        feedsPath,
        300000,
        createMockCheckpoints(),
        logger,
        undefined,
        onFeedError
      );
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
      expect(onFeedError).toHaveBeenCalledWith({
        feed: "Failing Feed",
        feedUrl: "https://example.com/failing",
        errorType: "parse_error",
      });
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

    it("loads feeds from multiple comma-separated feed config files", async () => {
      const feedsPath2 = join(tmpDir, "feeds-2.yaml");
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Feed One
    url: https://example.com/feed-one
`
      );
      writeFileSync(
        feedsPath2,
        `
official_blogs:
  - name: Feed Two
    url: https://example.com/feed-two
`
      );

      const adapter = new RSSAdapter(
        `${feedsPath},${feedsPath2}`,
        300000,
        createMockCheckpoints(),
        createTestLogger()
      );

      expect((adapter as any).feeds).toHaveLength(2);
      expect((adapter as any).feeds.map((feed: any) => feed.name)).toEqual([
        "Feed One",
        "Feed Two",
      ]);
    });

    it("applies market gate to low-volume feeds and adds market metadata", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: Policy Feed
    url: https://example.com/policy
    market_gate: true
    signal_tier: low_volume
`
      );

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "guid-1",
              title: "Acquirer launches new payment terminal",
              contentSnippet: "New point of sale rollout for merchants",
            },
            {
              guid: "guid-2",
              title: "Unrelated post",
              contentSnippet: "No payment relevance in this entry",
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new RSSAdapter(
        feedsPath,
        300000,
        createMockCheckpoints(),
        createTestLogger(),
        undefined,
        undefined,
        {
          marketFilterProfiles: [
            {
              key: "pos",
              name: "POS",
              matchers: [{ type: "keyword", raw: "point of sale", keyword: "point of sale" }],
            },
          ],
        }
      );
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(1);
      expect(results[0].event.tags).toEqual(["market.pos"]);
      expect(results[0].event.source_meta).toEqual(
        expect.objectContaining({
          market_profiles: ["pos"],
          match_reasons: expect.arrayContaining(["pos:keyword:point of sale"]),
          signal_tier: "low_volume",
        })
      );
    });

    it("enforces strict entity and keyword gate for high-volume feeds", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: PR Newswire
    url: https://example.com/pr-newswire
    market_gate: true
    signal_tier: high_volume
    entity_terms: ["adyen", "block"]
`
      );

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "guid-pass",
              title: "Adyen launches next-gen point of sale suite",
              contentSnippet: "Merchant payment expansion announcement",
            },
            {
              guid: "guid-fail",
              title: "Point of sale upgrades announced",
              contentSnippet: "No watchlist entity mentioned",
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new RSSAdapter(
        feedsPath,
        300000,
        createMockCheckpoints(),
        createTestLogger(),
        undefined,
        undefined,
        {
          marketFilterProfiles: [
            {
              key: "pos",
              name: "POS",
              matchers: [{ type: "keyword", raw: "point of sale", keyword: "point of sale" }],
            },
          ],
        }
      );
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(1);
      expect(results[0].checkpointValue).toBe("guid-pass");
      expect(results[0].event.source_meta).toEqual(
        expect.objectContaining({
          signal_tier: "high_volume",
          match_reasons: expect.arrayContaining([
            "pos:keyword:point of sale",
            "entity:adyen",
          ]),
        })
      );
    });

    it("derives high-volume entity terms from EDGAR watchlist feeds", async () => {
      writeFileSync(
        feedsPath,
        `
edgar_watchlist:
  - name: EDGAR - Adyen
    url: https://example.com/edgar-adyen
    source_type: edgar
    market_gate: true
    signal_tier: low_volume
    entity_terms: ["adyen"]
wire_feeds:
  - name: PR Newswire
    url: https://example.com/pr-newswire
    market_gate: true
    signal_tier: high_volume
`
      );

      const mockParser = {
        parseURL: vi.fn(async (url: string) => {
          if (url.includes("edgar-adyen")) {
            return { items: [] };
          }
          if (url.includes("pr-newswire")) {
            return {
              items: [
                {
                  guid: "guid-pass",
                  title: "Adyen expands point of sale footprint",
                  contentSnippet: "Merchant payment growth continues",
                },
                {
                  guid: "guid-fail",
                  title: "Point of sale upgrades announced",
                  contentSnippet: "No watchlist entity mentioned",
                },
              ],
            };
          }
          return { items: [] };
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new RSSAdapter(
        feedsPath,
        300000,
        createMockCheckpoints(),
        createTestLogger(),
        undefined,
        undefined,
        {
          marketFilterProfiles: [
            {
              key: "pos",
              name: "POS",
              matchers: [{ type: "keyword", raw: "point of sale", keyword: "point of sale" }],
            },
          ],
          edgarFetchDetailMetadata: false,
        }
      );
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(1);
      expect(results[0].checkpointValue).toBe("guid-pass");
      expect(results[0].event.source_meta).toEqual(
        expect.objectContaining({
          signal_tier: "high_volume",
          match_reasons: expect.arrayContaining([
            "pos:keyword:point of sale",
            "entity:adyen",
          ]),
        })
      );
    });

    it("enforces EDGAR form allowlist and enriches detail metadata", async () => {
      writeFileSync(
        feedsPath,
        `
official_blogs:
  - name: EDGAR - Adyen
    url: https://example.com/edgar-adyen
    source_type: edgar
    market_gate: true
    signal_tier: low_volume
    cik: "0001707432"
`
      );

      const mockParser = {
        parseURL: vi.fn().mockResolvedValue({
          items: [
            {
              guid: "guid-allowed",
              title: "Adyen N.V. - 8-K - Current report",
              link: "https://www.sec.gov/ixviewer/ix.html?doc=/Archives/example.htm",
              contentSnippet: "Point of sale expansion filing",
              pubDate: "2026-02-01T12:00:00Z",
            },
            {
              guid: "guid-disallowed",
              title: "Adyen N.V. - 3 - Insider filing",
              link: "https://www.sec.gov/ixviewer/ix.html?doc=/Archives/example2.htm",
              contentSnippet: "Point of sale filing",
              pubDate: "2026-02-01T13:00:00Z",
            },
          ],
        }),
      };

      (Parser as any).mockImplementation(() => mockParser);

      const adapter = new RSSAdapter(
        feedsPath,
        300000,
        createMockCheckpoints(),
        createTestLogger(),
        undefined,
        undefined,
        {
          marketFilterProfiles: [
            {
              key: "pos",
              name: "POS",
              matchers: [{ type: "keyword", raw: "point of sale", keyword: "point of sale" }],
            },
          ],
          edgarFormsAllowlist: ["8-K"],
          fetchEdgarDetailMetadata: vi.fn(async () => ({
            filedDate: "2026-02-01",
            acceptedAt: "2026-02-01T12:05:00Z",
            primaryDocumentName: "form8k.htm",
          })),
        }
      );
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(1);
      expect(results[0].checkpointValue).toBe("guid-allowed");
      expect(results[0].event.source_meta).toEqual(
        expect.objectContaining({
          source_type: "edgar",
          signal_tier: "low_volume",
          form_type: "8-K",
          cik: "0001707432",
          filed_date: "2026-02-01",
          accepted_at: "2026-02-01T12:05:00Z",
          primary_document_name: "form8k.htm",
        })
      );
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

  describe("real config smoke test", () => {
    const realPosConfig = join(
      __dirname,
      "..",
      "..",
      "..",
      "..",
      "infra",
      "config",
      "feeds.pos.yaml"
    );

    it("feeds.pos.yaml loads with market_gate and signal_tier defaults applied to all feeds", () => {
      const adapter = new RSSAdapter(
        realPosConfig,
        300000,
        createMockCheckpoints(),
        createTestLogger()
      );
      const feeds = (adapter as any).feeds as any[];

      // 10 feeds: 4 EDGAR + 3 policy + 1 security + 1 merchant + 1 wire
      expect(feeds.length).toBeGreaterThanOrEqual(10);

      // Every feed must have market_gate: true (inherited from defaults)
      for (const feed of feeds) {
        expect(feed.market_gate).toBe(true);
      }

      // All feeds except PR Newswire should be low_volume
      const lowVolume = feeds.filter((f: any) => f.signal_tier === "low_volume");
      const highVolume = feeds.filter((f: any) => f.signal_tier === "high_volume");
      expect(lowVolume.length).toBe(feeds.length - 1);
      expect(highVolume).toHaveLength(1);
      expect(highVolume[0].name).toMatch(/PR Newswire/i);

      // EDGAR feeds must have source_type and cik
      const edgarFeeds = feeds.filter((f: any) => f.source_type === "edgar");
      expect(edgarFeeds.length).toBeGreaterThanOrEqual(4);
      for (const feed of edgarFeeds) {
        expect(feed.cik).toBeTruthy();
      }
    });
  });

  describe("factory function", () => {
    it("createRSSAdapter returns a valid SourceAdapter", () => {
      writeFileSync(feedsPath, "official_blogs: []");
      const adapter = createRSSAdapter({
        feedsConfigPath: feedsPath,
        pollIntervalMs: 300000,
        checkpoints: createMockCheckpoints(),
        logger: createTestLogger(),
      });

      expect(adapter.name).toBe("rss");
      expect(adapter.source).toBe("rss");
      expect(adapter.pollIntervalMs).toBe(300000);
      expect(typeof adapter.initialize).toBe("function");
      expect(typeof adapter.fetch).toBe("function");
      expect(typeof adapter.shutdown).toBe("function");
    });
  });
});
