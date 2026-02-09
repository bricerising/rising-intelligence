import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { HackerNewsAdapter, createHackerNewsAdapter } from "../../src/adapters/hackernews.js";

// Mock global fetch
const mockFetch = vi.fn();
global.fetch = mockFetch;

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

function mockFetchResponse(data: unknown, ok = true, status = 200) {
  return Promise.resolve({
    ok,
    status,
    statusText: ok ? "OK" : "Error",
    json: () => Promise.resolve(data),
  });
}

describe("HackerNewsAdapter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("initialization", () => {
    it("initializes with correct properties", async () => {
      const adapter = new HackerNewsAdapter("top", 300000, 30, createMockCheckpoints(), createTestLogger());

      expect(adapter.name).toBe("hackernews");
      expect(adapter.source).toBe("hackernews");
      expect(adapter.pollIntervalMs).toBe(300000);

      await adapter.initialize();
      // Should not throw
    });
  });

  describe("fetch()", () => {
    it("fetches new stories from HN API", async () => {
      const checkpoints = createMockCheckpoints();

      mockFetch
        .mockImplementationOnce(() =>
          // topstories.json
          mockFetchResponse([103, 102, 101])
        )
        .mockImplementationOnce(() =>
          // item/103.json
          mockFetchResponse({
            id: 103,
            type: "story",
            title: "Story 103",
            url: "https://example.com/103",
            by: "user1",
            time: 1705320000,
            score: 150,
            descendants: 45,
          })
        )
        .mockImplementationOnce(() =>
          // item/102.json
          mockFetchResponse({
            id: 102,
            type: "story",
            title: "Story 102",
            url: "https://example.com/102",
            by: "user2",
            time: 1705310000,
            score: 80,
            descendants: 20,
          })
        )
        .mockImplementationOnce(() =>
          // item/101.json
          mockFetchResponse({
            id: 101,
            type: "story",
            title: "Story 101",
            text: "This is a text post",
            by: "user3",
            time: 1705300000,
            score: 50,
            descendants: 10,
          })
        );

      const adapter = new HackerNewsAdapter("top", 300000, 30, checkpoints, createTestLogger());
      await adapter.initialize();

      const results: any[] = [];
      const fetchPromise = (async () => {
        for await (const result of adapter.fetch()) {
          results.push(result);
        }
      })();

      // Advance timers to cover the 100ms delays between fetches
      await vi.advanceTimersByTimeAsync(500);
      await fetchPromise;

      expect(results).toHaveLength(3);

      // Verify event structure
      expect(results[0].event).toMatchObject({
        event_id: "hn:103",
        source: "hackernews",
        title: "Story 103",
        url: "https://example.com/103",
        author: { handle: "user1", display_name: "user1" },
        engagement: { score: 150, comments: 45 },
      });
      // 1705320000 * 1000 = 2024-01-15T12:00:00.000Z
      expect(results[0].event.published_at).toBe("2024-01-15T12:00:00.000Z");

      // Text post should use text field, not url
      expect(results[2].event.text).toBe("This is a text post");
      expect(results[2].event.url).toBe("https://news.ycombinator.com/item?id=101");
    });

    it("respects checkpoint and only fetches newer stories", async () => {
      const checkpoints = createMockCheckpoints();
      checkpoints.getCheckpoint = vi.fn().mockReturnValue("100"); // Already seen up to 100

      mockFetch
        .mockImplementationOnce(() => mockFetchResponse([103, 102, 101, 100, 99]))
        .mockImplementationOnce(() =>
          mockFetchResponse({
            id: 103,
            type: "story",
            title: "New Story",
            by: "user1",
            time: 1705320000,
            score: 50,
          })
        )
        .mockImplementationOnce(() =>
          mockFetchResponse({
            id: 102,
            type: "story",
            title: "Another New Story",
            by: "user2",
            time: 1705310000,
            score: 30,
          })
        )
        .mockImplementationOnce(() =>
          mockFetchResponse({
            id: 101,
            type: "story",
            title: "Third New Story",
            by: "user3",
            time: 1705300000,
            score: 20,
          })
        );

      const adapter = new HackerNewsAdapter("top", 300000, 30, checkpoints, createTestLogger());

      const results: any[] = [];
      const fetchPromise = (async () => {
        for await (const result of adapter.fetch()) {
          results.push(result);
        }
      })();

      await vi.advanceTimersByTimeAsync(500);
      await fetchPromise;

      // Should only fetch stories > 100 (i.e., 101, 102, 103)
      expect(results).toHaveLength(3);

      // Checkpoint should be updated to max ID
      expect(results[2].checkpointKey).toBe("last_max_id_top");
      expect(results[2].checkpointValue).toBe("103");
    });

    it("limits items per poll to maxItems", async () => {
      const checkpoints = createMockCheckpoints();

      // Return 50 story IDs
      const storyIds = Array.from({ length: 50 }, (_, i) => 150 - i);
      mockFetch.mockImplementationOnce(() => mockFetchResponse(storyIds));

      // Mock each story fetch
      for (let i = 0; i < 30; i++) {
        mockFetch.mockImplementationOnce(() =>
          mockFetchResponse({
            id: 150 - i,
            type: "story",
            title: `Story ${150 - i}`,
            by: "user",
            time: 1705320000,
            score: 10,
          })
        );
      }

      const adapter = new HackerNewsAdapter("top", 300000, 30, checkpoints, createTestLogger());

      const results: any[] = [];
      const fetchPromise = (async () => {
        for await (const result of adapter.fetch()) {
          results.push(result);
        }
      })();

      await vi.advanceTimersByTimeAsync(5000);
      await fetchPromise;

      // Should be capped at maxItems (30)
      expect(results).toHaveLength(30);
    });

    it("skips non-story items", async () => {
      const checkpoints = createMockCheckpoints();

      mockFetch
        .mockImplementationOnce(() => mockFetchResponse([103, 102]))
        .mockImplementationOnce(() =>
          mockFetchResponse({
            id: 103,
            type: "job", // Not a story
            title: "Job posting",
            by: "company",
          })
        )
        .mockImplementationOnce(() =>
          mockFetchResponse({
            id: 102,
            type: "story",
            title: "Actual story",
            by: "user",
            time: 1705320000,
            score: 50,
          })
        );

      const adapter = new HackerNewsAdapter("top", 300000, 30, checkpoints, createTestLogger());

      const results: any[] = [];
      const fetchPromise = (async () => {
        for await (const result of adapter.fetch()) {
          results.push(result);
        }
      })();

      await vi.advanceTimersByTimeAsync(500);
      await fetchPromise;

      expect(results).toHaveLength(1);
      expect(results[0].event.title).toBe("Actual story");
      expect(results[0].checkpointValue).toBe("103");
    });

    it("handles null items from API", async () => {
      const checkpoints = createMockCheckpoints();

      mockFetch
        .mockImplementationOnce(() => mockFetchResponse([103, 102]))
        .mockImplementationOnce(() => mockFetchResponse(null)) // Deleted item
        .mockImplementationOnce(() =>
          mockFetchResponse({
            id: 102,
            type: "story",
            title: "Valid story",
            by: "user",
            time: 1705320000,
            score: 50,
          })
        );

      const adapter = new HackerNewsAdapter("top", 300000, 30, checkpoints, createTestLogger());

      const results: any[] = [];
      const fetchPromise = (async () => {
        for await (const result of adapter.fetch()) {
          results.push(result);
        }
      })();

      await vi.advanceTimersByTimeAsync(500);
      await fetchPromise;

      expect(results).toHaveLength(1);
      expect(results[0].checkpointValue).toBe("103");
    });

    it("continues on individual story fetch failure", async () => {
      const checkpoints = createMockCheckpoints();
      const logger = createTestLogger();

      mockFetch
        .mockImplementationOnce(() => mockFetchResponse([103, 102]))
        .mockImplementationOnce(() => Promise.reject(new Error("Network error")))
        .mockImplementationOnce(() =>
          mockFetchResponse({
            id: 102,
            type: "story",
            title: "Story 102",
            by: "user",
            time: 1705320000,
            score: 50,
          })
        );

      const adapter = new HackerNewsAdapter("top", 300000, 30, checkpoints, logger);

      const results: any[] = [];
      const fetchPromise = (async () => {
        for await (const result of adapter.fetch()) {
          results.push(result);
        }
      })();

      await vi.advanceTimersByTimeAsync(500);
      await fetchPromise;

      expect(results).toHaveLength(1);
      expect(results[0].event.title).toBe("Story 102");
      expect(logger.warn).toHaveBeenCalled();
    });

    it("throws when story IDs fetch fails", async () => {
      const checkpoints = createMockCheckpoints();
      const logger = createTestLogger();

      mockFetch.mockImplementationOnce(() => Promise.reject(new Error("API down")));

      const adapter = new HackerNewsAdapter("top", 300000, 30, checkpoints, logger);

      await expect(async () => {
        for await (const _result of adapter.fetch()) {
          // No-op
        }
      }).rejects.toThrow("API down");
      expect(logger.error).toHaveBeenCalled();
    });

    it("handles empty story list", async () => {
      const checkpoints = createMockCheckpoints();
      checkpoints.getCheckpoint = vi.fn().mockReturnValue("1000"); // All stories already seen

      mockFetch.mockImplementationOnce(() => mockFetchResponse([100, 99, 98]));

      const adapter = new HackerNewsAdapter("top", 300000, 30, checkpoints, createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(results).toHaveLength(0);
    });
  });

  describe("different modes", () => {
    it("uses correct endpoint for 'new' mode", async () => {
      const checkpoints = createMockCheckpoints();
      mockFetch.mockImplementationOnce(() => mockFetchResponse([]));

      const adapter = new HackerNewsAdapter("new", 300000, 30, checkpoints, createTestLogger());
      await adapter.initialize();

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(mockFetch).toHaveBeenCalledWith(
        "https://hacker-news.firebaseio.com/v0/newstories.json",
        expect.any(Object)
      );
    });

    it("uses correct endpoint for 'best' mode", async () => {
      const checkpoints = createMockCheckpoints();
      mockFetch.mockImplementationOnce(() => mockFetchResponse([]));

      const adapter = new HackerNewsAdapter("best", 300000, 30, checkpoints, createTestLogger());

      const results: any[] = [];
      for await (const result of adapter.fetch()) {
        results.push(result);
      }

      expect(mockFetch).toHaveBeenCalledWith(
        "https://hacker-news.firebaseio.com/v0/beststories.json",
        expect.any(Object)
      );
    });
  });

  describe("factory function", () => {
    it("createHackerNewsAdapter returns valid adapter with default mode", () => {
      const adapter = createHackerNewsAdapter("invalid", 300000, 30, createMockCheckpoints(), createTestLogger());

      expect(adapter.name).toBe("hackernews");
      expect(adapter.source).toBe("hackernews");
      // Invalid mode should default to 'top'
    });

    it("createHackerNewsAdapter accepts valid modes", () => {
      for (const mode of ["top", "new", "best"]) {
        const adapter = createHackerNewsAdapter(mode, 300000, 30, createMockCheckpoints(), createTestLogger());
        expect(adapter.name).toBe("hackernews");
      }
    });
  });

  describe("event construction", () => {
    it("extracts URLs and hashtags from content", async () => {
      const checkpoints = createMockCheckpoints();

      mockFetch
        .mockImplementationOnce(() => mockFetchResponse([101]))
        .mockImplementationOnce(() =>
          mockFetchResponse({
            id: 101,
            type: "story",
            title: "Check out #AI and #ML",
            text: "Visit https://example.com for more info",
            by: "user",
            time: 1705320000,
            score: 50,
          })
        );

      const adapter = new HackerNewsAdapter("top", 300000, 30, checkpoints, createTestLogger());

      const results: any[] = [];
      const fetchPromise = (async () => {
        for await (const result of adapter.fetch()) {
          results.push(result);
        }
      })();

      await vi.advanceTimersByTimeAsync(200);
      await fetchPromise;

      // Note: extractHashtags returns lowercase without # prefix
      expect(results[0].event.extracted).toEqual({
        urls: ["https://example.com"],
        hashtags: ["ai", "ml"],
      });
    });

    it("includes HN-specific metadata", async () => {
      const checkpoints = createMockCheckpoints();

      mockFetch
        .mockImplementationOnce(() => mockFetchResponse([101]))
        .mockImplementationOnce(() =>
          mockFetchResponse({
            id: 101,
            type: "story",
            title: "Test",
            by: "user",
            time: 1705320000,
            score: 100,
            descendants: 50,
          })
        );

      const adapter = new HackerNewsAdapter("top", 300000, 30, checkpoints, createTestLogger());

      const results: any[] = [];
      const fetchPromise = (async () => {
        for await (const result of adapter.fetch()) {
          results.push(result);
        }
      })();

      await vi.advanceTimersByTimeAsync(200);
      await fetchPromise;

      expect(results[0].event.source_meta).toEqual({
        hn_id: 101,
        hn_type: "story",
        mode: "top",
      });
    });
  });
});
