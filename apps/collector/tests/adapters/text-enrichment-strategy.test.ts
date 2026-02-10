import { describe, it, expect, vi, beforeEach } from "vitest";
import type { ContentFetcherConfig } from "../../src/content-fetcher.js";

const { mockFetchArticleContent } = vi.hoisted(() => ({
  mockFetchArticleContent: vi.fn(),
}));

vi.mock("../../src/content-fetcher.js", async () => {
  const actual = await vi.importActual<typeof import("../../src/content-fetcher.js")>(
    "../../src/content-fetcher.js"
  );
  return {
    ...actual,
    fetchArticleContent: mockFetchArticleContent,
  };
});

import {
  createTextEnrichmentStrategy,
  NoopTextEnrichmentStrategy,
  ArticleFetchTextEnrichmentStrategy,
} from "../../src/adapters/text-enrichment-strategy.js";

function createTestLogger() {
  return {
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    child: vi.fn(),
  } as any;
}

const enabledConfig: ContentFetcherConfig = {
  enabled: true,
  timeoutMs: 5000,
  maxContentLength: 10000,
  minContentLength: 200,
  domainDelayMs: 250,
  userAgent: "test-agent",
  blockedDomains: new Set<string>(),
};

describe("text enrichment strategy", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("uses noop strategy when content fetching config is omitted", async () => {
    const strategy = createTextEnrichmentStrategy(
      undefined,
      createTestLogger(),
      "message"
    );

    expect(strategy).toBeInstanceOf(NoopTextEnrichmentStrategy);
    await expect(
      strategy.enrich({
        text: "existing text",
        url: "https://example.com",
        minLength: 300,
      })
    ).resolves.toBe("existing text");
  });

  it("uses article fetch strategy when fetching is enabled", () => {
    const strategy = createTextEnrichmentStrategy(
      enabledConfig,
      createTestLogger(),
      "message"
    );

    expect(strategy).toBeInstanceOf(ArticleFetchTextEnrichmentStrategy);
  });

  it("skips article fetch when text already meets minimum length", async () => {
    const strategy = createTextEnrichmentStrategy(
      enabledConfig,
      createTestLogger(),
      "message"
    );

    await expect(
      strategy.enrich({
        text: "x".repeat(500),
        url: "https://example.com",
        minLength: 300,
      })
    ).resolves.toBe("x".repeat(500));
    expect(mockFetchArticleContent).not.toHaveBeenCalled();
  });

  it("returns fetched article text when enrichment succeeds", async () => {
    mockFetchArticleContent.mockResolvedValue({
      success: true,
      text: "enriched text",
      title: "Article",
      htmlLength: 1234,
    });

    const logger = createTestLogger();
    const strategy = createTextEnrichmentStrategy(
      enabledConfig,
      logger,
      "Fetched article content"
    );

    await expect(
      strategy.enrich({
        text: "short",
        url: "https://example.com/article",
        minLength: 300,
        logContext: { source: "rss" },
      })
    ).resolves.toBe("enriched text");
    expect(mockFetchArticleContent).toHaveBeenCalledTimes(1);
    expect(logger.debug).toHaveBeenCalledWith(
      expect.objectContaining({
        source: "rss",
        url: "https://example.com/article",
        textLength: 13,
      }),
      "Fetched article content"
    );
  });

  it("keeps original text when enrichment fails", async () => {
    mockFetchArticleContent.mockResolvedValue(null);

    const strategy = createTextEnrichmentStrategy(
      enabledConfig,
      createTestLogger(),
      "Fetched article content"
    );

    await expect(
      strategy.enrich({
        text: "short",
        url: "https://example.com/article",
        minLength: 300,
      })
    ).resolves.toBe("short");
  });
});
