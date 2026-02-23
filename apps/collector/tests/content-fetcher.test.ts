import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  createArticleContentFetcher,
  fetchArticleContent,
  createContentFetcherConfig,
  type ContentFetcherConfig,
} from "../src/content-fetcher.js";

const mockFetch = vi.fn();
global.fetch = mockFetch as unknown as typeof fetch;

const contentFetcherConfig: ContentFetcherConfig = {
  enabled: true,
  timeoutMs: 1_000,
  maxContentLength: 50_000,
  minContentLength: 200,
  domainDelayMs: 0,
  userAgent: "test-agent",
  blockedDomains: new Set<string>(),
};

function createTestLogger() {
  return {
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    child: vi.fn(),
  } as any;
}

describe("content fetcher URL safety", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("skips localhost URLs", async () => {
    const logger = createTestLogger();

    await expect(
      fetchArticleContent(
        "http://localhost/internal",
        contentFetcherConfig,
        logger
      )
    ).resolves.toBeNull();

    expect(mockFetch).not.toHaveBeenCalled();
    expect(logger.debug).toHaveBeenCalledWith(
      { url: "http://localhost/internal" },
      "Skipping disallowed fetch URL"
    );
  });

  it("skips private IPv4 URLs", async () => {
    const logger = createTestLogger();

    await expect(
      fetchArticleContent(
        "http://192.168.1.10/internal",
        contentFetcherConfig,
        logger
      )
    ).resolves.toBeNull();

    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("skips loopback IPv6 URLs", async () => {
    const logger = createTestLogger();

    await expect(
      fetchArticleContent("http://[::1]/", contentFetcherConfig, logger)
    ).resolves.toBeNull();

    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("skips unspecified IPv6 URLs", async () => {
    const logger = createTestLogger();

    await expect(
      fetchArticleContent("http://[::]/", contentFetcherConfig, logger)
    ).resolves.toBeNull();

    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("skips IPv4-mapped IPv6 loopback URLs", async () => {
    const logger = createTestLogger();

    await expect(
      fetchArticleContent(
        "http://[::ffff:127.0.0.1]/internal",
        contentFetcherConfig,
        logger
      )
    ).resolves.toBeNull();

    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("skips non-http protocols", async () => {
    const logger = createTestLogger();

    await expect(
      fetchArticleContent(
        "file:///etc/passwd",
        contentFetcherConfig,
        logger
      )
    ).resolves.toBeNull();

    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("skips blocked parent domains", async () => {
    const logger = createTestLogger();
    const fetcher = createArticleContentFetcher(
      {
        ...contentFetcherConfig,
        blockedDomains: new Set(["example.com"]),
      },
      logger
    );

    await expect(
      fetcher.fetch("https://sub.example.com/article")
    ).resolves.toBeNull();

    expect(mockFetch).not.toHaveBeenCalled();
    expect(logger.debug).toHaveBeenCalledWith(
      { url: "https://sub.example.com/article" },
      "Skipping blocked domain"
    );
  });

  it("allows public https URLs and attempts fetch", async () => {
    const logger = createTestLogger();
    mockFetch.mockResolvedValue({
      ok: false,
      status: 404,
      statusText: "Not Found",
    });

    await expect(
      fetchArticleContent(
        "https://example.com/article",
        contentFetcherConfig,
        logger
      )
    ).resolves.toBeNull();

    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("supports dependency injection seams for safety, throttling, and transport", async () => {
    const logger = createTestLogger();
    const wait = vi.fn(async (_url: string) => undefined);
    const createDomainRequestLimiter = vi.fn(() => ({ wait }));
    const fetchHtml = vi.fn(async () => "<html />");
    const extractContent = vi.fn(() => ({
      success: true,
      text: "x".repeat(250),
      title: "t",
      htmlLength: 7,
    }));
    const safetyFacade = {
      isAllowedFetchUrl: vi.fn(() => true),
    };
    const fetcher = createArticleContentFetcher(contentFetcherConfig, logger, {
      safetyFacade,
      createDomainRequestLimiter,
      fetchHtml,
      extractContent,
    });

    const result = await fetcher.fetch("https://example.com/article");

    expect(result).toEqual({
      success: true,
      text: "x".repeat(250),
      title: "t",
      htmlLength: 7,
    });
    expect(createDomainRequestLimiter).toHaveBeenCalledWith(0);
    expect(wait).toHaveBeenCalledWith("https://example.com/article");
    expect(safetyFacade.isAllowedFetchUrl).toHaveBeenCalledWith(
      "https://example.com/article"
    );
    expect(fetchHtml).toHaveBeenCalledWith(
      "https://example.com/article",
      1_000,
      "test-agent"
    );
    expect(extractContent).toHaveBeenCalledWith("<html />", "https://example.com/article");
  });

  it("serializes concurrent fetches for the same domain", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-02-11T00:00:00.000Z"));

    try {
      const logger = createTestLogger();
      const callTimes: number[] = [];
      const fetchHtml = vi.fn(async () => {
        callTimes.push(Date.now());
        return "<html />";
      });
      const extractContent = vi.fn(() => ({
        success: true,
        text: "x".repeat(250),
        title: "t",
        htmlLength: 7,
      }));
      const fetcher = createArticleContentFetcher(
        {
          ...contentFetcherConfig,
          domainDelayMs: 100,
        },
        logger,
        {
          safetyFacade: {
            isAllowedFetchUrl: () => true,
          },
          fetchHtml,
          extractContent,
        }
      );

      const firstFetch = fetcher.fetch("https://example.com/first");
      const secondFetch = fetcher.fetch("https://example.com/second");

      await Promise.resolve();
      expect(fetchHtml).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(99);
      expect(fetchHtml).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(1);
      expect(fetchHtml).toHaveBeenCalledTimes(2);

      const [firstResult, secondResult] = await Promise.all([firstFetch, secondFetch]);
      expect(firstResult).toEqual({
        success: true,
        text: "x".repeat(250),
        title: "t",
        htmlLength: 7,
      });
      expect(secondResult).toEqual({
        success: true,
        text: "x".repeat(250),
        title: "t",
        htmlLength: 7,
      });
      expect(callTimes).toHaveLength(2);
      expect(callTimes[1] - callTimes[0]).toBeGreaterThanOrEqual(100);
    } finally {
      vi.useRealTimers();
    }
  });

  it("returns a disabled fetcher when content fetching is off", async () => {
    const logger = createTestLogger();
    const fetcher = createArticleContentFetcher(
      {
        ...contentFetcherConfig,
        enabled: false,
      },
      logger
    );

    await expect(
      fetcher.fetch("https://example.com/article")
    ).resolves.toBeNull();

    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("normalizes blocked domains to lowercase", () => {
    const config = createContentFetcherConfig({
      FETCH_ARTICLE_CONTENT: "true",
      ARTICLE_BLOCKED_DOMAINS: "Example.COM, Sub.Example.com",
    });

    expect(config.blockedDomains.has("example.com")).toBe(true);
    expect(config.blockedDomains.has("sub.example.com")).toBe(true);
  });

  it("falls back to safe defaults when numeric env values are invalid", () => {
    const config = createContentFetcherConfig({
      FETCH_ARTICLE_CONTENT: "true",
      ARTICLE_FETCH_TIMEOUT_MS: "invalid",
      ARTICLE_MAX_CONTENT_LENGTH: "-5",
      ARTICLE_MIN_CONTENT_LENGTH: "0",
      ARTICLE_DOMAIN_DELAY_MS: "10ms",
      ARTICLE_USER_AGENT: "   ",
    });

    expect(config.enabled).toBe(true);
    expect(config.timeoutMs).toBe(10_000);
    expect(config.maxContentLength).toBe(50_000);
    expect(config.minContentLength).toBe(200);
    expect(config.domainDelayMs).toBe(1_000);
    expect(config.userAgent).toBe(
      "RisingIntelligence/1.0 (+https://github.com/rising-intelligence)"
    );
  });

  it("enforces max content length to be at least min content length", () => {
    const config = createContentFetcherConfig({
      FETCH_ARTICLE_CONTENT: "true",
      ARTICLE_MAX_CONTENT_LENGTH: "100",
      ARTICLE_MIN_CONTENT_LENGTH: "300",
    });

    expect(config.minContentLength).toBe(300);
    expect(config.maxContentLength).toBe(300);
  });
});
