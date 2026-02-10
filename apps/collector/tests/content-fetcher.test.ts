import { beforeEach, describe, expect, it, vi } from "vitest";
import {
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
