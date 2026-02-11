import { Readability } from "@mozilla/readability";
import {
  createUrlSafetyFacade,
  type UrlSafetyFacade,
} from "@rising-intelligence/shared";
import { JSDOM, VirtualConsole } from "jsdom";
import type { Logger } from "pino";

/**
 * Configuration for article content fetching
 */
export interface ContentFetcherConfig {
  /** Enable/disable article fetching */
  enabled: boolean;
  /** Timeout for HTTP requests (ms) */
  timeoutMs: number;
  /** Maximum content length to extract (chars) */
  maxContentLength: number;
  /** Minimum content length to consider valid (chars) */
  minContentLength: number;
  /** Delay between requests to same domain (ms) */
  domainDelayMs: number;
  /** User agent string */
  userAgent: string;
  /** Domains to skip (e.g., paywalled sites) */
  blockedDomains: Set<string>;
}

/**
 * Result of article content extraction
 */
export interface ArticleContent {
  /** Extracted article text */
  text: string;
  /** Article title from content */
  title: string | null;
  /** Length of original HTML */
  htmlLength: number;
  /** Whether extraction was successful */
  success: boolean;
}

export interface ArticleContentFetcher {
  fetch(url: string): Promise<ArticleContent | null>;
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const urlSafetyFacade = createUrlSafetyFacade();

/**
 * Extract domain from URL
 */
function extractDomain(url: string): string | null {
  try {
    const parsed = new URL(url);
    return parsed.hostname.trim().toLowerCase();
  } catch {
    return null;
  }
}

/**
 * Check if domain is blocked
 */
function isDomainBlocked(url: string, blockedDomains: Set<string>): boolean {
  const domain = extractDomain(url);
  if (!domain) return false;

  // Check exact match
  if (blockedDomains.has(domain)) return true;

  // Check parent domains (e.g., block "example.com" blocks "www.example.com")
  const parts = domain.split(".");
  for (let i = 1; i < parts.length; i++) {
    const parentDomain = parts.slice(i).join(".");
    if (blockedDomains.has(parentDomain)) return true;
  }

  return false;
}

class InMemoryDomainRequestLimiter {
  private static readonly MAX_TRACKED_DOMAINS = 10_000;

  private readonly lastRequestByDomain = new Map<string, number>();

  constructor(
    private readonly domainDelayMs: number,
    private readonly now: () => number = Date.now
  ) {}

  async wait(url: string): Promise<void> {
    if (this.domainDelayMs <= 0) {
      return;
    }

    const domain = extractDomain(url);
    if (!domain) {
      return;
    }

    const lastRequest = this.lastRequestByDomain.get(domain);
    if (lastRequest !== undefined) {
      const elapsed = this.now() - lastRequest;
      const delayMs = this.domainDelayMs - elapsed;
      if (delayMs > 0) {
        await wait(delayMs);
      }
    }

    this.rememberRequest(domain, this.now());
  }

  private rememberRequest(domain: string, timestamp: number): void {
    if (this.lastRequestByDomain.has(domain)) {
      this.lastRequestByDomain.delete(domain);
    }
    this.lastRequestByDomain.set(domain, timestamp);

    if (this.lastRequestByDomain.size <= InMemoryDomainRequestLimiter.MAX_TRACKED_DOMAINS) {
      return;
    }

    const oldestDomain = this.lastRequestByDomain.keys().next().value;
    if (typeof oldestDomain === "string") {
      this.lastRequestByDomain.delete(oldestDomain);
    }
  }
}

/**
 * Fetch HTML from URL with timeout
 */
async function fetchHtml(
  url: string,
  timeoutMs: number,
  userAgent: string
): Promise<string> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        "User-Agent": userAgent,
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
      },
      redirect: "follow",
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    return await response.text();
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Extract article content from HTML using Readability
 */
function extractContent(html: string, url: string): ArticleContent {
  try {
    // Create a virtual console that suppresses CSS parsing warnings
    const virtualConsole = new VirtualConsole();
    virtualConsole.on("error", () => {
      // Suppress errors (mostly CSS parsing warnings from parse5)
    });

    const dom = new JSDOM(html, { url, virtualConsole });
    const reader = new Readability(dom.window.document);
    const article = reader.parse();

    if (!article || !article.textContent) {
      return {
        text: "",
        title: null,
        htmlLength: html.length,
        success: false,
      };
    }

    return {
      text: article.textContent.trim(),
      title: article.title || null,
      htmlLength: html.length,
      success: true,
    };
  } catch {
    return {
      text: "",
      title: null,
      htmlLength: html.length,
      success: false,
    };
  }
}

class DisabledArticleContentFetcher implements ArticleContentFetcher {
  async fetch(): Promise<null> {
    return null;
  }
}

class ReadabilityArticleContentFetcher implements ArticleContentFetcher {
  private readonly domainRequestLimiter: InMemoryDomainRequestLimiter;

  constructor(
    private readonly config: ContentFetcherConfig,
    private readonly logger: Logger,
    private readonly safetyFacade: UrlSafetyFacade = urlSafetyFacade
  ) {
    this.domainRequestLimiter = new InMemoryDomainRequestLimiter(
      config.domainDelayMs
    );
  }

  async fetch(url: string): Promise<ArticleContent | null> {
    if (!this.safetyFacade.isAllowedFetchUrl(url)) {
      this.logger.debug({ url }, "Skipping disallowed fetch URL");
      return null;
    }

    if (isDomainBlocked(url, this.config.blockedDomains)) {
      this.logger.debug({ url }, "Skipping blocked domain");
      return null;
    }

    await this.domainRequestLimiter.wait(url);

    try {
      const html = await fetchHtml(url, this.config.timeoutMs, this.config.userAgent);
      const content = extractContent(html, url);

      if (!content.success || content.text.length < this.config.minContentLength) {
        this.logger.debug(
          { url, textLength: content.text.length },
          "Content extraction failed or too short"
        );
        return null;
      }

      const normalizedContent: ArticleContent = {
        ...content,
        text: content.text.slice(0, this.config.maxContentLength),
      };

      this.logger.debug(
        {
          url,
          textLength: normalizedContent.text.length,
          htmlLength: normalizedContent.htmlLength,
        },
        "Article content extracted successfully"
      );

      return normalizedContent;
    } catch (error) {
      this.logger.debug(
        {
          url,
          error: error instanceof Error ? error.message : String(error),
        },
        "Failed to fetch article content"
      );
      return null;
    }
  }
}

export function createArticleContentFetcher(
  config: ContentFetcherConfig,
  logger: Logger
): ArticleContentFetcher {
  if (!config.enabled) {
    return new DisabledArticleContentFetcher();
  }

  return new ReadabilityArticleContentFetcher(config, logger);
}

/**
 * Backward-compatible convenience API used by tests and one-off callers.
 */
export async function fetchArticleContent(
  url: string,
  config: ContentFetcherConfig,
  logger: Logger
): Promise<ArticleContent | null> {
  const fetcher = createArticleContentFetcher(config, logger);
  return fetcher.fetch(url);
}

/**
 * Create default content fetcher configuration
 */
const DEFAULT_CONTENT_FETCHER_CONFIG: Readonly<
  Omit<ContentFetcherConfig, "blockedDomains">
> = Object.freeze({
  enabled: false,
  timeoutMs: 10_000,
  maxContentLength: 50_000,
  minContentLength: 200,
  domainDelayMs: 1_000,
  userAgent:
    "RisingIntelligence/1.0 (+https://github.com/rising-intelligence)",
});

function parseIntegerEnv(value: string | undefined): number | null {
  if (value === undefined) {
    return null;
  }

  const normalized = value.trim();
  if (normalized.length === 0) {
    return null;
  }
  if (!/^-?\d+$/.test(normalized)) {
    return null;
  }

  const parsed = Number.parseInt(normalized, 10);
  if (Number.isNaN(parsed)) {
    return null;
  }

  return parsed;
}

function parseBlockedDomains(value: string | undefined): Set<string> {
  if (!value) {
    return new Set<string>();
  }

  return new Set(
    value
      .split(",")
      .map((domain) => domain.trim().toLowerCase())
      .filter((domain) => domain.length > 0)
  );
}

class ContentFetcherConfigBuilder {
  private enabled = DEFAULT_CONTENT_FETCHER_CONFIG.enabled;
  private timeoutMs = DEFAULT_CONTENT_FETCHER_CONFIG.timeoutMs;
  private maxContentLength = DEFAULT_CONTENT_FETCHER_CONFIG.maxContentLength;
  private minContentLength = DEFAULT_CONTENT_FETCHER_CONFIG.minContentLength;
  private domainDelayMs = DEFAULT_CONTENT_FETCHER_CONFIG.domainDelayMs;
  private userAgent = DEFAULT_CONTENT_FETCHER_CONFIG.userAgent;
  private blockedDomains = new Set<string>();

  fromEnv(env: Record<string, string | undefined>): this {
    this.enabled = env.FETCH_ARTICLE_CONTENT === "true";
    this.timeoutMs = this.parsePositiveInteger(
      env.ARTICLE_FETCH_TIMEOUT_MS,
      DEFAULT_CONTENT_FETCHER_CONFIG.timeoutMs
    );
    this.maxContentLength = this.parsePositiveInteger(
      env.ARTICLE_MAX_CONTENT_LENGTH,
      DEFAULT_CONTENT_FETCHER_CONFIG.maxContentLength
    );
    this.minContentLength = this.parsePositiveInteger(
      env.ARTICLE_MIN_CONTENT_LENGTH,
      DEFAULT_CONTENT_FETCHER_CONFIG.minContentLength
    );
    this.domainDelayMs = this.parseNonNegativeInteger(
      env.ARTICLE_DOMAIN_DELAY_MS,
      DEFAULT_CONTENT_FETCHER_CONFIG.domainDelayMs
    );
    this.userAgent = this.parseUserAgent(env.ARTICLE_USER_AGENT);
    this.blockedDomains = parseBlockedDomains(env.ARTICLE_BLOCKED_DOMAINS);

    return this;
  }

  build(): ContentFetcherConfig {
    const minContentLength = this.minContentLength;
    const maxContentLength = Math.max(this.maxContentLength, minContentLength);

    return {
      enabled: this.enabled,
      timeoutMs: this.timeoutMs,
      maxContentLength,
      minContentLength,
      domainDelayMs: this.domainDelayMs,
      userAgent: this.userAgent,
      blockedDomains: new Set(this.blockedDomains),
    };
  }

  private parsePositiveInteger(
    value: string | undefined,
    fallback: number
  ): number {
    const parsed = parseIntegerEnv(value);
    if (parsed === null || parsed <= 0) {
      return fallback;
    }
    return parsed;
  }

  private parseNonNegativeInteger(
    value: string | undefined,
    fallback: number
  ): number {
    const parsed = parseIntegerEnv(value);
    if (parsed === null || parsed < 0) {
      return fallback;
    }
    return parsed;
  }

  private parseUserAgent(value: string | undefined): string {
    if (!value) {
      return DEFAULT_CONTENT_FETCHER_CONFIG.userAgent;
    }

    const normalized = value.trim();
    return normalized.length > 0
      ? normalized
      : DEFAULT_CONTENT_FETCHER_CONFIG.userAgent;
  }
}

export function createContentFetcherConfig(
  env: Record<string, string | undefined>
): ContentFetcherConfig {
  return new ContentFetcherConfigBuilder().fromEnv(env).build();
}
