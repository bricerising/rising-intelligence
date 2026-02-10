import { Readability } from "@mozilla/readability";
import { createUrlSafetyFacade } from "@rising-intelligence/shared";
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

/**
 * Track last request time per domain for rate limiting
 */
const domainLastRequest = new Map<string, number>();
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

/**
 * Wait for domain rate limit delay
 */
async function waitForDomainDelay(
  url: string,
  domainDelayMs: number
): Promise<void> {
  const domain = extractDomain(url);
  if (!domain) return;

  const lastRequest = domainLastRequest.get(domain);
  if (lastRequest !== undefined) {
    const elapsed = Date.now() - lastRequest;
    if (elapsed < domainDelayMs) {
      await new Promise((resolve) =>
        setTimeout(resolve, domainDelayMs - elapsed)
      );
    }
  }

  domainLastRequest.set(domain, Date.now());
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
  } catch (error) {
    return {
      text: "",
      title: null,
      htmlLength: html.length,
      success: false,
    };
  }
}

/**
 * Fetch and extract article content from URL
 */
export async function fetchArticleContent(
  url: string,
  config: ContentFetcherConfig,
  logger: Logger
): Promise<ArticleContent | null> {
  // Check if fetching is enabled
  if (!config.enabled) {
    return null;
  }

  if (!urlSafetyFacade.isAllowedFetchUrl(url)) {
    logger.debug({ url }, "Skipping disallowed fetch URL");
    return null;
  }

  // Check if domain is blocked
  if (isDomainBlocked(url, config.blockedDomains)) {
    logger.debug({ url }, "Skipping blocked domain");
    return null;
  }

  // Wait for domain rate limit
  await waitForDomainDelay(url, config.domainDelayMs);

  try {
    // Fetch HTML
    const html = await fetchHtml(url, config.timeoutMs, config.userAgent);

    // Extract content
    const content = extractContent(html, url);

    // Validate content length
    if (!content.success || content.text.length < config.minContentLength) {
      logger.debug(
        { url, textLength: content.text.length },
        "Content extraction failed or too short"
      );
      return null;
    }

    // Truncate if too long
    if (content.text.length > config.maxContentLength) {
      content.text = content.text.substring(0, config.maxContentLength);
    }

    logger.debug(
      { url, textLength: content.text.length, htmlLength: content.htmlLength },
      "Article content extracted successfully"
    );

    return content;
  } catch (error) {
    logger.debug(
      { url, error: (error as Error).message },
      "Failed to fetch article content"
    );
    return null;
  }
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
