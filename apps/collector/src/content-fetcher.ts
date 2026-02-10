import { Readability } from "@mozilla/readability";
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

/**
 * Extract domain from URL
 */
function extractDomain(url: string): string | null {
  try {
    const parsed = new URL(url);
    return parsed.hostname;
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
  if (lastRequest) {
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
export function createContentFetcherConfig(
  env: Record<string, string | undefined>
): ContentFetcherConfig {
  const enabled = env.FETCH_ARTICLE_CONTENT === "true";
  const timeoutMs = parseInt(env.ARTICLE_FETCH_TIMEOUT_MS ?? "10000", 10);
  const maxContentLength = parseInt(
    env.ARTICLE_MAX_CONTENT_LENGTH ?? "50000",
    10
  );
  const minContentLength = parseInt(
    env.ARTICLE_MIN_CONTENT_LENGTH ?? "200",
    10
  );
  const domainDelayMs = parseInt(env.ARTICLE_DOMAIN_DELAY_MS ?? "1000", 10);
  const userAgent =
    env.ARTICLE_USER_AGENT ??
    "RisingIntelligence/1.0 (+https://github.com/rising-intelligence)";

  // Parse blocked domains
  const blockedDomainsStr = env.ARTICLE_BLOCKED_DOMAINS ?? "";
  const blockedDomains = new Set(
    blockedDomainsStr
      .split(",")
      .map((d) => d.trim())
      .filter((d) => d.length > 0)
  );

  return {
    enabled,
    timeoutMs,
    maxContentLength,
    minContentLength,
    domainDelayMs,
    userAgent,
    blockedDomains,
  };
}
